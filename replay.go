package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type SQLExecutionRecord struct {
	SQL           string `json:"sql"`
	QueryTime     int64  `json:"query_time"`
	RowsSent      int    `json:"rows_sent"`
	ExecutionTime int64  `json:"execution_time"`
	RowsReturned  int64  `json:"rows_returned"`
	ErrorInfo     string `json:"error_info,omitempty"`
	FileName      string // File name
	DBName        string `json:"dbname"`
}

type LogEntry struct {
	ConnectionID string  `json:"connection_id"`
	QueryTime    int64   `json:"query_time"`
	SQL          string  `json:"sql"`
	RowsSent     int     `json:"rows_sent"`
	Username     string  `json:"username"`
	SQLType      string  `json:"sql_type"`
	DBName       string  `json:"dbname"`
	Timestamp    float64 `json:"ts"`
	Digest       string  `json:"digest"`
	CloseConn    bool    `json:"close_conn,omitempty"`
}

// CSVRecord 对应CSV的每一行
type CSVRecord struct {
	Timestamp     string  `csv:"TS"`
	SQLID         string  `csv:"TID"`
	SQLText       string  `csv:"LOG"`
	DBName        string  `csv:"DB"`
	ExecutionTime float64 `csv:"LATENCY"`
	LockWaitTime  float64 `csv:"LOCK_TIME"`
	ReturnRows    int     `csv:"RETURN_ROWS"`
	ScanRows      int64   `csv:"UPDATE_ROWS"`
	SourceIP      string  `csv:"USER_IP"`
	SQLTye        string  `csv:"SQL_TYPE"`
	Username      string  `csv:"USER"`
	ThreadID      string  `csv:"TID"`
	TableNames    string  `csv:"TABLE_NAME"`
	Tags          string  `csv:"TID"`
}

// // CSVRecord 对应CSV的每一行
// type CSVRecord struct {
// 	Timestamp     string  `csv:"Timestamp"`
// 	SQLID         string  `csv:"SQL ID"`
// 	SQLText       string  `csv:"SQLText"`
// 	DBName        string  `csv:"DBName"`
// 	ExecutionTime float64 `csv:"执行耗时(秒)"`
// 	LockWaitTime  float64 `csv:"锁等待耗时(秒)"`
// 	ReturnRows    int     `csv:"返回行数"`
// 	ScanRows      int64   `csv:"扫描行数"`
// 	SourceIP      string  `csv:"访问来源"`
// 	Username      string  `csv:"用户"`
// 	ThreadID      string  `csv:"线程"`
// 	TableNames    string  `csv:"表名"`
// 	Tags          string  `csv:"标签"`
// }

type SQLTask struct {
	Entry LogEntry
	DB    *sql.DB
}

var i18n *I18n
var totalReplayed int64

func init() {
	var err error
	i18n, err = NewI18n("en")
	if err != nil {
		panic(err)
	}
}

type replayWriter struct {
	path string
	fh   *os.File
	buf  *bufio.Writer
}

func newReplayWriter(basePath, connID string) (*replayWriter, error) {
	path := fmt.Sprintf("%s.%s", basePath, connID)
	fh, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &replayWriter{
		path: path,
		fh:   fh,
		buf:  bufio.NewWriterSize(fh, 256*1024), // 256 KB write buffer
	}, nil
}

func (w *replayWriter) writeRecord(record SQLExecutionRecord) error {
	jsonData, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := w.buf.Write(jsonData); err != nil {
		return err
	}
	return w.buf.WriteByte('\n')
}

func (w *replayWriter) close() error {
	if w.buf != nil {
		w.buf.Flush()
	}
	if w.fh != nil {
		return w.fh.Close()
	}
	return nil
}

func executeSQLAndRecord(task SQLTask, w *replayWriter) error {
	if task.DB == nil {
		return fmt.Errorf("database connection is nil")
	}
	startTime := time.Now()

	rows, err := task.DB.Query(task.Entry.SQL)
	var rowsReturned int64
	var errorInfo string

	if err != nil {
		errorInfo = err.Error()
	} else {
		for rows.Next() {
			rowsReturned++
		}
		rows.Close()
	}

	executionTime := time.Since(startTime).Microseconds()

	return w.writeRecord(SQLExecutionRecord{
		SQL:           task.Entry.SQL,
		QueryTime:     task.Entry.QueryTime,
		RowsSent:      task.Entry.RowsSent,
		DBName:        task.Entry.DBName,
		ExecutionTime: executionTime,
		RowsReturned:  rowsReturned,
		ErrorInfo:     errorInfo,
	})
}

func streamEntries(slowOutputPath, filterUsername, filterSQLType, filterDBName string, ignoreDigestList []string, dbConnStr, replayOutputFilePath string, speed float64, lang string) error {
	inputFile, err := os.Open(slowOutputPath)
	if err != nil {
		return fmt.Errorf("file open error: %w", err)
	}
	defer inputFile.Close()

	logFilePath := "ignored_digests.log"
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
	}
	if logFile != nil {
		defer logFile.Close()
	}

	scanner := bufio.NewScanner(inputFile)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // 64 KB initial, 16 MB max per line

	const overflowWarnLimit = 100000

	var (
		connChans     = make(map[string]chan LogEntry)
		overflow      = make(map[string][]LogEntry)
		connMu        sync.Mutex
		wg            sync.WaitGroup
		minTimestamp  float64
		minTSOnce     sync.Once
		totalParsed   int64
		parseStart    = time.Now()
		connsClosedEarly int64
	)

	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal([]byte(scanner.Text()), &entry); err != nil {
			fmt.Println("Error parsing log entry:", err)
			continue
		}

		if filterUsername != "all" && entry.Username != filterUsername {
			continue
		}
		if filterSQLType != "all" && entry.SQLType != filterSQLType {
			continue
		}
		if filterDBName != "all" && entry.DBName != filterDBName {
			continue
		}
		if contains(ignoreDigestList, entry.Digest) {
			if logFile != nil {
				fmt.Fprintf(logFile, "%s, %s\n", entry.Digest, entry.SQL)
			}
			continue
		}

		minTSOnce.Do(func() { minTimestamp = entry.Timestamp })

		connMu.Lock()
		ch, ok := connChans[entry.ConnectionID]
		if !ok {
			ch = make(chan LogEntry, 10000)
			connChans[entry.ConnectionID] = ch
			wg.Add(1)
			go replaySQLForConnection(entry.ConnectionID, ch, overflow, connChans, dbConnStr, replayOutputFilePath, minTimestamp, speed, lang, &wg, &connMu)
		}
		connMu.Unlock()

		// Non-blocking send; fall back to overflow (same as original).
		// Soft warn at overflowWarnLimit to signal high memory usage,
		// but never block the parser — a single blocked send stalls
		// ALL connections because the parser is the sole producer.
		select {
		case ch <- entry:
		default:
			connMu.Lock()
			oLen := len(overflow[entry.ConnectionID])
			if oLen == overflowWarnLimit {
				fmt.Fprintf(os.Stderr, "[WARN] conn %s overflow reached %d, memory usage climbing\n", entry.ConnectionID, oLen)
			}
			overflow[entry.ConnectionID] = append(overflow[entry.ConnectionID], entry)
			connMu.Unlock()
		}

		// If this entry has a close marker, close the channel immediately.
		// The goroutine will drain buffered entries + overflow, then exit,
		// freeing its DB connection and memory.
		if entry.CloseConn {
			connMu.Lock()
			delete(connChans, entry.ConnectionID)
			close(ch)
			connMu.Unlock()
			atomic.AddInt64(&connsClosedEarly, 1)
		}

		totalParsed++
		if totalParsed%100000 == 0 {
			elapsed := time.Since(parseStart).Seconds()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			fmt.Printf("[%s] 解析进度: %d 条, %.0f 条/秒, %d 活跃连接, heap %.0fMB\n",
				time.Now().Format("15:04:05"), totalParsed, float64(totalParsed)/elapsed, len(connChans), float64(m.HeapAlloc)/1024/1024)
		}
	}

	connMu.Lock()
	activeConns := len(connChans)
	for _, ch := range connChans {
		close(ch)
	}
	connMu.Unlock()

	replayStart := time.Now()
	fmt.Printf("[%s] 解析完成: %d 条, 解析耗时 %v, 剩余活跃连接 %d, 已提前关闭连接 %d\n",
		replayStart.Format("15:04:05"), totalParsed, replayStart.Sub(parseStart), activeConns, atomic.LoadInt64(&connsClosedEarly))

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastReplayed int64
	lastTick := time.Now()
	for {
		select {
		case <-done:
			elapsed := time.Since(replayStart).Seconds()
			replayed := atomic.LoadInt64(&totalReplayed)
			fmt.Printf("[%s] 回放完成: %d/%d 条, 总耗时 %v, 平均 QPS %.0f\n",
				time.Now().Format("15:04:05"), replayed, totalParsed, time.Since(parseStart), float64(replayed)/elapsed)
			return nil
		case <-ticker.C:
			replayed := atomic.LoadInt64(&totalReplayed)
			elapsed := time.Since(lastTick).Seconds()
			qps := float64(replayed-lastReplayed) / elapsed
			lastReplayed = replayed
			lastTick = time.Now()
			connMu.Lock()
			conns := len(connChans)
			connMu.Unlock()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			fmt.Printf("[%s] 回放进度: %d/%d (%.1f%%), 活跃连接 %d, QPS %.0f, heap %.0fMB, goroutines %d\n",
				time.Now().Format("15:04:05"), replayed, totalParsed, float64(replayed)/float64(totalParsed)*100, conns, qps,
				float64(m.HeapAlloc)/1024/1024, runtime.NumGoroutine())
		}
	}
}

func contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func replaySQLForConnection(connID string, ch <-chan LogEntry, overflow map[string][]LogEntry, connChans map[string]chan LogEntry, dbConnStr string, replayOutputFilePath string, minTimestamp float64, speed float64, lang string, wg *sync.WaitGroup, connMu *sync.Mutex) {
	defer func() {
		connMu.Lock()
		delete(connChans, connID)
		connMu.Unlock()
		wg.Done()
	}()

	db, err := sql.Open("mysql", dbConnStr)
	if err != nil {
		fmt.Printf(i18n.T(lang, "db_open_error")+"\n", connID, err)
		for range ch {
		}
		return
	}
	defer db.Close()

	w, err := newReplayWriter(replayOutputFilePath, connID)
	if err != nil {
		fmt.Printf("[WARN] conn %s: cannot open output file: %v\n", connID, err)
		for range ch {
		}
		return
	}
	defer w.close()

	var prevTimestamp float64
	var lastQueryTime int64
	firstEntry := true

	replayOne := func(entry LogEntry) {
		if firstEntry {
			prevTimestamp = entry.Timestamp - (entry.Timestamp - minTimestamp)
			firstEntry = false
		} else {
			interval := (entry.Timestamp - prevTimestamp - float64(lastQueryTime)/1e6) / speed
			if interval > 0 {
				time.Sleep(time.Duration(interval * float64(time.Second)))
			}
			prevTimestamp = entry.Timestamp
		}
		task := SQLTask{Entry: entry, DB: db}
		if err := executeSQLAndRecord(task, w); err != nil {
			fmt.Printf(i18n.T(lang, "sql_exec_error")+"\n", connID, err)
		}
		lastQueryTime = entry.QueryTime
		atomic.AddInt64(&totalReplayed, 1)
	}

	for entry := range ch {
		replayOne(entry)
	}

	connMu.Lock()
	tail := overflow[connID]
	delete(overflow, connID)
	connMu.Unlock()
	for _, entry := range tail {
		replayOne(entry)
	}
}

func StartSQLReplay(dbConnStr string, speed float64, slowOutputPath, replayOutputFilePath, filterUsername, filterSQLType, filterDBName, ignoreDigests string, lang string) {
	if dbConnStr == "" || slowOutputPath == "" || replayOutputFilePath == "" {
		fmt.Println(i18n.T(lang, "usage"))
		return
	}

	if speed <= 0 {
		fmt.Println(i18n.T(lang, "invalid_speed"))
		return
	}
	var ignoreDigestList []string
	if ignoreDigests != "" {
		ignoreDigestList = strings.Split(ignoreDigests, ",")
	}
	fmt.Printf(i18n.T(lang, "replay_info")+"\n", filterUsername, filterDBName, filterSQLType, speed)
	fmt.Println("Ignored Digests: " + ignoreDigests)
	fmt.Println("Ignored Digests And SQL Info: ignored_digests.log")

	ts0 := time.Now()
	fmt.Printf("[%s] %s\n", ts0.Format("2006-01-02 15:04:05.000"), i18n.T(lang, "parsing_start"))

	if err := streamEntries(slowOutputPath, filterUsername, filterSQLType, filterDBName, ignoreDigestList, dbConnStr, replayOutputFilePath, speed, lang); err != nil {
		fmt.Println(i18n.T(lang, "file_open_error"), err)
		return
	}

	ts1 := time.Now()
	fmt.Printf("[%s] %s, ", ts1.Format("2006-01-02 15:04:05.000"), i18n.T(lang, "replay_complete"))
	fmt.Printf("%s %v\n", i18n.T(lang, "replay_time"), ts1.Sub(ts0))
}
