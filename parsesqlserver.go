package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
)

// SQLServerXEventRecord 对应 SQL Server Extended Events 导出的 CSV 格式
// 字段顺序: event_time, duration_us, cpu_time_us, logical_reads, writes, row_count, session_id, database_name, username, sql_text
type SQLServerXEventRecord struct {
	EventTime     string // 事件时间
	DurationUS    int64  // 执行时长 (微秒)
	CPUTimeUS     int64  // CPU 时间 (微秒)
	LogicalReads  int64  // 逻辑读次数
	Writes        int64  // 写入次数
	RowCount      int    // 返回行数
	SessionID     string // 会话 ID
	DatabaseName  string // 数据库名
	Username      string // 用户名
	SQLText       string // SQL 文本
}

// ParseSQLServerXEvents 解析 SQL Server Extended Events 导出的 CSV 文件
// CSV 格式要求:
//   - 第一行为表头
//   - 字段顺序: event_time, duration_us, cpu_time_us, logical_reads, writes, row_count, session_id, database_name, username, sql_text
//   - 时间格式支持: "2006-01-02 15:04:05.0000000" 或 "2006-01-02 15:04:05"
func ParseSQLServerXEvents(csvFilePath, slowOutputPath string) {
	if csvFilePath == "" || slowOutputPath == "" {
		fmt.Println("Usage: ./sql-replay -mode parsesqlserver -slow-in <path_to_xevent_csv> -slow-out <path_to_slow_output_file>")
		return
	}

	// 打开 CSV 文件
	file, err := os.Open(csvFilePath)
	if err != nil {
		log.Fatal("Error opening CSV file:", err)
	}
	defer file.Close()

	// 创建输出文件
	outputFile, err := os.Create(slowOutputPath)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile.Close()

	// 创建 CSV reader
	reader := csv.NewReader(file)
	reader.LazyQuotes = true        // 处理带引号的字段
	reader.FieldsPerRecord = -1     // 允许字段数量可变

	// 读取表头
	headers, err := reader.Read()
	if err != nil {
		log.Fatal("Error reading CSV header:", err)
	}

	// 打印表头信息
	fmt.Fprintf(os.Stderr, "CSV headers: %v\n", headers)

	// 建立字段索引映射
	fieldIndex := buildFieldIndex(headers)

	// 处理 CSV 记录
	recordCount := 0
	skippedCount := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV record: %v", err)
			continue
		}

		// 解析记录
		xeventRecord, err := parseSQLServerRecord(record, fieldIndex)
		if err != nil {
			log.Printf("Error parsing record: %v", err)
			skippedCount++
			continue
		}

		// 跳过空 SQL
		if strings.TrimSpace(xeventRecord.SQLText) == "" {
			skippedCount++
			continue
		}

		// 转换为 LogEntry 格式
		logEntry := convertXEventToLogEntry(xeventRecord)

		// 输出 JSON
		jsonData, err := json.Marshal(logEntry)
		if err != nil {
			log.Printf("Error marshaling JSON: %v", err)
			skippedCount++
			continue
		}

		fmt.Fprintln(outputFile, string(jsonData))
		recordCount++
	}

	fmt.Fprintf(os.Stderr, "Parse completed. Total records: %d, Skipped: %d\n", recordCount, skippedCount)
}

// buildFieldIndex 建立字段名到索引的映射
// 支持多种可能的字段名写法
func buildFieldIndex(headers []string) map[string]int {
	index := make(map[string]int)
	
	// 定义字段名的可能别名
	fieldAliases := map[string][]string{
		"event_time":    {"event_time", "EventTime", "event time", "timestamp", "Timestamp"},
		"duration_us":   {"duration_us", "DurationUS", "duration_us", "duration", "Duration"},
		"cpu_time_us":   {"cpu_time_us", "CPUTimeUS", "cpu_time", "CPUTime"},
		"logical_reads": {"logical_reads", "LogicalReads", "reads", "Reads"},
		"writes":        {"writes", "Writes"},
		"row_count":     {"row_count", "RowCount", "rows", "Rows", "row_count"},
		"session_id":    {"session_id", "SessionID", "session_id", "spid", "SPID"},
		"database_name": {"database_name", "DatabaseName", "db_name", "DBName", "database"},
		"username":      {"username", "Username", "user_name", "UserName", "user"},
		"sql_text":      {"sql_text", "SQLText", "statement", "Statement", "sql"},
	}

	for i, header := range headers {
		header = strings.TrimSpace(header)
		for fieldName, aliases := range fieldAliases {
			for _, alias := range aliases {
				if strings.EqualFold(header, alias) {
					index[fieldName] = i
					break
				}
			}
		}
	}

	return index
}

// parseSQLServerRecord 解析单条 CSV 记录
func parseSQLServerRecord(record []string, index map[string]int) (*SQLServerXEventRecord, error) {
	getField := func(name string) string {
		if i, ok := index[name]; ok && i < len(record) {
			return record[i]
		}
		return ""
	}

	xevent := &SQLServerXEventRecord{
		EventTime:    getField("event_time"),
		SessionID:    getField("session_id"),
		DatabaseName: getField("database_name"),
		Username:     getField("username"),
		SQLText:      getField("sql_text"),
	}

	// 解析数值字段
	if v := getField("duration_us"); v != "" {
		// 支持微秒和毫秒两种格式
		if val, err := strconv.ParseInt(v, 10, 64); err == nil {
			// 如果值很大，可能是微秒；如果是小值，可能是毫秒，需要转换
			xevent.DurationUS = val
		}
	}

	if v := getField("cpu_time_us"); v != "" {
		if val, err := strconv.ParseInt(v, 10, 64); err == nil {
			xevent.CPUTimeUS = val
		}
	}

	if v := getField("logical_reads"); v != "" {
		if val, err := strconv.ParseInt(v, 10, 64); err == nil {
			xevent.LogicalReads = val
		}
	}

	if v := getField("writes"); v != "" {
		if val, err := strconv.ParseInt(v, 10, 64); err == nil {
			xevent.Writes = val
		}
	}

	if v := getField("row_count"); v != "" {
		if val, err := strconv.Atoi(v); err == nil {
			xevent.RowCount = val
		}
	}

	return xevent, nil
}

// convertXEventToLogEntry 将 SQL Server XEvent 记录转换为 LogEntry 格式
func convertXEventToLogEntry(xevent *SQLServerXEventRecord) *LogEntry {
	// 解析时间戳
	var timestamp float64
	if xevent.EventTime != "" {
		// 尝试多种时间格式
		formats := []string{
			"2006-01-02 15:04:05.0000000",
			"2006-01-02 15:04:05.000000",
			"2006-01-02 15:04:05.000",
			"2006-01-02 15:04:05",
			time.RFC3339,
			time.RFC3339Nano,
		}

		var parsedTime time.Time
		var err error
		for _, format := range formats {
			parsedTime, err = time.Parse(format, xevent.EventTime)
			if err == nil {
				break
			}
		}

		if err == nil {
			timestamp = float64(parsedTime.Unix()) + float64(parsedTime.Nanosecond())/1e9
		}
	}

	// 提取 SQL 类型
	sqlType := extractSQLServerSQLType(xevent.SQLText)

	// 清理 SQL 文本
	cleanedSQL := cleanSQLServerSQL(xevent.SQLText)

	// 生成 SQL digest (fingerprint)
	digest := generateSQLDigest(cleanedSQL)

	return &LogEntry{
		ConnectionID: xevent.SessionID,
		QueryTime:    xevent.DurationUS, // 微秒
		SQL:          cleanedSQL,
		RowsSent:     xevent.RowCount,
		Username:     xevent.Username,
		SQLType:      sqlType,
		DBName:       xevent.DatabaseName,
		Timestamp:    timestamp,
		Digest:       digest,
	}
}

// extractSQLServerSQLType 从 SQL Server SQL 文本中提取 SQL 类型
func extractSQLServerSQLType(sqlText string) string {
	if sqlText == "" {
		return "other"
	}

	// 清理 SQL 文本
	cleaned := strings.TrimSpace(sqlText)
	cleaned = strings.Trim(cleaned, "\"'")
	cleaned = strings.TrimSpace(cleaned)

	// 转为大写便于匹配
	upper := strings.ToUpper(cleaned)

	// SQL Server 常见语句类型
	switch {
	case strings.HasPrefix(upper, "SELECT"):
		return "select"
	case strings.HasPrefix(upper, "INSERT"):
		return "insert"
	case strings.HasPrefix(upper, "UPDATE"):
		return "update"
	case strings.HasPrefix(upper, "DELETE"):
		return "delete"
	case strings.HasPrefix(upper, "EXEC") || strings.HasPrefix(upper, "EXECUTE"):
		return "exec"
	case strings.HasPrefix(upper, "CALL"):
		return "call"
	case strings.HasPrefix(upper, "WITH"):
		return "select" // CTE 通常是 SELECT
	default:
		return "other"
	}
}

// cleanSQLServerSQL 清理 SQL Server SQL 文本
func cleanSQLServerSQL(sqlText string) string {
	// 移除首尾空白和引号
	cleaned := strings.TrimSpace(sqlText)
	cleaned = strings.Trim(cleaned, "\"'")

	// 替换 Windows 换行符
	cleaned = strings.ReplaceAll(cleaned, "\r\n", " ")
	cleaned = strings.ReplaceAll(cleaned, "\n", " ")

	// 压缩多余空白
	spaceBuf := make([]byte, 0, len(cleaned))
	inSpace := false
	for i := 0; i < len(cleaned); i++ {
		if cleaned[i] == ' ' || cleaned[i] == '\t' {
			if !inSpace {
				spaceBuf = append(spaceBuf, ' ')
				inSpace = true
			}
		} else {
			spaceBuf = append(spaceBuf, cleaned[i])
			inSpace = false
		}
	}

	return string(spaceBuf)
}

// generateSQLDigest 生成 SQL digest (fingerprint)
func generateSQLDigest(sqlText string) string {
	if sqlText == "" {
		return ""
	}

	// 使用 TiDB parser 进行标准化
	normalized := parser.Normalize(sqlText)
	digest := parser.DigestNormalized(normalized)
	
	return digest.String()
}