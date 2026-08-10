package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/parser"
)

const (
	batchSize = 1000
	workers   = 4
)

// errorCodeRE matches MySQL-style error codes in driver error messages:
//
//	"Error 1062 (23000): Duplicate entry ..."
//	"Error 1146 (42S02): Table 'db.t' doesn't exist"
var errorCodeRE = regexp.MustCompile(`Error (\d{4})`)

// errorClass holds the three classification dimensions for one error.
type errorClass struct {
	Category   string // human-readable category label
	Fixability string // e.g. "配置调整", "回放机制", "⚠️ 待确认"
	Suggestion string // concrete fix suggestion
}

// classifyError categorises a MySQL error string using the standard error code
// when present, and falls back to keyword matching otherwise.  It returns all
// three dimensions so that both load and report can store/consume them without
// hard-coding category names.
func classifyError(errorInfo string) errorClass {
	if errorInfo == "" {
		return errorClass{}
	}

	// 1) Try MySQL standard error codes first (database-agnostic).
	if m := errorCodeRE.FindStringSubmatch(errorInfo); len(m) >= 2 {
		switch m[1] {
		case "1062": // Duplicate entry for key
			return errorClass{
				Category:   "主键/唯一键冲突",
				Fixability: "回放机制",
				Suggestion: "使用 INSERT IGNORE 或 REPLACE INTO",
			}
		case "1146", "1051": // Table doesn't exist / Unknown table
			return errorClass{
				Category:   "表不存在",
				Fixability: "迁移补充",
				Suggestion: "补充迁移缺失表",
			}
		case "1054": // Unknown column
			return errorClass{
				Category:   "列缺失",
				Fixability: "⚠️ 待确认",
				Suggestion: "确认表结构差异，检查列是否被删除或重命名",
			}
		case "1055": // only_full_group_by
			return errorClass{
				Category:   "SQL_MODE 不兼容",
				Fixability: "配置调整",
				Suggestion: "调整 session sql_mode 或重写 GROUP BY 子句",
			}
		case "1290", "1792": // READ ONLY / non-deterministic function
			return errorClass{
				Category:   "只读/非确定性操作",
				Fixability: "配置调整",
				Suggestion: "检查只读设置或开启 tidb_enable_noop_functions",
			}
		case "1406": // Data too long for column
			return errorClass{
				Category:   "数据截断",
				Fixability: "配置调整",
				Suggestion: "调整列长度或修改 sql_mode 放宽截断限制",
			}
		case "1064": // Syntax error
			return errorClass{
				Category:   "SQL 语法错误",
				Fixability: "⚠️ 待确认",
				Suggestion: "检查 SQL 语法兼容性，确认是否为目标库不支持的特性",
			}
		case "1105": // General error (TiDB often uses this for internal errors)
			return errorClass{
				Category:   "通用错误（含 TiDB 特性差异）",
				Fixability: "⚠️ 待确认",
				Suggestion: "检查 TiDB 兼容性文档，确认是否为已知差异",
			}
		case "1142": // Permission denied
			return errorClass{
				Category:   "权限不足",
				Fixability: "配置调整",
				Suggestion: "补充目标库所需权限",
			}
		case "1136": // Column count mismatch
			return errorClass{
				Category:   "列数不匹配",
				Fixability: "⚠️ 待确认",
				Suggestion: "确认 INSERT 语句列数与表结构是否一致",
			}
		}
	}

	// 2) Fallback: keyword matching for errors without a recognised code.
	lower := strings.ToLower(errorInfo)
	switch {
	case strings.Contains(lower, "_tidb_rowid"):
		return errorClass{
			Category:   "_tidb_rowid 聚簇表差异",
			Fixability: "表结构调整",
			Suggestion: "目标表建为非聚簇表或使用 SHARD_ROW_ID_BITS",
		}
	case strings.Contains(lower, "read only"):
		return errorClass{
			Category:   "只读/非确定性操作",
			Fixability: "配置调整",
			Suggestion: "检查只读设置或开启 tidb_enable_noop_functions",
		}
	case strings.Contains(lower, "duplicate") || strings.Contains(lower, "primary key"):
		return errorClass{
			Category:   "主键/唯一键冲突",
			Fixability: "回放机制",
			Suggestion: "使用 INSERT IGNORE 或 REPLACE INTO",
		}
	case strings.Contains(lower, "doesn't exist") || strings.Contains(lower, "does not exist") || strings.Contains(lower, "not exist"):
		return errorClass{
			Category:   "表不存在",
			Fixability: "迁移补充",
			Suggestion: "补充迁移缺失表",
		}
	case strings.Contains(lower, "only_full_group_by"):
		return errorClass{
			Category:   "SQL_MODE 不兼容",
			Fixability: "配置调整",
			Suggestion: "调整 session sql_mode 或重写 GROUP BY 子句",
		}
	case strings.Contains(lower, "data too long") || strings.Contains(lower, "truncated"):
		return errorClass{
			Category:   "数据截断",
			Fixability: "配置调整",
			Suggestion: "调整列长度或修改 sql_mode 放宽截断限制",
		}
	case strings.Contains(lower, "unknown column"):
		return errorClass{
			Category:   "列缺失",
			Fixability: "⚠️ 待确认",
			Suggestion: "确认表结构差异，检查列是否被删除或重命名",
		}
	case strings.Contains(lower, "syntax error"):
		return errorClass{
			Category:   "SQL 语法错误",
			Fixability: "⚠️ 待确认",
			Suggestion: "检查 SQL 语法兼容性",
		}
	default:
		return errorClass{
			Category:   "其他错误",
			Fixability: "⚠️ 待确认",
			Suggestion: "逐例分析，确认是否为兼容性问题",
		}
	}
}

func LoadData(dbConnStr, outDir, replayOut, tableName string) {
	if !validateInputs(dbConnStr, outDir, replayOut, tableName) {
		return
	}

	fmt.Printf("load batchsize: %d, load workers: %d\n", batchSize, workers)

	db, err := sql.Open("mysql", dbConnStr)
	if err != nil {
		fmt.Println("connect to db failed:", err)
		return
	}
	defer db.Close()

	ts_create_table := time.Now()
	fmt.Printf("[%s] Begin create table - REPLAY_INFO\n", ts_create_table.Format("2006-01-02 15:04:05.000"))
	if err := createTableIfNotExists(db, tableName); err != nil {
		fmt.Println("create table failed:", err)
		return
	}

	if err := processFilesParallel(outDir, replayOut, tableName, db); err != nil {
		fmt.Println("process files failed:", err)
	}
}

func createTableIfNotExists(db *sql.DB, tableName string) error {
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			sql_text longtext DEFAULT NULL,
			sql_type varchar(16) DEFAULT NULL,
			sql_digest varchar(64) DEFAULT NULL,
			query_time bigint(20) DEFAULT NULL,
			rows_sent bigint(20) DEFAULT NULL,
			execution_time bigint(20) DEFAULT NULL,
			rows_returned bigint(20) DEFAULT NULL,
			error_info text DEFAULT NULL,
			error_category varchar(64) DEFAULT NULL,
			error_fixability varchar(64) DEFAULT NULL,
			error_suggestion varchar(256) DEFAULT NULL,
			file_name varchar(64) NOT NULL,
			db_name varchar(64) DEFAULT NULL
		)`, tableName)

	_, err := db.Exec(createTableSQL)
	return err
}

func processFilesParallel(outDir, replayName, tableName string, db *sql.DB) error {
	filePaths, err := filepath.Glob(filepath.Join(outDir, replayName+"*"))
	if err != nil {
		return fmt.Errorf("find files failed: %w", err)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(filePaths))
	semaphore := make(chan struct{}, workers)

	for _, filePath := range filePaths {
		wg.Add(1)
		go func(fp string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			fileName := filepath.Base(fp)
			if err := processFile(fp, fileName, tableName, db); err != nil {
				errChan <- fmt.Errorf("process file %s failed: %w", fileName, err)
			} else {
				logCompletion(fileName)
			}
		}(filePath)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func validateInputs(dbConnStr, outDir, replayOut, tableName string) bool {
	if dbConnStr == "" || outDir == "" || replayOut == "" || tableName == "" {
		fmt.Println("Usage: ./sql-replay -mode load -db <DB_CONN_STRING> -out-dir <DIRECTORY> -replay-name <REPORT_OUT_FILE_NAME> -table <replay_info>")
		return false
	}
	return true
}

func processFiles(outDir, replayName, tableName string, db *sql.DB) error {
	filePaths, err := filepath.Glob(filepath.Join(outDir, replayName+"*"))
	if err != nil {
		return fmt.Errorf("error finding files: %w", err)
	}

	for _, filePath := range filePaths {
		fileName := filepath.Base(filePath)
		if err := processFile(filePath, fileName, tableName, db); err != nil {
			return fmt.Errorf("error processing file %s: %w", fileName, err)
		} else {
			logCompletion(fileName)
		}
	}

	return nil
}

func processFile(filePath, fileName, tableName string, db *sql.DB) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // 64 KB initial, 16 MB max per line

	var batch []string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		batch = append(batch, line)
		if len(batch) >= batchSize {
			if err := insertBatch(batch, fileName, tableName, db); err != nil {
				return fmt.Errorf("error inserting batch: %w", err)
			}
			batch = batch[:0]
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read error: %w", err)
	}

	// flush remaining
	if len(batch) > 0 {
		if err := insertBatch(batch, fileName, tableName, db); err != nil {
			return fmt.Errorf("error inserting batch: %w", err)
		}
	}
	return nil
}

func insertBatch(lines []string, fileName, tableName string, db *sql.DB) error {
	records := parseRecords(lines)
	if len(records) == 0 {
		return nil
	}

	query, args := buildInsertQuery(records, fileName, tableName)
	_, err := db.Exec(query, args...)
	return err
}

func parseRecords(lines []string) []SQLExecutionRecord {
	var records []SQLExecutionRecord
	for _, line := range lines {
		if line == "" {
			continue
		}
		var record SQLExecutionRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			fmt.Printf("Error parsing JSON: %v\n", err)
			continue
		}
		records = append(records, record)
	}
	return records
}

func buildInsertQuery(records []SQLExecutionRecord, fileName, tableName string) (string, []interface{}) {
	valueStrings := make([]string, 0, len(records))
	valueArgs := make([]interface{}, 0, len(records)*13)

	for _, record := range records {
		normalizedSQL := parser.Normalize(record.SQL)
		digest := parser.DigestNormalized(normalizedSQL).String()
		sqlType := getSQLType(normalizedSQL)
		cls := classifyError(record.ErrorInfo)

		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs,
			record.SQL, sqlType, digest,
			record.QueryTime, record.RowsSent,
			record.ExecutionTime, record.RowsReturned,
			record.ErrorInfo,
			cls.Category, cls.Fixability, cls.Suggestion,
			fileName, record.DBName,
		)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (sql_text, sql_type, sql_digest, query_time, rows_sent, execution_time, rows_returned, error_info, error_category, error_fixability, error_suggestion, file_name, db_name) VALUES %s",
		tableName, strings.Join(valueStrings, ","),
	)
	return query, valueArgs
}

func getSQLType(normalizedSQL string) string {
	words := strings.Fields(normalizedSQL)
	if len(words) > 0 {
		return strings.ToLower(words[0])
	}
	return "other"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func logCompletion(fileName string) {
	currentTime := time.Now().Format("2006-01-02 15:04:05.000")
	fmt.Printf("[%s] Completed processing file: %s\n", currentTime, fileName)
}
