package main

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
)

// XEventRow 对应客户导出的 CSV 格式
// 列: UUID, UUID, event_name, event_data (XML), xel_path, offset, timestamp
type XEventRow struct {
	EventName  string // sql_statement_completed, rpc_completed 等
	EventData  string // XML 格式的事件数据
	Timestamp  string // 时间戳
}

// XEventData 解析 XML event_data
type XEventData struct {
	XMLName   xml.Name `xml:"event"`
	Timestamp string   `xml:"timestamp,attr"`
	Data      []struct {
		Name  string `xml:"name,attr"`
		Value string `xml:"value"`
	} `xml:"data"`
	Action []struct {
		Name  string `xml:"name,attr"`
		Value string `xml:"value"`
	} `xml:"action"`
}

// ParseSQLServerXEvents 解析 SQL Server Extended Events 导出的 CSV 文件
// CSV 格式来自: SELECT object_name, event_data FROM sys.fn_xe_file_target_read_file(...)
func ParseSQLServerXEvents(csvFilePath, slowOutputPath string) {
	if csvFilePath == "" || slowOutputPath == "" {
		fmt.Println("Usage: ./sql-replay -mode parsesqlserver -slow-in <path_to_xevent_csv> -slow-out <path_to_slow_output_file>")
		return
	}

	file, err := os.Open(csvFilePath)
	if err != nil {
		log.Fatal("Error opening CSV file:", err)
	}
	defer file.Close()

	outputFile, err := os.Create(slowOutputPath)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile.Close()

	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	recordCount := 0
	skippedCount := 0
	lineNum := 0

	for {
		lineNum++
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV line %d: %v", lineNum, err)
			continue
		}

		// 跳过表头或格式不符的行
		if len(record) < 4 {
			skippedCount++
			continue
		}

		// 第一行可能是客户自己的查询语句（SELECT...），跳过
		if lineNum == 1 && strings.Contains(record[0], "SELECT") {
			continue
		}

		// 解析行数据
		// 格式: UUID, UUID, event_name, event_data_xml, xel_path, offset, timestamp
		var eventName, eventData, timestamp string
		
		// 根据列数确定格式
		if len(record) >= 7 {
			eventName = record[2]
			eventData = record[3]
			timestamp = record[6]
		} else if len(record) >= 4 {
			// 简化格式：event_name, event_data
			eventName = record[0]
			eventData = record[1]
			if len(record) >= 3 {
				timestamp = record[2]
			}
		} else {
			skippedCount++
			continue
		}

		// 只处理 sql_statement_completed 和 rpc_completed
		if eventName != "sql_statement_completed" && eventName != "rpc_completed" {
			skippedCount++
			continue
		}

		// 解析 XML
		logEntry, err := parseXEventData(eventName, eventData, timestamp)
		if err != nil {
			log.Printf("Error parsing XML at line %d: %v", lineNum, err)
			skippedCount++
			continue
		}

		// 跳过空 SQL
		if strings.TrimSpace(logEntry.SQL) == "" {
			skippedCount++
			continue
		}

		// 输出 JSON
		jsonData, err := json.Marshal(logEntry)
		if err != nil {
			log.Printf("Error marshaling JSON at line %d: %v", lineNum, err)
			skippedCount++
			continue
		}

		fmt.Fprintln(outputFile, string(jsonData))
		recordCount++
	}

	fmt.Fprintf(os.Stderr, "Parse completed. Total records: %d, Skipped: %d\n", recordCount, skippedCount)
}

// parseXEventData 解析 XML 格式的 event_data
func parseXEventData(eventName, eventDataXML, timestamp string) (*LogEntry, error) {
	var xevent XEventData
	if err := xml.Unmarshal([]byte(eventDataXML), &xevent); err != nil {
		return nil, fmt.Errorf("XML parse error: %w", err)
	}

	entry := &LogEntry{}

	// 解析 timestamp (从 XML)
	if xevent.Timestamp != "" {
		if t, err := time.Parse(time.RFC3339Nano, xevent.Timestamp); err == nil {
			entry.Timestamp = float64(t.Unix()) + float64(t.Nanosecond())/1e9
		}
	}

	// 解析 data 字段
	for _, d := range xevent.Data {
		switch d.Name {
		case "duration":
			if val, err := strconv.ParseInt(d.Value, 10, 64); err == nil {
				// SQL Server Extended Events duration 单位是微秒
				entry.QueryTime = val
			}
		case "cpu_time":
			// cpu_time 也是微秒
		case "logical_reads":
			// 逻辑读
		case "row_count":
			if val, err := strconv.Atoi(d.Value); err == nil {
				entry.RowsSent = val
			}
		case "statement":
			entry.SQL = cleanSQLText(d.Value)
		}
	}

	// 解析 action 字段
	for _, a := range xevent.Action {
		switch a.Name {
		case "username":
			entry.Username = a.Value
		case "session_id":
			entry.ConnectionID = a.Value
		case "database_name":
			entry.DBName = a.Value
		}
	}

	// 解析传入的 timestamp
	if timestamp != "" && entry.Timestamp == 0 {
		// 尝试多种时间格式
		formats := []string{
			"2006-01-02 15:04:05.0000000",
			"2006-01-02 15:04:05.000000",
			"2006-01-02 15:04:05.000",
			"2006-01-02 15:04:05",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, timestamp); err == nil {
				entry.Timestamp = float64(t.Unix()) + float64(t.Nanosecond())/1e9
				break
			}
		}
	}

	// 提取 SQL 类型
	entry.SQLType = extractSQLServerType(entry.SQL)

	// 生成 digest
	if entry.SQL != "" {
		normalized := parser.Normalize(entry.SQL)
		digest := parser.DigestNormalized(normalized)
		entry.Digest = digest.String()
	}

	return entry, nil
}

// cleanSQLText 清理 SQL 文本并转换 SQL Server 语法到 MySQL/TiDB 兼容格式
func cleanSQLText(sql string) string {
	// 移除多余的空白
	cleaned := strings.TrimSpace(sql)
	
	// 替换换行符
	cleaned = strings.ReplaceAll(cleaned, "\r\n", " ")
	cleaned = strings.ReplaceAll(cleaned, "\n", " ")
	
	// 转换 SQL Server 标识符引用 [name] -> `name`
	cleaned = convertSQLServerIdentifiers(cleaned)
	
	// 压缩多余空格
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

// convertSQLServerIdentifiers 将 SQL Server 标识符语法转换为 MySQL/TiDB 兼容格式
// [column_name] -> `column_name`
// [schema].[table] -> `schema`.`table`
// WITH(NOLOCK) -> (移除)
func convertSQLServerIdentifiers(sql string) string {
	var result strings.Builder
	i := 0
	
	for i < len(sql) {
		// 检查是否是标识符开始 [
		if sql[i] == '[' {
			// 找到匹配的 ]
			end := strings.Index(sql[i+1:], "]")
			if end >= 0 {
				// 提取标识符名称
				identifier := sql[i+1 : i+1+end]
				// 写入 MySQL 格式 `identifier`
				result.WriteByte('`')
				result.WriteString(identifier)
				result.WriteByte('`')
				i = i + 1 + end + 1 // 跳过 [identifier]
				continue
			}
		}
		result.WriteByte(sql[i])
		i++
	}
	
	// 移除 WITH(NOLOCK) / WITH (NOLOCK) 表提示
	converted := result.String()
	converted = removeWithHints(converted)
	
	return converted
}

// removeWithHints 移除 SQL Server 的 WITH(...) 表提示
func removeWithHints(sql string) string {
	// 匹配 WITH(...) 表提示，包括 NOLOCK, INDEX, TABLOCK 等
	// 模式: WITH(NOLOCK), WITH (NOLOCK), WITH(NOLOCK, INDEX=xxx), WITH(TABLOCK)
	
	// 简单实现：逐个查找 WITH( 并移除到 )
	result := strings.Builder{}
	i := 0
	upper := strings.ToUpper(sql)
	
	for i < len(sql) {
		// 检查是否是 WITH(
		if i+4 < len(sql) && upper[i:i+4] == "WITH" {
			// 找 WITH 后面的 (
			j := i + 4
			for j < len(sql) && (sql[j] == ' ' || sql[j] == '\t') {
				j++
			}
			if j < len(sql) && sql[j] == '(' {
				// 找到匹配的 )
				depth := 1
				k := j + 1
				for k < len(sql) && depth > 0 {
					if sql[k] == '(' {
						depth++
					} else if sql[k] == ')' {
						depth--
					}
					k++
				}
				if depth == 0 {
					// 跳过 WITH(...)
					i = k
					continue
				}
			}
		}
		result.WriteByte(sql[i])
		i++
	}
	
	return result.String()
}

// extractSQLServerType 从 SQL Server SQL 文本中提取 SQL 类型
func extractSQLServerType(sql string) string {
	if sql == "" {
		return "other"
	}

	cleaned := strings.TrimSpace(sql)
	upper := strings.ToUpper(cleaned)

	// 移除开头的 exec sp_executesql 或 exec sp_prepexec
	if strings.HasPrefix(upper, "EXEC SP_EXECUTESQL") || strings.HasPrefix(upper, "EXEC SP_PREPEXEC") {
		// 找到 N' 后面的 SQL
		idx := strings.Index(upper, "N'")
		if idx >= 0 {
			remaining := upper[idx+2:]
			// 找到下一个单引号
			for i := 0; i < len(remaining); i++ {
				if remaining[i] == '\'' {
					upper = strings.TrimSpace(remaining[:i])
					break
				}
			}
		}
	}

	// 处理 exec proc_xxx 的情况
	if strings.HasPrefix(upper, "EXEC ") || strings.HasPrefix(upper, "EXECUTE ") {
		return "exec"
	}

	// 常见 SQL 类型
	switch {
	case strings.HasPrefix(upper, "SELECT"):
		return "select"
	case strings.HasPrefix(upper, "INSERT"):
		return "insert"
	case strings.HasPrefix(upper, "UPDATE"):
		return "update"
	case strings.HasPrefix(upper, "DELETE"):
		return "delete"
	case strings.HasPrefix(upper, "CALL"):
		return "call"
	case strings.HasPrefix(upper, "WITH"):
		return "select" // CTE
	default:
		return "other"
	}
}