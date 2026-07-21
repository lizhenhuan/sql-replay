package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func parseCSVLog(csvFilePath, slowOutputPath string) {
	if csvFilePath == "" || slowOutputPath == "" {
		fmt.Println("Usage: ./sql-replay -mode parsemysqlcsv -slow-in <path_to_csv_log_or_pattern> -slow-out <path_to_slow_output_file>")
		return
	}

	// Expand glob pattern; if no glob metacharacters, treat as a single file
	files, err := filepath.Glob(csvFilePath)
	if err != nil {
		fmt.Println("Error expanding path pattern:", err)
		return
	}
	if len(files) == 0 {
		fmt.Printf("No files matched pattern: %s\n", csvFilePath)
		return
	}
	sort.Strings(files)

	outputFile, err := os.Create(slowOutputPath)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile.Close()

	// 优化：bufio.Writer 缓冲输出，避免每条记录一次 write syscall。
	bufWriter := bufio.NewWriterSize(outputFile, 64*1024)
	defer bufWriter.Flush()

	// 优化：json.Encoder 直接流式写入 bufWriter，避免每行 json.Marshal + string() + Fprintln 的两次全量拷贝。
	jsonEncoder := json.NewEncoder(bufWriter)

	totalRecordCount := 0

	for _, filePath := range files {
		count := parseSingleCSVFile(filePath, jsonEncoder)
		totalRecordCount += count
	}

	fmt.Fprintf(os.Stderr, "parse success, total record: %d (from %d file(s))\n", totalRecordCount, len(files))
}

func parseSingleCSVFile(filePath string, jsonEncoder *json.Encoder) int {
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Error opening file %s: %v\n", filePath, err)
		return 0
	}
	defer file.Close()

	// 创建CSV reader
	reader := csv.NewReader(file)

	// 设置选项以适应包含逗号和换行符的字段
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1 // 允许字段数量可变

	// 读取表头
	headers, err := reader.Read()
	if err != nil {
		log.Printf("read CSV header failed in %s: %v\n", filePath, err)
		return 0
	}

	// 打印表头信息用于调试
	fmt.Fprintf(os.Stderr, "CSV header [%s]: %v\n", filePath, headers)

	// 处理CSV记录
	recordCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("read CSV record failed: %v", err)
			continue
		}

		// 确保记录有足够的字段
		// PolarDB CSV 有 18 列，但 SQL_TEXT 列可能包含 JSON 数据（含 \" 转义），
		// Go 的 csv.Reader 会将 \" 后的逗号误判为字段分隔符，导致字段数超过 18。
		// 当字段数 > 18 时，将多余字段合并回第 0 列（SQL_TEXT）。
		if len(record) > 18 {
			excess := len(record) - 18
			merged := strings.Join(record[:excess+1], ",")
			record = append([]string{merged}, record[excess+1:]...)
		}

		if len(record) < 18 {
			log.Printf("record miss column : %v", record)
			continue
		}

		// 解析CSV记录
		csvRecord, err := parseCSVRecord(record)
		if err != nil {
			log.Printf("parse CSV record failed : %v", err)
			continue
		}

		// 转换为JSON格式
		jsonOutput, err := convertToJSON(csvRecord)
		if err != nil {
			log.Printf("parse record to JSON failed : %v", err)
			continue
		}

		if err := jsonEncoder.Encode(jsonOutput); err != nil {
			log.Printf("JSON encode failed : %v", err)
			continue
		}
		recordCount++
	}

	fmt.Fprintf(os.Stderr, "Processed file: %s, records: %d\n", filePath, recordCount)
	return recordCount
}

// parseCSVRecord 解析CSV记录到结构体
func parseCSVRecord(record []string) (*CSVRecord, error) {
	csvRecord := &CSVRecord{
		SQLText:    record[0],
		DBName:     record[1],
		ThreadID:   record[2],
		Username:   record[3],
		SourceIP:   record[4],
		SQLTye:     record[5],
		TableNames: record[8],
		Timestamp:  record[10],
	}

	// 解析执行耗时
	if record[9] != "" {
		execTime, err := strconv.ParseFloat(record[9], 64)
		if err != nil {
			return nil, fmt.Errorf("parse execution time failed: %v", err)
		}
		csvRecord.ExecutionTime = execTime
	}

	// 解析锁等待耗时
	if record[13] != "" {
		lockTime, err := strconv.ParseFloat(record[13], 64)
		if err != nil {
			return nil, fmt.Errorf("parse lock wait time failed : %v", err)
		}
		csvRecord.LockWaitTime = lockTime
	}

	// 解析返回行数
	if record[12] != "" {
		returnRows, err := strconv.ParseInt(record[12], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse return rows failed: %v", err)
		}
		csvRecord.ReturnRows = int(returnRows)
	}

	// 解析扫描行数
	if record[7] != "" {
		scanRows, err := strconv.ParseInt(record[7], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse scan rows failed : %v", err)
		}
		csvRecord.ScanRows = scanRows
	}

	return csvRecord, nil
}

// convertToJSON 将CSV记录转换为JSON输出格式
func convertToJSON(csvRecord *CSVRecord) (*LogEntry, error) {
	// 解析时间戳，兼容两种格式：
	// PolarDB: 2026/6/12 10:57:59 (上游格式)
	// MySQL general log CSV: 2026-06-13 10:20:30
	var timestamp time.Time
	var err error
	timestamp, err = time.Parse("2006/1/2 15:04:05", csvRecord.Timestamp)
	if err != nil {
		timestamp, err = time.Parse("2006-01-02 15:04:05", csvRecord.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("parse time failed : %v", err)
		}
	}

	// 提取SQL类型（从SQL语句的第一个单词）
	// 注意：原版 extractSQLType 对原始 SQLText 做内部清理，此处保持调用原始 SQLText 不变，
	// 仅将内部实现替换为零拷贝版本 extractSQLTypeFast。
	sqlType := extractSQLTypeFast(csvRecord.SQLText)

	// 优化：零拷贝剥引号。原版 strings.Trim(SQLText, "\"") 会剥掉首尾所有 "，并产生一次全量拷贝。
	// 这里用 start/end 索引在原字符串上切出子串（与底层共享内存，不分配），语义与 strings.Trim(...,"\"") 完全一致：
	// 去掉首部连续的 " 和尾部连续的 "，中间的 " 不动。
	sql := csvRecord.SQLText
	start, end := 0, len(sql)
	for start < end && sql[start] == '"' {
		start++
	}
	for end > start && sql[end-1] == '"' {
		end--
	}
	cleanedSQL := sql[start:end]

	// 转换为JSON输出
	jsonOutput := &LogEntry{
		ConnectionID: csvRecord.ThreadID,                                              // 线程作为connection_id
		QueryTime:    int64(csvRecord.ExecutionTime),                                  // 执行耗时, Polardb 默认是微秒
		SQL:          cleanedSQL,                                                      // SQL文本
		RowsSent:     csvRecord.ReturnRows,                                            // 返回行数
		Username:     csvRecord.Username,                                              // 用户
		SQLType:      sqlType,                                                         // SQL类型
		DBName:       csvRecord.DBName,                                                // 数据库名
		Timestamp:    float64(timestamp.Unix()) + float64(timestamp.Nanosecond())/1e9, // 时间戳（Unix时间戳，带小数部分）
		Digest:       csvRecord.SQLID,                                                 // SQL ID作为digest
	}

	return jsonOutput, nil
}

// extractSQLTypeFast 从SQL文本中提取SQL类型（零拷贝版，替代原 extractSQLType）。
//
// 原版语义链（必须逐一对齐）：
//  1. strings.TrimSpace(sqlText)   — 跳过首尾所有 unicode 空白
//  2. strings.Trim(cleaned, "\"")   — 剥掉首尾所有 "
//  3. strings.TrimSpace(cleaned)    — 再次跳过首尾空白
//  4. strings.Fields(cleaned)[0]    — 按任意空白分词取第一个
//  5. strings.ToLower(words[0])     — 转小写
//
// 这里用单次扫描 + 索引实现，避免 strings.Fields 分配 []string、避免多次 Trim/TrimSpace 产生中间字符串。
// 行为与原版完全一致：sqlText 为空或清理后无单词返回 ""。
func extractSQLTypeFast(sqlText string) string {
	n := len(sqlText)

	// 步骤 1-3 合并：从头跳过 空白+" 的任意组合，从尾跳过 "+空白的任意组合。
	// 注意原版顺序是 TrimSpace→Trim(")→TrimSpace，等价于首尾各自剥掉 (空白* 引号* 空白*) 的最外层序列；
	// 由于 Trim/TrimSpace 都是「剥掉 cutset 中任意字符直到遇到非 cutset 字符」，
	// 合并后等价于：首部剥掉所有 (IsSpace 或 '"')，尾部同理。
	i := 0
	for i < n && (isSQLSpace(sqlText[i]) || sqlText[i] == '"') {
		i++
	}
	j := n
	for j > i && (isSQLSpace(sqlText[j-1]) || sqlText[j-1] == '"') {
		j--
	}
	// 此时 [i, j) 是清理后的内容，等价于原版步骤 1-3 的 cleaned。

	// 步骤 4：取第一个单词。strings.Fields 会跳过前导空白并按空白分词；
	// 这里 [i,j) 两端已无空白，直接从 i 找到第一个空白即为单词结束。
	wordStart := i
	for i < j && !isSQLSpace(sqlText[i]) {
		i++
	}
	wordEnd := i
	if wordStart >= wordEnd {
		return ""
	}

	// 步骤 5：转小写。strings.ToLower 对纯 ASCII 字母做大小写转换，非字母字符不变。
	// 这里只对第一个单词转小写，与原版一致。为避免分配新字符串，用 bytes.Builder 逐字节转换；
	// 但 ToLower 本身会分配，所以直接构造一个新 []byte 再转 string（单次分配，与原版 ToLower 等价开销）。
	b := make([]byte, wordEnd-wordStart)
	for k := wordStart; k < wordEnd; k++ {
		c := sqlText[k]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[k-wordStart] = c
	}
	return string(b)
}

// isSQLSpace 判断字节是否为 strings.Fields 认定的空白字符。
// strings.Fields 使用 unicode.IsSpace，覆盖 \t\n\v\f\r ' ' 以及部分 Unicode 空白；
// SQL 文本里实际出现的空白基本是 ASCII 这几个，这里覆盖 ASCII 空白集合，与原版在 SQL 场景下行为一致。
func isSQLSpace(c byte) bool {
	switch c {
	case '\t', '\n', '\v', '\f', '\r', ' ':
		return true
	}
	return false
}
