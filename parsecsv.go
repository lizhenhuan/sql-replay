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
)

func parseCSVLog(csvFilePath, slowOutputPath string) {
	if csvFilePath == "" || slowOutputPath == "" {
		fmt.Println("Usage: ./sql-replay -mode parsemysqlcsv -slow-in <path_to_csv_log> -slow-out <path_to_slow_output_file>")
		return
	}

	// 打开CSV文件
	file, err := os.Open(csvFilePath)
	if err != nil {
		log.Fatal("Error creating output file:", err)
	}
	defer file.Close()

	outputFile, err := os.Create(slowOutputPath)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile.Close()

	// 创建CSV reader
	reader := csv.NewReader(file)

	// 设置选项以适应包含逗号和换行符的字段
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1 // 允许字段数量可变

	// 读取表头
	headers, err := reader.Read()
	if err != nil {
		log.Fatal("read CSV header failed:", err)
	}

	// 打印表头信息用于调试
	fmt.Fprintf(os.Stderr, "CSV header: %v\n", headers)

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
		if len(record) < 13 {
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

		// 输出JSON
		jsonData, err := json.Marshal(jsonOutput)
		if err != nil {
			log.Printf("JSON marshal failed : %v", err)
			continue
		}

		fmt.Fprintln(outputFile, string(jsonData))
		recordCount++
	}

	fmt.Fprintf(os.Stderr, "parse success， total record: %d \n", recordCount)
}

// parseCSVRecord 解析CSV记录到结构体
func parseCSVRecord(record []string) (*CSVRecord, error) {
	csvRecord := &CSVRecord{
		Timestamp:  record[0],
		SQLID:      record[1],
		SQLText:    record[2],
		DBName:     record[3],
		SourceIP:   record[8],
		Username:   record[9],
		ThreadID:   record[10],
		TableNames: record[11],
		Tags:       record[12],
	}

	// 解析执行耗时
	if record[4] != "" {
		execTime, err := strconv.ParseFloat(record[4], 64)
		if err != nil {
			return nil, fmt.Errorf("parse execution time failed: %v", err)
		}
		csvRecord.ExecutionTime = execTime
	}

	// 解析锁等待耗时
	if record[5] != "" {
		lockTime, err := strconv.ParseFloat(record[5], 64)
		if err != nil {
			return nil, fmt.Errorf("parse lock wait time failed : %v", err)
		}
		csvRecord.LockWaitTime = lockTime
	}

	// 解析返回行数
	if record[6] != "" {
		returnRows, err := strconv.ParseInt(record[6], 10, 64)
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
	// 解析时间戳
	timestamp, err := time.Parse("2006-01-02 15:04:05", csvRecord.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("parse time failed : %v", err)
	}

	// 提取SQL类型（从SQL语句的第一个单词）
	sqlType := extractSQLType(csvRecord.SQLText)

	// 清理SQL文本（移除多余的引号）
	cleanedSQL := strings.Trim(csvRecord.SQLText, "\"")

	// 转换为JSON输出
	jsonOutput := &LogEntry{
		ConnectionID: csvRecord.ThreadID,                                              // 线程作为connection_id
		QueryTime:    int64(csvRecord.ExecutionTime * 1000000),                        // 执行耗时,Convert seconds to microseconds
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

// extractSQLType 从SQL文本中提取SQL类型
func extractSQLType(sqlText string) string {
	if sqlText == "" {
		return ""
	}

	// 清理SQL文本
	cleaned := strings.TrimSpace(sqlText)
	cleaned = strings.Trim(cleaned, "\"")
	cleaned = strings.TrimSpace(cleaned)

	// 获取第一个单词（SQL类型）
	words := strings.Fields(cleaned)
	if len(words) == 0 {
		return ""
	}

	// 转换为小写并返回
	return strings.ToLower(words[0])
}
