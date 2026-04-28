package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
)

type tencentAuditCSVRecord struct {
	RowNumber int
	Timestamp string
	NsTime    string
	SQL       string
	ThreadID  string
	ExecTime  string
	Username  string
	DBName    string
	SQLType   string
	SentRows  string
}

type sortedTencentAuditEntry struct {
	Entry     LogEntry
	RowNumber int
}

func ParseTencentAuditCSV(csvFilePath, slowOutputPath string) {
	if csvFilePath == "" || slowOutputPath == "" {
		fmt.Println("Usage: ./sql-replay -mode parsetencentaudit -slow-in <path_to_audit_csv> -slow-out <path_to_slow_output_file>")
		return
	}

	file, err := os.Open(csvFilePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
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

	headers, err := reader.Read()
	if err != nil {
		fmt.Println("Error reading CSV header:", err)
		return
	}

	fieldIndex, err := buildTencentAuditFieldIndex(headers)
	if err != nil {
		fmt.Println("Error parsing CSV header:", err)
		return
	}

	var entries []sortedTencentAuditEntry
	totalRows := 0
	skippedRows := 0

	for rowNumber := 2; ; rowNumber++ {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Skip row %d: read CSV record failed: %v\n", rowNumber, err)
			skippedRows++
			continue
		}

		totalRows++
		rawRecord := buildTencentAuditRecord(record, fieldIndex, rowNumber)
		if shouldSkipTencentAuditRecord(rawRecord) {
			skippedRows++
			continue
		}

		entry, err := convertTencentAuditToLogEntry(rawRecord)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Skip row %d: %v\n", rowNumber, err)
			skippedRows++
			continue
		}

		entries = append(entries, sortedTencentAuditEntry{
			Entry:     *entry,
			RowNumber: rowNumber,
		})
	}

	reverseTencentAuditEntriesIfDescending(entries)

	for _, item := range entries {
		jsonEntry, err := json.Marshal(item.Entry)
		if err != nil {
			fmt.Println("Error marshaling JSON:", err)
			return
		}
		fmt.Fprintln(outputFile, string(jsonEntry))
	}

	fmt.Fprintf(os.Stderr, "Tencent audit CSV parsed. Total rows: %d, Written: %d, Skipped: %d\n", totalRows, len(entries), skippedRows)
}

func reverseTencentAuditEntriesIfDescending(entries []sortedTencentAuditEntry) {
	if len(entries) < 2 {
		return
	}

	ascending := true
	descending := true
	for i := 1; i < len(entries); i++ {
		prevTS := entries[i-1].Entry.Timestamp
		currTS := entries[i].Entry.Timestamp
		if prevTS < currTS {
			descending = false
		}
		if prevTS > currTS {
			ascending = false
		}
		if !ascending && !descending {
			return
		}
	}

	if descending && !ascending {
		for left, right := 0, len(entries)-1; left < right; left, right = left+1, right-1 {
			entries[left], entries[right] = entries[right], entries[left]
		}
	}
}

func buildTencentAuditFieldIndex(headers []string) (map[string]int, error) {
	index := make(map[string]int, len(headers))
	for i, header := range headers {
		normalized := normalizeTencentAuditHeader(header)
		if normalized == "" {
			continue
		}
		index[normalized] = i
	}

	requiredHeaders := []string{"timestamp", "sql", "threadid", "exectime"}
	var missing []string
	for _, header := range requiredHeaders {
		if _, ok := index[header]; !ok {
			missing = append(missing, header)
		}
	}

	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required CSV headers: %s", strings.Join(missing, ", "))
	}

	return index, nil
}

func normalizeTencentAuditHeader(header string) string {
	header = strings.TrimPrefix(header, "\uFEFF")
	header = strings.TrimSpace(header)
	header = strings.ToLower(header)
	return header
}

func buildTencentAuditRecord(record []string, fieldIndex map[string]int, rowNumber int) *tencentAuditCSVRecord {
	getField := func(name string) string {
		i, ok := fieldIndex[name]
		if !ok || i >= len(record) {
			return ""
		}
		return strings.TrimSpace(record[i])
	}

	return &tencentAuditCSVRecord{
		RowNumber: rowNumber,
		Timestamp: getField("timestamp"),
		NsTime:    getField("nstime"),
		SQL:       getField("sql"),
		ThreadID:  getField("threadid"),
		ExecTime:  getField("exectime"),
		Username:  getField("user"),
		DBName:    getField("dbname"),
		SQLType:   getField("sqltype"),
		SentRows:  getField("sentrows"),
	}
}

func shouldSkipTencentAuditRecord(record *tencentAuditCSVRecord) bool {
	sqlType := strings.ToLower(strings.TrimSpace(record.SQLType))
	switch sqlType {
	case "login", "logout", "changeuser":
		return true
	}

	sqlText := strings.TrimSpace(record.SQL)
	if sqlText == "" {
		return true
	}
	if strings.EqualFold(sqlText, "/* SP */") {
		return true
	}

	return false
}

func convertTencentAuditToLogEntry(record *tencentAuditCSVRecord) (*LogEntry, error) {
	threadID := strings.TrimSpace(record.ThreadID)
	if threadID == "" {
		return nil, fmt.Errorf("missing threadid")
	}

	queryTime, err := parseTencentAuditInt64(record.ExecTime)
	if err != nil {
		return nil, fmt.Errorf("parse exectime failed: %w", err)
	}

	rowsSent := 0
	if strings.TrimSpace(record.SentRows) != "" {
		rowsSent, err = strconv.Atoi(strings.TrimSpace(record.SentRows))
		if err != nil {
			return nil, fmt.Errorf("parse sentrows failed: %w", err)
		}
	}

	timestamp, err := parseTencentAuditTimestamp(record.Timestamp, record.NsTime)
	if err != nil {
		return nil, err
	}

	cleanedSQL := cleanTencentAuditSQL(record.SQL)
	if cleanedSQL == "" {
		return nil, fmt.Errorf("empty sql")
	}

	normalizedSQL := parser.Normalize(cleanedSQL)
	digest := parser.DigestNormalized(normalizedSQL).String()

	return &LogEntry{
		ConnectionID: threadID,
		QueryTime:    queryTime,
		SQL:          cleanedSQL,
		RowsSent:     rowsSent,
		Username:     strings.TrimSpace(record.Username),
		SQLType:      normalizeTencentAuditSQLType(record.SQLType, cleanedSQL),
		DBName:       strings.TrimSpace(record.DBName),
		Timestamp:    timestamp,
		Digest:       digest,
	}, nil
}

func parseTencentAuditInt64(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("value is empty")
	}

	if parsedInt, err := strconv.ParseInt(value, 10, 64); err == nil {
		return parsedInt, nil
	}

	parsedFloat, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, err
	}
	return int64(parsedFloat), nil
}

func parseTencentAuditTimestamp(timestampValue, nsValue string) (float64, error) {
	timestampValue = strings.TrimSpace(timestampValue)
	if timestampValue == "" {
		return 0, fmt.Errorf("missing timestamp")
	}

	if unixSeconds, err := strconv.ParseInt(timestampValue, 10, 64); err == nil {
		parsedTime := time.Unix(unixSeconds, 0)
		if nsOffset, nsErr := strconv.ParseInt(strings.TrimSpace(nsValue), 10, 64); nsErr == nil && nsOffset >= 0 && nsOffset < int64(time.Second) {
			parsedTime = parsedTime.Add(time.Duration(nsOffset))
		}
		return float64(parsedTime.UnixNano()) / 1e9, nil
	}

	layoutsWithoutFraction := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}

	parsedTime, err := parseTencentAuditTimeWithLayouts(timestampValue, layoutsWithoutFraction)
	if err == nil {
		if nsOffset, nsErr := strconv.ParseInt(strings.TrimSpace(nsValue), 10, 64); nsErr == nil && nsOffset >= 0 && nsOffset < int64(time.Second) {
			parsedTime = parsedTime.Add(time.Duration(nsOffset))
		}
	}
	if err != nil {
		return 0, fmt.Errorf("parse timestamp failed: %w", err)
	}

	return float64(parsedTime.UnixNano()) / 1e9, nil
}

func parseTencentAuditTimeWithLayouts(value string, layouts []string) (time.Time, error) {
	var lastErr error
	for _, layout := range layouts {
		parsedTime, err := time.ParseInLocation(layout, value, time.Local)
		if err == nil {
			return parsedTime, nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}

func cleanTencentAuditSQL(sqlText string) string {
	cleaned := strings.TrimSpace(sqlText)
	cleaned = strings.ReplaceAll(cleaned, "\r\n", " ")
	cleaned = strings.ReplaceAll(cleaned, "\n", " ")
	cleaned = strings.ReplaceAll(cleaned, "\r", " ")
	return cleaned
}

func normalizeTencentAuditSQLType(sqlType, sqlText string) string {
	sqlType = strings.ToLower(strings.TrimSpace(sqlType))
	if sqlType != "" {
		return sqlType
	}

	words := strings.Fields(sqlText)
	if len(words) == 0 {
		return "other"
	}
	return strings.ToLower(words[0])
}
