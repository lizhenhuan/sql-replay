package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
)

type aliAuditCSVRecord struct {
	RowNumber  int
	SQL        string
	DBName     string
	ThreadID   string
	Username   string
	SQLType    string
	Fail       string
	ReturnRows string
	Latency    string
	OriginTime string
}

type sortedAliAuditEntry struct {
	Entry     LogEntry
	RowNumber int
}

func ParseAliAuditCSV(csvFilePath, slowOutputPath string) {
	if csvFilePath == "" || slowOutputPath == "" {
		fmt.Println("Usage: ./sql-replay -mode parsealiaudit -slow-in <path_to_audit_csv> -slow-out <path_to_slow_output_file>")
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

	fieldIndex, err := buildAliAuditFieldIndex(headers)
	if err != nil {
		fmt.Println("Error parsing CSV header:", err)
		return
	}

	var entries []sortedAliAuditEntry
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
		rawRecord := buildAliAuditRecord(record, fieldIndex, rowNumber)
		if shouldSkipAliAuditRecord(rawRecord) {
			skippedRows++
			continue
		}

		entry, err := convertAliAuditToLogEntry(rawRecord)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Skip row %d: %v\n", rowNumber, err)
			skippedRows++
			continue
		}

		entries = append(entries, sortedAliAuditEntry{
			Entry:     *entry,
			RowNumber: rowNumber,
		})
	}

	sortAliAuditEntries(entries)

	for _, item := range entries {
		jsonEntry, err := json.Marshal(item.Entry)
		if err != nil {
			fmt.Println("Error marshaling JSON:", err)
			return
		}
		fmt.Fprintln(outputFile, string(jsonEntry))
	}

	fmt.Fprintf(os.Stderr, "Alibaba Cloud audit CSV parsed. Total rows: %d, Written: %d, Skipped: %d\n", totalRows, len(entries), skippedRows)
}

func buildAliAuditFieldIndex(headers []string) (map[string]int, error) {
	index := make(map[string]int, len(headers))
	for i, header := range headers {
		normalized := normalizeAliAuditHeader(header)
		if normalized == "" {
			continue
		}
		index[normalized] = i
	}

	requiredHeaders := []string{"log", "tid", "latency", "origin_time"}
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

func normalizeAliAuditHeader(header string) string {
	header = strings.TrimPrefix(header, "\uFEFF")
	header = strings.TrimSpace(header)
	header = strings.ToLower(header)
	return header
}

func buildAliAuditRecord(record []string, fieldIndex map[string]int, rowNumber int) *aliAuditCSVRecord {
	getField := func(name string) string {
		i, ok := fieldIndex[name]
		if !ok || i >= len(record) {
			return ""
		}
		return strings.TrimSpace(record[i])
	}

	return &aliAuditCSVRecord{
		RowNumber:  rowNumber,
		SQL:        getField("log"),
		DBName:     getField("db"),
		ThreadID:   getField("tid"),
		Username:   getField("user"),
		SQLType:    getField("sql_type"),
		Fail:       getField("fail"),
		ReturnRows: getField("return_rows"),
		Latency:    getField("latency"),
		OriginTime: getField("origin_time"),
	}
}

func shouldSkipAliAuditRecord(record *aliAuditCSVRecord) bool {
	if fail := strings.TrimSpace(record.Fail); fail != "" && fail != "0" {
		return true
	}

	sqlType := strings.ToLower(strings.TrimSpace(record.SQLType))
	switch sqlType {
	case "login", "logout", "changeuser":
		return true
	}

	sqlText := strings.TrimSpace(record.SQL)
	return sqlText == ""
}

func convertAliAuditToLogEntry(record *aliAuditCSVRecord) (*LogEntry, error) {
	threadID := strings.TrimSpace(record.ThreadID)
	if threadID == "" {
		return nil, fmt.Errorf("missing tid")
	}

	queryTime, err := parseTencentAuditInt64(record.Latency)
	if err != nil {
		return nil, fmt.Errorf("parse latency failed: %w", err)
	}

	rowsSent := 0
	if strings.TrimSpace(record.ReturnRows) != "" {
		rowsSent, err = strconv.Atoi(strings.TrimSpace(record.ReturnRows))
		if err != nil {
			return nil, fmt.Errorf("parse return_rows failed: %w", err)
		}
	}

	timestamp, err := parseAliAuditTimestamp(record.OriginTime)
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
		SQLType:      normalizeAliAuditSQLType(record.SQLType, cleanedSQL),
		DBName:       strings.TrimSpace(record.DBName),
		Timestamp:    timestamp,
		Digest:       digest,
	}, nil
}

func parseAliAuditTimestamp(originTimeValue string) (float64, error) {
	originTimeValue = strings.TrimSpace(originTimeValue)
	if originTimeValue == "" {
		return 0, fmt.Errorf("missing origin_time")
	}

	originTimeInt, err := strconv.ParseInt(originTimeValue, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse origin_time failed: %w", err)
	}

	return float64(time.UnixMicro(originTimeInt).UnixNano()) / 1e9, nil
}

func normalizeAliAuditSQLType(sqlType, sqlText string) string {
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

func sortAliAuditEntries(entries []sortedAliAuditEntry) {
	// The Alibaba export can contain local time inversions, so sort by event time before replay.
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Entry.Timestamp == entries[j].Entry.Timestamp {
			return entries[i].RowNumber < entries[j].RowNumber
		}
		return entries[i].Entry.Timestamp < entries[j].Entry.Timestamp
	})
}
