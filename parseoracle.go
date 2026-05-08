package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"golang.org/x/net/html"
)

// OracleAWRRecord represents a parsed Oracle AWR SQL entry
type OracleAWRRecord struct {
	SQLID         string  `json:"sql_id"`
	SQLText       string  `json:"sql_text"`
	SQLType       string  `json:"sql_type"`
	Executions    int64   `json:"executions"`
	ElapsedTimeS  float64 `json:"elapsed_time_s"`
	CPUTimeS      float64 `json:"cpu_time_s"`
	BufferGets    int64   `json:"buffer_gets"`
	PhysicalReads int64   `json:"physical_reads"`
	RowsProcessed int64   `json:"rows_processed"`
	Module        string  `json:"module"`
}

// ParseOracleAWR parses Oracle AWR HTML report files and converts SQL to MySQL-compatible syntax
func ParseOracleAWR(awrPaths, slowOutputPath string) {
	if awrPaths == "" || slowOutputPath == "" {
		fmt.Println("Usage: ./sql-replay -mode parseoracle -slow-in <path_to_awr_html_or_dir> -slow-out <path_to_slow_output_file>")
		return
	}

	// Collect HTML files
	var files []string
	info, err := os.Stat(awrPaths)
	if err != nil {
		fmt.Println("Error accessing path:", err)
		return
	}
	if info.IsDir() {
		entries, err := os.ReadDir(awrPaths)
		if err != nil {
			fmt.Println("Error reading directory:", err)
			return
		}
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(strings.ToLower(e.Name()), ".html") {
				files = append(files, awrPaths+"/"+e.Name())
			}
		}
	} else {
		files = []string{awrPaths}
	}

	if len(files) == 0 {
		fmt.Println("No HTML files found")
		return
	}

	// Parse all AWR files and collect SQL
	allSQLs := make(map[string]*OracleAWRRecord) // keyed by SQL ID
	for _, f := range files {
		fmt.Fprintf(os.Stderr, "Parsing %s...\n", f)
		sqls, err := parseAWRFile(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing %s: %v\n", f, err)
			continue
		}
		fmt.Fprintf(os.Stderr, "  Found %d SQL statements\n", len(sqls))
		for id, sql := range sqls {
			if _, exists := allSQLs[id]; !exists {
				allSQLs[id] = sql
			}
		}
	}

	fmt.Fprintf(os.Stderr, "Total unique SQL statements: %d\n", len(allSQLs))

	// Convert Oracle SQL to MySQL and output as LogEntry JSON
	outputFile, err := os.Create(slowOutputPath)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile.Close()

	converted := 0
	failed := 0
	skipped := 0

	for _, record := range allSQLs {
		// Skip PL/SQL blocks and internal Oracle statements
		if shouldSkipSQL(record.SQLText) {
			skipped++
			continue
		}

		// Convert Oracle SQL to MySQL
		mysqlSQL, convertErr := ConvertOracleToMySQL(record.SQLText)
		if convertErr != nil {
			fmt.Fprintf(os.Stderr, "  [SKIP] SQL ID %s: %v\n", record.SQLID, convertErr)
			skipped++
			continue
		}

		// Post-conversion validation: skip results that aren't valid SQL statements
		upper := strings.TrimSpace(strings.ToUpper(mysqlSQL))
		if !isValidMySQLStatement(upper) {
			skipped++
			continue
		}

		// Extract SQL type
		sqlType := extractOracleSQLType(mysqlSQL)

		// Generate digest
		normalizedSQL := parser.Normalize(mysqlSQL)
		digest := parser.DigestNormalized(normalizedSQL).String()

		// Build timestamp from AWR snapshot time (use current time as fallback)
		timestamp := float64(time.Now().Unix()) + float64(time.Now().Nanosecond())/1e9

		entry := LogEntry{
			ConnectionID: record.SQLID,
			QueryTime:    int64(record.ElapsedTimeS * 1e6), // convert to microseconds
			SQL:          mysqlSQL,
			RowsSent:     int(record.RowsProcessed),
			Username:     "",
			SQLType:      sqlType,
			DBName:       "",
			Timestamp:    timestamp,
			Digest:       digest,
		}

		jsonData, err := json.Marshal(entry)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  [ERROR] JSON marshal failed for SQL ID %s: %v\n", record.SQLID, err)
			failed++
			continue
		}

		fmt.Fprintln(outputFile, string(jsonData))
		converted++
	}

	fmt.Fprintf(os.Stderr, "Conversion complete. Converted: %d, Skipped: %d, Failed: %d\n", converted, skipped, failed)
}

// parseAWRFile parses a single AWR HTML file and extracts SQL statements with metrics
func parseAWRFile(filePath string) (map[string]*OracleAWRRecord, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read the entire file
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	content := string(data)

	// Parse the HTML to extract SQL text from pre_sqltext elements
	sqlMap := extractSQLFromAWR(content)

	// Parse metrics from "SQL ordered by Elapsed Time" section
	metricsMap := extractMetricsFromAWR(content)

	// Merge metrics into SQL records
	for sqlID, record := range sqlMap {
		if metrics, ok := metricsMap[sqlID]; ok {
			record.Executions = metrics.Executions
			record.ElapsedTimeS = metrics.ElapsedTimeS
			record.CPUTimeS = metrics.CPUTimeS
			record.BufferGets = metrics.BufferGets
			record.PhysicalReads = metrics.PhysicalReads
			record.RowsProcessed = metrics.RowsProcessed
			record.Module = metrics.Module
		}
	}

	return sqlMap, nil
}

// extractSQLFromAWR extracts SQL text from the "Complete List of SQL Text" section
func extractSQLFromAWR(content string) map[string]*OracleAWRRecord {
	result := make(map[string]*OracleAWRRecord)

	// Use HTML tokenizer to find pre_sqltext elements and their associated SQL IDs
	tokenizer := html.NewTokenizer(strings.NewReader(content))

	var currentSQLID string
	var inPreSQLText bool
	var sqlTextBuf strings.Builder

	for {
		tt := tokenizer.Next()
		if tt == html.ErrorToken {
			break
		}

		switch tt {
		case html.StartTagToken:
			tagName, _ := tokenizer.TagName()
			if string(tagName) == "a" {
				// Check for anchor with name attribute (SQL ID)
				for {
					attrName, attrVal, more := tokenizer.TagAttr()
					if string(attrName) == "name" {
						name := string(attrVal)
						// SQL IDs are 13-char alphanumeric strings
						if len(name) == 13 && isAlphanumeric(name) {
							currentSQLID = name
						}
					}
					if !more {
						break
					}
				}
			}
			if string(tagName) == "pre_sqltext" {
				inPreSQLText = true
				sqlTextBuf.Reset()
			}

		case html.EndTagToken:
			tagName, _ := tokenizer.TagName()
			if string(tagName) == "pre_sqltext" && inPreSQLText {
				inPreSQLText = false
				if currentSQLID != "" {
					sqlText := strings.TrimSpace(sqlTextBuf.String())
					if sqlText != "" {
						result[currentSQLID] = &OracleAWRRecord{
							SQLID:   currentSQLID,
							SQLText: sqlText,
						}
					}
				}
				currentSQLID = ""
			}

		case html.TextToken:
			if inPreSQLText {
				sqlTextBuf.Write(tokenizer.Text())
			}
		}
	}

	return result
}

// extractMetricsFromAWR extracts SQL metrics from the "SQL ordered by Elapsed Time" section
func extractMetricsFromAWR(content string) map[string]*OracleAWRRecord {
	result := make(map[string]*OracleAWRRecord)

	// Find the "SQL ordered by Elapsed Time" section
	reSection := regexp.MustCompile(`(?is)SQL ordered by Elapsed Time.*?<table[^>]*>(.*?)</table>`)
	sectionMatch := reSection.FindStringSubmatch(content)
	if sectionMatch == nil {
		return result
	}

	tableContent := sectionMatch[1]

	// Parse rows to extract SQL ID and metrics
	// Each row has: Elapsed Time, Executions, Elapsed Time per Exec, %Total, %CPU, %IO, SQL Id, SQL Module, SQL Text
	reRow := regexp.MustCompile(`(?is)<tr[^>]*>(.*?)</tr>`)
	rows := reRow.FindAllStringSubmatch(tableContent, -1)

	for _, row := range rows {
		rowContent := row[1]
		reCell := regexp.MustCompile(`(?is)<td[^>]*>(.*?)</td>`)
		cells := reCell.FindAllStringSubmatch(rowContent, -1)
		if len(cells) < 7 {
			continue
		}

		// Extract values from cells (strip HTML tags)
		getText := func(s string) string {
			return stripHTMLTags(strings.TrimSpace(s))
		}

		// Cell 0: Elapsed Time (s) - format: "729,056.47"
		elapsedStr := getText(cells[0][1])
		elapsedStr = strings.ReplaceAll(elapsedStr, ",", "")
		var elapsed float64
		fmt.Sscanf(elapsedStr, "%f", &elapsed)

		// Cell 1: Executions
		execStr := getText(cells[1][1])
		execStr = strings.ReplaceAll(execStr, ",", "")
		var execs int64
		fmt.Sscanf(execStr, "%d", &execs)

		// Cell 6: SQL ID (contains anchor link)
		sqlIDText := getText(cells[6][1])
		reSQLID := regexp.MustCompile(`[a-z0-9]{13}`)
		sqlIDMatch := reSQLID.FindString(sqlIDText)
		if sqlIDMatch == "" {
			continue
		}

		// Cell 7: Module (if exists)
		module := ""
		if len(cells) > 7 {
			module = getText(cells[7][1])
		}

		result[sqlIDMatch] = &OracleAWRRecord{
			SQLID:        sqlIDMatch,
			Executions:   execs,
			ElapsedTimeS: elapsed,
			Module:       module,
		}
	}

	return result
}

// shouldSkipSQL returns true for PL/SQL blocks and internal Oracle statements
// that cannot be converted to MySQL
func shouldSkipSQL(sql string) bool {
	upper := strings.TrimSpace(strings.ToUpper(sql))

	// Skip DECLARE blocks (PL/SQL anonymous blocks)
	if strings.HasPrefix(upper, "DECLARE") {
		return true
	}

	// Skip standalone BEGIN blocks (PL/SQL)
	if strings.HasPrefix(upper, "BEGIN") {
		return true
	}

	// Skip Oracle internal system statements
	if strings.Contains(upper, "INSERT INTO SYS.AUD$") {
		return true
	}
	if strings.Contains(upper, "UPDATE USER$ SET") {
		return true
	}
	if strings.Contains(upper, "DBMS_PARALLEL_EXECUTE") {
		return true
	}
	if strings.Contains(upper, "DBMS_REPORT") {
		return true
	}
	if strings.Contains(upper, "DBMS_SCHEDULER") {
		return true
	}
	if strings.Contains(upper, "DBMS_STATS") {
		return true
	}
	if strings.Contains(upper, "DBMS_LOB") {
		return true
	}
	if strings.Contains(upper, "DBMS_OUTPUT") {
		return true
	}
	if strings.Contains(upper, "DBMS_LOCK") {
		return true
	}
	if strings.Contains(upper, "DBMS_METADATA") {
		return true
	}

	// Skip DDL that is Oracle-specific (ALTER INDEX ... REBUILD, etc.)
	if strings.HasPrefix(upper, "ALTER INDEX") && strings.Contains(upper, "REBUILD") {
		return true
	}

	// Skip CALL to Oracle procedures (non-standard SQL)
	if strings.HasPrefix(upper, "CALL DBMS_") {
		return true
	}

	// Skip internal monitoring queries
	if strings.Contains(upper, "GV$(") || strings.Contains(upper, "V$(") {
		return true
	}
	if strings.Contains(upper, "TABLE(GV$") || strings.Contains(upper, "TABLE(V$") {
		return true
	}
	if strings.Contains(upper, "USERENV(") {
		return true
	}

	// Skip XMLTYPE queries
	if strings.Contains(upper, "XMLTYPE(") {
		return true
	}

	// Skip Oracle system table queries (tables with $ in name)
	oracleSysTables := []string{
		"FROM USER$", "FROM TS$", "FROM OBJ$", "FROM CON$", "FROM CDEF$",
		"FROM SYSAUTH$", "FROM SQLOBJ$", "FROM DBA_SEGMENTS", "FROM DBA_TABLESPACES",
		"FROM V$LOG", "FROM V$RECOVERY_AREA_USAGE", "FROM V$OPEN_CURSOR",
		"FROM DBA_SCHEDULER_JOBS", "FROM ALL_SCHEDULER_JOBS", "FROM ALL_TAB_COMMENTS",
		"FROM WRI$_ADV_MESSAGE_GROUPS", "FROM ACLMVREFSTAT$",
		"FROM T_INDEX_COLUMN_DEF", "FROM DOPROCESS_MONITOR", "FROM T_PROCESS_LOG",
		"FROM CS_KR_MODULES", "FROM CS_JBXX", "FROM DOPROCESS ",
		"FROM SYS.ACL", "FROM DBA_FREE_SPACE",
	}
	for _, pattern := range oracleSysTables {
		if strings.Contains(upper, pattern) {
			return true
		}
	}

	// Skip CONNECT BY queries (Oracle hierarchical queries)
	if strings.Contains(upper, "CONNECT BY") {
		return true
	}

	// Skip CALL statements (Oracle procedure calls)
	if strings.HasPrefix(upper, "CALL ") {
		return true
	}

	// Skip CONCAT() statement wrappers (Oracle PL/SQL construct, not SQL)
	if strings.HasPrefix(upper, "CONCAT(") || strings.HasPrefix(upper, "CONCAT (") {
		return true
	}

	// Skip CREATE TABLE ... AS SELECT (TiDB doesn't support CTAS)
	if strings.HasPrefix(upper, "CREATE TABLE") && strings.Contains(upper, " AS") {
		return true
	}

	// Skip CREATE TABLE ... AS (without SELECT, same issue)
	if strings.HasPrefix(upper, "CREATE TABLE") && strings.Contains(upper, " AS\n") {
		return true
	}

	// Skip OPTIMIZE TABLE (different syntax in TiDB)
	if strings.HasPrefix(upper, "OPTIMIZE TABLE") {
		return true
	}

	// Skip Oracle internal system tables (more patterns)
	oracleSysTablePatterns := []string{
		"FROM WRI$_", "FROM SQLOBJ$", "FROM SCHEDULER$_",
		"FROM SYS.ACL", "FROM SQL$",
		"INTO WRI$_", "INTO SQLOBJ$", "INTO SCHEDULER$_",
		"FROM DEPENDENCY$", "FROM OBJ$ ",
	}
	for _, pattern := range oracleSysTablePatterns {
		if strings.Contains(upper, pattern) {
			return true
		}
	}

	// Skip Oracle pipelined/table functions
	if strings.Contains(upper, "TABLE(PKG_") || strings.Contains(upper, "TABLE (PKG_") {
		return true
	}

	return false
}

// extractOracleSQLType determines the SQL type from the converted SQL text
func extractOracleSQLType(sql string) string {
	upper := strings.TrimSpace(strings.ToUpper(sql))

	switch {
	case strings.HasPrefix(upper, "SELECT"):
		return "select"
	case strings.HasPrefix(upper, "INSERT"):
		return "insert"
	case strings.HasPrefix(upper, "UPDATE"):
		return "update"
	case strings.HasPrefix(upper, "DELETE"):
		return "delete"
	case strings.HasPrefix(upper, "MERGE"):
		return "merge"
	case strings.HasPrefix(upper, "CREATE"):
		return "create"
	case strings.HasPrefix(upper, "ALTER"):
		return "alter"
	case strings.HasPrefix(upper, "DROP"):
		return "drop"
	case strings.HasPrefix(upper, "WITH"):
		return "select" // CTE is typically a SELECT
	default:
		return "other"
	}
}

// isValidMySQLStatement checks if the converted SQL starts with a valid MySQL statement keyword
func isValidMySQLStatement(upper string) bool {
	validPrefixes := []string{
		"SELECT ", "SELECT(", "SELECT/*", "SELECT\n", "SELECT\t",
		"INSERT ", "INSERT\n", "INSERT\t",
		"UPDATE ", "UPDATE\n", "UPDATE\t",
		"DELETE ", "DELETE\n", "DELETE\t", "DELETE`",
		"WITH ", "WITH(", "WITH\n", "WITH\t",
		"CREATE ", "CREATE\n", "CREATE\t",
		"ALTER ", "ALTER\n", "ALTER\t",
		"DROP ", "DROP\n", "DROP\t",
		"TRUNCATE ", "OPTIMIZE ", "REPLACE ", "CALL ",
	}
	for _, prefix := range validPrefixes {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	// Also allow MERGE (even though TiDB doesn't support it, keep for analysis)
	if strings.HasPrefix(upper, "MERGE ") || strings.HasPrefix(upper, "MERGE\n") {
		return true
	}
	return false
}

// Helper functions

func isAlphanumeric(s string) bool {
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
}

func stripHTMLTags(s string) string {
	// Simple HTML tag stripper
	var result strings.Builder
	inTag := false
	for _, c := range s {
		if c == '<' {
			inTag = true
			continue
		}
		if c == '>' {
			inTag = false
			continue
		}
		if !inTag {
			result.WriteRune(c)
		}
	}
	// Decode common HTML entities
	resultStr := result.String()
	resultStr = strings.ReplaceAll(resultStr, "&amp;", "&")
	resultStr = strings.ReplaceAll(resultStr, "&lt;", "<")
	resultStr = strings.ReplaceAll(resultStr, "&gt;", ">")
	resultStr = strings.ReplaceAll(resultStr, "&quot;", "\"")
	resultStr = strings.ReplaceAll(resultStr, "&#39;", "'")
	resultStr = strings.ReplaceAll(resultStr, "&nbsp;", " ")
	return resultStr
}

// ParseOracleAWRBatch parses multiple AWR files (comma-separated or directory)
func ParseOracleAWRBatch(awrPaths, slowOutputPath string) {
	ParseOracleAWR(awrPaths, slowOutputPath)
}

// ExtractOracleSQLForTest extracts all SQL from AWR files and writes them to a text file for review
func ExtractOracleSQLForTest(awrPaths, outputPath string) {
	var files []string
	info, err := os.Stat(awrPaths)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	if info.IsDir() {
		entries, _ := os.ReadDir(awrPaths)
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(strings.ToLower(e.Name()), ".html") {
				files = append(files, awrPaths+"/"+e.Name())
			}
		}
	} else {
		for _, p := range strings.Split(awrPaths, ",") {
			files = append(files, strings.TrimSpace(p))
		}
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		fmt.Println("Error creating output:", err)
		return
	}
	defer outFile.Close()

	allSQLs := make(map[string]string)
	for _, f := range files {
		sqls, err := parseAWRFile(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			continue
		}
		for id, rec := range sqls {
			if _, exists := allSQLs[id]; !exists {
				allSQLs[id] = rec.SQLText
			}
		}
	}

	writer := bufio.NewWriter(outFile)
	for id, sql := range allSQLs {
		fmt.Fprintf(writer, "=== SQL ID: %s ===\n%s\n\n", id, sql)
	}
	writer.Flush()

	fmt.Fprintf(os.Stderr, "Extracted %d SQL statements to %s\n", len(allSQLs), outputPath)
}
