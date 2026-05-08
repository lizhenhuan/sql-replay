package main

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
)

// ConvertOracleToMySQL converts Oracle SQL to MySQL-compatible SQL
// Strategy: regex-based conversion first, then AST-based verification/fallback
func ConvertOracleToMySQL(oracleSQL string) (string, error) {
	sql := strings.TrimSpace(oracleSQL)
	if sql == "" {
		return "", fmt.Errorf("empty SQL")
	}

	upper := strings.ToUpper(sql)

	// Determine statement type for targeted conversion
	stmtType := classifyStatement(upper)

	switch stmtType {
	case stmtSELECT, stmtWITH:
		return convertDML(sql, stmtType)
	case stmtINSERT:
		return convertInsert(sql)
	case stmtUPDATE:
		return convertUpdate(sql)
	case stmtDELETE:
		return convertDelete(sql)
	case stmtMERGE:
		return convertMerge(sql)
	case stmtCREATE:
		return convertCreate(sql)
	case stmtALTER:
		return convertAlter(sql)
	default:
		// Try generic conversion
		return convertGeneric(sql)
	}
}

const (
	stmtSELECT = "SELECT"
	stmtWITH   = "WITH"
	stmtINSERT = "INSERT"
	stmtUPDATE = "UPDATE"
	stmtDELETE = "DELETE"
	stmtMERGE  = "MERGE"
	stmtCREATE = "CREATE"
	stmtALTER  = "ALTER"
	stmtOTHER  = "OTHER"
)

func classifyStatement(upper string) string {
	// Handle SQL hints at the beginning
	trimmed := strings.TrimSpace(upper)
	if strings.HasPrefix(trimmed, "/*") {
		end := strings.Index(trimmed, "*/")
		if end >= 0 {
			trimmed = strings.TrimSpace(trimmed[end+2:])
		}
	}
	// Handle comment lines at the beginning
	for strings.HasPrefix(trimmed, "--") {
		nl := strings.Index(trimmed, "\n")
		if nl < 0 {
			return stmtOTHER
		}
		trimmed = strings.TrimSpace(trimmed[nl+1:])
	}

	words := strings.SplitN(trimmed, " ", 2)
	if len(words) == 0 {
		return stmtOTHER
	}
	first := words[0]

	switch first {
	case "SELECT":
		return stmtSELECT
	case "WITH":
		return stmtWITH
	case "INSERT":
		return stmtINSERT
	case "UPDATE":
		return stmtUPDATE
	case "DELETE":
		return stmtDELETE
	case "MERGE":
		return stmtMERGE
	case "CREATE":
		return stmtCREATE
	case "ALTER":
		return stmtALTER
	default:
		return stmtOTHER
	}
}

// ==================== Generic Conversion ====================

func convertGeneric(sql string) (string, error) {
	result := sql
	result = applyCommonConversions(result)
	return result, nil
}

// ==================== DML Conversion (SELECT/WITH) ====================

func convertDML(sql string, stmtType string) (string, error) {
	result := sql

	// Apply all common conversions
	result = applyCommonConversions(result)

	// Handle SELECT-specific conversions
	result = convertSelectSpecific(result)

	return result, nil
}

// ==================== INSERT Conversion ====================

func convertInsert(sql string) (string, error) {
	result := sql
	result = applyCommonConversions(result)

	// Oracle: INSERT INTO ... (col1, col2) VALUES (seq.NEXTVAL, ...)
	// Convert sequence references
	result = convertSequenceRefs(result)

	return result, nil
}

// ==================== UPDATE Conversion ====================

func convertUpdate(sql string) (string, error) {
	result := sql
	result = applyCommonConversions(result)
	return result, nil
}

// ==================== DELETE Conversion ====================

func convertDelete(sql string) (string, error) {
	result := sql
	result = applyCommonConversions(result)

	// Oracle: DELETE ... WHERE ROWID BETWEEN :B2 AND :B1
	// ROWID is Oracle-specific, remove such conditions
	result = convertRowIDConditions(result)

	// Oracle: DELETE table [alias] WHERE ... -> MySQL: DELETE FROM table WHERE ...
	// MySQL requires FROM keyword and doesn't support table aliases in simple DELETE
	upper := strings.ToUpper(strings.TrimSpace(result))
	if strings.HasPrefix(upper, "DELETE") && !strings.HasPrefix(upper, "DELETE FROM") {
		// Add FROM and optionally remove alias
		re := regexp.MustCompile(`(?i)^(\s*DELETE\s+)(\S+)(\s+\w+)?(\s+.*)$`)
		match := re.FindStringSubmatch(result)
		if match != nil {
			result = match[1] + "FROM " + match[2] + match[4]
		} else {
			// Fallback: just add FROM after DELETE
			re2 := regexp.MustCompile(`(?i)^(\s*DELETE)\s+`)
			result = re2.ReplaceAllString(result, "${1} FROM ")
		}
	}

	return result, nil
}

// ==================== MERGE Conversion ====================

func convertMerge(sql string) (string, error) {
	result := sql
	result = applyCommonConversions(result)

	// MERGE INTO is not supported by TiDB
	// Convert to INSERT ... ON DUPLICATE KEY UPDATE
	upper := strings.ToUpper(strings.TrimSpace(result))
	if !strings.HasPrefix(upper, "MERGE") {
		return result, nil
	}

	// Handle various MERGE formats:
	// MERGE INTO table A USING ...
	// MERGE/**/ INTO table A USING ...
	// merge into table a using(...)
	// MERGE INTO (SELECT ...) A USING ...

	// Normalize: remove comments between MERGE and INTO
	re := regexp.MustCompile(`(?i)MERGE\s*(?:/\*[^*]*\*/\s*)+INTO`)
	result = re.ReplaceAllString(result, "MERGE INTO")

	// Parse MERGE INTO target USING
	reTarget := regexp.MustCompile(`(?i)MERGE\s+INTO\s+(.+?)\s+USING\s+`)
	match := reTarget.FindStringSubmatch(result)
	if match == nil {
		// Try with USING( (no space before paren)
		reTarget2 := regexp.MustCompile(`(?i)MERGE\s+INTO\s+(.+?)\s+USING\s*\(`)
		match = reTarget2.FindStringSubmatch(result)
		if match == nil {
			return result, nil
		}
		// Put the ( back for source parsing
		match[1] = strings.TrimSpace(match[1])
	}

	targetPart := strings.TrimSpace(match[1])
	targetParts := strings.Fields(targetPart)
	if len(targetParts) < 1 {
		return result, nil
	}
	targetTable := targetParts[0]
	targetAlias := "A"
	if len(targetParts) >= 2 {
		targetAlias = strings.ToUpper(targetParts[1])
	}

	// Find the rest after USING
	usingIdx := strings.Index(strings.ToUpper(result), "USING")
	if usingIdx < 0 {
		return result, nil
	}
	rest := result[usingIdx+5:]

	// Find ON clause - need to handle nested parens in USING source
	onIdx := findKeywordAtTopLevel(rest, "ON")
	if onIdx < 0 {
		return result, nil
	}
	source := strings.TrimSpace(rest[:onIdx])
	afterOn := strings.TrimSpace(rest[onIdx+2:])

	// Find WHEN MATCHED THEN UPDATE SET
	whenMatchedIdx := indexOfCaseInsensitive(afterOn, "WHEN MATCHED THEN UPDATE SET")
	updateSet := ""
	insertPart := ""
	if whenMatchedIdx >= 0 {
		updatePart := afterOn[whenMatchedIdx+len("WHEN MATCHED THEN UPDATE SET"):]
		// Find WHERE or WHEN NOT MATCHED in the update part
		nextClause := -1
		for _, kw := range []string{"WHEN NOT MATCHED", "WHEN MATCHED"} {
			idx := indexOfCaseInsensitive(updatePart, kw)
			if idx >= 0 && (nextClause < 0 || idx < nextClause) {
				nextClause = idx
			}
		}
		if nextClause >= 0 {
			updateSet = strings.TrimSpace(updatePart[:nextClause])
		} else {
			updateSet = strings.TrimSpace(updatePart)
		}
	}

	// Find WHEN NOT MATCHED THEN INSERT
	whenNotIdx := indexOfCaseInsensitive(afterOn, "WHEN NOT MATCHED")
	if whenNotIdx >= 0 {
		insertPart = afterOn[whenNotIdx:]
	}

	// Build INSERT ... ON DUPLICATE KEY UPDATE
	if insertPart != "" && source != "" {
		// Extract INSERT columns and values
		reInsert := regexp.MustCompile(`(?i)INSERT\s*\(([^)]+)\)\s+VALUES\s*\((.+)\)$`)
		insertMatch := reInsert.FindStringSubmatch(strings.TrimSpace(insertPart))
		if insertMatch == nil {
			// Try INSERT VALUES(...) without column list
			reInsert2 := regexp.MustCompile(`(?i)INSERT\s+VALUES\s*\((.+)\)$`)
			insertMatch2 := reInsert2.FindStringSubmatch(strings.TrimSpace(insertPart))
			if insertMatch2 != nil {
				insertMatch = []string{"", "", insertMatch2[1]}
			}
		}

		if insertMatch != nil && len(insertMatch) >= 3 {
			insertCols := strings.TrimSpace(insertMatch[1])
			insertVals := strings.TrimSpace(insertMatch[2])

			var buf strings.Builder
			buf.WriteString("INSERT INTO ")
			buf.WriteString(targetTable)
			if insertCols != "" {
				buf.WriteString(" (")
				buf.WriteString(insertCols)
				buf.WriteString(")")
			}
			buf.WriteString(" SELECT ")
			buf.WriteString(insertVals)
			buf.WriteString(" FROM ")
			// Ensure source ends properly (may need a closing paren)
			if !strings.HasSuffix(strings.TrimSpace(source), ")") {
				buf.WriteString("(")
				buf.WriteString(source)
				buf.WriteString(")")
			} else {
				buf.WriteString(source)
			}

			if updateSet != "" {
				buf.WriteString(" ON DUPLICATE KEY UPDATE ")
				updateSetClean := strings.ReplaceAll(updateSet, targetAlias+".", "")
				buf.WriteString(updateSetClean)
			}
			return buf.String(), nil
		}
	}

	// Fallback: if only MATCHED (UPDATE), convert to UPDATE with JOIN
	if updateSet != "" && source != "" {
		var buf strings.Builder
		buf.WriteString("UPDATE ")
		buf.WriteString(targetTable)
		buf.WriteString(" A INNER JOIN ")
		buf.WriteString(source)
		buf.WriteString(" B ON ")
		// We don't have the ON condition parsed separately, so this is best-effort
		buf.WriteString("1=1 ")
		buf.WriteString("SET ")
		updateSetClean := strings.ReplaceAll(updateSet, targetAlias+".", "A.")
		updateSetClean = strings.ReplaceAll(updateSetClean, "B.", "B.")
		buf.WriteString(updateSetClean)
		return buf.String(), nil
	}

	return result, nil
}

// findKeywordAtTopLevel finds a keyword at the top level (not inside parens or strings)
func findKeywordAtTopLevel(sql string, keyword string) int {
	upper := strings.ToUpper(sql)
	kw := strings.ToUpper(keyword)
	inQuote := false
	depth := 0
	for i := 0; i <= len(sql)-len(kw); i++ {
		ch := sql[i]
		if ch == '\'' && !inQuote {
			inQuote = true
			continue
		}
		if ch == '\'' && inQuote {
			if i+1 < len(sql) && sql[i+1] == '\'' {
				i++
			} else {
				inQuote = false
			}
			continue
		}
		if inQuote {
			continue
		}
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' {
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth == 0 && upper[i:i+len(kw)] == kw {
			// Check word boundary
			if (i == 0 || !isWordChar(upper[i-1])) && (i+len(kw) >= len(sql) || !isWordChar(upper[i+len(kw)])) {
				return i
			}
		}
	}
	return -1
}

// ==================== CREATE Conversion ====================

func convertCreate(sql string) (string, error) {
	result := sql

	// CREATE TABLE ... PARALLEL n NOLOGGING AS SELECT ...
	// Remove PARALLEL and NOLOGGING
	result = removeOracleHints(result)

	// Oracle data types to MySQL
	result = convertOracleDataTypes(result)

	return result, nil
}

// ==================== ALTER Conversion ====================

func convertAlter(sql string) (string, error) {
	result := sql

	// ALTER TABLE ... SHRINK SPACE CASCADE
	// MySQL doesn't support SHRINK SPACE, convert to OPTIMIZE TABLE
	upper := strings.ToUpper(result)
	if strings.Contains(upper, "SHRINK SPACE") {
		reTable := regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(\S+)\s+SHRINK\s+SPACE(\s+CASCADE)?`)
		if match := reTable.FindStringSubmatch(result); len(match) > 1 {
			result = fmt.Sprintf("OPTIMIZE TABLE %s", match[1])
		}
		return result, nil
	}

	return result, nil
}

// ==================== Common Conversions ====================

func applyCommonConversions(sql string) string {
	result := sql

	// 1. Remove Oracle-specific hints (PARALLEL, NOLOGGING, etc.)
	result = removeOracleHints(result)

	// 2. Remove Oracle SQL hints that MySQL doesn't support
	result = removeUnsupportedHints(result)

	// 3. Replace Oracle double-quoted identifiers with backtick-quoted
	result = convertDoubleQuotesToBackticks(result)

	// 4. Convert Oracle functions to MySQL equivalents
	result = convertFunctions(result)

	// 5. Convert || concatenation to CONCAT()
	result = convertConcatOperator(result)

	// 6. Convert sequence references (seq.NEXTVAL, seq.CURRVAL)
	result = convertSequenceRefs(result)

	// 7. Remove FROM DUAL where unnecessary
	result = convertFromDual(result)

	// 8. Convert Oracle data types
	result = convertOracleDataTypes(result)

	// 9. Convert ROWNUM to LIMIT (simplified)
	result = convertRowNum(result)

	// 10. Convert ROWID references
	result = convertRowIDConditions(result)

	// 11. Convert Oracle JOIN syntax (+) to ANSI JOIN
	result = convertOuterJoinPlus(result)

	// 12. Convert Oracle date literals
	result = convertDateLiterals(result)

	// 13. Clean up whitespace
	result = cleanWhitespace(result)

	// 14. Convert Oracle bind variables :B1, :1, :name to MySQL ?
	result = convertBindVariables(result)

	// 15. Remove Oracle-specific function calls that can't be converted
	result = convertUnsupportedFunctions(result)

	// 16. Remove Oracle PARTITION() clauses on tables
	result = removePartitionClauses(result)

	// 17. Remove Oracle database link references (@dblink)
	result = removeDBLinks(result)

	// 18. Add missing aliases for derived tables
	result = addDerivedTableAliases(result)

	// 19. Strip table alias prefix from INSERT column lists
	result = stripInsertColumnPrefix(result)

	// 20. Fix TO_CHAR(.col, format) - Oracle shorthand for missing table alias
	// Replace .col with col (remove the dot prefix)
	result = regexp.MustCompile(`(?i)TO_CHAR\(\.(\w+)`).ReplaceAllString(result, "TO_CHAR($1")

	return result
}

// ==================== Individual Conversion Functions ====================

// removeOracleHints removes PARALLEL, NOLOGGING, and similar Oracle-specific hints
func removeOracleHints(sql string) string {
	// Remove PARALLEL(n) or PARALLEL n
	re := regexp.MustCompile(`(?i)\s+PARALLEL\s*(\(\s*\d+\s*\)|\d+)`)
	result := re.ReplaceAllString(sql, " ")

	// Remove NOLOGGING
	re = regexp.MustCompile(`(?i)\s+NOLOGGING`)
	result = re.ReplaceAllString(result, " ")

	// Remove SHARED(n) hint
	re = regexp.MustCompile(`(?i)/\*\s*\+?\s*SHARED\s*\(\s*\d+\s*\)\s*\*/`)
	result = re.ReplaceAllString(result, " ")

	return result
}

// removeUnsupportedHints removes Oracle-specific SQL hints that MySQL doesn't support
func removeUnsupportedHints(sql string) string {
	// Remove Oracle-specific hints but keep MySQL-compatible ones (like FIRST_ROWS)
	// MySQL supports: MAX_EXECUTION_TIME, QB_NAME, SET_VAR, etc.
	// Oracle-specific to remove: OPAQUE_TRANSFORM, SWAP_JOIN_INPUTS, USE_HASH, FULL, etc.

	// Keep hints that are commonly supported by both
	// For now, remove Oracle optimizer hints that start with /*+
	// but keep basic ones
	unsupportedHints := []string{
		"OPAQUE_TRANSFORM",
		"SWAP_JOIN_INPUTS",
		"USE_HASH",
		"USE_NL",
		"USE_MERGE",
		"FULL",
		"INDEX",
		"PARALLEL_INDEX",
		"NO_INDEX",
		"APPEND",
		"DRIVING_SITE",
		"MATERIALIZE",
		"INLINE",
		"PUSH_PRED",
		"NO_PUSH_PRED",
		"CURSOR_SHARING_EXACT",
		"DYNAMIC_SAMPLING",
		"OPT_PARAM",
		"CARDINALITY",
		"LEADING",
		"ORDERED",
		"RULE",
		"STAR_TRANSFORMATION",
		"FACT",
		"NO_FACT",
		"NO_ACCESS",
		"NO_BUFFER",
		"NO_CPU_COSTING",
		"NO_EXPAND",
		"NO_PUSH_SUBQ",
		"NO_QKN_BUFF",
		"NO_REWRITE",
		"NO_UNNEST",
		"NO_XML_QUERY_REWRITE",
		"PX_JOIN_FILTER",
		"NO_PX_JOIN_FILTER",
		"QB_NAME",
		"MONITOR",
		"NO_MONITOR",
		"RESULT_CACHE",
		"NO_RESULT_CACHE",
	}

	result := sql
	for _, hint := range unsupportedHints {
		re := regexp.MustCompile(`(?i)\s*\+\s*` + regexp.QuoteMeta(hint) + `\s*\(\s*[^)]*\s*\)`)
		result = re.ReplaceAllString(result, "")
		// Also handle hint without parentheses
		re2 := regexp.MustCompile(`(?i)\s*\+\s*` + regexp.QuoteMeta(hint) + `\s*([\s\*/]+)`)
		result = re2.ReplaceAllString(result, "$1")
	}

	// Clean up empty hints: /*+  */ -> /*+ */ or remove entirely
	result = regexp.MustCompile(`(?s)/\*\+\s*\*/`).ReplaceAllString(result, "")
	// Clean up hints with only spaces: /*+    */ -> remove
	result = regexp.MustCompile(`(?s)/\*\+\s+?\*/`).ReplaceAllString(result, " ")

	return result
}

// convertDoubleQuotesToBackticks converts Oracle "identifier" to MySQL `identifier`
func convertDoubleQuotesToBackticks(sql string) string {
	// We need to be careful not to convert double-quoted strings
	// In Oracle, "identifier" is for identifiers, 'string' is for strings
	// This function assumes the input is valid Oracle SQL where "" means identifier

	var result strings.Builder
	i := 0
	inSingleQuote := false

	for i < len(sql) {
		ch := sql[i]

		if ch == '\'' && !inSingleQuote {
			inSingleQuote = true
			result.WriteByte(ch)
			i++
			continue
		}
		if ch == '\'' && inSingleQuote {
			// Check for escaped quote ''
			if i+1 < len(sql) && sql[i+1] == '\'' {
				result.WriteString("''")
				i += 2
				continue
			}
			inSingleQuote = false
			result.WriteByte(ch)
			i++
			continue
		}

		if !inSingleQuote && ch == '"' {
			result.WriteByte('`')
			i++
			// Read until closing quote
			for i < len(sql) {
				if sql[i] == '"' {
					result.WriteByte('`')
					i++
					break
				}
				result.WriteByte(sql[i])
				i++
			}
			continue
		}

		result.WriteByte(ch)
		i++
	}

	return result.String()
}

// convertFunctions converts Oracle functions to MySQL equivalents
func convertFunctions(sql string) string {
	result := sql

	// SYSDATE -> NOW()
	result = regexp.MustCompile(`(?i)\bSYSDATE\b`).ReplaceAllString(result, "NOW()")

	// NVL(x, y) -> IFNULL(x, y)
	result = convertTwoArgFunction(result, "NVL", "IFNULL")

	// NVL2(x, y, z) -> IF(x IS NOT NULL, y, z)
	result = convertNVL2(result)

	// DECODE(expr, search1, result1, search2, result2, ..., default)
	// -> CASE expr WHEN search1 THEN result1 WHEN search2 THEN result2 ELSE default END
	result = convertDecode(result)

	// TO_CHAR(expr, format) -> handle various formats
	result = convertToChar(result)

	// TO_DATE(expr, format) -> STR_TO_DATE(expr, format)
	result = convertToDate(result)

	// TO_NUMBER(expr) -> CAST(expr AS SIGNED) or CONVERT(expr, SIGNED)
	result = regexp.MustCompile(`(?i)\bTO_NUMBER\s*\(\s*([^)]+)\s*\)`).ReplaceAllString(result, "CAST($1 AS SIGNED)")

	// TRUNC(date) -> DATE(NOW())
	result = convertTrunc(result)

	// TRUNC(number, n) -> TRUNCATE(number, n) - MySQL uses TRUNCATE
	// Already compatible in this case, but Oracle uses TRUNC for both

	// ADD_MONTHS(date, n) -> DATE_ADD(date, INTERVAL n MONTH)
	result = convertAddMonths(result)

	// MONTHS_BETWEEN(date1, date2) -> TIMESTAMPDIFF(MONTH, date2, date1)
	result = convertMonthsBetween(result)

	// LAST_DAY(date) -> LAST_DAY(date) - MySQL also supports this
	// No conversion needed

	// INSTR(str, substr) - MySQL supports INSTR with same 2-arg form
	// Oracle 3-arg form: INSTR(str, substr, position) -> LOCATE(substr, str, position)
	result = convertInstr(result)

	// SUBSTR(str, pos, len) -> SUBSTRING(str, pos, len)
	// MySQL supports SUBSTR as alias, no conversion needed

	// REPLACE(str, search, replace) - both support, no conversion needed

	// Fun_ChkCardId(...) - custom function, keep as-is but note it needs to exist in TiDB

	return result
}

// convertTwoArgFunction converts a 2-arg Oracle function to MySQL equivalent
func convertTwoArgFunction(sql, oracleFunc, mysqlFunc string) string {
	re := regexp.MustCompile(fmt.Sprintf(`(?i)\b%s\s*\(`, regexp.QuoteMeta(oracleFunc)))
	return re.ReplaceAllString(sql, mysqlFunc+"(")
}

// convertNVL2 converts NVL2(x, y, z) to IF(x IS NOT NULL, y, z)
func convertNVL2(sql string) string {
	// Simple pattern matching for NVL2
	re := regexp.MustCompile(`(?i)\bNVL2\s*\(`)
	result := re.ReplaceAllString(sql, "NVL2_PLACEHOLDER_(")

	// Find and replace each NVL2 occurrence
	var output strings.Builder
	i := 0
	upper := strings.ToUpper(result)

	for i < len(result) {
		idx := strings.Index(upper[i:], "NVL2_PLACEHOLDER_(")
		if idx < 0 {
			output.WriteString(result[i:])
			break
		}

		output.WriteString(result[i : i+idx])

		// Find the matching closing paren
		start := i + idx + len("NVL2_PLACEHOLDER_(")
		parenDepth := 1
		j := start
		inStr := false
		for j < len(result) && parenDepth > 0 {
			ch := result[j]
			if ch == '\'' && !inStr {
				inStr = true
			} else if ch == '\'' && inStr {
				if j+1 < len(result) && result[j+1] == '\'' {
					j++ // skip escaped quote
				} else {
					inStr = false
				}
			} else if !inStr {
				if ch == '(' {
					parenDepth++
				} else if ch == ')' {
					parenDepth--
				}
			}
			j++
		}

		args := result[start : j-1]
		parts := splitFunctionArgs(args, 3)
		if len(parts) == 3 {
			output.WriteString(fmt.Sprintf("IF(%s IS NOT NULL, %s, %s)", parts[0], parts[1], parts[2]))
		} else {
			// Can't parse, leave as-is
			output.WriteString(result[i+idx : j])
		}
		i = j
		if i >= len(sql) {
			break
		}
	}

	return output.String()
}

// convertDecode converts Oracle DECODE to CASE WHEN
func convertDecode(sql string) string {
	var output strings.Builder
	i := 0
	upper := strings.ToUpper(sql)

	for i < len(sql) {
		idx := strings.Index(upper[i:], "DECODE(")
		if idx < 0 {
			output.WriteString(sql[i:])
			break
		}

		// Make sure it's a standalone DECODE, not part of another word
		if idx > 0 && isWordChar(sql[i+idx-1]) {
			output.WriteString(sql[i : i+idx+6])
			i = i + idx + 6
			continue
		}

		output.WriteString(sql[i : i+idx])

		// Find the matching closing paren
		start := i + idx + 7 // after "DECODE("
		parenDepth := 1
		j := start
		inStr := false
		for j < len(sql) && parenDepth > 0 {
			ch := sql[j]
			if ch == '\'' && !inStr {
				inStr = true
			} else if ch == '\'' && inStr {
				if j+1 < len(sql) && sql[j+1] == '\'' {
					j++
				} else {
					inStr = false
				}
			} else if !inStr {
				if ch == '(' {
					parenDepth++
				} else if ch == ')' {
					parenDepth--
				}
			}
			j++
		}

		args := sql[start : j-1]
		parts := splitFunctionArgs(args, 100)
		if len(parts) >= 3 {
			expr := parts[0]
			var caseBuf strings.Builder
			caseBuf.WriteString("CASE ")
			caseBuf.WriteString(expr)
			caseBuf.WriteString(" ")

			k := 1
			for k+1 < len(parts) {
				caseBuf.WriteString(fmt.Sprintf("WHEN %s THEN %s ", parts[k], parts[k+1]))
				k += 2
			}
			// Default value (odd number of remaining args)
			if k < len(parts) {
				caseBuf.WriteString(fmt.Sprintf("ELSE %s ", parts[k]))
			}
			caseBuf.WriteString("END")
			output.WriteString(caseBuf.String())
		} else {
			output.WriteString(sql[i+idx : j])
		}
		i = j
		if i >= len(sql) {
			break
		}
	}

	return output.String()
}

// convertToChar converts Oracle TO_CHAR to MySQL equivalent
func convertToChar(sql string) string {
	return convertFunctionCall(sql, "TO_CHAR", 8, func(args []string) string {
		if len(args) == 2 {
			format := strings.Trim(strings.TrimSpace(args[1]), "'\"")
			mysqlFormat := oracleDateFormatToMySQL(format)
			return fmt.Sprintf("DATE_FORMAT(%s, '%s')", args[0], mysqlFormat)
		} else if len(args) == 1 {
			return fmt.Sprintf("CAST(%s AS CHAR)", args[0])
		}
		return "TO_CHAR(" + strings.Join(args, ", ") + ")"
	})
}

// convertFunctionCall is a generic helper that finds and converts function calls
func convertFunctionCall(sql, funcName string, funcNameLen int, converter func([]string) string) string {
	var output strings.Builder
	i := 0

	for i < len(sql) {
		idx := indexOfCaseInsensitive(sql[i:], funcName+"(")
		if idx < 0 {
			output.WriteString(sql[i:])
			break
		}

		// Check it's a standalone function name
		if idx > 0 && isWordChar(sql[i+idx-1]) {
			output.WriteString(sql[i : i+idx+funcNameLen])
			i = i + idx + funcNameLen
			if i >= len(sql) {
				break
			}
			continue
		}

		output.WriteString(sql[i : i+idx])

		start := i + idx + funcNameLen + 1 // after "FUNC_NAME("
		if start >= len(sql) {
			output.WriteString(funcName + "(")
			break
		}

		// Find matching closing paren
		end := findMatchingParen(sql, start)
		if end < 0 {
			output.WriteString(funcName + "(")
			i = start
			if i >= len(sql) {
				break
			}
			continue
		}

		argsStr := sql[start:end]
		parts := splitFunctionArgs(argsStr, 10)
		result := converter(parts)
		output.WriteString(result)

		i = end + 1 // skip closing paren
		if i >= len(sql) {
			break
		}
	}

	return output.String()
}

// findMatchingParen finds the matching closing parenthesis, respecting strings and nesting
func findMatchingParen(sql string, start int) int {
	if start >= len(sql) || sql[start-1] != '(' {
		return -1
	}
	depth := 1
	j := start
	inStr := false
	for j < len(sql) && depth > 0 {
		ch := sql[j]
		if ch == '\'' && !inStr {
			inStr = true
		} else if ch == '\'' && inStr {
			if j+1 < len(sql) && sql[j+1] == '\'' {
				j++
			} else {
				inStr = false
			}
		} else if !inStr {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			}
		}
		j++
	}
	if depth == 0 {
		return j - 1
	}
	return -1
}

// indexOfCaseInsensitive does a case-insensitive search for a substring.
// It works on bytes to avoid issues with multi-byte characters.
func indexOfCaseInsensitive(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	substrBytes := []byte(strings.ToLower(substr))
	for i := 0; i <= len(s)-len(substrBytes); i++ {
		match := true
		for j := 0; j < len(substrBytes); j++ {
			sc := s[i+j]
			// Simple ASCII case-insensitive comparison
			if sc >= 'A' && sc <= 'Z' {
				sc = sc + 32 // toLower
			}
			if sc != substrBytes[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// convertToDate converts Oracle TO_DATE to MySQL STR_TO_DATE
func convertToDate(sql string) string {
	return convertFunctionCall(sql, "TO_DATE", 7, func(args []string) string {
		if len(args) == 2 {
			format := strings.Trim(strings.TrimSpace(args[1]), "'\"")
			mysqlFormat := oracleDateFormatToMySQL(format)
			return fmt.Sprintf("STR_TO_DATE(%s, '%s')", args[0], mysqlFormat)
		} else if len(args) == 1 {
			return fmt.Sprintf("CAST(%s AS DATETIME)", args[0])
		}
		return "TO_DATE(" + strings.Join(args, ", ") + ")"
	})
}

// convertTrunc converts Oracle TRUNC function
func convertTrunc(sql string) string {
	return convertFunctionCall(sql, "TRUNC", 5, func(args []string) string {
		if len(args) == 2 {
			fmtArg := strings.Trim(strings.ToUpper(strings.TrimSpace(args[1])), "'\"")
			switch fmtArg {
			case "YYYY", "YEAR", "YY":
				return fmt.Sprintf("DATE_FORMAT(%s, '%%Y-01-01')", args[0])
			case "MM", "MON", "MONTH":
				return fmt.Sprintf("DATE_FORMAT(%s, '%%Y-%%m-01')", args[0])
			case "DD", "DAY", "J":
				return fmt.Sprintf("DATE(%s)", args[0])
			case "HH", "HH12", "HH24":
				return fmt.Sprintf("DATE_FORMAT(%s, '%%Y-%%m-%%d %%H:00:00')", args[0])
			case "MI":
				return fmt.Sprintf("DATE_FORMAT(%s, '%%Y-%%m-%%d %%H:%%i:00')", args[0])
			default:
				return fmt.Sprintf("TRUNCATE(%s, %s)", args[0], args[1])
			}
		} else if len(args) == 1 {
			return fmt.Sprintf("DATE(%s)", args[0])
		}
		return "TRUNC(" + strings.Join(args, ", ") + ")"
	})
}

// convertAddMonths converts ADD_MONTHS(date, n) to DATE_ADD(date, INTERVAL n MONTH)
func convertAddMonths(sql string) string {
	re := regexp.MustCompile(`(?i)\bADD_MONTHS\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)`)
	return re.ReplaceAllString(sql, "DATE_ADD($1, INTERVAL $2 MONTH)")
}

// convertMonthsBetween converts MONTHS_BETWEEN(date1, date2) to TIMESTAMPDIFF(MONTH, date2, date1)
func convertMonthsBetween(sql string) string {
	re := regexp.MustCompile(`(?i)\bMONTHS_BETWEEN\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)`)
	return re.ReplaceAllString(sql, "TIMESTAMPDIFF(MONTH, $2, $1)")
}

// convertInstr converts Oracle INSTR to MySQL LOCATE for 3+ arg form
func convertInstr(sql string) string {
	// INSTR(str, substr, pos, [occurrence])
	// MySQL LOCATE(substr, str, pos) - args are reversed!
	// 2-arg: INSTR(str, substr) = LOCATE(substr, str) = INSTR(str, substr) in MySQL (compatible)
	// 3-arg: INSTR(str, substr, pos) -> LOCATE(substr, str, pos)

	var output strings.Builder
	i := 0
	upper := strings.ToUpper(sql)

	for i < len(sql) {
		idx := strings.Index(upper[i:], "INSTR(")
		if idx < 0 {
			output.WriteString(sql[i:])
			break
		}

		if idx > 0 && isWordChar(sql[i+idx-1]) {
			output.WriteString(sql[i : i+idx+5])
			i = i + idx + 5
			continue
		}

		output.WriteString(sql[i : i+idx])

		start := i + idx + 6
		parenDepth := 1
		j := start
		inStr := false
		for j < len(sql) && parenDepth > 0 {
			ch := sql[j]
			if ch == '\'' && !inStr {
				inStr = true
			} else if ch == '\'' && inStr {
				if j+1 < len(sql) && sql[j+1] == '\'' {
					j++
				} else {
					inStr = false
				}
			} else if !inStr {
				if ch == '(' {
					parenDepth++
				} else if ch == ')' {
					parenDepth--
				}
			}
			j++
		}

		args := sql[start : j-1]
		parts := splitFunctionArgs(args, 4)

		if len(parts) >= 3 {
			// 3+ args: INSTR(str, substr, pos) -> LOCATE(substr, str, pos)
			output.WriteString(fmt.Sprintf("LOCATE(%s, %s, %s)", parts[1], parts[0], parts[2]))
		} else {
			// 2 args or less: compatible, keep as-is
			output.WriteString(sql[i+idx : j])
		}
		i = j
		if i >= len(sql) {
			break
		}
	}

	return output.String()
}

// convertConcatOperator converts || string concatenation to CONCAT()
// convertConcatOperator converts || string concatenation to CONCAT()
// Uses a two-pass approach: first handles top-level ||, then processes || inside parentheses
func convertConcatOperator(sql string) string {
	// First pass: convert || at paren depth 0 (top-level expressions)
	result := simpleConcatConvert(sql)

	// Second pass: repeatedly find and convert || inside innermost parentheses
	// Process from innermost to outermost to handle nested cases correctly
	for {
		// Find innermost (...) containing || but not nested parens
		re := regexp.MustCompile(`\(([^()]*\|\|[^()]*)\)`)
		loc := re.FindStringIndex(result)
		if loc == nil {
			break
		}
		inner := result[loc[0]+1 : loc[1]-1]
		converted := simpleConcatConvert(inner)
		result = result[:loc[0]] + "(" + converted + ")" + result[loc[1]:]
	}

	return result
}

// simpleConcatConvert handles || to CONCAT() conversion at a single nesting level.
// Only converts || at paren depth 0 of the input string.
func simpleConcatConvert(sql string) string {
	var result strings.Builder
	var segments []string
	currentSeg := strings.Builder{}
	inQuote := false
	parenDepth := 0
	hasConcat := false

	flushSegments := func() {
		if len(segments) > 1 {
			result.WriteString("CONCAT(" + strings.Join(segments, ", ") + ")")
		} else if len(segments) == 1 {
			result.WriteString(segments[0])
		}
		segments = nil
		hasConcat = false
	}

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		if ch == '\'' && !inQuote {
			inQuote = true
			currentSeg.WriteByte(ch)
			continue
		}
		if ch == '\'' && inQuote {
			if i+1 < len(sql) && sql[i+1] == '\'' {
				currentSeg.WriteString("''")
				i++
				continue
			}
			inQuote = false
			currentSeg.WriteByte(ch)
			continue
		}
		if inQuote {
			currentSeg.WriteByte(ch)
			continue
		}
		if ch == '(' {
			parenDepth++
			currentSeg.WriteByte(ch)
			continue
		}
		if ch == ')' {
			if parenDepth > 0 {
				parenDepth--
			}
			if parenDepth == 0 && hasConcat {
				segments = append(segments, strings.TrimSpace(currentSeg.String()))
				currentSeg.Reset()
				flushSegments()
			} else {
				currentSeg.WriteByte(ch)
			}
			continue
		}

		// Only convert || at paren depth 0
		if ch == '|' && i+1 < len(sql) && sql[i+1] == '|' && parenDepth == 0 {
			hasConcat = true
			segments = append(segments, strings.TrimSpace(currentSeg.String()))
			currentSeg.Reset()
			i++
			continue
		}

		if hasConcat && parenDepth == 0 && (ch == ',' || ch == ';') {
			segments = append(segments, strings.TrimSpace(currentSeg.String()))
			currentSeg.Reset()
			flushSegments()
			result.WriteByte(ch)
			continue
		}

		currentSeg.WriteByte(ch)
	}

	if hasConcat {
		segments = append(segments, strings.TrimSpace(currentSeg.String()))
		flushSegments()
	} else {
		result.WriteString(currentSeg.String())
	}

	return result.String()
}

// convertSequenceRefs converts Oracle sequence references to placeholder values
// seq.NEXTVAL -> (SELECT AUTO_INCREMENT_VALUE or placeholder)
// seq.CURRVAL -> (SELECT AUTO_INCREMENT_VALUE or placeholder)
func convertSequenceRefs(sql string) string {
	// .NEXTVAL -> a placeholder that can be filled
	result := regexp.MustCompile(`(?i)(\w+)\.NEXTVAL`).ReplaceAllString(sql, "(SELECT 1)")

	// .CURRVAL -> a placeholder
	result = regexp.MustCompile(`(?i)(\w+)\.CURRVAL`).ReplaceAllString(result, "(SELECT 1)")

	return result
}

// convertFromDual removes unnecessary FROM DUAL
func convertFromDual(sql string) string {
	// SELECT ... FROM DUAL -> SELECT ...
	// But keep if it's SELECT ... INTO ... FROM DUAL (unlikely in AWR)
	re := regexp.MustCompile(`(?i)\s+FROM\s+DUAL\s*$`)
	return re.ReplaceAllString(sql, "")
}

// convertOracleDataTypes converts Oracle-specific data types to MySQL equivalents
func convertOracleDataTypes(sql string) string {
	// VARCHAR2(n) -> VARCHAR(n)
	result := regexp.MustCompile(`(?i)\bVARCHAR2\b`).ReplaceAllString(sql, "VARCHAR")

	// NUMBER(p,s) -> DECIMAL(p,s) or INT for special cases
	// NUMBER conversion - order matters: do the most specific patterns first
	// NUMBER(p,s) -> DECIMAL(p,s)
	result = regexp.MustCompile(`(?i)\bNUMBER\s*\(\s*\d+\s*,\s*\d+\s*\)`).ReplaceAllStringFunc(result, func(match string) string {
		re := regexp.MustCompile(`(?i)\bNUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)`)
		parts := re.FindStringSubmatch(match)
		if len(parts) == 3 {
			return "DECIMAL(" + parts[1] + "," + parts[2] + ")"
		}
		return match
	})
	// NUMBER(p) -> INT or BIGINT
	result = regexp.MustCompile(`(?i)\bNUMBER\s*\(\s*\d+\s*\)`).ReplaceAllStringFunc(result, func(match string) string {
		re := regexp.MustCompile(`(?i)\bNUMBER\s*\(\s*(\d+)\s*\)`)
		parts := re.FindStringSubmatch(match)
		if len(parts) == 2 {
			switch parts[1] {
			case "1", "2", "3", "4", "5", "6", "7", "8", "9", "10":
				return "INT"
			default:
				return "BIGINT"
			}
		}
		return match
	})
	// Bare NUMBER -> DECIMAL(65,30)
	result = regexp.MustCompile(`(?i)\bNUMBER\b`).ReplaceAllString(result, "DECIMAL(65,30)")

	// BINARY_INTEGER -> INT
	result = regexp.MustCompile(`(?i)\bBINARY_INTEGER\b`).ReplaceAllString(result, "INT")

	// PLS_INTEGER -> INT
	result = regexp.MustCompile(`(?i)\bPLS_INTEGER\b`).ReplaceAllString(result, "INT")

	// TIMESTAMP WITH TIME ZONE -> DATETIME
	result = regexp.MustCompile(`(?i)\bTIMESTAMP\s+WITH\s+TIME\s+ZONE\b`).ReplaceAllString(result, "DATETIME")

	// TIMESTAMP WITH LOCAL TIME ZONE -> DATETIME
	result = regexp.MustCompile(`(?i)\bTIMESTAMP\s+WITH\s+LOCAL\s+TIME\s+ZONE\b`).ReplaceAllString(result, "DATETIME")

	// RAW(n) -> VARBINARY(n)
	result = regexp.MustCompile(`(?i)\bRAW\s*\(`).ReplaceAllString(result, "VARBINARY(")

	return result
}

// convertRowNum converts Oracle ROWNUM to LIMIT
func convertRowNum(sql string) string {
	// Simple case: WHERE ROWNUM <= n -> add LIMIT n
	// Complex cases with ROWNUM in subqueries are harder
	// For now, handle the simple WHERE ROWNUM <= n case

	upper := strings.ToUpper(sql)

	// Check if ROWNUM is used in WHERE clause
	if !strings.Contains(upper, "ROWNUM") {
		return sql
	}

	// Pattern: WHERE ... ROWNUM <= n or WHERE ROWNUM < n
	re := regexp.MustCompile(`(?i)\bROWNUM\s*(<=|<|=)\s*(\d+)`)
	match := re.FindStringSubmatch(sql)
	if match != nil {
		limit := match[2]
		op := strings.TrimSpace(match[1])
		if op == "<" {
			n, _ := fmt.Sscanf(limit, "%d", new(int))
			_ = n
			// ROWNUM < n -> LIMIT n-1
			limit = fmt.Sprintf("%d", parseInt(limit)-1)
		}
		// Remove the ROWNUM condition from WHERE
		result := re.ReplaceAllString(sql, "")
		// Add LIMIT at the end (before ; if present)
		if strings.HasSuffix(strings.TrimSpace(result), ";") {
			result = strings.TrimSpace(result)
			result = result[:len(result)-1] + " LIMIT " + limit + ";"
		} else {
			result = result + " LIMIT " + limit
		}
		return result
	}

	// More complex ROWNUM usage - remove and note
	// For subqueries with ROWNUM, we can't easily convert
	return regexp.MustCompile(`(?i)\bROWNUM\b`).ReplaceAllString(sql, "1=1 /* ROWNUM not supported */")
}

// convertRowIDConditions removes or replaces ROWID references
func convertRowIDConditions(sql string) string {
	// ROWID is Oracle-specific
	// Simple removal for WHERE ROWID BETWEEN :B2 AND :B1
	result := regexp.MustCompile(`(?i)\s+WHERE\s+ROWID\s+BETWEEN\s+:\S+\s+AND\s+:\S+`).ReplaceAllString(sql, " WHERE 1=0")

	// alias.ROWID BETWEEN :B2 AND :B1
	result = regexp.MustCompile(`(?i)\s+WHERE\s+\w+\.ROWID\s+BETWEEN\s+:\S+\s+AND\s+:\S+`).ReplaceAllString(result, " WHERE 1=0")

	// ROWID = :B1
	result = regexp.MustCompile(`(?i)\s+ROWID\s*=\s*:\S+`).ReplaceAllString(result, " 1=0")

	// alias.ROWID = :B1
	result = regexp.MustCompile(`(?i)\s+\w+\.ROWID\s*=\s*:\S+`).ReplaceAllString(result, " 1=0")

	// .ROWID references in subqueries - replace with 1
	result = regexp.MustCompile(`(?i)(\w+)\.ROWID\b`).ReplaceAllString(result, "1")

	// Standalone ROWID references (not already handled) - replace with 1
	// Use word boundary to avoid matching inside comments
	result = regexp.MustCompile(`(?i)\bROWID\b`).ReplaceAllString(result, "1")

	return result
}

// convertOuterJoinPlus converts Oracle (+) outer join syntax to ANSI JOIN
// This is a simplified implementation for common cases
func convertOuterJoinPlus(sql string) string {
	// Oracle: WHERE a.id = b.id(+)
	// MySQL: LEFT JOIN b ON a.id = b.id

	// This is complex and requires understanding the table structure
	// For now, mark (+) as a comment and note the limitation
	result := regexp.MustCompile(`\(\s*\+\s*\)`).ReplaceAllString(sql, " /* (+) outer join - manual conversion needed */")

	return result
}

// convertSelectSpecific handles SELECT-specific conversions
func convertSelectSpecific(sql string) string {
	// Oracle: SELECT ... FROM table WHERE ROWNUM <= n ORDER BY ...
	// MySQL needs: SELECT ... FROM table ORDER BY ... LIMIT n
	// (ROWNUM is handled in convertRowNum, but order matters)

	return sql
}

// convertDateLiterals converts Oracle date literal formats
func convertDateLiterals(sql string) string {
	// Oracle: DATE 'YYYY-MM-DD' -> 'YYYY-MM-DD'
	result := regexp.MustCompile(`(?i)\bDATE\s+'`).ReplaceAllString(sql, "'")

	// Oracle: TIMESTAMP 'YYYY-MM-DD HH24:MI:SS' -> 'YYYY-MM-DD HH24:MI:SS'
	result = regexp.MustCompile(`(?i)\bTIMESTAMP\s+'`).ReplaceAllString(result, "'")

	return result
}

// ==================== Oracle Date Format to MySQL Format ====================

func oracleDateFormatToMySQL(format string) string {
	result := format
	// Year
	result = strings.ReplaceAll(result, "YYYY", "%Y")
	result = strings.ReplaceAll(result, "YYY", "%Y")
	result = strings.ReplaceAll(result, "YY", "%y")
	// Month
	result = strings.ReplaceAll(result, "MM", "%m")
	result = strings.ReplaceAll(result, "MON", "%b")
	result = strings.ReplaceAll(result, "MONTH", "%M")
	// Day
	result = strings.ReplaceAll(result, "DD", "%d")
	result = strings.ReplaceAll(result, "DY", "%a")
	result = strings.ReplaceAll(result, "DAY", "%W")
	// Hour
	result = strings.ReplaceAll(result, "HH24", "%H")
	result = strings.ReplaceAll(result, "HH12", "%h")
	result = strings.ReplaceAll(result, "HH", "%h")
	// Minute
	result = strings.ReplaceAll(result, "MI", "%i")
	// Second
	result = strings.ReplaceAll(result, "SS", "%s")
	// AM/PM
	result = strings.ReplaceAll(result, "AM", "%p")
	result = strings.ReplaceAll(result, "PM", "%p")

	return result
}

// removePartitionClauses removes Oracle PARTITION(name) clauses from table references
// TiDB doesn't support partition pruning syntax in queries on non-partitioned tables
func removePartitionClauses(sql string) string {
	return regexp.MustCompile(`(?i)\s+PARTITION\s*\(\s*\w+\s*\)`).ReplaceAllString(sql, "")
}

// removeDBLinks removes Oracle database link references (@dblink_name)
// Oracle: SELECT * FROM schema.table@dblink WHERE ...
// MySQL: SELECT * FROM schema.table WHERE ...
func removeDBLinks(sql string) string {
	// Pattern: @`dbname` or @dbname after a table reference
	// Be careful not to remove @ in email addresses or other contexts
	return regexp.MustCompile(`(?i)@` + "`" + `(\w+)` + "`").ReplaceAllString(sql, "")
}

// addDerivedTableAliases ensures every derived table (subquery in FROM) has an alias.
// MySQL/TiDB requires: SELECT * FROM (SELECT ...) AS alias
// Oracle allows: SELECT * FROM (SELECT ...) WHERE ...
func addDerivedTableAliases(sql string) string {
	var result strings.Builder
	inQuote := false
	i := 0

	// Track positions of ( and ) to detect FROM subqueries
	// Strategy: look backwards from each ) to see if it was preceded by FROM
	type parenPos struct {
		index    int
		isFrom  bool // true if preceded by FROM
		depth   int
	}
	var openParens []parenPos

	for i < len(sql) {
		ch := sql[i]

		if ch == '\'' && !inQuote {
			inQuote = true
			result.WriteByte(ch)
			i++
			continue
		}
		if ch == '\'' && inQuote {
			result.WriteByte(ch)
			if i+1 < len(sql) && sql[i+1] == '\'' {
				result.WriteByte(ch)
				i += 2
			} else {
				inQuote = false
				i++
			}
			continue
		}
		if inQuote {
			result.WriteByte(ch)
			i++
			continue
		}

		if ch == '(' {
			// Check if this ( is preceded by FROM (indicating a derived table)
			isFrom := false
			// Look backwards to find the word before this (
			j := result.Len() - 1
			for j >= 0 && (result.String()[j] == ' ' || result.String()[j] == '\t' || result.String()[j] == '\n') {
				j--
			}
			if j >= 4 {
				prev := strings.ToUpper(result.String()[j-4 : j+1])
				if prev == "FROM" || (j >= 3 && result.String()[j-3:j+1] == "FROM") {
					isFrom = true
				}
			}
			openParens = append(openParens, parenPos{index: i, isFrom: isFrom, depth: len(openParens)})
			result.WriteByte(ch)
			i++
			continue
		}

		if ch == ')' {
			result.WriteByte(ch)
			i++

			if len(openParens) > 0 {
				popped := openParens[len(openParens)-1]
				openParens = openParens[:len(openParens)-1]

				// If this ) closes a FROM subquery, check if it has an alias
				if popped.isFrom {
					if i < len(sql) {
						// Skip whitespace
						j := i
						for j < len(sql) && (sql[j] == ' ' || sql[j] == '\t' || sql[j] == '\n' || sql[j] == '\r') {
							j++
						}

						if j < len(sql) {
							nextWord := strings.ToUpper(getWordAt(sql, j))
							// Only AS and simple identifiers mean "already has an alias"
							hasAlias := nextWord == "AS" || (isLetter(sql[j]) && !isSQLKeyword(nextWord))
							if !hasAlias {
								result.WriteString(" _subq")
							}
						} else {
							// End of string - needs alias
							result.WriteString(" _subq")
						}
					} else {
						// ) is the last character - needs alias
						result.WriteString(" _subq")
					}
				}
			}
			continue
		}

		result.WriteByte(ch)
		i++
	}

	return result.String()
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isSQLKeyword(word string) bool {
	keywords := map[string]bool{
		"SELECT": true, "FROM": true, "INSERT": true, "UPDATE": true, "DELETE": true,
		"MERGE": true, "CREATE": true, "ALTER": true, "DROP": true, "WITH": true,
		"NOT": true, "NULL": true, "TRUE": true, "FALSE": true, "EXISTS": true,
		"IN": true, "BETWEEN": true, "LIKE": true, "IS": true, "BY": true,
		"ALL": true, "ANY": true, "SOME": true, "AS": true, "DESC": true, "ASC": true,
		"DISTINCT": true, "CASE": true, "WHEN": true, "THEN": true, "ELSE": true,
		"END": true, "IF": true, "IFNULL": true, "NULLIF": true, "COALESCE": true,
		"CONCAT": true, "COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true,
		"NOW": true, "DATE": true, "AND": true, "OR": true, "ON": true,
	}
	return keywords[word]
}

func getWordAt(sql string, pos int) string {
	if pos >= len(sql) {
		return ""
	}
	if !isWordChar(sql[pos]) {
		return ""
	}
	end := pos
	for end < len(sql) && isWordChar(sql[end]) {
		end++
	}
	return strings.ToUpper(sql[pos:end])
}

// stripInsertColumnPrefix removes table alias prefixes from INSERT column lists.
// Oracle: INSERT INTO table (T.col1, T.col2) VALUES (...)
// MySQL:  INSERT INTO table (col1, col2) VALUES (...)
// Also handles: INSERT INTO table ALIAS (T.col1, T.col2) VALUES (...)
// MySQL:       INSERT INTO table (col1, col2) VALUES (...)
func stripInsertColumnPrefix(sql string) string {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(upper, "INSERT") {
		return sql
	}

	// Find INSERT INTO table [alias] (columns)
	// Groups: (1) INSERT INTO table, (2) optional alias, (3) columns
	re := regexp.MustCompile(`(?i)(INSERT\s+INTO\s+[^\s(]+)(\s+\w+)?\s*\(([^)]+)\)`)
	match := re.FindStringSubmatchIndex(sql)
	if match == nil {
		return sql
	}

	tableEnd := match[3] // end of group 1 (INSERT INTO table)
	// Group 2 is optional alias: match[4:6]
	hasAlias := match[4] != -1 && match[5] != -1
	// Group 3 is columns: match[6:8]
	colsStart := match[6]
	colsEnd := match[7]
	cols := sql[colsStart:colsEnd]

	// Strip alias prefix from each column: T.col -> col
	colList := strings.Split(cols, ",")
	for i, col := range colList {
		col = strings.TrimSpace(col)
		if dotIdx := strings.Index(col, "."); dotIdx > 0 {
			colList[i] = col[dotIdx+1:]
		}
	}

	newCols := strings.Join(colList, ", ")

	if hasAlias {
		// Remove the alias, keep table name and columns
		return sql[:tableEnd] + " (" + newCols + sql[colsEnd:]
	}
	return sql[:colsStart] + newCols + sql[colsEnd:]
}

// ==================== AST-based Fallback Converter ====================

// ASTConvertOracleToMySQL attempts AST-based conversion for SQL that failed regex conversion
// This is a fallback for complex cases where regex-based conversion produces invalid SQL
func ASTConvertOracleToMySQL(oracleSQL string) (string, error) {
	// Use TiDB parser to validate the regex-converted SQL
	// If it fails to parse, try AST-based corrections

	// For now, implement a few AST-aware fixes:
	result := oracleSQL

	// 1. Fix MERGE statement syntax for TiDB
	result = fixMergeSyntax(result)

	// 2. Fix subquery in FROM clause
	result = fixSubqueries(result)

	// 3. Fix CASE expressions
	result = fixCaseExpressions(result)

	return result, nil
}

// fixMergeSyntax adjusts MERGE statement for TiDB compatibility
func fixMergeSyntax(sql string) string {
	upper := strings.ToUpper(sql)
	if !strings.HasPrefix(strings.TrimSpace(upper), "MERGE") {
		return sql
	}

	// TiDB MERGE requires explicit column list in INSERT clause
	// Oracle: WHEN NOT MATCHED THEN INSERT VALUES (...)
	// TiDB: WHEN NOT MATCHED THEN INSERT (col1, col2, ...) VALUES (...)

	// Also ensure the USING subquery doesn't have ROWID references
	result := regexp.MustCompile(`(?i)\bROWID\b`).ReplaceAllString(sql, "_ROWID_")

	return result
}

// fixSubqueries ensures subqueries are properly parenthesized
func fixSubqueries(sql string) string {
	// MySQL requires subqueries in FROM clause to have an alias
	// Oracle: SELECT * FROM (SELECT ...) WHERE ...
	// MySQL: SELECT * FROM (SELECT ...) AS t WHERE ...
	// (but MySQL is actually OK with unaliased subqueries in some contexts)

	return sql
}

// fixCaseExpressions ensures CASE expressions are properly formatted
func fixCaseExpressions(sql string) string {
	// Check for balanced CASE/END pairs
	caseCount := strings.Count(strings.ToUpper(sql), "CASE ")
	endCount := strings.Count(strings.ToUpper(sql), " END")

	if caseCount > endCount {
		// Add missing END
		sql = sql + " END"
	}

	return sql
}

// ==================== Utility Functions ====================

// cleanWhitespace normalizes whitespace in SQL
func cleanWhitespace(sql string) string {
	// Replace tabs and multiple spaces with single space
	result := strings.ReplaceAll(sql, "\t", " ")
	result = strings.ReplaceAll(result, "\r\n", " ")
	result = strings.ReplaceAll(result, "\n", " ")

	// Compress multiple spaces to single space, but preserve string literals
	var output strings.Builder
	inQuote := false
	prevSpace := false

	for i := 0; i < len(result); i++ {
		ch := result[i]
		if ch == '\'' {
			if inQuote && i+1 < len(result) && result[i+1] == '\'' {
				output.WriteByte(ch)
				output.WriteByte(ch)
				i++
				prevSpace = false
				continue
			}
			inQuote = !inQuote
			output.WriteByte(ch)
			prevSpace = false
			continue
		}

		if inQuote {
			output.WriteByte(ch)
			prevSpace = false
			continue
		}

		if ch == ' ' {
			if prevSpace {
				continue
			}
			prevSpace = true
			output.WriteByte(ch)
			continue
		}
		prevSpace = false
		output.WriteByte(ch)
	}

	return strings.TrimSpace(output.String())
}

func isWordChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_'
}

func parseInt(s string) int {
	n := 0
	for _, c := range strings.TrimSpace(s) {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		}
	}
	return n
}

// splitFunctionArgs splits function arguments respecting nesting and strings
func splitFunctionArgs(args string, maxArgs int) []string {
	var parts []string
	current := strings.Builder{}
	depth := 0
	inQuote := false

	for i := 0; i < len(args); i++ {
		ch := args[i]

		if ch == '\'' && !inQuote {
			inQuote = true
			current.WriteByte(ch)
			continue
		}
		if ch == '\'' && inQuote {
			if i+1 < len(args) && args[i+1] == '\'' {
				current.WriteString("''")
				i++
				continue
			}
			inQuote = false
			current.WriteByte(ch)
			continue
		}

		if inQuote {
			current.WriteByte(ch)
			continue
		}

		if ch == '(' {
			depth++
			current.WriteByte(ch)
			continue
		}
		if ch == ')' {
			depth--
			current.WriteByte(ch)
			continue
		}

		if ch == ',' && depth == 0 {
			if maxArgs > 0 && len(parts) >= maxArgs-1 {
				// Last argument absorbs the rest
				current.WriteByte(ch)
				continue
			}
			parts = append(parts, current.String())
			current.Reset()
			continue
		}

		current.WriteByte(ch)
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// ValidateMySQLSyntax uses TiDB parser to check if SQL is valid MySQL syntax
func ValidateMySQLSyntax(sql string) bool {
	_, _, err := parser.New().Parse(sql, "", "")
	return err == nil
}

// convertBindVariables converts Oracle bind variables to MySQL placeholders
// Oracle: :B1, :1, :name -> MySQL: ?
// Only converts outside of string literals
func convertBindVariables(sql string) string {
	var output strings.Builder
	inQuote := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' && !inQuote {
			inQuote = true
			output.WriteByte(ch)
			continue
		}
		if ch == '\'' && inQuote {
			if i+1 < len(sql) && sql[i+1] == '\'' {
				output.WriteString("''")
				i++
				continue
			}
			inQuote = false
			output.WriteByte(ch)
			continue
		}
		if !inQuote && ch == ':' {
			// Check if followed by a digit or letter (Oracle bind var)
			if i+1 < len(sql) && isBindVarChar(sql[i+1]) {
				// Skip the entire bind variable name
				i++
				for i < len(sql) && isBindVarChar(sql[i]) {
					i++
				}
				i-- // back up one since the loop will increment
				output.WriteString("?")
				continue
			}
		}
		output.WriteByte(ch)
	}
	return output.String()
}

func isBindVarChar(ch byte) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

// convertUnsupportedFunctions handles Oracle-specific functions that have no MySQL equivalent
func convertUnsupportedFunctions(sql string) string {
	// SYS_CONTEXT('USERENV', ...) -> replace with placeholder values
	result := regexp.MustCompile(`(?i)SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'([^']+)'\s*\)`).ReplaceAllStringFunc(sql, func(match string) string {
		re := regexp.MustCompile(`(?i)SYS_CONTEXT\s*\(\s*'USERENV'\s*,\s*'([^']+)'\s*\)`)
		parts := re.FindStringSubmatch(match)
		if len(parts) > 1 {
			switch strings.ToUpper(parts[1]) {
			case "SERVER_HOST":
				return "'localhost'"
			case "DB_UNIQUE_NAME":
				return "'tidb'"
			case "INSTANCE_NAME":
				return "'tidb'"
			case "SESSION_USER":
				return "USER()"
			case "CURRENT_USER":
				return "CURRENT_USER()"
			case "CON_ID":
				return "1"
			default:
				return "'unknown'"
			}
		}
		return "'unknown'"
	})

	// TABLE(PKG_NAME.FUNCTION(...)) - Oracle pipelined function, replace with empty result
	// This is very Oracle-specific and can't be converted
	result = regexp.MustCompile(`(?i)\bTABLE\s*\(\s*\w+\.\w+\s*\(`).ReplaceAllString(result, "(SELECT 1 WHERE 1=0 /* Oracle pipelined function */ UNION SELECT ")

	// zh_concat - Oracle-specific aggregation, replace with GROUP_CONCAT
	result = regexp.MustCompile(`(?i)\bZH_CONCAT\s*\(`).ReplaceAllString(result, "GROUP_CONCAT(")

	// Fun_ChkCardId - custom function, keep as-is (needs to exist in TiDB)

	// SYS_GUID() -> UUID() or (SELECT UUID())
	result = regexp.MustCompile(`(?i)\bSYS_GUID\s*\(\s*\)`).ReplaceAllString(result, "(SELECT UUID())")

	// Oracle database link syntax: table@dblink -> table (remove dblink)
	result = regexp.MustCompile(`(\w+)\s*@\s*\w+`).ReplaceAllStringFunc(result, func(match string) string {
		re := regexp.MustCompile(`(\w+)\s*@\s*(\w+)`)
		parts := re.FindStringSubmatch(match)
		if len(parts) == 3 {
			return parts[1]
		}
		return match
	})

	// REGEXP_LIKE(expr, pattern, match_param) -> expr REGEXP pattern
	// Oracle: REGEXP_LIKE(A.ZDSJ, '^[0-9]{8}$')  -> MySQL: A.ZDSJ REGEXP '^[0-9]{8}$'
	result = regexp.MustCompile(`(?i)\bREGEXP_LIKE\s*\(\s*([^,]+)\s*,\s*'([^']*)'\s*(?:,\s*'[^']*'\s*)?\)`).ReplaceAllString(result, "$1 REGEXP '$2'")

	// BITAND(x, n) -> x & n (MySQL uses & for bitwise AND)
	result = regexp.MustCompile(`(?i)\bBITAND\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)`).ReplaceAllString(result, "($1 & $2)")

	return result
}
