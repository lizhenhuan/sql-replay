package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
)

func ParseTiDBLogs(slowLogPath, slowOutputPath string) {
	if slowLogPath == "" || slowOutputPath == "" {
		fmt.Println("Usage: ./sql-replay -mode parsetidbslow -slow-in <path_to_slow_query_log> -slow-out <path_to_slow_output_file>")
		return
	}

	outputFile, err := os.Create(slowOutputPath)
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile.Close()

	writer := bufio.NewWriterSize(outputFile, 8*1024*1024)
	defer writer.Flush()

	file, err := os.Open(slowLogPath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	bufferSize := 1024 * 1024 * 10
	scanner := bufio.NewScanner(file)
	buf := make([]byte, bufferSize)
	scanner.Buffer(buf, bufferSize)

	var entry LogEntry
	var isInternal bool
	var sqlStatement strings.Builder
	var isPrepared string
	entryCount := 0

	timeRegex := regexp.MustCompile(`# Time:\s+(\d+-\d+-\d+T\d+:\d+:\d+\.\d+[+-]\d+:\d+)`)
	userHostRegex := regexp.MustCompile(`# User@Host:\s+(\w+)`)
	connIDRegex := regexp.MustCompile(`# Conn_ID:\s+(\d+)`)
	queryTimeRegex := regexp.MustCompile(`# Query_time:\s+(\d+\.\d+)`)
	dbRegex := regexp.MustCompile(`# DB:\s+(\w+)`)
	isInternalRegex := regexp.MustCompile(`# Is_internal:\s+(true|false)`)
	preparedRegex := regexp.MustCompile(`# Prepared:\s+(true|false)`)

	flushEntry := func() {
		if isInternal {
			return
		}
		sql := sqlStatement.String()
		if isPrepared == "true" {
			sql = formatSQL(sql)
		}
		if entry.ConnectionID == "" || sql == "" {
			return
		}
		entry.SQL = sql
		normalizedSQL := parser.Normalize(entry.SQL)
		entry.Digest = parser.DigestNormalized(normalizedSQL).String()
		words := strings.Fields(normalizedSQL)
		entry.SQLType = "other"
		if len(words) > 0 {
			entry.SQLType = words[0]
		}
		jsonEntry, err := json.Marshal(entry)
		if err != nil {
			fmt.Println("Error marshaling JSON:", err)
			return
		}
		fmt.Fprintln(writer, string(jsonEntry))
		entryCount++
	}

	for scanner.Scan() {
		line := scanner.Text()

		if timeRegex.MatchString(line) {
			flushEntry()

			sqlStatement.Reset()
			entry = LogEntry{}
			isInternal = false
			isPrepared = "false"

			match := timeRegex.FindStringSubmatch(line)
			if match != nil {
				parsedTime, err := time.Parse(time.RFC3339Nano, match[1])
				if err == nil {
					entry.Timestamp = float64(parsedTime.UnixNano()) / 1e9
				}
			}
		} else if userHostRegex.MatchString(line) {
			match := userHostRegex.FindStringSubmatch(line)
			if match != nil {
				entry.Username = match[1]
			}
		} else if connIDRegex.MatchString(line) {
			match := connIDRegex.FindStringSubmatch(line)
			if match != nil {
				entry.ConnectionID = match[1]
			}
		} else if queryTimeRegex.MatchString(line) {
			match := queryTimeRegex.FindStringSubmatch(line)
			if match != nil {
				queryTime, _ := strconv.ParseFloat(match[1], 64)
				entry.QueryTime = int64(queryTime * 1e6)
			}
		} else if dbRegex.MatchString(line) {
			match := dbRegex.FindStringSubmatch(line)
			if match != nil {
				entry.DBName = match[1]
			}
		} else if isInternalRegex.MatchString(line) {
			match := isInternalRegex.FindStringSubmatch(line)
			if match != nil && match[1] == "true" {
				isInternal = true
			}
		} else if preparedRegex.MatchString(line) {
			match := preparedRegex.FindStringSubmatch(line)
			if match != nil {
				isPrepared = match[1]
			}
		} else if !strings.HasPrefix(line, "#") {
			if !strings.HasPrefix(strings.ToLower(line), "use ") {
				sqlStatement.WriteString(strings.TrimSpace(line))
			}
		}
	}

	flushEntry()

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}

	fmt.Printf("Logs processed and written to output json, total entries: %d\n", entryCount)
}

// formatSQL 函数用于格式化 SQL 语句，替换 ? 占位符为对应的参数值。
func formatSQL(input string) string {
	// 使用正则表达式匹配 arguments 部分
	argumentsRegex := regexp.MustCompile(`\[arguments:\s*(\((.*?)\)|([^()]+))\]`)
	match := argumentsRegex.FindStringSubmatch(input)

	var arguments []string
	if len(match) > 1 {
		// 提取 arguments 部分并去掉多余的空格
		var argumentsStr string
		if match[2] != "" {
			// 如果存在括号，提取括号内的内容
			argumentsStr = match[2]
		} else {
			// 否则直接使用匹配的内容
			argumentsStr = match[3]
		}

		// 拆分参数并去掉多余的空格
		arguments = strings.Split(argumentsStr, ",")
		for i := range arguments {
			arguments[i] = strings.TrimSpace(arguments[i]) // 去除空格
		}

		// 去掉原始 input 中的 arguments 部分
		input = strings.Replace(input, match[0], "", 1)
	}

	// 替换 ? 占位符，注意考虑引号情况
	var result strings.Builder
	argIndex := 0 // 当前参数索引
	inQuotes := 0 // 引号计数：0 表示不在引号内，1 表示在单引号内，2 表示在双引号内

	for i, char := range input {
		if char == '"' {
			inQuotes = (inQuotes + 2) % 4 // 切换双引号状态
		} else if char == '\'' {
			inQuotes = (inQuotes + 1) % 2 // 切换单引号状态
		}

		// 判断是否为 ? 占位符
		if char == '?' && inQuotes == 0 {
			// 判断是否有参数可用
			if argIndex < len(arguments) {
				arg := arguments[argIndex]
				argIndex++ // 递增参数索引

				// 判断参数类型，如果是字符串则加上引号
				if strings.HasPrefix(arg, "'") && strings.HasSuffix(arg, "'") && len(arg) > 1 {
					arg = arg[1 : len(arg)-1] // 去掉引号
					result.WriteString("'" + arg + "'")
				} else if strings.HasPrefix(arg, "\"") && strings.HasSuffix(arg, "\"") && len(arg) > 1 {
					arg = arg[1 : len(arg)-1] // 去掉引号
					result.WriteString("'" + arg + "'")
				} else {
					result.WriteString(arg)
				}
				continue
			}
		}

		// 处理转义字符和保留原字符
		if char == '\\' && i < len(input)-1 && (input[i+1] == '"' || input[i+1] == '\'') {
			result.WriteRune(char)
			continue
		}
		result.WriteRune(char) // 其他字符直接写入
	}

	return result.String()
}
