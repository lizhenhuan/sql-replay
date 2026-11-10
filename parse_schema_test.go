package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseLogsSchema(t *testing.T) {
	tests := []struct {
		name     string
		metadata string
		want     string
	}{
		{name: "schema before other fields", metadata: "Thread_id: 1 Schema: monitor Errno: 0", want: "monitor"},
		{name: "schema at end of line", metadata: "Schema: weibao", want: "weibao"},
		{name: "missing schema after named schema", metadata: "Thread_id: 1 Errno: 0", want: ""},
		{name: "empty schema before other fields", metadata: "Schema:  Errno: 0 Killed: 0", want: ""},
		{name: "schema with extra whitespace", metadata: "Schema:\t  monitor-prod \t Errno: 0", want: "monitor-prod"},
		{name: "schema containing spaces", metadata: "Schema: data warehouse Errno: 0", want: "data warehouse"},
		{name: "unicode schema", metadata: "Schema: 监控_数据 Errno: 0", want: "监控_数据"},
		{name: "empty schema at end of line", metadata: "Schema:  \t", want: ""},
		{name: "different field name", metadata: "OtherSchema: unrelated Errno: 0", want: ""},
	}

	var input strings.Builder
	for i, tt := range tests {
		fmt.Fprintf(&input, "# Time: 261109 15:27:%02d\n", i)
		input.WriteString("# User@Host: app[app] @ localhost [127.0.0.1] Id: 1\n")
		fmt.Fprintf(&input, "# Query_time: 2.559774 Lock_time: 0.000049 Rows_sent: 1 Rows_examined: 1 %s\n", tt.metadata)
		input.WriteString("SET timestamp=1794209220;\nSELECT 1;\n")
	}

	dir := t.TempDir()
	inputPath := filepath.Join(dir, "slow.log")
	outputPath := filepath.Join(dir, "parsed.json")
	if err := os.WriteFile(inputPath, []byte(input.String()), 0600); err != nil {
		t.Fatal(err)
	}
	ParseLogs(inputPath, outputPath)

	output, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	var entries []LogEntry
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(tests) {
		t.Fatalf("got %d entries, want %d", len(entries), len(tests))
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := entries[i]
			if entry.DBName != tt.want {
				t.Errorf("dbname = %q, want %q", entry.DBName, tt.want)
			}
			if entry.QueryTime != 2559774 || entry.RowsSent != 1 {
				t.Errorf("unexpected query metadata: query_time=%d rows_sent=%d", entry.QueryTime, entry.RowsSent)
			}
		})
	}
}
