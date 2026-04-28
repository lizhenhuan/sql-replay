package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseTencentAuditCSV(t *testing.T) {
	tempDir := t.TempDir()
	inputPath := filepath.Join(tempDir, "audit.csv")
	outputPath := filepath.Join(tempDir, "audit.jsonl")

	input := strings.Join([]string{
		"\uFEFFAffectRows,ErrCode,SqlType,TableName,PolicyName,DBName,Sql,Host,UsEr,ExecTime,CpuTime,LockWaitTime,CheckRows,SentRows,ThreadId,NsTime,IoWaitTime,TrxLivingTime,Timestamp,Result,RuleTemplateId,RiskLevel,TrxId,ClientPort",
		`0,0,LOGIN,[],default_audit,,/* SP */,10.0.0.4,root,0,0.000,0,0,0,0,0,0,0,2026-04-28 11:22:37,,,,0,45042`,
		`0,0,UPDATE,"[""test.test""]",sys_default,test,"update test.test set v = 1 where id = 0",10.0.0.4,root,3000,20.000,0,0,0,511579,250000000,0,0,2026-04-28 11:22:33,,,,0,45043`,
		`1,0,INSERT,"[""test.test""]",sys_default,test,"insert into test.test(id, v) values(0, 13105)",10.0.0.4,root,4202,315.197,1,0,0,511578,155216116,0,0,2026-04-28 11:22:32,,,,7537,45042`,
		`0,0,SeLeCt,[null],sys_default,test,select @@version_comment limit 1,10.0.0.4,root,5755,68.518,0,1,1,511578,151216116,0,0,2026-04-28 11:22:32,,,,0,45042`,
	}, "\n")

	if err := os.WriteFile(inputPath, []byte(input), 0644); err != nil {
		t.Fatalf("write input file failed: %v", err)
	}

	ParseTencentAuditCSV(inputPath, outputPath)

	outputFile, err := os.Open(outputPath)
	if err != nil {
		t.Fatalf("open output file failed: %v", err)
	}
	defer outputFile.Close()

	var entries []LogEntry
	scanner := bufio.NewScanner(outputFile)
	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatalf("unmarshal output failed: %v", err)
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read output file failed: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("unexpected output entry count: got %d want 3", len(entries))
	}

	expectedTimes := []float64{
		float64(time.Date(2026, 4, 28, 11, 22, 32, 151216116, time.Local).UnixNano()) / 1e9,
		float64(time.Date(2026, 4, 28, 11, 22, 32, 155216116, time.Local).UnixNano()) / 1e9,
		float64(time.Date(2026, 4, 28, 11, 22, 33, 250000000, time.Local).UnixNano()) / 1e9,
	}

	if entries[0].SQLType != "select" || entries[1].SQLType != "insert" || entries[2].SQLType != "update" {
		t.Fatalf("unexpected sql types: %#v", []string{entries[0].SQLType, entries[1].SQLType, entries[2].SQLType})
	}
	if entries[0].SQL != "select @@version_comment limit 1" {
		t.Fatalf("unexpected first sql: %q", entries[0].SQL)
	}
	if entries[1].SQL != "insert into test.test(id, v) values(0, 13105)" {
		t.Fatalf("unexpected second sql: %q", entries[1].SQL)
	}
	if entries[2].SQL != "update test.test set v = 1 where id = 0" {
		t.Fatalf("unexpected third sql: %q", entries[2].SQL)
	}
	if entries[0].RowsSent != 1 || entries[1].RowsSent != 0 {
		t.Fatalf("unexpected rows sent: first=%d second=%d", entries[0].RowsSent, entries[1].RowsSent)
	}
	if entries[0].ConnectionID != "511578" || entries[2].ConnectionID != "511579" {
		t.Fatalf("unexpected connection ids: first=%s third=%s", entries[0].ConnectionID, entries[2].ConnectionID)
	}
	if entries[0].QueryTime != 5755 || entries[1].QueryTime != 4202 || entries[2].QueryTime != 3000 {
		t.Fatalf("unexpected query times: %#v", []int64{entries[0].QueryTime, entries[1].QueryTime, entries[2].QueryTime})
	}
	for i, expectedTime := range expectedTimes {
		if !almostEqual(entries[i].Timestamp, expectedTime) {
			t.Fatalf("unexpected timestamp at %d: got %.9f want %.9f", i, entries[i].Timestamp, expectedTime)
		}
		if entries[i].Digest == "" {
			t.Fatalf("digest should not be empty at %d", i)
		}
	}
}

func TestBuildTencentAuditFieldIndexMissingRequiredHeaders(t *testing.T) {
	headers := []string{"\uFEFFSqlType", "Sql", "User"}
	_, err := buildTencentAuditFieldIndex(headers)
	if err == nil {
		t.Fatal("expected missing header error")
	}
	if !strings.Contains(err.Error(), "timestamp") || !strings.Contains(err.Error(), "threadid") || !strings.Contains(err.Error(), "exectime") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseTencentAuditTimestampUnixSeconds(t *testing.T) {
	ts, err := parseTencentAuditTimestamp("1777346902", "200561048")
	if err != nil {
		t.Fatalf("parse timestamp failed: %v", err)
	}

	expected := float64(time.Unix(1777346902, 200561048).UnixNano()) / 1e9
	if !almostEqual(ts, expected) {
		t.Fatalf("unexpected timestamp: got %.9f want %.9f", ts, expected)
	}
}

func TestParseTencentAuditTimestampLocalTime(t *testing.T) {
	ts, err := parseTencentAuditTimestamp("2026-04-28 11:22:32", "151216116")
	if err != nil {
		t.Fatalf("parse timestamp failed: %v", err)
	}

	expected := float64(time.Date(2026, 4, 28, 11, 22, 32, 151216116, time.Local).UnixNano()) / 1e9
	if !almostEqual(ts, expected) {
		t.Fatalf("unexpected timestamp: got %.9f want %.9f", ts, expected)
	}
}

func TestReverseTencentAuditEntriesIfDescendingKeepsMixedOrder(t *testing.T) {
	entries := []sortedTencentAuditEntry{
		{Entry: LogEntry{Timestamp: 3}},
		{Entry: LogEntry{Timestamp: 1}},
		{Entry: LogEntry{Timestamp: 2}},
	}

	reverseTencentAuditEntriesIfDescending(entries)

	got := []float64{entries[0].Entry.Timestamp, entries[1].Entry.Timestamp, entries[2].Entry.Timestamp}
	want := []float64{3, 1, 2}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected order after reverse check: got %#v want %#v", got, want)
		}
	}
}

func TestCleanTencentAuditSQLPreservesInternalWhitespace(t *testing.T) {
	sql := "  select 'a   b',\r\n\t'c\t d'  \nfrom dual  "
	cleaned := cleanTencentAuditSQL(sql)
	expected := "select 'a   b', \t'c\t d'   from dual"
	if cleaned != expected {
		t.Fatalf("unexpected cleaned sql: got %q want %q", cleaned, expected)
	}
}

func almostEqual(a, b float64) bool {
	const epsilon = 1e-9
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff <= epsilon
}
