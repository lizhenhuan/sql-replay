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

func TestParseAliAuditCSV(t *testing.T) {
	tempDir := t.TempDir()
	inputPath := filepath.Join(tempDir, "audit.csv")
	outputPath := filepath.Join(tempDir, "audit.jsonl")

	input := strings.Join([]string{
		"\uFEFFLOG,DB,TID,USER,USER_IP,SQL_TYPE,FAIL,CHECK_ROWS,UPDATE_ROWS,TABLE_NAME,LATENCY,TS,ORIGIN_TIME,RETURN_ROWS,LOCK_TIME,TRX_ID,LOGIC_READ,PHYSIC_SYNC_READ,PHYSIC_ASYNC_READ",
		`SELECT c FROM test.t WHERE id=2,test,8,root,10.0.0.4,select,0,1,0,test.t,59,2026-05-12 15:00:17,1778569217381720,1,1,0,2,0,0`,
		`DELETE FROM test.t WHERE id=9,test,9,root,10.0.0.4,delete,1,1,1,test.t,25,2026-05-12 15:00:17,1778569217381800,0,1,0,2,0,0`,
		`logout!,test,8,root,10.0.0.4,logout,0,0,0,"",3,2026-05-12 15:00:17,1778569217382000,0,0,0,0,0,0`,
		`BEGIN,test,7,root,10.0.0.4,begin,0,0,0,"",73,2026-05-12 15:00:17,1778569217370000,0,0,0,0,0,0`,
		`"INSERT INTO test.t(id, note) VALUES (1, 'a,b')",test,7,root,10.0.0.4,insert,0,0,1,test.t,110,2026-05-12 15:00:17,1778569217398386,0,1,19693,5,0,0`,
	}, "\n")

	if err := os.WriteFile(inputPath, []byte(input), 0644); err != nil {
		t.Fatalf("write input file failed: %v", err)
	}

	ParseAliAuditCSV(inputPath, outputPath)

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
		float64(time.Date(2026, 5, 12, 15, 0, 17, 370000000, time.Local).UnixNano()) / 1e9,
		float64(time.Date(2026, 5, 12, 15, 0, 17, 381720000, time.Local).UnixNano()) / 1e9,
		float64(time.Date(2026, 5, 12, 15, 0, 17, 398386000, time.Local).UnixNano()) / 1e9,
	}

	if entries[0].SQLType != "begin" || entries[1].SQLType != "select" || entries[2].SQLType != "insert" {
		t.Fatalf("unexpected sql types: %#v", []string{entries[0].SQLType, entries[1].SQLType, entries[2].SQLType})
	}
	if entries[0].ConnectionID != "7" || entries[1].ConnectionID != "8" || entries[2].ConnectionID != "7" {
		t.Fatalf("unexpected connection ids: %#v", []string{entries[0].ConnectionID, entries[1].ConnectionID, entries[2].ConnectionID})
	}
	if entries[1].RowsSent != 1 || entries[2].RowsSent != 0 {
		t.Fatalf("unexpected rows sent: second=%d third=%d", entries[1].RowsSent, entries[2].RowsSent)
	}
	if entries[0].QueryTime != 73 || entries[1].QueryTime != 59 || entries[2].QueryTime != 110 {
		t.Fatalf("unexpected query times: %#v", []int64{entries[0].QueryTime, entries[1].QueryTime, entries[2].QueryTime})
	}
	if entries[2].SQL != "INSERT INTO test.t(id, note) VALUES (1, 'a,b')" {
		t.Fatalf("unexpected insert sql: %q", entries[2].SQL)
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

func TestBuildAliAuditFieldIndexMissingRequiredHeaders(t *testing.T) {
	headers := []string{"\uFEFFLOG", "DB", "USER"}
	_, err := buildAliAuditFieldIndex(headers)
	if err == nil {
		t.Fatal("expected missing header error")
	}
	if !strings.Contains(err.Error(), "tid") || !strings.Contains(err.Error(), "latency") || !strings.Contains(err.Error(), "origin_time") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseAliAuditTimestampOriginTime(t *testing.T) {
	ts, err := parseAliAuditTimestamp("1778569217377884")
	if err != nil {
		t.Fatalf("parse timestamp failed: %v", err)
	}

	expected := float64(time.Date(2026, 5, 12, 15, 0, 17, 377884000, time.Local).UnixNano()) / 1e9
	if !almostEqual(ts, expected) {
		t.Fatalf("unexpected timestamp: got %.9f want %.9f", ts, expected)
	}
}
