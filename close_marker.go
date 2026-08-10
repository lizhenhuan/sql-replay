package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// addCloseMarkers scans the output JSON file to find the last occurrence of each
// connection ID, then rewrites the file adding "close_conn": true to those entries.
// This allows the replay phase to close database connections once all SQL for a
// connection has been replayed, significantly reducing memory usage for large files.
func addCloseMarkers(outputPath string) error {
	// Phase 1: scan output file, find last line number for each connection
	lastLine, totalLines, err := findLastLinePerConnection(outputPath)
	if err != nil {
		return err
	}
	if len(lastLine) == 0 {
		return nil
	}

	fmt.Printf("Adding close markers for %d connections (scanning %d lines)...\n", len(lastLine), totalLines)

	// Phase 2: rewrite with close markers
	return rewriteWithCloseMarkers(outputPath, lastLine)
}

func findLastLinePerConnection(outputPath string) (map[string]int64, int64, error) {
	f, err := os.Open(outputPath)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	lastLine := make(map[string]int64)
	var lineNum int64

	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			lineNum++
			continue
		}
		if entry.ConnectionID != "" {
			lastLine[entry.ConnectionID] = lineNum
		}
		lineNum++
	}

	if err := scanner.Err(); err != nil {
		return nil, 0, err
	}

	return lastLine, lineNum, nil
}

func rewriteWithCloseMarkers(outputPath string, lastLine map[string]int64) error {
	f, err := os.Open(outputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	tmpPath := outputPath + ".tmp"
	tmpF, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	writer := bufio.NewWriterSize(tmpF, 8*1024*1024)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	var lineNum int64
	markedCount := 0

	for scanner.Scan() {
		line := scanner.Text()

		if isLastLineForConnection(line, lineNum, lastLine) {
			var entry LogEntry
			if err := json.Unmarshal([]byte(line), &entry); err == nil {
				entry.CloseConn = true
				if data, err := json.Marshal(entry); err == nil {
					fmt.Fprintln(writer, string(data))
					markedCount++
				} else {
					fmt.Fprintln(writer, line)
				}
			} else {
				fmt.Fprintln(writer, line)
			}
		} else {
			fmt.Fprintln(writer, line)
		}
		lineNum++
	}

	if err := scanner.Err(); err != nil {
		tmpF.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := writer.Flush(); err != nil {
		tmpF.Close()
		os.Remove(tmpPath)
		return err
	}
	tmpF.Close()

	fmt.Printf("Close markers added to %d entries\n", markedCount)

	return os.Rename(tmpPath, outputPath)
}

// isLastLineForConnection checks if the current line is the last entry for its connection.
func isLastLineForConnection(line string, lineNum int64, lastLine map[string]int64) bool {
	var entry LogEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		return false
	}
	if entry.ConnectionID == "" {
		return false
	}
	target, ok := lastLine[entry.ConnectionID]
	return ok && target == lineNum
}
