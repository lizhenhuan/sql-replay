package main

import (
	"testing"
)

func TestStripInlineCommentFromLine(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "simple inline comment",
			input: "SELECT 1,-- this is a comment",
			want:  "SELECT 1,",
		},
		{
			name:  "comment at end without space after --",
			input: "SELECT 1--",
			want:  "SELECT 1",
		},
		{
			name:  "no comment",
			input: "SELECT 1, 2, 3 FROM t",
			want:  "SELECT 1, 2, 3 FROM t",
		},
		{
			name:  "comment inside single quote should be preserved",
			input: "SELECT '-- not a comment' FROM t",
			want:  "SELECT '-- not a comment' FROM t",
		},
		{
			name:  "comment after single-quoted string",
			input: "SELECT 'hello',-- comment \n col2 FROM t",
			want:  "SELECT 'hello',",
		},
		{
			name:  "double dash in number literal not treated as comment",
			input: "SELECT 1--2 FROM t",
			want:  "SELECT 1--2 FROM t",
		},
		{
			name:  "empty line",
			input: "",
			want:  "",
		},
		{
			name:  "only comment line",
			input: "-- just a comment",
			want:  "",
		},
		{
			name:  "comment with Chinese text",
			input: "cm.CONSULT_ID AS DE010003705,-- 业务主键",
			want:  "cm.CONSULT_ID AS DE010003705,",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripInlineCommentFromLine(tt.input)
			if got != tt.want {
				t.Errorf("stripInlineCommentFromLine(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestStripSQLInlineComments(t *testing.T) {
	// Reproduces the exact bug from the issue: multi-line SQL with inline
	// comments. Before the fix, newlines were replaced with spaces, causing
	// the first `--` comment to eat all subsequent SQL.
	input := `SELECT
	cm.CONSULT_ID AS DE010003705,-- 业务主键
	cm.CONSULT_ID AS DE010000801,-- 会诊申请单号
	cm.CHIEF_CONFIRM_DOC_CODE AS DE020106240-- 科主任编码
FROM
	CONSULT_APPLY_MAIN cm
WHERE
	cm.CONSULT_ID = 'ee72ba7c81d2d5f6154a1caf397d0f01';`

	expected := `SELECT
	cm.CONSULT_ID AS DE010003705,
	cm.CONSULT_ID AS DE010000801,
	cm.CHIEF_CONFIRM_DOC_CODE AS DE020106240
FROM
	CONSULT_APPLY_MAIN cm
WHERE
	cm.CONSULT_ID = 'ee72ba7c81d2d5f6154a1caf397d0f01';`

	got := stripSQLInlineComments(input)
	if got != expected {
		t.Errorf("stripSQLInlineComments mismatch:\nGOT:\n%s\nWANT:\n%s", got, expected)
	}
}

func TestStripSQLInlineComments_PreservesStringWithDash(t *testing.T) {
	// Ensure `--` inside a string literal is not treated as comment
	input := "SELECT '--fake' AS col1, 'a--b' AS col2,-- real comment\n col3"
	want := "SELECT '--fake' AS col1, 'a--b' AS col2,"
	got := stripInlineCommentFromLine(input)
	if got != want {
		t.Errorf("Got %q, want %q", got, want)
	}
}

func TestStripSQLInlineComments_SelectWithInlineComment(t *testing.T) {
	// From 李振环's test case
	// select 1 , -- first
	// 2 from dual;
	input := "select 1 , -- first\n 2 from dual;"

	expected := "select 1 ,\n 2 from dual;"

	got := stripSQLInlineComments(input)
	if got != expected {
		t.Errorf("stripSQLInlineComments mismatch:\nGOT:  %q\nWANT: %q", got, expected)
	}
}
