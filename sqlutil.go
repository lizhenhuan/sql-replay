package main

import "strings"

// stripSQLInlineComments removes `-- comment` from SQL text.
// It processes line by line: within each line, everything from `-- ` (or `--` at
// end of line) to the end of that line is stripped, but only when not inside a
// string literal.
//
// This must be called BEFORE newlines are replaced with spaces. Once newlines
// are gone, an inline `-- comment` would eat all subsequent SQL on the joined line.
func stripSQLInlineComments(sql string) string {
	lines := strings.Split(sql, "\n")
	for i, line := range lines {
		lines[i] = stripInlineCommentFromLine(line)
	}
	return strings.Join(lines, "\n")
}

// stripInlineCommentFromLine removes everything from the first inline `--` comment
// to the end of the line, respecting single- and double-quoted string literals.
// A `--` is treated as a comment start when:
//   - it is not inside a string literal, AND
//   - it is followed by a space/tab, OR it is at the end of the line
//
// This matches MySQL's `-- ` comment syntax requirement.
func stripInlineCommentFromLine(line string) string {
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(line); i++ {
		c := line[i]

		switch c {
		case '\'':
			if !inDoubleQuote {
				inSingleQuote = !inSingleQuote
			}
		case '"':
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
		case '-':
			if !inSingleQuote && !inDoubleQuote && i+1 < len(line) && line[i+1] == '-' {
				// MySQL requires whitespace (or end-of-line) after `--`
				if i+2 >= len(line) || line[i+2] == ' ' || line[i+2] == '\t' || line[i+2] == '\r' {
					return strings.TrimRight(line[:i], " \t\r")
				}
			}
		}
	}

	return line
}
