package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	_ "net/http/pprof"
)

// Version information for the SQL Replay Tool
var version = "0.3.4, build date 20241014"

var showVersion bool

func main() {
	var mode string
	flag.StringVar(&mode, "mode", "", "Mode of operation: parsemysqlslow, parsetidbslow, parsemysqlcsv, parsesqlserver, parsetencentaudit, parseoracle, replay, load, report, cloudreport, json_replay_route")

	// Define flags for various operation parameters
	var slowLogPath, slowOutputPath, dbConnStr, replayOutputFilePath, filterUsername, filterSQLType, filterDBName, ignoreDigests, outDir, replayOut, tableName, Port string
	var routeInDir, routeOutDir, routePrefix string
	var Speed float64
	var lang string
	var routeDryRun, routeSkipNoDBname, routeSkipSelf, routeNoProgress, routeNoLineCount, routeQuiet bool
	var pprofAddr string

	flag.StringVar(&pprofAddr, "pprof", "", "Start pprof HTTP server on this address (e.g. :6060) for flame graph capture")

	flag.BoolVar(&showVersion, "version", false, "Show version info")
	flag.StringVar(&slowLogPath, "slow-in", "", "Path to slow query log file")
	flag.StringVar(&slowOutputPath, "slow-out", "", "Path to slow output JSON file")
	flag.StringVar(&dbConnStr, "db", "username:password@tcp(localhost:3306)/test", "Database connection string")
	flag.StringVar(&outDir, "out-dir", "", "Directory containing the JSON files")
	flag.StringVar(&replayOut, "replay-name", "", "Replay output filename")
	flag.StringVar(&tableName, "table", "replay_info", "Name of the table to insert data into")
	flag.StringVar(&replayOutputFilePath, "replay-out", "", "Path to output json file")
	flag.StringVar(&filterUsername, "username", "all", "Username to filter (default 'all', or specific username)")
	flag.StringVar(&filterSQLType, "sqltype", "all", "SQL type to filter (default 'all', or 'select')")
	flag.StringVar(&filterDBName, "dbname", "all", "Database name to filter (default 'all', or specific dbname)")
	flag.StringVar(&ignoreDigests, "ignoredigests", "", "Ignore the Specific digests")
	flag.Float64Var(&Speed, "speed", 1.0, "Replay speed multiplier")
	flag.StringVar(&Port, "port", ":8081", "Report web server port")
	flag.StringVar(&lang, "lang", "en", "Language for output (e.g., 'en' for English, 'zh' for Chinese)")
	flag.StringVar(&routeInDir, "route-in", "", "Input directory containing .json files to route (for json_replay_route mode)")
	flag.StringVar(&routeOutDir, "route-out", "", "Output directory for routed files (default: same as route-in)")
	flag.StringVar(&routePrefix, "route-prefix", "replay_", "Output filename prefix (for json_replay_route mode)")
	flag.BoolVar(&routeDryRun, "route-dry-run", false, "Print what would be done, do not write")
	flag.BoolVar(&routeSkipNoDBname, "route-skip-no-dbname", true, "Skip lines without dbname field")
	flag.BoolVar(&routeSkipSelf, "route-skip-self", true, "Skip input files matching output pattern")
	flag.BoolVar(&routeNoProgress, "route-no-progress", false, "Disable progress bar")
	flag.BoolVar(&routeNoLineCount, "route-no-line-count", false, "Skip pre-pass that counts total lines")
	flag.BoolVar(&routeQuiet, "route-quiet", false, "Suppress per-file output")

	flag.Parse()

	if showVersion {
		fmt.Println("SQL Replay Tool Version:", version)
		os.Exit(0)
	}

	// Start pprof server if requested
	if pprofAddr != "" {
		fmt.Printf("[pprof] listening on %s\n", pprofAddr)
		fmt.Printf("[pprof] flame graph: go tool pprof http://%s/debug/pprof/profile?seconds=30\n", pprofAddr)
		fmt.Printf("[pprof] heap:       go tool pprof http://%s/debug/pprof/heap\n", pprofAddr)
		fmt.Printf("[pprof] goroutine:  go tool pprof http://%s/debug/pprof/goroutine\n", pprofAddr)
		go http.ListenAndServe(pprofAddr, nil)
	}

	if mode == "" {
		printUsage()
		os.Exit(1)
	}

	// Execute the appropriate function based on the selected mode
	switch mode {
	case "parsemysqlslow":
		ParseLogs(slowLogPath, slowOutputPath)
	case "parsemysqlcsv":
		parseCSVLog(slowLogPath, slowOutputPath)
	case "parsetidbslow":
		ParseTiDBLogs(slowLogPath, slowOutputPath)
	case "parsesqlserver":
		ParseSQLServerXEvents(slowLogPath, slowOutputPath)
	case "parsetencentaudit":
		ParseTencentAuditCSV(slowLogPath, slowOutputPath)
	case "replay":
		StartSQLReplay(dbConnStr, Speed, slowOutputPath, replayOutputFilePath, filterUsername, filterSQLType, filterDBName, ignoreDigests, lang)
	case "load":
		LoadData(dbConnStr, outDir, replayOut, tableName)
	case "report":
		Report(dbConnStr, replayOut, Port)
	case "cloudreport":
		CloudReport(dbConnStr, replayOut, Port)
	case "json_replay_route":
		if routeInDir == "" {
			fmt.Fprintln(os.Stderr, "ERROR: -route-in is required for json_replay_route mode")
			os.Exit(1)
		}
		RouteJSONReplay(routeInDir, routeOutDir, routePrefix, routeDryRun, routeSkipNoDBname, routeSkipSelf, routeNoProgress, routeNoLineCount, routeQuiet)

	default:
		fmt.Println("Invalid mode. Available modes: parsemysqlslow, parsemysqlcsv, parsetidbslow, parsesqlserver, parsetencentaudit, parseoracle, replay, load, report, cloudreport, json_replay_route")
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: ./sql-replay -mode [parsemysqlslow|parsemysqlcsv|parsetidbslow|parsesqlserver|parsetencentaudit|parseoracle|replay|load|report|json_replay_route]")
	fmt.Println("    1. parse mysql slow log: ./sql-replay -mode parsemysqlslow -slow-in <path_to_slow_query_log> -slow-out <path_to_slow_output_file>")
	fmt.Println("    2. parse tidb slow log: ./sql-replay -mode parsetidbslow -slow-in <path_to_slow_query_log> -slow-out <path_to_slow_output_file>")
	fmt.Println("    3. parse sql server xevent csv: ./sql-replay -mode parsesqlserver -slow-in <path_to_xevent_csv> -slow-out <path_to_slow_output_file>")
	fmt.Println("    4. parse Tencent Cloud audit csv: ./sql-replay -mode parsetencentaudit -slow-in <path_to_audit_csv> -slow-out <path_to_slow_output_file>")
	fmt.Println("    5. parse Oracle AWR report: ./sql-replay -mode parseoracle -slow-in <path_to_awr_html_or_dir> -slow-out <path_to_slow_output_file>")
	fmt.Println("    6. replay mode: ./sql-replay -mode replay -db <mysql_connection_string> -speed 1.0 -slow-out <slow_output_file> -replay-out <replay_output_file> -username <all|username> -sqltype <all|select> -dbname <all|dbname> -ignoredigests <digest1,digest2...> -lang <en|zh>")
	fmt.Println("    7. load mode: ./sql-replay -mode load -db <DB_CONN_STRING> -out-dir <DIRECTORY> -replay-name <REPORT_OUT_FILE_NAME> -table <replay_info>")
	fmt.Println("    8. report mode: ./sql-replay -mode report -db <mysql_connection_string> -replay-name <replay name> -port ':8081'")
	fmt.Println("    9. cloudreport mode: ./sql-replay -mode cloudreport -db <mysql_connection_string> -replay-name <replay name> -port ':8081'")
	fmt.Println("   10. json_replay_route mode: ./sql-replay -mode json_replay_route -route-in <input_dir> [-route-out <output_dir>] [-route-prefix replay_] [-route-dry-run] [-route-skip-no-dbname] [-route-skip-self] [-route-no-progress] [-route-no-line-count] [-route-quiet]")
}
