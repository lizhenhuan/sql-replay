# ============================================================================
# SQL Server Extended Events Export Script
# 将捕获的慢查询导出为 CSV 格式，供 sql-replay 解析
# ============================================================================

param(
    [string]$ServerInstance = "localhost",
    [string]$Database = "master",
    [string]$XEventPath = "C:\xe\slow_query*.xel",  # XEvent 文件路径
    [string]$OutputPath = "C:\xe\slow_query.csv",   # 输出 CSV 路径
    [int]$DurationThreshold = 0,                     # 最小执行时间过滤 (微秒)，0 表示不过滤
    [switch]$IncludeHeaders = $true
)

Write-Host "=== SQL Server Extended Events Export ===" -ForegroundColor Cyan
Write-Host "Server: $ServerInstance"
Write-Host "XEvent Path: $XEventPath"
Write-Host "Output Path: $OutputPath"
Write-Host ""

# 构建 SQL 查询
$sql = @"
SELECT 
    FORMAT(DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()), 
        xed.event_data.value('(@timestamp)[1]', 'datetime2')), 'yyyy-MM-dd HH:mm:ss.fffffff') AS event_time,
    xed.event_data.value('(data[@name="duration"]/value)[1]', 'bigint') / 1000 AS duration_us,
    xed.event_data.value('(data[@name="cpu_time"]/value)[1]', 'bigint') / 1000 AS cpu_time_us,
    xed.event_data.value('(data[@name="logical_reads"]/value)[1]', 'bigint') AS logical_reads,
    xed.event_data.value('(data[@name="writes"]/value)[1]', 'bigint') AS writes,
    xed.event_data.value('(data[@name="row_count"]/value)[1]', 'int') AS row_count,
    xed.event_data.value('(action[@name="session_id"]/value)[1]', 'int') AS session_id,
    xed.event_data.value('(action[@name="database_name"]/value)[1]', 'nvarchar(128)') AS database_name,
    xed.event_data.value('(action[@name="username"]/value)[1]', 'nvarchar(128)') AS username,
    xed.event_data.value('(data[@name="statement"]/value)[1]', 'nvarchar(max)') AS sql_text
FROM sys.fn_xe_file_target_read_file('$($XEventPath.Replace('\', '\\'))', NULL, NULL, NULL) xet
CROSS APPLY xet.event_data.nodes('//event') AS xed(event_data)
$(if ($DurationThreshold -gt 0) { "WHERE xed.event_data.value('(data[@name=""duration""]/value)[1]', 'bigint') / 1000 >= $DurationThreshold" })
ORDER BY event_time
"@

try {
    Write-Host "Querying Extended Events data..." -ForegroundColor Yellow
    
    $result = Invoke-Sqlcmd -Query $sql -ServerInstance $ServerInstance -Database $Database -ErrorAction Stop
    
    if ($result -and $result.Count -gt 0) {
        Write-Host "Found $($result.Count) records" -ForegroundColor Green
        
        # 导出到 CSV
        $result | Export-Csv -Path $OutputPath -NoTypeInformation -Encoding UTF8
        
        Write-Host "Export completed: $OutputPath" -ForegroundColor Green
        
        # 显示前几条记录预览
        Write-Host ""
        Write-Host "Preview (first 3 records):" -ForegroundColor Cyan
        $result | Select-Object -First 3 | Format-Table event_time, duration_us, database_name, username, sql_text -AutoSize
    } else {
        Write-Host "No records found in the specified XEvent files" -ForegroundColor Yellow
    }
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "Troubleshooting:" -ForegroundColor Yellow
    Write-Host "1. Ensure the XEvent session is running: ALTER EVENT SESSION [SlowQueryCapture] ON SERVER STATE = START"
    Write-Host "2. Check the XEvent file path exists and has data"
    Write-Host "3. Verify SQL Server connectivity"
}

Write-Host ""
Write-Host "Next step: Use sql-replay to parse the CSV" -ForegroundColor Cyan
Write-Host "  ./sql-replay -mode parsesqlserver -slow-in $OutputPath -slow-out slow_query.json"