-- ============================================================================
-- SQL Server Extended Events Setup for Slow Query Capture
-- 用于 SQL Replay 工具捕获慢查询
-- ============================================================================

-- 1. 创建 Extended Events Session
-- 注意：修改以下参数以适应您的环境
--   - duration > 1000000: 捕获执行时间超过 1 秒的查询 (单位: 微秒)
--   - filename: 输出文件路径，确保目录存在且有写入权限
--   - max_file_size: 单个文件最大大小 (MB)
--   - max_rollover_files: 滚动文件数量

CREATE EVENT SESSION [SlowQueryCapture] ON SERVER
ADD EVENT sqlserver.sql_statement_completed(
    ACTION(
        sqlserver.client_app_name,
        sqlserver.client_hostname,
        sqlserver.database_name,
        sqlserver.username,
        sqlserver.session_id
    )
    WHERE [duration] > 1000000  -- 1秒 = 1000000 微秒，可根据需要调整
)
ADD TARGET package0.event_file(
    SET filename = N'C:\xe\slow_query.xel',  -- Windows 路径
    -- SET filename = N'/var/opt/mssql/data/xe/slow_query.xel',  -- Linux 路径
    max_file_size = 100,  -- MB
    max_rollover_files = 10
)
WITH (
    MAX_MEMORY = 4096 KB,
    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,
    MAX_DISPATCH_LATENCY = 30 SECONDS,
    TRACK_CAUSALITY = OFF,
    STARTUP_STATE = ON  -- 服务器启动时自动启动
);
GO

-- 2. 启动 Session
ALTER EVENT SESSION [SlowQueryCapture] ON SERVER STATE = START;
GO

-- 3. 验证 Session 状态
SELECT 
    s.name AS session_name,
    s.create_time,
    CASE s.is_running 
        WHEN 1 THEN 'Running' 
        ELSE 'Stopped' 
    END AS status
FROM sys.server_event_sessions s
LEFT JOIN sys.dm_xe_sessions xs ON s.name = xs.name
WHERE s.name = 'SlowQueryCapture';
GO

-- ============================================================================
-- 查询和导出 Extended Events 数据
-- ============================================================================

-- 4. 查询捕获的慢查询数据
-- 注意：修改文件路径以匹配实际文件位置
SELECT 
    DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()), 
        xed.event_data.value('(@timestamp)[1]', 'datetime2')) AS event_time,
    xed.event_data.value('(data[@name="duration"]/value)[1]', 'bigint') / 1000 AS duration_us,
    xed.event_data.value('(data[@name="cpu_time"]/value)[1]', 'bigint') / 1000 AS cpu_time_us,
    xed.event_data.value('(data[@name="logical_reads"]/value)[1]', 'bigint') AS logical_reads,
    xed.event_data.value('(data[@name="physical_reads"]/value)[1]', 'bigint') AS physical_reads,
    xed.event_data.value('(data[@name="writes"]/value)[1]', 'bigint') AS writes,
    xed.event_data.value('(data[@name="row_count"]/value)[1]', 'int') AS row_count,
    xed.event_data.value('(action[@name="session_id"]/value)[1]', 'int') AS session_id,
    xed.event_data.value('(action[@name="database_name"]/value)[1]', 'nvarchar(128)') AS database_name,
    xed.event_data.value('(action[@name="username"]/value)[1]', 'nvarchar(128)') AS username,
    xed.event_data.value('(data[@name="statement"]/value)[1]', 'nvarchar(max)') AS sql_text
FROM sys.fn_xe_file_target_read_file('C:\xe\slow_query*.xel', NULL, NULL, NULL) xet
CROSS APPLY xet.event_data.nodes('//event') AS xed(event_data)
ORDER BY event_time;
GO

-- 5. 导出到 CSV (使用 PowerShell 执行)
-- 在 PowerShell 中运行以下命令:
/*
$sql = @"
SELECT 
    FORMAT(DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()), 
        xed.event_data.value('(@timestamp)[1]', 'datetime2')), 'yyyy-MM-dd HH:mm:ss.fff') AS event_time,
    xed.event_data.value('(data[@name=\"duration\"]/value)[1]', 'bigint') / 1000 AS duration_us,
    xed.event_data.value('(data[@name=\"cpu_time\"]/value)[1]', 'bigint') / 1000 AS cpu_time_us,
    xed.event_data.value('(data[@name=\"logical_reads\"]/value)[1]', 'bigint') AS logical_reads,
    xed.event_data.value('(data[@name=\"writes\"]/value)[1]', 'bigint') AS writes,
    xed.event_data.value('(data[@name=\"row_count\"]/value)[1]', 'int') AS row_count,
    xed.event_data.value('(action[@name=\"session_id\"]/value)[1]', 'int') AS session_id,
    xed.event_data.value('(action[@name=\"database_name\"]/value)[1]', 'nvarchar(128)') AS database_name,
    xed.event_data.value('(action[@name=\"username\"]/value)[1]', 'nvarchar(128)') AS username,
    xed.event_data.value('(data[@name=\"statement\"]/value)[1]', 'nvarchar(max)') AS sql_text
FROM sys.fn_xe_file_target_read_file('C:\\xe\\slow_query*.xel', NULL, NULL, NULL) xet
CROSS APPLY xet.event_data.nodes('//event') AS xed(event_data)
"@

Invoke-Sqlcmd -Query $sql -ServerInstance "localhost" -Database "master" | 
    Export-Csv -Path "C:\xe\slow_query.csv" -NoTypeInformation -Encoding UTF8
*/

-- ============================================================================
-- 管理命令
-- ============================================================================

-- 停止 Session
-- ALTER EVENT SESSION [SlowQueryCapture] ON SERVER STATE = STOP;
-- GO

-- 删除 Session
-- DROP EVENT SESSION [SlowQueryCapture] ON SERVER;
-- GO

-- 查看所有 Extended Events Sessions
-- SELECT name, create_time, is_running FROM sys.server_event_sessions;