/* 6 - 2019-10-31 Current System State*/
USE tempdb
GO
IF CAST(CAST(SERVERPROPERTY('ProductVersion') as CHAR(2)) as NUMERIC) < 10
BEGIN
SELECT @@VERSION
RAISERROR ('To run this script SQL Server has to be Version 2008 or higher.',16,1)
END
GO
IF 3!=IsNull(HAS_PERMS_BY_NAME(null,null,'VIEW SERVER STATE')+HAS_PERMS_BY_NAME(null,null,'VIEW ANY DATABASE')+HAS_PERMS_BY_NAME(null,null,'VIEW ANY DEFINITION'),0)
BEGIN
RAISERROR ('To run this script you need to have at least the following permissions: "VIEW SERVER STATE", "VIEW ANY DATABASE", "VIEW ANY DEFINITION".',16,1)
END
GO
IF HAS_PERMS_BY_NAME('xp_readerrorlog','OBJECT','EXECUTE')=0
PRINT 'You might need to have a permission to execute "xp_readerrorlog".';
GO
IF OBJECT_ID('tempdb..#CurLog') IS NOT NULL
DROP TABLE #CurLog;
GO
CREATE TABLE #CurLog(LogDate DATETIME, ProcessInfo SYSNAME, Text VARCHAR(MAX));
GO
IF OBJECT_ID('tempdb..#USP_PROCESS') IS NOT NULL
DROP PROCEDURE #USP_PROCESS;
GO
CREATE PROCEDURE #USP_PROCESS
@Param VARCHAR(100) = Null
WITH RECOMPILE
AS
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
SET NOCOUNT ON
DECLARE @SQL NVARCHAR(MAX),
@S CHAR(80), @Ex VARCHAR(2000),
@V INT,--SQL Server Major Version
@or CHAR(20)=' OPTION (RECOMPILE);',
@us CHAR(17)='UNION ALL SELECT ';

CREATE TABLE #tbl_Latches(latch_class NVARCHAR(120),wait_time_ms BIGINT,waiting_requests_count BIGINT,[% of latches] NUMERIC(9,3));
CREATE TABLE #tbl_IOWaitsSnapshot(
database_id INT,file_id INT,sample_ms BIGINT,num_of_reads BIGINT,num_of_bytes_read BIGINT
,io_stall_read_ms BIGINT,num_of_writes BIGINT,num_of_bytes_written BIGINT,io_stall_write_ms BIGINT,io_stall BIGINT);
CREATE TABLE #tbl_WaitsSnapshot(
wait_type nvarchar(60),waiting_tasks_count bigint,wait_time_ms bigint,max_wait_time_ms bigint,
signal_wait_time_ms bigint,wait_xtp_recovery bigint,Dataset bit);

SELECT @V=CAST(CAST(SERVERPROPERTY('ProductVersion') as CHAR(2)) as NUMERIC), @S=REPLICATE('-',80);

PRINT 'Function Ctrl-6 Options: SQL Server Current State and Activity.
1. No options: Returns List of currently running tasks, list of all user opened trnsactions and Current Waits stats.
2. Letter "B": Returns only running blocking chains.
3. Letter "S": Returns data in simple mode without Query Plan.
4. Letter "W": Returns Only Current Waits stats.
5. Letter "W" + Number of seconds : Returns Waits Stats Delta (Including I/O). (Max=999) (Example: "W10" Returns Delta waits within 10 seconds).
6. Letter "M": Concentrate on Memory. Adds Memory allocation per database.
7. Letter "C": Concentrate on CPU Utilization. Adds dm_os_schedulers, dm_os_workers & dm_os_waiting_tasks.
8. Letter "I": Information about SQL Server, Windows and SQL Services. Current Trace Flags. Server configuration.
9. Letter "L": SQL Server Error Log in reversed order.
10. Letter "L" + Log File Number: Older SQL Server Error Log File. (F.i.:"L3")
11. Letters "LE": Search for errors in SQL Server Error Log.
12. Letter "L" + any words: Searches those words in SQL Server Error Log. (F.i.:''LDefault collation'')';
RAISERROR (@S,10,1) WITH NOWAIT

SET @Ex='N''BROKER_EVENTHANDLER'',N''BROKER_RECEIVE_WAITFOR'',N''BROKER_TASK_STOP'',N''BROKER_TO_FLUSH'',N''BROKER_TRANSMITTER'',N''CHECKPOINT_QUEUE'',
N''CHKPT'',N''CLR_AUTO_EVENT'',N''CLR_MANUAL_EVENT'',N''CLR_SEMAPHORE'',N''DBMIRROR_DBM_EVENT'',N''DBMIRROR_EVENTS_QUEUE'',
N''DBMIRROR_WORKER_QUEUE'',N''DBMIRRORING_CMD'',N''DIRTY_PAGE_POLL'',N''DISPATCHER_QUEUE_SEMAPHORE'',N''EXECSYNC'',N''FSAGENT'',
N''FT_IFTS_SCHEDULER_IDLE_WAIT'',N''FT_IFTSHC_MUTEX'',N''HADR_CLUSAPI_CALL'',N''HADR_FILESTREAM_IOMGR_IOCOMPLETION'',N''XE_DISPATCHER_WAIT'',
N''HADR_LOGCAPTURE_WAIT'',N''HADR_NOTIFICATION_DEQUEUE'',N''HADR_TIMER_TASK'',N''HADR_WORK_QUEUE'',N''KSOURCE_WAKEUP'',N''LAZYWRITER_SLEEP'',
N''LOGMGR_QUEUE'',N''ONDEMAND_TASK_QUEUE'',N''PWAIT_ALL_COMPONENTS_INITIALIZED'',N''QDS_PERSIST_TASK_MAIN_LOOP_SLEEP'',N''XE_TIMER_EVENT'',
N''QDS_SHUTDOWN_QUEUE'',N''REDO_THREAD_PENDING_WORK'',N''QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP'',N''REQUEST_FOR_DEADLOCK_SEARCH'',N''RESOURCE_QUEUE'',
N''SERVER_IDLE_CHECK'',N''SLEEP_BPOOL_FLUSH'',N''SLEEP_DBSTARTUP'',N''SLEEP_DCOMSTARTUP'',N''SLEEP_MASTERDBREADY'',N''SLEEP_MASTERMDREADY'',
N''SLEEP_MASTERUPGRADED'',N''SLEEP_MSDBSTARTUP'',N''SLEEP_SYSTEMTASK'',N''SLEEP_TASK'',N''SLEEP_TEMPDBSTARTUP'',N''SNI_HTTP_ACCEPT'',
N''SP_SERVER_DIAGNOSTICS_SLEEP'',N''SQLTRACE_BUFFER_FLUSH'',N''SQLTRACE_INCREMENTAL_FLUSH_SLEEP'',N''SQLTRACE_WAIT_ENTRIES'',N''WAIT_FOR_RESULTS'',
N''WAITFOR'',N''WAITFOR_TASKSHUTDOWN'',N''WAIT_XTP_HOST_WAIT'',N''WAIT_XTP_OFFLINE_CKPT_NEW_LOG'',N''WAIT_XTP_CKPT_CLOSE'',N''XE_DISPATCHER_JOIN''';

-- Active sessions Full, Simple and Blocked/Blockers (including Query plan)
IF IsNull(Upper(@Param),'') COLLATE database_default in ('B','S','')
BEGIN
SET @SQL=N'
;WITH DS AS (	
SELECT c.session_id, Blk_Id=r.blocking_session_id, r.wait_type, r.last_wait_type, r.wait_time,
r.wait_resource, [DB_Name]=DB_Name(r.database_id), s.login_name, r.command,
SUBSTRING(t.text
	, r.statement_start_offset/2 + CASE r.statement_start_offset WHEN 0 THEN 0 ELSE 1 END
	, ABS(
		CASE r.statement_end_offset
			WHEN -1 THEN DATALENGTH(t.text) ELSE r.statement_end_offset
		END - r.statement_start_offset
	)/2
) as Query_Text,'+
CASE ASCII(Upper(IsNull(@Param,'B'))) WHEN ASCII('B') THEN '
Query_Plan=tqp.query_plan, Batch_Plan=qp.query_plan,
' ELSE '' END+
'qmg.query_cost
,total_time_Min=(2 * (CAST(r.total_elapsed_time AS BIGINT) & 0x80000000) + r.total_elapsed_time) / 60000.
,Memory_Request=qmg.request_time
,qmg.requested_memory_kb
,qmg.granted_memory_kb
,qmg.required_memory_kb
,qmg.used_memory_kb
,qmg.max_used_memory_kb
,r.status
,r.cpu_time
,total_elapsed_time = 2 * (CAST(r.total_elapsed_time AS BIGINT) & 0x80000000) + r.total_elapsed_time
,Memory=r.granted_query_memory
,cnn_reads=c.num_reads
,c.last_read
,cnn_writes=c.num_writes
,c.last_write
,Batch_Text=t.text
,request_reads=r.reads
,request_writes=r.writes
,r.logical_reads
,s.host_name
,s.program_name
,c.client_net_address
FROM sys.[dm_exec_connections] c
CROSS APPLY sys.dm_exec_sql_text(c.[most_recent_sql_handle]) AS t
INNER JOIN sys.dm_exec_sessions s on c.session_id = s.session_id
LEFT JOIN sys.dm_exec_query_memory_grants qmg on c.session_id = qmg.session_id
LEFT JOIN sys.dm_exec_requests r on c.session_id = r.session_id
'+CASE ASCII(Upper(IsNull(@Param,'B'))) WHEN ASCII('B') THEN
'OUTER APPLY sys.dm_exec_query_plan(r.[plan_handle]) AS qp
OUTER APPLY sys.dm_exec_text_query_plan(r.[plan_handle],r.statement_start_offset,r.statement_end_offset) AS tqp
' ELSE '' END+' WHERE c.[most_recent_session_id] != @@spid and s.is_user_process = 1
) SELECT s1.* FROM DS as s1 ';

IF ASCII(Upper(@Param))=ASCII('B') -- Show only blocked and blockers
SET @SQL=@SQL+N' WHERE IsNull(s1.Blk_Id,0) > 0 '+@us+'s1.* FROM DS as s1 WHERE EXISTS (SELECT TOP 1 1 FROM DS as s2 WHERE s2.Blk_Id = s1.session_id)';
ELSE
SET @SQL=@SQL+N' WHERE s1.Blk_Id is not Null or EXISTS (SELECT TOP 1 1 FROM DS as s2 WHERE s2.Blk_Id = s1.session_id)';

SET @SQL=@SQL+N' ORDER BY Blk_Id DESC, s1.session_id'+@or;
	
PRINT @SQL;
RAISERROR (@S,10,1) WITH NOWAIT
EXEC (@SQL);
END

IF ASCII(@Param) is Null
BEGIN -- Active transaction list
SET @SQL=N'
SELECT DISTINCT s.session_id, a.transaction_id, a.name, c.client_net_address, se.login_name
,se.host_name, se.program_name
,Duration=CASE
	WHEN DATEDIFF(SECOND,a.transaction_begin_time,GETDATE()) >= 3600  THEN
		CAST(CAST(DATEDIFF(SECOND,a.transaction_begin_time,GETDATE()) /3600. as Decimal(7,2)) as VARCHAR) + '' Hr''
	WHEN DATEDIFF(SECOND,a.transaction_begin_time,GETDATE()) >= 60  THEN
		CAST(CAST(DATEDIFF(SECOND,a.transaction_begin_time,GETDATE()) /60. as Decimal(7,2)) as VARCHAR) + '' Min''
	ELSE
		CAST(CAST(DATEDIFF(MILLISECOND,a.transaction_begin_time,GETDATE()) /1000. as Decimal(7,2)) as VARCHAR) + '' Sec''
	END
,a.transaction_begin_time
,c.last_read
,c.last_write
,Delay_Min=CAST(DATEDIFF(SECOND,CASE WHEN c.last_read > c.last_write THEN c.last_write ELSE c.last_read END,GETDATE()) /60. as Decimal(7,2))
,CASE a.transaction_type
	WHEN 1 THEN ''Read/write transaction''
	WHEN 2 THEN ''Read-only transaction''
	WHEN 3 THEN ''System transaction''
	WHEN 4 THEN ''Distributed transaction''
END transaction_type
, CASE a.transaction_state
	WHEN 0 THEN ''The transaction has not been completely initialized yet.''
	WHEN 1 THEN ''The transaction has been initialized but has not started.''
	WHEN 2 THEN ''transaction is active.''
	WHEN 3 THEN ''The transaction has ended. This is used for read-only transactions.''
	WHEN 4 THEN ''The commit process has been initiated on the distributed transaction. The distributed transaction is still active but further processing cannot take place.''
	WHEN 5 THEN ''The transaction is in a prepared state and waiting resolution.''
	WHEN 6 THEN ''The transaction has been committed.''
	WHEN 7 THEN ''The transaction is being rolled back.''
	WHEN 8 THEN ''The transaction has been rolled back.''
END transaction_state
,dtc_state=CASE a.dtc_state WHEN 1 THEN ''ACTIVE'' WHEN 2 THEN ''PREPARED'' WHEN 3 THEN ''COMMITTED'' WHEN 4 THEN ''ABORTED'' WHEN 5 THEN ''RECOVERED'' END
,[Database ID]=dt.database_id
,[Database Name]=DB_Name(dt.database_id)
,dt.database_transaction_begin_time
,database_transaction_type=CASE dt.database_transaction_type WHEN 1 THEN ''Read/write transaction'' WHEN 2 THEN ''Read-only transaction'' WHEN 3 THEN ''System transaction'' END
,database_transaction_state=CASE dt.database_transaction_state
	WHEN 1 THEN ''The transaction has not been initialized.''
	WHEN 3 THEN ''The transaction has been initialized but has not generated any log records.''
	WHEN 4 THEN ''The transaction has generated log records.''
	WHEN 5 THEN ''The transaction has been prepared.''
	WHEN 10 THEN ''The transaction has been committed.''
	WHEN 11 THEN ''The transaction has been rolled back.''
	WHEN 12 THEN ''The transaction is being committed. In this state the log record is being generated, but it has not been materialized or persisted.''
END
,[Initiator]=CASE s.is_user_transaction WHEN 0 THEN ''System'' ELSE ''User'' END
,[Is_Local]=CASE s.is_local WHEN 0 THEN ''No'' ELSE ''Yes'' END
,cnn_reads=c.num_reads
,cnn_writes=c.num_writes
,dt.database_transaction_log_record_count
,dt.database_transaction_log_bytes_used
,dt.database_transaction_log_bytes_reserved
,dt.database_transaction_log_bytes_used_system
,dt.database_transaction_log_bytes_reserved_system
,dt.database_transaction_begin_lsn
,dt.database_transaction_last_lsn
,[Transaction_Text]=IsNull((SELECT text FROM sys.dm_exec_sql_text(sp.[sql_handle])),'''')
FROM sys.dm_tran_active_transactions a
LEFT JOIN sys.dm_tran_session_transactions s ON a.transaction_id=s.transaction_id
LEFT JOIN sys.[dm_exec_connections] c ON s.session_id  = c.session_id
LEFT JOIN sys.dm_exec_sessions se on c.session_id = se.session_id
LEFT JOIN sys.dm_tran_database_transactions dt
	ON a.transaction_id = dt.transaction_id
LEFT JOIN sys.sysprocesses as sp ON sp.spid = s.session_id
WHERE s.session_id is Not Null
ORDER BY a.transaction_begin_time, s.session_id
'+@or;

PRINT @SQL;
RAISERROR (@S,10,1) WITH NOWAIT
EXEC (@SQL);
END

IF IsNull(Upper(@Param),'') COLLATE database_default in ('M','S','')
BEGIN
	SET @SQL='
;WITH RAM as (
SELECT system_memory_state_desc
,tpm=CAST(CAST(Round(total_physical_memory_kb / 1048576.,3) as FLOAT) as VARCHAR) + '' Gb''
,apm=CAST(CAST(Round(available_physical_memory_kb / 1048576.,3) as FLOAT) as VARCHAR) + '' Gb''
,amp=CAST(CAST(Round(available_physical_memory_kb * 100. / total_physical_memory_kb,2) as FLOAT) as VARCHAR) + '' %''
,tpf=CAST(CAST(Round(total_page_file_kb / 1048576.,3) as FLOAT) as VARCHAR) + '' Gb''
,apf=CASE total_page_file_kb WHEN 0 THEN ''0 Gb'' ELSE CAST(CAST(Round(available_page_file_kb / 1048576.,3) as FLOAT) as VARCHAR) END + '' Gb''
,mu=CASE total_page_file_kb WHEN 0 THEN ''0 Gb'' ELSE CAST(CAST(Round((total_page_file_kb - available_page_file_kb) / 1048576.,3) as FLOAT) as VARCHAR) END + '' Gb''
,map=CASE total_page_file_kb WHEN 0 THEN ''0 %'' ELSE CAST(CAST(Round(available_page_file_kb * 100. / total_page_file_kb,2) as FLOAT) as VARCHAR) END + '' %''
,sc=CAST(CAST(Round(system_cache_kb / 1048576.,3) as FLOAT) as VARCHAR) + '' Gb''
,kpp=CAST(CAST(Round(kernel_paged_pool_kb / 1024.,3) as FLOAT) as VARCHAR) + '' Mb''
,knp=CAST(CAST(Round(kernel_nonpaged_pool_kb / 1024.,3) as FLOAT) as VARCHAR) + '' Mb''
,pfs=CAST(CAST(Round((total_page_file_kb - total_physical_memory_kb)/ 1048576.,3) as FLOAT) as VARCHAR) + '' Gb''
,pfu=CAST(CAST(Round((total_page_file_kb - total_physical_memory_kb + available_physical_memory_kb - available_page_file_kb) / 1048576.,3) as FLOAT) as VARCHAR) + '' Gb''
FROM sys.dm_os_sys_memory
),
OS_RAM as (
SELECT
pmiu=CAST(CAST(Round(physical_memory_in_use_kb / 1048576.,3) as FLOAT) as VARCHAR) + '' Gb''
,lpa=CAST(CAST(Round(locked_page_allocations_kb / 128.,3) as FLOAT) as VARCHAR) + '' Mb''
,pfc=CAST(page_fault_count as VARCHAR)
,mup=CAST(memory_utilization_percentage as VARCHAR) + '' %''
,acl=CAST(CAST(Round(available_commit_limit_kb / 1048576.,3) as FLOAT) as VARCHAR) + '' Gb''
,ppml=CASE process_physical_memory_low WHEN 0 THEN ''OK'' ELSE ''LOW'' END
,pvml=CASE process_virtual_memory_low WHEN 0 THEN ''OK'' ELSE ''LOW'' END
FROM sys.dm_os_process_memory
),
HitRatio as (
	SELECT cntr_value,rb=CASE counter_name WHEN ''Buffer cache hit ratio'' THEN ''r'' ELSE ''b'' END
	FROM sys.dm_os_performance_counters
	WHERE OBJECT_NAME like ''%Buffer Manager%''
	and counter_name in (''Buffer cache hit ratio'',''Buffer cache hit ratio base'')
),
MemoryDataCollection as (
SELECT [Counter Name]=''Servername''
,[Counter Value]=SERVERPROPERTY(''Servername'')
,id=-100
,[Source]=''Serverproperty''
UNION ALL
SELECT  ''Last Start'', CONVERT(CHAR(23), sqlserver_start_time, 121), -90, ''sys.dm_os_sys_info'' FROM sys.dm_os_sys_info
UNION ALL
SELECT tt.field, tt.ff, id, ''sys.dm_os_sys_memory'' FROM RAM as m
CROSS APPLY (VALUES
	(system_memory_state_desc, ''Memory State'', 10)
	,(tpm,''Physical Memory'', 20)
	,(apm,''Available Memory'', 30)
	,(amp,''Available Memory %'', 40)
	,(pfs,''Page File Size'', 44)
	,(pfu,''Page File Usage'', 46)
	,(tpf,''Total Memory Size'', 50)
	,(apf,''Free Memory'', 60)
	,(mu,''Memory Usage'', 70)
	,(map,''Memory Available %'', 80)
	,(sc,''System Cache'', 90)
	,(kpp,''Kernel Paged'', 100)
	,(knp,''Kernel Non-Paged'', 120)
) tt (ff, field, id)
UNION ALL
SELECT tt.field, tt.ff, id, ''sys.dm_os_process_memory'' FROM OS_RAM as m
CROSS APPLY (VALUES
	(pmiu,''Memory in Use'', 210)
	,(lpa,''Locked Page Allocations'', 220)
	,(pfc,''page_fault_count'', 230)
	,(mup,''Memory Utilization %'', 240)
	,(acl,''Available Commit Limit'', 250)
	,(ppml,''Physical Memory State'', 260)
	,(pvml,''Virtual Memory State'', 270)
) tt (ff, field, id)
UNION ALL
SELECT 	CASE WHEN [counter_name] in (''Total Server Memory (KB)'',''Target Server Memory (KB)'',''SQL Cache Memory (KB)'')
	THEN LEFT([counter_name],LEN([counter_name])-5) ELSE [counter_name] END,
	CASE WHEN [counter_name] = ''Total Server Memory (KB)'' THEN CAST(CAST(Round([cntr_value] / 1048576.,3) as FLOAT) as VARCHAR) + '' GB''
		WHEN [counter_name] = ''Target Server Memory (KB)'' THEN CAST(CAST(Round([cntr_value] / 1048576.,3) as FLOAT) as VARCHAR) + '' GB''
		WHEN [counter_name] = ''SQL Cache Memory (KB)'' THEN CAST(CAST(Round([cntr_value] / 1024.,3) as FLOAT) as VARCHAR) + '' MB''
	ELSE CAST([cntr_value] as VARCHAR) END, 1000, ''sys.dm_os_performance_counters''
FROM sys.dm_os_performance_counters
WHERE ([object_name] LIKE ''%Buffer Manager%''
	AND [counter_name] in (''Page life expectancy'',''Page reads/sec'', ''Page writes/sec'', ''Lazy writes/sec'')
	) OR ( [object_name] LIKE ''%Memory Manager%''
	AND [counter_name] in (''Memory Grants Pending'', ''Total Server Memory (KB)'', ''SQL Cache Memory (KB)'',''Target Server Memory (KB)'')
)
UNION ALL
SELECT ''Buffer cache hit ratio, %'', CAST(CAST((SELECT cntr_value FROM HitRatio WHERE rb = ''r'')*100./
	(SELECT cntr_value FROM HitRatio WHERE rb = ''b'') as DECIMAL(6,2)) AS VARCHAR) + '' %''
	, 1000, ''sys.dm_os_performance_counters''
)
SELECT [Counter Name], [Counter Value], [Source]
FROM MemoryDataCollection
ORDER BY id, [Counter Name]'+@or;

PRINT CAST(@SQL as VARCHAR(8000));
RAISERROR (@S,10,1) WITH NOWAIT
EXEC (@SQL);
END

IF ASCII(Upper(@Param))=ASCII('M')
BEGIN
-- Memory usage by database
SET @SQL='SELECT [Database Name]=DB_NAME(database_id)
,[Cached Size (GB)]=CAST(COUNT(*) / 131072.0 as DECIMAL(7,3))
FROM sys.dm_os_buffer_descriptors
WHERE database_id <> 32767 -- ResourceDB
GROUP BY DB_NAME(database_id)
ORDER BY [Cached Size (GB)] DESC'+@or;
PRINT @SQL;
RAISERROR (@S,10,1) WITH NOWAIT
EXEC (@SQL);
END

IF IsNull(Upper(@Param),'') COLLATE database_default in ('W','S','')
BEGIN		
-- Latches
SET @SQL='SELECT latch_class,wait_time_ms,waiting_requests_count
,[% of latches]=CASE SUM (wait_time_ms) OVER() WHEN 0 THEN 0
	ELSE 100.0 * wait_time_ms / SUM (wait_time_ms) OVER() END
FROM sys.dm_os_latch_stats
WHERE latch_class NOT IN (''BUFFER'')'+@or;

PRINT @SQL;
RAISERROR (@S,10,1) WITH NOWAIT
INSERT INTO #tbl_Latches EXEC (@SQL);

SET @SQL='WITH [Waits] AS
(SELECT [wait_type]
	,[WaitS]=[wait_time_ms] / 1000.0
	,[ResourceS]=([wait_time_ms] - [signal_wait_time_ms]) / 1000.0
	,[SignalS]=[signal_wait_time_ms] / 1000.0
	,[WaitCount]=[waiting_tasks_count]
	,[Percentage]=100.0 * [wait_time_ms] / SUM ([wait_time_ms]) OVER()
	,[RowNum]=ROW_NUMBER() OVER(ORDER BY [wait_time_ms] DESC)
FROM sys.dm_os_wait_stats
WHERE [wait_type] NOT IN ('+@Ex COLLATE database_default +') AND [waiting_tasks_count] > 0
)
SELECT [WaitType]=MAX ([W1].[wait_type])
	,[Wait_S]=CAST (MAX ([W1].[WaitS]) AS DECIMAL (16,2))
	,[Resource_S]=CAST (MAX ([W1].[ResourceS]) AS DECIMAL (16,2))
	,[Signal_S]=CAST (MAX ([W1].[SignalS]) AS DECIMAL (16,2))
	,[WaitCount]=MAX ([W1].[WaitCount])
	,[Percentage]=CAST (MAX ([W1].[Percentage]) AS DECIMAL (5,2))
	,[AvgWait_S]=CAST ((MAX ([W1].[WaitS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4))
	,[AvgRes_S]=CAST ((MAX ([W1].[ResourceS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4))
	,[AvgSig_S]=CAST ((MAX ([W1].[SignalS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4))
FROM [Waits] AS [W1]
INNER JOIN [Waits] AS [W2]
		ON [W2].[RowNum] <= [W1].[RowNum]
GROUP BY [W1].[RowNum]
HAVING SUM ([W2].[Percentage]) - MAX ([W1].[Percentage]) < 95 -- percentage threshold
'+@or;

PRINT @SQL;
RAISERROR (@S,10,1) WITH NOWAIT
EXEC (@SQL);

SELECT TOP 5 * FROM #tbl_Latches WHERE wait_time_ms > 0 ORDER BY [% of latches] DESC;
END

IF ASCII(Upper(@Param))=ASCII('W') and IsNumeric(SUBSTRING(@Param,2,4))=1
BEGIN
	DECLARE @m VARCHAR(100), @d as datetime;

	SELECT @m='Waiting for '+SUBSTRING(@Param,2,4) COLLATE database_default +' second(s).',
	 @d=DATEADD(second,CAST(SUBSTRING(@Param,2,4) as INT),'1900-01-01')

SET @SQL='SELECT 1';

IF @V>=11
BEGIN
SET @SQL='SELECT s.database_id, s.file_id
, s.sample_ms
, s.num_of_reads
, s.num_of_bytes_read
, s.io_stall_read_ms
, s.num_of_writes
, s.num_of_bytes_written
, s.io_stall_write_ms
, s.io_stall
FROM sys.dm_io_virtual_file_stats(NULL, NULL) as s
CROSS APPLY sys.dm_os_volume_stats(s.database_id, s.[file_id]) AS vs' + @or;

PRINT @SQL;
RAISERROR (@S,10,1) WITH NOWAIT
INSERT INTO #tbl_IOWaitsSnapshot EXEC (@SQL);
END

SET @SQL='SELECT wait_type, waiting_tasks_count, wait_time_ms, max_wait_time_ms, signal_wait_time_ms, Dataset=0
FROM sys.dm_os_wait_stats
WHERE [waiting_tasks_count] > 0'+@or;

PRINT @SQL;
RAISERROR (@S,10,1) WITH NOWAIT
INSERT INTO #tbl_WaitsSnapshot(wait_type, waiting_tasks_count, wait_time_ms, max_wait_time_ms, signal_wait_time_ms, Dataset)
EXEC (@SQL);

RAISERROR (@m,10,1) WITH NOWAIT
BEGIN TRY
IF @V>=11
SELECT * INTO #AlertsCount FROM msdb.dbo.sysalerts_performance_counters_view;
END TRY
BEGIN CATCH
PRINT 'You do not have rights to "msdb" database.'
END CATCH
WAITFOR DELAY @d

SET @SQL=';WITH t1 as (
SELECT wait_type, waiting_tasks_count, wait_time_ms, max_wait_time_ms, signal_wait_time_ms
FROM sys.dm_os_wait_stats WHERE [waiting_tasks_count] > 0
)
SELECT TOP 10 t1.wait_type
,waiting_tasks_count=t1.waiting_tasks_count - IsNull(t0.waiting_tasks_count,0)
,wait_time_ms=t1.wait_time_ms - IsNull(t0.wait_time_ms,0)
,t1.max_wait_time_ms
,signal_wait_time_ms=t1.signal_wait_time_ms - IsNull(t0.signal_wait_time_ms,0)
FROM t1 LEFT JOIN ( SELECT * FROM #tbl_WaitsSnapshot WHERE Dataset = 0)  as t0
	ON t1.wait_type = t0.wait_type
WHERE (t1.waiting_tasks_count + t1.wait_time_ms - IsNull(t0.wait_time_ms,0) - IsNull(t0.waiting_tasks_count,0)) > 0
	and t1.wait_type NOT IN ('+@Ex COLLATE database_default +')
ORDER BY 3 DESC'+@or;
EXEC (@SQL);

SET @SQL='SELECT l.latch_class
,wait_time_ms=l.wait_time_ms-IsNull(t.wait_time_ms,0)
,waiting_requests_count=l.waiting_requests_count-IsNull(t.waiting_requests_count,0)
FROM sys.dm_os_latch_stats as l
LEFT JOIN #tbl_Latches as t
	ON l.latch_class = t.latch_class
WHERE l.latch_class NOT IN (''BUFFER'') AND ( l.wait_time_ms-IsNull(t.wait_time_ms,0) > 0 OR
		l.waiting_requests_count-IsNull(t.waiting_requests_count,0) > 0)'+@or;
EXEC (@SQL);


BEGIN TRY
IF @V>=11
SELECT  a1.object_name, a1.counter_name, a1.instance_name
	,Counter_Increase=a1.cntr_value - a2.cntr_value
FROM msdb.dbo.sysalerts_performance_counters_view as a1
INNER JOIN #AlertsCount as a2 ON a1.object_name = a2.object_name AND
	a1.counter_name = a2.counter_name AND a1.instance_name = a2.instance_name
WHERE a1.cntr_value - a2.cntr_value > 0
and (
	(a1.object_name = 'Locks' and a1.counter_name = 'Lock Requests/sec')
	 OR (a1.object_name = 'Databases' and a1.counter_name in ('Log Bytes Flushed/sec','Transactions/sec','Log Flushes/sec'))
	 OR (a1.object_name = 'Access Methods' and a1.counter_name in ('Index Searches/sec','Probe Scans/sec','Range Scans/sec','Worktables Created/sec','Full Scans/sec','Page Splits/sec'))
	 OR (a1.object_name = 'SQL Statistics' and a1.counter_name in ('Batch Requests/sec','SQL Compilations/sec'))
	 OR (a1.object_name = 'Buffer Manager' and a1.counter_name in ('Page lookups/sec','Page writes/sec'))
) and a1.instance_name != '_Total'
ORDER BY 1,2,4 DESC
END TRY
BEGIN CATCH
PRINT 'You do not have rights to "msdb" database.'
END CATCH

SET @SQL='
SELECT TOP 5 Database_Name=DB_NAME(s.database_id)
	,[File_Name]=mf.name
	,[Mount Point]=vs.volume_mount_point
	,[Read Wait, ms]=CAST(CASE s.num_of_reads - t.num_of_reads WHEN 0 THEN 0 ELSE (s.io_stall_read_ms - t.io_stall_read_ms)* 1. / (s.num_of_reads - t.num_of_reads) END as DECIMAL(19,2))
	,[Mb Read]=CAST((s.num_of_bytes_read - t.num_of_bytes_read) / 1048576. as DECIMAL(19,2))
	,[Write Wait, ms]=CAST(CASE s.num_of_writes - t.num_of_writes WHEN 0 THEN 0 ELSE (s.io_stall_write_ms - t.io_stall_write_ms)* 1. / (s.num_of_writes - t.num_of_writes) END as DECIMAL(19,2))
	,[Mb Written]=CAST((s.num_of_bytes_written - t.num_of_bytes_written) / 1048576. as DECIMAL(19,2))
	,[Sample Time, sec]=CAST((s.sample_ms - t.sample_ms) / 1000. as DECIMAL(9,3))
	,[Total IO Stall, ms]=s.io_stall - t.io_stall
FROM sys.dm_io_virtual_file_stats(NULL, NULL) as s
CROSS APPLY sys.dm_os_volume_stats(s.database_id, s.[file_id]) AS vs
INNER JOIN #tbl_IOWaitsSnapshot as t ON s.database_id = t.database_id and t.file_id = s.file_id
INNER JOIN sys.master_files as mf ON s.database_id = mf.database_id and mf.file_id = s.file_id
WHERE (s.num_of_reads - t.num_of_reads + s.num_of_writes - t.num_of_writes) > 0
	and (
		CASE s.num_of_reads - t.num_of_reads WHEN 0 THEN 0 ELSE (s.io_stall_read_ms - t.io_stall_read_ms) / (s.num_of_reads - t.num_of_reads) END > 1 or
		CASE s.num_of_writes - t.num_of_writes WHEN 0 THEN 0 ELSE (s.io_stall_write_ms - t.io_stall_write_ms) / (s.num_of_writes - t.num_of_writes) END > 1)
ORDER BY (s.io_stall_read_ms + s.io_stall_write_ms - t.io_stall_write_ms - t.io_stall_read_ms) / (s.num_of_reads + s.num_of_writes - t.num_of_writes - t.num_of_reads) DESC
'+@or

IF @V>=11
BEGIN
RAISERROR (@S,10,1) WITH NOWAIT
EXEC (@SQL);
END

END

IF IsNull(Upper(@Param),'') COLLATE database_default in ('C','S','')
BEGIN		
SET @SQL='
DECLARE @gc VARCHAR(MAX), @gi VARCHAR(MAX);
WITH BR_Data as (
	SELECT timestamp, record=CONVERT(XML, record)
	FROM sys.dm_os_ring_buffers
	WHERE ring_buffer_type =N''RING_BUFFER_SCHEDULER_MONITOR'' and record like ''%<SystemHealth>%''
), Extracted_XML as (
	SELECT timestamp, record_id=record.value(''(./Record/@id)[1]'', ''int'')
	,SystemIdle=record.value(''(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]'', ''bigint'')
	,SQLCPU=record.value(''(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]'', ''bigint'')
	FROM BR_Data
), CPU_Data as (
	SELECT record_id, rn=ROW_NUMBER() OVER(ORDER BY record_id)
	,EventTime=dateadd(ms, -1 * ((SELECT ms_ticks  FROM sys.dm_os_sys_info) - [timestamp]), GETDATE())
	,SQLCPU, SystemIdle
	,OtherCPU=100 - SystemIdle - SQLCPU
	FROM Extracted_XML), MX as (SELECT MAX(rn) m FROM CPU_Data)
SELECT @gc = CAST((SELECT CAST(d1.rn-m.m as VARCHAR) + '' '' + CAST(d1.SQLCPU as VARCHAR) + '','' FROM CPU_Data as d1, MX as m ORDER BY d1.rn FOR XML PATH('''')) as VARCHAR(MAX)),
@gi = CAST((SELECT CAST(d1.rn-m.m as VARCHAR) + '' '' + CAST(d1.OtherCPU as VARCHAR) + '','' FROM CPU_Data as d1, MX as m ORDER BY d1.rn FOR XML PATH('''')) as VARCHAR(MAX))
'+@or+'
SELECT Measure=CAST(''LINESTRING('' + LEFT(@gc,LEN(@gc)-1) + '')'' as GEOMETRY), ''SQL CPU %''
UNION ALL
SELECT CAST(''LINESTRING(1 100,2 100)'' as GEOMETRY), ''''
UNION ALL
SELECT CAST(''LINESTRING('' + LEFT(@gi,LEN(@gi)-1) + '')'' as GEOMETRY), ''Other CPU %'''+@or;
PRINT @SQL;
RAISERROR (@S,10,1) WITH NOWAIT
EXEC (@SQL);

IF  ASCII(Upper(@Param))=ASCII('C')
BEGIN
	SET @SQL='
/* Activity By CPU */
WITH CPUStats as (	SELECT s.cpu_id,
	Suspended = SUM(CASE w.wait_started_ms_ticks WHEN 0 THEN 0 ELSE i.ms_ticks - w.wait_started_ms_ticks END),
	Runnable = SUM(CASE w.wait_resumed_ms_ticks WHEN 0 THEN 0 ELSE i.ms_ticks - w.wait_resumed_ms_ticks END)
FROM sys.dm_os_workers AS w CROSS JOIN sys.dm_os_sys_info AS i
	INNER JOIN sys.dm_os_schedulers as s ON w.scheduler_address = s.scheduler_address
GROUP BY s.cpu_id)
SELECT * FROM CPUStats WHERE Suspended + Runnable > 10 ORDER BY cpu_id'+@or+'
	
/*
	how long a worker has been running in a SUSPENDED or RUNNABLE state
	https://msdn.microsoft.com/en-us/library/ms178626.aspx
*/
SELECT r.session_id
	,status=CONVERT(varchar(10), r.status)
	,command=CONVERT(varchar(15), r.command)
	,worker_state=CONVERT(varchar(10), w.state)
	,Suspended=CASE w.wait_started_ms_ticks WHEN 0 THEN 0 ELSE i.ms_ticks - w.wait_started_ms_ticks END
	,Runnable=CASE w.wait_resumed_ms_ticks WHEN 0 THEN 0 ELSE i.ms_ticks - w.wait_resumed_ms_ticks END
	,t.wait_type
	,t.wait_duration_ms
	,s.cpu_id
	,w.context_switch_count
	,w.pending_io_count
	,w.pending_io_byte_count
	,w.pending_io_byte_average
FROM sys.dm_exec_requests AS r
FULL JOIN sys.dm_os_waiting_tasks as t on r.session_id = t.session_id
FULL JOIN sys.dm_os_workers AS w ON w.task_address = r.task_address
INNER JOIN sys.dm_os_schedulers as s ON w.scheduler_address = s.scheduler_address
CROSS JOIN sys.dm_os_sys_info AS i
WHERE r.scheduler_id IS NOT NULL
ORDER BY Runnable Desc'+@or;
	PRINT @SQL;
	RAISERROR (@S,10,1) WITH NOWAIT
	EXEC (@SQL);
END--Param C

END

IF ASCII(Upper(@Param))=ASCII('I')
BEGIN
SET @SQL=N'; WITH si as (SELECT *
	' + CASE WHEN @V=10 THEN ',physical_memory_gb=CAST(CAST(physical_memory_in_bytes/1073741824. AS DECIMAL(6,2)) as VARCHAR)
	,committed_target_gb=''N/A''' ELSE '
	,physical_memory_gb=CAST(CAST(physical_memory_kb/1048576. AS DECIMAL(6,2)) as VARCHAR)
	,committed_target_gb=CAST(CAST(committed_target_kb/1048576. AS DECIMAL(6,2)) as VARCHAR)' END
+' FROM sys.dm_os_sys_info)
'+CASE WHEN @V>11 THEN ', Win_Info as (
		SELECT windows_release, windows_service_pack_level, windows_sku, os_language_version
		FROM sys.dm_os_windows_info)
	, hadr as (SELECT cluster_name, quorum_type_desc, quorum_state_desc FROM sys.dm_hadr_cluster)' ELSE '' END +'
SELECT 0 as ParamOrder,Parameter=''Servername'',Value=SERVERPROPERTY(''Servername'')
'+@us+'10, ''MachineName'', SERVERPROPERTY(''MachineName'')
'+@us+'20, ''HostName'', SERVERPROPERTY(''ComputerNamePhysicalNetBIOS'')
'+@us+'30, ''SQL Server Version'', @@VERSION
'+@us+'40, ''Edition'', SERVERPROPERTY(''Edition'')
'+@us+'60, ''Product Level'', SERVERPROPERTY(''ProductLevel'')
'+@us+'70, ''Install Date'', CONVERT(CHAR(23), MIN(create_date), 121) FROM sys.server_principals WHERE name =N''NT AUTHORITY\SYSTEM'' OR name =N''NT AUTHORITY\NETWORK SERVICE''
'+@us+'80, ''Collation'', DATABASEPROPERTYEX(''master'', ''Collation'')
'+@us+'90, ''Clustered'', CASE SERVERPROPERTY(''IsClustered'') WHEN 1 THEN ''Yes'' ELSE ''No'' END
'+@us+'100, ''AlwaysOn'', CASE SERVERPROPERTY(''HadrManagerStatus'') WHEN 1 THEN ''Enabled'' ELSE ''Disabled'' END
'+@us+'110, ''SQL Server Start Time'', CONVERT(CHAR(23), sqlserver_start_time, 121) FROM si
'+@us+'120, ''Logical CPU Count'', CAST(cpu_count as VARCHAR) FROM si
'+@us+'130, ''Hyperthread Ratio'', CAST(hyperthread_ratio as VARCHAR) FROM si
'+@us+'140, ''Physical CPU Count'', CAST(cpu_count/hyperthread_ratio as VARCHAR) FROM si
'+@us+'150, ''Physical Memory (GB)'', physical_memory_gb FROM si
'+@us+'160, ''Committed Target (GB)'', committed_target_gb FROM si
'+@us+'170, ''Max Workers Count'', CAST(max_workers_count as VARCHAR) FROM si
'+CASE WHEN @V>=11 THEN @us+'180, ''Affinity Type'', affinity_type_desc FROM si' ELSE '' END +'
'+@us+'190, ''Security Authentication'', CASE SERVERPROPERTY(''IsIntegratedSecurityOnly'') WHEN 0 THEN ''Windows & SQL'' WHEN 1 THEN ''Windows Only'' ELSE ''N/A'' END
'+@us+'200, ''Single User Mode'', CASE SERVERPROPERTY(''IsSingleUser'') WHEN 1 THEN ''Yes'' ELSE ''No'' END
'+@us+'210, ''In-Memory OLTP'', CASE SERVERPROPERTY(''IsXTPSupported'') WHEN 1 THEN ''Supported'' ELSE ''N/A'' END
'+@us+'220, ''Polybase'', CASE SERVERPROPERTY(''IsPolybaseInstalled'') WHEN 1 THEN ''Installed'' ELSE ''N/A'' END
'+@us+'230, ''Full Text Search'', CASE SERVERPROPERTY(''IsFullTextInstalled'') WHEN 1 THEN ''Installed'' ELSE ''N/A'' END
' + CASE WHEN @V>=11 THEN @us+'500, ''Windows Release'', CAST(windows_release as VARCHAR) FROM Win_Info
'+@us+'510, ''Windows Pack Level'', CAST(windows_service_pack_level as VARCHAR) FROM Win_Info
'+@us+'520, ''Windows SKU'', CAST(windows_sku as VARCHAR) FROM Win_Info
'+@us+'530, ''OS Language Version'', CAST(os_language_version as VARCHAR) FROM Win_Info
'+@us+'540, ''Cluster Name'', CAST(cluster_name as VARCHAR) FROM hadr
'+@us+'550, ''Quorum Type'', CAST(quorum_type_desc as VARCHAR) FROM hadr
'+@us+'560, ''Quorum State'', CAST(quorum_state_desc as VARCHAR) FROM hadr' ELSE '' END +'
ORDER BY ParamOrder'+@or;

PRINT @SQL
RAISERROR (@S,10,1) WITH NOWAIT
EXEC (@SQL)

IF @V>=11
BEGIN
SET @SQL=N'SELECT [Cluster Member]=IsNull(cm.member_name, cn.member_name)
,[Cluster Member Type]=cm.member_type_desc
,[State]=cm.member_state_desc
,[Quorum Votes]=cm.number_of_quorum_votes
,[Subnet IP]=cn.network_subnet_ip
,[Subnet Mask]=cn.network_subnet_ipv4_mask + ''/'' + CAST(cn.network_subnet_prefix_length as VARCHAR)
FROM sys.dm_hadr_cluster_members as cm
FULL JOIN sys.dm_hadr_cluster_networks as cn ON cm.member_name = cn.member_name'+@or;
PRINT @SQL
RAISERROR (@S,10,1) WITH NOWAIT
EXEC (@SQL)

SET @SQL=N'DBCC TRACESTATUS (-1) WITH NO_INFOMSGS;
SELECT servicename, process_id, startup_type_desc, status_desc,
last_startup_time, service_account, is_clustered, cluster_nodename, [filename]
FROM sys.dm_server_services'+@or;
PRINT @SQL
RAISERROR (@S,10,1) WITH NOWAIT
EXEC (@SQL)
END

SET @SQL=N'SELECT * FROM sys.configurations ORDER BY name '+@or;
PRINT @SQL
RAISERROR (@S,10,1) WITH NOWAIT
EXEC (@SQL)
END

BEGIN TRY
IF ASCII(Upper(@Param))=ASCII('L')
BEGIN
SET @SQL=N'EXEC sys.xp_readerrorlog';

SET @Param=SUBSTRING(@Param,2,100)

IF IsNumeric(@Param)=1 SET @SQL+=' '+@Param
SET @SQL+=';/*Reversed*/'
PRINT 'INSERT INTO #CurLog '+@SQL
RAISERROR (@S,10,1) WITH NOWAIT

INSERT INTO #CurLog EXEC (@SQL);

SET @SQL='SELECT l1.LogDate, l1.ProcessInfo, TEXT=IsNull(l2.Text+'' '','''')+l1.Text
FROM #CurLog as l1 '+CASE WHEN ASCII(Upper(@Param))=ASCII('E') THEN 'INNER' ELSE 'LEFT' END
+' JOIN #CurLog as l2 ON l2.text like ''Error:%''
	AND l1.LogDate  = l2.LogDate and l2.ProcessInfo = l1.ProcessInfo
WHERE l1.text NOT like ''Error:%'''+ CASE WHEN IsNumeric(@Param)=0 and LEN(@Param)>1 THEN '
	AND IsNull(l2.Text+'' '','''')+l1.Text like ''%'+@Param+'%''' ELSE '' END
+' ORDER BY l1.LogDate DESC;'

PRINT @SQL
RAISERROR (@S,10,1) WITH NOWAIT
EXEC (@SQL)

END
END TRY
BEGIN CATCH
PRINT 'You do not have rights to read ERRORLOG.'
END CATCH

RETURN;
GO
EXEC #USP_PROCESS 