/* 8 - 2019-06-06 Query Cache
Consolidated by Slava Murygin
http://slavasql.blogspot.com/2016/02/ssms-query-shortcuts.html */

IF OBJECT_ID('tempdb..#USP_GETCACHE') IS NOT NULL
DROP PROCEDURE #USP_GETCACHE;
GO
IF OBJECT_ID('tempdb..#CharElements') IS NOT NULL
DROP TABLE #CharElements;
GO
IF OBJECT_ID('tempdb..#TextTable') IS NOT NULL
DROP TABLE #TextTable;
GO
CREATE TABLE #TextTable (
	CValue CHAR(1), /* Character */
	Position INT, /* Position */
	ValueSet INT);
GO
CREATE TABLE #CharElements (/* Table of Char to Elements relationship */
	CValue CHAR(1), /* Character */
	ElementType INT, /*1,2,3*/
	x1 INT, y1 INT,
	x2 INT, y2 INT,
	x3 INT, y3 INT);
GO
INSERT INTO #TextTable(ValueSet, CValue,Position) VALUES
(1,'I',1),(1,'/',2),(1,'O',3), (1,'1',4)
,(2,'T',1),(2,'I',2),(2,'M',3),(2,'E',4),(2,'2',5)
,(3,'S',1),(3,'I',2),(3,'Z',3),(3,'E',4),(3,' ',5)
,(3,'=',6),(3,'>',7),(3,' ',8),(3,'N',9),(3,'U',10)
,(3,'M',11),(3,'B',12),(3,'E',13),(3,'R',14),(3,' ',15)
,(3,'O',16),(3,'F',17),(3,' ',18),(3,'E',19),(3,'X',20)
,(3,'E',21),(3,'C',22),(3,'U',23),(3,'T',24),(3,'I',25)
,(3,'O',26),(3,'N',27),(3,'S',28);
GO
INSERT INTO #CharElements(CValue, ElementType, x1, y1, x2, y2, x3, y3) VALUES
('B',2,0,0,0,8,0,0),('B',2,0,0,2,0,0,0),('B',2,0,8,2,8,0,0),('B',2,0,4,2,4,0,0),('B',3,2,0,4,2,2,4),('B',3,2,8,4,6,2,4),
('/',2,0,0,3,8,0,0),(' ',0,5,0,0,0,0,0),
('=',2,0,2,4,2,0,0),('=',2,0,6,4,6,0,0),
('>',2,0,0,4,4,0,0),('>',2,0,8,4,4,0,0),
('T',2,3,0,3,8,0,0),('T',2,0,8,6,8,0,0),
('I',2,1,0,1,8,0,0),('I',2,0,0,2,0,0,0),('I',2,0,8,2,8,0,0),
('M',2,0,0,0,8,0,0),('M',2,0,8,3,4,0,0),('M',2,3,4,6,8,0,0),('M',2,6,0,6,8,0,0),
('N',2,0,0,0,8,0,0),('N',2,4,0,4,8,0,0),('N',2,0,8,4,0,0,0),
('E',2,0,0,0,8,0,0),('E',2,0,0,4,0,0,0),('E',2,0,8,4,8,0,0),('E',2,0,4,3,4,0,0),
('F',2,0,0,0,8,0,0),('F',2,0,8,4,8,0,0),('F',2,0,4,3,4,0,0),
('O',2,0,2,0,6,0,0),('O',2,5,2,5,6,0,0),('O',3,0,2,3,0,5,2),('O',3,0,6,3,8,5,6),
('R',2,0,0,0,8,0,0),('R',2,0,8,2,8,0,0),('R',2,0,4,2,4,0,0),('R',3,2,8,4,6,2,4),('R',2,2,4,4,0,0,0),
('X',2,5,0,0,8,0,0),('X',2,0,0,5,8,0,0),
('C',2,0,2,0,6,0,0),('C',3,0,2,3,0,5,2),('C',3,0,6,3,8,5,6),
('U',2,0,2,0,8,0,0),('U',2,5,2,5,8,0,0),('U',3,0,2,3,0,5,2),
('N',2,0,0,0,8,0,0),('N',2,4,0,4,8,0,0),('N',2,0,8,4,0,0,0),
('S',2,0,0,2,0,0,0),('S',2,2,8,4,8,0,0),('S',3,2,8,0,6,2,4),('S',3,2,4,4,2,2,0),
('Z',2,0,0,5,0,0,0),('Z',2,0,8,5,8,0,0),('Z',2,0,0,5,8,0,0),
('1',2,0,-10,0,20,0,0),('1',2,0,23,-3,13,0,0),('1',2,0,23,3,13,0,0),
('2',2,-40,12,15,12,0,0),('2',2,15,12,5,15,0,0),('2',2,15,12,5,9,0,0);
GO
CREATE PROCEDURE #USP_GETCACHE
/*
Procedure returns current SS cache.
If no params - returns all.
Or searches for param
*/
@Parameter SQL_VARIANT = ''
WITH RECOMPILE
AS
DECLARE @SQL VARCHAR(8000);
DECLARE @Search_String VARCHAR(200) = '';
DECLARE @S CHAR(80);
DECLARE @V INT; --SQL Server Major Version

SELECT @V = CAST(CAST(SERVERPROPERTY('ProductVersion') as CHAR(2)) as NUMERIC), @S = REPLICATE('-',80);

PRINT 'Function Ctrl-8 Options: SQL Server Query Cache.';
PRINT '1. No options: Returns: % of Adhoc Queries; TOP 1000 List of all queries stored in SQL Server Cache and
a Diagram with three sets of TOP 5 queries by Timing, I/O usage and number of executions (see the "Spatial" tab).';
PRINT '2. "X": Query Plan in XML format. (Might generate an error: "XML datatype instance has too many levels of nested nodes. Maximum allowed depth is 128 levels.")';
PRINT '3. Part of a query: Will search (TOP 1000) for that part of a query within SQL Server Cache.';
PRINT '4. sql_hqsh or plan_handle: Returns all plans for particular SQL Query or only one plan for plan_handle';
PRINT '5. "P" - Information about parallelism in current list of query plans. Might take long time to run.';

PRINT 'Parameter Base Type: ' + CAST(SQL_VARIANT_PROPERTY(@Parameter, 'BaseType') as VARCHAR);
RAISERROR (@S,10,1) WITH NOWAIT

IF SQL_VARIANT_PROPERTY(@Parameter, 'BaseType') in ('varchar','nvarchar')
	SET @Search_String = CAST(@Parameter as VARCHAR(200))


	
IF SQL_VARIANT_PROPERTY(@Parameter, 'BaseType') = 'varbinary'
BEGIN
-- Returns all plans for particular SQL Query or only one plan for plan_handle
SET @SQL =
'SELECT TOP 1000 DB_NAME(st.dbid) DatabaseNm, st.objectid,
		qs.creation_time,
		cp.cacheobjtype,
		cp.objtype,
		qs.last_execution_time,
		OBJECT_NAME(st.objectid,st.dbid) AS ObjectName, st.TEXT AS SQLBatch,
		SUBSTRING(st.text,1+qs.statement_start_offset/2,
		(CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text) ELSE qs.statement_end_offset END
		- qs.statement_start_offset)/2) as Query_Text,
		CAST(qp.query_plan as XML) AS query_plan,
		qs.execution_count,
		cp.usecounts AS UseCounts,
		cp.refcounts AS ReferenceCounts,
		qs.max_worker_time,
		qs.last_worker_time,
		qs.total_worker_time,
		qs.total_elapsed_time/1000000 total_elapsed_time_in_S,
		qs.last_elapsed_time/1000000 last_elapsed_time_in_S,
		cp.size_in_bytes/1048576. AS SizeMb,
		qs.total_logical_reads, qs.last_logical_reads,
		qs.total_logical_writes, qs.last_logical_writes,
		qs.[plan_handle]
	FROM sys.dm_exec_query_stats qs WITH (NOLOCK)
	LEFT JOIN sys.dm_exec_cached_plans AS cp WITH (NOLOCK)
		on qs.[plan_handle] = cp.[plan_handle]
	CROSS APPLY sys.dm_exec_sql_text(qs.[plan_handle]) AS st
	CROSS APPLY sys.dm_exec_text_query_plan(qs.[plan_handle],qs.statement_start_offset,qs.statement_end_offset) AS qp
	WHERE (qs.[plan_handle] = '
		+ sys.fn_varbintohexsubstring(1,CAST(@Parameter as VARBINARY(64)),1,0) + ' OR
		qs.sql_handle = ' + sys.fn_varbintohexsubstring(1,CAST(@Parameter as VARBINARY(64)),1,0) + ' OR
		qs.query_hash = ' + sys.fn_varbintohexsubstring(1,CAST(@Parameter as VARBINARY(64)),1,0) + ')
	OPTION (RECOMPILE);'
	PRINT @SQL;
	RAISERROR (@S,10,1) WITH NOWAIT
	EXEC (@SQL);

	
  RETURN 0;
END

IF LEN(@Search_String) = 1 and UPPER(@Search_String) COLLATE database_default in ('' , 'P')
BEGIN
-- Return Count of Parallel plans
	SET @SQL ='WITH QS as (
	SELECT query_hash, query_plan_len = LEN(query_plan)
		, Parallel = SIGN(PATINDEX(''%Parallel="1"%'',query_plan))
		, fNonParallelPlanReason = IsNull(PATINDEX(''%NonParallelPlanReason%'',query_plan),0)
		, fEstimatedAvailableDegreeOfParallelism = PATINDEX(''%EstimatedAvailableDegreeOfParallelism%'',query_plan)
		, query_plan
	FROM sys.dm_exec_query_stats qs with (nolock)
	CROSS APPLY sys.dm_exec_text_query_plan(qs.[plan_handle],qs.statement_start_offset,qs.statement_end_offset) AS qp
), QST as (
	SELECT
		Parallel, QLen = DATALENGTH(query_plan)
		, NonParallelPlanReason = CASE fNonParallelPlanReason WHEN 0 THEN ''Non-Parallel Plans - Other Reasons''
			ELSE SUBSTRING(query_plan,fNonParallelPlanReason+23,CHARINDEX(''"'',query_plan,fNonParallelPlanReason+23) - fNonParallelPlanReason-23) END
		, EstimatedAvailableDegreeOfParallelism = CASE WHEN IsNull(fEstimatedAvailableDegreeOfParallelism,0) = 0 THEN 0
		' + CASE WHEN @V > 10 THEN ' ELSE IsNull(TRY_CAST(SUBSTRING(query_plan,fEstimatedAvailableDegreeOfParallelism+39
				, CHARINDEX(''"'',query_plan,fEstimatedAvailableDegreeOfParallelism+39) - fEstimatedAvailableDegreeOfParallelism-39) as INT),0) END'
	ELSE '
			WHEN IsNumeric(
				SUBSTRING(query_plan,fEstimatedAvailableDegreeOfParallelism+39
				, CHARINDEX(''"'',query_plan,fEstimatedAvailableDegreeOfParallelism+39) - fEstimatedAvailableDegreeOfParallelism-39)) = 1
				THEN CAST(CAST(SUBSTRING(query_plan,fEstimatedAvailableDegreeOfParallelism+39
				, CHARINDEX(''"'',query_plan,fEstimatedAvailableDegreeOfParallelism+39) - fEstimatedAvailableDegreeOfParallelism-39) as NUMERIC) AS INT)
			ELSE 0 END'
END + '
	FROM QS
	WHERE Parallel Is Not Null
)
SELECT [Plan Type] = CASE WHEN Parallel = 1 THEN '' Parallel Plans''
		WHEN LEN(NonParallelPlanReason) > 100 THEN ''Non-Parallel Plans - Other Reasons''
		ELSE NonParallelPlanReason END
	, EstimatedAvailableDegreeOfParallelism
	, [Number of Plans] = COUNT(*)
	, [Total Size, Mb] = CAST(SUM(QLen) / 1048576. as DECIMAL(10,3))
FROM QST
GROUP BY Parallel, NonParallelPlanReason, EstimatedAvailableDegreeOfParallelism
ORDER BY [Plan Type]
OPTION (RECOMPILE);';
	PRINT @SQL;
	RAISERROR (@S,10,1) WITH NOWAIT
	EXEC (@SQL);
END
ELSE IF UPPER(@Search_String) COLLATE database_default in ('' , 'X')
BEGIN
-- Return Percentage of Adhoc queries
	SET @SQL = '
		SELECT
			[Adhoc Queries by Size] =
			CAST(CAST(ROUND(SUM(CASE WHEN cp.cacheobjtype = N''Compiled Plan'' AND cp.objtype = N''Adhoc'' AND cp.usecounts = 1 THEN CAST(cp.size_in_bytes as BIGINT) ELSE 0 END)*100./SUM(CAST(cp.size_in_bytes as BIGINT)),2) as FLOAT) AS VARCHAR) + ''%'',
			[Adhoc Queries by Count] =
			CAST(CAST(ROUND(SUM(CASE WHEN cp.cacheobjtype = N''Compiled Plan'' AND cp.objtype = N''Adhoc'' AND cp.usecounts = 1 THEN 1 ELSE 0 END)*100./count(*),2) as FLOAT) AS VARCHAR) + ''%'',
			[Sum Size of All Queries in Cache] =
			CAST(CAST(ROUND(SUM(cp.size_in_bytes/1073741824.),3) as FLOAT) AS VARCHAR) + '' Gb'',
			[Count of All Queries in Cache] = count(*)
		FROM sys.dm_exec_cached_plans AS cp WITH (NOLOCK)
		OPTION (RECOMPILE);
	';

	PRINT @SQL;
	RAISERROR (@S,10,1) WITH NOWAIT
	EXEC (@SQL);

-- Return TOP 100 plans by total_elapsed_time
	SET @SQL =
		'SELECT TOP 100 DB_NAME(st.dbid) DatabaseNm, st.objectid,
			qs.creation_time,
			cp.cacheobjtype,
			cp.objtype,
			qs.last_execution_time,
			OBJECT_NAME(st.objectid,st.dbid) AS ObjectName, st.TEXT AS SQLBatch,
			SUBSTRING(st.text,1+qs.statement_start_offset/2,
			(CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text) ELSE qs.statement_end_offset END
			- qs.statement_start_offset)/2) as Query_Text,';

	IF ASCII(UPPER(@Search_String)) = ASCII('X')
		SET @SQL = @SQL COLLATE database_default + ' CAST(qp.query_plan as XML) AS Batch_Plan,';
	ELSE
		SET @SQL = @SQL COLLATE database_default + ' qp.query_plan AS Batch_Plan,';

	SET @SQL = @SQL COLLATE database_default +'
			qs.execution_count,
			cp.usecounts AS UseCounts,
			cp.refcounts AS ReferenceCounts,
			qs.max_worker_time,
			qs.last_worker_time,
			qs.total_worker_time,
			qs.total_elapsed_time/1000000 total_elapsed_time_in_S,
			qs.last_elapsed_time/1000000 last_elapsed_time_in_S,
			cp.size_in_bytes/1048576. AS SizeMb,
			qs.total_logical_reads, qs.last_logical_reads,
			qs.total_logical_writes, qs.last_logical_writes,
			qs.[plan_handle]
		FROM sys.dm_exec_query_stats qs WITH (NOLOCK)
		LEFT JOIN sys.dm_exec_cached_plans AS cp WITH (NOLOCK)
			on qs.[plan_handle] = cp.[plan_handle]
		CROSS APPLY sys.dm_exec_sql_text(qs.[plan_handle]) AS st
		CROSS APPLY sys.dm_exec_text_query_plan(qs.[plan_handle],qs.statement_start_offset,qs.statement_end_offset) AS qp
		ORDER BY qs.total_elapsed_time  DESC
		OPTION (RECOMPILE);'

	PRINT @SQL;
	RAISERROR (@S,10,1) WITH NOWAIT
	EXEC (@SQL);

-- Draw TOP 10 Cached queries Diagram with Tail	
IF @V > 10
SET @SQL = 'DECLARE @i INT = 1;
DECLARE @x VARCHAR(MAX),@TextScale DECIMAL = 1;
DECLARE @g GEOMETRY = ''POINT EMPTY'';
DECLARE @XOffset INT = 0, @YOffset INT = 0
DECLARE @IOScaleMax FLOAT, @IOScaleMin FLOAT
	, @TimeScaleMax FLOAT, @TimeScaleMin FLOAT;

DECLARE @PlanRadius TABLE (
Plan_ID INT,
query_hash BINARY(8),
exec_count INT,
total_worker_time FLOAT,
total_elapsed_time FLOAT,
Total_IO FLOAT,
total_logical_reads FLOAT,
total_logical_writes FLOAT,
total_physical_reads FLOAT,
max_worker_time FLOAT,
max_logical_reads FLOAT,
max_logical_writes FLOAT,
max_physical_reads FLOAT,
max_elapsed_time FLOAT);

;WITH total_worker_time as (
	SELECT TOP 5 WITH TIES query_hash
		, execution_count
		, total_worker_time
		, total_logical_reads + total_logical_writes as Total_IO
		, total_elapsed_time
		, total_logical_reads
		, total_logical_writes
		, total_physical_reads
		, max_worker_time
		, max_logical_reads
		, max_logical_writes
		, max_physical_reads
		, max_elapsed_time
	FROM sys.dm_exec_query_stats qs WITH (NOLOCK)
	ORDER BY total_worker_time DESC
), Total_IO as (
	SELECT TOP 5 WITH TIES query_hash
		, execution_count
		, total_worker_time
		, total_logical_reads + total_logical_writes as Total_IO
		, total_elapsed_time
		, total_logical_reads
		, total_logical_writes
		, total_physical_reads
		, max_worker_time
		, max_logical_reads
		, max_logical_writes
		, max_physical_reads
		, max_elapsed_time
	FROM sys.dm_exec_query_stats qs WITH (NOLOCK)
	ORDER BY total_logical_reads + total_logical_writes DESC
), Total_Exec as (
	SELECT TOP 5 WITH TIES query_hash
		, execution_count
		, total_worker_time
		, total_logical_reads + total_logical_writes as Total_IO
		, total_elapsed_time
		, total_logical_reads
		, total_logical_writes
		, total_physical_reads
		, max_worker_time
		, max_logical_reads
		, max_logical_writes
		, max_physical_reads
		, max_elapsed_time
	FROM sys.dm_exec_query_stats qs WITH (NOLOCK)
	ORDER BY execution_count DESC
), CPlans AS (
	SELECT * FROM total_worker_time  UNION SELECT * FROM Total_IO  UNION SELECT * FROM Total_Exec
) INSERT INTO @PlanRadius
SELECT
	Plan_ID = ROW_NUMBER() OVER(ORDER BY total_worker_time DESC)
		+ ROW_NUMBER() OVER(ORDER BY Total_IO DESC)
		+ ROW_NUMBER() OVER(ORDER BY execution_count DESC)
	, query_hash
	, exec_count = execution_count
	, total_worker_time = total_worker_time/1000000.
	, total_elapsed_time = total_elapsed_time/1000000.
	, Total_IO = (Total_IO)/128.
	, total_logical_reads = total_logical_reads/128.
	, total_logical_writes = total_logical_writes/128.
	, total_physical_reads = total_physical_reads/128.
	, max_worker_time = max_worker_time/1000000.
	, max_logical_reads = max_logical_reads/128.
	, max_logical_writes = max_logical_writes/128.
	, max_physical_reads = max_physical_reads/128.
	, max_elapsed_time = max_elapsed_time/1000000.
FROM CPlans ORDER BY 1 OPTION (RECOMPILE);

SELECT @IOScaleMax = LOG(MAX(Total_IO+1))*100 + LOG(MAX(exec_count)+10)*5
	, @IOScaleMin = LOG(MIN(Total_IO+1))*100 - LOG(MAX(exec_count)+10)*5
	, @TimeScaleMax = LOG(MAX(total_worker_time)+1)*500 + LOG(MAX(exec_count)+10)*5
	, @TimeScaleMin = LOG(MIN(total_worker_time)+1)*500 - LOG(MAX(exec_count)+10)*5
FROM @PlanRadius

SELECT @TextScale = CASE WHEN @IOScaleMax - @IOScaleMin > @TimeScaleMax - @TimeScaleMin
		THEN @IOScaleMax - @IOScaleMin ELSE @TimeScaleMax - @TimeScaleMin END / 600

WHILE @i <= 3
BEGIN
	IF @i = 1
		SELECT @XOffset = @TimeScaleMin - 30*@TextScale, @YOffset = @IOScaleMax - 30*@TextScale;
	ELSE IF @i = 2
		SELECT @XOffset = @TimeScaleMax - 50*@TextScale, @YOffset = @IOScaleMIN - 12*@TextScale;
	ELSE IF @i = 3
		SELECT @XOffset = @TimeScaleMin, @YOffset = @IOScaleMIN - 12*@TextScale;

	;WITH Txt as (
		SELECT p.Position, Cum =  SUM(IsNull(c.StringOffset,0)) OVER(ORDER BY p.Position ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
		FROM #TextTable as p LEFT JOIN (
			SELECT CValue, StringOffset = 3 + MAX(IIF(x1>x2 and x1>x3,x1,IIF(x2>x3,x2,x3)))
			FROM #CharElements GROUP BY CValue
		) as c ON c.CValue = p.CValue
		WHERE p.ValueSet = @i
	)
	SELECT @i += 1, @x = (
	SELECT CASE c.ElementType WHEN 1 THEN ''POINT'' WHEN 2 THEN ''LINESTRING'' WHEN 3 THEN ''CIRCULARSTRING'' WHEN 5 THEN ''CIRCULARSTRING'' END +
		''('' + CAST(@XOffset + (IsNull(p.Cum,0) + c.x1) * @TextScale as VARCHAR) + '' '' + CAST(@YOffset + c.y1 * @TextScale as VARCHAR) +
		IIF(c.ElementType>1,'','' + CAST(@XOffset + (IsNull(p.Cum,0) + c.x2) * @TextScale as VARCHAR) + '' '' + CAST(@YOffset + c.y2 * @TextScale as VARCHAR),'''') +
		IIF(c.ElementType>2,'','' + CAST(@XOffset + (IsNull(p.Cum,0) + c.x3) * @TextScale as VARCHAR) + '' '' + CAST(@YOffset + c.y3 * @TextScale as VARCHAR),'''')  +
		IIF(c.ElementType=5,'','' + CAST(@XOffset + IsNull(p.Cum,0) * @TextScale as VARCHAR) + '' '' + CAST(@YOffset + c.y2 * @TextScale as VARCHAR)+
			'',''+ CAST(@XOffset + (IsNull(p.Cum,0) + c.x1) * @TextScale as VARCHAR) + '' '' + CAST(@YOffset + c.y1 * @TextScale as VARCHAR),'''') + ''),''
	FROM #CharElements as c INNER JOIN #TextTable as t ON t.CValue = c.CValue
	LEFT JOIN Txt as p ON t.Position = p.Position + 1
	WHERE t.ValueSet = @i FOR XML PATH (''''))

	SELECT @x = ''GEOMETRYCOLLECTION('' + LEFT(@x,LEN(@x)-1) + '')''
	SELECT @g = @g.STUnion(CONVERT(GEOMETRY,@x).STBuffer(@TextScale))
END

SELECT Plan_ID = ROW_NUMBER() OVER ( ORDER BY Plan_ID)
	, exec_count as [# of Executions]
	, CAST(total_worker_time as DECIMAL(9,3)) as [Worker time, Sec]
	, CAST(Total_IO/1024. as DECIMAL(9,3)) as [Total IO, Gb]
	, CAST(max_worker_time as DECIMAL(9,3)) as [Max Worker time, Sec]
	, CAST(total_worker_time/exec_count as DECIMAL(9,3)) as[Avg Worker time, Sec]
	, CAST(total_elapsed_time as DECIMAL(9,3)) as [Elapsed time, Sec]
	, CAST(max_elapsed_time as DECIMAL(9,3)) as [Max Elapsed time, Sec]
	, CAST(total_elapsed_time/exec_count as DECIMAL(9,3)) as [Elapsed time, Sec]
	, CAST(Total_IO/1024./exec_count as DECIMAL(9,3)) as [Average IO, Gb]
	, CAST(total_logical_reads/1024. as DECIMAL(9,3)) as [Logical Reads, Gb]
	, CAST(max_logical_reads/1024. as DECIMAL(9,3)) as [Max Logical Reads, Gb]
	, CAST(total_logical_reads/1024./exec_count as DECIMAL(9,3)) as [Avg Logical Reads, Gb]
	, CAST(total_logical_writes/1024. as DECIMAL(9,3)) as [Logical Writes, Gb]
	, CAST(max_logical_writes/1024. as DECIMAL(9,3)) as [Max Logical Writes, Gb]
	, CAST(total_logical_writes/1024./exec_count as DECIMAL(9,3)) as [Avg Logical Writes, Gb]
	, CAST(total_physical_reads/1024. as DECIMAL(9,3)) as [Physical Reads, Gb]
	, CAST(max_physical_reads/1024. as DECIMAL(9,3)) as [Max Physical Reads, Gb]
	, CAST(total_physical_reads/1024./exec_count as DECIMAL(9,3)) as [Avg Physical Reads, Gb]
	, query_hash
	, Diagram = CONVERT(GEOMETRY,''CURVEPOLYGON(CIRCULARSTRING(''
	+ CONVERT(VARCHAR,LOG(total_worker_time+1)*500 + LOG(exec_count+10)*5) + '' '' + CONVERT(VARCHAR,LOG(Total_IO+1)*100) + '',''
	+ CONVERT(VARCHAR,LOG(total_worker_time+1)*500) + '' '' + CONVERT(VARCHAR,LOG(Total_IO+1)*100 + LOG(exec_count+10)*5) + '',''
	+ CONVERT(VARCHAR,LOG(total_worker_time+1)*500 - LOG(exec_count+10)*5) + '' '' + CONVERT(VARCHAR,LOG(Total_IO+1)*100) + '',''
	+ CONVERT(VARCHAR,LOG(total_worker_time+1)*500) + '' '' + CONVERT(VARCHAR,LOG(Total_IO+1)*100 - LOG(exec_count+10)*5) + '',''
	+ CONVERT(VARCHAR,LOG(total_worker_time+1)*500 + LOG(exec_count+10)*5) + '' '' + CONVERT(VARCHAR,LOG(Total_IO+1)*100) + ''))'')
FROM @PlanRadius as p UNION ALL
SELECT 0,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,@g
ORDER BY 1;';
ELSE
SET @SQL = '
;WITH total_worker_time as (
	SELECT TOP 10 WITH TIES query_hash
		, execution_count
		, total_worker_time
		, total_logical_reads + total_logical_writes as Total_IO
		, total_elapsed_time
		, total_logical_reads
		, total_logical_writes
		, total_physical_reads
		, max_worker_time
		, max_logical_reads
		, max_logical_writes
		, max_physical_reads
		, max_elapsed_time
	FROM sys.dm_exec_query_stats qs WITH (NOLOCK)
	ORDER BY total_worker_time DESC
), Total_IO as (
	SELECT TOP 10 WITH TIES query_hash
		, execution_count
		, total_worker_time
		, total_logical_reads + total_logical_writes as Total_IO
		, total_elapsed_time
		, total_logical_reads
		, total_logical_writes
		, total_physical_reads
		, max_worker_time
		, max_logical_reads
		, max_logical_writes
		, max_physical_reads
		, max_elapsed_time
	FROM sys.dm_exec_query_stats qs WITH (NOLOCK)
	ORDER BY total_logical_reads + total_logical_writes DESC
), CPlans AS (
	SELECT *, ROW_NUMBER() over(order by total_worker_time ) as Weight FROM total_worker_time  UNION ALL
	SELECT *, ROW_NUMBER() over(order by Total_IO ) as Weight FROM Total_IO
), PlanRadius as (
SELECT DISTINCT query_hash
	, execution_count as exec_count
	, total_worker_time/1000000. AS total_worker_time
	, total_elapsed_time/1000000. AS total_elapsed_time
	, Total_IO/128. as Total_IO
	, total_logical_reads/128. as total_logical_reads
	, total_logical_writes/128. as total_logical_writes
	, total_physical_reads/128. as total_physical_reads
	, max_worker_time/1000000. AS max_worker_time
	, max_logical_reads/128. as max_logical_reads
	, max_logical_writes/128. as max_logical_writes
	, max_physical_reads/128. as max_physical_reads
	, max_elapsed_time/1000000. as max_elapsed_time
FROM CPlans as cp
WHERE cp.query_hash in (SELECT TOP 10 WITH TIES query_hash FROM CPlans GROUP BY query_hash ORDER BY SUM(Weight) DESC)
)
SELECT ROW_NUMBER() over(order by total_worker_time DESC) as Plan_ID
	, exec_count as [# of Executions]
	, CAST(total_worker_time as DECIMAL(9,3)) as [Worker time, Sec]
	, CAST(Total_IO/1024. as DECIMAL(9,3)) as [Total IO, Gb]
  , CAST(max_worker_time as DECIMAL(9,3)) as [Max Worker time, Sec]
	, CAST(total_worker_time/exec_count as DECIMAL(9,3)) as[Avg Worker time, Sec]
	, CAST(total_elapsed_time as DECIMAL(9,3)) as [Elapsed time, Sec]
	, CAST(max_elapsed_time as DECIMAL(9,3)) as [Max Elapsed time, Sec]
	, CAST(total_elapsed_time/exec_count as DECIMAL(9,3)) as [Elapsed time, Sec]
	, CAST(Total_IO/1024./exec_count as DECIMAL(9,3)) as [Average IO, Gb]
	, CAST(total_logical_reads/1024. as DECIMAL(9,3)) as [Logical Reads, Gb]
	, CAST(max_logical_reads/1024. as DECIMAL(9,3)) as [Max Logical Reads, Gb]
	, CAST(total_logical_reads/1024./exec_count as DECIMAL(9,3)) as [Avg Logical Reads, Gb]
	, CAST(total_logical_writes/1024. as DECIMAL(9,3)) as [Logical Writes, Gb]
	, CAST(max_logical_writes/1024. as DECIMAL(9,3)) as [Max Logical Writes, Gb]
	, CAST(total_logical_writes/1024./exec_count as DECIMAL(9,3)) as [Avg Logical Writes, Gb]
	, CAST(total_physical_reads/1024. as DECIMAL(9,3)) as [Physical Reads, Gb]
	, CAST(max_physical_reads/1024. as DECIMAL(9,3)) as [Max Physical Reads, Gb]
	, CAST(total_physical_reads/1024./exec_count as DECIMAL(9,3)) as [Avg Physical Reads, Gb]
	, query_hash
	,	Diagram = CONVERT(GEOMETRY,''POLYGON((''
	+ CONVERT(VARCHAR,LOG(total_worker_time+1)*500 + LOG(exec_count+10)*5) + '' '' + CONVERT(VARCHAR,LOG(Total_IO+1)*100) + '',''
	+ CONVERT(VARCHAR,LOG(total_worker_time+1)*500) + '' '' + CONVERT(VARCHAR,LOG(Total_IO+1)*100 + LOG(exec_count+10)*5) + '',''
	+ CONVERT(VARCHAR,LOG(total_worker_time+1)*500 - LOG(exec_count+10)*5) + '' '' + CONVERT(VARCHAR,LOG(Total_IO+1)*100) + '',''
	+ CONVERT(VARCHAR,LOG(total_worker_time+1)*500) + '' '' + CONVERT(VARCHAR,LOG(Total_IO+1)*100 - LOG(exec_count+10)*5) + '',''
	+ CONVERT(VARCHAR,LOG(total_worker_time+1)*500 + LOG(exec_count+10)*5) + '' '' + CONVERT(VARCHAR,LOG(Total_IO+1)*100) + ''))'')
FROM PlanRadius as p
OPTION (RECOMPILE);';
	PRINT '------------------------------------------------------------------------------'
	PRINT 'Graphical representation of TOP 10 SQL Queries Cached Plans.'
	PRINT 'Coordinate X - Total query execution time in seconds (it includes all plans)'
	PRINT 'Coordinate Y - Total query IO (Reads+Writes) in Megabytes'
	PRINT 'Circle Radius - Total number of query executions'
	PRINT '------------------------------------------------------------------------------'
	PRINT @SQL;
	PRINT 'Query size: ' + CAST(LEN(@SQL) as varchar);
	RAISERROR (@S,10,1) WITH NOWAIT
	EXEC (@SQL);
END
ELSE
BEGIN
-- Do a search for particular query
SET @SQL =
';WITH Handles AS (
	SELECT qs.[plan_handle],
		DB_NAME(st.dbid) DatabaseNm,
		st.objectid,
		OBJECT_NAME(st.objectid,st.dbid) AS ObjectName,
		st.TEXT AS SQLBatch,
		SUBSTRING(st.text,1+qs.statement_start_offset/2,
		(CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text) ELSE qs.statement_end_offset END
		- qs.statement_start_offset)/2) as Query_Text,
		qs.query_hash
	FROM sys.dm_exec_query_stats qs
	CROSS APPLY sys.dm_exec_sql_text(qs.[plan_handle]) AS st
	WHERE st.text like ''%' + @Search_String COLLATE database_default + '%''
)
SELECT TOP 1000 h.DatabaseNm,
	h.objectid,
	qs.creation_time,
	cp.cacheobjtype,
	cp.objtype,
	qs.last_execution_time,
	h.ObjectName,
	h.SQLBatch,
	h.Query_Text,
	CAST(qp.query_plan as XML) AS query_plan,
	qs.execution_count,
	cp.usecounts AS UseCounts,
	cp.refcounts AS ReferenceCounts,
	qs.max_worker_time,
	qs.last_worker_time,
	qs.total_worker_time,
	qs.total_elapsed_time/1000000 total_elapsed_time_in_S,
	qs.last_elapsed_time/1000000 last_elapsed_time_in_S,
	cp.size_in_bytes/1048576. AS SizeMb,
	qs.total_logical_reads, qs.last_logical_reads,
	qs.total_logical_writes, qs.last_logical_writes,
	qs.[plan_handle]
FROM sys.dm_exec_query_stats qs WITH (NOLOCK)
INNER JOIN Handles as h ON qs.[plan_handle] = h.[plan_handle]
	and qs.query_hash = h.query_hash
LEFT JOIN sys.dm_exec_cached_plans AS cp WITH (NOLOCK)
	on qs.[plan_handle] = cp.[plan_handle]
CROSS APPLY sys.dm_exec_text_query_plan(qs.[plan_handle],qs.statement_start_offset,qs.statement_end_offset) AS qp
OPTION (RECOMPILE);';
	PRINT @SQL;
	RAISERROR (@S,10,1) WITH NOWAIT
	EXEC (@SQL);
END

RETURN;
GO
#USP_GETCACHE 