/* 9 - 2020-03-02 Statistics' Information
Consolidated by Slava Murygin
http://slavasql.blogspot.com/2016/02/ssms-query-shortcuts.html */

IF OBJECT_ID('tempdb..#tbl_Statistic_Histogram') IS NOT NULL
	DROP TABLE #tbl_Statistic_Histogram;
GO
CREATE TABLE #tbl_Statistic_Histogram(
	[RANGE_HI_KEY] SQL_VARIANT
,	[RANGE_ROWS] BIGINT
,	[EQ_ROWS] BIGINT
,	[DISTINCT_RANGE_ROWS]  INT
,	[AVG_RANGE_ROWS] NUMERIC
,	[TOTAL_ROWS] AS [RANGE_ROWS] + [EQ_ROWS]
,	[STEP_ID] INT IDENTITY (1,1) PRIMARY KEY
,	[stats_id] INT
);
GO
IF OBJECT_ID('tempdb..#USP_GETSTATS') IS NOT NULL
	DROP PROCEDURE #USP_GETSTATS;
GO
CREATE PROCEDURE #USP_GETSTATS
@Object_Name SYSNAME = NULL
WITH RECOMPILE
AS
PRINT 'Function Ctrl-9 Options: SQL Server Statistics.';
PRINT '1. No options: Returns List of all Statistics in current DB.';
PRINT '2. Table Name: Returns List of all Statistics for specified table (Applicable for SQL Server 2008R2 SP2 or higher).';
PRINT '3. Statistic name: Returns Statistics'' Header, Density Vector and Histogram.';
PRINT '----------------------------------------------------------------------'

DECLARE @nocount_off BIT=CASE @@OPTIONS & 512 WHEN 0 THEN 1 ELSE 0 END;
DECLARE @SQL NVARCHAR(MAX);
DECLARE @Table_Name SYSNAME;
DECLARE @stat_name SYSNAME, @id INT;
DECLARE @V INT; --SQL Server Major Version

SET NOCOUNT ON
SELECT @V= CAST(CAST(SERVERPROPERTY('ProductVersion') as CHAR(2)) as NUMERIC);

IF @Object_Name COLLATE database_default Is Null
BEGIN
SET @SQL = '
SELECT Obj_Type = CASE WHEN OBJECTPROPERTY(s.object_id,''IsTable'') = 1 THEN ''Table''
	WHEN OBJECTPROPERTY(s.object_id,''IsView'') = 1 THEN ''VIEW'' ELSE ''N/A'' END
	, [ObjectName] = OBJECT_SCHEMA_NAME(s.object_id) + ''.'' + OBJECT_NAME(s.object_id)
	, [Column Name] = c.name
	, [StatisticName] = s.[name]
	, [StatisticUpdateDate] = STATS_DATE(s.[object_id], s.[stats_id])
	, p.object_id, p.stats_id, p.rows, p.rows_sampled, p.steps
	, p.modification_counter'
	+ CASE WHEN @V > 13 THEN ', p.persisted_sample_percent, s.is_temporary, s.is_incremental' ELSE '' END + '
	, s.auto_created, s.user_created, s.no_recompute, s.has_filter, s.filter_definition
	, [Obj_Definition] = OBJECT_DEFINITION(s.object_id)
FROM sys.stats as s
INNER JOIN sys.stats_columns AS sc ON sc.object_id = s.object_id AND sc.stats_id = s.stats_id
INNER JOIN sys.columns AS c ON c.object_id = s.object_id AND sc.column_id = c.column_id
OUTER APPLY sys.dm_db_stats_properties(s.object_id,s.[stats_id]) AS p
WHERE STATS_DATE(s.[object_id], s.[stats_id]) Is not Null
	AND OBJECTPROPERTY(s.object_id,''IsIndexable'') = 1
	AND sc.stats_column_id = 1
ORDER BY STATS_DATE(s.[object_id], s.[stats_id])  OPTION (RECOMPILE)';

	PRINT @SQL;
	EXEC (@SQL);
END
ELSE IF OBJECTPROPERTY(OBJECT_ID(@Object_Name), 'IsTable') = 1
BEGIN
	IF REPLACE(CAST(SERVERPROPERTY ('ProductVersion') as VARCHAR),'.','') < '105040000'
		PRINT 'Sorry, but that query runs on SQL Server 2008R2 SP2 or higher.'
	ELSE
	BEGIN

		DECLARE TableStatistics CURSOR LOCAL STATIC FORWARD_ONLY
		FOR SELECT name, stats_id FROM sys.stats WHERE object_id = OBJECT_ID(@Object_Name);

		OPEN TableStatistics;

		FETCH NEXT FROM TableStatistics INTO @stat_name, @id;

		WHILE (@@fetch_status <> -1)
		BEGIN
			SET @SQL = 'DBCC SHOW_STATISTICS(''' + @Object_Name + ''',''' + @stat_name + ''') WITH  HISTOGRAM, NO_INFOMSGS;'
			--PRINT @SQL

			INSERT INTO #tbl_Statistic_Histogram(RANGE_HI_KEY, RANGE_ROWS, EQ_ROWS, DISTINCT_RANGE_ROWS, AVG_RANGE_ROWS)
			EXEC (@SQL)
			UPDATE #tbl_Statistic_Histogram SET stats_id = @id WHERE stats_id IS NULL

			FETCH NEXT FROM TableStatistics INTO @stat_name, @id;
		END

		CLOSE TableStatistics
		DEALLOCATE TableStatistics

		SET @SQL = '
;WITH rn AS (
	SELECT sc.stats_id
		, h.range_rows, rn = ROW_NUMBER() OVER(PARTITION BY sc.stats_id ORDER BY h.range_rows)
		, h.distinct_range_rows, dn = ROW_NUMBER() OVER(PARTITION BY sc.stats_id ORDER BY h.distinct_range_rows)
	FROM sys.stats_columns AS sc
	-- CROSS APPLY sys.dm_db_stats_histogram(sc.object_id,sc.stats_id) AS h
	INNER JOIN #tbl_Statistic_Histogram AS h ON sc.stats_id = h.stats_id
	WHERE sc.object_id = OBJECT_ID(''' + @Object_Name + ''') AND sc.stats_column_id = 1
), nd as (
	SELECT stats_id
		, Numeric_Steps = CAST(CAST(SUM(IsNumeric(CAST(RANGE_HI_KEY as VARCHAR(8000))))*100./Count(*) as NUMERIC(5,2)) as VARCHAR)+'' %''
		, Date_Steps = CAST(CAST(SUM(IsDate(CAST(RANGE_HI_KEY as VARCHAR(8000))))*100./Count(*) as NUMERIC(5,2)) as VARCHAR)+'' %''
	FROM #tbl_Statistic_Histogram
	GROUP BY stats_id
), cs AS (
	SELECT sc.stats_id, sc.column_id, Steps = COUNT(*)
	, MaxLength = MAX(LEN(CAST(RANGE_HI_KEY as VARCHAR(8000))))
	, NotEmptyRange = SUM(CASE range_rows WHEN 0 THEN 0 ELSE 1 END)
	, NotEmptyDistincts = SUM(CASE distinct_range_rows WHEN 0 THEN 0 ELSE 1 END)
	, MAXRange_rows = MAX(range_rows)
	, MAXDistinctRange_rows = MAX(distinct_range_rows)
	, DistinctRows = SUM(distinct_range_rows) + COUNT(*)
	--, NotNullRows = SUM(equal_rows) + SUM(range_rows)
	, NotNullRows = SUM(eq_rows) + SUM(range_rows)
	FROM sys.stats_columns AS sc
	--CROSS APPLY sys.dm_db_stats_histogram(sc.object_id,sc.stats_id) AS h
	INNER JOIN #tbl_Statistic_Histogram AS h ON sc.stats_id = h.stats_id
	WHERE sc.object_id = OBJECT_ID(''' + @Object_Name + ''') AND sc.stats_column_id = 1
	GROUP BY sc.stats_id, sc.column_id
	)
SELECT s.object_id,
	OBJECT_SCHEMA_NAME(s.object_id) + ''.'' + OBJECT_NAME(s.object_id) AS [ObjectName],
	sp.[stats_id] AS "Statistic ID",
	s.[name] AS [StatisticName],
	sp.[last_updated] AS [StatisticUpdateDate],
	IsNull(c.name,'''') as First_Column_Name,
	IsNull(t.name,'''') as Column_Type,
	c.max_length as Column_Length,
	s.user_created, s.no_recompute,
	CASE s.has_filter WHEN 0 THEN '''' ELSE s.filter_definition END as Filter,
	sp.rows,
	cs.DistinctRows,
	cs.Steps,
	sp.rows_sampled,
	cs.NotNullRows,
	NullRows = sp.rows - cs.NotNullRows,
	cs.MaxLength,
	nd.Numeric_Steps,
	nd.Date_Steps,
	sp.unfiltered_rows,
	sp.modification_counter,
	cs.NotEmptyRange, cs.NotEmptyDistincts,
	cs.MAXRange_rows, cs.MAXDistinctRange_rows,
	r.MeanRangeRows, d.MeanDistinctRangeRows
FROM sys.stats as s
INNER JOIN nd ON nd.stats_id = s.stats_id
INNER JOIN cs ON cs.stats_id = s.stats_id
INNER JOIN sys.columns as c ON s.object_id = c.object_id and ( cs.column_id = c.column_id OR
	s.name like ''_WA_Sys_%'' and CONVERT(INT,CONVERT(VARBINARY(4),SUBSTRING(s.name,9,8),2)) = c.column_id
	)
OUTER APPLY sys.dm_db_stats_properties ([s].[object_id],[s].[stats_id]) AS [sp]
LEFT JOIN sys.types as t ON t.system_type_id = c.system_type_id and c.user_type_id = t.user_type_id
CROSS APPLY (
	SELECT AVG(rn.range_rows) FROM rn
	WHERE rn.stats_id = cs.stats_id
		and rn BETWEEN FLOOR(cs.Steps/2.) + cs.Steps%2 AND CEILING(cs.Steps/2.)
) AS r(MeanRangeRows)
CROSS APPLY (
	SELECT AVG(rn.distinct_range_rows) FROM rn
	WHERE rn.stats_id = cs.stats_id
		and dn BETWEEN FLOOR(cs.Steps/2.) + cs.Steps%2 AND CEILING(cs.Steps/2.)
) AS d(MeanDistinctRangeRows)
WHERE s.object_id = OBJECT_ID(''' + @Object_Name + ''')
ORDER BY sp.[last_updated] DESC OPTION (RECOMPILE)';

		PRINT @SQL;
		EXEC (@SQL);
	END
END	
ELSE IF EXISTS (SELECT TOP 1 1 FROM sys.stats WHERE [name] = @Object_Name)
BEGIN

	SELECT @Table_Name = OBJECT_SCHEMA_NAME(object_id) + '.' + OBJECT_NAME(object_id)
	FROM sys.stats WHERE [name] = @Object_Name

	DBCC SHOW_STATISTICS (@Table_Name,@Object_Name) WITH STAT_HEADER, NO_INFOMSGS;
	DBCC SHOW_STATISTICS (@Table_Name,@Object_Name) WITH DENSITY_VECTOR, NO_INFOMSGS

	SET @SQL = 'DBCC SHOW_STATISTICS (''' + @Table_Name + ''',''' + @Object_Name + ''') WITH HISTOGRAM, NO_INFOMSGS;';
	PRINT @SQL

	INSERT INTO #tbl_Statistic_Histogram(RANGE_HI_KEY, RANGE_ROWS, EQ_ROWS, DISTINCT_RANGE_ROWS, AVG_RANGE_ROWS)
	EXEC (@SQL);

	SET @SQL = '
DECLARE @c TINYINT = (SELECT MAX(STEP_ID) FROM #tbl_Statistic_Histogram); -- Number of histogram steps
DECLARE @MRange BIGINT = (SELECT MAX(TOTAL_ROWS) FROM #tbl_Statistic_Histogram); -- Max Number of items in range
DECLARE @CSize FLOAT = 100;
DECLARE @FSize FLOAT = @CSize * 1.1;
DECLARE @HRate FLOAT = (@FSize * @c / 1.5) / @MRange; -- Dimensions 1x2
DECLARE @Diagram VARCHAR(MAX)='''';
	
;WITH dta AS (
SELECT STEP_ID, RANGE_ROWS, EQ_ROWS, DISTINCT_RANGE_ROWS, AVG_RANGE_ROWS, TOTAL_ROWS,
	RANGE_HI_KEY = CAST(RANGE_HI_KEY as VARCHAR(100)), z = '' 0'',
	A = CONVERT(VARCHAR,(STEP_ID-1) * @FSize),
	B = CONVERT(VARCHAR,(STEP_ID-1) * @FSize + @CSize),
	C = CONVERT(VARCHAR,EQ_ROWS * @HRate),
	D = CONVERT(VARCHAR,TOTAL_ROWS * @HRate)
FROM #tbl_Statistic_Histogram
)
SELECT STEP_ID, RANGE_HI_KEY, RANGE_ROWS, EQ_ROWS, DISTINCT_RANGE_ROWS, AVG_RANGE_ROWS, TOTAL_ROWS,
	HISTOGRAM_BAR = CONVERT(GEOMETRY,''POLYGON((''+A+z+'',''+B+z+'',''+B+'' ''+C+'',''+A+'' ''+C+'',''+A+z+''),
		(''+A+z+'',''+B+z+'',''+B+'' ''+D+'',''+A+'' ''+D+'',''+A+z+''))'')
FROM dta
ORDER BY STEP_ID  OPTION (RECOMPILE);';

	PRINT @SQL;
	EXEC (@SQL);
END
ELSE
PRINT 'Object is not a table nor statistic'

IF @nocount_off>0 SET NOCOUNT OFF
RETURN;
GO
EXEC #USP_GETSTATS 