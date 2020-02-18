/* Ctrl-F1 - 2020-02-18 List Tables
Consolidated by Slava Murygin
http://slavasql.blogspot.com/2016/02/ssms-query-shortcuts.html */
IF OBJECT_ID('tempdb..#USP_GETSTAT') IS NOT NULL
DROP PROCEDURE #USP_GETSTAT;
GO
CREATE PROCEDURE #USP_GETSTAT
@Parameter1 SYSNAME = NULL,
@Parameter2 VARCHAR(8) = 'LIMITED'
WITH RECOMPILE
AS
SET NOCOUNT ON;
PRINT 'Procedure "Ctrl-F1" returns following, depending on parameters:';
PRINT '1. No parameters: List of all user tables in current databases sorted by size and number of records.';
PRINT '	It is normal that tables with less records are bigger in size.';
PRINT '2. "Name of a table": "LIMITED" statistics for all indexes on that table.';
PRINT '	For large tables that operation can run for several minutes.';
PRINT '3. "Name of an index": "LIMITED" statistics for that index along with list of Page allocation.';
PRINT '4. "Name of a table or an index" + reporting mode: SAMPLED or DETAILED.';
PRINT '   Example 1: tbl_MyTable ';
PRINT '   Example 2: ''CLUIX_MyTable'',''DETAILED'' ';
PRINT '5. Single Letter Paremeters:';
PRINT '   "C" - Compression suggestions for 25 biggest tables.';

CREATE TABLE  #Compression_results(
	Table_Name	SYSNAME NOT NULL,
	Table_Schema	SYSNAME NOT NULL,
	index_id INT  NOT NULL,
	partition_number INT  NOT NULL,
	Compression_Type CHAR(4) NULL,
	Gain_Percentage as CAST(CASE  
		WHEN [size_with_current_compression_setting(KB)] = 0 THEN 0 
		WHEN [size_with_requested_compression_setting(KB)] = 0 THEN 0 
		ELSE 100 - [size_with_requested_compression_setting(KB)]*100./[size_with_current_compression_setting(KB)] END as DECIMAL(38,3)),
	[size_with_current_compression_setting(KB)] INT  NOT NULL,
	[size_with_requested_compression_setting(KB)] INT  NOT NULL,
	[sample_size_with_current_compression_setting(KB)] INT  NOT NULL,
	[sample_size_with_requested_compression_setting(KB)] INT  NOT NULL,
	Current_Compression_Type NVARCHAR(60) NULL,
	TableId INT NULL
);
DECLARE @SQL VARCHAR(MAX);
DECLARE @Object_Id INT;
DECLARE @Index_Id INT = NULL;
DECLARE @Schema SYSNAME, @Table SYSNAME, @Message NVARCHAR(1000);
DECLARE @Current NVARCHAR(60), @TableId INT;
DECLARE @SQLComp NVARCHAR(MAX)=N'
INSERT INTO #Compression_results(
	Table_Name, Table_Schema,	[index_id], [partition_number],
	[size_with_current_compression_setting(KB)],
	[size_with_requested_compression_setting(KB)],
	[sample_size_with_current_compression_setting(KB)],
	[sample_size_with_requested_compression_setting(KB)])
EXEC sys.sp_estimate_data_compression_savings @Schema, @Table, NULL, NULL, @dc;

UPDATE #Compression_results
SET TableId = @Object_Id, Compression_Type = @dc
WHERE Compression_Type is NULL;';

DECLARE @FF NCHAR(3); /* Current Default Server FillFactor */
SELECT @FF = CAST(CASE WHEN value_in_use <> 0 THEN value_in_use ELSE 100 END as CHAR(3))
FROM sys.configurations WHERE name = 'fill factor (%)';

DECLARE @S CHAR(80);
DECLARE @V INT; /* SQL Server Major Version */
SELECT @V = CAST(CAST(SERVERPROPERTY('ProductVersion') as CHAR(2)) as NUMERIC), @S = REPLICATE('-',80);
RAISERROR (@S, 0, 1) WITH NOWAIT

/* #1. No parameters. */
IF @Parameter1 COLLATE database_default Is Null
BEGIN
	SET @SQL = CASE WHEN @V >= 12 THEN ';WITH InMemory as (
	SELECT object_id
		, SUM(CASE memory_consumer_type WHEN 2 THEN allocation_count ELSE 0 END) as row_count
		, SUM(CASE memory_consumer_type WHEN 2 THEN allocated_bytes ELSE 0 END)/8192. as Alloc_Pages
		, SUM(CASE memory_consumer_type WHEN 2 THEN used_bytes ELSE 0 END)/8192. as Used_Pages
		, SUM(CASE memory_consumer_type WHEN 2 THEN 0 ELSE allocated_bytes END)/8192. as Index_Alloc_Pages
		, SUM(CASE memory_consumer_type WHEN 2 THEN 0 ELSE used_bytes END)/8192. as Index_Used_Pages
		, SUM(allocated_bytes)/8192. as Total_Alloc_Pages
		, SUM(used_bytes)/8192. as Total_Used_Pages
	FROM sys.dm_db_xtp_memory_consumers
	WHERE object_id > 0
	GROUP BY object_id
	)' ELSE '' END + '
	SELECT CASE WHEN t.name Is Null THEN ''View'' ELSE ''Table'' END as Type,
	[Object Name] = ''['' + OBJECT_SCHEMA_NAME(st.object_id) + ''].['' + OBJECT_NAME(st.object_id) + '']'',' + CASE WHEN @V < 12 THEN '
	SUM(CASE WHEN st.index_id < 2 THEN row_count ELSE 0 END) row_count,
	ROUND(CAST(SUM(CASE WHEN st.index_id < 2 THEN reserved_page_count ELSE 0 END) AS float)/128.,3) as Data_Size_MB,
	ROUND(CAST(SUM(CASE WHEN st.index_id < 2 THEN used_page_count ELSE 0 END) AS float)/128.,3) as Used_Data_Space_MB,
	[Fill factor, %] = IsNull(CAST(CASE i.fill_factor WHEN 0 THEN ' + @FF + ' ELSE i.fill_factor END as CHAR(3)),''N/A''),
	ROUND(CAST(SUM(CASE WHEN st.index_id > 1 THEN reserved_page_count ELSE 0 END) AS float)/128.,3) as Index_Size_MB,
	ROUND(CAST(SUM(CASE WHEN st.index_id > 1 THEN used_page_count ELSE 0 END) AS float)/128.,3)as Used_Index_Space_MB,
	ROUND(CAST(SUM(reserved_page_count) AS float)/128.,3) as Full_Size_MB,
	ROUND(CAST(SUM(used_page_count) AS float)/128.,3) AS Full_Used_Space_MB,' ELSE '
	CASE WHEN t.is_memory_optimized > 0 THEN mc.row_count ELSE SUM(CASE WHEN st.index_id < 2 THEN st.row_count ELSE 0 END) END as row_count,
	ROUND(CAST(CASE WHEN t.is_memory_optimized > 0 THEN mc.Alloc_Pages ELSE SUM(CASE WHEN st.index_id < 2 THEN st.reserved_page_count ELSE 0 END) END AS float)/128.,3) as Data_Size_MB,
	ROUND(CAST(CASE WHEN t.is_memory_optimized > 0 THEN mc.Used_Pages ELSE SUM(CASE WHEN st.index_id < 2 THEN st.used_page_count ELSE 0 END) END AS float)/128.,3) as Used_Data_Space_MB,
	[Fill factor, %] = IsNull(CAST(CASE i.fill_factor WHEN 0 THEN ' + @FF + ' ELSE i.fill_factor END as CHAR(3)),''N/A''),
	ROUND(CAST(CASE WHEN t.is_memory_optimized > 0 THEN mc.Index_Alloc_Pages ELSE SUM(CASE WHEN st.index_id > 1 THEN st.reserved_page_count ELSE 0 END) END AS float)/128.,3) as Index_Size_MB,
	ROUND(CAST(CASE WHEN t.is_memory_optimized > 0 THEN mc.Index_Used_Pages ELSE SUM(CASE WHEN st.index_id > 1 THEN st.used_page_count ELSE 0 END) END AS float)/128.,3)as Used_Index_Space_MB,
	ROUND(CAST(CASE WHEN t.is_memory_optimized > 0 THEN mc.Total_Alloc_Pages ELSE SUM(st.reserved_page_count) END AS float)/128.,3) as Full_Size_MB,
	ROUND(CAST(CASE WHEN t.is_memory_optimized > 0 THEN mc.Total_Used_Pages ELSE SUM(st.used_page_count) END AS float)/128.,3) as Full_Used_Space_MB,
	' END + 't.max_column_id_used as MaxColumn,
	' + CASE WHEN @V < 12 THEN '' ELSE 'CASE WHEN t.is_memory_optimized > 0 THEN ''Yes'' ELSE '''' END as ''In-Memory'',
	' END + CASE WHEN @V < 13 THEN '' ELSE 'CASE WHEN t.temporal_type > 0 THEN t.temporal_type_desc ELSE '''' END as Temporal, ' END + '
	t.lock_escalation_desc as ''Lock Escalation''
FROM sys.dm_db_partition_stats st ' + CASE WHEN @V < 12 THEN '' ELSE '
LEFT JOIN InMemory as mc ON mc.object_id = st.object_id' END + '
LEFT JOIN sys.tables as t ON t.object_id = st.object_id
LEFT JOIN sys.indexes i ON i.OBJECT_ID = st.OBJECT_ID AND i.index_id = 1
WHERE OBJECT_SCHEMA_NAME(st.object_id) != ''sys''
GROUP BY OBJECT_SCHEMA_NAME(st.object_id), OBJECT_NAME(st.object_id), t.name
	, t.max_column_id_used, t.lock_escalation_desc, CAST(CASE i.fill_factor WHEN 0 THEN ' + @FF + ' ELSE i.fill_factor END as CHAR(3))'
	+ CASE WHEN @V < 12 THEN '' ELSE ', t.is_memory_optimized, mc.row_count, mc.Alloc_Pages, mc.Used_Pages, mc.Index_Alloc_Pages, mc.Index_Used_Pages, mc.Total_Alloc_Pages, mc.Total_Used_Pages' END
	+ CASE WHEN @V < 13 THEN '' ELSE ', t.temporal_type, t.temporal_type_desc' END + '
ORDER BY Full_Size_MB DESC, row_count DESC
OPTION (RECOMPILE);';
	PRINT @SQL;
	EXEC (@SQL);
END
/* Parameter 1 Exists */
ELSE
BEGIN
	IF OBJECTPROPERTY(OBJECT_ID(@Parameter1), 'IsTable') = 1
		/* Parameter 1 is Table */
		SET @Object_Id = OBJECT_ID(@Parameter1);
	ELSE IF EXISTS (SELECT TOP 1 1 FROM sys.indexes WHERE name = @Parameter1)
	BEGIN
		/* Parameter 1 is Index */
		SELECT @Object_Id = Object_Id, @Index_Id = index_Id
		FROM sys.indexes
		WHERE name = @Parameter1
	END
	ELSE IF UPPER(@Parameter1) COLLATE database_default = 'C'
	BEGIN
		/* Parameter "C" - Compression suggestions for 25 biggest tables */
		PRINT 'Estimating compression for 25 biggest tables:';
		RAISERROR (@S, 0, 1) WITH NOWAIT

		DECLARE biggest_tables_cursor  CURSOR LOCAL FOR
		SELECT TOP 25 [Schema], [Table], data_compression_desc, object_id
		FROM (
			SELECT [Schema] = OBJECT_SCHEMA_NAME(st.object_id)
				, [Table] = OBJECT_NAME(st.object_id)
				, data_compression_desc = MAX(ISNUll(p.data_compression_desc,'NONE'))
				, st.object_id
				, used_page_count = SUM(used_page_count)
			FROM sys.dm_db_partition_stats st with (NOLOCK)
			INNER JOIN sys.tables as t with (NOLOCK) ON t.object_id = st.object_id
			LEFT JOIN sys.indexes i with (NOLOCK) ON i.object_id = st.object_id AND i.index_id = 1
			LEFT JOIN sys.partitions AS p with (NOLOCK) ON i.index_id = p.index_id and i.object_id = p.object_id
			WHERE OBJECT_SCHEMA_NAME(st.object_id) != 'sys'
			GROUP BY st.object_id
		) as a ORDER BY used_page_count DESC
		FOR READ ONLY;

		OPEN biggest_tables_cursor;
		FETCH NEXT from biggest_tables_cursor into @Schema, @Table, @Current, @TableId;

		WHILE @@fetch_status >= 0
		BEGIN
			PRINT @SQLComp + ' (@Object_Id=' + CAST(@TableId as VARCHAR) + ', @Schema="' + @Schema + '", @Table="' + @Table + '", Check Compression: NONE'
			RAISERROR (@S, 0, 1) WITH NOWAIT
			EXEC sp_executesql @SQLComp, N'@Object_Id INT, @Schema SYSNAME, @Table SYSNAME, @dc CHAR(4)', @TableId, @Schema, @Table, 'NONE';

			PRINT @SQLComp + ' (@Object_Id=' + CAST(@TableId as VARCHAR) + ', @Schema="' + @Schema + '", @Table="' + @Table + '", Check Compression: ROW'
			RAISERROR (@S, 0, 1) WITH NOWAIT
			EXEC sp_executesql @SQLComp, N'@Object_Id INT, @Schema SYSNAME, @Table SYSNAME, @dc CHAR(4)', @TableId, @Schema, @Table, 'ROW';

			PRINT @SQLComp + ' (@Object_Id=' + CAST(@TableId as VARCHAR) + ', @Schema="' + @Schema + '", @Table="' + @Table + '", Check Compression: PAGE'
			RAISERROR (@S, 0, 1) WITH NOWAIT
			EXEC sp_executesql @SQLComp, N'@Object_Id INT, @Schema SYSNAME, @Table SYSNAME, @dc CHAR(4)', @TableId, @Schema, @Table, 'PAGE';

			--UPDATE #Compression_results SET Current_Compression_Type = @Current, TableId = @TableId WHERE TableId is NULL;
			FETCH NEXT from biggest_tables_cursor into @Schema, @Table, @Current, @TableId;
		END

		CLOSE  biggest_tables_cursor;
		DEALLOCATE 	biggest_tables_cursor;
		
	END
	ELSE
	BEGIN
		/* Parameter 1 is not identified */
		PRINT 'Parameter "' + @Parameter1 + '" is not identified as table or index.';
		RETURN 1;
	END

	SELECT @Schema = OBJECT_SCHEMA_NAME(@Object_Id), @Table = OBJECT_NAME(@Object_Id)

	SET @SQL = 'SELECT [Table Name] = ''[' + @Schema	+ '].[' + @Table + ']''
	, [Index Name] = IsNull(i.name,''HEAP'')
	, ps.index_id
	, ps.partition_number
	, ps.index_level
	, ps.index_type_desc
	, [Compression Type] = p.data_compression_desc
	, ps.alloc_unit_type_desc
	, [Index Depth] = ps.index_depth
	, [Fill Factor] = CASE WHEN i.fill_factor = 0 OR (ps.index_level > 0 and i.is_padded = 0)
			THEN ' + @FF + ' ELSE i.fill_factor END
	, [AVG Fragmentation, %] = Round(ps.avg_fragmentation_in_percent,3)
	, ps.fragment_count
	, [AVG Fragmentation Size in Pages] =  CAST(ps.avg_fragment_size_in_pages as DECIMAL(19,3))
	, ps.page_count
	, [Index Size, Mb] = CAST(ps.page_count/128. as DECIMAL(19,3))
	, ps.record_count
	, [AVG Records per Page] = (ps.record_count / ps.page_count)
	, ps.ghost_record_count
	, ps.forwarded_record_count
FROM sys.dm_db_index_physical_stats('
	/*Specifying Current Database */
	+ CAST(DB_ID() AS VARCHAR) + ', '
	/*Specifying Table*/
	+ CAST(@Object_Id as VARCHAR) + ', '
	/*Specifying Index*/
	+ IsNull(CAST(@Index_Id as VARCHAR),'NULL')
	/*Specifying Reporting Mode*/
	+ ', NULL, ''' + @Parameter2 COLLATE database_default + ''') ps
INNER JOIN sys.indexes i ON i.OBJECT_ID = ps.OBJECT_ID AND i.index_id = ps.index_id
INNER JOIN sys.partitions AS p with (NOLOCK) 
	ON ps.partition_number = p.partition_number 
		and ps.index_id = p.index_id and p.object_id = i.object_id
WHERE p.object_id = ' + CAST(@Object_Id as NVARCHAR) + '
ORDER BY i.index_id, ps.partition_number
OPTION (RECOMPILE);';
	PRINT @SQL;
	EXEC (@SQL);
	RAISERROR (@S, 0, 1) WITH NOWAIT

	IF @Index_Id Is Null AND @Object_Id Is not Null
	BEGIN
		PRINT @SQLComp + ' (@Object_Id=' + CAST(@Object_Id as VARCHAR) + ', @Schema="' + @Schema + '", @Table="' + @Table + '", Check Compression: NONE'
		RAISERROR (@S, 0, 1) WITH NOWAIT
		EXEC sp_executesql @SQLComp, N'@Object_Id INT, @Schema SYSNAME, @Table SYSNAME, @dc CHAR(4)', @Object_Id, @Schema, @Table, 'NONE';

		PRINT @SQLComp + ' (@Object_Id=' + CAST(@Object_Id as VARCHAR) + ', @Schema="' + @Schema + '", @Table="' + @Table + '", Check Compression: ROW'
		RAISERROR (@S, 0, 1) WITH NOWAIT
		EXEC sp_executesql @SQLComp, N'@Object_Id INT, @Schema SYSNAME, @Table SYSNAME, @dc CHAR(4)', @Object_Id, @Schema, @Table, 'ROW';

		PRINT @SQLComp + ' (@Object_Id=' + CAST(@Object_Id as VARCHAR) + ', @Schema="' + @Schema + '", @Table="' + @Table + '", Check Compression: PAGE'
		RAISERROR (@S, 0, 1) WITH NOWAIT
		EXEC sp_executesql @SQLComp, N'@Object_Id INT, @Schema SYSNAME, @Table SYSNAME, @dc CHAR(4)', @Object_Id, @Schema, @Table, 'PAGE';
	END
	ELSE IF @Index_Id Is Not Null
	/* Show Page allocations for specified Index*/
	BEGIN
		SET @SQL = 'SELECT ''' + DB_NAME(DB_ID()) + ''' as Database_Name,
		''' + OBJECT_NAME(@Object_Id) + ''' as Table_Name,
		' + CAST(@Index_Id as VARCHAR) + ' AS Index_id,
		a.allocation_unit_type,
		a.allocation_unit_type_desc,
		a.allocated_page_iam_file_id,
		a.extent_file_id,
		a.extent_page_id,
		a.allocated_page_page_id,
		a.is_allocated,
		a.is_iam_page,
		a.is_mixed_page_allocation,
		a.page_type,
		a.page_type_desc,
		a.page_level,
		a.next_page_page_id,
		a.previous_page_page_id,
		a.is_page_compressed
	FROM sys.dm_db_database_page_allocations('
		/*Specifying Current Database */
		+ CAST(DB_ID() AS VARCHAR) + ', '
		/*Specifying Table*/
		+ CAST(@Object_Id as VARCHAR) + ', '
		/*Specifying Index*/
		+ IsNull(CAST(@Index_Id as VARCHAR),'NULL')
		+ ', NULL, ''DETAILED'') a
	ORDER BY a.page_level DESC, a.previous_page_page_id, a.extent_page_id, a.allocated_page_page_id
	OPTION (RECOMPILE);';
		PRINT @SQL;
		EXEC (@SQL);
	END
END

IF @Parameter1 COLLATE database_default Is Not Null AND
	(@Index_Id Is Null OR UPPER(@Parameter1) COLLATE database_default = 'C')
BEGIN
	SELECT [Table Name] = '[' + OBJECT_SCHEMA_NAME(n.TableId) + '].[' + OBJECT_NAME(n.TableId) + ']'
		, [Index Name] = IsNull(i.name,'HEAP')
		, [Partition] = n.partition_number
		, [Compression Current] = ps.data_compression_desc
		, [Recommended] = CASE
			WHEN n.Gain_Percentage > r.Gain_Percentage AND n.Gain_Percentage > p.Gain_Percentage THEN 'NONE'
			WHEN r.Gain_Percentage > n.Gain_Percentage AND r.Gain_Percentage > p.Gain_Percentage THEN 'ROW'
			WHEN p.Gain_Percentage > n.Gain_Percentage AND p.Gain_Percentage > r.Gain_Percentage THEN 'PAGE'
			ELSE 'UNDETERMINED' END
		, [Implementation Script] = CASE
			WHEN n.Gain_Percentage > r.Gain_Percentage AND n.Gain_Percentage > p.Gain_Percentage
					AND ps.data_compression_desc != 'NONE'
				THEN CASE WHEN n.index_id > 1 THEN 'ALTER INDEX [' + i.name + '] ON ' ELSE 'ALTER TABLE ' END +
					'[' + n.Table_Schema collate catalog_default + '].[' + n.Table_Name collate catalog_default + '] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = NONE)'
			WHEN r.Gain_Percentage > n.Gain_Percentage AND r.Gain_Percentage > p.Gain_Percentage
					AND ps.data_compression_desc != 'ROW'
				THEN  CASE WHEN n.index_id > 1 THEN 'ALTER INDEX [' + i.name + '] ON ' ELSE 'ALTER TABLE ' END +
					'[' + n.Table_Schema collate catalog_default + '].[' + n.Table_Name collate catalog_default + '] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = ROW)'
			WHEN p.Gain_Percentage > n.Gain_Percentage AND p.Gain_Percentage > r.Gain_Percentage
					AND ps.data_compression_desc != 'PAGE'
				THEN  CASE WHEN n.index_id > 1 THEN 'ALTER INDEX [' + i.name + '] ON ' ELSE 'ALTER TABLE ' END +
					'[' + n.Table_Schema collate catalog_default + '].[' + n.Table_Name collate catalog_default + '] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)'
			ELSE '/* No changes determined */' END
		, [Current Size] = CAST(CAST(n.[size_with_current_compression_setting(KB)] / 1024. as DECIMAL(38,3)) as VARCHAR) + ' Mb'
		, [Gain for "NONE"] = CAST(CAST(n.[size_with_current_compression_setting(KB)] * n.Gain_Percentage / 102400. as DECIMAL(38,3)) as VARCHAR) + ' Mb'
		, [Gain for "NONE"] = CAST(n.Gain_Percentage as VARCHAR) + ' %'
		, [Gain for "ROW"] = CAST(CAST(n.[size_with_current_compression_setting(KB)] * r.Gain_Percentage / 102400. as DECIMAL(38,3)) as VARCHAR) + ' Mb'
		, [Gain for "ROW"] = CAST(r.Gain_Percentage as VARCHAR) + ' %'
		, [Gain for "PAGE"] = CAST(CAST(n.[size_with_current_compression_setting(KB)] * p.Gain_Percentage / 102400. as DECIMAL(38,3)) as VARCHAR) + ' Mb'
		, [Gain for "PAGE"] = CAST(p.Gain_Percentage as VARCHAR) + ' %'
	FROM #Compression_results as n
	INNER JOIN sys.indexes as i ON n.index_id = i.index_id AND i.object_id = n.TableId
	INNER JOIN #Compression_results as r ON n.index_id = r.index_id AND n.TableId = r.TableId AND n.partition_number = r.partition_number
	INNER JOIN #Compression_results as p ON n.index_id = p.index_id AND n.TableId = p.TableId AND n.partition_number = p.partition_number
	INNER JOIN sys.partitions AS ps with (NOLOCK) ON ps.object_id = n.TableId AND i.index_id = ps.index_id
		AND n.partition_number = ps.partition_number
	WHERE n.Compression_Type collate catalog_default = 'NONE' 
		AND r.Compression_Type collate catalog_default = 'ROW' 
		AND p.Compression_Type collate catalog_default = 'PAGE'
	ORDER BY [Table Name], [Index Name], [Partition];
END

RETURN 0;
GO
EXEC #USP_GETSTAT 