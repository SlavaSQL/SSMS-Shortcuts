/* 4 - 2019-12-16 Returns Index Troubleshooting
Consolidated by Slava Murygin
http://slavasql.blogspot.com/2016/02/ssms-query-shortcuts.html */

IF OBJECT_ID('tempdb..#USP_GETINDEX') IS NOT NULL
	DROP PROCEDURE #USP_GETINDEX;
GO
CREATE PROCEDURE #USP_GETINDEX
@Object_Name SYSNAME = NULL
WITH RECOMPILE
AS
SET NOCOUNT ON
DECLARE @oid INT, @iid INT;
DECLARE @S CHAR(80);
DECLARE @V INT; --SQL Server Major Version

SELECT @V = CAST(CAST(SERVERPROPERTY('ProductVersion') as CHAR(2)) as NUMERIC)
	, @S = REPLICATE('-',80);

PRINT 'Function Ctrl-4: SQL Server Index Troubleshooting.';
PRINT '1. No options: Returns following: ';
PRINT '   - Lists of TOP 10 Not-Used Indexes and TOP 10 Missing Indexes.';
PRINT '   - Lists of TOP 10 Missing Indexes.';
PRINT '   - Lists of all non-empty indexes sorted by fragmentation impact.';
PRINT '   Disclosure A: Lists are valid only after full regular business workload cycle. Without SQL Server Reboots and Index Maintenance.';
PRINT '   Disclosure B: Before applying Missing Indexes, always check if any similar index exist. Maybe you can just simply modify an existing one.';
PRINT '2. Table Name: Returns Tables'' Indexes Usage Statistics and possible missing indexes.';
PRINT '3. Index Name: Returns Index Usage Statistics and general info.';
RAISERROR (@S,10,1) WITH NOWAIT

If @Object_Name COLLATE database_default Is not Null
	PRINT 'Used Parameter: "' + @Object_Name + '"';

DECLARE @SQL NVARCHAR(MAX);

DECLARE @Top10Unused  NVARCHAR(MAX) =
'SELECT TOP 10 [Schema Name] = SCHEMA_NAME(t.[schema_id])
	, [Table Name] = t.name
	, Last_Object_Modification = t.modify_date
	, [Bad Index Name] = i.name
	, i.index_id
	' + CASE WHEN @V > 10 THEN ', ps.Page_Count' ELSE '' END +
	', [Indexed Columns] = SUBSTRING(
		(
		SELECT '', '' + c.name
		FROM sys.index_columns as ic
		INNER JOIN sys.columns as c ON c.object_id = ic.object_id and ic.column_id = c.column_id
		WHERE ic.object_id = s.object_id  and ic.index_id = s.index_id and ic.is_included_column = 0
		FOR XML PATH('''')
		),3,2147483647		
	)
	, [Included Columns] = SUBSTRING(IsNull(
		(
		SELECT '', '' + c.name
		FROM sys.index_columns as ic
		INNER JOIN sys.columns as c ON c.object_id = ic.object_id and ic.column_id = c.column_id
		WHERE ic.object_id = s.object_id  and ic.index_id = s.index_id and ic.is_included_column = 1
		FOR XML PATH('''')
		),''''),3,2147483647		
	)
	, Total_Writes =  s.user_updates
	, Total_Reads = s.user_seeks + s.user_scans + s.user_lookups
	, Factor = (s.user_updates - s.user_seeks - s.user_scans - s.user_lookups - 1.) / s.user_updates
FROM sys.dm_db_index_usage_stats AS s
INNER JOIN sys.indexes AS i ON s.object_id = i.object_id AND i.index_id = s.index_id
INNER JOIN sys.tables as t on i.[object_id] = t.[object_id]
' + CASE WHEN @V > 10 THEN 'CROSS APPLY sys.dm_db_index_physical_stats(DB_ID(), i.object_id, i.index_id, NULL, ''LIMITED'') as ps' ELSE '' END + '
INNER JOIN sys.dm_db_index_operational_stats (DB_ID(),NULL,NULL,NULL ) o
	ON i.[object_id] = o.[object_id] AND i.index_id = o.index_id
WHERE objectproperty(s.object_id,''IsUserTable'') = 1
AND s.database_id = db_id()
AND s.user_updates > (s.user_seeks + s.user_scans + s.user_lookups)
AND NOT (o.leaf_insert_count = 0' + CASE WHEN @V > 10 THEN ' and ps.Page_Count = 0' ELSE '' END +	')
AND i.index_id <> 0
ORDER BY s.user_seeks + s.user_scans + s.user_lookups ASC, Factor DESC
OPTION (RECOMPILE);';
/*
-- Glenn Berry
-- http://sqlserverperformance.wordpress.com/2007/11/29/useful-tricks-for-finding-missing-and-bad-indexes-in-sql-server-2005/
*/
DECLARE @Top10Missing  NVARCHAR(MAX) =
'SELECT TOP 10
	[Schema Name] = SCHEMA_NAME(t.[schema_id])
	, [Affected Table] = t.name
	, SQL_Statement = ''CREATE NONCLUSTERED INDEX NCIX_'' + t.name COLLATE DATABASE_DEFAULT + ''_''
		+ REPLACE(REPLACE(REPLACE(IsNull(mid.equality_columns,mid.inequality_columns), ''], ['', ''_''), ''['', ''''),'']'', '''')
		+ '' ON '' + mid.STATEMENT + '' ('' + IsNull(mid.equality_columns,'''')
		+ CASE WHEN mid.equality_columns IS Not Null
			And mid.inequality_columns IS Not Null THEN '','' ELSE '''' END
		+ IsNull(mid.inequality_columns, '''')
		+ '')'' + IsNull('' Include ('' + mid.included_columns + '');'', '';'')
	, Estimated_Impact = CAST( (migs.user_seeks + migs.user_scans )
		* migs.avg_user_impact * migs.avg_total_user_cost / 100 AS INT)
	, Equality_Columns = IsNull(mid.equality_columns,'''')
	, Inequality_Columns = IsNull(mid.inequality_columns,'''')
	, Included_Columns = IsNull(mid.included_columns,'''')
FROM sys.dm_db_missing_index_groups AS mig
INNER Join sys.dm_db_missing_index_group_stats AS migs
	ON migs.group_handle = mig.index_group_handle
INNER Join sys.dm_db_missing_index_details AS mid
	ON mig.index_handle = mid.index_handle
INNER Join sys.tables AS t ON mid.object_id = t.object_id
WHERE mid.database_id = DB_ID()';

DECLARE @IndexUsage  NVARCHAR(MAX) =
'SELECT [Schema Name] = SCHEMA_NAME(t.[schema_id])
	, [OBJECT NAME] = OBJECT_NAME(i.object_id),
	t.modify_date as Last_Object_Modification,
	[INDEX NAME]=ISNUll(i.name,''HEAP-''+ps.alloc_unit_type_desc)
	' + CASE WHEN @V > 10 THEN ', ps.Page_Count' ELSE '' END +
	',o.partition_number
	,[Indexed Columns] = SUBSTRING(
		(SELECT '', '' + c.name
		FROM sys.index_columns as ic
		INNER JOIN sys.columns as c ON c.object_id = ic.object_id and ic.column_id = c.column_id
		WHERE ic.object_id = i.object_id  and ic.index_id = i.index_id and ic.is_included_column = 0
		FOR XML PATH('''')
		),3,2147483647)
	,[Included Columns]=SUBSTRING(IsNull(
		(SELECT '', '' + c.name
		FROM sys.index_columns as ic
		INNER JOIN sys.columns as c ON c.object_id = ic.object_id and ic.column_id = c.column_id
		WHERE ic.object_id = i.object_id  and ic.index_id = i.index_id and ic.is_included_column = 1
		FOR XML PATH('''')
		),''''),3,2147483647)
	,u.USER_SEEKS
	,u.USER_SCANS
	,u.USER_LOOKUPS
	,u.USER_UPDATES
	,o.LEAF_INSERT_COUNT
	,o.LEAF_UPDATE_COUNT
	,o.LEAF_DELETE_COUNT
	,u.last_user_seek
	,u.last_user_scan
	,u.last_user_lookup
	,u.last_user_update
	,u.last_system_seek
	,u.last_system_scan
	,u.last_system_lookup
	,u.last_system_update
FROM sys.indexes AS i with (nolock)
INNER JOIN sys.dm_db_index_operational_stats(DB_ID(),NULL,NULL,NULL ) o
	ON i.object_id = o.object_id AND i.index_id = o.index_id
INNER JOIN sys.tables as t with (nolock) on i.object_id = t.object_id
' + CASE WHEN @V > 10 THEN 'CROSS APPLY sys.dm_db_index_physical_stats(DB_ID(), i.object_id, i.index_id, o.partition_number, ''LIMITED'') as ps' ELSE '' END + '
LEFT JOIN sys.dm_db_index_usage_stats AS u with (nolock)
	ON i.object_id = u.object_id AND i.index_id = u.index_id and u.database_id = DB_ID() ';

IF @Object_Name COLLATE database_default Is Null
BEGIN
	/* Lists of TOP 10 Not-Used Indexes */
	PRINT @Top10Unused;
	RAISERROR (@S,10,1) WITH NOWAIT
	EXEC (@Top10Unused);

	/* Lists of TOP 10 Missing Indexes */
	SET @Top10Missing += ' ORDER BY ''Estimated_Impact'' DESC OPTION (RECOMPILE);';
	PRINT @Top10Missing;
	RAISERROR (@S,10,1) WITH NOWAIT
	EXEC (@Top10Missing);

	IF  @V > 10
	BEGIN
		/* Report page fragmentation/allocation for all Indexes sorted by fragmentation impact */
		SET @SQL = ';WITH IndexData as (
SELECT
	OBJECT_SCHEMA_NAME(ps.object_id) as [Schema Name],
	OBJECT_NAME(ps.object_id) as [Table Name],
	i.name as [Index Name],
	i.index_id,
	IsNull(idnt.yn,''No'') as [Identity],
	CASE i.fill_factor WHEN 0 THEN (
		SELECT CASE [value] WHEN 0 THEN [maximum] ELSE [value] END
		FROM master.sys.configurations WHERE name = ''fill factor (%)''	
	) ELSE i.fill_factor END as fill_factor,
	CAST(ROUND(ps.avg_fragmentation_in_percent,3) as Decimal(9,3)) as [Avg Frag %],
	ps.fragment_count as [Fragments],
	ps.record_count as [Records],
	ps.page_count as [Pages],
	CAST(ROUND(ps.avg_page_space_used_in_percent,3) as Decimal(9,3)) as [Avg page use %],
	CAST(ROUND(ps.page_count / 128.,3) as Decimal(9,3)) AS [Data Space, Mb],
	CAST(ROUND(ps.page_count * ps.avg_page_space_used_in_percent / 12800.,3) as Decimal(9,3)) as [Space Used, Mb],
	CAST(ROUND(ps.page_count * (100 - ps.avg_page_space_used_in_percent) / 12800.,3) as Decimal(9,3)) as [Reserved, Mb],
	CAST(ROUND(100 - ps.avg_page_space_used_in_percent * 100
	/ (CASE i.fill_factor WHEN 0 THEN 100 ELSE i.fill_factor END),3) as Decimal(9,3)) as [Space Overuse %]
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, ''SAMPLED'') ps
INNER JOIN sys.indexes i ON i.OBJECT_ID = ps.OBJECT_ID AND i.index_id = ps.index_id
OUTER APPLY (
	SELECT ''Yes'' FROM sys.index_columns as ic
	INNER JOIN sys.columns as c
		ON i.object_id = c.object_id and ic.column_id = c.column_id
	WHERE c.is_identity = 1 and ic.index_column_id = 1 and
		i.object_id = ic.object_id and i.index_id = ic.index_id
) as idnt(yn)
WHERE ps.page_count > 1 and ps.avg_fragmentation_in_percent > 0
), TotalPages as (SELECT SUM([Pages]) as TotalPages FROM IndexData)
SELECT
	[Schema Name], [Table Name], [Index Name], [index_id], [Identity]
	, [fill_factor], [Avg Frag %], [Fragments], [Records], [Pages]
	, [Avg page use %], [Data Space, Mb], [Space Used, Mb], [Reserved, Mb], [Space Overuse %]
FROM IndexData, TotalPages
ORDER BY [Pages]*[Avg Frag %]/TotalPages DESC OPTION (RECOMPILE);'

		PRINT @SQL;
		RAISERROR (@S,10,1) WITH NOWAIT
		EXEC (@SQL);
	END
END
ELSE IF OBJECTPROPERTY(Object_ID(@Object_Name),'IsTable') = 1
BEGIN
	/* Tables' Indexes Usage Statistics */
	SET @IndexUsage += ' WHERE i.object_id = OBJECT_ID(''' + @Object_Name
		+ ''') ORDER BY i.index_id DESC OPTION (RECOMPILE);';
	PRINT @IndexUsage;
	RAISERROR (@S,10,1) WITH NOWAIT
	EXEC (@IndexUsage);

	/* Lists of TOP 10 Missing Indexes for a table */
	SET @Top10Missing += ' and t.object_id = OBJECT_ID(''' + @Object_Name + ''') ORDER BY ''Estimated_Impact'' DESC OPTION (RECOMPILE);';
	PRINT @Top10Missing;
	RAISERROR (@S,10,1) WITH NOWAIT
	EXEC (@Top10Missing);

END

IF EXISTS ( SELECT TOP 1 1 FROM sys.indexes with (nolock) WHERE name = @Object_Name)
	OR EXISTS (SELECT TOP 1 1 FROM sys.indexes with (nolock)
		WHERE object_id = OBJECT_ID(@Object_Name) and index_id = 0)
BEGIN /* Report Index */

	SELECT @oid = object_id, @iid = index_id
	FROM sys.indexes
	WHERE name = @Object_Name OR (object_id = OBJECT_ID(@Object_Name) and index_id = 0)

	If @iid = 0
		PRINT 'Table does not have clustered index. Will provide HEAP information.' + CHAR(10) + @S;
	ELSE
	BEGIN
		SET @SQL = 'SELECT ''INDEX'' as [Object Type]
		, SCHEMA_NAME(o.schema_id) as [Schema_Name]
		, o.Name as [Table/View Name]
		, i.name as Index_Name
		, i.type_desc
		, CASE i.is_Unique WHEN 0 THEN ''No'' ELSE ''Yes'' END as Is_Unique
		, CASE i.ignore_dup_key WHEN 0 THEN ''No'' ELSE ''Yes'' END as Ignore_Dup_Key
		, CASE i.fill_factor when 0 then 100 ELSE i.fill_factor END Fill_Factor
		, CASE i.is_disabled WHEN 0 THEN ''No'' ELSE ''Yes'' END as Is_Disabled
		, CASE i.has_filter WHEN 0 THEN ''No'' ELSE ''Yes'' END as Has_Filter
		, IsNull(i.filter_definition,'''') as Filter_Definition
		, i.object_id
		, i.index_id
		FROM sys.indexes as i
		INNER JOIN sys.objects AS o on o.object_id = i.object_id
		WHERE i.index_id = ' + CAST(@iid as VARCHAR) + ' and i.object_id = ' + CAST(@oid as VARCHAR) + '
		OPTION (RECOMPILE);';
		PRINT @SQL;
		RAISERROR (@S,10,1) WITH NOWAIT
		EXEC (@SQL);
	
		SET @SQL = 'SELECT c.name as Index_Column
		, CASE ic.is_included_column WHEN 0 THEN ''No'' ELSE ''Yes'' END as Included
		, CASE c.is_nullable WHEN 0 THEN ''No'' ELSE ''Yes'' END as Is_Nullable
		, t.name as Column_Type
		, c.max_length
		, c.precision
		, c.scale
		, c.collation_name
		FROM sys.indexes as i
		INNER JOIN sys.objects AS o on o.object_id = i.object_id
		INNER JOIN sys.index_columns as ic on ic.object_id = i.object_id and i.index_id = ic.index_id
		INNER JOIN sys.columns as c on c.object_id = i.object_id and ic.column_id = c.column_id
		INNER JOIN sys.types as t ON t.system_type_id =c.system_type_id
		WHERE i.index_id = ' + CAST(@iid as VARCHAR) + ' and i.object_id = ' + CAST(@oid as VARCHAR) + '
		OPTION (RECOMPILE);';
		PRINT @SQL;
		RAISERROR (@S,10,1) WITH NOWAIT
		EXEC (@SQL);

		/* Report Index usage */
		SET @IndexUsage += ' WHERE i.name = ''' + @Object_Name + ''' OPTION (RECOMPILE);'
		PRINT @IndexUsage;
		RAISERROR (@S,10,1) WITH NOWAIT
		EXEC (@IndexUsage);
	END

	/* Report Index allocation */
	SET @SQL = 'SELECT [Schema Name] = SCHEMA_NAME(o.[schema_id])
	, [Table Name] = OBJECT_NAME(' + CAST(@oid as VARCHAR) + '),
		[Table Index Name] = i.name,
		ps.index_id,
		ps.index_level,
		ps.index_type_desc,
		ps.alloc_unit_type_desc,
		ps.index_depth, index_level,
		CASE WHEN i.fill_factor = 0 OR (ps.index_level > 0 and i.is_padded = 0)
		THEN 100 ELSE i.fill_factor END AS fill_factor,
		ROUND(ps.avg_fragmentation_in_percent,3) as [AVG Frgmnt %],
		ROUND(ps.avg_page_space_used_in_percent,3) as [AVG Space Use %],
		ps.fragment_count,
		ROUND(ps.avg_fragment_size_in_pages,3) as [AVG Frgmnt Size],
		ps.page_count,
		CAST(ps.page_count/128. as NUMERIC(19,3)) as [Index Size Mb],
		ps.record_count,
		CASE WHEN ps.page_count = 0 THEN 0 ELSE (ps.record_count / ps.page_count) END as AVG_Records_per_Page,
		ps.ghost_record_count,
		ps.forwarded_record_count
	FROM sys.indexes i
	INNER JOIN sys.objects as o ON o.object_id = i.object_id
	CROSS APPLY sys.dm_db_index_physical_stats(DB_ID(), ' + CAST(@oid as VARCHAR) + ', ' + CAST(@iid as VARCHAR) + ', NULL, ''DETAILED'') ps
	WHERE i.index_id = ' + CAST(@iid as VARCHAR) + ' and i.object_id = ' + CAST(@oid as VARCHAR) + '
	OPTION (RECOMPILE);';
	PRINT @SQL;
	RAISERROR (@S,10,1) WITH NOWAIT
	EXEC (@SQL);

	IF  @V > 10
	BEGIN
		/* Report Index page allocation */
		SET @SQL = 'SELECT [Database Name]=DB_NAME(),
			[Schema Name] = SCHEMA_NAME(o.[schema_id])
		, [Table Name] = OBJECT_NAME(i.object_id),
			i.Index_id,
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
		FROM sys.indexes AS i
		INNER JOIN sys.objects as o ON o.object_id = i.object_id
		CROSS APPLY sys.dm_db_database_page_allocations(DB_ID(),' + CAST(@oid as VARCHAR) + ', ' + CAST(@iid as VARCHAR) + ', NULL, ''DETAILED'') a
		WHERE i.index_id = ' + CAST(@iid as VARCHAR) + ' and i.object_id = ' + CAST(@oid as VARCHAR) + '
		ORDER BY a.page_level DESC, a.previous_page_page_id, a.extent_page_id, a.allocated_page_page_id
		OPTION (RECOMPILE);';
		PRINT @SQL;
		RAISERROR (@S,10,1) WITH NOWAIT
		EXEC (@SQL);
	END

END /* Report Index */

ELSE
	PRINT 'No objects found with a name: "'	+ @Object_Name + '".'

RETURN 0;
GO
EXEC #USP_GETINDEX 