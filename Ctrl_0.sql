/* 0 - 2023-07-25 Object Information
Consolidated by Slava Murygin
http://slavasql.blogspot.com/2016/02/ssms-query-shortcuts.html
Mostly used materials from Paul S. Randal, SQLskills.com
*/
IF OBJECT_ID('tempdb..#USP_HELP') IS NOT NULL
DROP PROCEDURE #USP_HELP;
GO
CREATE PROCEDURE #USP_HELP
@Object_Name SYSNAME = NULL,
@Parameter2 VARCHAR(8) = 'LIMITED'
WITH RECOMPILE
AS
SET NOCOUNT ON

PRINT 'Function Ctrl-0 Options: SQL Server Objects.';
PRINT '1. No options: Returns List of all tables and List all objects in current DB.';
PRINT '	It is normal that tables with less records are bigger in size.';
PRINT '2. Object Name/ID: Object''s code or full description.';
PRINT '3. Index/Table name (+ parameter): Gives info about index allocation. Second is reporting mode: SAMPLED or DETAILED.';
PRINT 'For system tables requires fully specifired name like: ''sys.objects''.'
PRINT 'Does not handle: Synonyms, Defaults, Constraints.'
PRINT 'Example 1: tbl_MyTable ';
PRINT 'Example 2: ''CLUIX_MyTable'',''DETAILED'' ';

DECLARE @S CHAR(80);
DECLARE @SQL NVARCHAR(MAX);
DECLARE @Object_Id INT;
DECLARE @Index_Id INT = NULL;
DECLARE @Object_Type char(2) = NULL;
DECLARE @V INT; --SQL Server Major Version

SELECT @V= CAST(CAST(SERVERPROPERTY('ProductVersion') as CHAR(2)) as NUMERIC), @S = REPLICATE('-',80);
PRINT @S;

IF @Object_Name COLLATE database_default Is Null
BEGIN
	SET @SQL = 'SELECT
		OBJECT_SCHEMA_NAME(object_id) + ''.'' + OBJECT_NAME(object_id) TableName,
		SUM(CASE WHEN index_id < 2 THEN row_count ELSE 0 END) row_count,
		ROUND(CAST(SUM(CASE WHEN index_id < 2 THEN reserved_page_count ELSE 0 END) AS float)/128.,3) as Data_Size_MB,
		ROUND(CAST(SUM(CASE WHEN index_id < 2 THEN used_page_count ELSE 0 END) AS float)/128.,3) as Used_Data_Space_MB,
		
		ROUND(CAST(SUM(CASE WHEN index_id > 1 THEN reserved_page_count ELSE 0 END) AS float)/128.,3) as Index_Size_MB,
		ROUND(CAST(SUM(CASE WHEN index_id > 1 THEN used_page_count ELSE 0 END) AS float)/128.,3)as Used_Index_Space_MB,

		ROUND(CAST(SUM(reserved_page_count) AS float)/128.,3) as Full_Size_MB,
		ROUND(CAST(SUM(used_page_count) AS float)/128.,3) AS Full_Used_Space_MB
	FROM sys.dm_db_partition_stats as st with (NOLOCK)
	WHERE OBJECT_SCHEMA_NAME(OBJECT_ID) != ''sys''
	GROUP BY OBJECT_SCHEMA_NAME(OBJECT_ID), OBJECT_NAME(OBJECT_ID)
	ORDER BY Full_Size_MB DESC, row_count DESC
	OPTION (RECOMPILE);';
	PRINT @SQL;
	PRINT @S;
	EXEC (@SQL);

	SET @SQL = 'SELECT [Object_Name] = ''['' + SCHEMA_NAME(SCHEMA_ID) + ''].['' + [name] + '']''
		, [Exec on SQL Startup] = CASE
			WHEN [type] = ''P'' and OBJECTPROPERTY([object_id], ''ExecIsStartUp'') = 0 THEN ''No''
			WHEN [type] = ''P'' and OBJECTPROPERTY([object_id], ''ExecIsStartUp'') = 1 THEN ''Yes''
			ELSE ''N/A'' END
		, [type], type_desc, create_date, modify_date, [object_id]
		, [Parent Object] = ''['' + OBJECT_SCHEMA_NAME(parent_object_id) + ''].['' + OBJECT_NAME(parent_object_id) + '']''
		, parent_object_id
		FROM sys.objects with (NOLOCK)
		WHERE [type] not in (''IT'', ''SQ'', ''U'')
		ORDER BY [Parent Object], [type], [name]
		OPTION (RECOMPILE);';
	PRINT @SQL;
	PRINT @S;
	EXEC (@SQL);

END
ELSE
BEGIN /* @Object_Name Is NOT Null */
	IF IsNumeric(@Object_Name) = 1
		SELECT TOP 1 @Object_Type = Type, @Object_Id = Object_Id
		FROM sys.objects WHERE object_id = CAST(@Object_Name as NUMERIC);
	ELSE
		SELECT TOP 1 @Object_Type = Type, @Object_Id = Object_Id
		FROM sys.objects WHERE object_id = OBJECT_ID(@Object_Name) or name = @Object_Name;

	IF @Object_Type COLLATE database_default in  ('TR', 'FN', 'P', 'V', 'FS', 'FT', 'IF')
	BEGIN

		SELECT @Object_Name = N'[' + SCHEMA_NAME(SCHEMA_ID) + N'].[' + name + N']'
		FROM sys.objects WHERE object_id = @Object_Id;

		SET @SQL = 'EXEC sp_help ''' + @Object_Name + ''';';
		PRINT @SQL;
		PRINT @S;
		EXEC (@SQL);

		SET @SQL = 'DECLARE @ObjectText nvarchar(MAX)='''';
			DECLARE @SyscomText	nvarchar(MAX);
			DECLARE @LineLen INT;
			DECLARE @LineEnd BIT = 0;
			DECLARE @CommentText TABLE(LineId int IDENTITY(1,1),Text nvarchar(MAX) '
			+ CASE WHEN @V > 10 THEN ' collate catalog_default' ELSE '' END + ')

			DECLARE ms_crs_syscom  CURSOR LOCAL FOR
			SELECT text FROM sys.syscomments
			WHERE id = ' + CAST(@Object_Id as VARCHAR) + ' and encrypted = 0
			ORDER BY number, colid
			FOR READ ONLY

			OPEN ms_crs_syscom
			FETCH NEXT from ms_crs_syscom into @SyscomText

			WHILE @@fetch_status >= 0
			BEGIN
				SET @LineLen = CHARINDEX(CHAR(10),@SyscomText);
				WHILE @LineLen > 0
				BEGIN

					SELECT	@ObjectText += LEFT(@SyscomText,@LineLen)
						,	@SyscomText = SUBSTRING(@SyscomText, @LineLen+1, 4000)
						,	@LineLen = CHARINDEX(CHAR(10),@SyscomText)
						,	@LineEnd = 1;
		
					INSERT INTO @CommentText(Text)
					VALUES (@ObjectText)

					SET @ObjectText = '''';
				END

				IF @LineLen = 0
					SET @ObjectText += @SyscomText;
				ELSE
					SELECT	@ObjectText = @SyscomText
						,	@LineLen = 0;

				FETCH NEXT from ms_crs_syscom into @SyscomText
			END

			CLOSE  ms_crs_syscom;
			DEALLOCATE 	ms_crs_syscom;

			INSERT INTO @CommentText(Text)
			SELECT @ObjectText;
			
			SELECT REPLACE(REPLACE(Text,CHAR(10),''''),CHAR(13),'''') AS text
			FROM @CommentText
			ORDER BY LineId
			OPTION (RECOMPILE);';
		PRINT @SQL;
		PRINT @S;
		EXEC (@SQL);
	END

	/* Reporting Tables */
	ELSE IF @Object_Type COLLATE database_default IN ('U','S')
	BEGIN
		SELECT @Object_Name = N'[' + SCHEMA_NAME(SCHEMA_ID) + N'].[' + name + N']'
		FROM sys.objects WHERE object_id = @Object_Id;

		IF @Object_Type COLLATE database_default IN ('U')
			and EXISTS (SELECT 1 FROM sys.tables WHERE temporal_type!=0 and [object_id]=@Object_Id)
		BEGIN
			IF EXISTS (SELECT 1 FROM sys.tables WHERE temporal_type=2 and history_table_id is not null and [object_id]=@Object_Id)
			SET @SQL = '
/* Temporal Table Note/Info */
				SELECT [Attention]=''This is a "'' + t.temporal_type_desc + ''"'', [Associated HISTORY_TABLE name] = h.name
					, [Auto-Generated Columns] = CASE LEN(GColumns) WHEN 0 THEN Null ELSE LEFT(GColumns,LEN(GColumns)-1) END
				FROM sys.tables as h
				INNER JOIN sys.tables as t ON t.history_table_id = h.[object_id]
				OUTER APPLY (SELECT (SELECT c.name + '','' FROM sys.columns as c
				WHERE c.generated_always_type != 0 and c.[object_id]=t.[object_id]
				FOR XML PATH(''''))) as c(GColumns)
				WHERE t.[object_id]=' + CAST(@Object_Id as VARCHAR) + '
				OPTION (RECOMPILE);';
			ELSE
			SET @SQL = '
/* Historical Table Note/Info */
				SELECT [Attention]=''This is a "'' + h.temporal_type_desc + ''"'', [Associated SYSTEM_VERSIONED_TEMPORAL_TABLE name] = t.name
					, [Auto-Generated Columns] = CASE LEN(GColumns) WHEN 0 THEN Null ELSE LEFT(GColumns,LEN(GColumns)-1) END
				FROM sys.tables as h
				INNER JOIN sys.tables as t ON t.history_table_id = h.[object_id]
				OUTER APPLY (SELECT (SELECT c.name + '','' FROM sys.columns as c
				WHERE c.generated_always_type != 0 and c.[object_id]=t.[object_id]
				FOR XML PATH(''''))) as c(GColumns)
				WHERE h.[object_id]=' + CAST(@Object_Id as VARCHAR) + '
				OPTION (RECOMPILE);';

			PRINT @SQL;
			PRINT @S;
			EXEC (@SQL);
		END

		SET @SQL = 'EXEC sp_spaceused ''' + @Object_Name + ''';';
		PRINT @SQL;
		PRINT @S;
		EXEC (@SQL);

		IF (SELECT IDENT_CURRENT(@Object_Name)) IS NOT NULL
		BEGIN
			SET @SQL = 'SELECT [Last_Identity_Value] = IDENT_CURRENT(''' + @Object_Name + ''');';
			PRINT @SQL;
			PRINT @S;
			EXEC (@SQL);
		END

		SET @SQL = 'EXEC sp_help ''' + @Object_Name + ''';';
		PRINT @SQL;
		EXEC (@SQL);
		PRINT @S;

/* Partition information */
	IF EXISTS (
		SELECT TOP 1 1 FROM sys.indexes i
		INNER JOIN sys.partition_schemes ps on ps.data_space_id = i.data_space_id
		WHERE i.object_id = @Object_Id
	)
	BEGIN
		SET @SQL = 'SELECT
		s.name as [Schema], o.name as Table_Name, IsNull(i.name,''HEAP'') as Index_Name
		, ps.name as Partition_Schema, pf.name as Partition_Function
		, pf.modify_date as Last_Modified, PA.partition_number as [Partition]
		, CASE pf.boundary_value_on_right WHEN 0 THEN ''LEFT'' ELSE ''RIGHT'' END as Function_Type
		, R1.value as Min_Border_Value, R2.value as Max_Border_Value
		, FG.name as [FileGroup_Name], PA.rows
		, SUM(AU.total_pages) as total_pages, SUM(AU.used_pages) as used_pages, SUM(AU.data_pages) as data_pages
		, sf.name as [File_Name], sf.filename as Physical_File_Name
		FROM sys.indexes as i with (NOLOCK)
		INNER JOIN sys.partition_schemes as ps with (NOLOCK) on ps.data_space_id = i.data_space_id
		INNER JOIN sys.partition_functions as pf with (NOLOCK) on pf.function_id = ps.function_id
		INNER JOIN sys.partitions AS PA with (NOLOCK)
			ON PA.object_id = i.object_id AND PA.index_id = i.index_id
		INNER JOIN sys.allocation_units AS AU with (NOLOCK)
			ON (AU.type IN (1, 3) AND AU.container_id = PA.hobt_id)
				OR (AU.type = 2 AND AU.container_id = PA.partition_id)
		INNER JOIN sys.objects AS o with (NOLOCK) ON i.object_id = o.object_id
		INNER JOIN sys.schemas AS s with (NOLOCK) ON o.schema_id = s.schema_id
		INNER JOIN sys.filegroups AS FG with (NOLOCK) ON FG.data_space_id = AU.data_space_id
		INNER JOIN sys.sysfiles AS sf with (NOLOCK) ON sf.groupid = AU.data_space_id
		LEFT JOIN sys.partition_range_values as R1 with (NOLOCK) ON R1.function_id = pf.function_id and R1.boundary_id + 1 = PA.partition_number
		LEFT JOIN sys.partition_range_values as R2 with (NOLOCK) ON R2.function_id = pf.function_id and R2.boundary_id = PA.partition_number
		WHERE o.object_id = ' + CAST(@Object_Id as NVARCHAR) + '
		GROUP BY s.name, o.name, i.name, PA.partition_number, R1.value, R2.value
		, ps.name, pf.name, pf.boundary_value_on_right, pf.modify_date, FG.name, PA.rows, sf.name, sf.filename
		ORDER BY o.name, PA.partition_number
		OPTION (RECOMPILE);';
		PRINT @SQL;
		PRINT @S;
		EXEC (@SQL);
	END
		
	IF EXISTS (SELECT TOP 1 1 FROM sys.triggers with (NOLOCK) WHERE parent_id = @Object_Id)
	BEGIN
		SET @SQL = 'SELECT tr.name as [Trigger Name], te.type_desc
			FROM sys.triggers as tr with (NOLOCK)
			INNER JOIN sys.trigger_events as te with (NOLOCK) on tr.object_id = te.object_id
			WHERE tr.parent_id = ' + CAST(@Object_Id as NVARCHAR) + '
			OPTION (RECOMPILE);';
		PRINT @SQL;
		PRINT @S;
		EXEC (@SQL);
	END

		/* Table's Index major stats */
		SET @SQL = N'
			SELECT [Object Name] = ''['' + OBJECT_SCHEMA_NAME(' + CAST(@Object_Id as NVARCHAR) + ')
					+ ''].['' + OBJECT_NAME(' + CAST(@Object_Id as NVARCHAR) + ') + '']''
				,	[Index ID] = p.index_id
				, [Compression Type] = p.data_compression_desc
				, [Alloc Unit ID] = a.allocation_unit_id
				, [Alloc Unit Type] = a.type_desc
				, [Partition] = p.partition_number
				, [First Page] = CAST(CAST(REVERSE(a.[first_page]) as binary(6)) as INT)
				, [Root Page] = CAST(CAST(REVERSE(a.[root_page]) as binary(6)) as INT)
				, [First IAM Page] = CAST(CAST(REVERSE(a.[first_iam_page]) as binary(6)) as INT)
			FROM sys.system_internals_allocation_units AS a with (NOLOCK)
			INNER JOIN sys.partitions AS p with (NOLOCK) on a.[container_id] = p.[partition_id]
			WHERE p.[object_id] = ' + CAST(@Object_Id as NVARCHAR) + '
			OPTION (RECOMPILE);';
		PRINT @SQL;
		PRINT @S;
		EXEC (@SQL);

		IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE major_id=@Object_Id)
		BEGIN
			SET @SQL = N'
/* Tables'' & columns'' Extended Properties */
			SELECT [Object Type] = CASE ep.minor_id WHEN 0 THEN o.type_desc ELSE ''COLUMN'' END
			, [Object Name] = IsNull(c.name,o.name)
			, [Extended Property Name] = ep.name
			, [Extended Property Value] = ep.value
			FROM sys.extended_properties as ep
			INNER JOIN sys.objects as o ON ep.major_id = o.object_id
			LEFT JOIN sys.columns as c ON c.column_id = ep.minor_id
				and o.object_id = c.object_id
			WHERE ep.class = 1 and ep.major_id = ' + CAST(@Object_Id as NVARCHAR) + '
			ORDER BY ep.minor_id
			OPTION (RECOMPILE);';
			PRINT @SQL;
			PRINT @S;
			EXEC (@SQL);
		END
		
		IF EXISTS (SELECT 1 FROM sys.indexes WHERE is_hypothetical=0 AND index_id=1 AND type=5 and [object_id]=@Object_Id)
		BEGIN
			SET @SQL = N'
/* Clustered Columnstore Index Info */
			DECLARE @oid INT = ' + CAST(@Object_Id as NVARCHAR) + ';
			DECLARE @groups TABLE (row_group_id INT,grows bigint,size_in_bytes bigint,[Description] nvarchar(60),state tinyint,partition_id bigint)
			DECLARE @segments TABLE (segment_id INT,encoding_type INT,on_disk_size bigint,column_id int,partition_id bigint)

			INSERT INTO @groups
			SELECT g.row_group_id
				, g.total_rows - IsNull(g.deleted_rows,0)
				, size_in_bytes
				, g.state_description
				, CASE WHEN g.state = 1 THEN 1 WHEN g.deleted_rows = 0 THEN 2 WHEN g.total_rows - g.deleted_rows > 0 THEN 3 ELSE 4 END
				, p.partition_id
			FROM sys.partitions as p, sys.column_store_row_groups as g
			WHERE p.object_id = @oid and p.index_id = 1 and g.object_id = @oid and g.index_id = 1 and p.partition_number = g.partition_number
			OPTION (RECOMPILE);

			SELECT ''COLUMNSTORE SEGMENT GROUP''
				, [Group(s) Status] = CASE state WHEN 1 THEN ''DELTA'' WHEN 2 THEN ''CURRENT'' WHEN 3 THEN ''CURRENT OLD'' ELSE ''DELETED'' END
				, [Description]
				, [# of Groups] = COUNT(*)
				, [Avg Group Rows] = AVG(grows)
				, [Total Stored Rows] = SUM(grows)
				, [Avg Segment Size, Mb] = CAST(AVG(size_in_bytes/1048576.) as DECIMAL(19,3))
				, [Total Disk Size, Mb] = CAST(SUM(size_in_bytes)/1048576. as DECIMAL(19,3))
			FROM @groups
			GROUP BY state, [Description]
			ORDER BY state
			OPTION (RECOMPILE);

			INSERT INTO @segments
			SELECT css.segment_id,css.encoding_type,css.on_disk_size,css.column_id,p.partition_id
			FROM sys.partitions as p
			INNER JOIN sys.column_store_segments as css ON css.hobt_id = p.hobt_id and p.partition_id = css.partition_id
			WHERE p.object_id = @oid and p.index_id = 1
			OPTION (RECOMPILE);

			SELECT [Columnstore Column Name] = c.name,g.[Description]
				, [Columnstore Encoding] = CASE encoding_type WHEN 1 THEN ''VALUE_BASED'' WHEN 2 THEN ''VALUE_HASH_BASED'' WHEN 3 THEN ''STRING_HASH_BASED''
					WHEN 4 THEN ''STORE_BY_VALUE_BASED'' WHEN 5 THEN ''STRING_STORE_BY_VALUE_BASED'' END
				,[# of Segments] = COUNT(c.column_id)
				,[Avg Segment Size, Mb] = CAST(CAST(AVG(on_disk_size/1048576.) as DECIMAL(19,3)) as VARCHAR)
				,[Total Disk Size, Mb] = CAST(CAST(SUM(on_disk_size/1048576.) as DECIMAL(19,3)) as VARCHAR)
			FROM sys.columns as c
			INNER JOIN @groups as g ON 1 = 1
			INNER JOIN @segments as s ON c.column_id = s.column_id and s.partition_id = g.partition_id and g.row_group_id = s.segment_id
			WHERE c.object_id = @oid and g.state != 4
			GROUP BY c.name,g.[Description],s.encoding_type
			OPTION (RECOMPILE);';
			PRINT @SQL;
			PRINT @S;
			EXEC (@SQL);
		END


	END

	IF /*OBJECTPROPERTY(OBJECT_ID(@Object_Name), 'IsPrimaryKey') = 1*/ @Object_Type COLLATE database_default = 'PK'
	BEGIN
		DECLARE @ParentID INT;

		SELECT @ParentID = parent_object_id FROM sys.objects WHERE object_id = @Object_Id;
		/*PRIMARY KEY*/
		/* 1. General Info */
		SET @SQL = 'SELECT ''PRIMARY KEY'' as [Type]
			, OBJECT_SCHEMA_NAME(' + CAST(@ParentID as NVARCHAR) + ') + ''.'' + OBJECT_NAME(' + CAST(@ParentID as NVARCHAR) + ') as Table_Name
			, i.name as Primary_Key_Name, i.type_desc, i.index_id
			, CASE i.fill_factor WHEN 0 THEN 100 ELSE i.fill_factor END as fill_factor, o.create_date, o.modify_date
			FROM sys.indexes as i with (NOLOCK)
			INNER JOIN sys.objects as o with (NOLOCK) ON o.parent_object_id = i.object_id
			WHERE o.type = ''PK'' and o.object_id = ' + CAST(@Object_Id as NVARCHAR) + ' and i.name = ''' + @Object_Name + '''
			OPTION (RECOMPILE);';
		PRINT @SQL;
		PRINT @S;
		EXEC (@SQL);

		/* 2. References */
		SET @SQL = 'SELECT RO.name as Referenced_Table, FK.Name as Foreign_Key_Name, FK.create_date, FK.modify_date
			FROM sys.objects AS PK with (NOLOCK)
			INNER JOIN sys.foreign_keys as FK with (NOLOCK) ON FK.referenced_object_id = PK.parent_object_id
			INNER JOIN sys.objects as RO with (NOLOCK) ON FK.parent_object_id = RO.object_id
			WHERE PK.name = ''' + @Object_Name + '''
			OPTION (RECOMPILE);';
		PRINT @SQL;
		PRINT @S;
		EXEC (@SQL);
	END /*PRIMARY KEY*/

	ELSE IF /*OBJECTPROPERTY(OBJECT_ID(@Object_Name), 'IsForeignKey') = 1*/ @Object_Type COLLATE database_default = 'F'
	BEGIN
		/* FOREIGN KEY */
		/* 1. General Info */
		SET @SQL = 'SELECT ''FOREIGN KEY'' as [Type],
					OBJECT_NAME(parent_object_id) as Table_Name,
					SCHEMA_NAME(schema_id) as [Schema_Name],
					name as Foreign_Key_Name,
					OBJECT_NAME(referenced_object_id) as Referenced_Table,
					Is_disabled, is_not_trusted,
					create_date, modify_date,
					object_id, parent_object_id, referenced_object_id
			FROM sys.foreign_keys with (NOLOCK)
			WHERE name = ''' + @Object_Name + '''
			OPTION (RECOMPILE);';
		PRINT @SQL;
		PRINT @S;
		EXEC (@SQL);

		/* 2. FK Columns */
		SET @SQL = 'SELECT OBJECT_NAME(fc.constraint_object_id) as Constraint_Object
				, OBJECT_NAME(fc.parent_object_id) as Parent_Object
				, COL_NAME(fc.parent_object_id, fc.parent_column_id) as Column_Name
				, t.name, c.max_length, c.is_nullable
				, OBJECT_NAME(fc.referenced_object_id) as Referenced_Object
				, COL_NAME(fc.referenced_object_id, fc.referenced_column_id) as Referenced_Column
			FROM sys.foreign_key_columns as fc with (NOLOCK)
			INNER JOIN sys.foreign_keys as fk with (NOLOCK) on fk.object_id = fc.constraint_object_id
			INNER JOIN sys.columns as c with (NOLOCK) on fc.parent_object_id = c.object_id and fc.parent_column_id = c.column_id
			INNER JOIN sys.types as t with (NOLOCK) on c.system_type_id = t.system_type_id
			WHERE fk.name = ''' + @Object_Name + '''
			ORDER BY fc.constraint_column_id
			OPTION (RECOMPILE);';
		PRINT @SQL;
		PRINT @S;
		EXEC (@SQL);

		/* Referencing Primary Key */
		SET @SQL = '
			SELECT OBJECT_SCHEMA_NAME(PK.parent_object_id) + ''.'' + OBJECT_NAME(PK.parent_object_id) as Referencing_Table
				, PK.name as Referenced_Primary_Key, PK.create_date, PK.modify_date, FK.Name as Foreign_Key_Name
			FROM sys.objects AS PK with (NOLOCK)
			INNER JOIN sys.foreign_keys as FK with (NOLOCK) ON FK.referenced_object_id = PK.parent_object_id
			-- INNER JOIN sys.objects as RO ON PK.parent_object_id = RO.object_id
			WHERE FK.Name = ''' + @Object_Name + ''' and PK.type = ''PK''
			OPTION (RECOMPILE);';
		PRINT @SQL;
		PRINT @S;
		EXEC (@SQL);

	END /* FOREIGN KEY */

	/* Report Index or table allocation */
	IF EXISTS (SELECT TOP 1 1 FROM sys.indexes WHERE name = @Object_Name) or @Object_Type COLLATE database_default ='U'
	BEGIN
		SET @SQL = 'SELECT OBJECT_NAME(i.OBJECT_ID) AS TableName,
			i.name AS TableIndexName,
			ps.index_id,
			ps.index_level,
			ps.index_type_desc,
			ps.alloc_unit_type_desc,
			ps.index_depth	index_level,
			CASE WHEN i.fill_factor = 0 OR (ps.index_level > 0 and i.is_padded = 0)
			THEN 100 ELSE i.fill_factor END AS fill_factor,
			ROUND(ps.avg_fragmentation_in_percent,3) as [AVG Frgmnt %],
			ROUND(ps.avg_page_space_used_in_percent,3) as [AVG Space Use %],
			ps.fragment_count,
			ROUND(ps.avg_fragment_size_in_pages,3) as [AVG Frgmnt Size],
			ps.page_count,
			CAST(ps.page_count/128. as NUMERIC(19,3)) as [Index Size Mb],
			ps.record_count,
			(ps.record_count / ps.page_count) as AVG_Records_per_Page,
			ps.ghost_record_count,
			ps.forwarded_record_count
		FROM sys.dm_db_index_physical_stats('
			/*Specifying Current Database */
			+ CAST(DB_ID() AS VARCHAR) + ', '
			/*Specifying Table*/
			+ CAST(@ParentID as VARCHAR) + ', '
			/*Specifying Index*/
			+ IsNull(CAST(@Index_Id as VARCHAR),'NULL')
			/*Specifying Reporting Mode*/

			+ ', NULL, ''' + @Parameter2 COLLATE database_default + ''') ps
		INNER JOIN sys.indexes i with (NOLOCK) ON i.OBJECT_ID = ps.OBJECT_ID AND i.index_id = ps.index_id
		WHERE i.name = ''' + @Object_Name + '''
		OPTION (RECOMPILE);';
		PRINT @SQL;
		PRINT @S;
		EXEC (@SQL);
	END /* Report Index or table allocation */

END /* @Object_Name Is NOT Null */

RETURN;
GO
EXEC #USP_HELP 