/* 3 - 2017-07-07 - Locks on objects 
Consolidated by Slava Murygin
http://slavasql.blogspot.com/2016/02/ssms-query-shortcuts.html
*/
/* https://msdn.microsoft.com/en-us/library/ms187749.aspx */
IF OBJECT_ID('tempdb..#USP_GETLOCKS') IS NOT NULL
DROP PROCEDURE #USP_GETLOCKS;
GO
CREATE PROCEDURE #USP_GETLOCKS
@Parameter1 SYSNAME = NULL
WITH RECOMPILE
AS
PRINT 'Procedure "Ctrl-3" returns following, depending on parameters:';
PRINT '1. No parameters: List of all current Locks. Similar to sp_lock with some extentions.';
PRINT '2. "Database ID": List of all current Locks for the specified Database.';
PRINT '3. "Session ID": List of all current Locks for the specified session.';
PRINT ' That is possible that "Session ID" and "Database ID" cross each other and wxtra results are returned.';
PRINT '4. "Database Name": List of all current Locks for the specified Database.';
PRINT '5. "Table Name": List of all current Locks on that table.';
PRINT '	Have to be a table in the current database or with fully qualified name.';
PRINT '6. "Object Id": List of all current Locks on that object.';
PRINT '7. "TAB", "PAG", "DB": List of all current Locks for specified type of an object.';
PRINT '8. "X", "U", etc. : List of all current Locks with specified mode.';
PRINT '9. "IP" or "Host Name" or "Login Name" or "Application Name": List of all current Locks for specified source.';
PRINT ' That option might match table or database name.';
PRINT 'Example 1: tbl_MyTable ';
PRINT 'Example 2: TAB ';
PRINT 'Example 3: Sch-M ';
PRINT 'Example 4: 123456789 ';
PRINT 'In case of necesity to narrow the result please tune the query below:';
PRINT '----------------------------------------------------------------------------------------------'

DECLARE @SQL VARCHAR(MAX);
DECLARE @Object_Id INT;
DECLARE @Index_Id INT = NULL;

SET @SQL = ';WITH Report as (
SELECT convert (smallint, sl.req_spid) as SPID
	, DB_NAME(sl.rsc_dbid) as [Database Name]
	, substring (v.name, 1, 4) As Type
	, substring (sl.rsc_text, 1, 32) as Resource
	, OBJECT_NAME(sl.rsc_objid, sl.rsc_dbid) as [Object Name]
	, sl.rsc_indid As Index_Id
	, sl.rsc_dbid as [Database_Id]
	, CASE WHEN substring (v.name, 1, 4) in (''PAG'', ''RID'')
		THEN LEFT(substring (sl.rsc_text, 1, 32),CHARINDEX('':'',substring (sl.rsc_text, 1, 32))-1) ELSE '''' END AS [File_Id]
	, CASE substring (v.name, 1, 4)
		WHEN ''PAG''	THEN SUBSTRING(substring (sl.rsc_text, 1, 32),CHARINDEX('':'',substring (sl.rsc_text, 1, 32))+1,512)
		WHEN ''RID''	THEN SUBSTRING(substring (sl.rsc_text, 1, 32),CHARINDEX('':'',substring (sl.rsc_text, 1, 32))+1,
			CHARINDEX('':'',substring (sl.rsc_text, 1, 32),CHARINDEX('':'',substring (sl.rsc_text, 1, 32))+1)-CHARINDEX('':'',substring (sl.rsc_text, 1, 32))-1)
		ELSE '''' END AS [Page_Id]
	, CASE substring (v.name, 1, 4) WHEN ''RID''
		THEN SUBSTRING(substring (sl.rsc_text, 1, 32),CHARINDEX('':'',substring (sl.rsc_text, 1, 32),CHARINDEX('':'',substring (sl.rsc_text, 1, 32))+1)+1,512) ELSE '''' END AS [Row_Id]
	, sl.rsc_objid
	, CASE substring (u.name, 1, 8)
		WHEN ''S'' THEN ''Shared''
		WHEN ''U'' THEN ''Update''
		WHEN ''X'' THEN ''Exclusive''
		WHEN ''IS'' THEN ''Intent Shared''
		WHEN ''IX'' THEN ''Intent Exclusive''
		WHEN ''SIX'' THEN ''Shared with Intent Exclusive''
		WHEN ''Sch-M'' THEN ''Schema Modification''
		WHEN ''Sch-S'' THEN ''Schema Stability''
		WHEN ''BU'' THEN ''Bulk Update''
		END + '' ('' + substring (u.name, 1, 8) + '')'' as Mode
	, substring (x.name, 1, 5) As Status
	, cn.client_net_address, s.[host_name], s.login_name, s.[program_name], cn.last_read, cn.last_write
FROM master.dbo.syslockinfo as sl
INNER JOIN master.dbo.spt_values as v ON sl.rsc_type = v.number and v.type = ''LR''
INNER JOIN master.dbo.spt_values as x ON sl.req_status = x.number and x.type = ''LS''
INNER JOIN master.dbo.spt_values as u ON sl.req_mode + 1 = u.number and u.type = ''L''
INNER JOIN sys.[dm_exec_connections] cn ON cn.session_id = convert (smallint, sl.req_spid)
INNER JOIN sys.dm_exec_sessions s on cn.session_id = s.session_id
WHERE IsNull(OBJECT_NAME(sl.rsc_objid, sl.rsc_dbid),'''') != ''spt_values''
	/* and not (LEFT(v.name, 2) = ''DB'' and LEFT(u.name, 1) = ''S'' and LEFT(x.name, 5) = ''GRANT'') */ /*exclude "Shared DB" granted access (can be omitted)*/
';

IF @Parameter1 is not null
BEGIN
	IF @Parameter1 COLLATE database_default in ('S','U','X','IS','IX','SIX','Sch-M','Sch-S','BU')
		SET @SQL = @SQL COLLATE database_default + 'and substring(u.name, 1, 8) = ''' + @Parameter1 COLLATE database_default + '''';
	ELSE IF @Parameter1 COLLATE database_default in ('TAB','DB','PAG')
		SET @SQL = @SQL COLLATE database_default + 'and substring (v.name, 1, 4) = ''' + @Parameter1 COLLATE database_default + '''';
	ELSE IF IsNumeric(@Parameter1) = 1
		SET @SQL = @SQL COLLATE database_default + ' and ( IsNull(sl.rsc_objid,0) = ' + CAST(@Parameter1 as VARCHAR)
			+ ' or IsNull(sl.rsc_dbid,0) = ' + CAST(@Parameter1 as VARCHAR)
			+ ' or IsNull(convert (smallint, sl.req_spid),0) = ' + CAST(@Parameter1 as VARCHAR) + ')';
	ELSE If OBJECT_ID(@Parameter1) Is Not Null
		SET @SQL = @SQL COLLATE database_default + ' and sl.rsc_objid = ' + CAST(OBJECT_ID(@Parameter1) as VARCHAR);
	ELSE If DB_ID(@Parameter1) Is Not Null
		SET @SQL = @SQL COLLATE database_default + ' and sl.rsc_dbid = ' + CAST(DB_ID(@Parameter1) as VARCHAR);
	ELSE
	BEGIN
		PRINT 'Parameter have''t been recognized, Searching for IPm Host/User/Program name...'
		SET @SQL = @SQL COLLATE database_default + ' and ( cn.client_net_address = ''' + CAST(@Parameter1 as NVARCHAR(256)) + ''''
			+ ' or s.[host_name] = ''' + CAST(@Parameter1 as NVARCHAR(256)) + ''''
			+ ' or s.login_name = ''' + CAST(@Parameter1 as NVARCHAR(256)) + ''''
			+ ' or s.[program_name] = ''' + CAST(@Parameter1 as NVARCHAR(256)) + ''')';
		PRINT '=============================================================================='
	END
END

SET @SQL = @SQL COLLATE database_default + ')
SELECT SPID, [Database Name], Type, Resource, [Object Name]
	, Index_Id, Database_Id, File_Id, Page_Id, Row_Id
	, rsc_objid, Mode, Status, client_net_address, [host_name]
	, login_name, [program_name], MAX(last_read) as [Last read], MAX(last_write) as [Last write]
FROM Report	
GROUP BY SPID, [Database Name], Type, Resource, [Object Name], Index_Id, Database_Id, File_Id, Page_Id, Row_Id
	, rsc_objid, Mode, Status, client_net_address, [host_name], login_name, [program_name]
ORDER BY SPID
OPTION (RECOMPILE);';

PRINT @SQL;
EXEC (@SQL);

RETURN 0;
GO
EXEC #USP_GETLOCKS 