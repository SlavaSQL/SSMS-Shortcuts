/* 7 - 2020-09-30 Connections' & Agent Information
Consolidated by Slava Murygin
http://slavasql.blogspot.com/2016/02/ssms-query-shortcuts.html */
-- Fixed duration timing
USE tempdb
GO
IF OBJECT_ID('tempdb..#USP_GET7') IS NOT NULL
DROP PROCEDURE #USP_GET7;
GO
IF OBJECT_ID('tempdb..#config') IS NOT NULL DROP TABLE #config;
GO
IF OBJECT_ID('tempdb..#media_set') IS NOT NULL DROP TABLE #media_set;
GO
CREATE TABLE #media_set(media_set_id INT PRIMARY KEY,[FileName] nvarchar(260),[Number_of_Backups] INT,IsExist BIT);
GO
CREATE PROCEDURE #USP_GET7
@Param varchar(1000)=NULL
AS
DECLARE @S CHAR(80),@Where NVARCHAR(1000),@SQL NVARCHAR(4000),@V INT,@H INT,@nocount_off BIT,@SP CHAR(1);
DECLARE @or NCHAR(20),@nl NCHAR(15),@nv NCHAR(19),@yn NCHAR(38),@ij NCHAR(15),@vc NCHAR(18),@R1 NVARCHAR(1000);

SELECT @V=CAST(CAST(SERVERPROPERTY('ProductVersion') as CHAR(2)) as NUMERIC)
,@S=REPLICATE('-',80),@or=N' OPTION (RECOMPILE);',@nl=N' WITH (NOLOCK) '
,@nv=' as NVARCHAR(4000))',@yn=' WHEN 1 THEN ''Yes'' ELSE ''No'' END '
,@ij='INNER JOIN ',@vc=' as VARCHAR(1000))'
,@H=-1,@nocount_off=CASE @@OPTIONS & 512 WHEN 0 THEN 1 ELSE 0 END;

SET NOCOUNT ON

SELECT @SP=CASE Upper(@Param) COLLATE database_default
WHEN 'B' THEN 'B' WHEN 'L' THEN 'L' WHEN 'D' THEN 'D'
WHEN 'F' THEN 'F' WHEN 'O' THEN 'O' WHEN 'S' THEN 'S'
WHEN 'J' THEN 'J' WHEN 'M' THEN 'M' WHEN 'A' THEN 'A'
WHEN 'H' THEN 'H' ELSE '' END

PRINT '------------------ Option Ctrl-7 Description:
Returns information about current sessions.
With parametes gives information about Backups, Jobs and their history.
Parameters:
* (No Parameters) - Returns:
	- Full list of current sessions.
	- List of TCP endpoints.
	- List of TCP listeners.
* Number - filter by Session ID
* IP Address or "<local machine>" - filter by client Net address.
"S" - SQL Agent Settings (Including SQL Mail).
"B" - Lists backups. Checks backup file existence for the last 1000 backups.
"F","L","D","O" - Lists different backup types (F-Full;L-Log;D-Differential;O-Other).
* Database name - Lists backups for that Database.
"J" - Lists jobs.
Job name - Information about of selected job, including steps and schedules.
"H" - History of SQL agent Jobs as of today.
"H#" - History of SQL agent Jobs for any day back. Example: "H1" - Yesterday, "H5" - 5 days ago.
"H20170720" - History of SQL agent Jobs for any date back.
"A" - List of Alerts.
"M" - Mailing history.
* mailitem_id - Returns individual email settings/log with email body
'+@S;

SET @SQL =N'SELECT DISTINCT cn.session_id,
r.blocking_session_id,
CASE WHEN p.name=''Dedicated Admin Connection'' THEN ''YES'' ELSE ''NO'' END as DAC,
cn.local_tcp_port,
DB_NAME(t.dbid) as DB_Name,
r.command,
r.percent_complete,
s.login_name,
s.host_name,
cn.client_net_address,
p.name AS [Endpoint],
s.program_name,
t.text,
cn.num_reads,
cn.last_read,
cn.num_writes,
cn.last_write,
user_objects_alloc_page_count,
user_objects_dealloc_page_count,
internal_objects_alloc_page_count,
internal_objects_dealloc_page_count
FROM sys.[dm_exec_connections] as cn'+@nl+@ij+'
sys.dm_exec_sessions as s'+@nl+'on cn.session_id=s.session_id
'+@ij+'sys.endpoints as p'+@nl+'ON p.endpoint_id=s.endpoint_id
LEFT JOIN sys.dm_exec_requests as r'+@nl+'on cn.session_id=r.session_id
CROSS APPLY sys.dm_exec_sql_text(cn.[most_recent_sql_handle]) AS t
/* Temp DB Usage */
'+@ij+'sys.dm_db_task_space_usage as u'+@nl+'ON u.session_id=cn.session_id';

SET @R1='=CASE h.run_status WHEN 0 THEN ''Failed'' WHEN 1 THEN ''Succeeded''
WHEN 2 THEN ''Retry'' WHEN 3 THEN ''Canceled'' ELSE ''N/A'' END
,[Last Run Date/Time]=CAST(CAST(CAST(h.run_date as char(8)) as date)'+@vc+'+'' ''+
RIGHT(''00''+CAST(h.run_time/10000'+@vc+',2)+'':'' +
RIGHT(''00''+CAST(h.run_time/100 - (h.run_time/10000)*100'+@vc+',2)+'':''+
RIGHT(''00''+CAST(h.run_time'+@vc+',2)
,[Duration]=CAST(CAST(LEFT(Dur,4) as INT)'+@vc+'+'':''+SUBSTRING(Dur,5,2)+'':''+RIGHT(Dur,2)
,[Duration,Sec]=(h.run_duration/10000)*3600+(h.run_duration/100)*60+h.run_duration%100';

IF @Param Is Null
BEGIN
SET @SQL=@SQL+N'
WHERE cn.[most_recent_session_id]!=@@spid and s.is_user_process=1
ORDER BY cn.session_id'+@or;
PRINT @SQL;RAISERROR (@S,10,1) WITH NOWAIT;EXEC(@SQL);

SET @SQL=N';WITH CPU as (SELECT CAST(cpu_count/hyperthread_ratio'+@vc+' as cpu FROM sys.dm_os_sys_info),
Architecture as (SELECT SIGN(COUNT(*))+1 as a FROM master.sys.configurations WHERE NAME LIKE ''%64%''),
MaxThreads as (SELECT mt=CASE
WHEN cpu<5 THEN 256*a
WHEN cpu<9 THEN 288*a
WHEN cpu<17 THEN 352*a
WHEN cpu<33 THEN 480*a
WHEN cpu<65 THEN 738*a
WHEN cpu<129 THEN 3968+256*a
WHEN cpu<257 THEN 8064+256*a
ELSE -1 END FROM CPU,Architecture)
SELECT name as [TCP Endpoint]
,endpoint_id
,[Allowed]=IsNull(c.value_in_use,@@MAX_CONNECTIONS)
,[Current Threads]=CASE endpoint_id WHEN 1 THEN c.value_in_use ELSE (SELECT COUNT(*) FROM sys.dm_os_threads) END
,[Max Threads]=CASE endpoint_id WHEN 1 THEN 0 ELSE (
SELECT CASE WHEN value_in_use=0 or value_in_use>mt THEN mt ELSE value_in_use END FROM sys.configurations,MaxThreads WHERE name =''max worker threads'') END
,protocol_desc as [Protocol]
,type_desc as [Type]
,state_desc as [State]
,port
,CASE is_dynamic_port'+@yn+'as [Is Dynamic]
,IsNull(ip_address,''Not set'') as [IP Address]
FROM sys.tcp_endpoints as e'+@nl+'
OUTER APPLY (SELECT value_in_use FROM master.sys.configurations	
WHERE e.endpoint_id=1 and name=''remote admin connections''
) as c ORDER BY e.endpoint_id'+@or;
PRINT @SQL;RAISERROR (@S,10,1) WITH NOWAIT;EXEC (@SQL);

IF @V >=11
BEGIN
SET @SQL=N'SELECT ip_address,port,
CASE is_ipv4'+@yn+'as [Is IPv4],
type_desc as [Listener Type],
state_desc as [State],
start_time
FROM sys.dm_tcp_listener_states as ls'+@nl+'
ORDER BY ls.listener_id'+@or;
END
GOTO RET;
END
ELSE IF ISNUMERIC(@Param)=1 and EXISTS (SELECT TOP 1 1 FROM sys.[dm_exec_connections] WHERE session_id=CAST(@Param AS INT))
BEGIN -- opened sessions for SID
SET @SQL=@SQL+N' WHERE cn.session_id='+@Param+N';';
GOTO RET;
END
ELSE IF @Param='<local machine>' COLLATE database_default OR
(IsNumeric(REPLACE(@Param,'.',''))=1 AND IsNumeric(@Param)=0) OR (
IsNumeric(@Param)=1 AND	EXISTS (SELECT TOP 1 1 FROM sys.[dm_exec_connections] WHERE session_id=CAST(@Param AS INT)) )
BEGIN -- Reports all sessions for provided IP Address or '<local machine>'
SET @SQL=@SQL+N'
WHERE cn.client_net_address='''+@Param+'''
ORDER BY cn.session_id'+@or;
GOTO RET;
END
ELSE IF @SP in ('B','L','D','F','O')
or EXISTS (SELECT TOP 1 1 FROM master.sys.databases WHERE name=@Param COLLATE database_default)
BEGIN -- Backup history
/*Look for BU files and check their exstence*/
DECLARE @i INT,@m INT,@r INT,@n INT,@FileName nvarchar(260);

INSERT INTO #media_set(media_set_id,FileName,Number_of_Backups)
SELECT m.media_set_id,MAX(m.physical_device_name),COUNT(*)
FROM msdb..backupset as s
INNER JOIN msdb..backupmediafamily as m ON s.media_set_id=m.media_set_id
WHERE @SP='B' OR (@SP='O' and s.type not in ('D','L','I')) OR
(@SP='F' and s.type='D') OR (@SP='L' and s.type='L') OR (@SP='D' and s.type='I') OR
(@SP not in ('B','L','D','F','O') AND Upper(@Param) COLLATE database_default=s.[database_name])
GROUP BY m.media_set_id--,m.physical_device_name
ORDER BY m.media_set_id;

SELECT @i=MAX(media_set_id),@m=MIN(media_set_id),@n=1000 FROM #media_set;
WHILE @n>0 and @i>=@m
BEGIN
SELECT @FileName=[FileName] FROM #media_set WHERE media_set_id=@i;
EXEC master.sys.xp_fileexist @FileName,@r OUTPUT
UPDATE #media_set SET IsExist=CAST(@r as BIT) WHERE media_set_id=@i;
IF @i=@m SET @i-=1;
ELSE SELECT @i=MAX(media_set_id),@n -=1 FROM #media_set WHERE media_set_id<@i;
END
IF @SP in ('B','L','D','F','O')
SET @Where=CASE Upper(@Param) WHEN 'B' THEN '' WHEN 'O' THEN 'WHERE B.type not in (''D'',''L'',''I'')'
ELSE 'WHERE B.type='''+REPLACE(REPLACE(@Param,'D','I'),'F','D')+'''' END;
ELSE
SET @Where='WHERE B.dbn='''+@Param COLLATE database_default+'''';
	
SET @SQL=N';WITH B as (SELECT s.database_name as dbn,s.type
,s.backup_start_date as sd,s.backup_finish_date as fd
,DATEDIFF(second,s.backup_start_date,s.backup_finish_date) as bt
,s.recovery_model as [Model]
,fsm=CAST(CAST(ROUND(IsNull(s.compressed_backup_size,s.backup_size)/1048576.,3) as DECIMAL(13,3)) as VARCHAR(16))
,IsNull(s.compressed_backup_size,s.backup_size)/1048576. as fs
,m.[FileName],m.IsExist,Number_of_Backups
,s.name+IsNull('' ''+s.description,'''') as bn
,CASE s.recovery_model WHEN ''FULL'' THEN CASE WHEN last_log_backup_lsn is Null THEN ''Broken'' ELSE ''OK'' END
	ELSE ''N/A'' END as [Chain]
FROM msdb..backupset as s'+@nl+@ij+'#media_set as m ON s.media_set_id=m.media_set_id
LEFT JOIN master.sys.database_recovery_status as r'+@nl+'ON DB_NAME(r.database_id)=s.database_name
)
SELECT dbn as [Database name],B.[Model]
,[Backup Type]=CASE B.type
WHEN ''D'' THEN ''Full Database''
WHEN ''I'' THEN ''Differential''
WHEN ''L'' THEN ''Log''
WHEN ''F'' THEN ''File or filegroup''
WHEN ''G'' THEN ''Differential file''
WHEN ''P'' THEN ''Partial''
WHEN ''Q'' THEN ''Differential partial''
ELSE ''Unknown'' END,B.[Chain]
,[Start Date]=CONVERT(CHAR(10),B.sd,121)
,[Start Time]=CONVERT(CHAR(8),B.sd,108)
,[End Time]=CASE WHEN DATEDIFF(day,B.sd,B.fd)>0
THEN ''(+''+CAST(DATEDIFF(day,B.sd,B.fd)'+@vc+'+'')'' ELSE '''' END
+CONVERT(CHAR(8),B.fd,108)
,[Duration]=Right(''00''+CAST(B.bt/3600'+@vc+',2)+'':''
+Right(''00''+CAST((B.bt%3600)/60'+@vc+',2)+'':''
+Right(''0''+CAST(B.bt%60'+@vc+',2)
,[File size,Mb]=
RIGHT(SPACE(13)+CASE WHEN Len(B.fsm)>7
THEN CASE WHEN Len(B.fsm)>10
THEN LEFT(B.fsm,LEN(B.fsm)-10)+'',''+SUBSTRING(B.fsm,LEN(B.fsm) - 10,3)+'',''+RIGHT(B.fsm,7)
ELSE LEFT(B.fsm,LEN(B.fsm)-7)+'',''+RIGHT(B.fsm,7) END ELSE B.fsm END,13)
,[Backup file name]=B.[FileName]
,[Backups in File]=B.Number_of_Backups
,[Backup file exists]=CASE B.[IsExist] WHEN 1 THEN ''Yes'' WHEN 0 THEN ''No'' ELSE ''Over historical limit'' END
,B.bn as [Backup Name/Description]
FROM B '+@Where+' ORDER BY B.sd DESC'+@or;
GOTO RET;
END
ELSE IF @SP='S'
BEGIN -- SQL Agent Settings
CREATE TABLE #config(aus INT Null,sn SYSNAME Null,sat INT Null,sc NVARCHAR(255) Null,sr INT Null,mr INT Null,mrj INT Null,ef NVARCHAR(255) Null,el INT Null,er NVARCHAR(30) Null,mas INT Null,hs SYSNAME Null,st INT Null,ca VARBINARY(64) Null,rc INT Null,ln SYSNAME Null,hp VARBINARY(512) Null,lt INT Null,ip INT Null,cd INT Null,ol INT Null,so INT Null,ep NVARCHAR(64) null,sf INT Null,pe INT Null,te INT Null);
INSERT INTO #config exec msdb..sp_get_sqlagent_properties;

SET @SQL=N'SELECT ColumnNames as [Agent Property],ColumnValues as [Property value]
FROM	(SELECT
[SQLAgent Type]=CAST(ISNULL(sat,N'''')'+@nv+'
,[Agent Auto-Restart]=CAST(CASE mas'+@yn+@nv+'
,[Max History]=CAST(mr'+@nv+'
,[Max Job History]=CAST(mrj'+@nv+'
,[ErrorLogFile]=CAST(ISNULL(ef,'''')'+@nv+'
,[AgentLogLevel]=CAST(el'+@nv+'
,[NetSendRecipient]=CAST(ISNULL(er,N'''')'+@nv+'
,[Shutdown Wait, sec]=CAST(st'+@nv+'
,[SaveInSentFolder]=CAST(CASE sf'+@yn+@nv+'
,[WriteOemErrorLog]=CAST(CASE ol'+@yn+@nv+'
,[CPU idling enabled]=CAST(CASE pe'+@yn+@nv+'
,[CPU idle %]=CAST(ip'+@nv+'
,[CPU idle Duration]=CAST(cd'+@nv+'
,[LoginTimeout]=CAST(lt'+@nv+'
,[HostLoginName]=CAST(ISNULL(ln,N'''')'+@nv+'
,[LocalHostAlias]=CAST(ISNULL(hs,N'''')'+@nv+'
,[Agent Auto-Start]=CAST(CASE aus'+@yn+@nv+'
,[Replace Alert Tokens Enabled]=CAST(CASE te'+@yn+@nv+'
FROM #config
) as aa
UNPIVOT (ColumnValues FOR ColumnNames IN ([SQLAgent Type],[ErrorLogFile],[Agent Auto-Start]
,[Agent Auto-Restart],[Max History],[Max Job History],[AgentLogLevel],[NetSendRecipient]
,[CPU idling enabled],[CPU idle %],[CPU idle Duration],[Shutdown Wait, sec]
,[SaveInSentFolder],[WriteOemErrorLog],[LoginTimeout],[HostLoginName],[LocalHostAlias]
,[Replace Alert Tokens Enabled]) ) AS Unpvt'+
CASE WHEN (SELECT COUNT(*) FROM msdb..sysmail_servertype)>0 THEN
' UNION ALL
SELECT ColumnNames as [Job Property],ColumnValues as [Property value] FROM (
SELECT [Mail Server type]=CAST(servertype
	+ CASE is_incoming WHEN 1 THEN ''; Incoming'' ELSE '''' END
	+ CASE is_outgoing WHEN 1 THEN ''; Outgoing'' ELSE '''' END
	as VARCHAR(8000)),
[Mail Server Modification Date]=CAST(CONVERT(CHAR(23),last_mod_datetime,121) as VARCHAR(8000)),
[Mail Server Modification User]=CAST(last_mod_user as VARCHAR(8000))
FROM msdb..[sysmail_servertype]
) as aa UNPIVOT (ColumnValues FOR ColumnNames IN
([Mail Server type],[Mail Server Modification Date],[Mail Server Modification User])
) AS Unpvt'
ELSE '' END+@or;
PRINT @SQL;RAISERROR (@S,10,1) WITH NOWAIT;EXEC (@SQL);

IF EXISTS (SELECT TOP 1 1 FROM msdb..sysmail_servertype)
/* Report mail settings */
BEGIN
SET @SQL=N'SELECT [Mail Profile name]=p.name
,[Profile description]=p.description
,[Profile modification date]=p.last_mod_datetime
,[Profile modification user]=p.last_mod_user
,[Mail Account name]=a.name
,[Account description]=a.description
,[Mail address]=a.email_address
,[display name]=a.display_name
,[Reply to address]=a.replyto_address
,[Account modification date]=a.last_mod_datetime
,[Account modification user]=a.last_mod_user
,[Server type]=s.servertype
,[Server name]=s.servername
,[Server port]=s.port
,s.username
,s.credential_id
,s.use_default_credentials
,s.enable_ssl
,s.flags
,[Server timeout]=s.timeout
,[Server modification date]=s.last_mod_datetime
,[Server modification user]=s.last_mod_user
FROM msdb..[sysmail_profileaccount] as pa
RIGHT JOIN msdb..[sysmail_profile] as p ON p.profile_id=pa.profile_id
RIGHT JOIN msdb..[sysmail_account] as a ON a.account_id=pa.account_id
LEFT JOIN msdb..[sysmail_server] as s ON a.account_id=s.account_id'+@or;
GOTO RET;
END
END
ELSE IF @SP='J'
BEGIN -- Reports List of Jobs
SET @SQL=N'SELECT [Job Name]=j.name
,s.[Job Steps]
,[Enabled]=CASE j.enabled'+@yn+'
,[Job Next Run]=CONVERT(CHAR(19),a.Next_Run,121)
,[Job Creation Date]=j.date_created
,[Last Modification Date]=j.date_modified
,[Job Description]=j.description
,[Last Run]'+@R1+'
,[Last Message]=CAST(IsNull(h.message,'''') as NVARCHAR(MAX))+REPLACE(REPLACE(REPLACE(l.msg,''&#x0D;'',''|''),''&gt;&gt;&gt;'',''>''),''&lt;&lt;&lt;'',''<'')
FROM msdb..sysjobs j'+@nl+'
OUTER APPLY (SELECT MAX(a.next_scheduled_run_date) as Next_Run FROM msdb..sysjobactivity as a'+@nl+'WHERE j.job_id=a.job_id) as a
OUTER APPLY (SELECT MAX(instance_id) FROM msdb..sysjobhistory as m'+@nl+' WHERE j.job_id=m.job_id and m.step_id=0) as x(instance_id)
LEFT JOIN (SELECT *,Dur=RIGHT(''0000000''+CAST(run_duration'+@vc+',8) FROM msdb..sysjobhistory '+@nl+') h ON j.job_id=h.job_id and h.instance_id=x.instance_id
OUTER APPLY ( SELECT COUNT(*) FROM msdb..sysjobsteps as s'+@nl+' WHERE j.job_id=s.job_id) as s([Job Steps])
OUTER APPLY (SELECT IsNull(l.log,'''')+'''' FROM msdb..sysjobsteps as s'+@nl+'
	LEFT JOIN msdb..sysjobstepslogs as l'+@nl+'ON l.step_uid=s.step_uid WHERE s.job_id=j.job_id FOR XML PATH('''') ) as l(msg)
ORDER BY j.name'+@or;
PRINT @SQL;RAISERROR (@S,10,1) WITH NOWAIT;EXEC (@SQL);
SET @H=0;
END
ELSE IF @SP='M'
BEGIN -- Reports SQL mail log
SET @SQL=N'SELECT [Email send request date]=m.send_request_date
,m.mailitem_id
,[Sent status]=CASE m.sent_status
WHEN 0 THEN ''Unsent'' WHEN 1 THEN ''Sent''
WHEN 3 THEN ''Retrying'' ELSE ''Failed'' END
,[Error Description]=IsNull(e.description,''N/A'')
,[Send date]=m.sent_date
,[Subject]=m.subject
,[Mail To:]=m.recipients
,[CC:]=IsNull(m.copy_recipients,''None'')
,[BCC:]=IsNull(m.blind_copy_recipients,''None'')
,m.body_format,m.importance,m.sensitivity
,[Attachments]=IsNull(m.file_attachments,''None'')
,m.attachment_encoding
,[Query]=IsNull(m.query,''None'')
,[Query DB]=IsNull(m.execute_query_database,''N/A'')
,[Attach query result]=CASE m.attach_query_result_as_file'+@yn+'
,[Include query result header]=CASE m.query_result_header'+@yn+'
,m.query_result_width,m.query_result_separator
,[Exclude query output]=CASE m.exclude_query_output'+@yn+'
,[Append query error]=CASE m.append_query_error'+@yn+'
,[Message Description]=l.description
,m.send_request_user
,[Profile name]=l.name
,[Send account]=a.name
,m.last_mod_date,m.last_mod_user
FROM msdb..sysmail_mailitems as m
LEFT JOIN msdb..[sysmail_profile] l ON m.profile_id=l.profile_id
LEFT JOIN msdb..[sysmail_account] a ON a.account_id=m.sent_account_id
LEFT JOIN msdb..sysmail_event_log e ON e.mailitem_id=m.mailitem_id
	and (e.event_type IS NULL OR e.event_type !=''information'')
ORDER BY send_request_date DESC'+@or;
GOTO RET;
END
ELSE IF EXISTS (SELECT TOP 1 1 FROM msdb..sysjobs WHERE name=@Param COLLATE database_default)
BEGIN
/*Return information about the Job */
SET @SQL=N';WITH Notify as (SELECT * FROM (VALUES(0,''Never''),(1,''When the job succeeds''),
(2,''When the job fails''),(3,''Whenever the job completes'')) as n(id,[Value]))
SELECT [Job Property]=ColumnNames,[Property value]=ColumnValues
FROM (
SELECT [Job name]=CAST(j.name'+@nv+'
,[Enabled]=CAST(CASE j.enabled'+@yn+@nv+'
,[Job owner]=CAST(p.name'+@nv+'
,[Job created]=CAST(CONVERT(CHAR(19),j.date_created,121)'+@nv+'
,[Job modified]=CAST(CONVERT(CHAR(19),j.date_modified,121)'+@nv+'
,[Description]=CAST(j.description'+@nv+'
,[Start step ID]=CAST(j.start_step_id'+@nv+'
,[Job Steps]=CAST(s.[Job Steps]'+@nv+'
,[Notify eventlog]=CAST(n1.[Value]'+@nv+'
,[Notify email]=CAST(n2.[Value]'+@nv+'
,[Email operator]=CAST(o1.name'+@nv+'
,[Operator email]=CAST(o1.email_address'+@nv+'
,[Notify netsend]=CAST(n3.[Value]'+@nv+'
,[Netsend operator]=CAST(o2.name'+@nv+'
,[Operator netsend]=CAST(o2.netsend_address'+@nv+'
,[Notify pager]=CAST(n4.[Value]'+@nv+'
,[Pager operator]=CAST(o3.name'+@nv+'
,[Operator pager]=CAST(o3.pager_address'+@nv+'
,[Delete Job]=CAST(n5.[Value]'+@nv+'
,[Job version #]=CAST(j.version_number'+@nv+'
FROM msdb..sysjobs j'+@nl+'LEFT JOIN master.sys.server_principals p'+@nl+'ON j.owner_sid=p.sid
'+@ij+'Notify n1 ON n1.id=j.notify_level_eventlog
'+@ij+'Notify n2 ON n2.id=j.notify_level_email
'+@ij+'Notify n3 ON n3.id=j.notify_level_netsend
'+@ij+'Notify n4 ON n4.id=j.notify_level_page
'+@ij+'Notify n5 ON n5.id=j.delete_level
LEFT JOIN msdb..sysoperators o1'+@nl+'ON o1.id=j.notify_email_operator_id
LEFT JOIN msdb..sysoperators o2'+@nl+'ON o2.id=j.notify_netsend_operator_id
LEFT JOIN msdb..sysoperators o3'+@nl+'ON o3.id=j.notify_page_operator_id
OUTER APPLY ( SELECT COUNT(*) FROM msdb..sysjobsteps s'+@nl+'
WHERE j.job_id=s.job_id) s([Job Steps])
WHERE j.name='''+@Param COLLATE database_default+N'''
) aa
UNPIVOT (ColumnValues FOR ColumnNames IN ([Job name],[Enabled],[Job owner],[Job created],[Job modified]
,[Description],[Start step ID],[Job Steps],[Notify eventlog],[Notify email],[Email operator],[Operator email]
,[Notify netsend],[Netsend operator],[Operator netsend],[Notify pager],[Pager operator]
,[Operator pager],[Delete Job],[Job version #])) AS Unpvt'+@or;
PRINT @SQL;RAISERROR (@S,10,1) WITH NOWAIT;EXEC (@SQL);
/*Job steps*/
SET @SQL=N'SELECT s.step_id
,[Step name]=s.step_name,s.subsystem,s.command
,[On fail acrtion]=CASE s.on_fail_action
WHEN 1 THEN ''Quit with success''
WHEN 2 THEN ''Quit with failure''
WHEN 3 THEN ''Go to the next step''
WHEN 4 THEN ''Go to step #''+CAST(s.on_fail_step_id'+@vc+' END
,[On success acrtion]=CASE s.on_success_action
WHEN 1 THEN ''Quit with success''
WHEN 2 THEN ''Quit with failure''
WHEN 3 THEN ''Go to the next step''
WHEN 4 THEN ''Go to step #''+CAST(s.on_success_step_id'+@vc+' END
,[Last Run]=CASE s.last_run_date WHEN 0 THEN ''N/A'' ELSE
CASE s.last_run_outcome
WHEN 0 THEN ''Failed''
WHEN 1 THEN ''Succeeded''
WHEN 2 THEN ''Retry''
WHEN 3 THEN ''Canceled''
ELSE ''N/A'' END END
,[Duration]=CAST(last_run_duration / 3600'+@vc+'+'':''
+ RIGHT(''0''+CAST( (last_run_duration % 3600) / 60'+@vc+',2)+'':''
+ RIGHT(''0''+CAST( last_run_duration % 60'+@vc+',2)
,[Duration,sec]=s.last_run_duration
,[Last Run Date/Time]=CASE s.last_run_date WHEN 0 THEN ''N/A'' ELSE
CAST(CAST(CAST(s.last_run_date as char(8)) as date)'+@vc+'+'' '' +
RIGHT(''00''+CAST(s.last_run_time/10000'+@vc+',2)+'':'' +
RIGHT(''00''+CAST(s.last_run_time/100 - (s.last_run_time/10000)*100'+@vc+',2)+'':'' +
RIGHT(''00''+CAST(s.last_run_time'+@vc+',2) END
,[Step Log]=CAST(N''<StepLog>
''+REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(IsNull(l.log,N''''),''&'',''&#65286;''),''<'',''&#65308;''),''>'',''&#65310;''),N''[SQLSTATE 01000]'',N''''),N''[SQLSTATE 42000]'',N'''+@S+''')+ N''</StepLog>'' AS XML)
,s.last_run_retries
,s.server
,[Context]=s.database_name
,s.database_user_name
,s.retry_attempts,s.retry_interval
,s.output_file_name
,s.proxy_id
FROM msdb..sysjobsteps s'+@nl+@ij+'msdb..sysjobs j'+@nl+'on j.job_id=s.job_id
LEFT JOIN msdb..sysjobstepslogs l WITH (NOLOCK) ON l.step_uid=s.step_uid
WHERE j.name='''+@Param COLLATE database_default+N''''+@or;
PRINT @SQL;RAISERROR (@S,10,1) WITH NOWAIT;EXEC (@SQL);
/*Job schedules*/
SET @SQL=N'SELECT [Schedule name]=s.name
,[Enabled]=CASE s.enabled'+@yn+'
,[Schedule Frequency Interval]=CASE s.freq_type
WHEN 1 THEN ''One time only''
WHEN 4 THEN ''Daily:''
WHEN 8 THEN ''Weekly:''
WHEN 16 THEN ''Monthly:''
WHEN 32 THEN ''Monthly: ''+CASE s.freq_relative_interval
WHEN 1 THEN ''First ''
WHEN 2 THEN ''Second ''
WHEN 4 THEN ''Third ''
WHEN 8 THEN ''Fourth ''
WHEN 16 THEN ''Last ''
ELSE '''' END
WHEN 64 THEN ''Runs when the SQL Server Agent service starts''
WHEN 128 THEN ''Runs when the computer is idle''
ELSE '''' END+CASE s.freq_type
WHEN 1 THEN ''''
WHEN 4 THEN '' Every ''+CASE s.freq_interval WHEN 1 THEN ''day'' ELSE ''"''+CAST(s.freq_interval'+@vc+'+''" days'' END
WHEN 8 THEN
CASE s.freq_interval & 1 WHEN 1 THEN '' Sunday;'' ELSE '''' END+
CASE s.freq_interval & 2 WHEN 2 THEN '' Monday;'' ELSE '''' END+
CASE s.freq_interval & 4 WHEN 4 THEN '' Tuesday;'' ELSE '''' END+
CASE s.freq_interval & 8 WHEN 8 THEN '' Wednesday;'' ELSE '''' END+
CASE s.freq_interval & 16 WHEN 16 THEN '' Thursday;'' ELSE '''' END+
CASE s.freq_interval & 32 WHEN 32 THEN '' Friday;'' ELSE '''' END+
CASE s.freq_interval & 64 WHEN 64 THEN '' Saturday;'' ELSE '''' END+''''
WHEN 16 THEN '' On the "''+CAST(s.freq_interval'+@vc+'+''" day of the month''
WHEN 32 THEN CASE s.freq_interval
WHEN 1 THEN ''Sunday''
WHEN 2 THEN ''Monday''
WHEN 3 THEN ''Tuesday''
WHEN 4 THEN ''Wednesday''
WHEN 5 THEN ''Thursday''
WHEN 6 THEN ''Friday''
WHEN 7 THEN ''Saturday''
WHEN 8 THEN ''Day''
WHEN 9 THEN ''Weekday''
WHEN 10 THEN ''Weekend day''
ELSE '''' END
ELSE '''' END +
CASE WHEN ISNULL(s.freq_subday_interval,0)=0 THEN '' '' ELSE '' Every ''+CAST(s.freq_subday_interval'+@vc+'+'' '' END +
CASE s.freq_subday_type
	WHEN 1 THEN ''At the specified time: "'' +
RIGHT(''00''+CAST(js.next_run_time/10000'+@vc+',2)+'':'' +
RIGHT(''00''+CAST(js.next_run_time/100 - (js.next_run_time/10000)*100'+@vc+',2)+'':'' +
RIGHT(''00''+CAST(js.next_run_time'+@vc+',2)+''"''
	WHEN 2 THEN ''Seconds''
	WHEN 4 THEN ''Minutes''
	WHEN 8 THEN ''Hours''
ELSE '''' END+
CASE
	WHEN s.freq_type=8 THEN '',Every ''++CASE s.freq_recurrence_factor WHEN 1 THEN ''Week'' ELSE CAST(s.freq_recurrence_factor'+@vc+'+'' Week(s)'' End
	WHEN s.freq_type in (16,32) THEN '',Every ''+CASE s.freq_recurrence_factor WHEN 1 THEN ''Month'' ELSE CAST(s.freq_recurrence_factor'+@vc+'+'' Month(s)'' END
ELSE '''' END
,[Next Run]=CASE js.next_run_date WHEN 0 THEN ''N/A'' ELSE
CAST(CAST(CAST(js.next_run_date as char(8)) as date)'+@vc+'+'' ''+
RIGHT(''00''+CAST(js.next_run_time/10000'+@vc+',2)+'':'' +
RIGHT(''00''+CAST(js.next_run_time/100 - (js.next_run_time/10000)*100'+@vc+',2)+'':''+
RIGHT(''00''+CAST(js.next_run_time'+@vc+',2) END
,[Schedule started] =
CAST(CAST(CAST(s.active_start_date as char(8)) as date)'+@vc+'+'' '' +
RIGHT(''00''+CAST(s.active_start_time/10000'+@vc+',2)+'':'' +
RIGHT(''00''+CAST(s.active_start_time/100 - (s.active_start_time/10000)*100'+@vc+',2)+'':''+
RIGHT(''00''+CAST(s.active_start_time'+@vc+',2)
,[Schedule will end] =
CAST(CAST(CAST(s.active_end_date as char(8)) as date)'+@vc+'+'' '' +
RIGHT(''00''+CAST(s.active_end_time/10000'+@vc+',2)+'':'' +
RIGHT(''00''+CAST(s.active_end_time/100 - (s.active_end_time/10000)*100'+@vc+',2)+'':''+
RIGHT(''00''+CAST(s.active_end_time'+@vc+',2)
,s.originating_server_id,s.date_created,s.date_modified,s.version_number
FROM msdb..sysjobs j'+@nl+'
LEFT JOIN msdb..sysjobschedules js'+@nl+'on j.job_id=js.job_id
LEFT JOIN msdb..sysschedules s'+@nl+'on js.schedule_id=s.schedule_id
WHERE j.name='''+@Param COLLATE database_default+N''''+@or;
PRINT @SQL;RAISERROR (@S,10,1) WITH NOWAIT;EXEC (@SQL);
/*Return information about Job schedules*/
SET @SQL=N'
;WITH Rep AS (
SELECT r1=NCHAR(code),r2,id=CAST(ROW_NUMBER() OVER(ORDER BY GETDATE()) AS INT) FROM (
SELECT code,r2=N'''' FROM (SELECT TOP 32 code=(ROW_NUMBER() OVER(ORDER BY GETDATE()))-1 FROM sys.messages) m
WHERE code NOT IN (9,10,13)
UNION ALL SELECT * FROM (VALUES(62,NCHAR(65310)),(60,NCHAR(65308)),(59,''</m><m>''),(124,''</m><m>''),(38,NCHAR(65286))) x(r1,r2)) a)
, Rec AS (
SELECT m=CAST(ISNULL(hi.message,'''') AS NVARCHAR(MAX)),hi.job_id,hi.instance_id,ID=0
FROM msdb.dbo.sysjobhistory hi'+@nl+@ij+'msdb..sysjobs ji'+@nl+'ON hi.job_id=ji.job_id
WHERE ji.name='''+@Param COLLATE database_default+N'''
UNION ALL
SELECT REPLACE(m,r.r1,r.r2),c.job_id,c.instance_id,r.ID
FROM Rep r INNER JOIN Rec c ON r.ID=c.ID+1)
SELECT h.step_id
,[Step name]=CASE WHEN h.step_name=''(Job outcome)'' THEN ''Job: "''+j.name+''"'' ELSE h.step_name END
,[Run]'+@R1+'
,[Output Message]=CAST(N''<message><m>''+REPLACE(REPLACE(msg.m,N''[SQLSTATE 01000]'',N''</m><m>''),N'' Command: '',N''</m><m>Command: '')+N''</m></message>'' AS XML)
,[Operator: Emailed/Netsended/Paged] =
IsNull(o1.name,''None'')+''/'' +
IsNull(o2.name,''None'')+''/'' +
IsNull(o3.name,''None'')
FROM msdb..sysjobs j'+@nl+@ij+'(SELECT *,Dur=RIGHT(''0000000''+CAST(run_duration'+@vc+',8) FROM msdb..sysjobhistory '+@nl+') h ON j.job_id=h.job_id
INNER JOIN (SELECT m,job_id,instance_id FROM Rec WHERE ID=(SELECT MAX(id) FROM Rep)) msg
ON msg.job_id=h.job_id AND msg.instance_id=h.instance_id
OUTER APPLY (SELECT inst_id=MAX(i.instance_id) FROM msdb..sysjobhistory i'+@nl+'WHERE j.job_id=i.job_id) e
LEFT JOIN msdb..sysjobsteps as s'+@nl+' ON j.job_id=s.job_id and h.step_id=s.step_id
OUTER APPLY (
SELECT inst_id=MIN(instance_id) FROM msdb..sysjobhistory'+@nl+'
WHERE step_id=0 and j.job_id=job_id and (instance_id>h.instance_id or (h.step_id=0 and instance_id=h.instance_id))
) ah
LEFT JOIN msdb..sysoperators o1'+@nl+'ON o1.id=h.operator_id_emailed
LEFT JOIN msdb..sysoperators o2'+@nl+'ON o2.id=h.operator_id_netsent
LEFT JOIN msdb..sysoperators o3'+@nl+'ON o3.id=h.operator_id_paged
WHERE j.name='''+@Param COLLATE database_default+N'''
ORDER BY ah.inst_id DESC,h.step_id'+@or;
GOTO RET;
END
ELSE IF Upper(LEFT(@Param,1)) COLLATE database_default='H' and
(LEN(@Param)=1 or	IsNumeric(RIGHT(@Param,LEN(@Param)-1))=1 or IsDate(RIGHT(@Param,LEN(@Param)-1))=1)
BEGIN
IF LEN(@Param)=1 SET @H=0
ELSE
BEGIN
SET @H=CAST(RIGHT(@Param,LEN(@Param)-1) as INT)
IF IsDate(@H)=1 SET @H=DATEDIFF(DAY,CAST(@H AS VARCHAR),GETDATE())
END
END
ELSE IF @SP='A'
BEGIN -- List of Alerts
SET @SQL=N'SELECT [Alert name]=a.name
,[Enabled]=CASE a.enabled'+@yn+'
,[Occured]=a.occurrence_count
,[Last occurance]=CASE a.last_occurrence_date WHEN 0 THEN NULL ELSE
CONVERT(CHAR(19),DATEADD(SECOND,
(a.last_occurrence_time / 10000) * 3600+(a.last_occurrence_time % 10000 / 100) * 60+a.last_occurrence_time % 100,
CAST(CAST(a.last_occurrence_date'+@vc+' as DATETIME)),121) END
,a.severity
,a.delay_between_responses
,[Include event desc]=CASE a.include_event_description'+@yn+'
,a.notification_message
,m.text
,a.count_reset_date
,a.count_reset_time
,[Job name]=j.name
,[Has notification]=CASE a.has_notification'+@yn+'
,a.performance_condition
FROM msdb..sysalerts a'+@nl+'
LEFT JOIN master.sys.messages m'+@nl+'ON a.message_id=m.message_id and m.language_id=1033
LEFT JOIN msdb..sysjobs j'+@nl+'ON j.job_id=a.job_id'+@or
GOTO RET;
END
ELSE IF EXISTS (SELECT TOP 1 1 FROM msdb..sysmail_servertype) and
	IsNumeric(@Param)=1 and CAST(@Param as INT)>0
BEGIN -- Mail Info
SET @SQL=N'SELECT ColumnNames as [Job Property],ColumnValues as [Property value] FROM (
SELECT mailitem_id=CAST(m.mailitem_id'+@nv+'
,[Send request date]=CAST(CONVERT(CHAR(23),m.send_request_date,121)'+@nv+'
,[Mail body]=CAST(m.body'+@nv+'
,[Sent status]=CAST(CASE m.sent_status
WHEN 0 THEN ''Unsent'' WHEN 1 THEN ''Sent''
WHEN 3 THEN ''Retrying'' ELSE ''Failed'' END'+@nv+'
,[Error Description]=CAST(IsNull(e.description,''N/A'')'+@nv+'
,[Send date]=CAST(CONVERT(CHAR(23),m.sent_date,121)'+@nv+'
,[Subject]=CAST(m.subject'+@nv+'
,[Mail To:]=CAST(m.recipients'+@nv+'
,[CC:]=CAST(IsNull(m.copy_recipients,''None'')'+@nv+'
,[BCC:]=CAST(IsNull(m.blind_copy_recipients,''None'')'+@nv+'
,body_format=CAST(m.body_format'+@nv+'
,importance=CAST(m.importance'+@nv+'
,sensitivity=CAST(m.sensitivity'+@nv+'
,[Attachments]=CAST(IsNull(m.file_attachments,''None'')'+@nv+'
,[Attachment type]=CAST(m.attachment_encoding'+@nv+'
,[Query]=CAST(IsNull(m.query,''None'')'+@nv+'
,[Query DB]=CAST(IsNull(m.execute_query_database,''N/A'')'+@nv+'
,[Attach query result]=CAST(CASE m.attach_query_result_as_file'+@yn+@nv+'
,[Include query result header]=CAST(CASE m.query_result_header'+@yn+@nv+'
,[Query result width]=CAST(m.query_result_width'+@nv+'
,[Query result separator]=CAST(m.query_result_separator'+@nv+'
,[Exclude query output]=CAST(CASE m.exclude_query_output'+@yn+@nv+'
,[Append query error]=CAST(CASE m.append_query_error'+@yn+@nv+'
,[Message Description]=CAST(l.description'+@nv+'
,[Request user]=CAST(m.send_request_user'+@nv+'
,[Profile name]=CAST(l.name'+@nv+'
,[Send account]=CAST(a.name'+@nv+'
,[Modification Date]=CAST(CONVERT(CHAR(23),m.last_mod_date,121)'+@nv+'
,[Last modification user]=CAST(m.last_mod_user'+@nv+'
FROM msdb..sysmail_mailitems m
LEFT JOIN msdb..[sysmail_profile] l ON m.profile_id=l.profile_id
LEFT JOIN msdb..[sysmail_account] a ON a.account_id=m.sent_account_id
LEFT JOIN msdb..sysmail_event_log e ON e.mailitem_id=m.mailitem_id and (e.event_type IS NULL OR e.event_type !=''information'')
WHERE m.mailitem_id='+@Param+N') aa
UNPIVOT (ColumnValues FOR ColumnNames IN
(mailitem_id,[Send request date],[Mail body],[Sent status],[Error Description]
,[Send date],[Subject],[Mail To:],[CC:],[BCC:]
,body_format,importance,sensitivity,[Attachments],[Attachment type]
,[Query],[Query DB],[Attach query result],[Include query result header],[Query result width]
,[Query result separator],[Exclude query output],[Append query error],[Message Description]
,[Request user],[Profile name],[Send account],[Modification Date],[Last modification user]
) ) AS Unpvt'+@or
GOTO RET;
END
ELSE PRINT 'Unknown Parameter';

IF @H>=0
BEGIN
-- One day historical diagram
SET @SQL=N'DECLARE @View Tinyint;
/*
@View - Days back:
0 - Report for Today
1 - Report for Yesterday
2 - Report for day before Yesterday
*/
SET @View='+CAST(@H AS VARCHAR)+';
WITH Job_Hist as (
SELECT j.name,run_time,run_duration,h.run_date,j.job_id,h.run_status
,TS=DATEADD(SECOND,(run_time/10000)*3600+((run_time % 10000)/100)*60+run_time % 100,CAST(CAST(run_date'+@vc+' as DATETIME))
,DATEDIFF(Day,CONVERT(Date,CONVERT(CHAR(10),run_date,121)),GetDate()) as Run_Day
,SH=run_time/10000,SM=(run_time % 10000) / 100,SS=run_time % 100
,DH=run_duration/3600,DM=(run_duration % 3600)/60,DS=run_duration % 60
,Dur=RIGHT(''0000000''+CAST(run_duration'+@vc+',8)
,DurS=(run_duration/10000)*3600+(run_duration/100)*60+run_duration%100
FROM msdb..sysjobs as j'+@nl+@ij+'msdb..sysjobhistory as h'+@nl+'ON j.job_id=h.job_id
WHERE h.step_id=0
),DataPoints as (
SELECT job_id,run_time,run_duration,run_date,run_status,DurS
,x_dur=DH+(DM*60+DS)/3600.
,y=(DENSE_RANK() OVER (ORDER BY name DESC) - 1)
,x=SH+(SM*60+SS)/3600.+CASE WHEN @View=2 and Run_Day=0 THEN 24 ELSE 0 END
,Start_Time=RIGHT(''0''+CAST(SH'+@vc+',2)+'':''+RIGHT(''0''+CAST(SM'+@vc+',2)+'':''+RIGHT(''0''+CAST(SS'+@vc+',2)
,Duration=CAST(CAST(LEFT(Dur,4) as INT)'+@vc+'+'':''+SUBSTRING(Dur,5,2)+'':''+RIGHT(Dur,2)
FROM Job_Hist
WHERE run_duration>0 and Run_Day=@View
),History_Aggregate as (
SELECT job_id,name
,[Earliest recorded date]=CONVERT(CHAR(19),MIN(TS),121)
,[Recorded runs]=COUNT(*)
,[Total Duration]=CAST(SUM(DurS)/3600'+@vc+'+'':''
+RIGHT(''0''+CAST((SUM(DurS) % 3600)/60'+@vc+',2)+'':''
+RIGHT(''0''+CAST(SUM(DurS)%60'+@vc+',2)
,[Total Duration,Sec]=SUM(DurS)
FROM Job_Hist h
GROUP BY job_id,name
)
SELECT d.y+1 as [Order],h.name as [Job name]
,[Start Date]=CONVERT(CHAR(10),CAST(CAST(d.run_date'+@vc+' as DATETIME),121)
,d.Start_Time
,[Last Run]=CASE d.run_status
WHEN 0 THEN ''Failed''
WHEN 1 THEN ''Succeeded''
WHEN 2 THEN ''Retry''
WHEN 3 THEN ''Canceled''
ELSE ''N/A'' END
,d.Duration
,[Duration,Sec]=d.DurS
,[Earliest recorded date]
,[Recorded runs]
,[Total Duration]
,[Total Duration,Sec]
,[Spatial results]=CONVERT(GEOMETRY,''POLYGON((''+CAST(d.x'+@vc+'+'' ''+CAST(d.y'+@vc+'
+'',''+CAST(d.x'+@vc+'+'' ''+CAST(d.y+1'+@vc+'
+'',''+CAST(d.x+d.x_dur'+@vc+'+'' ''+CAST(d.y+1'+@vc+'
+'',''+CAST(d.x+d.x_dur'+@vc+'+'' ''+CAST(d.y'+@vc+'
+'',''+CAST(d.x'+@vc+'+'' ''+CAST(d.y'+@vc+'+''))'')
FROM DataPoints d '+@ij+'History_Aggregate h ON d.job_id=h.job_id
ORDER BY h.name,d.run_time'+@or;
END

RET:
PRINT @SQL;RAISERROR (@S,10,1) WITH NOWAIT;EXEC (@SQL);

IF @nocount_off>0 SET NOCOUNT OFF
RETURN 0;
GO
EXEC #USP_GET7 
