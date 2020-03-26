/*** *******************
give basic status of AG
Important points :
- synchronization_health_desc : should be HEALTHY (caution on AAG level, the DMV could show NOT_HEALTHY, here we are on database level and it is more reliable)
- synchronization_state_desc : should be SYNCRHONIZED
- is_local : 0 is remote server, 1 is local
- is_primary_replica : 1 is the primary for the db
- est_redo_completion_time_min : avg estimated redo time in minute when logs are redo on replica
This script will return multiple rows on primary for each db (one for primary and one for each secondary)

There are various entry-points of the data into the secondary systems.
The data first enters the server into memory at last_received_time
The data first enters the server on disk at last_hardened_time
The data first enters the database data files at last_redone_time
The data first becomes committed and available for reading by queries (outside of strange NOLOCK situations) at last_commit_time
************************/
SELECT 
	ar.replica_server_name, 
	adc.database_name, 
	ag.name AS ag_name, 
	drs.is_local, 
	drs.is_primary_replica, 
	drs.synchronization_state_desc, 
	drs.is_commit_participant, 
	drs.synchronization_health_desc, 
	drs.recovery_lsn, 
	drs.truncation_lsn, 
	drs.last_sent_lsn, 
	drs.last_sent_time, 
	drs.last_received_lsn, 
	drs.last_received_time, 
	drs.last_hardened_lsn, 
	drs.last_hardened_time, 
	drs.last_redone_lsn, 
	drs.last_redone_time, 
	drs.log_send_queue_size, 
	drs.log_send_rate, 
	drs.redo_queue_size, 
	drs.redo_rate, 
	drs.filestream_send_rate, 
	drs.end_of_log_lsn, 
	drs.last_commit_lsn, 
	drs.last_commit_time,
	CASE WHEN drs.redo_rate > 0 THEN (drs.redo_queue_size / drs.redo_rate) / 60.0 END AS est_redo_completion_time_min
FROM sys.dm_hadr_database_replica_states AS drs
INNER JOIN sys.availability_databases_cluster AS adc 
	ON drs.group_id = adc.group_id AND 
	drs.group_database_id = adc.group_database_id
INNER JOIN sys.availability_groups AS ag
	ON ag.group_id = drs.group_id
INNER JOIN sys.availability_replicas AS ar 
	ON drs.group_id = ar.group_id AND 
	drs.replica_id = ar.replica_id
ORDER BY 
	ag.name, 
	ar.replica_server_name, 
	adc.database_name;
GO

/*** ***********
check latency 
***************/

SELECT CAST(DB_NAME(database_id)as VARCHAR(40)) database_name,
Convert(VARCHAR(20),last_commit_time,113) last_commit_time
,CAST(CAST(((DATEDIFF(s,last_commit_time,GetDate()))/3600) as varchar) + ' hour(s), '
+ CAST((DATEDIFF(s,last_commit_time,GetDate())%3600)/60 as varchar) + ' min, '
+ CAST((DATEDIFF(s,last_commit_time,GetDate())%60) as varchar) + ' sec' as VARCHAR(30)) time_behind_primary
 ,DATEDIFF(s, last_commit_time, GETDATE()) as time_behind_primary_sec
, log_send_queue_size
,redo_queue_size
,redo_rate
, CASE WHEN redo_rate > 0 THEN CONVERT(VARCHAR(20),DATEADD(mi,(redo_queue_size/redo_rate/60.0),GETDATE()),113) END estimated_completion_time
, CASE WHEN redo_rate > 0 THEN CAST((redo_queue_size/redo_rate/60.0) as decimal(10,2)) END [estimated_recovery_time_minutes]
, CASE WHEN redo_rate > 0 THEN (redo_queue_size/redo_rate) END [estimated_recovery_time_seconds]
,CONVERT(VARCHAR(20),GETDATE(),113) [current_time]
, synchronization_health_desc, synchronization_state_desc
FROM sys.dm_hadr_database_replica_states
WHERE last_redone_time is not null;


/********************
Check error 1480 (failover) in alwayson trace
*********************/
;WITH cte_HADR AS (SELECT object_name, CONVERT(XML, event_data) AS data
FROM sys.fn_xe_file_target_read_file('AlwaysOn*.xel', null, null, null)
WHERE object_name = 'error_reported'
)

SELECT data.value('(/event/@timestamp)[1]','datetime') AS [timestamp],
       data.value('(/event/data[@name=''error_number''])[1]','int') AS [error_number],
       data.value('(/event/data[@name=''message''])[1]','varchar(max)') AS [message]
FROM cte_HADR
WHERE data.value('(/event/data[@name=''error_number''])[1]','int') = 1480


/***
Restore db en recovery depuis les BACK full, diff et trn
Changer le @chemin,0,0,1 (1 = scripter) en @chemin, 0,0,0 (exécuter)
laisse la base en RECOVERY
**/
/*
EXEC SYSDBA.[dbo].[sp_adm_restoredb_DIFF_LOGS] 'OPTV1_PRD001',@replica,@base,@chemin,0,0,1, 'TFPRDOPTSQL35_OPTV1_PRD001_FULL_A.BAK', 'TFPRDOPTSQL35_OPTV1_PRD001_FULL_B.BAK', 'TFPRDOPTSQL35_OPTV1_PRD001_FULL_C.BAK', 0,0,1
*/
go


/**
https://sqlundercover.com/2017/09/19/7-ways-to-query-always-on-availability-groups-using-sql/
**/
-- get status and mode

WITH AGStatus AS(
SELECT
name as AGname,
replica_server_name,
CASE WHEN  (primary_replica  = replica_server_name) THEN  1
ELSE  '' END AS IsPrimaryServer,
secondary_role_allow_connections_desc AS ReadableSecondary,
[availability_mode]  AS [Synchronous],
failover_mode_desc
FROM master.sys.availability_groups Groups
INNER JOIN master.sys.availability_replicas Replicas ON Groups.group_id = Replicas.group_id
INNER JOIN master.sys.dm_hadr_availability_group_states States ON Groups.group_id = States.group_id
)
 
Select
[AGname],
[Replica_server_name],
[IsPrimaryServer],
[Synchronous],
[ReadableSecondary],
[Failover_mode_desc]
FROM AGStatus
--WHERE
--IsPrimaryServer = 1
--AND Synchronous = 1
ORDER BY
AGname ASC,
IsPrimaryServer DESC;

GO

/* dashboard */
SET NOCOUNT ON;
 
DECLARE @AGname NVARCHAR(128);
 
DECLARE @SecondaryReplicasOnly BIT;
 
SET @AGname = 'AG1';        --SET AGname for a specific AG for SET to NULL for ALL AG's
 
IF OBJECT_ID('TempDB..#tmpag_availability_groups') IS NOT NULL
DROP TABLE [#tmpag_availability_groups];
 
SELECT *
INTO [#tmpag_availability_groups]
FROM   [master].[sys].[availability_groups];
 
IF(@AGname IS NULL
OR EXISTS
(
SELECT [Name]
FROM   [#tmpag_availability_groups]
WHERE  [Name] = @AGname
))
BEGIN
 
IF OBJECT_ID('TempDB..#tmpdbr_availability_replicas') IS NOT NULL
DROP TABLE [#tmpdbr_availability_replicas];
 
IF OBJECT_ID('TempDB..#tmpdbr_database_replica_cluster_states') IS NOT NULL
DROP TABLE [#tmpdbr_database_replica_cluster_states];
 
IF OBJECT_ID('TempDB..#tmpdbr_database_replica_states') IS NOT NULL
DROP TABLE [#tmpdbr_database_replica_states];
 
IF OBJECT_ID('TempDB..#tmpdbr_database_replica_states_primary_LCT') IS NOT NULL
DROP TABLE [#tmpdbr_database_replica_states_primary_LCT];
 
IF OBJECT_ID('TempDB..#tmpdbr_availability_replica_states') IS NOT NULL
DROP TABLE [#tmpdbr_availability_replica_states];
 
SELECT [group_id],
[replica_id],
[replica_server_name],
[availability_mode],
[availability_mode_desc]
INTO [#tmpdbr_availability_replicas]
FROM   [master].[sys].[availability_replicas];
 
SELECT [replica_id],
[group_database_id],
[database_name],
[is_database_joined],
[is_failover_ready]
INTO [#tmpdbr_database_replica_cluster_states]
FROM   [master].[sys].[dm_hadr_database_replica_cluster_states];
 
SELECT *
INTO [#tmpdbr_database_replica_states]
FROM   [master].[sys].[dm_hadr_database_replica_states];
 
SELECT [replica_id],
[role],
[role_desc],
[is_local]
INTO [#tmpdbr_availability_replica_states]
FROM   [master].[sys].[dm_hadr_availability_replica_states];
 
SELECT [ars].[role],
[drs].[database_id],
[drs].[replica_id],
[drs].[last_commit_time]
INTO [#tmpdbr_database_replica_states_primary_LCT]
FROM   [#tmpdbr_database_replica_states] AS [drs]
LEFT JOIN [#tmpdbr_availability_replica_states] [ars] ON [drs].[replica_id] = [ars].[replica_id]
WHERE  [ars].[role] = 1;
 
SELECT [AG].[name] AS [AvailabilityGroupName],
[AR].[replica_server_name] AS [AvailabilityReplicaServerName],
[dbcs].[database_name] AS [AvailabilityDatabaseName],
ISNULL([dbcs].[is_failover_ready],0) AS [IsFailoverReady],
ISNULL([arstates].[role_desc],3) AS [ReplicaRole],
[AR].[availability_mode_desc] AS [AvailabilityMode],
CASE [dbcs].[is_failover_ready]
WHEN 1
THEN 0
ELSE ISNULL(DATEDIFF([ss],[dbr].[last_commit_time],[dbrp].[last_commit_time]),0)
END AS [EstimatedDataLoss_(Seconds)],
ISNULL(CASE [dbr].[redo_rate]
WHEN 0
THEN-1
ELSE CAST([dbr].[redo_queue_size] AS FLOAT) / [dbr].[redo_rate]
END,-1) AS [EstimatedRecoveryTime_(Seconds)],
ISNULL([dbr].[is_suspended],0) AS [IsSuspended],
ISNULL([dbr].[suspend_reason_desc],'-') AS [SuspendReason],
ISNULL([dbr].[synchronization_state_desc],0) AS [SynchronizationState],
ISNULL([dbr].[last_received_time],0) AS [LastReceivedTime],
ISNULL([dbr].[last_redone_time],0) AS [LastRedoneTime],
ISNULL([dbr].[last_sent_time],0) AS [LastSentTime],
ISNULL([dbr].[log_send_queue_size],-1) AS [LogSendQueueSize],
ISNULL([dbr].[log_send_rate],-1) AS [LogSendRate_KB/S],
ISNULL([dbr].[redo_queue_size],-1) AS [RedoQueueSize_KB],
ISNULL([dbr].[redo_rate],-1) AS [RedoRate_KB/S],
ISNULL(CASE [dbr].[log_send_rate]
WHEN 0
THEN-1
ELSE CAST([dbr].[log_send_queue_size] AS FLOAT) / [dbr].[log_send_rate]
END,-1) AS [SynchronizationPerformance],
ISNULL([dbr].[filestream_send_rate],-1) AS [FileStreamSendRate],
ISNULL([dbcs].[is_database_joined],0) AS [IsJoined],
[arstates].[is_local] AS [IsLocal],
ISNULL([dbr].[last_commit_lsn],0) AS [LastCommitLSN],
ISNULL([dbr].[last_commit_time],0) AS [LastCommitTime],
ISNULL([dbr].[last_hardened_lsn],0) AS [LastHardenedLSN],
ISNULL([dbr].[last_hardened_time],0) AS [LastHardenedTime],
ISNULL([dbr].[last_received_lsn],0) AS [LastReceivedLSN],
ISNULL([dbr].[last_redone_lsn],0) AS [LastRedoneLSN]
FROM   [#tmpag_availability_groups] AS [AG]
INNER JOIN [#tmpdbr_availability_replicas] AS [AR] ON [AR].[group_id] = [AG].[group_id]
INNER JOIN [#tmpdbr_database_replica_cluster_states] AS [dbcs] ON [dbcs].[replica_id] = [AR].[replica_id]
LEFT OUTER JOIN [#tmpdbr_database_replica_states] AS [dbr] ON [dbcs].[replica_id] = [dbr].[replica_id]
AND [dbcs].[group_database_id] = [dbr].[group_database_id]
LEFT OUTER JOIN [#tmpdbr_database_replica_states_primary_LCT] AS [dbrp] ON [dbr].[database_id] = [dbrp].[database_id]
INNER JOIN [#tmpdbr_availability_replica_states] AS [arstates] ON [arstates].[replica_id] = [AR].[replica_id]
WHERE  [AG].[name] = ISNULL(@AGname,[AG].[name])
ORDER BY [AvailabilityReplicaServerName] ASC,
[AvailabilityDatabaseName] ASC;
 
/*********************/
 
END;
ELSE
BEGIN
RAISERROR('Invalid AG name supplied, please correct and try again',12,0);
END;

GO

/*
get last backup for DB
*/
select @@SERVERNAME as server, name as db, x.backup_finish_date as last_backup_log, d.recovery_model_desc, d.state_desc, case when ha.database_id is not null then 'AAG' else 'STANDALONE' end as HADR from sys.databases d
left outer join (
select database_name, backup_finish_date, ROW_NUMBER() over(PARTITION by database_name order by backup_finish_date desc) as rn
from msdb.dbo.backupset b
where type ='L') x on x.database_name = d.name and rn = 1
left outer join (select distinct database_id from sys.dm_hadr_database_replica_states) ha on ha.database_id = d.database_id
where d.recovery_model_desc <> 'SIMPLE'