/*
2020-03-16
Records waits events
Create a job executing the procedure every hour.
*/

IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE name = 'monitor')
BEGIN
    PRINT N'Creating schema monitor'
    EXEC ('CREATE SCHEMA monitor;')
END
GO

IF EXISTS(SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('monitor.wait_stats'))
BEGIN
	PRINT N'Drop table monitor.wait_stats'
	DROP TABLE monitor.wait_stats;
END
PRINT N'Creating table monitor.wait_stats'
CREATE TABLE monitor.wait_stats
(
	wait_type varchar(60) NOT NULL,
	waiting_tasks_count bigint,
	wait_time_ms bigint,
	max_wait_time_ms bigint,
	signal_wait_time_ms bigint,
	collect_date DATETIME
)
PRINT N'Table monitor.wait_stats created'
PRINT N'Creating index on monitor.wait_stats'
CREATE CLUSTERED INDEX idx_wait_stats_collect_date ON monitor.wait_stats (collect_date);


IF EXISTS(SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('monitor.last_wait_stats'))
BEGIN
	PRINT N'Drop table monitor.last_wait_stats'
	DROP TABLE monitor.last_wait_stats;
END
PRINT N'Creating table monitor.last_wait_stats'
CREATE TABLE monitor.last_wait_stats
(
	wait_type varchar(60) NOT NULL,
	waiting_tasks_count bigint,
	wait_time_ms bigint,
	max_wait_time_ms bigint,
	signal_wait_time_ms bigint,
	collect_date DATETIME
)
PRINT N'Table monitor.last_wait_stats created'
GO


IF EXISTS(SELECT 1 FROM sys.procedures WHERE object_id = OBJECT_ID('monitor.usp_save_wait_stats'))
BEGIN
	PRINT N'Dropping procedure monitor.usp_save_wait_stats';
	DROP PROCEDURE monitor.usp_save_wait_stats;
END
PRINT N'Creating procedure monitor.usp_save_wait_stats'
GO
CREATE PROCEDURE monitor.usp_save_wait_stats
AS
BEGIN
	DECLARE @collect_date DATETIME = GETDATE();
	DECLARE @waits TABLE (collect_date DATETIME, wait_type varchar(60)
		, waiting_tasks_count bigint, wait_time_ms bigint
		, max_wait_time_ms bigint, signal_wait_time_ms bigint);
	INSERT INTO @waits (collect_date, wait_type, waiting_tasks_count, wait_time_ms, max_wait_time_ms, signal_wait_time_ms)
	SELECT @collect_date, wait_type, waiting_tasks_count, wait_time_ms, max_wait_time_ms, signal_wait_time_ms
	FROM sys.dm_os_wait_stats
WHERE wait_type NOT IN ( -- from Paul Randal
        N'BROKER_EVENTHANDLER', -- https://www.sqlskills.com/help/waits/BROKER_EVENTHANDLER
        N'BROKER_RECEIVE_WAITFOR', -- https://www.sqlskills.com/help/waits/BROKER_RECEIVE_WAITFOR
        N'BROKER_TASK_STOP', -- https://www.sqlskills.com/help/waits/BROKER_TASK_STOP
        N'BROKER_TO_FLUSH', -- https://www.sqlskills.com/help/waits/BROKER_TO_FLUSH
        N'BROKER_TRANSMITTER', -- https://www.sqlskills.com/help/waits/BROKER_TRANSMITTER
        N'CHECKPOINT_QUEUE', -- https://www.sqlskills.com/help/waits/CHECKPOINT_QUEUE
        N'CHKPT', -- https://www.sqlskills.com/help/waits/CHKPT
        N'CLR_AUTO_EVENT', -- https://www.sqlskills.com/help/waits/CLR_AUTO_EVENT
        N'CLR_MANUAL_EVENT', -- https://www.sqlskills.com/help/waits/CLR_MANUAL_EVENT
        N'CLR_SEMAPHORE', -- https://www.sqlskills.com/help/waits/CLR_SEMAPHORE
        N'CXCONSUMER', -- https://www.sqlskills.com/help/waits/CXCONSUMER
 
        -- Maybe comment these four out if you have mirroring issues
        N'DBMIRROR_DBM_EVENT', -- https://www.sqlskills.com/help/waits/DBMIRROR_DBM_EVENT
        N'DBMIRROR_EVENTS_QUEUE', -- https://www.sqlskills.com/help/waits/DBMIRROR_EVENTS_QUEUE
        N'DBMIRROR_WORKER_QUEUE', -- https://www.sqlskills.com/help/waits/DBMIRROR_WORKER_QUEUE
        N'DBMIRRORING_CMD', -- https://www.sqlskills.com/help/waits/DBMIRRORING_CMD
 
        N'DIRTY_PAGE_POLL', -- https://www.sqlskills.com/help/waits/DIRTY_PAGE_POLL
        N'DISPATCHER_QUEUE_SEMAPHORE', -- https://www.sqlskills.com/help/waits/DISPATCHER_QUEUE_SEMAPHORE
        N'EXECSYNC', -- https://www.sqlskills.com/help/waits/EXECSYNC
        N'FSAGENT', -- https://www.sqlskills.com/help/waits/FSAGENT
        N'FT_IFTS_SCHEDULER_IDLE_WAIT', -- https://www.sqlskills.com/help/waits/FT_IFTS_SCHEDULER_IDLE_WAIT
        N'FT_IFTSHC_MUTEX', -- https://www.sqlskills.com/help/waits/FT_IFTSHC_MUTEX
 
        -- Maybe comment these six out if you have AG issues
        N'HADR_CLUSAPI_CALL', -- https://www.sqlskills.com/help/waits/HADR_CLUSAPI_CALL
        N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', -- https://www.sqlskills.com/help/waits/HADR_FILESTREAM_IOMGR_IOCOMPLETION
        N'HADR_LOGCAPTURE_WAIT', -- https://www.sqlskills.com/help/waits/HADR_LOGCAPTURE_WAIT
        N'HADR_NOTIFICATION_DEQUEUE', -- https://www.sqlskills.com/help/waits/HADR_NOTIFICATION_DEQUEUE
        N'HADR_TIMER_TASK', -- https://www.sqlskills.com/help/waits/HADR_TIMER_TASK
        N'HADR_WORK_QUEUE', -- https://www.sqlskills.com/help/waits/HADR_WORK_QUEUE
 
        N'KSOURCE_WAKEUP', -- https://www.sqlskills.com/help/waits/KSOURCE_WAKEUP
        N'LAZYWRITER_SLEEP', -- https://www.sqlskills.com/help/waits/LAZYWRITER_SLEEP
        N'LOGMGR_QUEUE', -- https://www.sqlskills.com/help/waits/LOGMGR_QUEUE
        N'MEMORY_ALLOCATION_EXT', -- https://www.sqlskills.com/help/waits/MEMORY_ALLOCATION_EXT
        N'ONDEMAND_TASK_QUEUE', -- https://www.sqlskills.com/help/waits/ONDEMAND_TASK_QUEUE
        N'PARALLEL_REDO_DRAIN_WORKER', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_DRAIN_WORKER
        N'PARALLEL_REDO_LOG_CACHE', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_LOG_CACHE
        N'PARALLEL_REDO_TRAN_LIST', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_TRAN_LIST
        N'PARALLEL_REDO_WORKER_SYNC', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_SYNC
        N'PARALLEL_REDO_WORKER_WAIT_WORK', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_WAIT_WORK
        N'PREEMPTIVE_XE_GETTARGETSTATE', -- https://www.sqlskills.com/help/waits/PREEMPTIVE_XE_GETTARGETSTATE
        N'PWAIT_ALL_COMPONENTS_INITIALIZED', -- https://www.sqlskills.com/help/waits/PWAIT_ALL_COMPONENTS_INITIALIZED
        N'PWAIT_DIRECTLOGCONSUMER_GETNEXT', -- https://www.sqlskills.com/help/waits/PWAIT_DIRECTLOGCONSUMER_GETNEXT
        N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', -- https://www.sqlskills.com/help/waits/QDS_PERSIST_TASK_MAIN_LOOP_SLEEP
        N'QDS_ASYNC_QUEUE', -- https://www.sqlskills.com/help/waits/QDS_ASYNC_QUEUE
        N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
            -- https://www.sqlskills.com/help/waits/QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP
        N'QDS_SHUTDOWN_QUEUE', -- https://www.sqlskills.com/help/waits/QDS_SHUTDOWN_QUEUE
        N'REDO_THREAD_PENDING_WORK', -- https://www.sqlskills.com/help/waits/REDO_THREAD_PENDING_WORK
        N'REQUEST_FOR_DEADLOCK_SEARCH', -- https://www.sqlskills.com/help/waits/REQUEST_FOR_DEADLOCK_SEARCH
        N'RESOURCE_QUEUE', -- https://www.sqlskills.com/help/waits/RESOURCE_QUEUE
        N'SERVER_IDLE_CHECK', -- https://www.sqlskills.com/help/waits/SERVER_IDLE_CHECK
        N'SLEEP_BPOOL_FLUSH', -- https://www.sqlskills.com/help/waits/SLEEP_BPOOL_FLUSH
        N'SLEEP_DBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_DBSTARTUP
        N'SLEEP_DCOMSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_DCOMSTARTUP
        N'SLEEP_MASTERDBREADY', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERDBREADY
        N'SLEEP_MASTERMDREADY', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERMDREADY
        N'SLEEP_MASTERUPGRADED', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERUPGRADED
        N'SLEEP_MSDBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_MSDBSTARTUP
        N'SLEEP_SYSTEMTASK', -- https://www.sqlskills.com/help/waits/SLEEP_SYSTEMTASK
        N'SLEEP_TASK', -- https://www.sqlskills.com/help/waits/SLEEP_TASK
        N'SLEEP_TEMPDBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_TEMPDBSTARTUP
        N'SNI_HTTP_ACCEPT', -- https://www.sqlskills.com/help/waits/SNI_HTTP_ACCEPT
        N'SP_SERVER_DIAGNOSTICS_SLEEP', -- https://www.sqlskills.com/help/waits/SP_SERVER_DIAGNOSTICS_SLEEP
        N'SQLTRACE_BUFFER_FLUSH', -- https://www.sqlskills.com/help/waits/SQLTRACE_BUFFER_FLUSH
        N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', -- https://www.sqlskills.com/help/waits/SQLTRACE_INCREMENTAL_FLUSH_SLEEP
        N'SQLTRACE_WAIT_ENTRIES', -- https://www.sqlskills.com/help/waits/SQLTRACE_WAIT_ENTRIES
        N'WAIT_FOR_RESULTS', -- https://www.sqlskills.com/help/waits/WAIT_FOR_RESULTS
        N'WAITFOR', -- https://www.sqlskills.com/help/waits/WAITFOR
        N'WAITFOR_TASKSHUTDOWN', -- https://www.sqlskills.com/help/waits/WAITFOR_TASKSHUTDOWN
        N'WAIT_XTP_RECOVERY', -- https://www.sqlskills.com/help/waits/WAIT_XTP_RECOVERY
        N'WAIT_XTP_HOST_WAIT', -- https://www.sqlskills.com/help/waits/WAIT_XTP_HOST_WAIT
        N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', -- https://www.sqlskills.com/help/waits/WAIT_XTP_OFFLINE_CKPT_NEW_LOG
        N'WAIT_XTP_CKPT_CLOSE', -- https://www.sqlskills.com/help/waits/WAIT_XTP_CKPT_CLOSE
        N'XE_DISPATCHER_JOIN', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_JOIN
        N'XE_DISPATCHER_WAIT', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_WAIT
        N'XE_TIMER_EVENT' -- https://www.sqlskills.com/help/waits/XE_TIMER_EVENT
		)
		AND [waiting_tasks_count] > 0

		INSERT INTO monitor.wait_stats(collect_date, wait_type, waiting_tasks_count, wait_time_ms, max_wait_time_ms, signal_wait_time_ms)
		SELECT w.collect_date
			, w.wait_type
			, w.waiting_tasks_count
			, CASE WHEN w.wait_time_ms > lw.wait_time_ms THEN  w.wait_time_ms - lw.wait_time_ms ELSE 0 END
			, w.max_wait_time_ms
			, CASE WHEN w.signal_wait_time_ms > lw.signal_wait_time_ms THEN  w.signal_wait_time_ms - lw.signal_wait_time_ms ELSE 0 END
		FROM @waits w
			INNER JOIN monitor.last_wait_stats lw ON lw.wait_type = w.wait_type
		-- insert into last_wait_stats
		TRUNCATE TABLE monitor.last_wait_stats;
		INSERT INTO monitor.last_wait_stats (collect_date, wait_type, waiting_tasks_count, wait_time_ms, max_wait_time_ms, signal_wait_time_ms)
		SELECT collect_date, wait_type, waiting_tasks_count, wait_time_ms, max_wait_time_ms, signal_wait_time_ms
		FROM @waits
END
PRINT N'Procedure monitor.usp_save_wait_stats created'
GO

-- VIEWS
GO
IF EXISTS(SELECT 1 FROM sys.views WHERE object_id = OBJECT_ID('monitor.readable_wait_stats'))
BEGIN
	PRINT N'Dropping view monitor.readable_wait_stats'
	DROP VIEW monitor.readable_wait_stats;
END
PRINT N'Create view monitor.readable_wait_stats'
GO
CREATE VIEW monitor.readable_wait_stats
AS
SELECT 
	W.collect_date
	, W.wait_type
	, W.waiting_tasks_count
	, W.wait_time_ms
	, W.max_wait_time_ms
	, W.signal_wait_time_ms
FROM (
	SELECT
		collect_date
		, wait_type
		, waiting_tasks_count
		, wait_time_ms
		, max_wait_time_ms
		, signal_wait_time_ms
		, ROW_NUMBER() OVER(PARTITION BY collect_date ORDER BY wait_time_ms DESC) as RN
	FROM
		monitor.wait_stats
	) W
WHERE W.RN <= 10;
GO


-- WAIT CATEGORIES
IF EXISTS(SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('monitor.wait_categories'))
BEGIN
	PRINT N'Dropping table monitor.wait_categories'
	DROP TABLE monitor.wait_categories;
END
PRINT N'Create table monitor.wait_categories';
CREATE TABLE monitor.wait_categories
(
	wait_category VARCHAR(60) NOT NULL,
	wait_type VARCHAR(60)
)
PRINT N'Filling table monitor.wait_categories'
INSERT INTO monitor.wait_categories (wait_category, wait_type)
SELECT 'Unknown', 'Unknown'
UNION ALL
SELECT 'Cpu', 'SOS_SCHEDULER_YIELD'
UNION ALL
SELECT 'Worker Thread', 'THREADPOOL'
UNION ALL
SELECT 'Lock', wait_type FROM sys.dm_os_wait_stats WHERE wait_type LIKE 'LCK_M%'
UNION ALL
SELECT 'Latch', wait_type FROM sys.dm_os_wait_stats WHERE wait_type LIKE 'LATCH_%'
UNION ALL
SELECT 'Buffer Latch', wait_type FROM sys.dm_os_wait_stats WHERE wait_type LIKE 'PAGELATCH_%'
UNION ALL
SELECT 'Buffer IO', wait_type FROM sys.dm_os_wait_stats WHERE wait_type LIKE 'PAGEIOLATCH_%'
UNION ALL
SELECT 'Compilation', 'RESOURCE_SEMAPHORE_QUERY_COMPILE'
UNION ALL
SELECT 'CLR', wait_type FROM sys.dm_os_wait_stats WHERE wait_type LIKE 'CLR%' OR wait_type LIKE 'SQLCLR%'
UNION ALL 
SELECT 'Mirroring', wait_type FROM sys.dm_os_wait_stats WHERE wait_type LIKE 'DBMIRROR%'
UNION ALL 
SELECT 'Transaction',wait_type FROM sys.dm_os_wait_stats WHERE wait_type LIKE  'XACT%' OR wait_type LIKE 'DTC%' OR wait_type LIKE 'TRAN_MARKLATCH_%' OR wait_type LIKE 'MSQL_XACT_%' OR  wait_type LIKE 'TRANSACTION_MUTEX'
UNION ALL 
SELECT 'Idle', wait_type FROM sys.dm_os_wait_stats WHERE wait_type LIKE 'SLEEP_%' OR wait_type LIKE 'LAZYWRITER_SLEEP' OR wait_type IN ('SQLTRACE_BUFFER_FLUSH', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP','SQLTRACE_WAIT_ENTRIES', 'FT_IFTS_SCHEDULER_IDLE_WAIT', 'XE_DISPATCHER_WAIT', 'REQUEST_FOR_DEADLOCK_SEARCH', 'LOGMGR_QUEUE', 'ONDEMAND_TASK_QUEUE', 'CHECKPOINT_QUEUE', 'XE_TIMER_EVENT')
UNION ALL 
SELECT 'Preemptive', wait_type FROM sys.dm_os_wait_stats WHERE wait_type LIKE 'PREEMPTIVE_%'
UNION ALL 
SELECT 'Service Broker', wait_type FROM sys.dm_os_wait_stats WHERE wait_type LIKE 'BROKER_%' AND wait_type <> 'BROKER_RECEIVE_WAITFOR'
UNION ALL 
SELECT 'Tran Log IO', wait_type FROM sys.dm_os_wait_stats WHERE wait_type IN ('LOGMGR', 'LOGBUFFER', 'LOGMGR_RESERVE_APPEND', 'LOGMGR_FLUSH', 'LOGMGR_PMM_LOG', 'CHKPT', 'WRITELOG')
UNION ALL 
SELECT 'Network IO', wait_type FROM sys.dm_os_wait_stats WHERE wait_type IN ('ASYNC_NETWORK_IO', 'NET_WAITFOR_PACKET', 'PROXY_NETWORK_IO', 'EXTERNAL_SCRIPT_NETWORK_IOF')
UNION ALL 
SELECT 'Parallelism', wait_type FROM sys.dm_os_wait_stats WHERE wait_type IN ('CXPACKET', 'EXCHANGE') OR wait_type LIKE 'HT%' OR wait_type LIKE 'BMP%' OR wait_type LIKE 'BP%'
UNION ALL 
SELECT 'Memory', wait_type FROM sys.dm_os_wait_stats WHERE wait_type IN ('RESOURCE_SEMAPHORE', 'CMEMTHREAD', 'CMEMPARTITIONED', 'EE_PMOLOCK', 'MEMORY_ALLOCATION_EXT', 'RESERVED_MEMORY_ALLOCATION_EXT', 'MEMORY_GRANT_UPDATE')
UNION ALL 
SELECT 'User Wait', wait_type FROM sys.dm_os_wait_stats WHERE wait_type IN ('WAITFOR', 'WAIT_FOR_RESULTS', 'BROKER_RECEIVE_WAITFOR')
UNION ALL 
SELECT 'Tracing',  wait_type FROM sys.dm_os_wait_stats WHERE wait_type IN ('TRACEWRITE', 'SQLTRACE_LOCK', 'SQLTRACE_FILE_BUFFER', 'SQLTRACE_FILE_WRITE_IO_COMPLETION', 'SQLTRACE_FILE_READ_IO_COMPLETION', 'SQLTRACE_PENDING_BUFFER_WRITERS', 'SQLTRACE_SHUTDOWN', 'QUERY_TRACEOUT', 'TRACE_EVTNOTIFF')
UNION ALL 
SELECT 'Full Text Search',  wait_type FROM sys.dm_os_wait_stats WHERE wait_type IN ('FT_RESTART_CRAWL', 'FULLTEXT GATHERER, MSSEARCH', 'FT_METADATA_MUTEX', 'FT_IFTSHC_MUTEX', 'FT_IFTSISM_MUTEX', 'FT_IFTS_RWLOCK', 'FT_COMPROWSET_RWLOCK', 'FT_MASTER_MERGE', 'FT_PROPERTYLIST_CACHE', 'FT_MASTER_MERGE_COORDINATOR', 'PWAIT_RESOURCE_SEMAPHORE_FT_PARALLEL_QUERY_SYNC')
UNION ALL 
SELECT 'Other Disk IO', wait_type FROM sys.dm_os_wait_stats WHERE wait_type IN ('ASYNC_IO_COMPLETION', 'IO_COMPLETION', 'BACKUPIO', 'WRITE_COMPLETION', 'IO_QUEUE_LIMIT', 'IO_RETRY')
UNION ALL 
SELECT 'Replication', wait_type FROM sys.dm_os_wait_stats WHERE wait_type LIKE 'SE_REPL_%' OR wait_type LIKE 'REPL_%' OR (wait_type LIKE 'HADR_%' AND wait_type <> 'HADR_THROTTLE_LOG_RATE_GOVERNOR') OR wait_type LIKE 'PWAIT_HADR_%' OR wait_type IN ('REPLICA_WRITES', 'FCB_REPLICA_WRITE', 'FCB_REPLICA_READ', 'PWAIT_HADRSIM')
UNION ALL 
SELECT 'Log Rate Governor', wait_type FROM sys.dm_os_wait_stats WHERE wait_type IN ('LOG_RATE_GOVERNOR', 'POOL_LOG_RATE_GOVERNOR', 'HADR_THROTTLE_LOG_RATE_GOVERNOR', 'INSTANCE_LOG_RATE_GOVERNOR');
 
 PRINT N'Initialization table monitor.wait_categories done';
