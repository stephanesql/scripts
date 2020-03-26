/*
/* 2020-03-13
Create a lightweight solution to record io virtual file stats
Create 2 tables, 1 procedure to record and 1 view to query.
Procedure execute the difference using one table filled with last values.
Call the procedure regularly (every 5 min) and record only when values are higher than thresholds in reads or writes.
See job script for job
*/
*/


use SYSDBA -- change to your database

IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE name = 'monitor')
BEGIN
	PRINT N'Creating schema monitor'
	EXEC ('CREATE SCHEMA monitor');
END
GO
IF EXISTS(SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('monitor.last_io_file_stats_for_alerting'))
BEGIN
	PRINT N'Dropping table monitor.last_io_file_stats_for_alerting'
	DROP TABLE monitor.last_io_file_stats_for_alerting;
END
PRINT N'Creating table monitor.last_io_file_stats_for_alerting'
CREATE TABLE monitor.last_io_file_stats_for_alerting
(
	database_id smallint not null,
	file_id smallint not null,
	database_name sysname not null,
	file_name sysname,
	type_desc varchar(4),
	drive char(1),
	num_of_reads bigint,
	num_of_bytes_read bigint,
	io_stall_read_ms bigint,
	num_of_writes bigint,
	num_of_bytes_written bigint,
	io_stall_write_ms bigint,
	io_stall_ms bigint,
	collect_date datetime not null
);
GO

IF EXISTS(SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('monitor.io_file_stats_for_alerting'))
BEGIN
	PRINT N'Dropping table monitor.io_file_stats_for_alerting'
	DROP TABLE monitor.io_file_stats_for_alerting;
END
PRINT N'Creating table monitor.io_file_stats_for_alerting'
CREATE TABLE monitor.io_file_stats_for_alerting
(
	database_id smallint not null,
	file_id smallint not null,
	database_name sysname not null,
	file_name sysname,
	type_desc varchar(4),
	drive char(1),
	num_of_reads bigint,
	num_of_bytes_read bigint,
	io_stall_read_ms bigint,
	num_of_writes bigint,
	num_of_bytes_written bigint,
	io_stall_write_ms bigint,
	io_stall_ms bigint,
	collect_date datetime not null
);
PRINT N'Create clustered index on monitor.io_file_stats_for_alerting'
CREATE CLUSTERED INDEX idx_io_file_stats_for_alerting_collect_date ON monitor.io_file_stats_for_alerting (collect_date);

GO
IF EXISTS(SELECT 1 FROM sys.procedures WHERE object_id = OBJECT_ID('monitor.usp_alert_io_file_stats'))
BEGIN
	PRINT N'Dropping procedure monitor.usp_alert_io_file_stats'
	DROP PROCEDURE monitor.usp_alert_io_file_stats;
END
PRINT N'Creating procedure monitor.usp_alert_io_file_stats'
GO
CREATE PROCEDURE monitor.usp_alert_io_file_stats(@read_threshold_ms int = 25, @write_threshold_ms int=50
	, @read_threshold int = 100, @write_threshold int=200)
AS
BEGIN
	DECLARE @dt DATETIME = GETDATE();
	DECLARE @ios TABLE (
		database_id smallint not null,
		file_id smallint not null,
		database_name sysname not null,
		file_name sysname,
		type_desc varchar(4),
		drive char(1),
		num_of_reads bigint,
		num_of_bytes_read bigint,
		io_stall_read_ms bigint,
		num_of_writes bigint,
		num_of_bytes_written bigint,
		io_stall_write_ms bigint,
		io_stall_ms bigint,
		collect_date datetime not null	
	);

	INSERT INTO @ios (
		database_id,
		file_id,
		database_name,
		file_name,
		type_desc,
		drive,
		num_of_reads,
		num_of_bytes_read,
		io_stall_read_ms,
		num_of_writes,
		num_of_bytes_written,
		io_stall_write_ms,
		io_stall_ms,
		collect_date
	)
	SELECT vf.database_id,
	vf.file_id,
	db_name(vf.database_id)
	, mf.name
	, mf.type_desc
	, UPPER(left(mf.physical_name,1)) as drive,
	num_of_reads,
	num_of_bytes_read,
	io_stall_read_ms,
	num_of_writes,
	num_of_bytes_written,
	io_stall_write_ms,
	io_stall,
	@dt
	FROM sys.dm_io_virtual_file_stats(NULL,NULL) vf 
		inner join sys.master_files mf on mf.database_id = vf.database_id and mf.file_id = vf.file_id

	INSERT INTO monitor.io_file_stats_for_alerting (
	database_id,
	file_id,
	database_name,
	file_name,
	type_desc,
	drive,
	num_of_reads,
	num_of_bytes_read,
	io_stall_read_ms,
	num_of_writes,
	num_of_bytes_written,
	io_stall_write_ms,
	io_stall_ms,
	collect_date
	)
	SELECT
		i.database_id,
		i.file_id,
		i.database_name,
		i.file_name,
		i.type_desc,
		i.drive,
		CASE WHEN l.num_of_reads > i.num_of_reads THEN 0 ELSE i.num_of_reads - l.num_of_reads END,
		CASE WHEN l.num_of_bytes_read > i.num_of_bytes_read THEN 0 ELSE  i.num_of_bytes_read - l.num_of_bytes_read END,
		CASE WHEN l.io_stall_read_ms > i.io_stall_read_ms THEN  0 ELSE i.io_stall_read_ms - l.io_stall_read_ms END,
		CASE WHEN l.num_of_writes > i.num_of_writes THEN 0 ELSE i.num_of_writes - l.num_of_writes END,
		CASE WHEN l.num_of_bytes_written > i.num_of_bytes_written THEN 0 ELSE i.num_of_bytes_written - l.num_of_bytes_written END,
		CASE WHEN l.io_stall_write_ms > i.io_stall_write_ms THEN 0 ELSE i.io_stall_write_ms - l.io_stall_write_ms END,
		CASE WHEN l.io_stall_ms > i.io_stall_ms THEN 0 ELSE i.io_stall_ms - l.io_stall_ms END,
		@dt
	FROM @ios i
		INNER JOIN monitor.last_io_file_stats_for_alerting l on l.database_id = i.database_id AND l.file_id = i.file_id
	WHERE  i.io_stall_read_ms - l.io_stall_read_ms >= @read_threshold_ms
		OR i.num_of_reads - l.num_of_reads >= @read_threshold
		OR i.io_stall_write_ms - l.io_stall_write_ms >= @write_threshold_ms
		OR i.num_of_writes - l.num_of_writes >= @write_threshold
	
	TRUNCATE TABLE monitor.last_io_file_stats_for_alerting;
	INSERT INTO monitor.last_io_file_stats_for_alerting (
		database_id,
		file_id,
		database_name,
		file_name,
		type_desc,
		drive,
		num_of_reads,
		num_of_bytes_read,
		io_stall_read_ms,
		num_of_writes,
		num_of_bytes_written,
		io_stall_write_ms,
		io_stall_ms,
		collect_date
	)
	SELECT database_id,
	file_id,
	database_name,
	file_name,
	type_desc,
	drive,
	num_of_reads,
	num_of_bytes_read,
	io_stall_read_ms,
	num_of_writes,
	num_of_bytes_written,
	io_stall_write_ms,
	io_stall_ms,
	collect_date
	FROM @ios;
END
PRINT N'Procedure monitor.usp_alert_io_file_stats created.'
GO
