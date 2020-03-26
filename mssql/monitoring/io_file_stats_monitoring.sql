/*
2020-03-13
Create a lightweight solution to record io virtual file stats
Create 2 tables, 1 procedure to record and 1 view to query.
Procedure execute the difference using one table filled with last values.
Call the procedure regularly (every 30 min) and add a data purge.
See job script
*/


use SYSDBA -- change to your database

IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE name = 'monitor')
BEGIN
	PRINT N'Creating schema monitor'
	EXEC ('CREATE SCHEMA monitor');
END
GO
IF EXISTS(SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('monitor.last_io_file_stats'))
BEGIN
	PRINT N'Dropping table monitor.last_io_file_stats'
	DROP TABLE monitor.last_io_file_stats;
END
PRINT N'Creating table monitor.last_io_file_stats'
CREATE TABLE monitor.last_io_file_stats
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
PRINT N'Create clustered index on monitor.last_io_file_stats'
CREATE CLUSTERED INDEX idx_io_file_stats_collect_date ON monitor.last_io_file_stats (database_id, file_id);

IF EXISTS(SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('monitor.io_file_stats'))
BEGIN
	PRINT N'Dropping table monitor.io_file_stats'
	DROP TABLE monitor.io_file_stats;
END
PRINT N'Creating table monitor.io_file_stats'
CREATE TABLE monitor.io_file_stats
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
)
PRINT N'Create clustered index on monitor.io_file_stats'
CREATE CLUSTERED INDEX idx_io_file_stats_collect_date ON monitor.io_file_stats (collect_date);

GO
IF EXISTS(SELECT 1 FROM sys.procedures WHERE object_id = OBJECT_ID('monitor.usp_save_io_file_stats'))
BEGIN
	DROP PROCEDURE monitor.usp_save_io_file_stats;
END
GO
PRINT N'Create procedure monitor.usp_save_io_file_stats'
GO
CREATE PROCEDURE monitor.usp_save_io_file_stats
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
	

	INSERT INTO monitor.io_file_stats (
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
		INNER JOIN monitor.last_io_file_stats l on l.database_id = i.database_id AND l.file_id = i.file_id
	WHERE i.num_of_reads + i.num_of_writes > 0
	
	TRUNCATE TABLE monitor.last_io_file_stats;
	INSERT INTO monitor.last_io_file_stats (
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
GO
-- readable view
IF EXISTS(SELECT 1 FROM sys.views WHERE object_id = OBJECT_ID('monitor.readable_io_file_stats'))
BEGIN
	PRINT N'Dropping existing view monitor.readable.io_file_stats'
	DROP VIEW monitor.readable_io_file_stats;
END
PRINT N'Creating view monitor.readable_io_file_stats'
GO
CREATE VIEW monitor.readable_io_file_stats
AS
SELECT collect_date, ifs.database_name, ifs.file_name, ifs.type_desc, ifs.drive
	, ifs.num_of_reads
	, ifs.num_of_bytes_read
	, ifs.io_stall_read_ms
	, CASE WHEN ifs.num_of_reads > 0 THEN ifs.num_of_bytes_read / ifs.num_of_reads ELSE 0 END AS avg_bytes_by_read
	, CASE WHEN ifs.num_of_reads > 0 THEN ifs.io_stall_read_ms/ifs.num_of_reads ELSE 0 END AS avg_read_latency_ms
	, CONVERT(numeric(18,2),CASE WHEN ifs.io_stall_read_ms > 0 THEN ifs.num_of_bytes_read / (1024 * 1024) / (ifs.io_stall_read_ms / 1000.0) ELSE 0 END) AS [Read_MB_sec]
	, ifs.num_of_writes
	, ifs.num_of_bytes_written
	, ifs.io_stall_write_ms
	, CASE WHEN ifs.num_of_writes > 0 THEN ifs.num_of_bytes_written / ifs.num_of_writes ELSE 0 END AS avg_bytes_by_write
	, CASE WHEN ifs.num_of_writes > 0 THEN ifs.io_stall_write_ms/ifs.num_of_writes ELSE 0 END AS avg_write_latency_ms
	, CONVERT(numeric(18,2),CASE WHEN ifs.io_stall_write_ms > 0 THEN ifs.num_of_bytes_written / (1024 * 1024) / (ifs.io_stall_write_ms / 1000.0) ELSE 0 END) AS [Write_MB_sec]
	, ifs.io_stall_ms
	, CASE WHEN ifs.num_of_reads + ifs.num_of_writes > 0 THEN (ifs.num_of_bytes_read + ifs.num_of_bytes_written) / (ifs.num_of_reads + ifs.num_of_writes ) ELSE 0 END AS avg_bytes_by_io
	, CASE WHEN ifs.num_of_reads + ifs.num_of_writes > 0 THEN ifs.io_stall_ms / (ifs.num_of_reads + ifs.num_of_writes ) ELSE 0 END AS avg_latency_ms
	, CONVERT(numeric(18,2),CASE WHEN ifs.io_stall_ms > 0 THEN (ifs.num_of_bytes_read + ifs.num_of_bytes_written) / (1024 * 1024) / (ifs.io_stall_ms / 1000.0) ELSE 0 END) AS [IO_MB_sec]
FROM	
	monitor.io_file_stats ifs


