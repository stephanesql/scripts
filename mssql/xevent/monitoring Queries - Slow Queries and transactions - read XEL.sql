use [tempdb]
go
	
select xs.name, xs.total_buffer_size/1024/1024 as total_bufer_size_mb, xs.buffer_policy_desc
	, xt.target_name
	, convert(xml,xt.target_data) as target_data
	, xs.flag_desc, xs.dropped_event_count, xs.dropped_buffer_count
	, xs.session_source
from sys.dm_xe_sessions xs
	inner join sys.dm_xe_session_targets xt on xt.event_session_address = xs.address
where name = 'queries'
go

SELECT
	event_data_XML.value('(/event/@timestamp)[1]', 'datetime') as [timestamp]
	, event_data_XML.value('(/event/@name)[1]', 'nvarchar(100)') as event_name
	, event_data_XML.value('(/event/action[@name="sql_text"]/value)[1]', 'nvarchar(max)') as sql_text
	, event_data_XML.value('(/event/data[@name="statement"]/value)[1]', 'nvarchar(max)') as [statement]
	, event_data_XML.value('(/event/data[@name="cpu_time"]/value)[1]', 'int') as cpu_time
	, event_data_XML.value('(/event/data[@name="duration"]/value)[1]', 'int') as duration
	, event_data_XML.value('(event/data[@name="logical_reads"]/value)[1]', 'int') as logical_reads
	, event_data_XML.value('(/event/data[@name="physical_reads"]/value)[1]', 'int') as physical_reads
	, event_data_XML.value('(/event/data[@name="writes"]/value)[1]', 'int') as writes
	, event_data_XML.value('(/event/data[@name="row_count"]/value)[1]', 'int') as row_count
	, event_data_XML.value('(/event/data[@name="object_name"]/value)[1]', 'nvarchar(255)') as [object_name]
	, event_data_XML.value('(/event/action[@name="username"]/value)[1]', 'nvarchar(255)') as username
	, event_data_XML.value('(/event/action[@name="database_name"]/value)[1]', 'nvarchar(255)') as database_name
	, event_data_XML.value('(/event/action[@name="client_app_name"]/value)[1]', 'nvarchar(255)') as client_app_name
	, event_data_XML.value('(/event/action[@name="client_hostname"]/value)[1]', 'nvarchar(255)') as client_hostname
	, event_data_XML.value('(/event/action[@name="query_hash"]/value)[1]', 'binary(8)') as query_hash
	, event_data_XML.value('(/event/action[@name="plan_handle"]/value)[1]', 'nvarchar(4000)') as plan_handle
	, event_data_XML.value('(/event/action[@name="session_id"]/value)[1]', 'int') as session_id
	, event_data_XML.value('(/event/action[@name="transaction_id"]/value)[1]', 'int') as transaction_id
	, event_data_XML.value('(/event/action[@name="event_sequence"]/value)[1]', 'bigint') as event_sequence
	-- , event_data_XML
FROM (
	select CONVERT(xml,event_data) as event_data_XML
	from sys.fn_xe_file_target_read_file('C:\temp\queries_20200317-09h33*.xel', null,null,null)
) xel
--ORDER BY event_data_XML.value('(/event/action[@name="event_sequence"]/value)[1]', 'bigint')
WHERE event_data_XML.value('(/event/action[@name="client_app_name"]/value)[1]', 'nvarchar(255)') <> 'Microsoft SQL Server Management Studio - Transact-SQL IntelliSense'
ORDER BY event_data_XML.value('(/event/@timestamp)[1]', 'datetime')

/*
SELECT TOP 1000 * FROM #TEMP ORDER BY event_sequence DESC

DROP TABLE #TEMP
*/