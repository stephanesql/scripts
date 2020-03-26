/****************************************
-- Get last growth in log and data files
****************************************/
DECLARE @curr_tracefilename sysname, @indx int, @base_tracefilename sysname
select @curr_tracefilename = path from sys.traces where is_default = 1
set @curr_tracefilename = reverse(@curr_tracefilename);
select @indx  = patindex('%\%', @curr_tracefilename)
set @curr_tracefilename = reverse(@curr_tracefilename) ;
set @base_tracefilename = left( @curr_tracefilename,len(@curr_tracefilename) - @indx) + '\log.trc' ;
select  (dense_rank() over (order by StartTime desc))%2 as l1
                ,       convert(int, EventClass) as EventClass
                ,       DatabaseName
                ,       Filename
                ,       (Duration/1000) as Duration
                ,       StartTime
                ,       EndTime
                ,       (IntegerData*8.0/1024) as ChangeInSize
                from ::fn_trace_gettable( @base_tracefilename, default )
                left outer join sys.databases as d on (d.name = DB_NAME())
                where EventClass >=  92      and EventClass <=  95        and ServerName = @@servername   and DatabaseName = db_name()  and (d.create_date < EndTime)
                order by StartTime desc
GO			

/**********************************
Found the biggest tables (more than 100 GB) in all databases
***********************************/
declare @tmp table (db sysname, tablename sysname, nb_rows bigint, nb_partition int, size_mb int, data_size_mb int, non_data_size_mb int)
insert into @tmp
exec sp_MSforeachdb N'
use [?]
select @@Servername as server, DB_NAME() as db, t.name, MAX(p.rows) as nb_rows, count(*) as nb_partition, sum(au.total_pages) /128 as size_mb
	, sum(au.used_pages) / 128 as data_size_mb
	, sum(au.total_pages - au.used_pages) /128 as non_data_size_mb
from sys.tables t inner join sys.partitions p on t.object_id = p.object_id
	inner join sys.allocation_units au on au.container_id = p.hobt_id 
group by t.name
having sum(au.total_pages)/128 > 100000
order by size_mb desc';
select * from @tmp order by size_mb desc

go


/****************************************************
Columns types of a table
*****************************************************/
select object_name(c.object_id) as objectname, c.name as column_name, c.column_id, t.name as [type_name], c.max_length, c.precision, c.scale, c.collation_name, c.is_nullable, c.is_identity from sys.columns c 
	inner join sys.types t on t.system_type_id = c.system_type_id and c.user_type_id = t.user_type_id
where c.object_id = OBJECT_ID('dbo.server_traces')
order by c.column_id
go

/*******************************************************
Index and ordinal columns
https://littlekendra.com/2016/03/15/find-the-partitioning-key-on-an-existing-table-with-partition_ordinal/
Find the Partitioning Key on an Existing Table with Partition_Ordinal
********************************************************/
;with partitionedtables AS (
	SELECT DISTINCT 
		t.object_id,
		t.name AS table_name
	FROM sys.tables AS t
	JOIN sys.indexes AS si on t.object_id=si.object_id 
	JOIN sys.partition_schemes AS sc on si.data_space_id=sc.data_space_id
)
SELECT 
	pt.table_name,
	si.index_id,
	si.name AS index_name,
	ISNULL(pf.name, 'NonAligned') AS partition_function,
	ISNULL(sc.name, fg.name) AS partition_scheme_or_filegroup,
	ic.partition_ordinal, /* 0= not a partitioning column*/
	ic.key_ordinal,
	ic.is_included_column,
	c.name AS column_name,
	t.name AS data_type_name,
	c.is_identity,
	ic.is_descending_key,
	si.filter_definition
FROM partitionedtables AS pt
JOIN sys.indexes AS si on pt.object_id=si.object_id
JOIN sys.index_columns AS ic on si.object_id=ic.object_id
	and si.index_id=ic.index_id
JOIN sys.columns AS c on ic.object_id=c.object_id
	and ic.column_id=c.column_id
JOIN sys.types AS t on c.system_type_id=t.system_type_id
LEFT JOIN sys.partition_schemes AS sc on si.data_space_id=sc.data_space_id
LEFT JOIN sys.partition_functions AS pf on sc.function_id=pf.function_id
LEFT JOIN sys.filegroups as fg on si.data_space_id=fg.data_space_id
ORDER BY 1,2,3,4,5,6 DESC,7,8
GO

/********************************************************
Table size, reserved, data and index size mb
*********************************************************/
with pages as (
    SELECT object_id, SUM (reserved_page_count) as reserved_pages, SUM (used_page_count) as used_pages,
            SUM (case 
                    when (index_id < 2) then (in_row_data_page_count + lob_used_page_count + row_overflow_used_page_count)
                    else lob_used_page_count + row_overflow_used_page_count
                 end) as pages, sum(case when index_id < 2 then row_count else 0 end) as row_count
    FROM sys.dm_db_partition_stats
    group by object_id
), extra as (
    SELECT p.object_id, sum(reserved_page_count) as reserved_pages, sum(used_page_count) as used_pages
    FROM sys.dm_db_partition_stats p, sys.internal_tables it
    WHERE it.internal_type IN (202,204,211,212,213,214,215,216) AND p.object_id = it.object_id
    group by p.object_id
)
SELECT object_schema_name(p.object_id) + '.' + object_name(p.object_id) as TableName, row_count, (p.reserved_pages + isnull(e.reserved_pages, 0)) / 128 as reserved_mb,
        pages /128 as data_mb,
        (CASE WHEN p.used_pages + isnull(e.used_pages, 0) > pages THEN (p.used_pages + isnull(e.used_pages, 0) - pages) ELSE 0 END) /128 as index_mb,
        (CASE WHEN p.reserved_pages + isnull(e.reserved_pages, 0) > p.used_pages + isnull(e.used_pages, 0) THEN (p.reserved_pages + isnull(e.reserved_pages, 0) - p.used_pages + isnull(e.used_pages, 0)) else 0 end) /128 as unused_mb
from pages p
left outer join extra e on p.object_id = e.object_id
where p.object_id = OBJECT_ID('dbo.table1')
or p.object_id = OBJECT_ID('dbo.table2')


/********************************************
Get duplicate indexes
For covering indexes, change where clause with a LIKE
Caution, the columns list also included columns.
***********************************************************/
; WITH MYINDEXES AS (
select t.object_id, t.name as tablename, i.name as indexname, i.index_id, (
		select stuff((
		select ', ' + CAST(c1.name as varchar(128))
		from  sys.index_columns ic1
			inner join sys.columns c1 on c1.object_id = ic1.object_id and c1.column_id = ic1.column_id
		where ic1.index_id = i.index_id and ic1.object_id = i.object_id
		ORDER BY ic1.index_column_id
		FOR XML PATH('')
		), 1,2, '')
	) as colonnes
	, i.is_unique, i.is_primary_key, i.is_unique_constraint, i.type_desc
	, indsize.total_pages / 128 as index_size_mb
from sys.indexes i inner join sys.tables t on t.object_id = i.object_id
	inner join (
			select p.object_id, p.index_id, sum(au.total_pages) as total_pages from sys.partitions p inner join sys.allocation_units au on au.container_id = p.hobt_id group by p.object_id, p.index_id
	) indsize on indsize.object_id = i.object_id and indsize.index_id = i.index_id 
where t.is_ms_shipped = 0
--order by t.name, i.index_id
)
select M1.tablename, M1.indexname as index1_name, M1.index_id as index1_id, M1.is_unique, M1.is_primary_key, M1.is_unique_constraint, M1.index_size_mb, M1.type_desc, M1.colonnes as index1_colonnes
	, M2.indexname as index2_name, M2.index_id as index2_id, M2.colonnes as index2_colonnes, M2.is_unique, M2.is_primary_key, M2.is_unique_constraint, M2.index_size_mb, M2.type_desc
from MYINDEXES M1 
	INNER JOIN MYINDEXES M2 ON M1.object_id = M2.object_id AND M1.index_id < M2.index_id
WHERE
	M1.colonnes = M2.colonnes
ORDER BY M1.tablename, M1.index_id
option(recompile)

GO
-- with included columns
; WITH MYINDEXES AS (
select t.object_id, t.name as tablename, i.name as indexname, i.index_id, (
		select stuff((
		select ', ' + CAST(c1.name as varchar(128))
		from  sys.index_columns ic1
			inner join sys.columns c1 on c1.object_id = ic1.object_id and c1.column_id = ic1.column_id
		where ic1.index_id = i.index_id and ic1.object_id = i.object_id and ic1.is_included_column = 0
		ORDER BY ic1.index_column_id
		FOR XML PATH('')
		), 1,2, '')
	) as colonnes
	, i.is_unique, i.is_primary_key, i.is_unique_constraint, i.type_desc
	, indsize.total_pages / 128 as index_size_mb	
from sys.indexes i inner join sys.tables t on t.object_id = i.object_id
	inner join (
			select p.object_id, p.index_id, sum(au.total_pages) as total_pages from sys.partitions p inner join sys.allocation_units au on au.container_id = p.hobt_id group by p.object_id, p.index_id
	) indsize on indsize.object_id = i.object_id and indsize.index_id = i.index_id 
where t.is_ms_shipped = 0
)
, MYINCLUDES AS (
select t.object_id,  i.index_id, (
		select stuff((
		select ', ' + CAST(c1.name as varchar(128))
		from  sys.index_columns ic1
			inner join sys.columns c1 on c1.object_id = ic1.object_id and c1.column_id = ic1.column_id
		where ic1.index_id = i.index_id and ic1.object_id = i.object_id and ic1.is_included_column = 1
		ORDER BY ic1.index_column_id
		FOR XML PATH('')
		), 1,2, '')
	) as included_colonnes	
from sys.indexes i inner join sys.tables t on t.object_id = i.object_id	
where t.is_ms_shipped = 0
--order by t.name, i.index_id
)
select M1.tablename, M1.indexname as index1_name, M1.index_id as index1_id, M1.is_unique, M1.is_primary_key, M1.is_unique_constraint, M1.index_size_mb, M1.type_desc, M1.colonnes as index1_colonnes, MU1.included_colonnes
	, M2.indexname as index2_name, M2.index_id as index2_id, M2.colonnes as index2_colonnes, MU2.included_colonnes as index2_included, M2.is_unique, M2.is_primary_key, M2.is_unique_constraint, M2.index_size_mb, M2.type_desc
	, M2.index_size_mb
	, CONCAT('DROP INDEX ', QUOTENAME(M2.indexname), ' ON dbo.', M1.tablename, ';
	PRINT N''Index ', M2.indexname, ' dropped''') as CMD_DROP 
from MYINDEXES M1 
	INNER JOIN MYINDEXES M2 ON M1.object_id = M2.object_id AND M1.index_id < M2.index_id
	INNER JOIN MYINCLUDES MU1 ON MU1.object_id = M1.object_id AND MU1.index_id = M1.index_id
	INNER JOIN MYINCLUDES MU2 ON MU2.object_id = M2.object_id AND MU2.index_id = M2.index_id
WHERE
	M1.colonnes = M2.colonnes
ORDER BY M1.tablename, M1.index_id
option(recompile)
GO


-- detail of one table
SELECT
	OBJECT_NAME(i.object_id) as objectname, i.index_id,i.type_desc, i.name as indexname, c.name as columnname, ic.index_column_id, i.is_primary_key, i.is_unique, i.is_unique_constraint, ic.key_ordinal, ic.is_included_column
	, taille.size_mb
FROM
	sys.indexes i
	INNER JOIN sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id
	INNER JOIN (select index_id, object_id , sum(total_pages)/128 as size_mb from sys.partitions p 
					inner join sys.allocation_units au on au.container_id = p.hobt_id 
					group by p.object_id, p.index_id
				) as taille ON taille.object_id = i.object_id and taille.index_id = i.index_id
	INNER JOIN sys.columns c on c.object_id = i.object_id AND c.column_id = ic.column_id	
WHERE i.object_id = OBJECT_ID('dbo.SUJET_PLANIF')
ORDER BY i.index_id, ic.is_included_column, ic.key_ordinal
option(recompile)
GO

-- get indexes NC but declared as PK
SELECT
	t.name as objectname, i.index_id,i.type_desc, i.name as indexname, 'primary key but not clustered' as txt,i.is_primary_key, i.is_unique, i.is_unique_constraint
	, taille.size_mb
FROM sys.indexes i inner join sys.tables t on t.object_id = i.object_id
	INNER JOIN (select index_id, object_id , sum(total_pages)/128 as size_mb from sys.partitions p inner join sys.allocation_units au on au.container_id = p.hobt_id group by p.object_id, p.index_id) as taille ON taille.object_id = i.object_id and taille.index_id = i.index_id
WHERE is_primary_key = 1 and i.index_id <> 1 
	AND t.is_ms_shipped = 0
order by t.name, i.name
option(recompile)
GO

-- non used indexes
SELECT
	SCHEMA_NAME(t.schema_id) as schemaname, t.name as tablename, i.name as indexname, ius.user_seeks, ius.user_scans, ius.user_lookups, ius.user_updates
	, ius.last_user_seek, ius.last_user_scan, ius.last_user_lookup, ius.last_user_update
	, taille.size_mb	
	, (select stuff((
		select ', ' + CAST(c1.name as varchar(128))
		from  sys.index_columns ic1
			inner join sys.columns c1 on c1.object_id = ic1.object_id and c1.column_id = ic1.column_id
		where ic1.index_id = i.index_id and ic1.object_id = i.object_id and ic1.is_included_column = 0
		ORDER BY ic1.index_column_id
		FOR XML PATH('')
		), 1,2, '')
	) as colonnes
	, (select stuff((
		select ', ' + CAST(c1.name as varchar(128))
		from  sys.index_columns ic1
			inner join sys.columns c1 on c1.object_id = ic1.object_id and c1.column_id = ic1.column_id
		where ic1.index_id = i.index_id and ic1.object_id = i.object_id and ic1.is_included_column = 1
		ORDER BY ic1.index_column_id
		FOR XML PATH('')
		), 1,2, '')
	) as colonne_incluses
	, CONCAT('DROP INDEX ' + QUOTENAME(i.name), ' ON ' + SCHEMA_NAME(t.schema_id), '.', t.name , ';
	PRINT N''index ', i.name, ' dropped''') as cmd_drop
FROM 
	sys.dm_db_index_usage_stats ius
	inner join sys.indexes i on i.index_id = ius.index_id and i.object_id = ius.object_id
	inner join sys.tables t on t.object_id = i.object_id
	INNER JOIN (select index_id, object_id , sum(total_pages)/128 as size_mb from sys.partitions p inner join sys.allocation_units au on au.container_id = p.hobt_id group by p.object_id, p.index_id) as taille ON taille.object_id = i.object_id and taille.index_id = i.index_id
WHERE 
	t.is_ms_shipped = 0 AND ius.database_id = DB_ID()
	AND (ius.user_lookups + ius.user_scans + ius.user_seeks) = 0 --and ius.user_updates > 1000
	and i.is_primary_key = 0
	AND i.is_unique = 0
	and i.index_id > 1
ORDER BY t.name, i.name
option(recompile)

/***************************************************
Check if table size reach the max limit of size (based on max row size)
***************************************************/

;WITH ROWSIZE AS (
SELECT t.object_id, SUM(c.max_length) as max_row_size_bytes
FROM
	sys.tables t 
	inner join sys.columns c on t.object_id = c.object_id
	inner join sys.types ty on ty.system_type_id = c.system_type_id
WHERE
	t.is_ms_shipped = 0
	--AND t.object_id = OBJECT_ID('dbo.fre_rpa_RevenueParAnnonceur')
GROUP BY t.object_id
)
, pages as (
    SELECT object_id, SUM (reserved_page_count) as reserved_pages, SUM (used_page_count) as used_pages,
            SUM (case 
                    when (index_id < 2) then (in_row_data_page_count + lob_used_page_count + row_overflow_used_page_count)
                    else lob_used_page_count + row_overflow_used_page_count
                 end) as pages, sum(case when index_id < 2 then row_count else 0 end) as row_count
    FROM sys.dm_db_partition_stats
    group by object_id
), extra as (
    SELECT p.object_id, sum(reserved_page_count) as reserved_pages, sum(used_page_count) as used_pages
    FROM sys.dm_db_partition_stats p, sys.internal_tables it
    WHERE it.internal_type IN (202,204,211,212,213,214,215,216) AND p.object_id = it.object_id
    group by p.object_id
)
SELECT TableName, row_count, reserved_mb, data_mb, max_row_size_bytes, full_columns_estimated_data_size_mb, index_mb, unused_mb
	, CASE WHEN full_columns_estimated_data_size_mb > 0 THEN CASE WHEN reserved_mb / full_columns_estimated_data_size_mb > 0.8 THEN 'data size is near full estimated table size' END END as comment1
	, CASE WHEN data_mb > full_columns_estimated_data_size_mb THEN 'data size > full estimated table size' END as comment2
FROM (
	SELECT object_schema_name(p.object_id) + '.' + object_name(p.object_id) as TableName, p.row_count, (p.reserved_pages + isnull(e.reserved_pages, 0)) / 128 as reserved_mb,		
			p.pages /128 as data_mb,
			r.max_row_size_bytes,
			r.max_row_size_bytes * p.row_count / 1024 / 1024 as full_columns_estimated_data_size_mb,				
			(CASE WHEN p.used_pages + isnull(e.used_pages, 0) > pages THEN (p.used_pages + isnull(e.used_pages, 0) - pages) ELSE 0 END) /128 as index_mb,
			(CASE WHEN p.reserved_pages + isnull(e.reserved_pages, 0) > p.used_pages + isnull(e.used_pages, 0) THEN (p.reserved_pages + isnull(e.reserved_pages, 0) - p.used_pages + isnull(e.used_pages, 0)) else 0 end) /128 as unused_mb
	from pages p
	left outer join extra e on p.object_id = e.object_id
	INNER JOIN  ROWSIZE r ON r.object_id = p.object_id
) X
WHERE X.reserved_mb > 102400 -- only more than 100 GB tables
ORDER BY data_mb desc

GO


/********************************************
Script index
https://littlekendra.com/2016/05/05/how-to-script-out-indexes-from-sql-server/
*********************************************/
SELECT 
    DB_NAME() AS database_name,
    sc.name + N'.' + t.name AS table_name,
    (SELECT MAX(user_reads) 
        FROM (VALUES (last_user_seek), (last_user_scan), (last_user_lookup)) AS value(user_reads)) AS last_user_read,
    last_user_update,
    CASE si.index_id WHEN 0 THEN N'/* No create statement (Heap) */'
    ELSE 
        CASE is_primary_key WHEN 1 THEN
            N'ALTER TABLE ' + QUOTENAME(sc.name) + N'.' + QUOTENAME(t.name) + N' ADD CONSTRAINT ' + QUOTENAME(si.name) + N' PRIMARY KEY ' +
                CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED '
            ELSE N'CREATE ' + 
                CASE WHEN si.is_unique = 1 then N'UNIQUE ' ELSE N'' END +
                CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED ' +
                N'INDEX ' + QUOTENAME(si.name) + N' ON ' + QUOTENAME(sc.name) + N'.' + QUOTENAME(t.name) + N' '
        END +
        /* key def */ N'(' + key_definition + N')' +
        /* includes */ CASE WHEN include_definition IS NOT NULL THEN 
            N' INCLUDE (' + include_definition + N')'
            ELSE N''
        END +
        /* filters */ CASE WHEN filter_definition IS NOT NULL THEN 
            N' WHERE ' + filter_definition ELSE N''
        END +
        /* with clause - compression goes here */
        CASE WHEN row_compression_partition_list IS NOT NULL OR page_compression_partition_list IS NOT NULL 
            THEN N' WITH (' +
                CASE WHEN row_compression_partition_list IS NOT NULL THEN
                    N'DATA_COMPRESSION = ROW ' + CASE WHEN psc.name IS NULL THEN N'' ELSE + N' ON PARTITIONS (' + row_compression_partition_list + N')' END
                ELSE N'' END +
                CASE WHEN row_compression_partition_list IS NOT NULL AND page_compression_partition_list IS NOT NULL THEN N', ' ELSE N'' END +
                CASE WHEN page_compression_partition_list IS NOT NULL THEN
                    N'DATA_COMPRESSION = PAGE ' + CASE WHEN psc.name IS NULL THEN N'' ELSE + N' ON PARTITIONS (' + page_compression_partition_list + N')' END
                ELSE N'' END
            + N')'
            ELSE N''
        END +
        /* ON where? filegroup? partition scheme? */
        ' ON ' + CASE WHEN psc.name is null 
            THEN ISNULL(QUOTENAME(fg.name),N'')
            ELSE psc.name + N' (' + partitioning_column.column_name + N')' 
            END
        + N';'
    END AS index_create_statement,
    si.index_id,
    si.name AS index_name,
    partition_sums.reserved_in_row_GB,
    partition_sums.reserved_LOB_GB,
    partition_sums.row_count,
    stat.user_seeks,
    stat.user_scans,
    stat.user_lookups,
    user_updates AS queries_that_modified,
    partition_sums.partition_count,
    si.allow_page_locks,
    si.allow_row_locks,
    si.is_hypothetical,
    si.has_filter,
    si.fill_factor,
    si.is_unique,
    ISNULL(pf.name, '/* Not partitioned */') AS partition_function,
    ISNULL(psc.name, fg.name) AS partition_scheme_or_filegroup,
    t.create_date AS table_created_date,
    t.modify_date AS table_modify_date
FROM sys.indexes AS si
JOIN sys.tables AS t ON si.object_id=t.object_id
JOIN sys.schemas AS sc ON t.schema_id=sc.schema_id
LEFT JOIN sys.dm_db_index_usage_stats AS stat ON 
    stat.database_id = DB_ID() 
    and si.object_id=stat.object_id 
    and si.index_id=stat.index_id
LEFT JOIN sys.partition_schemes AS psc ON si.data_space_id=psc.data_space_id
LEFT JOIN sys.partition_functions AS pf ON psc.function_id=pf.function_id
LEFT JOIN sys.filegroups AS fg ON si.data_space_id=fg.data_space_id
/* Key list */ OUTER APPLY ( SELECT STUFF (
    (SELECT N', ' + QUOTENAME(c.name) +
        CASE ic.is_descending_key WHEN 1 then N' DESC' ELSE N'' END
    FROM sys.index_columns AS ic 
    JOIN sys.columns AS c ON 
        ic.column_id=c.column_id  
        and ic.object_id=c.object_id
    WHERE ic.object_id = si.object_id
        and ic.index_id=si.index_id
        and ic.key_ordinal > 0
    ORDER BY ic.key_ordinal FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS keys ( key_definition )
/* Partitioning Ordinal */ OUTER APPLY (
    SELECT MAX(QUOTENAME(c.name)) AS column_name
    FROM sys.index_columns AS ic 
    JOIN sys.columns AS c ON 
        ic.column_id=c.column_id  
        and ic.object_id=c.object_id
    WHERE ic.object_id = si.object_id
        and ic.index_id=si.index_id
        and ic.partition_ordinal = 1) AS partitioning_column
/* Include list */ OUTER APPLY ( SELECT STUFF (
    (SELECT N', ' + QUOTENAME(c.name)
    FROM sys.index_columns AS ic 
    JOIN sys.columns AS c ON 
        ic.column_id=c.column_id  
        and ic.object_id=c.object_id
    WHERE ic.object_id = si.object_id
        and ic.index_id=si.index_id
        and ic.is_included_column = 1
    ORDER BY c.name FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS includes ( include_definition )
/* Partitions */ OUTER APPLY ( 
    SELECT 
        COUNT(*) AS partition_count,
        CAST(SUM(ps.in_row_reserved_page_count)*8./1024./1024. AS NUMERIC(32,1)) AS reserved_in_row_GB,
        CAST(SUM(ps.lob_reserved_page_count)*8./1024./1024. AS NUMERIC(32,1)) AS reserved_LOB_GB,
        SUM(ps.row_count) AS row_count
    FROM sys.partitions AS p
    JOIN sys.dm_db_partition_stats AS ps ON
        p.partition_id=ps.partition_id
    WHERE p.object_id = si.object_id
        and p.index_id=si.index_id
    ) AS partition_sums
/* row compression list by partition */ OUTER APPLY ( SELECT STUFF (
    (SELECT N', ' + CAST(p.partition_number AS VARCHAR(32))
    FROM sys.partitions AS p
    WHERE p.object_id = si.object_id
        and p.index_id=si.index_id
        and p.data_compression = 1
    ORDER BY p.partition_number FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS row_compression_clause ( row_compression_partition_list )
/* data compression list by partition */ OUTER APPLY ( SELECT STUFF (
    (SELECT N', ' + CAST(p.partition_number AS VARCHAR(32))
    FROM sys.partitions AS p
    WHERE p.object_id = si.object_id
        and p.index_id=si.index_id
        and p.data_compression = 2
    ORDER BY p.partition_number FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS page_compression_clause ( page_compression_partition_list )
WHERE 
    si.type IN (0,1,2) /* heap, clustered, nonclustered */
ORDER BY table_name, si.index_id
    OPTION (RECOMPILE);

GO
/* ***************************
Some search index queries
**************************** */	

select t.name, c.name, c.is_nullable, c.is_identity, ty.name, c.max_length from sys.tables t inner join sys.columns c on c.object_id = t.object_id
	inner join sys.types ty on ty.system_type_id = c.system_type_id
where t.name like '%reservation%'
and ty.name not in ('int', 'bigint', 'varchar', 'nvarchar', 'bit')
order by t.name, c.column_id


go


select t.name, i.name as indexname, i.type_desc, i.is_primary_key, i.is_unique, c.name, c.is_nullable, c.is_identity
	, ty.name, c.max_length, ic.index_column_id, ic.key_ordinal, ic.partition_ordinal, ic.is_included_column
	from sys.tables t 	
	inner join sys.indexes i on i.object_id = t.object_id
	inner join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id
	inner join sys.columns c on c.object_id = ic.object_id and ic.column_id = c.column_id
	inner join sys.types ty on ty.system_type_id = c.system_type_id
where t.name like '%sometablename%'
order by t.name, i.index_id, ic.is_included_column, ic.key_ordinal

go


select t.name, i.name as indexname, i.type_desc, i.is_primary_key, i.is_unique, ius.user_seeks, ius.user_scans, ius.user_lookups, ius.user_updates, ius.last_user_seek, ius.last_user_scan, ius.last_user_lookup, ius.last_user_update
from
	sys.dm_db_index_usage_stats ius
	inner join sys.indexes i on i.object_id = ius.object_id and ius.index_id = i.index_id
	inner join sys.tables t on t.object_id = i.object_id
where t.name like '%sometablename%'
order by t.name, i.index_id

select i.name as indexname, i.type_desc, i.is_primary_key, i.is_unique, c.name, c.is_nullable, c.is_identity
from 
	sys.indexes i
	left outer join sys.index_columns ic on ic.index_id = i.index_id and ic.object_id = i.object_id
	left outer join sys.columns c on c.column_id = ic.column_id and c.object_id = ic.object_id
where i.object_id = object_id('dbo.table1')

/* ****************************************
Files size and free space
***************************************** */
select databasename, name, drive, size_mb, spaceused_mb, convert(decimal(10,2), (100.0 * spaceused_mb / size_mb)) as percent_space_used, free_mb, convert(decimal(10,2),(100* free_mb/size_mb)) as percent_free
from
(
select db_name(database_id) as databasename, name, left(physical_name,1) as drive, size/128 as size_mb, FILEPROPERTY(name, 'spaceused') /128 as spaceused_mb, (size- FILEPROPERTY(name, 'spaceused'))/128 as free_mb from sys.master_files where left(physical_name,1) = 'E' and database_id = db_id()
) x
order by size_mb desc
go

go
select *, case when size_mb > 0 then convert(decimal(5,2), 100.0 * free_mb / size_mb) end as percent_free
from (

select name as fichier, size/128 as size_mb, FILEPROPERTY(name, 'SpaceUsed')/128 as spaceused_mb, (size - FILEPROPERTY(name, 'SpaceUsed'))/128 as free_mb from sys.master_files where database_id = 2
) x
order by free_mb desc

GO
select *, case when size_mb > 0 then convert(decimal(5,2), 100.0 * free_mb / size_mb) end as percent_free
from (
select name as fichier,LEFT(physical_name,1) as drive,  size/128 as size_mb, FILEPROPERTY(name, 'SpaceUsed')/128 as spaceused_mb, (size - FILEPROPERTY(name, 'SpaceUsed'))/128 as free_mb 
from sys.database_files
) x
order by free_mb desc
GO


-- for all DB
DECLARE @tmp table (db sysname, fichier sysname, drive char(1), size_mb int, space_used_mb int, free_mb int)
INSERT INTO @tmp (db, fichier, drive, size_mb, space_used_mb, free_mb)
EXEC sp_msforeachdb N'
USE [?]
select DB_NAME(), name, LEFT(physical_name,1), size/128 as size_mb, FILEPROPERTY(name, ''SpaceUsed'')/128 as space_used_mb, (size - FILEPROPERTY(name, ''SpaceUsed''))/128 as free_mb from sys.master_files
where database_id = db_id()
'
SELECT *, CASE WHEN size_mb > 0 THEN convert(decimal(5,2), 100.0 * free_mb/size_mb ) END as percent_free FROM @tmp 
ORDER BY free_mb DESC
GO

/*
Deal with too much log files
DBCC SHRINKFILE('xxx_log', 0, TRUNCATEONLY)
ALTER DATABASE xxx MODIFY FILE(NAME='xxx_log', SIZE=2048MB)
*/


/*********************
Backup check
*********************/
use msdb
select top 100 bs.database_name, bs.type, bs.backup_finish_date, convert(numeric(18,2), bs.compressed_backup_size/1024/1024) as compressed_backup_size_mb 
	, bmf.logical_device_name, bmf.physical_device_name, DATEDIFF(second, bs.backup_start_date, bs.backup_finish_date) as duration_sec
	, RIGHT('0' + CONVERT(VARCHAR(100), datediff(ss,bs.backup_start_date, bs.backup_finish_date) / 3600 ),2) + ':' +
		RIGHT('0' + CONVERT(VARCHAR(100),datediff(ss,bs.backup_start_date, bs.backup_finish_date) % 3600 / 60),2) + ':' + 
		RIGHT('0' + CONVERT(VARCHAR(100),datediff(ss,bs.backup_start_date, bs.backup_finish_date) % 3600),2) as HoursMinutes
from dbo.backupset bs
	inner join dbo.backupmediafamily bmf on bmf.media_set_id = bs.media_set_id
where bs.database_name = 'SomeDatabase' --and type = 'D'
ORDER BY bs.backup_finish_date desc
GO

use msdb
go

SELECT *
FROM 
(
SELECT @@SERVERNAME as servername, database_name, type, backup_start_date, backup_finish_date
	, DATEDIFF(SECOND, backup_start_date, backup_finish_date) as duration_sec
	, CAST(DATEDIFF(day,'1900-01-01', backup_finish_date - backup_start_date) AS VARCHAR) +  'd ' + CONVERT(varchar(22), backup_finish_date - backup_start_date, 114) as duration
	, CONVERT(numeric(10,2), compressed_backup_size/1024/1024) as compressed_backup_size_mb
	, ROW_NUMBER() OVER(PARTITION BY database_name, type ORDER BY backup_start_date DESC) AS RN
FROM dbo.backupset
WHERE database_name in ('Database1' , 'Database2')
) x
WHERE RN <= 10
ORDER BY backup_finish_date DESC

/******************
Reads SQL trace
******************/
select starttime as st1, eventclass as evt, duration/1000000 as duration_sec, reads*8/1024 as reads_mb, * from sys.fn_trace_gettable('D:\path_to_trc_file.trc', -1)
where hostname = 'hostname'


/**************
Manage VLF
**************/

DECLARE @file_name sysname,
@file_size int,
@file_growth int,
@shrink_command nvarchar(max),
@alter_command nvarchar(max)

SELECT @file_name = name,
@file_size = (size / 128)
FROM sys.database_files
WHERE type_desc = 'log'

SELECT @shrink_command = 'DBCC SHRINKFILE (N''' + @file_name + ''' , 0, TRUNCATEONLY)'
PRINT @shrink_command
--EXEC sp_executesql @shrink_command

SELECT @shrink_command = 'DBCC SHRINKFILE (N''' + @file_name + ''' , 0)'
PRINT @shrink_command
--EXEC sp_executesql @shrink_command

SELECT @alter_command = 'ALTER DATABASE [' + db_name() + '] MODIFY FILE (NAME = N''' + @file_name + ''', SIZE = ' + CAST(@file_size AS nvarchar) + 'MB)'
PRINT @alter_command
--EXEC sp_executesql @alter_command

go

/***************************************
Misc queries
***************************************/
-- List databases
SELECT @@servername as servername, db_name(mf.database_id) as db, mf.type_desc, SUM(mf.size)/128 as size_mb
	, d.state_desc, d.compatibility_level, d.is_read_only, d.create_date, d.collation_name, d.recovery_model_desc
from sys.master_files mf
	inner join sys.databases d on d.database_id = mf.database_id
where mf.database_id > 4 and lower(DB_NAME(mf.database_id)) <> 'sysdba'
group by mf.database_id, mf.type_desc, d.state_desc, d.compatibility_level, d.is_read_only, d.create_date, d.collation_name, d.recovery_model_desc

-- List jobs
select @@servername as servername, name as jobname, date_created, enabled, x.last_run_date, x.avg_run_duration_sec
from msdb.dbo.sysjobs j
	LEFT OUTER JOIN (
		SELECT jh.job_id, MAX(jh.run_date) as last_run_date, AVG(run_duration) as avg_run_duration_sec from msdb.dbo.sysjobhistory jh group by job_id
	)x ON x.job_id = j.job_id
where name not like 'DBA%' 
order by name


/****************************************
Permissions
*****************************************/
use DUJARDINV1_REC01
go

select
	pr.name, pr.authentication_type_desc, CASE WHEN class_desc like 'OBJECT%' THEN object_name(dp.major_id) WHEN class_desc = 'SCHEMA' THEN schema_name(major_id) END as [object_name], dp.class_desc, dp.permission_name, dp.state_desc, dp.type
from sys.database_permissions dp
	inner join sys.database_principals pr on pr.principal_id = dp.grantee_principal_id
where pr.name = 'somedomain\someaccount'

select rol.name as [role], grantee.name as [member]
from sys.database_role_members drm
	inner join sys.database_principals grantee on grantee.principal_id = drm.member_principal_id
	inner join sys.database_principals rol on rol.principal_id = drm.role_principal_id
-- where grantee.name = 'somedomain\someaccount'
