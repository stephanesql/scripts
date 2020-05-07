/*
Useful queries bases on pg_catalog to get some informations helping for analyzing performance problems
*/

-- how many connections
select count(*) from pg_stat_activity;

select count(*), datname from pg_stat_activity group by datname;

select count(*), state from pg_stat_activity group by state;


-- What is cluster max connection
select name, setting, unit from pg_settings where name = 'max_connections';

-- Are there some limit for connection defined by database
select datname, datconnlimit from pg_database;


-- Databases size, cache hit ratio, stat uptime (how long since last stat reset call), temp files and size, conflicts and deadlocks
select
    datname, pg_size_pretty(pg_database_size(datname)) as size,
    case when blks_read > 0 then round(100.0 * blks_hit / (blks_read +  blks_hit),2) end as ratio,
    clock_timestamp() - stats_reset as stat_uptime, temp_files, pg_size_pretty(temp_bytes) as temp_size, conflicts, deadlocks
from pg_stat_database;

-- Relation dead and alive tuples. If dead tuples are more than 20% of live tuples, it could means vacuum is not frequent enough for the relation
select schemaname, relname, n_live_tup, n_dead_tup, CASE WHEN n_live_tup + n_dead_tup > 0 THEN round(100 * n_dead_tup / (n_dead_tup + n_live_tup),2) END as vacuum_ratio
, CASE WHEN n_live_tup > 0 AND n_dead_tup >= 0.2 * (n_dead_tup + n_live_tup) THEN 'WARNING - VACCUM Frequency could be low' END
, last_vacuum, last_autovacuum
from pg_stat_user_tables
order by vacuum_ratio desc nulls last;


-- not used indexes
select schemaname,relname,indexrelname
	, idx_scan
	, idx_tup_read, idx_tup_fetch
	, pg_size_pretty(pg_relation_size(schemaname || '.' || indexrelname)) as size 
from pg_stat_user_indexes  
where idx_scan <= 100 -- or 0
order by idx_scan, size desc;



-- Get long running sessions
SELECT pid, age(clock_timestamp(), query_start), usename, substring(query,0,50) as query, state
FROM pg_stat_activity 
WHERE 
-- query != '<IDLE>' AND
 pid <> pg_backend_pid()
--AND query NOT ILIKE '%pg_stat_activity%'
AND state <> 'idle' -- not idle sessions
ORDER BY query_start desc;


/**
Cache hit ratios. 
Used statistics are from last reset. 
**/

-- Check Buffer cache hit ratio
SELECT sum(heap_blks_read) as heap_read, sum(heap_blks_hit)  as heap_hit, --(sum(heap_blks_hit) - sum(heap_blks_read)) / sum(heap_blks_hit) as ratio
round(100.0 * sum(heap_blks_hit) / (sum(heap_blks_read) + sum(heap_blks_hit)),2) as ratio
FROM pg_statio_user_tables;

-- for databases
select db.datname, d.blks_read, d.blks_hit, case when d.blks_hit + d.blks_read > 0 then  round(100.0 * d.blks_hit / (d.blks_hit+d.blks_read),2) end as blks_hit_ratio from pg_stat_database d inner join pg_database db on db.oid = d.datid;


-- For all tables
with 
all_tables as
(
SELECT  *
FROM    (
    SELECT  'all'::text as table_name, 
        sum( (coalesce(heap_blks_read,0) + coalesce(idx_blks_read,0) + coalesce(toast_blks_read,0) + coalesce(tidx_blks_read,0)) ) as from_disk, 
        sum( (coalesce(heap_blks_hit,0)  + coalesce(idx_blks_hit,0)  + coalesce(toast_blks_hit,0)  + coalesce(tidx_blks_hit,0))  ) as from_cache    
    FROM    pg_statio_all_tables  --> change to pg_statio_USER_tables if you want to check only user tables (excluding postgres's own tables)
    ) a
WHERE   (from_disk + from_cache) > 0 -- discard tables without hits
),
tables as 
(
SELECT  *
FROM    (
    SELECT  relname as table_name, 
        ( (coalesce(heap_blks_read,0) + coalesce(idx_blks_read,0) + coalesce(toast_blks_read,0) + coalesce(tidx_blks_read,0)) ) as from_disk, 
        ( (coalesce(heap_blks_hit,0)  + coalesce(idx_blks_hit,0)  + coalesce(toast_blks_hit,0)  + coalesce(tidx_blks_hit,0))  ) as from_cache    
    FROM    pg_statio_all_tables --> change to pg_statio_USER_tables if you want to check only user tables (excluding postgres's own tables)
    ) a
WHERE   (from_disk + from_cache) > 0 -- discard tables without hits
)
SELECT  table_name as "table name",
    from_disk as "disk hits",
    round((from_disk::numeric / (from_disk + from_cache)::numeric)*100.0,2) as "% disk hits",
    round((from_cache::numeric / (from_disk + from_cache)::numeric)*100.0,2) as "% cache hits",
    (from_disk + from_cache) as "total hits"
FROM    (SELECT * FROM all_tables UNION ALL SELECT * FROM tables) a
ORDER   BY (case when table_name = 'all' then 0 else 1 end), from_disk desc;

