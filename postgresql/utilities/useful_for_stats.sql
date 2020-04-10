/*
Useful queries bases on pg_catalog to get some informations
*/

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

