/*
pg_buffercache
https://sites.google.com/site/itmyshare/database-tips-and-examples/postgres/useful-sqls-to-check-contents-of-postgresql-shared_buffer

 postgres=# CREATE EXTENSION pg_buffercache;  
*/
 
--#1 This SQL show you the relations buffered in database share buffer, ordered by relation percentage taken in shared buffer. 
--It also shows that how much of the whole relation is buffered.
select c.relname,pg_size_pretty(count(*) * 8192) as buffered
	, round(100.0 * count(*) / ( select setting from pg_settings where name='shared_buffers')::integer,1) as buffer_percent
	, round(100.0*count(*)*8192 / pg_table_size(c.oid),1) as percent_of_relation 
from pg_class c 
	inner join pg_buffercache b on b.relfilenode = c.relfilenode 
	inner join pg_database d on ( b.reldatabase =d.oid and d.datname =current_database())
group by c.oid,c.relname 
order by 3 desc limit 10;

--#2 relation usage count in PostgreSQL database shared buffer
select c.relname,count(*) as buffers,usagecount 
from pg_class c 
	inner join pg_buffercache b on b.relfilenode = c.relfilenode 
	inner join pg_database d on (b.reldatabase = d.oid and d.datname =current_database()) 
group by c.relname,usagecount 
order by c.relname,usagecount;

--#3 disk usage
select nspname,relname,pg_size_pretty(pg_relation_size(c.oid)) as "size" 
from pg_class c left join pg_namespace n on ( n.oid=c.relnamespace) 
where nspname not in ('pg_catalog','information_schema') 
order by pg_relation_size(c.oid) desc limit 30;

--#4 top relation in cache
select c.relname,count(*) as buffers 
from pg_class c 
	inner join pg_buffercache b on b.relfilenode=c.relfilenode 
	inner join pg_database d on (b.reldatabase=d.oid and d.datname=current_database()) 
group by c.relname 
order by 2 desc limit 20;

-- #5 summary of buffer usage count
select usagecount, count(*) as count, isdirty 
from pg_buffercache
group by usagecount, isdirty
order by usagecount, isdirty

--#6 lock information in postgreSQL
-- postgresql 9.2
select locktype,virtualtransaction,transactionid,nspname,relname,mode,granted
	,cast(date_trunc('second',query_start) as timestamp) as query_start, substr(current_query,1,60) as query 
from pg_locks 
	left outer join pg_class on (pg_locks.relation = pg_class.oid) 
	left outer join pg_namespace on (pg_namespace.oid = pg_class.relnamespace)
	, pg_stat_activity 
where not pg_locks.pid=pg_backend_pid() and pg_locks.pid = pg_stat_activity.procpid 
order by virtualtransaction;
-- postgresql 12
select locktype,virtualtransaction,transactionid,nspname,relname,mode,granted
	,cast(date_trunc('second',query_start) as timestamp) as query_start, substr(query,1,60) as query 
from pg_locks 
	left outer join pg_class on (pg_locks.relation = pg_class.oid) 
	left outer join pg_namespace on (pg_namespace.oid = pg_class.relnamespace)
	left outer join pg_stat_activity on pg_locks.pid = pg_stat_activity.pid
	--, pg_stat_activity
where not pg_locks.pid=pg_backend_pid() --and pg_locks.pid = pg_stat_activity.pid 
order by virtualtransaction;





/*************************
Query for buffer cache
*************************/
 -- by database
select d.datname, pg_size_pretty(count(*) * 8192) as buffer_size
	,round(100.0 * count(*) / ( SELECT setting FROM pg_settings WHERE name='shared_buffers')::integer,1) AS buffers_percent
from pg_buffercache bc 
	LEFT OUTER JOIN pg_database d on d.oid = bc.reldatabase 
group by d.datname
ORDER BY buffers_percent DESC;

-- with ratio of database size
select d.datname, pg_size_pretty(count(*) * 8192) as buffer_size
	,round(100.0 * count(*) / ( SELECT setting FROM pg_settings WHERE name='shared_buffers')::integer,1) AS buffers_percent
	, pg_size_pretty(pg_database_size(oid)) as database_size
	, round(100.0 * count(*) * 8192 / pg_database_size(d.oid), 1) as percent_database_in_cache
from pg_buffercache bc 
	LEFT OUTER JOIN pg_database d on d.oid = bc.reldatabase 
group by d.datname, d.oid
ORDER BY buffers_percent DESC;

-- by relation
SELECT nsp.nspname, c.relname
  , pg_size_pretty(count(*) * 8192) as buffered
  , round(100.0 * count(*) / ( SELECT setting FROM pg_settings WHERE name='shared_buffers')::integer,1) AS buffers_percent
  , round(100.0 * count(*) * 8192 / pg_relation_size(c.oid),1) AS percent_of_relation
  --, round(100.0 * count(*) * 8192 / pg_database_size(current_database()), 1) as percent_of_database_size
 FROM pg_class c
	INNER JOIN pg_namespace nsp on nsp.oid = c.relnamespace
 INNER JOIN pg_buffercache b ON b.relfilenode = c.relfilenode
 INNER JOIN pg_database d ON (b.reldatabase = d.oid AND d.datname = current_database())
 WHERE pg_relation_size(c.oid) > 0
 GROUP BY c.oid, nsp.nspname, c.relname
 ORDER BY buffers_percent DESC
 LIMIT 10;
 
-- plus add ratio of relation buffer / database buffer
SELECT nsp.nspname, c.relname
  , pg_size_pretty(count(*) * 8192) as buffered
  , round(100.0 * count(*) / ( SELECT setting FROM pg_settings WHERE name='shared_buffers')::integer,1) AS buffers_percent
  , round(100.0 * count(*) * 8192 / pg_relation_size(c.oid),1) AS percent_of_relation
  , round(100.0 * count(*) * 8192 / pg_database_size(current_database()), 1) as percent_of_total_database_size
  , round(100.0 * count(*) /  db.db_pages, 1)  AS percent_database_in_cache
 FROM pg_class c
	INNER JOIN pg_namespace nsp on nsp.oid = c.relnamespace
 INNER JOIN pg_buffercache b ON b.relfilenode = c.relfilenode
 INNER JOIN pg_database d ON (b.reldatabase = d.oid AND d.datname = current_database())
 INNER JOIN LATERAL (
	SELECT count(*) as db_pages FROM pg_buffercache bd WHERE bd.reldatabase = d.oid
	) db ON TRUE
 WHERE pg_relation_size(c.oid) > 0
 GROUP BY c.oid, nsp.nspname, c.relname, db.db_pages
 ORDER BY buffers_percent DESC, buffered DESC
 LIMIT 10;
 
 -- with CTE (most elegant solution)
 WITH dbcache AS (
 SELECT count(*) as db_pages, bd.reldatabase FROM pg_buffercache bd GROUP BY bd.reldatabase
 )
 SELECT nsp.nspname, c.relname
  , pg_size_pretty(count(*) * 8192) as buffered
  , round(100.0 * count(*) / ( SELECT setting FROM pg_settings WHERE name='shared_buffers')::integer,1) AS buffers_percent
  , round(100.0 * count(*) * 8192 / pg_relation_size(c.oid),1) AS percent_of_relation
  , round(100.0 * count(*) * 8192 / pg_database_size(current_database()), 1) as percent_of_total_database_size
  , round(100.0 * count(*) /  db.db_pages, 1)  AS percent_database_in_cache
 FROM pg_class c
	INNER JOIN pg_namespace nsp on nsp.oid = c.relnamespace
 INNER JOIN pg_buffercache b ON b.relfilenode = c.relfilenode
 INNER JOIN pg_database d ON (b.reldatabase = d.oid AND d.datname = current_database())
 INNER JOIN dbcache db ON db.reldatabase = b.reldatabase
 WHERE pg_relation_size(c.oid) > 0
 GROUP BY c.oid, nsp.nspname, c.relname, db.db_pages
 ORDER BY buffers_percent DESC, buffered DESC
 LIMIT 10;
 
 --with windows function (works only without filtering on relname)
 SELECT nsp.nspname, c.relname
  , pg_size_pretty(count(*) * 8192) as buffered
  , round(100.0 * count(*) / ( SELECT setting FROM pg_settings WHERE name='shared_buffers')::integer,1) AS buffers_percent
  , round(100.0 * count(*) * 8192 / pg_relation_size(c.oid),1) AS percent_of_relation
  , round(100.0 * count(*) * 8192 / pg_database_size(current_database()), 1) as percent_of_total_database
  , round(100.0 * count(*) /  SUM(count(*)) OVER(), 1)  AS percent_database_in_cache
 FROM pg_class c
	INNER JOIN pg_namespace nsp on nsp.oid = c.relnamespace
 INNER JOIN pg_buffercache b ON b.relfilenode = c.relfilenode
 INNER JOIN pg_database d ON (b.reldatabase = d.oid AND d.datname = current_database())
 WHERE pg_relation_size(c.oid) > 0
 GROUP BY c.oid, nsp.nspname, c.relname
 ORDER BY buffers_percent DESC, buffered DESC
 LIMIT 10;

/*************************************
pg_stat_statements queries
*************************************/
SELECT 
  (total_time / 1000 / 60) as total_min, 
  (total_time/calls) as avg, stddev_time, 
  calls,
  query 
FROM pg_stat_statements 
ORDER BY 1 DESC 
LIMIT 100;


/****************************************
Database & Relation size
*****************************************/
-- by database
SELECT datname, pg_size_pretty(pg_database_size(oid)) as size
FROM pg_database
ORDER BY pg_database_size(oid) DESC;

-- by relation
SELECT nsp.nspname, c.relname, c.relkind, pg_size_pretty(pg_total_relation_size(c.oid)) as total_size
	, pg_size_pretty(pg_relation_size(c.oid)) as relation_size
	, pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid)) as index_size
FROM pg_class c
	INNER JOIN pg_namespace nsp ON nsp.oid = c.relnamespace
ORDER BY pg_total_relation_size(c.oid) DESC
LIMIT 10;



/*****************
https://blog.dataegret.com/2017/05/deep-dive-into-postgres-stats.html
https://wiki.postgresql.org/wiki/Index_Maintenance
https://github.com/dataegret/pg-utils/blob/master/sql/low_used_indexes.sql
https://www.footcow.com/index.php/post/2009/10/23/PostgreSQL-%3A-Vos-index-sont-ils-utilis%C3%A9s
*****************/

/************************************
psql useful
*************************************/
-- use \t and \a 
-- or \pset footer off
SELECT 'select ''' || n.nspname || '.' || relname || ''' as tablename, count(*) as nb_tuples from ' || n.nspname || '.' || relname ||';' as cmd
FROM pg_class c
	INNER JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'm')
\gexec


/********************************************
Tablespaces
********************************************/
select ts.spcname, pg_tablespace_location(ts.oid), pg_size_pretty(pg_tablespace_size(ts.oid)) as size
from pg_tablespace ts 
order by size desc;


/****************************
Bloat estimation from 
https://github.com/ioguix/pgsql-bloat-estimation/blob/master/btree/btree_bloat-8.0-8.1.sql
*****************************/
SELECT current_database(), nspname AS schemaname, tblname, idxname, pg_size_pretty(bs*(sub.relpages)::bigint) AS real_size,
  pg_size_pretty(bs*est_pages::bigint) as estimated_size,
  pg_size_pretty(bs*(sub.relpages-est_pages)::bigint) AS bloat_size,
  round(100.0 * (sub.relpages-est_pages)::numeric/ sub.relpages, 1) AS bloat_ratio, is_na
  -- , est_pages, index_tuple_hdr_bm, maxalign, pagehdr, nulldatawidth, nulldatahdrwidth, sub.reltuples, sub.relpages -- (DEBUG INFO)
FROM (
  SELECT bs, nspname, table_oid, tblname, idxname, relpages, coalesce(
      1+ceil(reltuples/floor((bs-pageopqdata-pagehdr)/(4+nulldatahdrwidth)::float)), 0 -- ItemIdData size + computed avg size of a tuple (nulldatahdrwidth)
    ) AS est_pages, is_na
    -- , index_tuple_hdr_bm, maxalign, pagehdr, nulldatawidth, nulldatahdrwidth, reltuples -- (DEBUG INFO)
  FROM (
    SELECT maxalign, bs, nspname, tblname, idxname, reltuples, relpages, relam, table_oid,
      ( index_tuple_hdr_bm +
          maxalign - CASE -- Add padding to the index tuple header to align on MAXALIGN
            WHEN index_tuple_hdr_bm%maxalign = 0 THEN maxalign
            ELSE index_tuple_hdr_bm%maxalign
          END
        + nulldatawidth + maxalign - CASE -- Add padding to the data to align on MAXALIGN
            WHEN nulldatawidth = 0 THEN 0
            WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign
            ELSE nulldatawidth::integer%maxalign
          END
      )::numeric AS nulldatahdrwidth, pagehdr, pageopqdata, is_na
      -- , index_tuple_hdr_bm, nulldatawidth -- (DEBUG INFO)
    FROM (
      SELECT
        i.nspname, i.tblname, i.idxname, i.reltuples, i.relpages, i.relam, a.attrelid AS table_oid,
        current_setting('block_size')::numeric AS bs,
        CASE -- MAXALIGN: 4 on 32bits, 8 on 64bits (and mingw32 ?)
          WHEN version() ~ 'mingw32' OR version() ~ '64-bit|x86_64|ppc64|ia64|amd64' THEN 8
          ELSE 4
        END AS maxalign,
        /* per page header, fixed size: 20 for 7.X, 24 for others */
        24 AS pagehdr,
        /* per page btree opaque data */
        16 AS pageopqdata,
        /* per tuple header: add IndexAttributeBitMapData if some cols are null-able */
        CASE WHEN max(coalesce(s.null_frac,0)) = 0
          THEN 2 -- IndexTupleData size
          ELSE 2 + (( 32 + 8 - 1 ) / 8) -- IndexTupleData size + IndexAttributeBitMapData size ( max num filed per index + 8 - 1 /8)
        END AS index_tuple_hdr_bm,
        /* data len: we remove null values save space using it fractionnal part from stats */
        sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024)) AS nulldatawidth,
        max( CASE WHEN a.atttypid = 'pg_catalog.name'::regtype THEN 1 ELSE 0 END ) > 0 AS is_na
      FROM pg_attribute AS a
        JOIN (
          SELECT nspname, tbl.relname AS tblname, idx.relname AS idxname, idx.reltuples, idx.relpages, idx.relam,
            indrelid, indexrelid, string_to_array(pg_catalog.textin(pg_catalog.int2vectorout(indkey)), ' ')::smallint[] AS attnum
          FROM pg_index
            JOIN pg_class idx ON idx.oid=pg_index.indexrelid
            JOIN pg_class tbl ON tbl.oid=pg_index.indrelid
            JOIN pg_namespace ON pg_namespace.oid = idx.relnamespace
          WHERE tbl.relkind = 'r' AND idx.relpages > 0
        ) AS i ON a.attrelid = i.indexrelid
        JOIN pg_stats AS s ON s.schemaname = i.nspname
          AND ((s.tablename = i.tblname AND s.attname = pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE)) -- stats from tbl
          OR   (s.tablename = i.idxname AND s.attname = a.attname))-- stats from functionnal cols
        JOIN pg_type AS t ON a.atttypid = t.oid
      WHERE a.attnum > 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    ) AS s1
  ) AS s2
    JOIN pg_am am ON s2.relam = am.oid WHERE am.amname = 'btree'
) AS sub
 --WHERE NOT is_na
	WHERE tblname !~ '^pg_'
ORDER BY 2,3,4;

-- from Dimitri Fontaine
-- http://pgsql.tapoueh.org/site/html/news/20080131.bloat.html
SELECT
        schemaname, tablename, reltuples::bigint, relpages::bigint, otta,
        ROUND(CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages/otta::numeric END,1) AS tbloat,
        relpages::bigint - otta AS wastedpages,
        bs*(sml.relpages-otta)::bigint AS wastedbytes,
        pg_size_pretty((bs*(relpages-otta))::bigint) AS wastedsize,
        iname, ituples::bigint, ipages::bigint, iotta,
        ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) AS ibloat,
        CASE WHEN ipages < iotta THEN 0 ELSE ipages::bigint - iotta END AS wastedipages,
        CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes,
        CASE WHEN ipages < iotta THEN pg_size_pretty(0::bigint) ELSE pg_size_pretty((bs*(ipages-iotta))::bigint) END AS wastedisize
      FROM (
        SELECT
          schemaname, tablename, cc.reltuples, cc.relpages, bs,
          CEIL((cc.reltuples*((datahdr+ma-
            (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta,
          COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
          COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
        FROM (
          SELECT
            ma,bs,schemaname,tablename,
            (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
            (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
          FROM (
            SELECT
              schemaname, tablename, hdr, ma, bs,
              SUM((1-null_frac)*avg_width) AS datawidth,
              MAX(null_frac) AS maxfracsum,
              hdr+(
                SELECT 1+count(*)/8
                FROM pg_stats s2
                WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
              ) AS nullhdr
            FROM pg_stats s, (
              SELECT
                (SELECT current_setting('block_size')::numeric) AS bs,
                CASE WHEN substring(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
                CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
              FROM (SELECT version() AS v) AS foo
            ) AS constants
            GROUP BY 1,2,3,4,5
          ) AS foo
        ) AS rs
        JOIN pg_class cc ON cc.relname = rs.tablename
        JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname
        LEFT JOIN pg_index i ON indrelid = cc.oid
        LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
      ) AS sml
      WHERE sml.relpages - otta > 0 OR ipages - iotta > 10
      ORDER BY wastedbytes DESC, wastedibytes DESC;

/**********************************
List schemas
***********************************/
 SELECT n.nspname AS "Name",                                          
   pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner",                 
   pg_catalog.array_to_string(n.nspacl, E'\n') AS "Access privileges",
   pg_catalog.obj_description(n.oid, 'pg_namespace') AS "Description" 
 FROM pg_catalog.pg_namespace n                                       
 WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'      
 ORDER BY 1;

  
