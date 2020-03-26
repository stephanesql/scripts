# scripts
Contains some utilities SQL scripts mostly for Microsoft SQL Server.

## mssql/monitoring
Contains monitoring scripts for MS SQL Server, grouped in solution

|Solution|File|Usage|
|--------|----|-----|
|IO file stats|io_file_stats_monitoring.sql|Solution for monitoring IO File stats. This script creates two tables and a procedure to records the io file stats from sys.dm_io_virutal_file_stats DMV and calculates the delta from last record. Create a view to help reading the data|
||io_file_stats_monitoring_job.sql|Create a job calling the procedure in io_file_stats_monitoring.sql every 30 min and purge data older than 1 month|
|IO File stats alerting|io_file_stats_monitoring_for_alerting.sql|Same as Solution for monitoring IO File stats, but the procedure records only a threshold in IO reads or writes is passed|
||io_file_stats_monitoring_for_alerting_job.sql|Same as Solution for monitoring IO File stats jobs, but the procedure is called every 5 min|
|Wait stats|wait_stats_monitoring.sql|Solution for monitoring wait events. Create two tables and one procedure and one view to help reading the data. The procedures records in the tables and calculate the delta from previous record|
||wait_stats_monitoring_job.sql|Create a job calling the procedure in wait_stats_monitoring.sql every 30 min and purge data older than 1 month|
|IO file stats and Wait stats|io_file_stats.pbix|Power BI to visualize the recorded data (IO file stats and Wait events)|

