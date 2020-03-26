-- execute as SQLCMD
:setvar directory "c:\temp"
:setvar sessionname "queries"
USE [msdb]
GO

/****** Object:  Job [XEVENT_queries]    Script Date: 17/03/2020 09:33:23 ******/
EXEC msdb.dbo.sp_delete_job @job_name=N'DBA_XEVENT_queries', @delete_unused_schedule=1
GO

/****** Object:  Job [XEVENT_queries]    Script Date: 17/03/2020 09:33:23 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Trace]    Script Date: 17/03/2020 09:33:23 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Trace' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Trace'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA_XEVENT_queries', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'Trace', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Delete historique]    Script Date: 17/03/2020 09:33:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Delete historique', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'/*
DECLARE @OS_commande NVARCHAR(255)
    DECLARE @nbficretention VARCHAR(10) = ''15''
    DECLARE @Repertoire VARCHAR(255) = ''$(directory)''
    DECLARE @Base_NomFic VARCHAR(255) = ''$(sessionname)*''
    -- Purge des traces précédentes avec rétention de @nbficretention fichiers  
    set @OS_commande = ''FOR /F "skip='' + @nbficretention + '' tokens=*" %i IN (''''DIR /O-D /B '' + @Repertoire + ''\'' + @Base_NomFic + ''*.xel'''') DO DEL "'' + @Repertoire + ''\%i"''  
    exec xp_cmdshell @OS_commande  

--EXEC xp_cmdshell ''FORFILES /p c:\BACKUP /s /m *.sql /d -30 /c "CMD /C del /Q /F @FILE"''

*/



EXEC xp_cmdshell ''FORFILES /p $(directory) /m *.xel /d -15 /c "CMD /C DEL /Q /F @FILE"''', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Stop Session]    Script Date: 17/03/2020 09:33:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Stop Session', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'IF EXISTS(select 1 from sys.dm_xe_sessions where name = ''$(sessionname)'')
    BEGIN
    	ALTER EVENT SESSION [$(sessionname)] ON SERVER  STATE = STOP;
    END
     
    ', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Drop Target file]    Script Date: 17/03/2020 09:33:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Drop Target file', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'ALTER EVENT SESSION [$(sessionname)] ON SERVER 
    DROP TARGET package0.event_file', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Add target file]    Script Date: 17/03/2020 09:33:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Add target file', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DECLARE @filename NVARCHAR(255) = N''$(directory)\$(sessionname)_'' + FORMAT(GETDATE(), ''yyyyMMdd-HH\hmm'') + ''.xel'';
    DECLARE @sql NVARCHAR(max) = N''ALTER EVENT SESSION [$(sessionname)] ON SERVER 
    ADD TARGET package0.event_file(SET filename=N'''''' + @filename + '''''',max_rollover_files=(1))''
    EXEC (@sql)', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Start session]    Script Date: 17/03/2020 09:33:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Start session', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'ALTER EVENT SESSION [$(sessionname)] ON SERVER  STATE = START;', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'DBA_XEVENT_queries', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20180710, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


