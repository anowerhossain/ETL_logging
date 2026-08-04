-- ============================================================================
-- ddl_logging.sql
-- ----------------------------------------------------------------------------
-- Table used by the Logging module (logging_module/etl_logger.py) to persist
-- job-level AND task-level status events.
--
-- One row is written:
--   * when a JOB starts / ends            (task_id IS NULL, log_level='JOB')
--   * when each TASK starts / ends        (task_id IS NOT NULL, log_level='TASK')
--
-- Written in SQL Server (T-SQL) syntax. For MySQL/Postgres, swap:
--   BIGINT IDENTITY(1,1)   -> BIGINT AUTO_INCREMENT (MySQL) / BIGSERIAL (Postgres)
--   DATETIME2              -> DATETIME(6) (MySQL) / TIMESTAMP (Postgres)
--   VARCHAR(MAX)           -> TEXT / LONGTEXT
--   SYSUTCDATETIME()       -> UTC_TIMESTAMP() (MySQL) / (NOW() AT TIME ZONE 'UTC') (Postgres)
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'etl_job_log')
BEGIN
    CREATE TABLE etl_job_log (
        log_id            BIGINT IDENTITY(1,1) PRIMARY KEY,
        job_id            VARCHAR(100)   NOT NULL,          -- unique run identifier (e.g. UUID or scheduler run id)
        job_name          VARCHAR(255)   NOT NULL,          -- pipeline / job name
        task_id           VARCHAR(100)   NULL,              -- NULL for job-level rows
        task_name         VARCHAR(255)   NULL,              -- e.g. "Extract_Sales_Data"
        log_level         VARCHAR(20)    NOT NULL,          -- 'JOB' or 'TASK'
        status            VARCHAR(20)    NOT NULL,          -- STARTED / RUNNING / SUCCESS / FAILED / WARNING
        start_time        DATETIME2      NOT NULL,
        end_time          DATETIME2      NULL,
        duration_seconds  DECIMAL(18,3)  NULL,
        rows_processed    BIGINT         NULL,
        failure_reason    VARCHAR(MAX)   NULL,              -- short error message
        error_traceback   VARCHAR(MAX)   NULL,              -- full python traceback, for deep re-investigation
        host_name         VARCHAR(255)   NULL,              -- server/container the job ran on
        log_file_path     VARCHAR(1000)  NULL,              -- path to the detailed .log file on disk
        created_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_etl_job_log_job_id     ON etl_job_log(job_id);
    CREATE INDEX IX_etl_job_log_status     ON etl_job_log(status);
    CREATE INDEX IX_etl_job_log_start_time ON etl_job_log(start_time);
END
GO

-- Example monitoring query: latest status per job
-- SELECT job_id, job_name, status, start_time, end_time, duration_seconds
-- FROM etl_job_log
-- WHERE log_level = 'JOB'
-- ORDER BY start_time DESC;

-- Example monitoring query: all failed tasks in the last 24 hours
-- SELECT job_id, task_id, task_name, failure_reason, start_time
-- FROM etl_job_log
-- WHERE log_level = 'TASK' AND status = 'FAILED'
--   AND start_time >= DATEADD(HOUR, -24, SYSUTCDATETIME())
-- ORDER BY start_time DESC;
