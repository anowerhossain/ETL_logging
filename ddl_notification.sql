-- ============================================================================
-- ddl_notification.sql
-- ----------------------------------------------------------------------------
-- Table used by the Notification module (notification_module/email_notifier.py)
-- to audit every email notification attempt (sent AND failed-to-send).
--
-- Written in SQL Server (T-SQL) syntax -- see ddl_logging.sql header for
-- MySQL/Postgres type substitutions.
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'etl_notification_log')
BEGIN
    CREATE TABLE etl_notification_log (
        notification_id    BIGINT IDENTITY(1,1) PRIMARY KEY,
        job_id              VARCHAR(100)   NOT NULL,        -- FK-like reference to etl_job_log.job_id
        job_name            VARCHAR(255)   NULL,
        notification_type   VARCHAR(50)    NOT NULL DEFAULT 'EMAIL',
        recipients           VARCHAR(1000)  NOT NULL,        -- comma-separated To: list
        cc                   VARCHAR(1000)  NULL,            -- comma-separated Cc: list
        subject              VARCHAR(500)   NOT NULL,
        overall_status       VARCHAR(20)    NOT NULL,        -- SUCCESS / FAILED / WARNING (derived from job results)
        send_status          VARCHAR(20)    NOT NULL,        -- SENT / FAILED (did the email itself go out?)
        failure_reason       VARCHAR(MAX)   NULL,            -- SMTP error, if send_status = 'FAILED'
        retry_count          INT            NOT NULL DEFAULT 0,
        sent_time            DATETIME2      NULL,
        created_at           DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_etl_notification_log_job_id ON etl_notification_log(job_id);
    CREATE INDEX IX_etl_notification_log_status ON etl_notification_log(send_status);
END
GO

-- Example monitoring query: notifications that failed to send
-- (i.e. the team was NOT actually alerted about a failed job -- critical to catch!)
-- SELECT job_id, job_name, subject, failure_reason, retry_count, created_at
-- FROM etl_notification_log
-- WHERE send_status = 'FAILED'
-- ORDER BY created_at DESC;
