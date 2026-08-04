-- ============================================================================
-- ddl_notification.sql
-- ----------------------------------------------------------------------------
-- Table used by the Notification module (notification_module/email_notifier.py)
-- to audit every email notification attempt (sent AND failed-to-send).

-- ============================================================================

    CREATE TABLE IF NOT EXISTS catalog.etl_db.etl_notification_log (
    notification_id     STRING    NOT NULL COMMENT 'Unique ID for this notification attempt (generate as UUID in application code -- Iceberg has no auto-increment)',
    job_id               STRING    NOT NULL COMMENT 'FK-like reference to etl_job_log.job_id',
    job_name              STRING            COMMENT 'Human readable pipeline/job name',
    notification_type     STRING    NOT NULL COMMENT 'EMAIL (extendable to SLACK, TEAMS, etc.)',
    recipients             STRING    NOT NULL COMMENT 'Comma-separated To: list',
    cc                      STRING            COMMENT 'Comma-separated Cc: list',
    subject                 STRING    NOT NULL COMMENT 'Email subject line as sent',
    overall_status          STRING    NOT NULL COMMENT 'SUCCESS / FAILED / WARNING -- derived from job results',
    send_status              STRING    NOT NULL COMMENT 'SENT / FAILED -- did the email itself go out?',
    failure_reason            STRING           COMMENT 'SMTP error, if send_status = FAILED',
    retry_count                INT      NOT NULL COMMENT 'Number of SMTP send attempts made',
    sent_time                   TIMESTAMP        COMMENT 'UTC timestamp the email was successfully sent (NULL if never sent)',
    created_at                   TIMESTAMP NOT NULL COMMENT 'UTC timestamp the attempt was recorded (set by application code)'
)
USING iceberg
PARTITIONED BY (days(created_at))
COMMENT 'Audit log of every ETL notification (email) attempt, sent or failed'
TBLPROPERTIES (
    'write.format.default'   = 'parquet',
    'format-version'         = '2'
);
-- Example monitoring query: notifications that failed to send
-- (i.e. the team was NOT actually alerted about a failed job -- critical to catch!)
-- SELECT job_id, job_name, subject, failure_reason, retry_count, created_at
-- FROM etl_notification_log
-- WHERE send_status = 'FAILED'
-- ORDER BY created_at DESC;
