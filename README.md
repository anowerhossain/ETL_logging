# ETL Logging & Monitoring Framework

A lightweight, dependency-free (stdlib-only) Python framework for ETL job
observability: file-based logging for deep troubleshooting, structured
DB-based status tracking, and HTML email notifications (Outlook/Office365)
summarizing job/task results.

## Structure

```
etl_framework/
├── common_db.py                    # Shared SQLite persistence layer (swap for prod DB)
├── logging_module/
│   ├── __init__.py
│   └── etl_logger.py                # ETLLogger + TaskContext
├── notification_module/
│   ├── __init__.py
│   └── email_notifier.py            # EmailNotifier
├── config/
│   └── email_config.toml            # SMTP credentials & settings (edit this)
├── sql/
│   ├── ddl_logging.sql              # T-SQL DDL for etl_job_log
│   └── ddl_notification.sql         # T-SQL DDL for etl_notification_log
├── example_usage.py                 # Runnable end-to-end demo pipeline
├── test_framework.py                # unittest suite (mocked SMTP, temp SQLite)
├── requirements.txt
└── logs/                            # Rotating .log files land here
```

## Quick start

```bash
pip install -r requirements.txt      # only needed on Python < 3.11
python example_usage.py              # runs a demo pipeline (dry_run email)
python -m unittest test_framework.py -v
```

## How it works

### 1. Logging module (`logging_module/etl_logger.py`)

```python
from logging_module.etl_logger import ETLLogger

logger = ETLLogger(job_name="Daily_Sales_Load", db_url="sqlite:///etl_logs.db")
logger.start_job()

with logger.task(task_id="T1", task_name="Extract_Sales_Data") as t:
    rows = extract()
    t.rows_processed = len(rows)

# suppress_exceptions=True lets the pipeline continue and still report
# a failed task in the final notification, instead of crashing the run
with logger.task(task_id="T2", task_name="Load_Sales_Data", suppress_exceptions=True) as t:
    load(rows)

logger.end_job()
summary = logger.get_job_summary()   # -> feed straight into EmailNotifier
```

Every job run gets its own rotating log file under `logs/`, containing
timestamped messages and full tracebacks on failure — the artifact you go
back to for deep re-investigation. In parallel, a structured status row is
written to the `etl_job_log` table for every job start/end and task
start/end, so dashboards/alerts don't need to parse log files.

### 2. Notification module (`notification_module/email_notifier.py`)

```python
from notification_module.email_notifier import EmailNotifier

notifier = EmailNotifier(config_path="config/email_config.toml")
notifier.send_status_email(
    job_id=logger.job_id,
    job_name=logger.job_name,
    records=logger.get_job_summary(),
)
```

Builds a colour-coded HTML table (`Job_id`, `Task_id`, `Task_status`,
`Task_failure_reason`, plus any extra columns like `Rows_processed` found
in the records) and emails it via Outlook/Office365 SMTP, retrying on
transient failures. Every attempt (sent or failed) is itself audited into
`etl_notification_log` — so a silently-failing notification is a
monitorable condition too.

## Configuration (`config/email_config.toml`)

Fill in your Outlook/Office365 SMTP credentials (an **app password** is
recommended over your normal password when MFA is enabled). Set
`dry_run = true` to build/log emails without actually sending — useful for
testing pipelines end-to-end without a live mailbox.

## Database

The reference implementation persists to SQLite via the stdlib `sqlite3`
module (zero extra dependencies, fully testable). For production, run the
DDL scripts in `sql/` against SQL Server/PostgreSQL/MySQL, and swap the
three functions in `common_db.py` (`insert_job_log_row`,
`insert_notification_log_row`, `query_job_log`/`query_notification_log`)
for your DB-API2 driver of choice (`pyodbc`, `psycopg2`, etc.) — that file
is the single integration point both modules depend on.

### `etl_job_log` (sql/ddl_logging.sql)
One row per JOB start/end and per TASK start/end: `job_id`, `task_id`,
`status`, `start_time`, `end_time`, `duration_seconds`, `rows_processed`,
`failure_reason`, `error_traceback`, `log_file_path`, etc.

### `etl_notification_log` (sql/ddl_notification.sql)
One row per notification attempt: `job_id`, `recipients`, `subject`,
`overall_status`, `send_status`, `failure_reason`, `retry_count`, etc.

## Design principles applied

- Timestamped, rotating log files per job run — no overwriting, capped disk usage.
- Structured + unstructured logs cross-referenced via `log_file_path`.
- Exceptions logged with full traceback before propagating (or being
  intentionally suppressed via `suppress_exceptions=True`).
- Credentials isolated in TOML, never hard-coded or logged.
- Notification delivery itself is audited, so "job failed AND nobody was told" is detectable.
- Retry with backoff on transient SMTP failures.
- `dry_run` mode for safe testing.
