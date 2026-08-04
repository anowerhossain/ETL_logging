"""
common_db.py
============
Shared database layer used by both the ``logging_module`` and the
``notification_module``.

Implementation notes
---------------------
This reference implementation uses Python's **built-in** ``sqlite3`` module
so the whole framework is runnable/testable with zero third-party
dependencies. The table schemas here are kept 1:1 with the production DDL
scripts shipped in ``sql/ddl_logging.sql`` and ``sql/ddl_notification.sql``
(written for SQL Server).

To point this framework at a real enterprise database (SQL Server,
PostgreSQL, MySQL) in production:
    1. Run the appropriate DDL script from ``sql/`` against that database.
    2. Replace ``get_connection`` / the two insert helper functions below
       with equivalent calls using your DB-API2 driver of choice
       (``pyodbc``, ``psycopg2``, ``mysql-connector-python``...). Because
       every write in this module goes through the small helper functions
       ``insert_job_log_row`` and ``insert_notification_log_row``, that is
       the *only* place you need to change -- ``etl_logger.py`` and
       ``email_notifier.py`` never talk to the DB directly.

Why a shared module?
---------------------
Both modules need to persist to the same two tables. Centralising the
connection + schema logic here guarantees both modules always agree on the
schema and avoids duplicated boilerplate.

Design notes / best practices applied
--------------------------------------
* One row per JOB and one row per TASK in the same ``etl_job_log`` table
  (``task_id IS NULL`` marks a job-level row), which keeps the schema simple
  while still supporting job-level and task-level SLA dashboards.
* All timestamps are stored in UTC (ISO-8601 strings) to avoid
  daylight-saving / timezone bugs when the ETL runs across regions.
* Every write uses a short-lived connection + explicit commit, which is
  simple and safe for the write volumes typical of ETL status logging.
"""

from __future__ import annotations

import datetime as _dt
import sqlite3
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Schema (kept structurally identical to sql/ddl_logging.sql and
# sql/ddl_notification.sql, translated to SQLite types)
# ---------------------------------------------------------------------------
_CREATE_JOB_LOG_TABLE = """
CREATE TABLE IF NOT EXISTS etl_job_log (
    log_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id            TEXT    NOT NULL,
    job_name          TEXT    NOT NULL,
    task_id           TEXT    NULL,
    task_name         TEXT    NULL,
    log_level         TEXT    NOT NULL,          -- 'JOB' or 'TASK'
    status            TEXT    NOT NULL,          -- STARTED/RUNNING/SUCCESS/FAILED/WARNING
    start_time        TEXT    NOT NULL,          -- ISO-8601 UTC
    end_time          TEXT    NULL,
    duration_seconds  REAL    NULL,
    rows_processed    INTEGER NULL,
    failure_reason    TEXT    NULL,
    error_traceback   TEXT    NULL,
    host_name         TEXT    NULL,
    log_file_path     TEXT    NULL,
    created_at        TEXT    NOT NULL
);
"""

_CREATE_JOB_LOG_INDEXES = [
    "CREATE INDEX IF NOT EXISTS IX_etl_job_log_job_id ON etl_job_log(job_id);",
    "CREATE INDEX IF NOT EXISTS IX_etl_job_log_status ON etl_job_log(status);",
]

_CREATE_NOTIFICATION_LOG_TABLE = """
CREATE TABLE IF NOT EXISTS etl_notification_log (
    notification_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id             TEXT    NOT NULL,
    job_name           TEXT    NULL,
    notification_type  TEXT    NOT NULL DEFAULT 'EMAIL',
    recipients          TEXT    NOT NULL,
    cc                  TEXT    NULL,
    subject             TEXT    NOT NULL,
    overall_status       TEXT    NOT NULL,        -- SUCCESS/FAILED/WARNING
    send_status          TEXT    NOT NULL,        -- SENT/FAILED
    failure_reason       TEXT    NULL,
    retry_count           INTEGER NOT NULL DEFAULT 0,
    sent_time             TEXT    NULL,
    created_at            TEXT    NOT NULL
);
"""

_CREATE_NOTIFICATION_LOG_INDEXES = [
    "CREATE INDEX IF NOT EXISTS IX_etl_notification_log_job_id ON etl_notification_log(job_id);",
]


def _resolve_sqlite_path(db_url: str) -> str:
    """Accept either a plain file path or a ``sqlite:///path`` URL."""
    prefix = "sqlite:///"
    return db_url[len(prefix):] if db_url.startswith(prefix) else db_url


def get_connection(db_url: str) -> sqlite3.Connection:
    """
    Open a SQLite connection and ensure both framework tables (and their
    indexes) exist.

    Parameters
    ----------
    db_url : str
        Either a plain SQLite file path (``"etl_logs.db"``) or a
        ``"sqlite:///etl_logs.db"`` style URL (matching the format used in
        ``config/email_config.toml``'s ``[database] db_url`` setting).

    Returns
    -------
    sqlite3.Connection
    """
    path = _resolve_sqlite_path(db_url)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(_CREATE_JOB_LOG_TABLE)
    for stmt in _CREATE_JOB_LOG_INDEXES:
        conn.execute(stmt)
    conn.execute(_CREATE_NOTIFICATION_LOG_TABLE)
    for stmt in _CREATE_NOTIFICATION_LOG_INDEXES:
        conn.execute(stmt)
    conn.commit()
    return conn


def insert_job_log_row(db_url: str, **fields: Any) -> None:
    """
    Insert one row into ``etl_job_log``.

    Accepted keyword fields mirror the table columns: ``job_id``,
    ``job_name``, ``task_id``, ``task_name``, ``log_level``, ``status``,
    ``start_time`` (datetime), ``end_time`` (datetime or None),
    ``duration_seconds``, ``rows_processed``, ``failure_reason``,
    ``error_traceback``, ``host_name``, ``log_file_path``.
    """
    conn = get_connection(db_url)
    try:
        conn.execute(
            """
            INSERT INTO etl_job_log (
                job_id, job_name, task_id, task_name, log_level, status,
                start_time, end_time, duration_seconds, rows_processed,
                failure_reason, error_traceback, host_name, log_file_path,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fields.get("job_id"),
                fields.get("job_name"),
                fields.get("task_id"),
                fields.get("task_name"),
                fields.get("log_level"),
                fields.get("status"),
                _to_iso(fields.get("start_time")),
                _to_iso(fields.get("end_time")),
                fields.get("duration_seconds"),
                fields.get("rows_processed"),
                fields.get("failure_reason"),
                fields.get("error_traceback"),
                fields.get("host_name"),
                fields.get("log_file_path"),
                _to_iso(_dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def insert_notification_log_row(db_url: str, **fields: Any) -> None:
    """
    Insert one row into ``etl_notification_log``.

    Accepted keyword fields mirror the table columns: ``job_id``,
    ``job_name``, ``notification_type``, ``recipients``, ``cc``,
    ``subject``, ``overall_status``, ``send_status``, ``failure_reason``,
    ``retry_count``, ``sent_time`` (datetime or None).
    """
    conn = get_connection(db_url)
    try:
        conn.execute(
            """
            INSERT INTO etl_notification_log (
                job_id, job_name, notification_type, recipients, cc,
                subject, overall_status, send_status, failure_reason,
                retry_count, sent_time, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fields.get("job_id"),
                fields.get("job_name"),
                fields.get("notification_type", "EMAIL"),
                fields.get("recipients"),
                fields.get("cc"),
                fields.get("subject"),
                fields.get("overall_status"),
                fields.get("send_status"),
                fields.get("failure_reason"),
                fields.get("retry_count", 0),
                _to_iso(fields.get("sent_time")),
                _to_iso(_dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def query_job_log(db_url: str, job_id: str) -> List[Dict[str, Any]]:
    """Return every ``etl_job_log`` row (job- and task-level) for a job_id."""
    conn = get_connection(db_url)
    try:
        cursor = conn.execute("SELECT * FROM etl_job_log WHERE job_id = ?", (job_id,))
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def query_notification_log(db_url: str, job_id: str) -> List[Dict[str, Any]]:
    """Return every ``etl_notification_log`` row for a job_id."""
    conn = get_connection(db_url)
    try:
        cursor = conn.execute(
            "SELECT * FROM etl_notification_log WHERE job_id = ?", (job_id,)
        )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def _to_iso(value: Optional[_dt.datetime]) -> Optional[str]:
    """Convert a datetime to an ISO-8601 string; pass through None."""
    if value is None:
        return None
    return value.isoformat(sep=" ", timespec="milliseconds")
