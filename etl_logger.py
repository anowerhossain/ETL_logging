"""
etl_logger.py
=============
Core ETL Logging module.

Responsibilities
-----------------
1. **File logging** – every job writes a dedicated, rotating log file to disk
   (default ``logs/<job_name>_<job_id>_<timestamp>.log``) containing
   timestamped INFO/WARNING/ERROR/DEBUG messages plus full stack traces on
   failure. These files are the primary artifact used for deep-dive
   re-investigation of a failed run.
2. **Structured DB logging** – job-level and task-level start/end/status
   events are also written to the ``etl_job_log`` table (see
   ``sql/ddl_logging.sql``) so that dashboards, SLA reports and the
   Notification module can query status without parsing log files.
3. **Task context manager** – wraps each ETL step (extract/transform/load)
   so start time, end time, duration, status and failure reason are always
   captured consistently, even when the step raises an exception.

Best practices encoded in this module
--------------------------------------
* Every job run gets a unique, timestamped log file -> no log overwriting,
  full history retained for audits.
* Rotating file handler caps individual file size so long-running/looping
  jobs cannot fill up disk.
* Structured (DB) + unstructured (file) logging are kept in sync: the
  ``log_file_path`` is stored on the DB row so anyone looking at a
  dashboard can jump straight to the raw log file.
* Exceptions are logged with full traceback (``exc_info=True``) before
  being re-raised, so nothing is ever silently swallowed unless the caller
  explicitly opts in via ``suppress_exceptions=True``.
* All timestamps are stored in UTC.

Example
-------
    from logging_module.etl_logger import ETLLogger

    logger = ETLLogger(job_id="JOB_100", job_name="Daily_Sales_Load")
    logger.start_job()

    with logger.task(task_id="T1", task_name="Extract_Sales_Data") as task:
        rows = extract_sales()
        task.rows_processed = len(rows)

    logger.end_job()
    summary = logger.get_job_summary()   # feed this into the notifier
"""

from __future__ import annotations

import datetime as _dt
import logging
import socket
import traceback
import uuid
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common_db import insert_job_log_row, query_job_log  # noqa: E402


class TaskContext:
    """
    Context manager representing a single ETL task (a step within a job,
    e.g. "Extract", "Transform", "Load").

    Instances are created via :meth:`ETLLogger.task` and should not be
    constructed directly.

    Attributes
    ----------
    task_id : str
        Caller-supplied identifier for the task (unique within the job).
    task_name : str
        Human readable task name.
    rows_processed : Optional[int]
        Set this from within the ``with`` block to record how many rows
        the task handled; it will be persisted on task completion.
    status : str
        Final status of the task: ``SUCCESS``, ``FAILED`` or ``WARNING``.
    failure_reason : Optional[str]
        Short human-readable failure reason (populated automatically on
        exception, or settable manually for a ``WARNING`` status).
    """

    def __init__(
        self,
        owner: "ETLLogger",
        task_id: str,
        task_name: str,
        suppress_exceptions: bool = False,
    ) -> None:
        self._owner = owner
        self.task_id = task_id
        self.task_name = task_name
        self.suppress_exceptions = suppress_exceptions

        self.rows_processed: Optional[int] = None
        self.status: str = "RUNNING"
        self.failure_reason: Optional[str] = None
        self.start_time: Optional[_dt.datetime] = None
        self.end_time: Optional[_dt.datetime] = None

    def __enter__(self) -> "TaskContext":
        self.start_time = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
        self._owner._logger.info(
            "TASK START | task_id=%s | task_name=%s", self.task_id, self.task_name
        )
        self._owner._write_db_row(
            task_id=self.task_id,
            task_name=self.task_name,
            log_level="TASK",
            status="STARTED",
            start_time=self.start_time,
            end_time=None,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.end_time = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
        duration = (self.end_time - self.start_time).total_seconds()

        if exc_type is not None:
            self.status = "FAILED"
            self.failure_reason = f"{exc_type.__name__}: {exc_val}"
            tb_text = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
            self._owner._logger.error(
                "TASK FAILED | task_id=%s | task_name=%s | reason=%s",
                self.task_id,
                self.task_name,
                self.failure_reason,
                exc_info=True,
            )
        else:
            if self.status == "RUNNING":
                self.status = "SUCCESS"
            tb_text = None
            self._owner._logger.info(
                "TASK END | task_id=%s | task_name=%s | status=%s | rows_processed=%s | duration_s=%.3f",
                self.task_id,
                self.task_name,
                self.status,
                self.rows_processed,
                duration,
            )

        self._owner._write_db_row(
            task_id=self.task_id,
            task_name=self.task_name,
            log_level="TASK",
            status=self.status,
            start_time=self.start_time,
            end_time=self.end_time,
            duration_seconds=duration,
            rows_processed=self.rows_processed,
            failure_reason=self.failure_reason,
            error_traceback=tb_text,
        )

        # Track this task on the parent job for the final summary/notification
        self._owner._task_results.append(
            {
                "Job_id": self._owner.job_id,
                "Task_id": self.task_id,
                "Task_name": self.task_name,
                "Task_status": self.status,
                "Task_failure_reason": self.failure_reason or "",
                "Rows_processed": self.rows_processed,
                "Duration_seconds": round(duration, 3),
            }
        )

        # Returning True suppresses the exception; False lets it propagate.
        return bool(exc_type is not None and self.suppress_exceptions)


class ETLLogger:
    """
    Main entry point for ETL job/task logging.

    Parameters
    ----------
    job_id : str
        Unique identifier for this job run. If omitted, a UUID4 is
        generated automatically.
    job_name : str
        Human readable job/pipeline name (used in the log filename).
    log_dir : str
        Directory where rotating log files are written. Created if it does
        not already exist.
    db_url : str
        SQLAlchemy connection string for structured status logging.
    console_output : bool
        If True (default), log messages are also streamed to stdout, which
        is convenient when running interactively or in an orchestrator
        (Airflow/ADF/cron) that captures stdout.
    max_bytes : int
        Rotate the log file once it exceeds this size (default 5 MB).
    backup_count : int
        Number of rotated backup log files to retain.
    """

    def __init__(
        self,
        job_name: str,
        job_id: Optional[str] = None,
        log_dir: str = "logs",
        db_url: str = "sqlite:///etl_logs.db",
        console_output: bool = True,
        max_bytes: int = 5 * 1024 * 1024,
        backup_count: int = 5,
    ) -> None:
        self.job_id = job_id or str(uuid.uuid4())
        self.job_name = job_name
        self.host_name = socket.gethostname()
        self._task_results: List[Dict[str, Any]] = []
        self._job_start_time: Optional[_dt.datetime] = None
        self._job_end_time: Optional[_dt.datetime] = None
        self._job_status: str = "STARTED"

        # ---- file logging setup -------------------------------------------------
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        timestamp = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None).strftime("%Y%m%dT%H%M%SZ")
        self.log_file_path = str(log_path / f"{job_name}_{self.job_id}_{timestamp}.log")

        self._logger = logging.getLogger(f"etl.{job_name}.{self.job_id}")
        self._logger.setLevel(logging.DEBUG)
        self._logger.propagate = False
        # Avoid duplicate handlers if the same logger name is reused (e.g. tests)
        self._logger.handlers.clear()

        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        file_handler = RotatingFileHandler(
            self.log_file_path, maxBytes=max_bytes, backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        self._logger.addHandler(file_handler)

        if console_output:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            self._logger.addHandler(stream_handler)

        # ---- DB logging setup -----------------------------------------------------
        self.db_url = db_url

    # ------------------------------------------------------------------ #
    # Job lifecycle
    # ------------------------------------------------------------------ #
    def start_job(self) -> None:
        """Mark the job as started: writes to both the log file and DB."""
        self._job_start_time = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
        self._logger.info(
            "JOB START | job_id=%s | job_name=%s | host=%s",
            self.job_id,
            self.job_name,
            self.host_name,
        )
        self._write_db_row(
            task_id=None,
            task_name=None,
            log_level="JOB",
            status="STARTED",
            start_time=self._job_start_time,
            end_time=None,
        )

    def end_job(self, status: Optional[str] = None) -> None:
        """
        Mark the job as finished.

        Parameters
        ----------
        status : Optional[str]
            Explicit overall status. If omitted, it is derived automatically:
            ``FAILED`` if any task failed, otherwise ``SUCCESS``.
        """
        self._job_end_time = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
        duration = (
            (self._job_end_time - self._job_start_time).total_seconds()
            if self._job_start_time
            else None
        )

        if status is None:
            has_failure = any(t["Task_status"] == "FAILED" for t in self._task_results)
            status = "FAILED" if has_failure else "SUCCESS"
        self._job_status = status

        self._logger.info(
            "JOB END | job_id=%s | job_name=%s | status=%s | duration_s=%s",
            self.job_id,
            self.job_name,
            status,
            f"{duration:.3f}" if duration is not None else "N/A",
        )
        self._write_db_row(
            task_id=None,
            task_name=None,
            log_level="JOB",
            status=status,
            start_time=self._job_start_time,
            end_time=self._job_end_time,
            duration_seconds=duration,
        )

    # ------------------------------------------------------------------ #
    # Task tracking
    # ------------------------------------------------------------------ #
    def task(
        self, task_id: str, task_name: str, suppress_exceptions: bool = False
    ) -> TaskContext:
        """
        Create a task context manager to track a single ETL step.

        Parameters
        ----------
        task_id : str
            Unique identifier for the task within this job.
        task_name : str
            Human readable task name.
        suppress_exceptions : bool
            If True, an exception raised inside the ``with`` block is
            logged and recorded as FAILED but NOT re-raised, allowing the
            pipeline to continue with subsequent tasks (useful when you
            want a full run summary/notification even after partial
            failure). Defaults to False (exceptions propagate normally).

        Returns
        -------
        TaskContext
        """
        return TaskContext(self, task_id, task_name, suppress_exceptions)

    # ------------------------------------------------------------------ #
    # Plain message logging passthroughs (for ad-hoc messages outside tasks)
    # ------------------------------------------------------------------ #
    def info(self, msg: str, *args: Any) -> None:
        """Log an INFO-level message to the job's log file."""
        self._logger.info(msg, *args)

    def warning(self, msg: str, *args: Any) -> None:
        """Log a WARNING-level message to the job's log file."""
        self._logger.warning(msg, *args)

    def error(self, msg: str, *args: Any, exc_info: bool = False) -> None:
        """Log an ERROR-level message to the job's log file."""
        self._logger.error(msg, *args, exc_info=exc_info)

    def debug(self, msg: str, *args: Any) -> None:
        """Log a DEBUG-level message to the job's log file."""
        self._logger.debug(msg, *args)

    # ------------------------------------------------------------------ #
    # Summary for the Notification module
    # ------------------------------------------------------------------ #
    def get_job_summary(self) -> List[Dict[str, Any]]:
        """
        Return the list of task result dicts collected during this run.

        Each dict has the keys: ``Job_id``, ``Task_id``, ``Task_name``,
        ``Task_status``, ``Task_failure_reason``, ``Rows_processed``,
        ``Duration_seconds`` — ready to be passed straight into
        ``EmailNotifier.send_status_email(records=...)``.
        """
        return self._task_results

    def get_job_status_from_db(self, job_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Re-query the DB for every log row (job + task level) belonging to a
        job. Useful when the notification step runs in a separate process
        from the ETL step and therefore cannot rely on in-memory state.

        Parameters
        ----------
        job_id : Optional[str]
            Defaults to this logger's own ``job_id``.
        """
        job_id = job_id or self.job_id
        return query_job_log(self.db_url, job_id)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _write_db_row(
        self,
        task_id: Optional[str],
        task_name: Optional[str],
        log_level: str,
        status: str,
        start_time: Optional[_dt.datetime],
        end_time: Optional[_dt.datetime],
        duration_seconds: Optional[float] = None,
        rows_processed: Optional[int] = None,
        failure_reason: Optional[str] = None,
        error_traceback: Optional[str] = None,
    ) -> None:
        """Insert one row into ``etl_job_log``. Never raises: a DB hiccup
        should not take down the ETL job itself, but it IS logged to the
        file handler so it's visible during re-investigation."""
        try:
            insert_job_log_row(
                self.db_url,
                job_id=self.job_id,
                job_name=self.job_name,
                task_id=task_id,
                task_name=task_name,
                log_level=log_level,
                status=status,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration_seconds,
                rows_processed=rows_processed,
                failure_reason=failure_reason,
                error_traceback=error_traceback,
                host_name=self.host_name,
                log_file_path=self.log_file_path,
            )
        except Exception:  # noqa: BLE001 - deliberate broad catch, see docstring
            self._logger.error("Failed to write status row to DB", exc_info=True)
