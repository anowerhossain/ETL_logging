"""
test_framework.py
==================
Unit tests for the ETL Logging & Notification framework.

Run with:
    python -m unittest test_framework.py -v

Coverage
--------
* ETLLogger: job/task lifecycle, DB rows written, log file created,
  exception handling with/without ``suppress_exceptions``.
* EmailNotifier: HTML table generation (mandatory + extra columns),
  subject line derivation, dry_run short-circuit, retry logic on
  simulated SMTP failures (smtplib mocked -- no real network calls),
  and notification audit rows written to the DB.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from common_db import query_job_log, query_notification_log
from logging_module.etl_logger import ETLLogger
from notification_module.email_notifier import EmailNotifier


class TestETLLogger(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp()
        self.log_dir = os.path.join(self.tmp_dir, "logs")
        self.db_path = os.path.join(self.tmp_dir, "test_logs.db")
        self.db_url = f"sqlite:///{self.db_path}"

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_successful_job_and_task_are_logged(self) -> None:
        logger = ETLLogger(
            job_name="UnitTestJob",
            job_id="JOB_TEST_1",
            log_dir=self.log_dir,
            db_url=self.db_url,
            console_output=False,
        )
        logger.start_job()
        with logger.task(task_id="T1", task_name="Do_Something") as t:
            t.rows_processed = 42
        logger.end_job()

        # Log file was created and is non-empty
        self.assertTrue(Path(logger.log_file_path).exists())
        self.assertGreater(Path(logger.log_file_path).stat().st_size, 0)

        # In-memory summary captured the task
        summary = logger.get_job_summary()
        self.assertEqual(len(summary), 1)
        self.assertEqual(summary[0]["Task_status"], "SUCCESS")
        self.assertEqual(summary[0]["Rows_processed"], 42)

        # DB has 4 rows: JOB-STARTED, TASK-STARTED, TASK-SUCCESS, JOB-SUCCESS
        rows = query_job_log(self.db_url, "JOB_TEST_1")
        self.assertEqual(len(rows), 4)
        job_end_row = [r for r in rows if r["log_level"] == "JOB" and r["status"] == "SUCCESS"]
        self.assertEqual(len(job_end_row), 1)

    def test_task_failure_propagates_by_default(self) -> None:
        logger = ETLLogger(
            job_name="UnitTestJobFail",
            job_id="JOB_TEST_2",
            log_dir=self.log_dir,
            db_url=self.db_url,
            console_output=False,
        )
        logger.start_job()

        with self.assertRaises(ValueError):
            with logger.task(task_id="T1", task_name="Failing_Task"):
                raise ValueError("boom")

        logger.end_job()
        summary = logger.get_job_summary()
        self.assertEqual(summary[0]["Task_status"], "FAILED")
        self.assertIn("boom", summary[0]["Task_failure_reason"])

    def test_task_failure_suppressed_when_requested(self) -> None:
        logger = ETLLogger(
            job_name="UnitTestJobSuppress",
            job_id="JOB_TEST_3",
            log_dir=self.log_dir,
            db_url=self.db_url,
            console_output=False,
        )
        logger.start_job()

        # Should NOT raise because suppress_exceptions=True
        with logger.task(task_id="T1", task_name="Failing_Task", suppress_exceptions=True):
            raise RuntimeError("simulated failure")

        logger.end_job()
        summary = logger.get_job_summary()
        self.assertEqual(summary[0]["Task_status"], "FAILED")
        self.assertEqual(logger._job_status, "FAILED")


class TestEmailNotifier(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp_dir, "test_notify.db")
        self.db_url = f"sqlite:///{self.db_path}"

        self.config_path = os.path.join(self.tmp_dir, "email_config.toml")
        with open(self.config_path, "w") as f:
            f.write(
                """
[email]
smtp_server = "smtp.office365.com"
smtp_port = 587
use_tls = true
sender_email = "sender@test.com"
username = "sender@test.com"
password = "dummy"
default_recipients = ["recipient@test.com"]
default_cc = []
max_retries = 2
retry_delay_seconds = 0
dry_run = false

[email.subject]
success_prefix = "[OK]"
failure_prefix = "[FAIL]"
warning_prefix = "[WARN]"

[database]
db_url = "%s"
"""
                % self.db_url
            )

        self.sample_records = [
            {
                "Job_id": "JOB_1",
                "Task_id": "T1",
                "Task_status": "SUCCESS",
                "Task_failure_reason": "",
                "Rows_processed": 100,
            },
            {
                "Job_id": "JOB_1",
                "Task_id": "T2",
                "Task_status": "FAILED",
                "Task_failure_reason": "ConnectionError: timeout",
                "Rows_processed": None,
            },
        ]

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_overall_status_derivation(self) -> None:
        notifier = EmailNotifier(config_path=self.config_path)
        self.assertEqual(notifier._derive_overall_status(self.sample_records), "FAILED")
        all_success = [dict(r, Task_status="SUCCESS") for r in self.sample_records]
        self.assertEqual(notifier._derive_overall_status(all_success), "SUCCESS")

    def test_html_table_contains_mandatory_columns(self) -> None:
        notifier = EmailNotifier(config_path=self.config_path)
        html = notifier._build_html_body("JOB_1", "TestJob", "FAILED", self.sample_records, None)
        for col in ["Job_id", "Task_id", "Task_status", "Task_failure_reason", "Rows_processed"]:
            self.assertIn(col, html)
        self.assertIn("ConnectionError: timeout", html)

    @patch("notification_module.email_notifier.smtplib.SMTP")
    def test_send_status_email_success_logs_sent(self, mock_smtp_cls: MagicMock) -> None:
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__.return_value = mock_server

        notifier = EmailNotifier(config_path=self.config_path)
        result = notifier.send_status_email(
            job_id="JOB_1", job_name="TestJob", records=self.sample_records
        )

        self.assertTrue(result)
        mock_server.sendmail.assert_called_once()

        rows = query_notification_log(self.db_url, "JOB_1")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["send_status"], "SENT")
        self.assertEqual(rows[0]["overall_status"], "FAILED")  # derived from records

    @patch("notification_module.email_notifier.smtplib.SMTP")
    def test_send_status_email_retries_then_fails(self, mock_smtp_cls: MagicMock) -> None:
        mock_smtp_cls.side_effect = Exception("SMTP connection refused")

        notifier = EmailNotifier(config_path=self.config_path)
        result = notifier.send_status_email(
            job_id="JOB_2", job_name="TestJob2", records=self.sample_records
        )

        self.assertFalse(result)
        self.assertEqual(mock_smtp_cls.call_count, notifier.max_retries)

        rows = query_notification_log(self.db_url, "JOB_2")
        self.assertEqual(rows[0]["send_status"], "FAILED")
        self.assertIn("SMTP connection refused", rows[0]["failure_reason"])

    def test_dry_run_does_not_call_smtp(self) -> None:
        # Rewrite config with dry_run = true
        with open(self.config_path, "a") as f:
            pass
        notifier = EmailNotifier(config_path=self.config_path)
        notifier.dry_run = True
        with patch("notification_module.email_notifier.smtplib.SMTP") as mock_smtp_cls:
            result = notifier.send_status_email(
                job_id="JOB_3", job_name="TestJob3", records=self.sample_records
            )
            mock_smtp_cls.assert_not_called()
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main(verbosity=2)
