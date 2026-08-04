"""
email_notifier.py
==================
Email Notification module for ETL pipelines.

Responsibilities
-----------------
1. Load SMTP/Outlook credentials and default settings from a TOML file
   (never hard-code credentials in source code).
2. Build an HTML email whose body contains a table of job/task results
   (``Job_id``, ``Task_id``, ``Task_status``, ``Task_failure_reason`` and
   any extra columns supplied), colour-coded by status for quick visual
   triage.
3. Send the email via SMTP (Outlook / Office365) with automatic retry on
   transient failures.
4. Record every notification attempt (sent or failed) to the
   ``etl_notification_log`` table (see ``sql/ddl_notification.sql``) for
   auditing — "did the on-call engineer actually get notified?".

Best practices encoded in this module
--------------------------------------
* Credentials are never logged, printed or stored anywhere except the TOML
  file supplied by the caller.
* Retry with backoff on transient SMTP errors, capped by ``max_retries``.
* ``dry_run`` mode lets you unit-test/build the HTML body without actually
  sending mail or requiring a real mailbox.
* Every attempt (success or failure) is persisted, so you can alert on
  "notification failed to send" as its own monitored condition.
* Subject line auto-prefixed with SUCCESS/FAILED/WARNING derived from the
  records, so recipients can triage from the inbox subject line alone.

Example
-------
    from notification_module.email_notifier import EmailNotifier

    notifier = EmailNotifier(config_path="config/email_config.toml")
    notifier.send_status_email(
        job_id="JOB_100",
        job_name="Daily_Sales_Load",
        records=logger.get_job_summary(),
    )
"""

from __future__ import annotations

import datetime as _dt
import os
import smtplib
import sys
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common_db import insert_notification_log_row  # noqa: E402

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]  # pip install tomli for <3.11


_STATUS_COLORS = {
    "SUCCESS": "#28a745",   # green
    "FAILED": "#dc3545",    # red
    "WARNING": "#ffc107",   # amber
    "STARTED": "#6c757d",   # grey
    "RUNNING": "#6c757d",
}


class EmailNotifier:
    """
    Builds and sends HTML status-table emails, and logs each attempt to the
    database for audit purposes.

    Parameters
    ----------
    config_path : str
        Path to the TOML file holding SMTP credentials/settings (see
        ``config/email_config.toml`` for the expected schema).
    db_url : Optional[str]
        SQLAlchemy connection string. If omitted, the ``db_url`` under the
        ``[database]`` section of the TOML config is used.
    """

    def __init__(self, config_path: str = "config/email_config.toml", db_url: Optional[str] = None) -> None:
        self.config_path = config_path
        self._config = self._load_config(config_path)

        email_cfg = self._config["email"]
        self.smtp_server: str = email_cfg["smtp_server"]
        self.smtp_port: int = int(email_cfg["smtp_port"])
        self.use_tls: bool = bool(email_cfg.get("use_tls", True))
        self.sender_email: str = email_cfg["sender_email"]
        self.username: str = email_cfg["username"]
        self.password: str = email_cfg["password"]
        self.default_recipients: List[str] = list(email_cfg.get("default_recipients", []))
        self.default_cc: List[str] = list(email_cfg.get("default_cc", []))
        self.max_retries: int = int(email_cfg.get("max_retries", 3))
        self.retry_delay_seconds: int = int(email_cfg.get("retry_delay_seconds", 5))
        self.dry_run: bool = bool(email_cfg.get("dry_run", False))

        subj_cfg = email_cfg.get("subject", {})
        self.success_prefix = subj_cfg.get("success_prefix", "[SUCCESS]")
        self.failure_prefix = subj_cfg.get("failure_prefix", "[FAILED]")
        self.warning_prefix = subj_cfg.get("warning_prefix", "[WARNING]")

        self.db_url = db_url or self._config.get("database", {}).get("db_url", "sqlite:///etl_logs.db")

    # ------------------------------------------------------------------ #
    # Config loading
    # ------------------------------------------------------------------ #
    @staticmethod
    def _load_config(config_path: str) -> Dict[str, Any]:
        """Load and parse the TOML credentials file."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(
                f"Email config TOML not found at '{config_path}'. "
                "Copy config/email_config.toml and fill in your credentials."
            )
        with open(path, "rb") as f:
            return tomllib.load(f)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def send_status_email(
        self,
        job_id: str,
        job_name: str,
        records: List[Dict[str, Any]],
        subject: Optional[str] = None,
        recipients: Optional[List[str]] = None,
        cc: Optional[List[str]] = None,
        extra_columns: Optional[List[str]] = None,
    ) -> bool:
        """
        Build and send the ETL status notification email.

        Parameters
        ----------
        job_id : str
            The job identifier this notification is about.
        job_name : str
            Human readable job name, used in the subject line.
        records : List[Dict[str, Any]]
            One dict per task, expected to contain at minimum
            ``Job_id``, ``Task_id``, ``Task_status``,
            ``Task_failure_reason``. Extra keys (e.g. ``Rows_processed``,
            ``Duration_seconds``) are rendered as extra table columns
            automatically unless ``extra_columns`` narrows the selection.
        subject : Optional[str]
            Custom subject line. If omitted, one is auto-generated as
            ``"<prefix> <job_name> (<job_id>) - <timestamp>"``.
        recipients : Optional[List[str]]
            Overrides ``default_recipients`` from the TOML config.
        cc : Optional[List[str]]
            Overrides ``default_cc`` from the TOML config.
        extra_columns : Optional[List[str]]
            Explicit list of extra column names (beyond the four
            mandatory ones) to include in the table, in order.

        Returns
        -------
        bool
            True if the email was sent successfully (or dry_run), False if
            all retry attempts failed.
        """
        recipients = recipients or self.default_recipients
        cc = cc or self.default_cc
        if not recipients:
            raise ValueError("No recipients configured or supplied for the notification.")

        overall_status = self._derive_overall_status(records)
        subject = subject or self._build_subject(job_name, job_id, overall_status)
        html_body = self._build_html_body(job_id, job_name, overall_status, records, extra_columns)

        success, failure_reason, attempts = self._send_with_retries(
            subject=subject, html_body=html_body, recipients=recipients, cc=cc
        )

        self._log_notification_attempt(
            job_id=job_id,
            job_name=job_name,
            recipients=recipients,
            cc=cc,
            subject=subject,
            overall_status=overall_status,
            send_status="SENT" if success else "FAILED",
            failure_reason=failure_reason,
            retry_count=attempts,
        )
        return success

    # ------------------------------------------------------------------ #
    # HTML building
    # ------------------------------------------------------------------ #
    @staticmethod
    def _derive_overall_status(records: List[Dict[str, Any]]) -> str:
        """SUCCESS unless any record failed; WARNING if any warned."""
        statuses = {str(r.get("Task_status", "")).upper() for r in records}
        if "FAILED" in statuses:
            return "FAILED"
        if "WARNING" in statuses:
            return "WARNING"
        return "SUCCESS"

    def _build_subject(self, job_name: str, job_id: str, overall_status: str) -> str:
        prefix = {
            "SUCCESS": self.success_prefix,
            "FAILED": self.failure_prefix,
            "WARNING": self.warning_prefix,
        }.get(overall_status, "")
        timestamp = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M UTC")
        return f"{prefix} {job_name} ({job_id}) - {timestamp}"

    def _build_html_body(
        self,
        job_id: str,
        job_name: str,
        overall_status: str,
        records: List[Dict[str, Any]],
        extra_columns: Optional[List[str]],
    ) -> str:
        """Render the records as a styled HTML table for the email body."""
        mandatory_cols = ["Job_id", "Task_id", "Task_status", "Task_failure_reason"]

        if extra_columns is not None:
            columns = mandatory_cols + [c for c in extra_columns if c not in mandatory_cols]
        else:
            # auto-detect any extra keys present across all records, preserving
            # first-seen order, e.g. Rows_processed / Duration_seconds
            seen: List[str] = []
            for r in records:
                for k in r.keys():
                    if k not in mandatory_cols and k not in seen and k != "Task_name":
                        seen.append(k)
            columns = mandatory_cols + seen

        header_html = "".join(f"<th style='{self._th_style()}'>{col}</th>" for col in columns)

        rows_html = ""
        for r in records:
            status = str(r.get("Task_status", "")).upper()
            color = _STATUS_COLORS.get(status, "#000000")
            cells = ""
            for col in columns:
                value = r.get(col, "")
                cell_style = self._td_style()
                if col == "Task_status":
                    cell_style += f"color:{color}; font-weight:bold;"
                cells += f"<td style='{cell_style}'>{value}</td>"
            rows_html += f"<tr>{cells}</tr>"

        overall_color = _STATUS_COLORS.get(overall_status, "#000000")

        html = f"""
        <html>
        <body style="font-family:Segoe UI, Arial, sans-serif; font-size:13px; color:#212529;">
            <p>Hello Team,</p>
            <p>
                ETL Job <b>{job_name}</b> (Job ID: <b>{job_id}</b>) completed with overall status:
                <b style="color:{overall_color};">{overall_status}</b>.
            </p>
            <table style="border-collapse:collapse; width:100%; margin-top:10px;">
                <thead><tr>{header_html}</tr></thead>
                <tbody>{rows_html}</tbody>
            </table>
            <p style="margin-top:16px; color:#6c757d; font-size:11px;">
                This is an automated notification generated at
                {_dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S UTC')}.
                For full technical details, refer to the job log file recorded
                in the ETL logging database (etl_job_log.log_file_path).
            </p>
        </body>
        </html>
        """
        return html

    @staticmethod
    def _th_style() -> str:
        return (
            "border:1px solid #dee2e6; padding:8px; background-color:#343a40; "
            "color:#ffffff; text-align:left;"
        )

    @staticmethod
    def _td_style() -> str:
        return "border:1px solid #dee2e6; padding:8px; text-align:left;"

    # ------------------------------------------------------------------ #
    # SMTP sending with retries
    # ------------------------------------------------------------------ #
    def _send_with_retries(
        self, subject: str, html_body: str, recipients: List[str], cc: List[str]
    ) -> tuple[bool, Optional[str], int]:
        """
        Attempt to send the email, retrying on failure up to
        ``self.max_retries`` times.

        Returns
        -------
        (success, failure_reason, attempts_made)
        """
        if self.dry_run:
            return True, None, 0

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.sender_email
        msg["To"] = ", ".join(recipients)
        if cc:
            msg["Cc"] = ", ".join(cc)
        msg.attach(MIMEText(html_body, "html"))

        all_recipients = recipients + cc
        last_error: Optional[str] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                with smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=30) as server:
                    if self.use_tls:
                        server.starttls()
                    server.login(self.username, self.password)
                    server.sendmail(self.sender_email, all_recipients, msg.as_string())
                return True, None, attempt
            except Exception as exc:  # noqa: BLE001 - want to capture & retry any SMTP error
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay_seconds)

        return False, last_error, self.max_retries

    # ------------------------------------------------------------------ #
    # Audit logging
    # ------------------------------------------------------------------ #
    def _log_notification_attempt(
        self,
        job_id: str,
        job_name: str,
        recipients: List[str],
        cc: List[str],
        subject: str,
        overall_status: str,
        send_status: str,
        failure_reason: Optional[str],
        retry_count: int,
    ) -> None:
        """Insert one audit row into ``etl_notification_log``. Never raises."""
        try:
            insert_notification_log_row(
                self.db_url,
                job_id=job_id,
                job_name=job_name,
                notification_type="EMAIL",
                recipients=", ".join(recipients),
                cc=", ".join(cc) if cc else None,
                subject=subject,
                overall_status=overall_status,
                send_status=send_status,
                failure_reason=failure_reason,
                retry_count=retry_count,
                sent_time=_dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None) if send_status == "SENT" else None,
            )
        except Exception:  # noqa: BLE001
            # Deliberately swallow: a DB audit failure should not crash the
            # calling ETL pipeline. In production, pair this with a
            # secondary alert (e.g. write to stderr / a fallback file).
            print("WARNING: failed to write notification audit row to DB.")
