"""
example_usage.py
=================
End-to-end, runnable example showing how the Logging module and the
Notification module are wired together inside a typical ETL pipeline.

Run it with:
    python example_usage.py

What it does
------------
1. Starts an ETL job with ``ETLLogger``.
2. Runs three tasks: Extract (succeeds), Transform (succeeds),
   Load (deliberately fails) to demonstrate both success and failure
   paths.
3. Ends the job and prints where the detailed log file was written.
4. Sends (or, in dry_run mode, simulates) an HTML status email via
   ``EmailNotifier`` and prints the resulting table for inspection.

Notes
-----
The bundled ``config/email_config.toml`` has ``dry_run = true`` by default,
so this script is safe to run without real Outlook credentials -- it will
build the full HTML email and log the attempt to the DB, but will not
actually call smtplib. Set ``dry_run = false`` and fill in real credentials
to send a live email.
"""

from __future__ import annotations

import random

from logging_module.etl_logger import ETLLogger
from notification_module.email_notifier import EmailNotifier


def extract_sales_data() -> list[dict]:
    """Simulated extract step."""
    return [{"order_id": i, "amount": random.uniform(10, 500)} for i in range(1, 1001)]


def transform_sales_data(rows: list[dict]) -> list[dict]:
    """Simulated transform step."""
    for r in rows:
        r["amount_usd"] = round(r["amount"], 2)
    return rows


def load_sales_data(rows: list[dict]) -> None:
    """Simulated load step that deliberately fails to demo error handling."""
    raise ConnectionError("Could not connect to target Data Warehouse (simulated failure).")


def run_pipeline() -> None:
    logger = ETLLogger(
        job_name="Daily_Sales_Load",
        db_url="sqlite:///etl_logs.db",
        log_dir="logs",
    )
    logger.start_job()

    rows: list[dict] = []

    # Task 1: Extract (succeeds)
    with logger.task(task_id="T1", task_name="Extract_Sales_Data") as t:
        rows = extract_sales_data()
        t.rows_processed = len(rows)

    # Task 2: Transform (succeeds)
    with logger.task(task_id="T2", task_name="Transform_Sales_Data") as t:
        rows = transform_sales_data(rows)
        t.rows_processed = len(rows)

    # Task 3: Load (fails) - suppress_exceptions=True so the pipeline can
    # still finish and send a full notification including the failure.
    with logger.task(task_id="T3", task_name="Load_Sales_Data", suppress_exceptions=True) as t:
        load_sales_data(rows)
        t.rows_processed = len(rows)

    logger.end_job()
    print(f"\nDetailed log file written to: {logger.log_file_path}\n")

    # ---- Notification -----------------------------------------------------
    notifier = EmailNotifier(config_path="config/email_config.toml")
    summary = logger.get_job_summary()

    sent = notifier.send_status_email(
        job_id=logger.job_id,
        job_name=logger.job_name,
        records=summary,
    )
    print(f"Notification sent (or dry-run simulated): {sent}")
    print("\nJob summary passed to the notifier:")
    for row in summary:
        print(row)


if __name__ == "__main__":
    run_pipeline()
