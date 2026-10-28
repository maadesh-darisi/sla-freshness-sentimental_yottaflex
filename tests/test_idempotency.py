"""Tests for pipeline idempotency, safe reruns, and retry behavior."""

from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest

from src.config import AppConfig
from src.pipeline import SLAPipeline


def test_pipeline_rerun_idempotency(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that re-running the exact same batch file multiple times creates zero duplicate rows."""
    input_file = tmp_path / "idempotent_batches.jsonl"
    t0 = datetime(2026, 10, 27, 8, 0, 0, tzinfo=timezone.utc)

    records = [
        {
            "batch_id": f"batch_idemp_{i}",
            "dataset_id": "test_orders_hourly",
            "schedule_interval_start": (t0 + timedelta(hours=i)).isoformat(),
            "schedule_interval_end": (t0 + timedelta(hours=i+1)).isoformat(),
            "expected_arrival_time": (t0 + timedelta(hours=i+1, minutes=15)).isoformat(),
            "actual_arrival_time": (t0 + timedelta(hours=i+1, minutes=10)).isoformat(),
            "actual_completion_time": (t0 + timedelta(hours=i+1, minutes=20)).isoformat(),
            "row_count": 1000 * (i + 1),
            "status": "SUCCESS",
        }
        for i in range(3)
    ]
    input_file.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")

    pipeline = SLAPipeline(config=temp_workspace)
    eval_time = t0 + timedelta(hours=5)

    # First run
    run1 = pipeline.run(input_file=input_file, evaluation_time=eval_time, run_id="run_pass_1")
    assert run1["valid_records"] == 3

    with pipeline.db.get_connection() as con:
        count1 = con.execute("SELECT COUNT(*) FROM curated_batches;").fetchone()[0]
        distinct1 = con.execute("SELECT COUNT(DISTINCT batch_id) FROM curated_batches;").fetchone()[0]
        assert count1 == 3
        assert distinct1 == 3

    # Second run with identical input
    run2 = pipeline.run(input_file=input_file, evaluation_time=eval_time, run_id="run_pass_2")
    assert run2["valid_records"] == 3

    with pipeline.db.get_connection() as con:
        count2 = con.execute("SELECT COUNT(*) FROM curated_batches;").fetchone()[0]
        distinct2 = con.execute("SELECT COUNT(DISTINCT batch_id) FROM curated_batches;").fetchone()[0]
        assert count2 == 3, "Rerun should NOT increase total row count in curated_batches"
        assert distinct2 == 3, "Primary key uniqueness must remain intact"

        # Check alert count remains distinct
        alert_dupes = con.execute("SELECT COUNT(*) - COUNT(DISTINCT alert_id) FROM alert_events;").fetchone()[0]
        assert alert_dupes == 0, "No duplicate alert IDs allowed"


def test_batch_retry_updates_existing_record(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that re-ingesting a previously failed batch with SUCCESS status updates it cleanly."""
    input_file = tmp_path / "retry_batch.jsonl"
    t0 = datetime(2026, 10, 27, 8, 0, 0, tzinfo=timezone.utc)

    # Initial failed batch
    failed_record = {
        "batch_id": "batch_retry_test_01",
        "dataset_id": "test_orders_hourly",
        "schedule_interval_start": t0.isoformat(),
        "schedule_interval_end": (t0 + timedelta(hours=1)).isoformat(),
        "expected_arrival_time": (t0 + timedelta(hours=1, minutes=15)).isoformat(),
        "actual_arrival_time": (t0 + timedelta(hours=1, minutes=10)).isoformat(),
        "actual_completion_time": (t0 + timedelta(hours=1, minutes=12)).isoformat(),
        "row_count": 0,
        "status": "FAILED",
    }
    input_file.write_text(json.dumps(failed_record) + "\n", encoding="utf-8")

    pipeline = SLAPipeline(config=temp_workspace)
    pipeline.run(input_file=input_file, evaluation_time=t0 + timedelta(hours=2), run_id="run_fail")

    with pipeline.db.get_connection() as con:
        row = con.execute("SELECT status, row_count, is_sla_breached FROM curated_batches WHERE batch_id = 'batch_retry_test_01';").fetchone()
        assert row[0] == "FAILED"
        assert row[1] == 0
        assert row[2] is True # Failed batch marks SLA breached

    # Producer retried: successful batch with the SAME batch_id and 15,000 rows
    success_record = dict(failed_record)
    success_record["status"] = "SUCCESS"
    success_record["row_count"] = 15000
    success_record["actual_completion_time"] = (t0 + timedelta(hours=1, minutes=25)).isoformat()
    input_file.write_text(json.dumps(success_record) + "\n", encoding="utf-8")

    pipeline.run(input_file=input_file, evaluation_time=t0 + timedelta(hours=2), run_id="run_retry")

    with pipeline.db.get_connection() as con:
        total_count = con.execute("SELECT COUNT(*) FROM curated_batches WHERE batch_id = 'batch_retry_test_01';").fetchone()[0]
        assert total_count == 1, "Must not create a second row"

        updated = con.execute("SELECT status, row_count, is_sla_breached FROM curated_batches WHERE batch_id = 'batch_retry_test_01';").fetchone()
        assert updated[0] == "SUCCESS"
        assert updated[1] == 15000
        assert updated[2] is False # SLA breach resolved upon successful retry
