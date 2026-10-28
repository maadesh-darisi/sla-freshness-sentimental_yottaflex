"""Tests for the happy path and empty batch scenarios."""

from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest

from src.config import AppConfig
from src.pipeline import SLAPipeline


def test_happy_path_single_batch(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that a valid batch record is ingested, curated, and audited with zero errors."""
    input_file = tmp_path / "valid_batch.jsonl"
    t0 = datetime(2026, 10, 27, 8, 0, 0, tzinfo=timezone.utc)

    record = {
        "batch_id": "test_batch_001",
        "dataset_id": "test_orders_hourly",
        "schedule_interval_start": t0.isoformat(),
        "schedule_interval_end": (t0 + timedelta(hours=1)).isoformat(),
        "expected_arrival_time": (t0 + timedelta(hours=1, minutes=15)).isoformat(),
        "actual_arrival_time": (t0 + timedelta(hours=1, minutes=10)).isoformat(),
        "actual_completion_time": (t0 + timedelta(hours=1, minutes=25)).isoformat(),
        "max_event_timestamp": (t0 + timedelta(hours=1)).isoformat(),
        "row_count": 5000,
        "bytes_size": 250000,
        "status": "SUCCESS",
        "source_file": "s3://test/orders_001.jsonl",
        "producer_id": "test_producer",
    }
    input_file.write_text(json.dumps(record) + "\n", encoding="utf-8")

    pipeline = SLAPipeline(config=temp_workspace)
    eval_time = t0 + timedelta(hours=1, minutes=30)
    summary = pipeline.run(input_file=input_file, evaluation_time=eval_time)

    assert summary["status"] == "SUCCESS"
    assert summary["input_records"] == 1
    assert summary["valid_records"] == 1
    assert summary["quarantined_records"] == 0

    with pipeline.db.get_connection() as con:
        batch_row = con.execute("SELECT batch_id, status, is_late_arrival, is_sla_breached, row_count FROM curated_batches;").fetchone()
        assert batch_row is not None
        assert batch_row[0] == "test_batch_001"
        assert batch_row[1] == "SUCCESS"
        assert batch_row[2] is False # On time
        assert batch_row[3] is False # SLA met
        assert batch_row[4] == 5000

        # Verify audit run log
        run_row = con.execute("SELECT status, input_records_count, valid_records_count FROM pipeline_runs WHERE run_id = ?;", [summary["run_id"]]).fetchone()
        assert run_row[0] == "SUCCESS"
        assert run_row[1] == 1
        assert run_row[2] == 1


def test_empty_input_batch(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that an empty input file is handled gracefully without crashing."""
    empty_file = tmp_path / "empty_batch.jsonl"
    empty_file.write_text("", encoding="utf-8")

    pipeline = SLAPipeline(config=temp_workspace)
    eval_time = datetime(2026, 10, 27, 8, 0, 0, tzinfo=timezone.utc)
    summary = pipeline.run(input_file=empty_file, evaluation_time=eval_time)

    assert summary["status"] == "SUCCESS"
    assert summary["input_records"] == 0
    assert summary["valid_records"] == 0
    assert summary["quarantined_records"] == 0

    with pipeline.db.get_connection() as con:
        curated_count = con.execute("SELECT COUNT(*) FROM curated_batches;").fetchone()[0]
        assert curated_count == 0
