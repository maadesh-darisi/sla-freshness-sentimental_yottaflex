"""Tests for source-to-target reconciliation and audit verification."""

from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import pytest

from src.config import AppConfig
from src.pipeline import SLAPipeline
from src.reconciliation import ReconciliationManager


def test_reconciliation_conservation_and_reporting(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that reconciliation queries accurately enforce the conservation equation and generate reports."""
    input_file = tmp_path / "reconciliation_test.jsonl"
    t0 = datetime(2026, 10, 27, 8, 0, 0, tzinfo=timezone.utc)

    # 2 valid records, 2 invalid records
    records = [
        # Valid 1
        json.dumps({
            "batch_id": "rec_batch_01",
            "dataset_id": "test_orders_hourly",
            "schedule_interval_start": "2026-10-27T08:00:00Z",
            "schedule_interval_end": "2026-10-27T09:00:00Z",
            "expected_arrival_time": "2026-10-27T09:15:00Z",
            "actual_arrival_time": "2026-10-27T09:10:00Z",
            "row_count": 1000,
            "status": "SUCCESS",
        }),
        # Valid 2
        json.dumps({
            "batch_id": "rec_batch_02",
            "dataset_id": "test_orders_hourly",
            "schedule_interval_start": "2026-10-27T09:00:00Z",
            "schedule_interval_end": "2026-10-27T10:00:00Z",
            "expected_arrival_time": "2026-10-27T10:15:00Z",
            "actual_arrival_time": "2026-10-27T10:10:00Z",
            "row_count": 2000,
            "status": "SUCCESS",
        }),
        # Invalid 1 (malformed JSON syntax)
        '{"batch_id": "bad_syntax", corrupted',
        # Invalid 2 (negative row count)
        json.dumps({
            "batch_id": "rec_batch_03",
            "dataset_id": "test_orders_hourly",
            "schedule_interval_start": "2026-10-27T10:00:00Z",
            "schedule_interval_end": "2026-10-27T11:00:00Z",
            "expected_arrival_time": "2026-10-27T11:15:00Z",
            "actual_arrival_time": "2026-10-27T11:10:00Z",
            "row_count": -10,
        }),
    ]
    input_file.write_text("\n".join(records) + "\n", encoding="utf-8")

    pipeline = SLAPipeline(config=temp_workspace)
    eval_time = datetime(2026, 10, 27, 12, 0, 0, tzinfo=timezone.utc)
    summary = pipeline.run(input_file=input_file, evaluation_time=eval_time)

    assert summary["input_records"] == 4
    assert summary["valid_records"] == 2
    assert summary["quarantined_records"] == 2

    # Run Reconciliation
    rec_mgr = ReconciliationManager(pipeline.db, temp_workspace.catalog, temp_workspace.pipeline.reports_dir)
    rec = rec_mgr.reconcile_run(summary["run_id"])

    # Assertions
    assert rec["conservation_equation_passed"] is True
    assert rec["conservation_discrepancy"] == 0
    assert rec["primary_key_duplicates_curated"] == 0
    assert rec["primary_key_duplicates_alerts"] == 0
    assert rec["referential_integrity_violations"] == 0
    assert rec["curated_records_in_db"] == 2
    assert rec["quarantine_records_in_db"] == 2

    # Generate Markdown Report and verify file exists
    md_path = rec_mgr.generate_markdown_report(rec)
    assert md_path.exists()
    content = md_path.read_text(encoding="utf-8")
    assert "Mathematical Conservation & Integrity Check" in content
    assert "PASSED" in content
