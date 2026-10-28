"""Tests for domain-specific edge cases: out-of-order arrivals, midnight crossing, zero-row loads, bulk backfills."""

from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest

from src.config import AppConfig
from src.engine import FreshnessEngine
from src.pipeline import SLAPipeline


def test_out_of_order_batch_arrivals(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that when batches arrive out of chronological order, the max event watermark is computed accurately."""
    input_file = tmp_path / "out_of_order.jsonl"
    t0 = datetime(2026, 10, 27, 8, 0, 0, tzinfo=timezone.utc)

    # Batch 2 (10:00) lands FIRST
    batch2 = {
        "batch_id": "batch_order_1000",
        "dataset_id": "test_orders_hourly",
        "schedule_interval_start": (t0 + timedelta(hours=2)).isoformat(),
        "schedule_interval_end": (t0 + timedelta(hours=3)).isoformat(),
        "expected_arrival_time": (t0 + timedelta(hours=3, minutes=15)).isoformat(),
        "actual_arrival_time": (t0 + timedelta(hours=3, minutes=10)).isoformat(),
        "actual_completion_time": (t0 + timedelta(hours=3, minutes=20)).isoformat(),
        "max_event_timestamp": (t0 + timedelta(hours=2, minutes=58)).isoformat(),
        "row_count": 5000,
        "status": "SUCCESS",
    }

    # Batch 1 (09:00) was delayed and lands SECOND
    batch1 = {
        "batch_id": "batch_order_0900",
        "dataset_id": "test_orders_hourly",
        "schedule_interval_start": (t0 + timedelta(hours=1)).isoformat(),
        "schedule_interval_end": (t0 + timedelta(hours=2)).isoformat(),
        "expected_arrival_time": (t0 + timedelta(hours=2, minutes=15)).isoformat(),
        "actual_arrival_time": (t0 + timedelta(hours=3, minutes=25)).isoformat(), # Delayed arrival!
        "actual_completion_time": (t0 + timedelta(hours=3, minutes=35)).isoformat(),
        "max_event_timestamp": (t0 + timedelta(hours=1, minutes=59)).isoformat(),
        "row_count": 4200,
        "status": "SUCCESS",
    }

    input_file.write_text(json.dumps(batch2) + "\n" + json.dumps(batch1) + "\n", encoding="utf-8")

    pipeline = SLAPipeline(config=temp_workspace)
    eval_time = t0 + timedelta(hours=4)
    summary = pipeline.run(input_file=input_file, evaluation_time=eval_time)

    assert summary["valid_records"] == 2

    # Verify freshness engine selects the true highest watermark (from batch 2, 10:58)
    with pipeline.db.get_connection() as con:
        engine = FreshnessEngine(catalog=temp_workspace.catalog, run_id="run_fresh_check")
        metrics, _ = engine.evaluate_freshness(con, evaluation_time=eval_time)
        order_metric = next(m for m in metrics if m.dataset_id == "test_orders_hourly")

        expected_max_event = t0 + timedelta(hours=2, minutes=58)
        assert order_metric.max_event_timestamp.isoformat() == expected_max_event.isoformat().replace("+00:00", "")
        # Freshness lag should be 62 minutes (from 10:58 to 12:00)
        assert round(order_metric.freshness_lag_minutes) == 62


def test_midnight_boundary_crossing(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that an interval spanning across UTC midnight (23:00 to 00:00) calculates delay and arrival correctly."""
    input_file = tmp_path / "midnight.jsonl"
    t_night = datetime(2026, 10, 27, 23, 0, 0, tzinfo=timezone.utc)
    t_next_day = datetime(2026, 10, 28, 0, 0, 0, tzinfo=timezone.utc)

    record = {
        "batch_id": "batch_midnight_001",
        "dataset_id": "test_orders_hourly",
        "schedule_interval_start": t_night.isoformat(),
        "schedule_interval_end": t_next_day.isoformat(),
        "expected_arrival_time": (t_next_day + timedelta(minutes=15)).isoformat(),
        "actual_arrival_time": (t_next_day + timedelta(minutes=20)).isoformat(),
        "actual_completion_time": (t_next_day + timedelta(minutes=30)).isoformat(),
        "row_count": 800,
        "status": "SUCCESS",
    }
    input_file.write_text(json.dumps(record) + "\n", encoding="utf-8")

    pipeline = SLAPipeline(config=temp_workspace)
    eval_time = t_next_day + timedelta(hours=1)
    summary = pipeline.run(input_file=input_file, evaluation_time=eval_time)

    assert summary["valid_records"] == 1
    with pipeline.db.get_connection() as con:
        row = con.execute("SELECT arrival_delay_seconds, is_late_arrival FROM curated_batches WHERE batch_id = 'batch_midnight_001';").fetchone()
        # 5 minutes delay (arrival at :20 vs expected :15), which is within 10m grace period -> NOT late
        assert row[0] == 300.0
        assert row[1] is False


def test_zero_row_valid_batch(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that a valid batch with 0 records (e.g. low-volume or off-hours) is accepted as successful."""
    input_file = tmp_path / "zero_rows.jsonl"
    t0 = datetime(2026, 10, 27, 3, 0, 0, tzinfo=timezone.utc)

    record = {
        "batch_id": "batch_quiet_hour_01",
        "dataset_id": "test_orders_hourly",
        "schedule_interval_start": t0.isoformat(),
        "schedule_interval_end": (t0 + timedelta(hours=1)).isoformat(),
        "expected_arrival_time": (t0 + timedelta(hours=1, minutes=15)).isoformat(),
        "actual_arrival_time": (t0 + timedelta(hours=1, minutes=10)).isoformat(),
        "actual_completion_time": (t0 + timedelta(hours=1, minutes=11)).isoformat(),
        "row_count": 0, # zero rows
        "status": "SUCCESS",
    }
    input_file.write_text(json.dumps(record) + "\n", encoding="utf-8")

    pipeline = SLAPipeline(config=temp_workspace)
    eval_time = t0 + timedelta(hours=2)
    summary = pipeline.run(input_file=input_file, evaluation_time=eval_time)

    assert summary["valid_records"] == 1
    assert summary["quarantined_records"] == 0
    with pipeline.db.get_connection() as con:
        cnt = con.execute("SELECT row_count, status FROM curated_batches WHERE batch_id = 'batch_quiet_hour_01';").fetchone()
        assert cnt[0] == 0
        assert cnt[1] == "SUCCESS"
