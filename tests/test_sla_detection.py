"""Tests for SLA detection, late arrival evaluation, freshness calculation, and alerts."""

from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest

from src.config import AppConfig
from src.engine import FreshnessEngine, SLADetector
from src.models import (
    AlertSeverity,
    AlertType,
    BatchStatus,
    FreshnessStatus,
    RawBatchArrivalRecord,
)


def test_late_arrival_detection(temp_workspace: AppConfig):
    """Verifies that an arrival exceeding grace period flags is_late_arrival and generates LATE_ARRIVAL alert."""
    detector = SLADetector(catalog=temp_workspace.catalog, run_id="run_late_test")
    t0 = datetime(2026, 10, 27, 8, 0, 0, tzinfo=timezone.utc)

    # test_orders_hourly grace is 10 min. Expected: 09:15. Actual: 09:40 (25 min late > 10m grace)
    raw = RawBatchArrivalRecord(
        batch_id="batch_late_01",
        dataset_id="test_orders_hourly",
        schedule_interval_start=t0,
        schedule_interval_end=t0 + timedelta(hours=1),
        expected_arrival_time=t0 + timedelta(hours=1, minutes=15),
        actual_arrival_time=t0 + timedelta(hours=1, minutes=40),
        actual_completion_time=t0 + timedelta(hours=1, minutes=50),
        row_count=1000,
        bytes_size=50000,
        status=BatchStatus.SUCCESS,
    )

    curated, alerts = detector.process_raw_batches([raw], evaluation_time=t0 + timedelta(hours=2))

    assert len(curated) == 1
    assert curated[0].is_late_arrival is True
    assert curated[0].arrival_delay_seconds == 25 * 60.0

    late_alerts = [a for a in alerts if a.alert_type == AlertType.LATE_ARRIVAL]
    assert len(late_alerts) == 1
    assert late_alerts[0].delay_minutes == 25.0
    assert late_alerts[0].severity == AlertSeverity.CRITICAL # P1 dataset


def test_sla_duration_breach(temp_workspace: AppConfig):
    """Verifies that processing duration exceeding SLA threshold flags is_sla_breached and generates SLA_BREACH alert."""
    detector = SLADetector(catalog=temp_workspace.catalog, run_id="run_sla_test")
    t0 = datetime(2026, 10, 27, 8, 0, 0, tzinfo=timezone.utc)

    # test_orders_hourly SLA duration is 30 min.
    # Actual arrival: 09:10. Completion: 10:10 (60 min duration > 30m limit)
    raw = RawBatchArrivalRecord(
        batch_id="batch_breach_01",
        dataset_id="test_orders_hourly",
        schedule_interval_start=t0,
        schedule_interval_end=t0 + timedelta(hours=1),
        expected_arrival_time=t0 + timedelta(hours=1, minutes=15),
        actual_arrival_time=t0 + timedelta(hours=1, minutes=10),
        actual_completion_time=t0 + timedelta(hours=2, minutes=10), # 60 min pipeline duration
        row_count=1000,
        bytes_size=50000,
        status=BatchStatus.SUCCESS,
    )

    curated, alerts = detector.process_raw_batches([raw], evaluation_time=t0 + timedelta(hours=3))

    assert len(curated) == 1
    assert curated[0].is_sla_breached is True
    assert curated[0].pipeline_duration_seconds == 60 * 60.0

    breach_alerts = [a for a in alerts if a.alert_type == AlertType.SLA_BREACH]
    assert len(breach_alerts) == 1
    assert breach_alerts[0].dataset_id == "test_orders_hourly"


def test_failed_batch_triggers_sla_breach(temp_workspace: AppConfig):
    """Verifies that a batch with FAILED status is automatically marked as SLA breached."""
    detector = SLADetector(catalog=temp_workspace.catalog, run_id="run_fail_test")
    t0 = datetime(2026, 10, 27, 8, 0, 0, tzinfo=timezone.utc)

    raw = RawBatchArrivalRecord(
        batch_id="batch_fail_01",
        dataset_id="test_orders_hourly",
        schedule_interval_start=t0,
        schedule_interval_end=t0 + timedelta(hours=1),
        expected_arrival_time=t0 + timedelta(hours=1, minutes=15),
        actual_arrival_time=t0 + timedelta(hours=1, minutes=10),
        actual_completion_time=t0 + timedelta(hours=1, minutes=15),
        row_count=0,
        status=BatchStatus.FAILED,
    )

    curated, alerts = detector.process_raw_batches([raw], evaluation_time=t0 + timedelta(hours=2))

    assert curated[0].is_sla_breached is True
    breach_alerts = [a for a in alerts if a.alert_type == AlertType.SLA_BREACH]
    assert len(breach_alerts) == 1


def test_missing_load_detection(temp_workspace: AppConfig, db_manager):
    """Verifies that an expected schedule interval without an arrival generates a MISSING_LOAD alert."""
    detector = SLADetector(catalog=temp_workspace.catalog, run_id="run_missing_test")
    t0 = datetime(2026, 10, 27, 12, 0, 0, tzinfo=timezone.utc)

    # Empty warehouse - no loads have arrived
    with db_manager.get_connection() as con:
        alerts = detector.detect_missing_loads(con, evaluation_time=t0, lookback_hours=6)

    missing = [a for a in alerts if a.alert_type == AlertType.MISSING_LOAD]
    assert len(missing) > 0
    # Every missing alert should have actionable guidance
    for alert in missing:
        assert alert.alert_title.startswith("Missing Load:")
        assert "owner_team" in alert.model_dump()
        assert len(alert.recommended_action) > 10


def test_freshness_engine_evaluation(temp_workspace: AppConfig, db_manager):
    """Verifies freshness calculation, lag thresholds, and staleness classification."""
    engine = FreshnessEngine(catalog=temp_workspace.catalog, run_id="run_fresh_test")
    t0 = datetime(2026, 10, 27, 8, 0, 0, tzinfo=timezone.utc)

    # Insert a batch completed 30 minutes ago
    with db_manager.get_connection() as con:
        con.execute("""
            INSERT INTO curated_batches (
                batch_id, dataset_id, schedule_interval_start, schedule_interval_end,
                expected_arrival_time, actual_arrival_time, actual_completion_time,
                max_event_timestamp, row_count, bytes_size, status, arrival_delay_seconds,
                is_late_arrival, pipeline_duration_seconds, sla_completion_deadline,
                is_sla_breached, source_file, producer_id, ingestion_run_id, processed_at
            ) VALUES (
                'b_fresh_01', 'test_orders_hourly', ?, ?, ?, ?, ?, ?, 100, 1000,
                'SUCCESS', 0.0, false, 300.0, ?, false, 'src', 'prod', 'run_01', ?
            );
        """, [
            t0, t0 + timedelta(hours=1), t0 + timedelta(hours=1, minutes=15),
            t0 + timedelta(hours=1, minutes=10), t0 + timedelta(hours=1, minutes=15),
            t0 + timedelta(hours=1), t0 + timedelta(hours=1, minutes=45), t0 + timedelta(hours=1, minutes=15)
        ])

        # Evaluate at t0 + 1 hour 45 minutes (30 minutes after completion) -> threshold is 120m -> should be FRESH
        eval_time = t0 + timedelta(hours=1, minutes=45)
        metrics, alerts = engine.evaluate_freshness(con, evaluation_time=eval_time)

        order_metric = next(m for m in metrics if m.dataset_id == "test_orders_hourly")
        assert order_metric.freshness_status == FreshnessStatus.FRESH
        assert order_metric.is_freshness_sla_breached is False

        # Evaluate at t0 + 5 hours (240 minutes after max event) -> threshold is 120m -> should be CRITICAL_STALE
        stale_eval_time = t0 + timedelta(hours=5)
        stale_metrics, stale_alerts = engine.evaluate_freshness(con, evaluation_time=stale_eval_time)

        stale_order_metric = next(m for m in stale_metrics if m.dataset_id == "test_orders_hourly")
        assert stale_order_metric.freshness_status == FreshnessStatus.CRITICAL_STALE
        assert stale_order_metric.is_freshness_sla_breached is True

        stale_alerts_for_orders = [a for a in stale_alerts if a.dataset_id == "test_orders_hourly"]
        assert len(stale_alerts_for_orders) == 1
        assert stale_alerts_for_orders[0].alert_type == AlertType.STALE_DATA
