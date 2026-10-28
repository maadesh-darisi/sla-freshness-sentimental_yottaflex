"""Detection engine for late arrivals, SLA duration breaches, and missing dataset loads."""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
import duckdb

from src.config import DatasetSLAConfig, SLACatalog
from src.utils import to_utc
from src.models import (

    AlertEvent,
    AlertSeverity,
    AlertStatus,
    AlertType,
    DatasetBatchRecord,
    RawBatchArrivalRecord,
)


class SLADetector:
    """Evaluates batches and expected schedules to detect SLA violations and generate alerts."""

    def __init__(self, catalog: SLACatalog, run_id: str):
        self.catalog = catalog
        self.run_id = run_id

    def process_raw_batches(
        self,
        raw_records: List[RawBatchArrivalRecord],
        evaluation_time: datetime,
    ) -> Tuple[List[DatasetBatchRecord], List[AlertEvent]]:
        """Transforms validated raw records into curated batch records and generates immediate arrival/SLA alerts."""
        curated_batches: List[DatasetBatchRecord] = []
        alerts: List[AlertEvent] = []

        eval_time = to_utc(evaluation_time)

        for raw in raw_records:
            config = self.catalog.get_dataset(raw.dataset_id)
            if not config:
                continue

            actual_arrival = to_utc(raw.actual_arrival_time)
            expected_arrival = to_utc(raw.expected_arrival_time)
            actual_completion = to_utc(raw.actual_completion_time)

            # Calculate arrival delay
            delay_sec = (actual_arrival - expected_arrival).total_seconds()
            delay_seconds = max(0.0, delay_sec)
            delay_minutes = round(delay_seconds / 60.0, 2)

            # Check if late arrival (beyond grace period)
            grace_seconds = config.arrival_grace_minutes * 60.0
            is_late_arrival = delay_seconds > grace_seconds

            # Calculate SLA deadline and duration
            sla_duration_sec = config.sla_duration_minutes * 60.0
            sla_completion_deadline = actual_arrival + timedelta(seconds=sla_duration_sec)

            pipeline_duration_seconds: Optional[float] = None
            is_sla_breached = False

            if actual_completion:
                duration = (actual_completion - actual_arrival).total_seconds()
                pipeline_duration_seconds = max(0.0, duration)
                if pipeline_duration_seconds > sla_duration_sec:
                    is_sla_breached = True
            elif raw.status == "FAILED":
                is_sla_breached = True
            elif eval_time > sla_completion_deadline:
                # Still running or incomplete beyond SLA deadline
                is_sla_breached = True

            # If status is failed, mark SLA breached regardless
            if raw.status == "FAILED":
                is_sla_breached = True


            curated_batch = DatasetBatchRecord(
                batch_id=raw.batch_id,
                dataset_id=raw.dataset_id,
                schedule_interval_start=raw.schedule_interval_start,
                schedule_interval_end=raw.schedule_interval_end,
                expected_arrival_time=raw.expected_arrival_time,
                actual_arrival_time=raw.actual_arrival_time,
                actual_completion_time=raw.actual_completion_time,
                max_event_timestamp=raw.max_event_timestamp or raw.actual_arrival_time,
                row_count=raw.row_count,
                bytes_size=raw.bytes_size,
                status=raw.status.value,
                arrival_delay_seconds=delay_seconds,
                is_late_arrival=is_late_arrival,
                pipeline_duration_seconds=pipeline_duration_seconds,
                sla_completion_deadline=sla_completion_deadline,
                is_sla_breached=is_sla_breached,
                source_file=raw.source_file or "unknown",
                producer_id=raw.producer_id or "default_producer",
                ingestion_run_id=self.run_id,
                processed_at=evaluation_time,
            )
            curated_batches.append(curated_batch)

            # Generate LATE_ARRIVAL alert if late
            if is_late_arrival:
                severity = AlertSeverity.CRITICAL if config.criticality_tier == "P1" else AlertSeverity.WARNING
                dedup_key = f"{raw.dataset_id}:{raw.schedule_interval_start.isoformat()}:LATE_ARRIVAL"
                alert_id = AlertEvent.generate_alert_id(raw.dataset_id, dedup_key)

                alert = AlertEvent(
                    alert_id=alert_id,
                    dataset_id=raw.dataset_id,
                    alert_type=AlertType.LATE_ARRIVAL,
                    severity=severity,
                    status=AlertStatus.OPEN,
                    schedule_interval_start=raw.schedule_interval_start,
                    expected_time=raw.expected_arrival_time,
                    actual_time=raw.actual_arrival_time,
                    delay_minutes=delay_minutes,
                    alert_title=f"Late Arrival: {config.dataset_name} exceeded grace period by {round(delay_minutes - config.arrival_grace_minutes, 1)}m",
                    alert_message=(
                        f"Dataset '{raw.dataset_id}' arrived at {raw.actual_arrival_time.isoformat()}, "
                        f"which is {delay_minutes} minutes after expected time {raw.expected_arrival_time.isoformat()} "
                        f"(Grace period: {config.arrival_grace_minutes} mins)."
                    ),
                    recommended_action=(
                        f"1. Check upstream producer {raw.producer_id} logs for delay cause. "
                        f"2. Verify whether downstream consumption DAGs for {raw.dataset_id} were delayed. "
                        f"3. Escalate to owning team {config.owner_team} if persistent."
                    ),
                    owner_team=config.owner_team,
                    dedup_key=dedup_key,
                    triggered_at=evaluation_time,
                    run_id=self.run_id,
                )
                alerts.append(alert)

            # Generate SLA_BREACH alert if breached or failed
            if is_sla_breached:
                severity = AlertSeverity.CRITICAL if config.criticality_tier in ["P1", "P2"] else AlertSeverity.WARNING
                reason = "Processing duration exceeded SLA deadline" if raw.status != "FAILED" else "Batch marked as FAILED during ingestion"
                dedup_key = f"{raw.dataset_id}:{raw.schedule_interval_start.isoformat()}:SLA_BREACH"
                alert_id = AlertEvent.generate_alert_id(raw.dataset_id, dedup_key)

                duration_info = f"{round(pipeline_duration_seconds / 60.0, 1)}m" if pipeline_duration_seconds is not None else "N/A"
                alert = AlertEvent(
                    alert_id=alert_id,
                    dataset_id=raw.dataset_id,
                    alert_type=AlertType.SLA_BREACH,
                    severity=severity,
                    status=AlertStatus.OPEN,
                    schedule_interval_start=raw.schedule_interval_start,
                    expected_time=sla_completion_deadline,
                    actual_time=raw.actual_completion_time or evaluation_time,
                    delay_minutes=round(max(0.0, (pipeline_duration_seconds or 0.0) - sla_duration_sec) / 60.0, 2),
                    alert_title=f"SLA Breach: {config.dataset_name} failed completion SLA",
                    alert_message=(
                        f"Dataset '{raw.dataset_id}' violated completion SLA. Reason: {reason}. "
                        f"Max allowed duration: {config.sla_duration_minutes}m, actual duration: {duration_info}. "
                        f"Batch status: {raw.status.value}."
                    ),
                    recommended_action=(
                        f"1. Check processing logs for cluster timeouts, OOM, or schema validation failures. "
                        f"2. Contact {config.owner_team} to trigger retry or manual pipeline re-execution. "
                        f"3. Notify downstream analytics consumers of degraded SLA."
                    ),
                    owner_team=config.owner_team,
                    dedup_key=dedup_key,
                    triggered_at=evaluation_time,
                    run_id=self.run_id,
                )
                alerts.append(alert)

        return curated_batches, alerts

    def detect_missing_loads(
        self,
        db_con: duckdb.DuckDBPyConnection,
        evaluation_time: datetime,
        lookback_hours: int = 48,
    ) -> List[AlertEvent]:
        """Detects expected scheduled batch intervals that have no successful landing."""
        missing_alerts: List[AlertEvent] = []
        eval_time = to_utc(evaluation_time)

        for config in self.catalog.datasets:
            if not config.sla_enabled:
                continue

            cadence_min = config.cadence_minutes
            if cadence_min <= 0:
                continue

            lookback_start = eval_time - timedelta(hours=lookback_hours)

            # Fetch all existing successful schedule intervals from database for this dataset
            rows = db_con.execute("""
                SELECT schedule_interval_start, status
                FROM curated_batches
                WHERE dataset_id = ?
                  AND schedule_interval_start >= ?
                  AND status = 'SUCCESS'
            """, [config.dataset_id, lookback_start]).fetchall()

            successful_intervals: Set[str] = {to_utc(r[0]).isoformat() for r in rows if r[0] is not None}

            # Generate expected intervals from lookback_start up to evaluation_time
            # Align intervals based on cadence_minutes
            current_slot = to_utc(lookback_start.replace(minute=0, second=0, microsecond=0))
            if cadence_min == 1440: # Daily
                current_slot = to_utc(current_slot.replace(hour=0))

            while current_slot <= eval_time:
                slot_end = current_slot + timedelta(minutes=cadence_min)
                expected_arrival = current_slot + timedelta(minutes=config.schedule_offset_minutes)
                deadline = expected_arrival + timedelta(minutes=config.arrival_grace_minutes)

                # Only check intervals whose grace period deadline has passed relative to eval_time
                if eval_time > deadline:
                    slot_iso = current_slot.isoformat()
                    if slot_iso not in successful_intervals:
                        delay_minutes = round((eval_time - expected_arrival).total_seconds() / 60.0, 2)
                        severity = AlertSeverity.CRITICAL if config.criticality_tier == "P1" else AlertSeverity.WARNING
                        dedup_key = f"{config.dataset_id}:{slot_iso}:MISSING_LOAD"
                        alert_id = AlertEvent.generate_alert_id(config.dataset_id, dedup_key)

                        alert = AlertEvent(
                            alert_id=alert_id,
                            dataset_id=config.dataset_id,
                            alert_type=AlertType.MISSING_LOAD,
                            severity=severity,
                            status=AlertStatus.OPEN,
                            schedule_interval_start=current_slot,
                            expected_time=expected_arrival,
                            actual_time=None,
                            delay_minutes=delay_minutes,
                            alert_title=f"Missing Load: {config.dataset_name} missed scheduled arrival window",
                            alert_message=(
                                f"Expected batch for interval [{current_slot.isoformat()} to {slot_end.isoformat()}] "
                                f"was expected at {expected_arrival.isoformat()}, but has not arrived as of {eval_time.isoformat()} "
                                f"({delay_minutes} minutes overdue)."
                            ),
                            recommended_action=(
                                f"1. Check upstream landing zone / bucket for missing file partition. "
                                f"2. Verify upstream producer scheduling job. "
                                f"3. Contact owner team {config.owner_team} for incident mitigation."
                            ),
                            owner_team=config.owner_team,
                            dedup_key=dedup_key,
                            triggered_at=eval_time,
                            run_id=self.run_id,
                        )
                        missing_alerts.append(alert)

                current_slot += timedelta(minutes=cadence_min)


        return missing_alerts
