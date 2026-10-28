"""Freshness calculation engine and data staleness alerting."""

from __future__ import annotations
from datetime import datetime
from typing import List, Tuple
import duckdb

from src.config import SLACatalog
from src.utils import to_utc
from src.models import (

    AlertEvent,
    AlertSeverity,
    AlertStatus,
    AlertType,
    DatasetFreshnessMetric,
    FreshnessStatus,
)


class FreshnessEngine:
    """Calculates dataset freshness lags, evaluates staleness thresholds, and issues alerts."""

    def __init__(self, catalog: SLACatalog, run_id: str):
        self.catalog = catalog
        self.run_id = run_id

    def evaluate_freshness(
        self,
        db_con: duckdb.DuckDBPyConnection,
        evaluation_time: datetime,
    ) -> Tuple[List[DatasetFreshnessMetric], List[AlertEvent]]:
        """Calculates freshness metrics for all active datasets in catalog."""
        metrics: List[DatasetFreshnessMetric] = []
        alerts: List[AlertEvent] = []

        for config in self.catalog.datasets:
            if not config.sla_enabled:
                continue

            query = """
                SELECT 
                    batch_id,
                    actual_completion_time,
                    max_event_timestamp,
                    actual_arrival_time
                FROM curated_batches
                WHERE dataset_id = ?
                  AND status = 'SUCCESS'
                ORDER BY coalesce(max_event_timestamp, actual_completion_time, actual_arrival_time) DESC
                LIMIT 1;
            """
            row = db_con.execute(query, [config.dataset_id]).fetchone()

            if row is None:
                # No successful batch has ever arrived
                freshness_lag_seconds = 864000.0 # 10 days placeholder
                freshness_lag_minutes = freshness_lag_seconds / 60.0
                status = FreshnessStatus.CRITICAL_STALE
                is_breached = True
                latest_batch_id = None
                latest_completion = None
                latest_event_ts = None
            else:
                latest_batch_id, latest_completion, latest_event_ts, latest_arrival = row
                watermark = latest_event_ts or latest_completion or latest_arrival
                watermark_utc = to_utc(watermark)
                eval_utc = to_utc(evaluation_time)

                diff = (eval_utc - watermark_utc).total_seconds()
                freshness_lag_seconds = max(0.0, diff)
                freshness_lag_minutes = round(freshness_lag_seconds / 60.0, 2)


                threshold = config.max_acceptable_freshness_minutes
                if freshness_lag_minutes <= threshold:
                    status = FreshnessStatus.FRESH
                    is_breached = False
                elif freshness_lag_minutes <= 1.5 * threshold:
                    status = FreshnessStatus.STALE_WARNING
                    is_breached = True
                else:
                    status = FreshnessStatus.CRITICAL_STALE
                    is_breached = True

            metric_id = DatasetFreshnessMetric.generate_metric_id(config.dataset_id, evaluation_time)
            metric = DatasetFreshnessMetric(
                metric_id=metric_id,
                dataset_id=config.dataset_id,
                as_of_time=evaluation_time,
                last_successful_batch_id=latest_batch_id,
                last_successful_load_time=latest_completion,
                max_event_timestamp=latest_event_ts,
                freshness_lag_seconds=freshness_lag_seconds,
                freshness_lag_minutes=freshness_lag_minutes,
                max_acceptable_freshness_minutes=config.max_acceptable_freshness_minutes,
                freshness_status=status,
                is_freshness_sla_breached=is_breached,
                evaluated_run_id=self.run_id,
            )
            metrics.append(metric)

            # Generate STALE_DATA alert if freshness SLA breached
            if is_breached:
                severity = AlertSeverity.CRITICAL if config.criticality_tier == "P1" else AlertSeverity.WARNING
                slot_key = evaluation_time.strftime("%Y%m%d%H")
                dedup_key = f"{config.dataset_id}:{slot_key}:STALE_DATA"
                alert_id = AlertEvent.generate_alert_id(config.dataset_id, dedup_key)

                alert = AlertEvent(
                    alert_id=alert_id,
                    dataset_id=config.dataset_id,
                    alert_type=AlertType.STALE_DATA,
                    severity=severity,
                    status=AlertStatus.OPEN,
                    schedule_interval_start=latest_completion or evaluation_time,
                    expected_time=evaluation_time,
                    actual_time=latest_event_ts or latest_completion,
                    delay_minutes=round(freshness_lag_minutes - config.max_acceptable_freshness_minutes, 2),
                    alert_title=f"Stale Data Alert: {config.dataset_name} exceeded freshness SLA",
                    alert_message=(
                        f"Dataset '{config.dataset_id}' freshness lag is {freshness_lag_minutes}m, "
                        f"exceeding max acceptable threshold of {config.max_acceptable_freshness_minutes}m. "
                        f"Status: {status.value}."
                    ),
                    recommended_action=(
                        f"1. Check if upstream producer is paused or facing backpressure. "
                        f"2. Inspect Airflow/Dagster orchestrator schedules for {config.dataset_id}. "
                        f"3. Alert analytics stakeholders of stale dashboard data."
                    ),
                    owner_team=config.owner_team,
                    dedup_key=dedup_key,
                    triggered_at=evaluation_time,
                    run_id=self.run_id,
                )
                alerts.append(alert)

        return metrics, alerts
