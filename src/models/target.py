"""Curated target data models."""

from __future__ import annotations
import hashlib
from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class AlertType(str, Enum):
    MISSING_LOAD = "MISSING_LOAD"
    LATE_ARRIVAL = "LATE_ARRIVAL"
    SLA_BREACH = "SLA_BREACH"
    STALE_DATA = "STALE_DATA"
    DATA_QUALITY_ANOMALY = "DATA_QUALITY_ANOMALY"


class AlertSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AlertStatus(str, Enum):
    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"


class FreshnessStatus(str, Enum):
    FRESH = "FRESH"
    STALE_WARNING = "STALE_WARNING"
    CRITICAL_STALE = "CRITICAL_STALE"


class DatasetBatchRecord(BaseModel):
    """Curated record for an ingested dataset batch with arrival and SLA duration calculations."""
    batch_id: str
    dataset_id: str
    schedule_interval_start: datetime
    schedule_interval_end: datetime
    expected_arrival_time: datetime
    actual_arrival_time: datetime
    actual_completion_time: Optional[datetime] = None
    max_event_timestamp: Optional[datetime] = None
    row_count: int
    bytes_size: int
    status: str
    arrival_delay_seconds: float
    is_late_arrival: bool
    pipeline_duration_seconds: Optional[float] = None
    sla_completion_deadline: Optional[datetime] = None
    is_sla_breached: bool
    source_file: str
    producer_id: str
    ingestion_run_id: str
    processed_at: datetime


class DatasetFreshnessMetric(BaseModel):
    """Calculated freshness metric snapshot for a dataset as of an evaluation timestamp."""
    metric_id: str
    dataset_id: str
    as_of_time: datetime
    last_successful_batch_id: Optional[str] = None
    last_successful_load_time: Optional[datetime] = None
    max_event_timestamp: Optional[datetime] = None
    freshness_lag_seconds: float
    freshness_lag_minutes: float
    max_acceptable_freshness_minutes: float
    freshness_status: FreshnessStatus
    is_freshness_sla_breached: bool
    evaluated_run_id: str

    @staticmethod
    def generate_metric_id(dataset_id: str, as_of_time: datetime) -> str:
        iso_str = as_of_time.isoformat()
        return hashlib.sha256(f"{dataset_id}:{iso_str}".encode("utf-8")).hexdigest()[:16]


class AlertEvent(BaseModel):
    """Actionable alert generated for missing, late, stale, or SLA-breached loads."""
    alert_id: str
    dataset_id: str
    alert_type: AlertType
    severity: AlertSeverity
    status: AlertStatus = AlertStatus.OPEN
    schedule_interval_start: Optional[datetime] = None
    expected_time: Optional[datetime] = None
    actual_time: Optional[datetime] = None
    delay_minutes: float = 0.0
    alert_title: str
    alert_message: str
    recommended_action: str
    owner_team: str
    dedup_key: str
    triggered_at: datetime
    run_id: str

    @staticmethod
    def generate_alert_id(dataset_id: str, dedup_key: str) -> str:
        return hashlib.sha256(f"{dataset_id}:{dedup_key}".encode("utf-8")).hexdigest()[:16]
