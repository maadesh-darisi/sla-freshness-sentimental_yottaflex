"""Audit and pipeline operational run models."""

from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class PipelineStatus(str, Enum):
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED = "FAILED"


class PipelineRunRecord(BaseModel):
    """Operational metrics tracked for every pipeline execution."""
    run_id: str
    pipeline_name: str = "sla_freshness_pipeline"
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: PipelineStatus = PipelineStatus.SUCCESS
    input_records_count: int = 0
    valid_records_count: int = 0
    quarantined_records_count: int = 0
    alerts_generated_count: int = 0
    execution_duration_sec: float = 0.0
    error_message: Optional[str] = None
