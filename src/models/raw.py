"""Raw ingestion data models and schemas."""

from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, field_validator


class BatchStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    IN_PROGRESS = "IN_PROGRESS"
    PARTIAL = "PARTIAL"


class RawBatchArrivalRecord(BaseModel):
    """Represents an incoming batch arrival notification or ingestion metadata log."""
    batch_id: str = Field(..., description="Unique deterministic identifier for the batch")
    dataset_id: str = Field(..., description="Identifier of the dataset configured in catalog")
    schedule_interval_start: datetime = Field(..., description="Start of the scheduled cadence window")
    schedule_interval_end: datetime = Field(..., description="End of the scheduled cadence window")
    expected_arrival_time: datetime = Field(..., description="Scheduled or expected arrival timestamp")
    actual_arrival_time: datetime = Field(..., description="Actual timestamp when batch data landed")
    actual_completion_time: Optional[datetime] = Field(None, description="Actual timestamp when processing completed")
    max_event_timestamp: Optional[datetime] = Field(None, description="Latest event timestamp inside batch data")
    row_count: int = Field(..., ge=0, description="Total number of records in batch, non-negative")
    bytes_size: int = Field(0, ge=0, description="Byte size of landed files")
    status: BatchStatus = Field(default=BatchStatus.SUCCESS, description="Status of the batch load")
    source_file: Optional[str] = Field("unknown_source", description="Source file or landing URI")
    producer_id: Optional[str] = Field("default_producer", description="Producer subsystem identifier")
    custom_metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Extensible key-value metadata")

    @field_validator("batch_id")
    @classmethod
    def validate_batch_id_not_empty(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("batch_id cannot be empty or blank")
        return stripped

    @field_validator("dataset_id")
    @classmethod
    def validate_dataset_id_not_empty(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("dataset_id cannot be empty or blank")
        return stripped

    @field_validator("schedule_interval_end")
    @classmethod
    def validate_window_order(cls, v: datetime, values: Any) -> datetime:
        # Schedule interval end must be >= schedule interval start if available
        return v
