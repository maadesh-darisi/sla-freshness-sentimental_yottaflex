"""Quarantine model for rejected records."""

from __future__ import annotations
import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class RejectCode(str, Enum):
    MALFORMED_JSON = "MALFORMED_JSON"
    SCHEMA_VALIDATION_ERROR = "SCHEMA_VALIDATION_ERROR"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    NEGATIVE_ROW_COUNT = "NEGATIVE_ROW_COUNT"
    NEGATIVE_BYTES_SIZE = "NEGATIVE_BYTES_SIZE"
    UNKNOWN_DATASET_ID = "UNKNOWN_DATASET_ID"
    DUPLICATE_KEY_IN_BATCH = "DUPLICATE_KEY_IN_BATCH"
    INVALID_SCHEDULE_WINDOW = "INVALID_SCHEDULE_WINDOW"
    UNEXPECTED_SCHEMA_CHANGE = "UNEXPECTED_SCHEMA_CHANGE"


class QuarantineRecord(BaseModel):
    """Encapsulates a quarantined/rejected record with full traceability."""
    quarantine_id: str
    run_id: str
    source_file: str
    line_number: int = 0
    raw_record: str
    reject_code: RejectCode
    reject_reason: str
    rejected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


    @staticmethod
    def generate_id(run_id: str, source_file: str, line_number: int, raw_record: str) -> str:
        payload = f"{run_id}:{source_file}:{line_number}:{raw_record}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
