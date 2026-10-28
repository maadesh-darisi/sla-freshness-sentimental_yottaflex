"""Ingestion validator enforcing schema integrity, catalog foreign keys, and quarantine routing."""

from __future__ import annotations
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from pydantic import ValidationError

from src.config import SLACatalog
from src.models import (
    QuarantineRecord,
    RawBatchArrivalRecord,
    RejectCode,
)


@dataclass
class ValidationResult:
    """Encapsulates valid records and quarantined records with metrics."""
    valid_records: List[RawBatchArrivalRecord] = field(default_factory=list)
    quarantined_records: List[QuarantineRecord] = field(default_factory=list)
    total_input_count: int = 0
    reject_reasons_count: Dict[str, int] = field(default_factory=dict)

    def record_quarantine(self, quarantine_record: QuarantineRecord) -> None:
        self.quarantined_records.append(quarantine_record)
        code = quarantine_record.reject_code.value
        self.reject_reasons_count[code] = self.reject_reasons_count.get(code, 0) + 1


class BatchValidator:
    """Validates raw records against SLA Catalog and business schemas."""

    def __init__(self, catalog: SLACatalog, run_id: str, source_file: str):
        self.catalog = catalog
        self.run_id = run_id
        self.source_file = source_file
        self.seen_batch_ids: Set[str] = set()

    def validate_stream(
        self, stream_iterator: Any
    ) -> ValidationResult:
        """Processes (line_number, raw_or_error, raw_str) from reader and categorizes records."""
        result = ValidationResult()

        for line_num, parsed_or_exc, raw_str in stream_iterator:
            result.total_input_count += 1

            # 1. Handle parsing / decode exceptions directly
            if isinstance(parsed_or_exc, Exception):
                q = QuarantineRecord(
                    quarantine_id=QuarantineRecord.generate_id(self.run_id, self.source_file, line_num, str(raw_str)),
                    run_id=self.run_id,
                    source_file=self.source_file,
                    line_number=line_num,
                    raw_record=str(raw_str),
                    reject_code=RejectCode.MALFORMED_JSON,
                    reject_reason=f"Malformed record syntax error: {str(parsed_or_exc)}",
                    rejected_at=datetime.now(timezone.utc),
                )
                result.record_quarantine(q)
                continue

            if not isinstance(parsed_or_exc, dict):
                q = QuarantineRecord(
                    quarantine_id=QuarantineRecord.generate_id(self.run_id, self.source_file, line_num, str(raw_str)),
                    run_id=self.run_id,
                    source_file=self.source_file,
                    line_number=line_num,
                    raw_record=str(raw_str),
                    reject_code=RejectCode.SCHEMA_VALIDATION_ERROR,
                    reject_reason=f"Record is not a JSON object: received {type(parsed_or_exc).__name__}",
                    rejected_at=datetime.now(timezone.utc),
                )
                result.record_quarantine(q)
                continue

            record_dict = parsed_or_exc

            # 2. Check for missing required fields explicitly for precise error coding
            required_fields = ["batch_id", "dataset_id", "schedule_interval_start", "schedule_interval_end", "expected_arrival_time", "actual_arrival_time", "row_count"]
            missing_fields = [f for f in required_fields if f not in record_dict or record_dict[f] is None]
            if missing_fields:
                q = QuarantineRecord(
                    quarantine_id=QuarantineRecord.generate_id(self.run_id, self.source_file, line_num, str(raw_str)),
                    run_id=self.run_id,
                    source_file=self.source_file,
                    line_number=line_num,
                    raw_record=str(raw_str),
                    reject_code=RejectCode.MISSING_REQUIRED_FIELD,
                    reject_reason=f"Missing required fields: {', '.join(missing_fields)}",
                    rejected_at=datetime.now(timezone.utc),
                )
                result.record_quarantine(q)
                continue

            # 3. Check Referential Integrity against SLA Catalog
            dataset_id = str(record_dict["dataset_id"]).strip()
            if self.catalog.get_dataset(dataset_id) is None:
                q = QuarantineRecord(
                    quarantine_id=QuarantineRecord.generate_id(self.run_id, self.source_file, line_num, str(raw_str)),
                    run_id=self.run_id,
                    source_file=self.source_file,
                    line_number=line_num,
                    raw_record=str(raw_str),
                    reject_code=RejectCode.UNKNOWN_DATASET_ID,
                    reject_reason=f"Referential integrity failure: dataset_id '{dataset_id}' not found in SLA catalog",
                    rejected_at=datetime.now(timezone.utc),
                )
                result.record_quarantine(q)
                continue

            # 4. Check In-Batch Duplicate Primary Key
            batch_id = str(record_dict["batch_id"]).strip()
            if batch_id in self.seen_batch_ids:
                q = QuarantineRecord(
                    quarantine_id=QuarantineRecord.generate_id(self.run_id, self.source_file, line_num, str(raw_str)),
                    run_id=self.run_id,
                    source_file=self.source_file,
                    line_number=line_num,
                    raw_record=str(raw_str),
                    reject_code=RejectCode.DUPLICATE_KEY_IN_BATCH,
                    reject_reason=f"Duplicate batch_id '{batch_id}' detected within current batch file",
                    rejected_at=datetime.now(timezone.utc),
                )
                result.record_quarantine(q)
                continue

            # 5. Pydantic Model Validation (type checks, ISO timestamp conversions, numeric checks)
            try:
                validated_model = RawBatchArrivalRecord(**record_dict)
            except ValidationError as val_err:
                # Classify specific validation error
                err_str = str(val_err)
                reject_code = RejectCode.SCHEMA_VALIDATION_ERROR
                if "row_count" in err_str and "greater_than_equal" in err_str:
                    reject_code = RejectCode.NEGATIVE_ROW_COUNT
                elif "bytes_size" in err_str and "greater_than_equal" in err_str:
                    reject_code = RejectCode.NEGATIVE_BYTES_SIZE
                elif "datetime" in err_str or "timestamp" in err_str:
                    reject_code = RejectCode.INVALID_TIMESTAMP

                q = QuarantineRecord(
                    quarantine_id=QuarantineRecord.generate_id(self.run_id, self.source_file, line_num, str(raw_str)),
                    run_id=self.run_id,
                    source_file=self.source_file,
                    line_number=line_num,
                    raw_record=str(raw_str),
                    reject_code=reject_code,
                    reject_reason=f"Schema validation error: {err_str}",
                    rejected_at=datetime.now(timezone.utc),
                )
                result.record_quarantine(q)
                continue

            # 6. Domain Business Rules: Schedule Interval Ordering
            if validated_model.schedule_interval_end < validated_model.schedule_interval_start:
                q = QuarantineRecord(
                    quarantine_id=QuarantineRecord.generate_id(self.run_id, self.source_file, line_num, str(raw_str)),
                    run_id=self.run_id,
                    source_file=self.source_file,
                    line_number=line_num,
                    raw_record=str(raw_str),
                    reject_code=RejectCode.INVALID_SCHEDULE_WINDOW,
                    reject_reason=(
                        f"Schedule interval end ({validated_model.schedule_interval_end.isoformat()}) "
                        f"cannot be earlier than start ({validated_model.schedule_interval_start.isoformat()})"
                    ),
                    rejected_at=datetime.now(timezone.utc),
                )
                result.record_quarantine(q)
                continue

            # Passed all validations!
            self.seen_batch_ids.add(batch_id)
            result.valid_records.append(validated_model)

        return result
