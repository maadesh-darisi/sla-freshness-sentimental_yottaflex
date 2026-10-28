"""Tests covering schema validation, malformed input, missing fields, duplicates, and quarantine routing."""

from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import pytest

from src.config import AppConfig
from src.ingestion import BatchValidator, IngestionReader
from src.models import RejectCode


def test_malformed_json_syntax(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that unparseable JSON syntax routes directly to quarantine with MALFORMED_JSON."""
    file_path = tmp_path / "malformed.jsonl"
    file_path.write_text('{"batch_id": "b1", "dataset_id": "test_orders_hourly", unclosed\n', encoding="utf-8")

    reader = IngestionReader()
    validator = BatchValidator(temp_workspace.catalog, run_id="test_run", source_file=str(file_path))
    stream = reader.read_file(file_path)
    result = validator.validate_stream(stream)

    assert result.total_input_count == 1
    assert len(result.valid_records) == 0
    assert len(result.quarantined_records) == 1
    assert result.quarantined_records[0].reject_code == RejectCode.MALFORMED_JSON
    assert result.quarantined_records[0].line_number == 1
    assert "unclosed" in result.quarantined_records[0].raw_record


def test_missing_required_fields(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that records missing required fields are quarantined with MISSING_REQUIRED_FIELD."""
    file_path = tmp_path / "missing_fields.jsonl"
    # Missing row_count and actual_arrival_time
    payload = {
        "batch_id": "b2",
        "dataset_id": "test_orders_hourly",
        "schedule_interval_start": "2026-10-27T08:00:00Z",
        "schedule_interval_end": "2026-10-27T09:00:00Z",
        "expected_arrival_time": "2026-10-27T09:15:00Z",
    }
    file_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    reader = IngestionReader()
    validator = BatchValidator(temp_workspace.catalog, run_id="test_run", source_file=str(file_path))
    stream = reader.read_file(file_path)
    result = validator.validate_stream(stream)

    assert len(result.quarantined_records) == 1
    assert result.quarantined_records[0].reject_code == RejectCode.MISSING_REQUIRED_FIELD
    assert "actual_arrival_time" in result.quarantined_records[0].reject_reason
    assert "row_count" in result.quarantined_records[0].reject_reason


def test_null_critical_field(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that explicit null values in required fields are quarantined."""
    file_path = tmp_path / "null_field.jsonl"
    payload = {
        "batch_id": "b3",
        "dataset_id": "test_orders_hourly",
        "schedule_interval_start": "2026-10-27T08:00:00Z",
        "schedule_interval_end": "2026-10-27T09:00:00Z",
        "expected_arrival_time": "2026-10-27T09:15:00Z",
        "actual_arrival_time": "2026-10-27T09:10:00Z",
        "row_count": None, # Explicit null
    }
    file_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    reader = IngestionReader()
    validator = BatchValidator(temp_workspace.catalog, run_id="test_run", source_file=str(file_path))
    stream = reader.read_file(file_path)
    result = validator.validate_stream(stream)

    assert len(result.quarantined_records) == 1
    assert result.quarantined_records[0].reject_code == RejectCode.MISSING_REQUIRED_FIELD


def test_in_batch_duplicate_primary_key(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that duplicate batch_ids within the same file are detected and quarantined."""
    file_path = tmp_path / "in_batch_dupe.jsonl"
    record1 = {
        "batch_id": "b_repeat_100",
        "dataset_id": "test_orders_hourly",
        "schedule_interval_start": "2026-10-27T08:00:00Z",
        "schedule_interval_end": "2026-10-27T09:00:00Z",
        "expected_arrival_time": "2026-10-27T09:15:00Z",
        "actual_arrival_time": "2026-10-27T09:10:00Z",
        "row_count": 100,
    }
    record2 = dict(record1) # Duplicate line
    lines = [json.dumps(record1), json.dumps(record2)]
    file_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    reader = IngestionReader()
    validator = BatchValidator(temp_workspace.catalog, run_id="test_run", source_file=str(file_path))
    stream = reader.read_file(file_path)
    result = validator.validate_stream(stream)

    assert result.total_input_count == 2
    assert len(result.valid_records) == 1
    assert len(result.quarantined_records) == 1
    assert result.quarantined_records[0].reject_code == RejectCode.DUPLICATE_KEY_IN_BATCH
    assert result.quarantined_records[0].line_number == 2


def test_referential_integrity_unknown_dataset(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that records referencing a dataset_id not in the catalog are quarantined."""
    file_path = tmp_path / "unknown_ds.jsonl"
    payload = {
        "batch_id": "b4",
        "dataset_id": "unknown_crypto_feed_xyz",
        "schedule_interval_start": "2026-10-27T08:00:00Z",
        "schedule_interval_end": "2026-10-27T09:00:00Z",
        "expected_arrival_time": "2026-10-27T09:15:00Z",
        "actual_arrival_time": "2026-10-27T09:10:00Z",
        "row_count": 50,
    }
    file_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    reader = IngestionReader()
    validator = BatchValidator(temp_workspace.catalog, run_id="test_run", source_file=str(file_path))
    stream = reader.read_file(file_path)
    result = validator.validate_stream(stream)

    assert len(result.quarantined_records) == 1
    assert result.quarantined_records[0].reject_code == RejectCode.UNKNOWN_DATASET_ID


def test_negative_row_count(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that negative row counts are quarantined."""
    file_path = tmp_path / "neg_rows.jsonl"
    payload = {
        "batch_id": "b5",
        "dataset_id": "test_orders_hourly",
        "schedule_interval_start": "2026-10-27T08:00:00Z",
        "schedule_interval_end": "2026-10-27T09:00:00Z",
        "expected_arrival_time": "2026-10-27T09:15:00Z",
        "actual_arrival_time": "2026-10-27T09:10:00Z",
        "row_count": -500,
    }
    file_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    reader = IngestionReader()
    validator = BatchValidator(temp_workspace.catalog, run_id="test_run", source_file=str(file_path))
    stream = reader.read_file(file_path)
    result = validator.validate_stream(stream)

    assert len(result.quarantined_records) == 1
    assert result.quarantined_records[0].reject_code == RejectCode.NEGATIVE_ROW_COUNT


def test_inverted_schedule_window(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that end < start schedule interval is quarantined."""
    file_path = tmp_path / "inverted_window.jsonl"
    payload = {
        "batch_id": "b6",
        "dataset_id": "test_orders_hourly",
        "schedule_interval_start": "2026-10-27T09:00:00Z",
        "schedule_interval_end": "2026-10-27T08:00:00Z", # End earlier than start
        "expected_arrival_time": "2026-10-27T09:15:00Z",
        "actual_arrival_time": "2026-10-27T09:10:00Z",
        "row_count": 100,
    }
    file_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    reader = IngestionReader()
    validator = BatchValidator(temp_workspace.catalog, run_id="test_run", source_file=str(file_path))
    stream = reader.read_file(file_path)
    result = validator.validate_stream(stream)

    assert len(result.quarantined_records) == 1
    assert result.quarantined_records[0].reject_code == RejectCode.INVALID_SCHEDULE_WINDOW


def test_unparseable_timestamp_quarantine(temp_workspace: AppConfig, tmp_path: Path):
    """Verifies that non-ISO timestamp string routes to quarantine with INVALID_TIMESTAMP."""
    file_path = tmp_path / "bad_timestamp.jsonl"
    payload = {
        "batch_id": "b7",
        "dataset_id": "test_orders_hourly",
        "schedule_interval_start": "not-a-timestamp",
        "schedule_interval_end": "2026-10-27T08:00:00Z",
        "expected_arrival_time": "2026-10-27T09:15:00Z",
        "actual_arrival_time": "2026-10-27T09:10:00Z",
        "row_count": 100,
    }
    file_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    reader = IngestionReader()
    validator = BatchValidator(temp_workspace.catalog, run_id="test_run", source_file=str(file_path))
    stream = reader.read_file(file_path)
    result = validator.validate_stream(stream)

    assert len(result.quarantined_records) == 1
    assert result.quarantined_records[0].reject_code == RejectCode.INVALID_TIMESTAMP

