"""Synthetic data generator for SLA, arrival time, and freshness tracking.

Supports:
1. Small starter sample dataset with explicit expected-answer labels.
2. Scalable high-volume generator (100,000+ records) with fixed random seed.
3. Realistic negative test cases, malformed lines, duplicates, and edge cases.
"""

from __future__ import annotations
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class SyntheticDataGenerator:
    """Generates reproducible synthetic batch arrival records for testing and scale evaluation."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        self.rng = random.Random(seed)

    def generate_starter_sample(
        self,
        output_file: str | Path = "data/sample/starter_batches.jsonl",
        labels_file: str | Path = "data/sample/expected_labels.json",
        base_time: Optional[datetime] = None,
    ) -> Tuple[int, Dict[str, Any]]:
        """Generates a small curated dataset with deliberate edge cases and a separate labels file."""
        out_path = Path(output_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        lab_path = Path(labels_file)
        lab_path.parent.mkdir(parents=True, exist_ok=True)

        t0 = base_time or datetime(2026, 10, 27, 0, 0, 0, tzinfo=timezone.utc)

        records: List[str] = []
        labels: Dict[str, Any] = {
            "total_records": 0,
            "expected_valid": 0,
            "expected_quarantine": 0,
            "expected_late_arrivals": 0,
            "expected_sla_breaches": 0,
            "quarantine_breakdown": {},
            "eval_timestamp": (t0 + timedelta(hours=14)).isoformat(),
        }

        def add_record(raw_line: str, is_valid: bool, reject_code: Optional[str] = None, is_late: bool = False, is_sla_breach: bool = False):
            records.append(raw_line)
            labels["total_records"] += 1
            if is_valid:
                labels["expected_valid"] += 1
                if is_late:
                    labels["expected_late_arrivals"] += 1
                if is_sla_breach:
                    labels["expected_sla_breaches"] += 1
            else:
                labels["expected_quarantine"] += 1
                code = reject_code or "UNKNOWN"
                labels["quarantine_breakdown"][code] = labels["quarantine_breakdown"].get(code, 0) + 1

        # 1. On-time, successful hourly orders batch (Happy Path)
        add_record(json.dumps({
            "batch_id": "batch_orders_20261027_0100",
            "dataset_id": "orders_stream_hourly",
            "schedule_interval_start": (t0 + timedelta(hours=1)).isoformat(),
            "schedule_interval_end": (t0 + timedelta(hours=2)).isoformat(),
            "expected_arrival_time": (t0 + timedelta(hours=2, minutes=15)).isoformat(),
            "actual_arrival_time": (t0 + timedelta(hours=2, minutes=12)).isoformat(),
            "actual_completion_time": (t0 + timedelta(hours=2, minutes=25)).isoformat(),
            "max_event_timestamp": (t0 + timedelta(hours=1, minutes=59)).isoformat(),
            "row_count": 12500,
            "bytes_size": 4500000,
            "status": "SUCCESS",
            "source_file": "s3://landing/orders/20261027_0100.parquet",
            "producer_id": "s3-order-collector"
        }), is_valid=True)

        # 2. Boundary value: Exactly on the grace period limit (10m grace -> arrives exactly at :25)
        add_record(json.dumps({
            "batch_id": "batch_orders_20261027_0200",
            "dataset_id": "orders_stream_hourly",
            "schedule_interval_start": (t0 + timedelta(hours=2)).isoformat(),
            "schedule_interval_end": (t0 + timedelta(hours=3)).isoformat(),
            "expected_arrival_time": (t0 + timedelta(hours=3, minutes=15)).isoformat(),
            "actual_arrival_time": (t0 + timedelta(hours=3, minutes=25)).isoformat(), # exactly 10 min grace
            "actual_completion_time": (t0 + timedelta(hours=3, minutes=40)).isoformat(),
            "max_event_timestamp": (t0 + timedelta(hours=2, minutes=58)).isoformat(),
            "row_count": 8900,
            "bytes_size": 3200000,
            "status": "SUCCESS",
            "source_file": "s3://landing/orders/20261027_0200.parquet",
            "producer_id": "s3-order-collector"
        }), is_valid=True, is_late=False)

        # 3. Late Arrival: Arrived 35 minutes late (grace is 10 min -> late!)
        add_record(json.dumps({
            "batch_id": "batch_orders_20261027_0300",
            "dataset_id": "orders_stream_hourly",
            "schedule_interval_start": (t0 + timedelta(hours=3)).isoformat(),
            "schedule_interval_end": (t0 + timedelta(hours=4)).isoformat(),
            "expected_arrival_time": (t0 + timedelta(hours=4, minutes=15)).isoformat(),
            "actual_arrival_time": (t0 + timedelta(hours=4, minutes=50)).isoformat(), # 35 min late
            "actual_completion_time": (t0 + timedelta(hours=5, minutes=5)).isoformat(),
            "max_event_timestamp": (t0 + timedelta(hours=3, minutes=55)).isoformat(),
            "row_count": 14200,
            "bytes_size": 5100000,
            "status": "SUCCESS",
            "source_file": "s3://landing/orders/20261027_0300.parquet",
            "producer_id": "s3-order-collector"
        }), is_valid=True, is_late=True)

        # 4. SLA Breach: Arrived on time, but processing took 75 minutes (SLA duration limit is 30m)
        add_record(json.dumps({
            "batch_id": "batch_orders_20261027_0400",
            "dataset_id": "orders_stream_hourly",
            "schedule_interval_start": (t0 + timedelta(hours=4)).isoformat(),
            "schedule_interval_end": (t0 + timedelta(hours=5)).isoformat(),
            "expected_arrival_time": (t0 + timedelta(hours=5, minutes=15)).isoformat(),
            "actual_arrival_time": (t0 + timedelta(hours=5, minutes=14)).isoformat(),
            "actual_completion_time": (t0 + timedelta(hours=6, minutes=30)).isoformat(), # 76 min duration!
            "max_event_timestamp": (t0 + timedelta(hours=4, minutes=59)).isoformat(),
            "row_count": 35000,
            "bytes_size": 12000000,
            "status": "SUCCESS",
            "source_file": "s3://landing/orders/20261027_0400.parquet",
            "producer_id": "s3-order-collector"
        }), is_valid=True, is_sla_breach=True)

        # 5. Failed Ingestion Run: status = "FAILED"
        add_record(json.dumps({
            "batch_id": "batch_orders_20261027_0500",
            "dataset_id": "orders_stream_hourly",
            "schedule_interval_start": (t0 + timedelta(hours=5)).isoformat(),
            "schedule_interval_end": (t0 + timedelta(hours=6)).isoformat(),
            "expected_arrival_time": (t0 + timedelta(hours=6, minutes=15)).isoformat(),
            "actual_arrival_time": (t0 + timedelta(hours=6, minutes=16)).isoformat(),
            "actual_completion_time": (t0 + timedelta(hours=6, minutes=20)).isoformat(),
            "max_event_timestamp": (t0 + timedelta(hours=5, minutes=30)).isoformat(),
            "row_count": 0,
            "bytes_size": 0,
            "status": "FAILED",
            "source_file": "s3://landing/orders/20261027_0500.parquet",
            "producer_id": "s3-order-collector"
        }), is_valid=True, is_sla_breach=True)

        # 6. Daily payments settlement (Happy Path)
        add_record(json.dumps({
            "batch_id": "batch_payments_20261027_0600",
            "dataset_id": "payments_settlement_daily",
            "schedule_interval_start": t0.isoformat(),
            "schedule_interval_end": (t0 + timedelta(hours=24)).isoformat(),
            "expected_arrival_time": (t0 + timedelta(hours=6, minutes=0)).isoformat(),
            "actual_arrival_time": (t0 + timedelta(hours=6, minutes=5)).isoformat(),
            "actual_completion_time": (t0 + timedelta(hours=6, minutes=35)).isoformat(),
            "max_event_timestamp": (t0 + timedelta(hours=5, minutes=59)).isoformat(),
            "row_count": 520000,
            "bytes_size": 89000000,
            "status": "SUCCESS",
            "source_file": "s3://finance/settlements/20261027.csv",
            "producer_id": "banking-gateway"
        }), is_valid=True)

        # 7. Customer profiles daily (Happy Path)
        add_record(json.dumps({
            "batch_id": "batch_crm_20261027_0400",
            "dataset_id": "customer_profiles_daily",
            "schedule_interval_start": t0.isoformat(),
            "schedule_interval_end": (t0 + timedelta(hours=24)).isoformat(),
            "expected_arrival_time": (t0 + timedelta(hours=4, minutes=0)).isoformat(),
            "actual_arrival_time": (t0 + timedelta(hours=4, minutes=10)).isoformat(),
            "actual_completion_time": (t0 + timedelta(hours=4, minutes=50)).isoformat(),
            "max_event_timestamp": (t0 + timedelta(hours=3, minutes=59)).isoformat(),
            "row_count": 98000,
            "bytes_size": 25000000,
            "status": "SUCCESS",
            "source_file": "s3://crm/profiles/20261027.parquet",
            "producer_id": "salesforce-sync"
        }), is_valid=True)

        # 8. Boundary: Zero row_count valid batch (valid empty load e.g. holiday or low volume)
        add_record(json.dumps({
            "batch_id": "batch_orders_20261027_0600",
            "dataset_id": "orders_stream_hourly",
            "schedule_interval_start": (t0 + timedelta(hours=6)).isoformat(),
            "schedule_interval_end": (t0 + timedelta(hours=7)).isoformat(),
            "expected_arrival_time": (t0 + timedelta(hours=7, minutes=15)).isoformat(),
            "actual_arrival_time": (t0 + timedelta(hours=7, minutes=10)).isoformat(),
            "actual_completion_time": (t0 + timedelta(hours=7, minutes=20)).isoformat(),
            "max_event_timestamp": (t0 + timedelta(hours=6, minutes=55)).isoformat(),
            "row_count": 0,
            "bytes_size": 128,
            "status": "SUCCESS",
            "source_file": "s3://landing/orders/20261027_0600.parquet",
            "producer_id": "s3-order-collector"
        }), is_valid=True)

        # --- DELIBERATE EDGE CASES AND ANOMALIES FOR QUARANTINE ---

        # 9. Edge Case: Malformed JSON syntax (missing closing bracket)
        add_record(
            '{"batch_id": "batch_malformed_001", "dataset_id": "orders_stream_hourly", "schedule_interval_start": "2026-10-27T07:00:00Z", "row_count": 10',
            is_valid=False,
            reject_code="MALFORMED_JSON"
        )

        # 10. Edge Case: Missing required field (missing 'expected_arrival_time')
        add_record(json.dumps({
            "batch_id": "batch_missing_fields_002",
            "dataset_id": "orders_stream_hourly",
            "schedule_interval_start": (t0 + timedelta(hours=7)).isoformat(),
            "schedule_interval_end": (t0 + timedelta(hours=8)).isoformat(),
            "actual_arrival_time": (t0 + timedelta(hours=8, minutes=10)).isoformat(),
            "row_count": 500,
        }), is_valid=False, reject_code="MISSING_REQUIRED_FIELD")

        # 11. Edge Case: Unknown dataset_id (Referential Integrity violation)
        add_record(json.dumps({
            "batch_id": "batch_unknown_ds_003",
            "dataset_id": "non_existent_crypto_stream_v99",
            "schedule_interval_start": (t0 + timedelta(hours=7)).isoformat(),
            "schedule_interval_end": (t0 + timedelta(hours=8)).isoformat(),
            "expected_arrival_time": (t0 + timedelta(hours=8, minutes=15)).isoformat(),
            "actual_arrival_time": (t0 + timedelta(hours=8, minutes=10)).isoformat(),
            "row_count": 500,
        }), is_valid=False, reject_code="UNKNOWN_DATASET_ID")

        # 12. Edge Case: Negative row_count
        add_record(json.dumps({
            "batch_id": "batch_neg_rows_004",
            "dataset_id": "orders_stream_hourly",
            "schedule_interval_start": (t0 + timedelta(hours=7)).isoformat(),
            "schedule_interval_end": (t0 + timedelta(hours=8)).isoformat(),
            "expected_arrival_time": (t0 + timedelta(hours=8, minutes=15)).isoformat(),
            "actual_arrival_time": (t0 + timedelta(hours=8, minutes=10)).isoformat(),
            "row_count": -450,
        }), is_valid=False, reject_code="NEGATIVE_ROW_COUNT")

        # 13. Edge Case: In-Batch Duplicate Primary Key (repeat batch_orders_20261027_0100)
        add_record(json.dumps({
            "batch_id": "batch_orders_20261027_0100", # duplicate of line 1
            "dataset_id": "orders_stream_hourly",
            "schedule_interval_start": (t0 + timedelta(hours=1)).isoformat(),
            "schedule_interval_end": (t0 + timedelta(hours=2)).isoformat(),
            "expected_arrival_time": (t0 + timedelta(hours=2, minutes=15)).isoformat(),
            "actual_arrival_time": (t0 + timedelta(hours=2, minutes=12)).isoformat(),
            "row_count": 12500,
        }), is_valid=False, reject_code="DUPLICATE_KEY_IN_BATCH")

        # 14. Edge Case: Inverted schedule window (end < start)
        add_record(json.dumps({
            "batch_id": "batch_inverted_window_005",
            "dataset_id": "orders_stream_hourly",
            "schedule_interval_start": (t0 + timedelta(hours=9)).isoformat(),
            "schedule_interval_end": (t0 + timedelta(hours=8)).isoformat(), # end before start!
            "expected_arrival_time": (t0 + timedelta(hours=9, minutes=15)).isoformat(),
            "actual_arrival_time": (t0 + timedelta(hours=9, minutes=10)).isoformat(),
            "row_count": 100,
        }), is_valid=False, reject_code="INVALID_SCHEDULE_WINDOW")

        # Write output files
        with open(out_path, "w", encoding="utf-8") as f:
            for r in records:
                f.write(r + "\n")

        with open(lab_path, "w", encoding="utf-8") as f:
            json.dump(labels, f, indent=2)

        return len(records), labels

    def generate_large_scale_dataset(
        self,
        output_file: str | Path,
        record_count: int = 100000,
        start_date: Optional[datetime] = None,
    ) -> int:
        """Generates a configurable high-volume dataset (e.g. 100,000+ records) with fixed seed."""
        out_path = Path(output_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        t0 = start_date or datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        dataset_ids = [
            ("orders_stream_hourly", 60, 15, 10, 30),
            ("clickstream_events_15m", 15, 5, 5, 10),
            ("customer_profiles_daily", 1440, 240, 30, 60),
            ("payments_settlement_daily", 1440, 360, 15, 45),
            ("inventory_snapshots_6h", 360, 20, 20, 40),
        ]

        logger_interval = max(10000, record_count // 10)
        written = 0

        with open(out_path, "w", encoding="utf-8") as f:
            for i in range(record_count):
                ds_id, cadence_min, offset_min, grace_min, sla_min = self.rng.choice(dataset_ids)
                slot_offset = self.rng.randint(0, 30 * 24 * 60 // max(1, cadence_min))
                interval_start = t0 + timedelta(minutes=slot_offset * cadence_min)
                interval_end = interval_start + timedelta(minutes=cadence_min)
                expected_arrival = interval_start + timedelta(minutes=offset_min)

                # 90% normal on-time, 7% late, 2% SLA breach, 1% invalid/quarantine
                scenario = self.rng.random()

                if scenario < 0.90:
                    # Normal on-time arrival
                    actual_arrival = expected_arrival + timedelta(minutes=self.rng.uniform(-5, grace_min * 0.8))
                    pipeline_duration = self.rng.uniform(2, sla_min * 0.8)
                    actual_completion = actual_arrival + timedelta(minutes=pipeline_duration)
                    status = "SUCCESS"
                    rows = self.rng.randint(100, 50000)
                    record = {
                        "batch_id": f"batch_{ds_id}_{i:08d}",
                        "dataset_id": ds_id,
                        "schedule_interval_start": interval_start.isoformat(),
                        "schedule_interval_end": interval_end.isoformat(),
                        "expected_arrival_time": expected_arrival.isoformat(),
                        "actual_arrival_time": actual_arrival.isoformat(),
                        "actual_completion_time": actual_completion.isoformat(),
                        "max_event_timestamp": (interval_end - timedelta(seconds=self.rng.randint(1, 60))).isoformat(),
                        "row_count": rows,
                        "bytes_size": rows * 180,
                        "status": status,
                        "source_file": f"s3://warehouse/{ds_id}/batch_{i}.parquet",
                        "producer_id": f"prod-{ds_id}",
                    }
                    f.write(json.dumps(record) + "\n")

                elif scenario < 0.97:
                    # Late arrival
                    late_by_min = self.rng.uniform(grace_min + 5, grace_min + 60)
                    actual_arrival = expected_arrival + timedelta(minutes=late_by_min)
                    actual_completion = actual_arrival + timedelta(minutes=self.rng.uniform(5, sla_min * 0.9))
                    record = {
                        "batch_id": f"batch_{ds_id}_{i:08d}",
                        "dataset_id": ds_id,
                        "schedule_interval_start": interval_start.isoformat(),
                        "schedule_interval_end": interval_end.isoformat(),
                        "expected_arrival_time": expected_arrival.isoformat(),
                        "actual_arrival_time": actual_arrival.isoformat(),
                        "actual_completion_time": actual_completion.isoformat(),
                        "max_event_timestamp": interval_end.isoformat(),
                        "row_count": self.rng.randint(100, 20000),
                        "bytes_size": 1000000,
                        "status": "SUCCESS",
                        "source_file": f"s3://warehouse/{ds_id}/late_{i}.parquet",
                        "producer_id": f"prod-{ds_id}",
                    }
                    f.write(json.dumps(record) + "\n")

                elif scenario < 0.99:
                    # SLA breach (long duration or failed)
                    actual_arrival = expected_arrival + timedelta(minutes=2)
                    actual_completion = actual_arrival + timedelta(minutes=sla_min + self.rng.uniform(10, 120))
                    status = "FAILED" if self.rng.random() < 0.3 else "SUCCESS"
                    record = {
                        "batch_id": f"batch_{ds_id}_{i:08d}",
                        "dataset_id": ds_id,
                        "schedule_interval_start": interval_start.isoformat(),
                        "schedule_interval_end": interval_end.isoformat(),
                        "expected_arrival_time": expected_arrival.isoformat(),
                        "actual_arrival_time": actual_arrival.isoformat(),
                        "actual_completion_time": actual_completion.isoformat(),
                        "max_event_timestamp": interval_end.isoformat(),
                        "row_count": self.rng.randint(500, 10000),
                        "bytes_size": 800000,
                        "status": status,
                        "source_file": f"s3://warehouse/{ds_id}/breach_{i}.parquet",
                        "producer_id": f"prod-{ds_id}",
                    }
                    f.write(json.dumps(record) + "\n")

                else:
                    # Invalid quarantine record (e.g. malformed or negative row count)
                    if self.rng.random() < 0.5:
                        f.write(f'{{"batch_id": "malformed_{i}", "bad_json\n')
                    else:
                        bad_record = {
                            "batch_id": f"batch_bad_{i:08d}",
                            "dataset_id": ds_id,
                            "schedule_interval_start": interval_start.isoformat(),
                            "schedule_interval_end": interval_end.isoformat(),
                            "expected_arrival_time": expected_arrival.isoformat(),
                            "actual_arrival_time": expected_arrival.isoformat(),
                            "row_count": -99,
                        }
                        f.write(json.dumps(bad_record) + "\n")

                written += 1

        return written
