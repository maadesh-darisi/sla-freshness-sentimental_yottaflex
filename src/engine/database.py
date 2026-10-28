"""DuckDB storage layer with schema definitions, idempotent upserts, and transactional safety."""

from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import duckdb

from src.models import (
    AlertEvent,
    DatasetBatchRecord,
    DatasetFreshnessMetric,
    PipelineRunRecord,
    QuarantineRecord,
)


class DatabaseManager:
    """Manages DuckDB warehouse tables, indexes, and idempotent writes."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_tables()

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Returns a DuckDB connection configured with UTC TimeZone."""
        con = duckdb.connect(str(self.db_path))
        con.execute("SET TimeZone='UTC';")
        return con


    def _init_tables(self) -> None:
        """Initialize all warehouse tables with primary keys and indexes."""
        with self.get_connection() as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS curated_batches (
                    batch_id VARCHAR PRIMARY KEY,
                    dataset_id VARCHAR NOT NULL,
                    schedule_interval_start TIMESTAMP NOT NULL,
                    schedule_interval_end TIMESTAMP NOT NULL,
                    expected_arrival_time TIMESTAMP NOT NULL,
                    actual_arrival_time TIMESTAMP NOT NULL,
                    actual_completion_time TIMESTAMP,
                    max_event_timestamp TIMESTAMP,
                    row_count BIGINT NOT NULL,
                    bytes_size BIGINT NOT NULL,
                    status VARCHAR NOT NULL,
                    arrival_delay_seconds DOUBLE NOT NULL,
                    is_late_arrival BOOLEAN NOT NULL,
                    pipeline_duration_seconds DOUBLE,
                    sla_completion_deadline TIMESTAMP,
                    is_sla_breached BOOLEAN NOT NULL,
                    source_file VARCHAR NOT NULL,
                    producer_id VARCHAR NOT NULL,
                    ingestion_run_id VARCHAR NOT NULL,
                    processed_at TIMESTAMP NOT NULL
                );
            """)

            con.execute("""
                CREATE TABLE IF NOT EXISTS dataset_freshness_metrics (
                    metric_id VARCHAR PRIMARY KEY,
                    dataset_id VARCHAR NOT NULL,
                    as_of_time TIMESTAMP NOT NULL,
                    last_successful_batch_id VARCHAR,
                    last_successful_load_time TIMESTAMP,
                    max_event_timestamp TIMESTAMP,
                    freshness_lag_seconds DOUBLE NOT NULL,
                    freshness_lag_minutes DOUBLE NOT NULL,
                    max_acceptable_freshness_minutes DOUBLE NOT NULL,
                    freshness_status VARCHAR NOT NULL,
                    is_freshness_sla_breached BOOLEAN NOT NULL,
                    evaluated_run_id VARCHAR NOT NULL
                );
            """)

            con.execute("""
                CREATE TABLE IF NOT EXISTS alert_events (
                    alert_id VARCHAR PRIMARY KEY,
                    dataset_id VARCHAR NOT NULL,
                    alert_type VARCHAR NOT NULL,
                    severity VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    schedule_interval_start TIMESTAMP,
                    expected_time TIMESTAMP,
                    actual_time TIMESTAMP,
                    delay_minutes DOUBLE NOT NULL,
                    alert_title VARCHAR NOT NULL,
                    alert_message VARCHAR NOT NULL,
                    recommended_action VARCHAR NOT NULL,
                    owner_team VARCHAR NOT NULL,
                    dedup_key VARCHAR NOT NULL,
                    triggered_at TIMESTAMP NOT NULL,
                    run_id VARCHAR NOT NULL
                );
            """)

            con.execute("""
                CREATE TABLE IF NOT EXISTS quarantine_records (
                    quarantine_id VARCHAR PRIMARY KEY,
                    run_id VARCHAR NOT NULL,
                    source_file VARCHAR NOT NULL,
                    line_number INTEGER NOT NULL,
                    raw_record TEXT NOT NULL,
                    reject_code VARCHAR NOT NULL,
                    reject_reason TEXT NOT NULL,
                    rejected_at TIMESTAMP NOT NULL
                );
            """)

            con.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id VARCHAR PRIMARY KEY,
                    pipeline_name VARCHAR NOT NULL,
                    started_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    status VARCHAR NOT NULL,
                    input_records_count BIGINT NOT NULL,
                    valid_records_count BIGINT NOT NULL,
                    quarantined_records_count BIGINT NOT NULL,
                    alerts_generated_count BIGINT NOT NULL,
                    execution_duration_sec DOUBLE NOT NULL,
                    error_message TEXT
                );
            """)

            # Create secondary indexes for fast lookup and reconciliation
            con.execute("CREATE INDEX IF NOT EXISTS idx_batches_dataset ON curated_batches (dataset_id, schedule_interval_start);")
            con.execute("CREATE INDEX IF NOT EXISTS idx_alerts_dedup ON alert_events (dedup_key);")
            con.execute("CREATE INDEX IF NOT EXISTS idx_freshness_dataset ON dataset_freshness_metrics (dataset_id, as_of_time);")

    def upsert_batches(self, records: List[DatasetBatchRecord], chunk_size: int = 5000) -> int:
        """Idempotently insert or update curated batch records in chunks."""
        if not records:
            return 0

        with self.get_connection() as con:
            con.execute("BEGIN TRANSACTION;")
            try:
                batch_ids = [r.batch_id for r in records]
                # Chunked deletion to avoid exceeding SQL parameter limits on 100k+ records
                for i in range(0, len(batch_ids), chunk_size):
                    chunk_ids = batch_ids[i:i + chunk_size]
                    placeholders = ", ".join(["?"] * len(chunk_ids))
                    con.execute(f"DELETE FROM curated_batches WHERE batch_id IN ({placeholders});", chunk_ids)

                insert_sql = """
                    INSERT INTO curated_batches (
                        batch_id, dataset_id, schedule_interval_start, schedule_interval_end,
                        expected_arrival_time, actual_arrival_time, actual_completion_time,
                        max_event_timestamp, row_count, bytes_size, status, arrival_delay_seconds,
                        is_late_arrival, pipeline_duration_seconds, sla_completion_deadline,
                        is_sla_breached, source_file, producer_id, ingestion_run_id, processed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """

                for i in range(0, len(records), chunk_size):
                    chunk = records[i:i + chunk_size]
                    tuples = [
                        (
                            r.batch_id, r.dataset_id, r.schedule_interval_start, r.schedule_interval_end,
                            r.expected_arrival_time, r.actual_arrival_time, r.actual_completion_time,
                            r.max_event_timestamp, r.row_count, r.bytes_size, r.status, r.arrival_delay_seconds,
                            r.is_late_arrival, r.pipeline_duration_seconds, r.sla_completion_deadline,
                            r.is_sla_breached, r.source_file, r.producer_id, r.ingestion_run_id, r.processed_at
                        )
                        for r in chunk
                    ]
                    con.executemany(insert_sql, tuples)

                con.execute("COMMIT;")
                return len(records)
            except Exception:
                con.execute("ROLLBACK;")
                raise

    def upsert_alerts(self, alerts: List[AlertEvent], chunk_size: int = 5000) -> int:
        """Idempotently insert alerts using dedup_key in chunks."""
        if not alerts:
            return 0

        with self.get_connection() as con:
            con.execute("BEGIN TRANSACTION;")
            try:
                alert_ids = [a.alert_id for a in alerts]
                for i in range(0, len(alert_ids), chunk_size):
                    chunk_ids = alert_ids[i:i + chunk_size]
                    placeholders = ", ".join(["?"] * len(chunk_ids))
                    con.execute(f"DELETE FROM alert_events WHERE alert_id IN ({placeholders});", chunk_ids)

                insert_sql = """
                    INSERT INTO alert_events (
                        alert_id, dataset_id, alert_type, severity, status, schedule_interval_start,
                        expected_time, actual_time, delay_minutes, alert_title, alert_message,
                        recommended_action, owner_team, dedup_key, triggered_at, run_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """

                for i in range(0, len(alerts), chunk_size):
                    chunk = alerts[i:i + chunk_size]
                    tuples = [
                        (
                            a.alert_id, a.dataset_id, a.alert_type.value, a.severity.value, a.status.value,
                            a.schedule_interval_start, a.expected_time, a.actual_time, a.delay_minutes,
                            a.alert_title, a.alert_message, a.recommended_action, a.owner_team,
                            a.dedup_key, a.triggered_at, a.run_id
                        )
                        for a in chunk
                    ]
                    con.executemany(insert_sql, tuples)

                con.execute("COMMIT;")
                return len(alerts)
            except Exception:
                con.execute("ROLLBACK;")
                raise

    def insert_quarantine(self, records: List[QuarantineRecord], chunk_size: int = 5000) -> int:
        """Insert quarantined records idempotently in chunks."""
        if not records:
            return 0

        with self.get_connection() as con:
            con.execute("BEGIN TRANSACTION;")
            try:
                q_ids = [q.quarantine_id for q in records]
                for i in range(0, len(q_ids), chunk_size):
                    chunk_ids = q_ids[i:i + chunk_size]
                    placeholders = ", ".join(["?"] * len(chunk_ids))
                    con.execute(f"DELETE FROM quarantine_records WHERE quarantine_id IN ({placeholders});", chunk_ids)

                insert_sql = """
                    INSERT INTO quarantine_records (
                        quarantine_id, run_id, source_file, line_number, raw_record,
                        reject_code, reject_reason, rejected_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """

                for i in range(0, len(records), chunk_size):
                    chunk = records[i:i + chunk_size]
                    tuples = [
                        (
                            q.quarantine_id, q.run_id, q.source_file, q.line_number,
                            q.raw_record, q.reject_code.value, q.reject_reason, q.rejected_at
                        )
                        for q in chunk
                    ]
                    con.executemany(insert_sql, tuples)

                con.execute("COMMIT;")
                return len(records)
            except Exception:
                con.execute("ROLLBACK;")
                raise


    def upsert_freshness_metrics(self, metrics: List[DatasetFreshnessMetric]) -> int:
        """Insert freshness metric snapshots."""
        if not metrics:
            return 0

        with self.get_connection() as con:
            con.execute("BEGIN TRANSACTION;")
            try:
                metric_ids = [m.metric_id for m in metrics]
                placeholders = ", ".join(["?"] * len(metric_ids))
                con.execute(f"DELETE FROM dataset_freshness_metrics WHERE metric_id IN ({placeholders});", metric_ids)

                insert_sql = """
                    INSERT INTO dataset_freshness_metrics (
                        metric_id, dataset_id, as_of_time, last_successful_batch_id,
                        last_successful_load_time, max_event_timestamp, freshness_lag_seconds,
                        freshness_lag_minutes, max_acceptable_freshness_minutes, freshness_status,
                        is_freshness_sla_breached, evaluated_run_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """
                tuples = [
                    (
                        m.metric_id, m.dataset_id, m.as_of_time, m.last_successful_batch_id,
                        m.last_successful_load_time, m.max_event_timestamp, m.freshness_lag_seconds,
                        m.freshness_lag_minutes, m.max_acceptable_freshness_minutes,
                        m.freshness_status.value, m.is_freshness_sla_breached, m.evaluated_run_id
                    )
                    for m in metrics
                ]
                con.executemany(insert_sql, tuples)
                con.execute("COMMIT;")
                return len(metrics)
            except Exception:
                con.execute("ROLLBACK;")
                raise

    def record_pipeline_run(self, run: PipelineRunRecord) -> None:
        """Record or update a pipeline run audit log."""
        with self.get_connection() as con:
            con.execute("""
                INSERT OR REPLACE INTO pipeline_runs (
                    run_id, pipeline_name, started_at, completed_at, status,
                    input_records_count, valid_records_count, quarantined_records_count,
                    alerts_generated_count, execution_duration_sec, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, [
                run.run_id, run.pipeline_name, run.started_at, run.completed_at, run.status.value,
                run.input_records_count, run.valid_records_count, run.quarantined_records_count,
                run.alerts_generated_count, run.execution_duration_sec, run.error_message
            ])
