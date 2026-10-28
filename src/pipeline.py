"""Main Pipeline Orchestrator for SLA and Freshness Sentinel."""

from __future__ import annotations
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.config import AppConfig, load_config
from src.engine import AlertManager, DatabaseManager, FreshnessEngine, SLADetector
from src.ingestion import BatchValidator, IngestionReader
from src.models import (
    AlertEvent,
    DatasetBatchRecord,
    DatasetFreshnessMetric,
    PipelineRunRecord,
    PipelineStatus,
    QuarantineRecord,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("sla_sentinel.pipeline")


class SLAPipeline:
    """Orchestrates ingestion, validation, SLA detection, freshness evaluation, and alerting."""

    def __init__(self, config: Optional[AppConfig] = None):
        self.config = config or load_config()
        self.db = DatabaseManager(self.config.pipeline.database_path)
        self.alert_manager = AlertManager(self.config.pipeline.reports_dir)

    def run(
        self,
        input_file: Optional[str | Path] = None,
        evaluation_time: Optional[datetime] = None,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Executes full vertical slice pipeline and returns summary metrics."""
        start_wall_time = time.time()
        eval_dt = evaluation_time or self.config.pipeline.freshness_evaluation_timestamp or datetime.now(timezone.utc)
        active_run_id = run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

        logger.info("Starting SLA Pipeline run: %s | Evaluation Time: %s", active_run_id, eval_dt.isoformat())

        run_record = PipelineRunRecord(
            run_id=active_run_id,
            pipeline_name="sla_freshness_pipeline",
            started_at=datetime.now(timezone.utc),
            status=PipelineStatus.SUCCESS,
        )

        all_curated_batches: List[DatasetBatchRecord] = []
        all_quarantine_records: List[QuarantineRecord] = []
        all_alerts: List[AlertEvent] = []
        all_freshness_metrics: List[DatasetFreshnessMetric] = []

        try:
            # Step 1: Ingestion & Validation (if input_file is provided)
            if input_file:
                input_path = Path(input_file)
                logger.info("Ingesting batch file: %s", input_path)
                reader = IngestionReader()
                validator = BatchValidator(
                    catalog=self.config.catalog,
                    run_id=active_run_id,
                    source_file=str(input_path),
                )

                stream = reader.read_file(input_path)
                val_result = validator.validate_stream(stream)

                run_record.input_records_count = val_result.total_input_count
                run_record.valid_records_count = len(val_result.valid_records)
                run_record.quarantined_records_count = len(val_result.quarantined_records)
                all_quarantine_records = val_result.quarantined_records

                # Persist quarantined records to DuckDB and quarantine log file
                if all_quarantine_records:
                    self.db.insert_quarantine(all_quarantine_records)
                    self._dump_quarantine_file(all_quarantine_records, active_run_id)
                    logger.warning("Quarantined %d invalid records into reject store", len(all_quarantine_records))

                # Step 2: Transform Valid Batches and Detect Late Arrivals / Processing SLA Breaches
                detector = SLADetector(catalog=self.config.catalog, run_id=active_run_id)
                curated, batch_alerts = detector.process_raw_batches(
                    raw_records=val_result.valid_records,
                    evaluation_time=eval_dt,
                )
                all_curated_batches = curated
                all_alerts.extend(batch_alerts)

                # Persist curated batches idempotently
                self.db.upsert_batches(all_curated_batches)
                logger.info("Upserted %d curated batch records into warehouse", len(all_curated_batches))

            # Step 3: Missing Load Detection across all catalog datasets
            detector = SLADetector(catalog=self.config.catalog, run_id=active_run_id)
            with self.db.get_connection() as con:
                missing_alerts = detector.detect_missing_loads(
                    db_con=con,
                    evaluation_time=eval_dt,
                    lookback_hours=48,
                )
            all_alerts.extend(missing_alerts)
            logger.info("Detected %d missing load incidents", len(missing_alerts))

            # Step 4: Freshness Calculations & Staleness Alerting
            freshness_engine = FreshnessEngine(catalog=self.config.catalog, run_id=active_run_id)
            with self.db.get_connection() as con:
                metrics, freshness_alerts = freshness_engine.evaluate_freshness(
                    db_con=con,
                    evaluation_time=eval_dt,
                )
            all_freshness_metrics = metrics
            all_alerts.extend(freshness_alerts)
            self.db.upsert_freshness_metrics(all_freshness_metrics)
            logger.info("Calculated freshness metrics for %d datasets", len(all_freshness_metrics))

            # Step 5: Deduplicate and Persist Actionable Alerts
            deduped_alerts = self.alert_manager.deduplicate(all_alerts)
            self.db.upsert_alerts(deduped_alerts)
            self.alert_manager.dispatch_alerts(deduped_alerts, mock_webhook=self.config.pipeline.enable_alert_webhook_mock)
            run_record.alerts_generated_count = len(deduped_alerts)

            # Check if there were any quarantines; mark PARTIAL_SUCCESS if so
            if run_record.quarantined_records_count > 0:
                run_record.status = PipelineStatus.PARTIAL_SUCCESS

        except Exception as exc:
            logger.exception("Pipeline execution failed: %s", exc)
            run_record.status = PipelineStatus.FAILED
            run_record.error_message = str(exc)
            raise
        finally:
            run_record.completed_at = datetime.now(timezone.utc)
            run_record.execution_duration_sec = round(time.time() - start_wall_time, 4)
            self.db.record_pipeline_run(run_record)

        summary = {
            "run_id": active_run_id,
            "status": run_record.status.value,
            "duration_sec": run_record.execution_duration_sec,
            "input_records": run_record.input_records_count,
            "valid_records": run_record.valid_records_count,
            "quarantined_records": run_record.quarantined_records_count,
            "alerts_generated": run_record.alerts_generated_count,
            "freshness_metrics_count": len(all_freshness_metrics),
        }
        logger.info("Pipeline run finished: %s", json.dumps(summary))
        return summary

    def _dump_quarantine_file(self, records: List[QuarantineRecord], run_id: str) -> None:
        """Dumps quarantined records to disk for audit investigation."""
        q_dir = Path(self.config.pipeline.quarantine_dir)
        q_dir.mkdir(parents=True, exist_ok=True)
        dump_path = q_dir / f"quarantine_{run_id}.jsonl"
        with open(dump_path, "w", encoding="utf-8") as f:
            for r in records:
                f.write(r.model_dump_json() + "\n")
