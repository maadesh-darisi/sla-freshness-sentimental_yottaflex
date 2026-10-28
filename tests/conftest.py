"""Pytest fixtures for SLA Sentinel test suite."""

from __future__ import annotations
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
import pytest
import yaml

from src.config import AppConfig, DatasetSLAConfig, PipelineSettings, SLACatalog
from src.engine.database import DatabaseManager


@pytest.fixture
def sample_catalog() -> SLACatalog:
    """Fixture providing a mock SLA catalog with known thresholds."""
    return SLACatalog(
        datasets=[
            DatasetSLAConfig(
                dataset_id="test_orders_hourly",
                dataset_name="Test Hourly Orders",
                domain="Sales",
                cadence="hourly",
                cadence_minutes=60,
                schedule_offset_minutes=15,
                arrival_grace_minutes=10,
                sla_duration_minutes=30,
                max_acceptable_freshness_minutes=120.0,
                criticality_tier="P1",
                target_table="curated_test_orders",
                primary_keys=["batch_id"],
                owner_team="sales-team@test.com",
                sla_enabled=True,
            ),
            DatasetSLAConfig(
                dataset_id="test_payments_daily",
                dataset_name="Test Daily Payments",
                domain="Finance",
                cadence="daily",
                cadence_minutes=1440,
                schedule_offset_minutes=360,
                arrival_grace_minutes=30,
                sla_duration_minutes=60,
                max_acceptable_freshness_minutes=1500.0,
                criticality_tier="P1",
                target_table="curated_test_payments",
                primary_keys=["batch_id"],
                owner_team="finance-team@test.com",
                sla_enabled=True,
            ),
            DatasetSLAConfig(
                dataset_id="test_sensors_15m",
                dataset_name="Test Sensor Stream 15m",
                domain="IoT",
                cadence="custom_interval",
                cadence_minutes=15,
                schedule_offset_minutes=5,
                arrival_grace_minutes=5,
                sla_duration_minutes=10,
                max_acceptable_freshness_minutes=30.0,
                criticality_tier="P3",
                target_table="curated_test_sensors",
                primary_keys=["batch_id"],
                owner_team="iot-team@test.com",
                sla_enabled=True,
            ),
        ]
    )


@pytest.fixture
def temp_workspace(tmp_path: Path, sample_catalog: SLACatalog) -> AppConfig:
    """Fixture providing isolated temporary directories and database for testing."""
    db_path = tmp_path / "test_warehouse.duckdb"
    quarantine_dir = tmp_path / "quarantine"
    reports_dir = tmp_path / "reports"
    catalog_path = tmp_path / "test_catalog.yaml"

    quarantine_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    with open(catalog_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(sample_catalog.model_dump(), f)

    settings = PipelineSettings(
        environment="test",
        database_path=str(db_path),
        quarantine_dir=str(quarantine_dir),
        reports_dir=str(reports_dir),
        catalog_path=str(catalog_path),
        enable_alert_webhook_mock=False,
    )
    return AppConfig(pipeline=settings, catalog=sample_catalog)


@pytest.fixture
def db_manager(temp_workspace: AppConfig) -> DatabaseManager:
    """Fixture providing an initialized DatabaseManager."""
    return DatabaseManager(temp_workspace.pipeline.database_path)
