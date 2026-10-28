"""Configuration loader and schema definitions for SLA Sentinel."""

from __future__ import annotations
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import yaml
from pydantic import BaseModel, Field


class DatasetSLAConfig(BaseModel):
    """Configuration definition for a single monitored dataset."""
    dataset_id: str
    dataset_name: str
    domain: str
    cadence: str = "hourly"
    cadence_minutes: int = 60
    schedule_offset_minutes: int = 15
    arrival_grace_minutes: int = 10
    sla_duration_minutes: int = 30
    max_acceptable_freshness_minutes: float = 120.0
    criticality_tier: str = "P2"
    target_table: str
    primary_keys: List[str] = Field(default_factory=lambda: ["id"])
    owner_team: str = "data-platform@company.internal"
    sla_enabled: bool = True


class SLACatalog(BaseModel):
    """Catalog of all monitored dataset SLA configurations."""
    datasets: List[DatasetSLAConfig]

    def get_dataset(self, dataset_id: str) -> Optional[DatasetSLAConfig]:
        for d in self.datasets:
            if d.dataset_id == dataset_id:
                return d
        return None

    def get_dataset_ids(self) -> List[str]:
        return [d.dataset_id for d in self.datasets]


class PipelineSettings(BaseModel):
    """Pipeline runtime settings."""
    environment: str = "local"
    database_path: str = "data/warehouse/sla_sentinel.duckdb"
    quarantine_dir: str = "data/warehouse/quarantine"
    reports_dir: str = "data/reports"
    catalog_path: str = "config/datasets_sla_catalog.yaml"
    log_level: str = "INFO"
    batch_size: int = 50000
    enable_quarantine_dump: bool = True
    enable_alert_webhook_mock: bool = True
    freshness_evaluation_timestamp: Optional[datetime] = None


class AppConfig(BaseModel):
    """Top-level unified application configuration."""
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    catalog: SLACatalog = Field(default_factory=lambda: SLACatalog(datasets=[]))


def load_catalog(catalog_path: str | Path) -> SLACatalog:
    """Load dataset SLA catalog from YAML file."""
    path = Path(catalog_path)
    if not path.exists():
        raise FileNotFoundError(f"SLA Catalog file not found at: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    return SLACatalog(**data)


def load_config(config_path: Optional[str | Path] = None) -> AppConfig:
    """Load unified configuration from YAML and load corresponding catalog."""
    default_config_path = Path("config/default_config.yaml")
    cfg_file = Path(config_path) if config_path else default_config_path

    pipeline_settings = PipelineSettings()
    if cfg_file.exists():
        with open(cfg_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            if "pipeline" in data:
                pipeline_settings = PipelineSettings(**data["pipeline"])

    # Load catalog referenced in settings
    catalog_path = Path(pipeline_settings.catalog_path)
    if not catalog_path.is_absolute() and not catalog_path.exists():
        # Try resolving relative to workspace
        alt_path = Path(__file__).resolve().parent.parent / pipeline_settings.catalog_path
        if alt_path.exists():
            catalog_path = alt_path

    catalog = load_catalog(catalog_path) if catalog_path.exists() else SLACatalog(datasets=[])
    return AppConfig(pipeline=pipeline_settings, catalog=catalog)
