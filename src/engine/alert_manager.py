"""Alert lifecycle manager and deduplication engine."""

from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Dict, List
from rich.console import Console

from src.models import AlertEvent, AlertSeverity

logger = logging.getLogger("sla_sentinel.alert_manager")
console = Console()


class AlertManager:
    """Manages alert deduplication, routing, and notifications."""

    def __init__(self, output_dir: Optional[str | Path] = None):
        self.output_dir = Path(output_dir) if output_dir else Path("data/reports/alerts")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def deduplicate(self, alerts: List[AlertEvent]) -> List[AlertEvent]:
        """Deduplicates alerts by dedup_key, retaining the highest severity."""
        severity_rank = {
            AlertSeverity.CRITICAL: 3,
            AlertSeverity.WARNING: 2,
            AlertSeverity.INFO: 1,
        }

        unique_alerts: Dict[str, AlertEvent] = {}
        for alert in alerts:
            key = alert.dedup_key
            if key not in unique_alerts:
                unique_alerts[key] = alert
            else:
                existing = unique_alerts[key]
                if severity_rank[alert.severity] > severity_rank[existing.severity]:
                    unique_alerts[key] = alert

        return list(unique_alerts.values())

    def dispatch_alerts(self, alerts: List[AlertEvent], mock_webhook: bool = True) -> int:
        """Dispatches alerts to destinations (mock webhook and audit log files)."""
        if not alerts:
            return 0

        alert_dicts = [a.model_dump(mode="json") for a in alerts]
        export_file = self.output_dir / "active_alerts.json"
        with open(export_file, "w", encoding="utf-8") as f:
            json.dump(alert_dicts, f, indent=2)

        if mock_webhook:
            for alert in alerts:
                logger.info(
                    "DISPATCHED ALERT [%s] %s | Team: %s | Delay: %sm | Action: %s",
                    alert.severity.value,
                    alert.alert_title,
                    alert.owner_team,
                    alert.delay_minutes,
                    alert.recommended_action[:60] + "...",
                )

        return len(alerts)
