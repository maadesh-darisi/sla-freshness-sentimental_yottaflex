"""Utility functions for timestamp normalization and formatting."""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional


def to_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Ensures a datetime object is offset-aware in UTC."""
    if dt is None:
        return None
    if isinstance(dt, str):
        # Handle ISO strings
        clean_str = dt.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean_str)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
