"""Data reader supporting JSON, JSONL, CSV, and Parquet with line-level traceability."""

from __future__ import annotations
import csv
import json
from pathlib import Path
from typing import Any, Dict, Generator, List, Tuple


class IngestionReader:
    """Reads dataset arrival files line-by-line or record-by-record, preserving source lines."""

    @staticmethod
    def read_file(file_path: str | Path) -> Generator[Tuple[int, Any, Optional[str]], None, None]:
        """Yields (line_number, parsed_record_or_raw, raw_string_content)."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Input file does not exist: {path}")

        suffix = path.suffix.lower()

        if suffix == ".jsonl":
            with open(path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, start=1):
                    raw_str = line.strip()
                    if not raw_str:
                        continue
                    try:
                        parsed = json.loads(raw_str)
                        yield (line_num, parsed, raw_str)
                    except json.JSONDecodeError as exc:
                        # Yield the raw string with JSONDecodeError for quarantine routing
                        yield (line_num, exc, raw_str)

        elif suffix == ".json":
            with open(path, "r", encoding="utf-8") as f:
                raw_content = f.read().strip()
                if not raw_content:
                    return
                try:
                    data = json.loads(raw_content)
                    if isinstance(data, list):
                        for idx, item in enumerate(data, start=1):
                            yield (idx, item, json.dumps(item))
                    elif isinstance(data, dict):
                        yield (1, data, raw_content)
                    else:
                        yield (1, ValueError(f"Unexpected JSON root type: {type(data)}"), raw_content)
                except json.JSONDecodeError as exc:
                    yield (1, exc, raw_content)

        elif suffix == ".csv":
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for line_num, row in enumerate(reader, start=2): # header is line 1
                    raw_str = json.dumps(row)
                    yield (line_num, dict(row), raw_str)

        else:
            raise ValueError(f"Unsupported file format: {suffix}. Supported: .jsonl, .json, .csv")
