"""Performance measurement and optimization benchmarking tool."""

from __future__ import annotations
import gc
import json
import time
from pathlib import Path
from typing import Any, Dict, List
import duckdb

from src.generator.synthetic_data import SyntheticDataGenerator
from src.engine.database import DatabaseManager
from src.config import load_config
from src.models import DatasetBatchRecord


class PerformanceBenchmark:
    """Measures ingestion and query throughput, profiling baseline vs optimized bulk methods."""

    def __init__(self, output_dir: str | Path = "data/reports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.config = load_config()

    def run_benchmark(self, record_count: int = 50000) -> Dict[str, Any]:
        """Runs comparative benchmark on high-volume synthetic dataset."""
        benchmark_file = Path("data/raw/benchmark_input.jsonl")
        benchmark_file.parent.mkdir(parents=True, exist_ok=True)

        print(f"Generating {record_count:,} synthetic records for benchmark...")
        gen = SyntheticDataGenerator(seed=123)
        gen.generate_large_scale_dataset(benchmark_file, record_count=record_count)

        # Baseline: Row-by-Row Unbatched Transactions (sampled on smaller subset for practical test)
        sample_size = min(5000, record_count)
        baseline_db_path = Path("data/warehouse/benchmark_baseline.duckdb")
        if baseline_db_path.exists():
            baseline_db_path.unlink()

        print(f"Benchmarking Baseline (Individual row-by-row transactions on {sample_size:,} records)...")
        t_base_start = time.perf_counter()
        with duckdb.connect(str(baseline_db_path)) as con:
            con.execute("""
                CREATE TABLE curated_batches (
                    batch_id VARCHAR PRIMARY KEY,
                    dataset_id VARCHAR,
                    schedule_interval_start TIMESTAMP,
                    schedule_interval_end TIMESTAMP,
                    expected_arrival_time TIMESTAMP,
                    actual_arrival_time TIMESTAMP,
                    actual_completion_time TIMESTAMP,
                    max_event_timestamp TIMESTAMP,
                    row_count BIGINT,
                    bytes_size BIGINT,
                    status VARCHAR,
                    arrival_delay_seconds DOUBLE,
                    is_late_arrival BOOLEAN,
                    pipeline_duration_seconds DOUBLE,
                    sla_completion_deadline TIMESTAMP,
                    is_sla_breached BOOLEAN,
                    source_file VARCHAR,
                    producer_id VARCHAR,
                    ingestion_run_id VARCHAR,
                    processed_at TIMESTAMP
                );
            """)

            with open(benchmark_file, "r", encoding="utf-8") as f:
                for idx, line in enumerate(f):
                    if idx >= sample_size:
                        break
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if "batch_id" in row and row.get("row_count", 0) >= 0:

                        # Individual insert with autocommit overhead
                        con.execute("""
                            INSERT INTO curated_batches VALUES (
                                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
                            );
                        """, [
                            row["batch_id"], row["dataset_id"], row["schedule_interval_start"],
                            row["schedule_interval_end"], row["expected_arrival_time"],
                            row["actual_arrival_time"], row.get("actual_completion_time"),
                            row.get("max_event_timestamp"), row["row_count"], row.get("bytes_size", 0),
                            row.get("status", "SUCCESS"), 0.0, False, 10.0,
                            row["actual_arrival_time"], False, row.get("source_file", "src"),
                            row.get("producer_id", "prod"), "bench_run"
                        ])

        t_base_elapsed = time.perf_counter() - t_base_start
        baseline_rps = round(sample_size / max(0.0001, t_base_elapsed), 2)

        # Optimized Approach: Streamed Vectorized Batch Chunking with Single Transaction
        optimized_db_path = Path("data/warehouse/benchmark_optimized.duckdb")
        if optimized_db_path.exists():
            optimized_db_path.unlink()

        print(f"Benchmarking Optimized (Vectorized batch chunking on full {record_count:,} records)...")
        gc.collect()
        t_opt_start = time.perf_counter()

        with duckdb.connect(str(optimized_db_path)) as con:
            con.execute("""
                CREATE TABLE curated_batches (
                    batch_id VARCHAR PRIMARY KEY,
                    dataset_id VARCHAR,
                    schedule_interval_start TIMESTAMP,
                    schedule_interval_end TIMESTAMP,
                    expected_arrival_time TIMESTAMP,
                    actual_arrival_time TIMESTAMP,
                    actual_completion_time TIMESTAMP,
                    max_event_timestamp TIMESTAMP,
                    row_count BIGINT,
                    bytes_size BIGINT,
                    status VARCHAR,
                    arrival_delay_seconds DOUBLE,
                    is_late_arrival BOOLEAN,
                    pipeline_duration_seconds DOUBLE,
                    sla_completion_deadline TIMESTAMP,
                    is_sla_breached BOOLEAN,
                    source_file VARCHAR,
                    producer_id VARCHAR,
                    ingestion_run_id VARCHAR,
                    processed_at TIMESTAMP
                );
            """)

            chunk_size = 10000
            batch_tuples: List[tuple] = []
            valid_processed = 0

            insert_query = """
                INSERT INTO curated_batches VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
                );
            """

            with open(benchmark_file, "r", encoding="utf-8") as f:
                con.execute("BEGIN TRANSACTION;")
                for line in f:
                    try:
                        row = json.loads(line)
                        if "batch_id" in row and row.get("row_count", 0) >= 0:
                            tup = (
                                row["batch_id"], row["dataset_id"], row["schedule_interval_start"],
                                row["schedule_interval_end"], row["expected_arrival_time"],
                                row["actual_arrival_time"], row.get("actual_completion_time"),
                                row.get("max_event_timestamp"), row["row_count"], row.get("bytes_size", 0),
                                row.get("status", "SUCCESS"), 0.0, False, 10.0,
                                row["actual_arrival_time"], False, row.get("source_file", "src"),
                                row.get("producer_id", "prod"), "bench_run"
                            )
                            batch_tuples.append(tup)
                            valid_processed += 1

                            if len(batch_tuples) >= chunk_size:
                                con.executemany(insert_query, batch_tuples)
                                batch_tuples = []
                    except Exception:
                        continue

                if batch_tuples:
                    con.executemany(insert_query, batch_tuples)
                con.execute("COMMIT;")

        t_opt_elapsed = time.perf_counter() - t_opt_start
        optimized_rps = round(valid_processed / max(0.0001, t_opt_elapsed), 2)
        speedup = round(optimized_rps / max(0.01, baseline_rps), 2)

        results = {
            "record_count_tested": record_count,
            "baseline": {
                "method": "Row-by-Row unbatched autocommit transactions",
                "sample_records": sample_size,
                "elapsed_seconds": round(t_base_elapsed, 3),
                "throughput_records_per_sec": baseline_rps,
            },
            "optimized": {
                "method": "Vectorized 10,000-row chunked executemany with single transaction",
                "records_processed": valid_processed,
                "elapsed_seconds": round(t_opt_elapsed, 3),
                "throughput_records_per_sec": optimized_rps,
            },
            "speedup_factor": f"{speedup}x",
            "optimization_analysis": (
                "Vectorized chunking avoids per-row database round-trips, transaction lock acquisitions, "
                "and Python call-stack overhead. Grouping into 10k tuples with DuckDB executemany achieves "
                f"{optimized_rps:,} rows/sec vs {baseline_rps:,} rows/sec ({speedup}x speedup)."
            )
        }

        # Write results to reports directory
        bench_report_path = self.output_dir / "performance_benchmark.json"
        with open(bench_report_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

        return results
