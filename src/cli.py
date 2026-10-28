"""CLI interface for SLA Sentinel Data Engineering Platform."""

from __future__ import annotations
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add project root to sys.path for direct CLI script execution
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from rich.console import Console
from rich.panel import Panel
from rich.table import Table


from src.config import load_config
from src.generator import SyntheticDataGenerator
from src.pipeline import SLAPipeline
from src.reconciliation import ReconciliationManager
from src.benchmark import PerformanceBenchmark

console = Console()


def print_banner():
    console.print(
        Panel.fit(
            "[bold cyan]SLA & Freshness Sentinel[/bold cyan]\n"
            "[dim]Production Data Engineering Pipeline for Arrival Tracking, Freshness & Alerts[/dim]",
            border_style="cyan",
        )
    )


def cmd_generate_sample(args):
    print_banner()
    console.print("[yellow]Generating starter sample dataset with ground-truth expected labels...[/yellow]")
    gen = SyntheticDataGenerator(seed=args.seed)
    count, labels = gen.generate_starter_sample(
        output_file=args.output,
        labels_file=args.labels,
    )
    console.print(f"[green]Successfully generated {count} starter records.[/green]")
    console.print(f"Data file: [bold]{args.output}[/bold]")
    console.print(f"Labels file: [bold]{args.labels}[/bold]")
    console.print(f"Expected Valid: [cyan]{labels['expected_valid']}[/cyan] | Expected Quarantine: [red]{labels['expected_quarantine']}[/red]")


def cmd_generate_large(args):
    print_banner()
    console.print(f"[yellow]Generating scalable dataset with {args.count:,} records (seed={args.seed})...[/yellow]")
    gen = SyntheticDataGenerator(seed=args.seed)
    count = gen.generate_large_scale_dataset(
        output_file=args.output,
        record_count=args.count,
    )
    console.print(f"[green]Generated {count:,} records at [bold]{args.output}[/bold].[/green]")


def cmd_run(args):
    print_banner()
    cfg = load_config(args.config)
    if args.db:
        cfg.pipeline.database_path = args.db

    eval_time = None
    if args.eval_time:
        eval_time = datetime.fromisoformat(args.eval_time)

    pipeline = SLAPipeline(config=cfg)
    summary = pipeline.run(
        input_file=args.input,
        evaluation_time=eval_time,
        run_id=args.run_id,
    )

    table = Table(title="Pipeline Execution Summary", border_style="green")
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    for k, v in summary.items():
        table.add_row(str(k), str(v))
    console.print(table)

    # If reconcile requested, run reconciliation report
    if args.reconcile:
        rec_mgr = ReconciliationManager(pipeline.db, cfg.catalog, cfg.pipeline.reports_dir)
        rec = rec_mgr.reconcile_run(summary["run_id"])
        report_path = rec_mgr.generate_markdown_report(rec)
        console.print(f"[bold green]Reconciliation report written to: {report_path}[/bold green]")


def cmd_reconcile(args):
    print_banner()
    cfg = load_config(args.config)
    pipeline = SLAPipeline(config=cfg)

    run_id = args.run_id
    if not run_id:
        # Find latest run ID from database
        with pipeline.db.get_connection() as con:
            latest = con.execute("SELECT run_id FROM pipeline_runs ORDER BY started_at DESC LIMIT 1;").fetchone()
            if not latest:
                console.print("[red]No pipeline runs found in database.[/red]")
                return
            run_id = latest[0]

    console.print(f"[cyan]Running reconciliation audit for run: [bold]{run_id}[/bold][/cyan]")
    rec_mgr = ReconciliationManager(pipeline.db, cfg.catalog, cfg.pipeline.reports_dir)
    rec = rec_mgr.reconcile_run(run_id)
    report_path = rec_mgr.generate_markdown_report(rec)

    table = Table(title="Reconciliation Audit & Conservation Check", border_style="cyan")
    table.add_column("Audit Check", style="bold")
    table.add_column("Status / Value", style="white")

    table.add_row("Run ID", rec["run_id"])
    table.add_row("Pipeline Status", rec["pipeline_status"])
    table.add_row("Conservation Equation", "[green]PASSED[/green]" if rec["conservation_equation_passed"] else "[red]FAILED[/red]")
    table.add_row("Input Records Count", str(rec["input_records_count"]))
    table.add_row("Curated Batches In DB", str(rec["curated_records_in_db"]))
    table.add_row("Quarantined Records In DB", str(rec["quarantine_records_in_db"]))
    table.add_row("Curated PK Duplicates", str(rec["primary_key_duplicates_curated"]))
    table.add_row("Alert PK Duplicates", str(rec["primary_key_duplicates_alerts"]))
    table.add_row("Referential Integrity Violations", str(rec["referential_integrity_violations"]))

    console.print(table)
    console.print(f"[bold green]Markdown validation report saved to: {report_path}[/bold green]")


def cmd_benchmark(args):
    print_banner()
    console.print(f"[yellow]Executing Performance Benchmark on {args.count:,} records...[/yellow]")
    bench = PerformanceBenchmark(output_dir="data/reports")
    results = bench.run_benchmark(record_count=args.count)

    table = Table(title="Performance Benchmark Results", border_style="magenta")
    table.add_column("Implementation", style="cyan")
    table.add_column("Records Tested", style="white")
    table.add_column("Elapsed Time", style="white")
    table.add_column("Throughput", style="bold green")

    b = results["baseline"]
    o = results["optimized"]
    table.add_row(b["method"], f"{b['sample_records']:,}", f"{b['elapsed_seconds']}s", f"{b['throughput_records_per_sec']:,} rows/sec")
    table.add_row(o["method"], f"{o['records_processed']:,}", f"{o['elapsed_seconds']}s", f"{o['throughput_records_per_sec']:,} rows/sec")
    table.add_row("[bold yellow]Speedup Factor[/bold yellow]", "", "", f"[bold yellow]{results['speedup_factor']}[/bold yellow]")

    console.print(table)
    console.print(f"\n[bold]Optimization Analysis:[/bold]\n{results['optimization_analysis']}")


def main():
    parser = argparse.ArgumentParser(description="SLA and Freshness Sentinel CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # generate-sample
    p_sample = subparsers.add_parser("generate-sample", help="Generate small starter sample dataset")
    p_sample.add_argument("--output", default="data/sample/starter_batches.jsonl", help="Output JSONL path")
    p_sample.add_argument("--labels", default="data/sample/expected_labels.json", help="Expected labels path")
    p_sample.add_argument("--seed", type=int, default=42, help="Random seed")
    p_sample.set_defaults(func=cmd_generate_sample)

    # generate-large
    p_large = subparsers.add_parser("generate-large", help="Generate scalable dataset (100k+ records)")
    p_large.add_argument("--output", default="data/raw/large_batches.jsonl", help="Output path")
    p_large.add_argument("--count", type=int, default=100000, help="Number of records")
    p_large.add_argument("--seed", type=int, default=42, help="Random seed")
    p_large.set_defaults(func=cmd_generate_large)

    # run
    p_run = subparsers.add_parser("run", help="Run ingestion, SLA detection & freshness pipeline")
    p_run.add_argument("--input", default="data/sample/starter_batches.jsonl", help="Input file path")
    p_run.add_argument("--config", default="config/default_config.yaml", help="Config file path")
    p_run.add_argument("--eval-time", default=None, help="Evaluation timestamp ISO-8601")
    p_run.add_argument("--db", default=None, help="DuckDB warehouse database path override")
    p_run.add_argument("--run-id", default=None, help="Custom run ID")
    p_run.add_argument("--reconcile", action="store_true", help="Automatically run reconciliation after run")
    p_run.set_defaults(func=cmd_run)

    # reconcile
    p_rec = subparsers.add_parser("reconcile", help="Run reconciliation queries and generate report")
    p_rec.add_argument("--run-id", default=None, help="Run ID to reconcile (defaults to latest)")
    p_rec.add_argument("--config", default="config/default_config.yaml", help="Config file path")
    p_rec.set_defaults(func=cmd_reconcile)

    # benchmark
    p_bench = subparsers.add_parser("benchmark", help="Run performance & optimization benchmark")
    p_bench.add_argument("--count", type=int, default=50000, help="Number of records to benchmark")
    p_bench.set_defaults(func=cmd_benchmark)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
