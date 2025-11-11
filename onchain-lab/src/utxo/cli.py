from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import pyarrow.parquet as pq
import typer
from rich.console import Console
from rich.table import Table

from .builder import LifecycleBuilder
from .config import ConfigError, LifecycleConfig, load_config
from .datasets import pipeline_version, read_created, read_spent, snapshot_path
from .qa import LifecycleQA
from .snapshots import SnapshotBuilder

app = typer.Typer(help="UTXO lifecycle pipeline CLI")
console = Console()


def _load_config(path: Optional[Path]) -> LifecycleConfig:
    try:
        return load_config(path)
    except ConfigError as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc


@app.command("build-lifecycle")
def build_lifecycle(config: Optional[Path] = typer.Option(None, "--config", help="Path to utxo.yaml")) -> None:
    cfg = _load_config(config)
    builder = LifecycleBuilder(cfg)
    result = builder.build(persist=True)

    console.print(
        f"[green]Lifecycle build complete[/green] (created={result.artifacts.created.num_rows} rows, "
        f"spent={result.artifacts.spent.num_rows} rows, pipeline={pipeline_version()})"
    )


@app.command("build-snapshots")
def build_snapshots(
    config: Optional[Path] = typer.Option(None, "--config", help="Path to utxo.yaml"),
    start: Optional[str] = typer.Option(None, help="Start date (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, help="End date (YYYY-MM-DD)"),
) -> None:
    cfg = _load_config(config)
    created = read_created(cfg.data.lifecycle_root)
    spent = read_spent(cfg.data.lifecycle_root)
    builder = SnapshotBuilder(cfg)
    start_date = date.fromisoformat(start) if start else None
    end_date = date.fromisoformat(end) if end else None
    snapshots = builder.build(created, spent, start_date=start_date, end_date=end_date, persist=True)
    console.print(f"[green]Generated {len(snapshots)} snapshot days[/green]")


@app.command("qa")
def qa(config: Optional[Path] = typer.Option(None, "--config", help="Path to utxo.yaml")) -> None:
    cfg = _load_config(config)
    qa_runner = LifecycleQA(cfg)
    results = qa_runner.run()

    table = Table(title="Lifecycle QA")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Details")

    for result in results:
        status = "PASS" if result.passed else "FAIL"
        detail_summary = ", ".join(f"{k}={v}" for k, v in result.details.items())
        table.add_row(result.name, status, detail_summary)

    console.print(table)

    if not all(result.passed for result in results):
        raise typer.Exit(code=2)


@app.command("show-snapshot")
def show_snapshot(
    snapshot_date: str,
    config: Optional[Path] = typer.Option(None, "--config", help="Path to utxo.yaml"),
) -> None:
    cfg = _load_config(config)
    day = date.fromisoformat(snapshot_date)
    path = snapshot_path(cfg.data.lifecycle_root, day)
    if not path.exists():
        typer.secho(f"Snapshot not found: {path}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1)
    table = pq.read_table(path)
    console.print(f"Snapshot {snapshot_date} — {table.num_rows} rows")
    if table.num_rows:
        console.print(table.to_pandas())


@app.command("audit-supply")
def audit_supply(config: Optional[Path] = typer.Option(None, "--config", help="Path to utxo.yaml")) -> None:
    cfg = _load_config(config)
    qa_runner = LifecycleQA(cfg)
    results = qa_runner.run()
    supply = next(result for result in results if result.name == "supply_reconciliation")
    status = "PASS" if supply.passed else "FAIL"
    console.print(f"Supply reconciliation: {status}")
    for key, value in supply.details.items():
        console.print(f" - {key}: {value}")
    if not supply.passed:
        raise typer.Exit(code=2)


__all__ = ["app"]
