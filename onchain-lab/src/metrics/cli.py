from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from .compute import MetricsBuildError, build_daily_metrics
from .config import ConfigError, MetricsConfig, load_config
from .docs import generate_metric_docs
from .golden import generate_golden_artifacts
from .inspect import InspectionSummary, inspect_metric
from .registry import load_metric_definitions

app = typer.Typer(help="Metrics engine CLI", add_completion=False)
_console = Console()


def _config(path: Optional[Path]) -> MetricsConfig:
    try:
        return load_config(path)
    except ConfigError as exc:
        typer.echo(f"Configuration error: {exc}")
        raise typer.Exit(code=1) from exc


@app.command()
def build(
    config_path: Optional[Path] = typer.Option(None, "--config"),
    registry_path: Optional[Path] = typer.Option(
        Path("config/metrics_registry.yaml"), "--registry"
    ),
) -> None:
    """Build the daily metrics dataset and run QA checks."""

    cfg = _config(config_path)
    try:
        result = build_daily_metrics(config=cfg, registry_path=registry_path)
    except MetricsBuildError as exc:
        typer.echo(f"Metrics build failed: {exc}")
        raise typer.Exit(code=2) from exc

    typer.echo(
        f"Wrote {result.rows} rows to {result.output_path} (lineage={result.lineage_id})"
    )
    if registry_path is not None:
        provenance = result.provenance
        typer.echo(f"price_root_commit: {provenance.price_root_commit}")
        typer.echo(f"utxo_snapshot_commit: {provenance.snapshot_commit}")
        typer.echo(f"utxo_spent_commit: {provenance.spent_commit}")
        typer.echo(f"formulas_version: {provenance.formulas_version}")
    if result.qa_report.warnings:
        for warning in result.qa_report.warnings:
            typer.echo(f"Warning: {warning}")

    for golden in result.qa_report.golden_days:
        status = "OK" if golden.passed else "FAIL"
        typer.echo(
            f"Golden day {golden.target_date.isoformat()}: {status}"
        )


@app.command()
def show_config(config_path: Optional[Path] = typer.Option(None, "--config")) -> None:
    """Display the active metrics configuration."""

    cfg = _config(config_path)
    typer.echo(f"data.price_glob: {cfg.data.price_glob}")
    typer.echo(f"data.symbol: {cfg.data.symbol}")
    typer.echo(f"data.frequency: {cfg.data.frequency}")
    typer.echo(f"lifecycle.created: {cfg.data.lifecycle.created}")
    typer.echo(f"lifecycle.spent: {cfg.data.lifecycle.spent}")
    typer.echo(f"lifecycle.snapshots_glob: {cfg.data.lifecycle.snapshots_glob}")
    typer.echo(f"output_root: {cfg.data.output_root}")
    typer.echo(f"engine.mvrv_window_days: {cfg.engine.mvrv_window_days}")
    typer.echo(f"engine.dormancy_window_days: {cfg.engine.dormancy_window_days}")
    typer.echo(f"engine.drawdown_window_days: {cfg.engine.drawdown_window_days}")
    typer.echo(f"qa.golden_days: {len(cfg.qa.golden_days)} entries")
    typer.echo(f"qa.max_drawdown_pct: {cfg.qa.max_drawdown_pct}")
    typer.echo(f"qa.min_price: {cfg.qa.min_price}")
    typer.echo(f"writer.compression: {cfg.writer.compression}")
    typer.echo(f"writer.compression_level: {cfg.writer.compression_level}")


@app.command()
def registry() -> None:
    """List registered metrics and their dependencies."""

    definitions = load_metric_definitions()
    for entry in definitions:
        deps = ", ".join(entry.dependencies) if entry.dependencies else "(none)"
        badge = entry.badge
        typer.echo(f"{entry.name}: {entry.description}")
        typer.echo(f"  deps: {deps}")
        typer.echo(
            "  badge: "
            f"status={badge.status} v{badge.version}, coverage={badge.coverage_pct:.2f}%, "
            f"null_ratio={badge.null_ratio:.4f}, no_lookahead={badge.no_lookahead}, "
            f"reproducible={badge.reproducible}"
        )
        typer.echo(
            "  provenance: "
            f"utxo={badge.utxo_snapshot_commit}, price={badge.price_root_commit}, "
            f"formulas={badge.formulas_version}"
        )
        if entry.docs_path:
            typer.echo(f"  docs: {entry.docs_path}")
        typer.echo()


@app.command()
def docs(
    output_dir: Path = typer.Option(Path("docs"), "--output", "-o"),
    registry_path: Optional[Path] = typer.Option(None, "--registry"),
    mkdocs_file: Path = typer.Option(Path("mkdocs.yml"), "--mkdocs-file"),
    config_path: Optional[Path] = typer.Option(None, "--config"),
    metrics_path: Optional[Path] = typer.Option(None, "--metrics"),
) -> None:
    """Generate Markdown evidence for each metric in the registry."""

    definitions = (
        load_metric_definitions(str(registry_path))
        if registry_path
        else load_metric_definitions()
    )
    cfg: Optional[MetricsConfig] = None
    golden_paths: list[Path] = []
    if config_path is not None:
        cfg = _config(config_path)
    if cfg is not None and cfg.qa.golden_days:
        inferred_metrics_path = metrics_path or (cfg.data.output_root / "metrics.parquet")
        if inferred_metrics_path.exists():
            artifacts = generate_golden_artifacts(
                definitions,
                inferred_metrics_path,
                output_dir,
                cfg.qa.golden_days,
            )
            golden_paths = [artifact.output_path for artifact in artifacts]
        else:
            typer.echo(
                f"Metrics dataset not found at {inferred_metrics_path}; skipping golden artifacts"
            )
    elif config_path is None and metrics_path is not None:
        typer.echo("--metrics provided without --config; golden artifacts require golden day config")

    summary = generate_metric_docs(definitions, output_dir, mkdocs_file)
    typer.echo(f"Wrote {len(summary.written_paths)} metric evidence pages to {output_dir}")
    typer.echo(f"Index: {summary.index_path}")
    typer.echo(f"MkDocs config: {summary.mkdocs_path}")

    if golden_paths:
        typer.echo(f"Golden artifacts generated: {len(golden_paths)}")


def _parse_date(raw: str) -> date:
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise typer.BadParameter("Date must be in YYYY-MM-DD format") from exc


def _print_inspection(summary: InspectionSummary) -> None:
    _console.print(
        f"[bold]{summary.metric.name}[/bold] — {summary.metric.description}"
    )
    badge = summary.badge
    _console.print(
        f"Date: {summary.target_date.isoformat()} | Status: {badge.status} v{badge.version} "
        f"| Coverage {badge.coverage_pct:.2f}% | Null Ratio {badge.null_ratio:.4f}"
    )
    _console.print(
        f"Provenance: utxo={badge.utxo_snapshot_commit}, price={badge.price_root_commit}, "
        f"formulas={badge.formulas_version}"
    )
    if summary.totals:
        total_rows = Table(show_header=True, header_style="bold magenta")
        total_rows.add_column("Field")
        total_rows.add_column("Value")
        for key, value in summary.totals.items():
            total_rows.add_row(key, "" if value is None else str(value))
        _console.print(total_rows)

    if summary.price_rows:
        price_table = Table(title="Price Oracle", show_header=True, header_style="bold blue")
        price_table.add_column("timestamp")
        price_table.add_column("close")
        for row in summary.price_rows:
            price_table.add_row(str(row["ts"]), f"{row['close']:.2f}")
        _console.print(price_table)

    if summary.snapshot_rows:
        snapshot_table = Table(title="UTXO Snapshot", show_header=True, header_style="bold green")
        snapshot_table.add_column("group_key")
        snapshot_table.add_column("age_bucket")
        snapshot_table.add_column("balance_btc")
        snapshot_table.add_column("cost_basis_usd")
        snapshot_table.add_column("market_value_usd")
        for row in summary.snapshot_rows:
            snapshot_table.add_row(
                row["group_key"],
                row["age_bucket"],
                f"{row['balance_btc']:.6f}",
                f"{row['cost_basis_usd']:.2f}",
                f"{row['market_value_usd']:.2f}",
            )
        _console.print(snapshot_table)

    if summary.spent_rows:
        spent_table = Table(title="Spent Outputs", show_header=True, header_style="bold red")
        spent_table.add_column("spend_time")
        spent_table.add_column("source_txid")
        spent_table.add_column("value_sats")
        spent_table.add_column("realized_value_usd")
        spent_table.add_column("realized_profit_usd")
        spent_table.add_column("holding_days")
        for row in summary.spent_rows:
            spent_table.add_row(
                str(row["spend_time"]),
                row["source_txid"],
                str(row["value_sats"]),
                f"{row['realized_value_usd']:.2f}",
                f"{row['realized_profit_usd']:.2f}",
                f"{row['holding_days']:.2f}",
            )
        _console.print(spent_table)


@app.command()
def show(
    metric: str = typer.Argument(..., help="Metric name to inspect."),
    date_str: str = typer.Option(..., "--date", help="Target metric date (YYYY-MM-DD)."),
    config_path: Optional[Path] = typer.Option(None, "--config"),
    registry_path: Optional[Path] = typer.Option(
        Path("config/metrics_registry.yaml"), "--registry"
    ),
    limit: int = typer.Option(5, "--limit", min=1, max=50, help="Maximum rows per upstream table."),
    offset: int = typer.Option(0, "--offset", min=0, help="Rows to skip before applying limit."),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON instead of tables."),
    output_path: Optional[Path] = typer.Option(None, "--output", help="Write JSON payload to a file."),
) -> None:
    """Inspect upstream records contributing to a metric on a given date."""

    target_date = _parse_date(date_str)
    cfg = _config(config_path)
    try:
        summary = inspect_metric(
            cfg,
            metric,
            target_date=target_date,
            registry_path=registry_path,
            limit=limit,
            offset=offset,
        )
    except (ValueError, FileNotFoundError) as exc:
        typer.echo(f"Inspection failed: {exc}")
        raise typer.Exit(code=2) from exc

    payload = summary.to_json()

    if output_path is not None:
        output_path.write_text(payload, encoding="utf-8")
        typer.echo(f"Wrote inspection payload to {output_path}")

    if json_output:
        typer.echo(payload)
    else:
        _print_inspection(summary)


if __name__ == "__main__":
    app()
