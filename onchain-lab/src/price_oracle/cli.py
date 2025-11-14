from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Sequence
from datetime import datetime, timedelta, timezone

import typer

from .config import ConfigError, PriceOracleConfig, load_config
from .oracle import PriceOracle, QAError
from .fetcher import FetchError, fetch_prices, write_manifest

app = typer.Typer(help="Price oracle CLI", add_completion=False)


def _config(path: Optional[Path]) -> PriceOracleConfig:
    try:
        return load_config(path)
    except ConfigError as exc:
        typer.echo(f"Configuration error: {exc}")
        raise typer.Exit(code=1) from exc


@app.command()
def build(
    symbol: Optional[str] = typer.Option(None, "--symbol", help="Symbol to build"),
    freq: Optional[str] = typer.Option(None, "--freq", help="Frequency to build"),
    config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path),
) -> None:
    """Build normalized price datasets and run QA checks."""
    cfg = _config(config_path)

    symbols: Sequence[str] | None = [symbol] if symbol else None
    freqs: Sequence[str] | None = [freq] if freq else None

    if symbol and symbol not in cfg.symbols:
        typer.echo(f"Symbol '{symbol}' not defined in config")
        raise typer.Exit(code=2)
    if freq and freq not in cfg.freqs:
        typer.echo(f"Frequency '{freq}' not defined in config")
        raise typer.Exit(code=2)

    oracle = PriceOracle(cfg)
    try:
        summaries = oracle.build(symbols=symbols, freqs=freqs)
    except QAError as exc:
        typer.echo(f"QA failed: {exc}")
        raise typer.Exit(code=3) from exc

    for summary in summaries:
        typer.echo(
            f"Built {summary.symbol} {summary.freq}: {summary.records_written} records (QA ok)"
        )


@app.command()
def latest(
    symbol: str = typer.Argument(..., help="Symbol to inspect"),
    freq: str = typer.Argument(..., help="Frequency to inspect"),
    limit: int = typer.Option(5, "--limit", min=1, max=100),
    config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path),
) -> None:
    """Display the latest normalized prices."""
    cfg = _config(config_path)
    oracle = PriceOracle(cfg)
    records = oracle.latest(symbol, freq, limit=limit)
    if not records:
        typer.echo(f"No records stored for {symbol} {freq}")
        return
    for rec in records:
        typer.echo(
            f"{rec.ts.isoformat()} symbol={rec.symbol} freq={rec.freq} close={rec.close} source={rec.source}"
        )


@app.command()
def show_config(config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path)) -> None:
    """Print the active price oracle configuration."""
    cfg = _config(config_path)
    typer.echo(f"data_root: {cfg.data_root}")
    typer.echo(f"symbols: {', '.join(cfg.symbols)}")
    typer.echo(f"freqs: {', '.join(cfg.freqs)}")
    typer.echo(f"primary: {cfg.primary}")
    typer.echo(f"fallback: {cfg.fallback}")
    typer.echo(f"alignment.daily_close_hhmm: {cfg.alignment.daily_close_hhmm}")
    typer.echo(f"qa.max_gap_hours: {cfg.qa.max_gap_hours}")
    typer.echo(f"qa.max_basis_diff_pct: {cfg.qa.max_basis_diff_pct}")


def _parse_date(raw: str) -> datetime.date:
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise typer.BadParameter("Date must be YYYY-MM-DD") from exc


def _raw_root(cfg: PriceOracleConfig) -> Path:
    root = cfg.data_root
    parent = root.parent if root.parent != root else root
    return parent / "raw"


@app.command()
def fetch(
    symbol: Optional[str] = typer.Option(None, "--symbol", "-s", help="Symbol to fetch"),
    freq: List[str] = typer.Option([], "--freq", "-f", help="Frequencies to fetch"),
    start: Optional[str] = typer.Option(None, "--start", help="Start date (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, "--end", help="End date inclusive (YYYY-MM-DD)"),
    days: int = typer.Option(730, "--days", min=1, help="Window length when start not provided"),
    coinbase_product: Optional[str] = typer.Option(
        None, "--coinbase-product", help="Override Coinbase product id"
    ),
    config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path),
) -> None:
    """Fetch raw exchange candles into the price oracle raw directory."""

    cfg = _config(config_path)
    target_symbol = symbol or (cfg.symbols[0] if cfg.symbols else None)
    if target_symbol is None:
        typer.echo("No symbol provided and configuration defines none")
        raise typer.Exit(code=2)
    target_freqs = freq or list(cfg.freqs)
    if not target_freqs:
        typer.echo("No frequencies provided and configuration defines none")
        raise typer.Exit(code=2)
    today = datetime.now(timezone.utc).date()
    end_date = _parse_date(end) if end else today - timedelta(days=1)
    if start:
        start_date = _parse_date(start)
    else:
        start_date = end_date - timedelta(days=days - 1)
    if start_date > end_date:
        typer.echo("Start date must be on or before end date")
        raise typer.Exit(code=2)
    if end_date >= today:
        end_date = today - timedelta(days=1)
    raw_root = _raw_root(cfg)
    try:
        reports = fetch_prices(
            symbol=target_symbol,
            freqs=target_freqs,
            start_date=start_date,
            end_date=end_date,
            raw_root=raw_root,
            coinbase_product=coinbase_product,
        )
    except FetchError as exc:
        typer.echo(str(exc))
        raise typer.Exit(code=3) from exc
    manifest_path = write_manifest(raw_root, target_symbol, reports)
    for report in reports:
        typer.echo(
            f"[{report.source}] {report.freq} -> {report.output_path} ({report.records} rows, requests={report.request_count})"
        )
    typer.echo(f"Manifest written to {manifest_path}")


if __name__ == "__main__":
    app()
