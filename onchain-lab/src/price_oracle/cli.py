from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence

import typer

from .config import ConfigError, PriceOracleConfig, load_config
from .oracle import PriceOracle, QAError

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


if __name__ == "__main__":
    app()
