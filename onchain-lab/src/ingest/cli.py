from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from .config import ConfigError, IngestConfig, load_config
from .pipeline import sync_from_tip, sync_range
from .qa import QAError, verify_date
from .rpc import BitcoinRPCClient, RPCError

app = typer.Typer(help="ONCHAIN LAB ingest CLI", add_completion=False)
console = Console()


def _config(path: Optional[Path]) -> IngestConfig:
    try:
        return load_config(path)
    except ConfigError as exc:
        console.print(f"[red]Configuration error:[/red] {exc}")
        raise typer.Exit(code=1) from exc


def _client(cfg: IngestConfig) -> BitcoinRPCClient:
    user, password = cfg.rpc.credentials()
    return BitcoinRPCClient(cfg.rpc.host, cfg.rpc.port, user, password)


@app.command()
def init(config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path)) -> None:
    """Validate RPC connectivity and show current tip."""
    cfg = _config(config_path)
    console.print("[bold]Validating ingestion setup...[/bold]")
    try:
        with _client(cfg) as client:
            tip = client.get_block_count()
            console.print(f"RPC reachable at {cfg.rpc.host}:{cfg.rpc.port} (tip height={tip})")
    except RPCError as exc:
        console.print(f"[red]RPC validation failed:[/red] {exc}")
        raise typer.Exit(code=2) from exc

    console.print(f"Data root: {cfg.data_root}")
    console.print("Ready to ingest.")


@app.command()
def backfill(
    from_height: int = typer.Option(..., "--from", min=0, help="Start height inclusive"),
    to_height: int = typer.Option(..., "--to", min=0, help="End height inclusive"),
    config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path),
) -> None:
    """Backfill a specific height range."""
    if to_height < from_height:
        console.print("[red]--to must be >= --from[/red]")
        raise typer.Exit(code=1)

    cfg = _config(config_path)
    try:
        counts = sync_range(from_height, to_height, config=cfg)
    except (RPCError, ConfigError) as exc:
        console.print(f"[red]Backfill failed:[/red] {exc}")
        raise typer.Exit(code=2) from exc

    console.print(f"Completed backfill {from_height}->{to_height}: {counts}")


@app.command()
def catchup(
    max_blocks: int = typer.Option(2000, "--max-blocks", min=1),
    config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path),
) -> None:
    """Ingest up to N blocks from the current tip backward."""
    cfg = _config(config_path)
    try:
        counts = sync_from_tip(max_blocks, config=cfg)
    except (RPCError, ConfigError) as exc:
        console.print(f"[red]Catchup failed:[/red] {exc}")
        raise typer.Exit(code=2) from exc

    console.print(f"Catchup processed: {counts}")


@app.command()
def verify(
    date_str: str = typer.Argument(..., help="Date (YYYY-MM-DD) to verify"),
    config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path),
) -> None:
    """Run golden-day QA checks for a given date."""
    cfg = _config(config_path)
    try:
        metrics = verify_date(date_str, config=cfg)
    except QAError as exc:
        console.print(f"[red]QA failed:[/red] {exc}")
        raise typer.Exit(code=3) from exc

    table = Table(title=f"QA metrics for {date_str}")
    table.add_column("Metric")
    table.add_column("Value")
    for key, value in metrics.items():
        table.add_row(key, f"{value}")
    console.print(table)


@app.command()
def info(config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path)) -> None:
    """Display the active ingest configuration."""
    cfg = _config(config_path)
    table = Table(title="Ingest configuration", show_header=True, header_style="bold")
    table.add_column("Field")
    table.add_column("Value")

    table.add_row("data_root", str(cfg.data_root))
    table.add_row("height_bucket_size", str(cfg.height_bucket_size))
    table.add_row("compression", cfg.compression)
    table.add_row("zstd_level", str(cfg.zstd_level))
    table.add_row("max_blocks_per_run", str(cfg.limits.max_blocks_per_run))
    table.add_row("io_batch_size", str(cfg.limits.io_batch_size))
    table.add_row("rpc_host", cfg.rpc.host)
    table.add_row("rpc_port", str(cfg.rpc.port))
    table.add_row("qa_golden_days", ", ".join(day.isoformat() for day in cfg.qa.golden_days))

    console.print(table)


if __name__ == "__main__":
    app()
