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


@app.command()
def progress(config_path: Optional[Path] = typer.Option(None, "--config", path_type=Path)) -> None:
    """Display blockchain ingestion progress."""
    from datetime import datetime
    from .pipeline import ProcessedHeightIndex
    
    cfg = _config(config_path)
    
    # Get processed height
    idx = ProcessedHeightIndex(cfg.data_root)
    max_height = idx.max_height()
    
    # Get blockchain tip
    try:
        with _client(cfg) as client:
            tip = client.get_block_count()
    except RPCError as exc:
        console.print(f"[red]Failed to get blockchain tip:[/red] {exc}")
        raise typer.Exit(code=2) from exc
    
    # Count marker files
    marker_dir = cfg.data_root / "_markers"
    if marker_dir.exists():
        markers = sorted([
            int(f.stem) for f in marker_dir.glob("*.done")
            if f.stem.isdigit()
        ])
        processed_count = len(markers)
        if markers:
            min_height = min(markers)
            max_marker = max(markers)
        else:
            min_height = -1
            max_marker = -1
    else:
        processed_count = 0
        min_height = -1
        max_marker = -1
        markers = []
    
    # Check for gaps
    gaps = []
    if markers:
        prev = markers[0] - 1
        for h in markers:
            if h - prev > 1:
                gaps.append((prev + 1, h - 1))
            prev = h
    
    # Count data files
    blocks_dir = cfg.data_root / "blocks"
    tx_dir = cfg.data_root / "tx"
    txin_dir = cfg.data_root / "txin"
    txout_dir = cfg.data_root / "txout"
    
    block_files = len(list(blocks_dir.glob("**/*.parquet"))) if blocks_dir.exists() else 0
    tx_files = len(list(tx_dir.glob("**/*.parquet"))) if tx_dir.exists() else 0
    txin_files = len(list(txin_dir.glob("**/*.parquet"))) if txin_dir.exists() else 0
    txout_files = len(list(txout_dir.glob("**/*.parquet"))) if txout_dir.exists() else 0
    
    # Calculate progress
    total_blocks = tip + 1
    processed_blocks = processed_count
    remaining_blocks = total_blocks - processed_blocks
    progress_pct = (processed_blocks / total_blocks * 100) if total_blocks > 0 else 0
    
    # Create progress table
    table = Table(title="Blockchain Ingestion Progress", show_header=True, header_style="bold")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Current Blockchain Tip", f"{tip:,} blocks")
    table.add_row("Total Blocks", f"{total_blocks:,} blocks (height 0-{tip})")
    table.add_row("", "")
    table.add_row("Processed Blocks", f"{processed_blocks:,} blocks")
    table.add_row("Height Range", f"{min_height} - {max_marker}")
    table.add_row("Remaining Blocks", f"{remaining_blocks:,} blocks")
    table.add_row("Progress", f"{progress_pct:.4f}%")
    
    if gaps:
        table.add_row("", "")
        table.add_row("[yellow]⚠️  Gaps Detected[/yellow]", f"{len(gaps)} gaps found")
        if len(gaps) <= 5:
            table.add_row("Gap Ranges", ", ".join(f"{start}-{end}" for start, end in gaps))
        else:
            table.add_row("First 5 Gaps", ", ".join(f"{start}-{end}" for start, end in gaps[:5]))
    else:
        table.add_row("", "")
        table.add_row("[green]✅ Status[/green]", "No gaps detected (continuous range)")
    
    table.add_row("", "")
    table.add_row("Data Files", "")
    table.add_row("  Block files", f"{block_files:,}")
    table.add_row("  Transaction files", f"{tx_files:,}")
    table.add_row("  TxIn files", f"{txin_files:,}")
    table.add_row("  TxOut files", f"{txout_files:,}")
    
    console.print(table)
    
    # Estimate time (rough)
    blocks_per_second = 1.0  # Very rough estimate
    estimated_seconds = remaining_blocks / blocks_per_second if blocks_per_second > 0 else 0
    estimated_hours = estimated_seconds / 3600
    estimated_days = estimated_hours / 24
    
    console.print(f"\n[dim]Estimated time remaining (at ~1 block/sec): {estimated_days:.1f} days ({estimated_hours:.1f} hours)[/dim]")
    console.print(f"[dim]Note: Actual time varies based on block sizes and RPC performance[/dim]")
    
    if progress_pct < 1:
        console.print(f"\n[yellow]⚠️  Very early stage ({progress_pct:.4f}% complete)[/yellow]")
        console.print(f"[dim]Consider running: onchain ingest backfill --from 0 --to 5000[/dim]")
        console.print(f"[dim]Or: onchain ingest catchup --max-blocks 5000[/dim]")


if __name__ == "__main__":
    app()
