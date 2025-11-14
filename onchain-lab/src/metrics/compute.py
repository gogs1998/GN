from __future__ import annotations

import glob
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import MetricsConfig, load_config
from .datasets import METRICS_SCHEMA, MetricsWriteError, append_hodl_columns, write_metrics
from .formulas import MetricsComputationResult, compute_metrics, pipeline_version
from .qa import QAReport, run_qa_checks
from .registry import load_metric_definitions, validate_metric_names
from .provenance import MetricProvenance, fingerprint_paths, update_registry_provenance


class MetricsBuildError(RuntimeError):
    """Raised when the metrics pipeline fails."""


@dataclass(frozen=True)
class BuildResult:
    output_path: Path
    rows: int
    lineage_id: str
    hodl_columns: tuple[str, ...]
    qa_report: QAReport
    provenance: MetricProvenance


def _resolve_config(
    *, config: MetricsConfig | None, config_path: Path | None
) -> MetricsConfig:
    if config is not None and config_path is not None:
        raise ValueError("Provide either config or config_path, not both")
    if config is not None:
        return config
    return load_config(config_path)


def _read_price_frame(cfg: MetricsConfig) -> Tuple[pd.DataFrame, Tuple[Path, ...]]:
    pattern = cfg.data.price_glob
    matches = sorted(glob.glob(pattern, recursive=True))
    if not matches:
        raise MetricsBuildError(f"No price parquet files found for pattern '{pattern}'")
    tables = [pq.read_table(path, columns=["symbol", "freq", "ts", "close"]) for path in matches]
    table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
    frame = table.to_pandas()
    filtered = frame[(frame["symbol"] == cfg.data.symbol) & (frame["freq"] == cfg.data.frequency)]
    if filtered.empty:
        raise MetricsBuildError(
            f"No price records for {cfg.data.symbol} {cfg.data.frequency} in '{pattern}'"
        )
    return filtered[["ts", "close"]].copy(), tuple(Path(path).resolve() for path in matches)


def _read_snapshots(cfg: MetricsConfig) -> Tuple[pd.DataFrame, Tuple[Path, ...]]:
    pattern = cfg.data.lifecycle.snapshots_glob
    matches = sorted(glob.glob(pattern, recursive=True))
    if not matches:
        raise MetricsBuildError(f"No snapshot parquet files found for pattern '{pattern}'")

    tables = [pq.read_table(path) for path in matches]
    table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
    frame = table.to_pandas()

    # Drop dataset metadata columns that pyarrow injects when globs match multiple files.
    metadata_columns = [name for name in frame.columns if name.startswith("__")]
    if metadata_columns:
        frame = frame.drop(columns=metadata_columns)

    required = {
        "snapshot_date",
        "group_key",
        "age_bucket",
        "balance_sats",
        "balance_btc",
        "cost_basis_usd",
        "market_value_usd",
    }
    missing_required = required.difference(frame.columns)
    if missing_required:
        joined = ", ".join(sorted(missing_required))
        raise MetricsBuildError(f"Snapshot parquet missing required columns: {joined}")

    optional_defaults: dict[str, object] = {
        "avg_age_days": 0.0,
        "price_close": float("nan"),
        "entity_id": None,
        "entity_type": None,
        "cluster_balance_sats": 0.0,
    }
    for column, default in optional_defaults.items():
        if column not in frame.columns:
            frame[column] = default

    return frame, tuple(Path(path).resolve() for path in matches)


def _read_spent(cfg: MetricsConfig) -> Tuple[pd.DataFrame, Tuple[Path, ...]]:
    path = cfg.data.lifecycle.spent
    if not path.exists():
        raise MetricsBuildError(f"Spent dataset missing at {path}")
    table = pq.read_table(path)
    frame = table.to_pandas()

    metadata_columns = [name for name in frame.columns if name.startswith("__")]
    if metadata_columns:
        frame = frame.drop(columns=metadata_columns)

    required = {
        "source_txid",
        "source_vout",
        "spend_txid",
        "value_sats",
        "created_time",
        "spend_time",
        "holding_days",
        "realized_value_usd",
        "realized_profit_usd",
        "spend_price_close",
        "creation_price_close",
    }
    missing_required = required.difference(frame.columns)
    if missing_required:
        joined = ", ".join(sorted(missing_required))
        raise MetricsBuildError(f"Spent parquet missing required columns: {joined}")

    optional_defaults: dict[str, object] = {
        "entity_id": None,
        "entity_type": None,
    }
    for column, default in optional_defaults.items():
        if column not in frame.columns:
            frame[column] = default

    return frame, (path.resolve(),)


def _core_metric_columns() -> Sequence[str]:
    excluded = {"date", "pipeline_version", "lineage_id"}
    return [name for name in METRICS_SCHEMA.names if name not in excluded]


def _validate_registry(core_metric_columns: Iterable[str], registry_path: Path) -> None:
    definitions = load_metric_definitions(str(registry_path))
    validate_metric_names(core_metric_columns, definitions)


def _result_to_table(result: MetricsComputationResult) -> pa.Table:
    frame = result.frame.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    frame.sort_values("date", inplace=True)
    schema = append_hodl_columns(METRICS_SCHEMA, result.hodl_columns)
    return pa.Table.from_pandas(frame, schema=schema, preserve_index=False)


def build_daily_metrics(
    *,
    config: MetricsConfig | None = None,
    config_path: Path | None = None,
    registry_path: Path | None = None,
) -> BuildResult:
    """Build the daily metrics dataset and run QA checks."""

    cfg = _resolve_config(config=config, config_path=config_path)
    price_df, price_paths = _read_price_frame(cfg)
    snapshot_df, snapshot_paths = _read_snapshots(cfg)
    spent_df, spent_paths = _read_spent(cfg)

    metrics = compute_metrics(price_df, snapshot_df, spent_df, cfg.engine)

    repo_root = Path(__file__).resolve().parents[2]
    definition_path = registry_path or (repo_root / "config" / "metrics_registry.yaml")
    _validate_registry(_core_metric_columns(), definition_path)

    qa_report = run_qa_checks(metrics, price_df=price_df, config=cfg, raise_on_failure=False)
    if not qa_report.ok:
        joined = "; ".join(qa_report.violations)
        raise MetricsBuildError(f"QA checks failed: {joined}")

    table = _result_to_table(metrics)
    try:
        output_path = write_metrics(
            table,
            cfg.data.output_root,
            compression=cfg.writer.compression,
            compression_level=cfg.writer.compression_level,
        )
    except MetricsWriteError as exc:
        raise MetricsBuildError(str(exc)) from exc

    provenance = MetricProvenance(
        price_hash=fingerprint_paths(price_paths),
        snapshot_hash=fingerprint_paths(snapshot_paths),
        spent_hash=fingerprint_paths(spent_paths),
        formulas_tag=f"{pipeline_version()}+{metrics.lineage_id}",
    )

    if registry_path is not None:
        update_registry_provenance(registry_path, provenance)

    return BuildResult(
        output_path=output_path,
        rows=table.num_rows,
        lineage_id=metrics.lineage_id,
        hodl_columns=tuple(metrics.hodl_columns),
        qa_report=qa_report,
        provenance=provenance,
    )


__all__ = ["build_daily_metrics", "BuildResult", "MetricsBuildError"]
