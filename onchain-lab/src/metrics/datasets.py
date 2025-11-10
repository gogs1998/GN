from __future__ import annotations

import os
import uuid
from datetime import date
from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.lib import ArrowException

SCHEMA_VERSION = "metrics.v1"
_METADATA = {b"schema_version": SCHEMA_VERSION.encode("utf-8")}

METRICS_SCHEMA = pa.schema(
    [
        pa.field("date", pa.date32()),
        pa.field("price_close", pa.float64()),
        pa.field("market_value_usd", pa.float64()),
        pa.field("realized_value_usd", pa.float64()),
        pa.field("realized_profit_usd", pa.float64()),
        pa.field("realized_loss_usd", pa.float64()),
        pa.field("realized_profit_loss_ratio", pa.float64()),
        pa.field("sopr", pa.float64()),
        pa.field("asopr", pa.float64()),
        pa.field("mvrv", pa.float64()),
        pa.field("mvrv_zscore", pa.float64()),
        pa.field("nupl", pa.float64()),
        pa.field("cdd", pa.float64()),
        pa.field("adjusted_cdd", pa.float64()),
        pa.field("dormancy_flow", pa.float64()),
        pa.field("utxo_profit_share", pa.float64()),
        pa.field("drawdown_pct", pa.float64()),
        pa.field("supply_btc", pa.float64()),
        pa.field("supply_sats", pa.float64()),
        pa.field("supply_cost_basis_usd", pa.float64()),
        pa.field("pipeline_version", pa.string()),
        pa.field("lineage_id", pa.string()),
    ],
    metadata=_METADATA,
)


class MetricsWriteError(RuntimeError):
    """Raised when persisting metrics fails."""


def _atomic_write(table: pa.Table, path: Path, *, compression: str, compression_level: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        pq.write_table(
            table,
            tmp,
            compression=compression,
            compression_level=compression_level,
            coerce_timestamps="us",
            use_dictionary=True,
        )
        os.replace(tmp, path)
    except (OSError, ArrowException) as exc:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise MetricsWriteError(f"Failed to write metrics dataset to {path}: {exc}") from exc


def write_metrics(
    table: pa.Table,
    output_root: Path,
    *,
    compression: str = "zstd",
    compression_level: int = 7,
) -> Path:
    target = output_root / "metrics.parquet"
    _atomic_write(table, target, compression=compression, compression_level=compression_level)
    return target


def read_metrics(path: Path) -> pa.Table:
    if not path.exists():
        raise FileNotFoundError(f"Metrics dataset missing at {path}")
    return pq.read_table(path)


def append_hodl_columns(schema: pa.Schema, bucket_names: List[str]) -> pa.Schema:
    fields = list(schema)
    for bucket in bucket_names:
        fields.append(pa.field(bucket, pa.float64()))
    return pa.schema(fields, metadata=schema.metadata)


__all__ = [
    "METRICS_SCHEMA",
    "SCHEMA_VERSION",
    "MetricsWriteError",
    "write_metrics",
    "read_metrics",
    "append_hodl_columns",
]
