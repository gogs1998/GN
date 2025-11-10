from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.lib import ArrowException

SCHEMA_VERSION = "utxo.lifecycle.v1"
_PIPELINE_VERSION = "lifecycle.v1"
_METADATA = {b"schema_version": SCHEMA_VERSION.encode("utf-8")}


def pipeline_version() -> str:
    return _PIPELINE_VERSION


CREATED_SCHEMA = pa.schema(
    [
        pa.field("txid", pa.string()),
        pa.field("vout", pa.int32()),
        pa.field("value_sats", pa.int64()),
        pa.field("script_type", pa.string()),
        pa.field("addresses", pa.list_(pa.string())),
        pa.field("created_height", pa.int64()).with_nullable(True),
        pa.field("created_time", pa.timestamp("us", tz="UTC")).with_nullable(True),
        pa.field("created_date", pa.date32()),
        pa.field("creation_price_close", pa.float64()).with_nullable(True),
        pa.field("creation_price_ts", pa.timestamp("us", tz="UTC")).with_nullable(True),
        pa.field("creation_price_source", pa.string()).with_nullable(True),
        pa.field("creation_price_hash", pa.string()).with_nullable(True),
        pa.field("creation_price_pipeline", pa.string()).with_nullable(True),
        pa.field("spend_txid_hint", pa.string()).with_nullable(True),
        pa.field("spend_height_hint", pa.int64()).with_nullable(True),
        pa.field("spend_time_hint", pa.timestamp("us", tz="UTC")).with_nullable(True),
        pa.field("is_spent", pa.bool_()),
        pa.field("lineage_id", pa.string()),
        pa.field("pipeline_version", pa.string()),
    ],
    metadata=_METADATA,
)


SPENT_SCHEMA = pa.schema(
    [
        pa.field("source_txid", pa.string()),
        pa.field("source_vout", pa.int32()),
        pa.field("spend_txid", pa.string()),
        pa.field("value_sats", pa.int64()),
        pa.field("created_height", pa.int64()),
        pa.field("created_time", pa.timestamp("us", tz="UTC")),
        pa.field("spend_height", pa.int64()),
        pa.field("spend_time", pa.timestamp("us", tz="UTC")),
        pa.field("holding_seconds", pa.float64()),
        pa.field("holding_days", pa.float64()),
        pa.field("creation_price_close", pa.float64()).with_nullable(True),
        pa.field("creation_price_ts", pa.timestamp("us", tz="UTC")).with_nullable(True),
        pa.field("creation_price_source", pa.string()).with_nullable(True),
        pa.field("spend_price_close", pa.float64()).with_nullable(True),
        pa.field("spend_price_ts", pa.timestamp("us", tz="UTC")).with_nullable(True),
        pa.field("spend_price_source", pa.string()).with_nullable(True),
        pa.field("realized_value_usd", pa.float64()).with_nullable(True),
        pa.field("realized_profit_usd", pa.float64()).with_nullable(True),
        pa.field("is_orphan", pa.bool_()),
        pa.field("lineage_id", pa.string()),
        pa.field("pipeline_version", pa.string()),
    ],
    metadata=_METADATA,
)


SNAPSHOT_SCHEMA = pa.schema(
    [
        pa.field("snapshot_date", pa.date32()),
        pa.field("group_key", pa.string()),
        pa.field("age_bucket", pa.string()),
        pa.field("output_count", pa.int32()),
        pa.field("balance_sats", pa.int64()),
        pa.field("balance_btc", pa.float64()),
        pa.field("avg_age_days", pa.float64()),
        pa.field("cost_basis_usd", pa.float64()),
        pa.field("market_value_usd", pa.float64()),
        pa.field("price_close", pa.float64()).with_nullable(True),
        pa.field("price_ts", pa.timestamp("us", tz="UTC")).with_nullable(True),
        pa.field("pipeline_version", pa.string()),
        pa.field("lineage_id", pa.string()),
    ],
    metadata=_METADATA,
)


class DatasetWriteError(RuntimeError):
    """Raised when lifecycle datasets fail to persist."""


def _atomic_write(table: pa.Table, path: Path, *, compression: str, compression_level: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = f".{path.name}.{uuid.uuid4().hex}.tmp"
    tmp_path = path.parent / tmp_name
    try:
        pq.write_table(
            table,
            tmp_path,
            compression=compression,
            compression_level=compression_level,
            use_dictionary=True,
            coerce_timestamps="us",
        )
        os.replace(tmp_path, path)
    except (OSError, ArrowException) as exc:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise DatasetWriteError(f"Failed to write dataset to {path}: {exc}") from exc


@dataclass
class LifecycleArtifacts:
    created: pa.Table
    spent: pa.Table


def write_created(table: pa.Table, root: Path, *, compression: str, compression_level: int) -> Path:
    target = root / "created" / "created.parquet"
    _atomic_write(table, target, compression=compression, compression_level=compression_level)
    return target


def write_spent(table: pa.Table, root: Path, *, compression: str, compression_level: int) -> Path:
    target = root / "spent" / "spent.parquet"
    _atomic_write(table, target, compression=compression, compression_level=compression_level)
    return target


def snapshot_path(root: Path, snapshot_date: date) -> Path:
    filename = f"{snapshot_date.isoformat()}.parquet"
    return root / "snapshots" / "daily" / filename


def write_snapshot(
    table: pa.Table,
    root: Path,
    snapshot_date: date,
    *,
    compression: str,
    compression_level: int,
) -> Path:
    target = snapshot_path(root, snapshot_date)
    _atomic_write(table, target, compression=compression, compression_level=compression_level)
    return target


def read_created(root: Path) -> pa.Table:
    path = root / "created" / "created.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Created dataset missing at {path}")
    return pq.read_table(path)


def read_spent(root: Path) -> pa.Table:
    path = root / "spent" / "spent.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Spent dataset missing at {path}")
    return pq.read_table(path)


def read_snapshots(root: Path) -> List[tuple[date, pa.Table]]:
    snapshots_dir = root / "snapshots" / "daily"
    if not snapshots_dir.exists():
        return []
    tables: List[tuple[date, pa.Table]] = []
    for path in sorted(snapshots_dir.glob("*.parquet")):
        snapshot_date = date.fromisoformat(path.stem)
        tables.append((snapshot_date, pq.read_table(path)))
    return tables


__all__ = [
    "CREATED_SCHEMA",
    "SPENT_SCHEMA",
    "SNAPSHOT_SCHEMA",
    "LifecycleArtifacts",
    "DatasetWriteError",
    "pipeline_version",
    "snapshot_path",
    "read_created",
    "read_snapshots",
    "read_spent",
    "write_created",
    "write_snapshot",
    "write_spent",
]
