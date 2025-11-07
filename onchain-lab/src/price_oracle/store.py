from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import logging
import os
import tempfile
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from .normalize import PIPELINE_VERSION, PriceRecord

logger = logging.getLogger(__name__)
SCHEMA_VERSION = 1

SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("freq", pa.string()),
        ("ts", pa.timestamp("s", tz="UTC")),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
        ("source", pa.string()),
        ("raw_file_hash", pa.string()),
        ("ingested_at", pa.timestamp("s", tz="UTC")),
        ("pipeline_version", pa.string()),
        ("schema_version", pa.int32()),
    ]
)


def _records_to_table(records: Sequence[PriceRecord]) -> pa.Table:
    sorted_records = sorted(records, key=lambda rec: rec.ts)
    return pa.table(
        {
            "symbol": [rec.symbol for rec in sorted_records],
            "freq": [rec.freq for rec in sorted_records],
            "ts": [rec.ts for rec in sorted_records],
            "open": [rec.open for rec in sorted_records],
            "high": [rec.high for rec in sorted_records],
            "low": [rec.low for rec in sorted_records],
            "close": [rec.close for rec in sorted_records],
            "volume": [rec.volume for rec in sorted_records],
            "source": [rec.source for rec in sorted_records],
            "raw_file_hash": [rec.raw_file_hash for rec in sorted_records],
            "ingested_at": [rec.ingested_at for rec in sorted_records],
            "pipeline_version": [rec.pipeline_version for rec in sorted_records],
            "schema_version": [SCHEMA_VERSION for _ in sorted_records],
        },
        schema=SCHEMA,
    )


def _table_to_records(table: pa.Table) -> List[PriceRecord]:
    arrays = {name: table[name].to_pylist() for name in table.column_names}
    count = table.num_rows
    default_hashes = ["unknown"] * count
    default_ingested = [datetime.now(timezone.utc)] * count
    default_pipeline = [PIPELINE_VERSION] * count
    default_schema = [SCHEMA_VERSION] * count
    schema_versions = arrays.get("schema_version", default_schema)
    if any(version != SCHEMA_VERSION for version in schema_versions):
        logger.warning(
            "Loaded price records with schema versions %s (expected %s)",
            sorted({version for version in schema_versions}),
            SCHEMA_VERSION,
        )
    records: List[PriceRecord] = []
    for idx in range(count):
        ts = arrays["ts"][idx]
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        ingested_at = arrays.get("ingested_at", default_ingested)[idx]
        if isinstance(ingested_at, datetime) and ingested_at.tzinfo is None:
            ingested_at = ingested_at.replace(tzinfo=timezone.utc)
        pipeline_version = arrays.get("pipeline_version", default_pipeline)[idx]
        raw_hash = arrays.get("raw_file_hash", default_hashes)[idx]
        records.append(
            PriceRecord(
                symbol=arrays["symbol"][idx],
                freq=arrays["freq"][idx],
                ts=ts,
                open=arrays["open"][idx],
                high=arrays["high"][idx],
                low=arrays["low"][idx],
                close=arrays["close"][idx],
                volume=arrays["volume"][idx],
                source=arrays["source"][idx],
                raw_file_hash=raw_hash,
                ingested_at=ingested_at,
                pipeline_version=pipeline_version,
            )
        )
    return records


class PriceStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def _dataset_path(self, symbol: str, freq: str) -> Path:
        return self.root / symbol.lower() / f"{freq}.parquet"

    def read(self, symbol: str, freq: str) -> List[PriceRecord]:
        path = self._dataset_path(symbol, freq)
        if not path.exists():
            return []
        table = pq.read_table(path)
        return _table_to_records(table)

    def write_all(self, records: Sequence[PriceRecord]) -> None:
        grouped: Dict[Tuple[str, str], List[PriceRecord]] = defaultdict(list)
        for record in records:
            grouped[(record.symbol, record.freq)].append(record)
        for (symbol, freq), group in grouped.items():
            self._write_group(symbol, freq, group)

    def upsert(self, records: Sequence[PriceRecord]) -> None:
        grouped: Dict[Tuple[str, str], List[PriceRecord]] = defaultdict(list)
        for record in records:
            grouped[(record.symbol, record.freq)].append(record)
        for (symbol, freq), group in grouped.items():
            existing = {rec.ts: rec for rec in self.read(symbol, freq)}
            for rec in group:
                if rec.ts in existing and existing[rec.ts] != rec:
                    logger.warning(
                        "Overwriting price record for %s %s at %s (existing source=%s, new source=%s)",
                        symbol,
                        freq,
                        rec.ts.isoformat(),
                        existing[rec.ts].source,
                        rec.source,
                    )
                existing[rec.ts] = rec
            merged = sorted(existing.values(), key=lambda rec: rec.ts)
            self._write_group(symbol, freq, merged)

    def _write_group(self, symbol: str, freq: str, records: Sequence[PriceRecord]) -> None:
        if not records:
            return
        path = self._dataset_path(symbol, freq)
        path.parent.mkdir(parents=True, exist_ok=True)
        table = _records_to_table(records)
        with tempfile.NamedTemporaryFile(delete=False, dir=path.parent, suffix=".tmp") as handle:
            temp_path = Path(handle.name)
        try:
            pq.write_table(table, str(temp_path), compression="snappy")
            os.replace(temp_path, path)
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            raise

    def delete(self, symbol: str, freq: str) -> None:
        path = self._dataset_path(symbol, freq)
        if path.exists():
            path.unlink()


__all__ = ["PriceStore"]
