from __future__ import annotations

import csv
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List

from .normalize import PIPELINE_VERSION, PriceRecord

logger = logging.getLogger(__name__)

_MS_THRESHOLD = 10_000_000_000  # Jan 1, 2286 as seconds; assumes larger values represent ms


def _parse_timestamp(value: str) -> datetime:
    try:
        ts_int = int(value)
    except ValueError as exc:
        raise ValueError(f"Expected integer timestamp, got {value!r}") from exc
    if ts_int > _MS_THRESHOLD:
        ts_int //= 1000
    return datetime.fromtimestamp(ts_int, tz=timezone.utc)


def _parse_float(value: str) -> float:
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"Expected float, got {value!r}") from exc


def _compute_file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _binance_records(
    path: Path,
    freq: str,
    symbol: str,
    source: str,
    *,
    raw_hash: str,
    ingested_at: datetime,
) -> List[PriceRecord]:
    records: List[PriceRecord] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            required = {"open_time", "open", "high", "low", "close", "volume", "close_time"}
            missing = required - set(reader.fieldnames or [])
            if missing:
                raise ValueError(f"Binance export missing columns: {', '.join(sorted(missing))}")
            for row in reader:
                ts = _parse_timestamp(row["close_time"])
                records.append(
                    PriceRecord(
                        symbol=symbol,
                        freq=freq,
                        ts=ts,
                        open=_parse_float(row["open"]),
                        high=_parse_float(row["high"]),
                        low=_parse_float(row["low"]),
                        close=_parse_float(row["close"]),
                        volume=_parse_float(row["volume"]),
                        source=source,
                        raw_file_hash=raw_hash,
                        ingested_at=ingested_at,
                        pipeline_version=PIPELINE_VERSION,
                    )
                )
    except ValueError as exc:
        raise ValueError(f"Failed to parse {path}: {exc}") from exc
    return records


def _coinbase_records(
    path: Path,
    freq: str,
    symbol: str,
    source: str,
    *,
    raw_hash: str,
    ingested_at: datetime,
) -> List[PriceRecord]:
    records: List[PriceRecord] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            required = {"time", "open", "high", "low", "close", "volume"}
            missing = required - set(reader.fieldnames or [])
            if missing:
                raise ValueError(f"Coinbase export missing columns: {', '.join(sorted(missing))}")
            for row in reader:
                ts_str = row["time"].replace("Z", "+00:00")
                ts = datetime.fromisoformat(ts_str)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                else:
                    ts = ts.astimezone(timezone.utc)
                records.append(
                    PriceRecord(
                        symbol=symbol,
                        freq=freq,
                        ts=ts,
                        open=_parse_float(row["open"]),
                        high=_parse_float(row["high"]),
                        low=_parse_float(row["low"]),
                        close=_parse_float(row["close"]),
                        volume=_parse_float(row["volume"]),
                        source=source,
                        raw_file_hash=raw_hash,
                        ingested_at=ingested_at,
                        pipeline_version=PIPELINE_VERSION,
                    )
                )
    except ValueError as exc:
        raise ValueError(f"Failed to parse {path}: {exc}") from exc
    return records


SOURCE_READERS: Dict[
    str, Callable[[Path, str, str, str, str, datetime], List[PriceRecord]]
] = {
    "binance": _binance_records,
    "coinbase": _coinbase_records,
}


def load_records(source: str, path: Path, freq: str, symbol: str) -> List[PriceRecord]:
    try:
        reader = SOURCE_READERS[source]
    except KeyError as exc:
        raise ValueError(f"Unsupported price source '{source}'") from exc
    raw_hash = _compute_file_hash(path)
    ingested_at = datetime.now(timezone.utc)
    records = reader(path, freq, symbol, source, raw_hash=raw_hash, ingested_at=ingested_at)
    sorted_records = sorted(records, key=lambda rec: rec.ts)
    logger.debug(
        "Loaded %s records for %s %s from %s (hash=%s)",
        len(sorted_records),
        symbol,
        freq,
        source,
        raw_hash,
    )
    return sorted_records
