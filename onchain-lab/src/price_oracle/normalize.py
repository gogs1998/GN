from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Sequence, Tuple

from zoneinfo import ZoneInfo

PIPELINE_VERSION = "price_oracle.v1"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PriceRecord:
    symbol: str
    freq: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str
    raw_file_hash: str
    ingested_at: datetime
    pipeline_version: str

    def aligned(self, ts: datetime) -> "PriceRecord":
        return replace(self, ts=ts)


FREQ_MINUTES: Dict[str, int] = {
    "1h": 60,
    "1d": 60 * 24,
}


def _parse_boundary(boundary: str) -> Tuple[int, int]:
    hour_str, minute_str = boundary.split(":", maxsplit=1)
    return int(hour_str), int(minute_str)


def align_timestamp(
    ts: datetime,
    freq: str,
    boundary: str | None,
    tzinfo: ZoneInfo | None,
) -> datetime:
    tz = tzinfo or timezone.utc
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)

    local = ts.astimezone(tz)

    if freq == "1h":
        aligned_local = local.replace(minute=0, second=0, microsecond=0)
        return aligned_local.astimezone(timezone.utc)

    if freq == "1d":
        hour = minute = 0
        if boundary:
            hour, minute = _parse_boundary(boundary)
        aligned_local = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if local < aligned_local:
            aligned_local -= timedelta(days=1)
        return aligned_local.astimezone(timezone.utc)

    raise ValueError(f"Unsupported frequency '{freq}' for alignment")


def apply_alignment(
    records: Iterable[PriceRecord],
    freq: str,
    boundary: str | None,
    *,
    tzinfo: ZoneInfo | None,
    tolerance_minutes: int = 2,
) -> List[PriceRecord]:
    aligned: List[PriceRecord] = []
    boundary_pair: Tuple[int, int] | None = None
    if freq == "1d" and boundary:
        boundary_pair = _parse_boundary(boundary)
    tolerance = timedelta(minutes=max(tolerance_minutes, 0))
    for rec in records:
        target_ts = align_timestamp(rec.ts, freq, boundary, tzinfo)
        if boundary_pair is not None:
            hour, minute = boundary_pair
            target_local = target_ts.astimezone(tzinfo or timezone.utc)
            rec_ts = rec.ts if rec.ts.tzinfo else rec.ts.replace(tzinfo=timezone.utc)
            rec_local = rec_ts.astimezone(tzinfo or timezone.utc)
            diff = abs(target_local - rec_local)
            if diff > tolerance:
                raise ValueError(
                    f"Record at {rec.ts.isoformat()} deviates {diff} from daily boundary {boundary}."
                )
            if diff > timedelta(0):
                logger.warning(
                    "Snapping %s %s record at %s to boundary %02d:%02d (offset %s)",
                    rec.symbol,
                    rec.freq,
                    rec.ts.isoformat(),
                    hour,
                    minute,
                    diff,
                )
        aligned.append(rec.aligned(target_ts))
    return aligned


def merge_sources(records: Iterable[PriceRecord], source_priority: Sequence[str]) -> List[PriceRecord]:
    priority: Dict[str, int] = {name: idx for idx, name in enumerate(source_priority)}
    best: Dict[Tuple[str, str, datetime], PriceRecord] = {}
    for record in records:
        key = (record.symbol, record.freq, record.ts)
        current = best.get(key)
        if current is None:
            best[key] = record
            continue
        current_priority = priority.get(current.source, float("inf"))
        new_priority = priority.get(record.source, float("inf"))
        if new_priority < current_priority:
            best[key] = record
    return sorted(best.values(), key=lambda rec: (rec.symbol, rec.freq, rec.ts))


def ensure_sorted(records: Iterable[PriceRecord]) -> List[PriceRecord]:
    return sorted(records, key=lambda rec: (rec.symbol, rec.freq, rec.ts))
