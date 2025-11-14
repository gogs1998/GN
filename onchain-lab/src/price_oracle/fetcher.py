from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import httpx

BINANCE_INTERVALS_MS: Dict[str, int] = {"1h": 60 * 60 * 1000, "1d": 24 * 60 * 60 * 1000}
COINBASE_GRANULARITY: Dict[str, int] = {"1h": 3600, "1d": 86400}
COINBASE_SYMBOL_DEFAULTS: Dict[str, str] = {"BTCUSDT": "BTC-USD"}
BINANCE_ENDPOINT = "https://api.binance.com/api/v3/klines"
COINBASE_ENDPOINT = "https://api.exchange.coinbase.com/products/{product}/candles"
COINBASE_MAX_POINTS = 300


@dataclass(frozen=True)
class FetchReport:
    source: str
    symbol: str
    freq: str
    start: datetime
    end: datetime
    records: int
    output_path: Path
    request_count: int
    checksum: str


class FetchError(RuntimeError):
    pass


def _to_utc_start(day: date) -> datetime:
    return datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _binance_candles(
    client: httpx.Client,
    symbol: str,
    freq: str,
    start: datetime,
    end: datetime,
) -> Tuple[List[Dict[str, str]], int]:
    interval_ms = BINANCE_INTERVALS_MS[freq]
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    cursor = start_ms
    candles: List[Dict[str, str]] = []
    requests = 0
    while cursor < end_ms:
        try:
            resp = client.get(
                BINANCE_ENDPOINT,
                params={
                    "symbol": symbol,
                    "interval": freq,
                    "startTime": cursor,
                    "limit": 1000,
                },
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise FetchError(f"Binance request failed: {exc}") from exc
        requests += 1
        payload = resp.json()
        if not payload:
            break
        for entry in payload:
            close_time = int(entry[6])
            if close_time > end_ms:
                break
            row = {
                "open_time": str(entry[0]),
                "open": entry[1],
                "high": entry[2],
                "low": entry[3],
                "close": entry[4],
                "volume": entry[5],
                "close_time": str(close_time),
            }
            candles.append(row)
        last_close = int(payload[-1][6])
        if last_close <= cursor:
            break
        cursor = last_close + interval_ms
    candles.sort(key=lambda row: int(row["open_time"]))
    return candles, requests


def _coinbase_product(symbol: str, explicit: str | None) -> str:
    if explicit:
        return explicit
    return COINBASE_SYMBOL_DEFAULTS.get(symbol, symbol)


def _coinbase_candles(
    client: httpx.Client,
    product: str,
    freq: str,
    start: datetime,
    end: datetime,
) -> Tuple[List[Dict[str, str]], int]:
    granularity = COINBASE_GRANULARITY[freq]
    step = timedelta(seconds=granularity * COINBASE_MAX_POINTS)
    cursor = start
    candles: List[Dict[str, str]] = []
    requests = 0
    while cursor < end:
        chunk_end = min(end, cursor + step)
        try:
            resp = client.get(
                COINBASE_ENDPOINT.format(product=product),
                params={
                    "start": cursor.isoformat(),
                    "end": chunk_end.isoformat(),
                    "granularity": granularity,
                },
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise FetchError(f"Coinbase request failed: {exc}") from exc
        requests += 1
        payload = resp.json()
        if not payload:
            cursor = chunk_end
            continue
        for item in payload:
            start_ts = datetime.fromtimestamp(item[0], tz=timezone.utc)
            close_dt = start_ts + timedelta(seconds=granularity)
            if freq in {"1d", "1h"}:
                close_dt -= timedelta(minutes=1)
            if close_dt < start:
                continue
            if close_dt > end:
                continue
            candles.append(
                {
                    "time": close_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "open": str(item[3]),
                    "high": str(item[2]),
                    "low": str(item[1]),
                    "close": str(item[4]),
                    "volume": str(item[5]),
                }
            )
        cursor = chunk_end
    candles.sort(key=lambda row: row["time"])
    dedup: Dict[str, Dict[str, str]] = {}
    for row in candles:
        dedup[row["time"]] = row
    ordered = [dedup[key] for key in sorted(dedup.keys())]
    return ordered, requests


def _write_binance_csv(path: Path, rows: Sequence[Dict[str, str]]) -> None:
    _ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["open_time", "open", "high", "low", "close", "volume", "close_time"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_coinbase_csv(path: Path, rows: Sequence[Dict[str, str]]) -> None:
    _ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["time", "open", "high", "low", "close", "volume"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def fetch_prices(
    *,
    symbol: str,
    freqs: Iterable[str],
    start_date: date,
    end_date: date,
    raw_root: Path,
    coinbase_product: str | None = None,
) -> List[FetchReport]:
    start = _to_utc_start(start_date)
    end = _to_utc_start(end_date + timedelta(days=1)) - timedelta(minutes=1)
    product = _coinbase_product(symbol, coinbase_product)
    reports: List[FetchReport] = []
    with httpx.Client(timeout=30) as binance_client, httpx.Client(timeout=30) as coinbase_client:
        for freq in freqs:
            if freq not in BINANCE_INTERVALS_MS:
                raise FetchError(f"Unsupported frequency '{freq}'")
            binance_rows, binance_requests = _binance_candles(
                binance_client, symbol, freq, start, end
            )
            binance_path = raw_root / "binance" / f"{symbol}-{freq}.csv"
            _write_binance_csv(binance_path, binance_rows)
            binance_report = FetchReport(
                source="binance",
                symbol=symbol,
                freq=freq,
                start=start,
                end=end,
                records=len(binance_rows),
                output_path=binance_path,
                request_count=binance_requests,
                checksum=_checksum(binance_path),
            )
            reports.append(binance_report)
            coinbase_rows, coinbase_requests = _coinbase_candles(
                coinbase_client, product, freq, start, end
            )
            coinbase_path = raw_root / "coinbase" / f"{symbol}-{freq}.csv"
            _write_coinbase_csv(coinbase_path, coinbase_rows)
            coinbase_report = FetchReport(
                source="coinbase",
                symbol=symbol,
                freq=freq,
                start=start,
                end=end,
                records=len(coinbase_rows),
                output_path=coinbase_path,
                request_count=coinbase_requests,
                checksum=_checksum(coinbase_path),
            )
            reports.append(coinbase_report)
    return reports


def write_manifest(raw_root: Path, symbol: str, reports: Sequence[FetchReport]) -> Path:
    manifest_root = raw_root / "_manifests"
    manifest_root.mkdir(parents=True, exist_ok=True)
    generated = datetime.now(timezone.utc)
    filename = f"{symbol}_{generated.strftime('%Y%m%dT%H%M%SZ')}.json"
    path = manifest_root / filename
    start_iso = min((report.start for report in reports), default=None)
    end_iso = max((report.end for report in reports), default=None)
    payload = {
        "generated_at": generated.isoformat(),
        "symbol": symbol,
        "start": start_iso.isoformat() if start_iso else None,
        "end": end_iso.isoformat() if end_iso else None,
        "reports": [
            {
                "source": report.source,
                "freq": report.freq,
                "records": report.records,
                "output_path": str(report.output_path),
                "start": report.start.isoformat(),
                "end": report.end.isoformat(),
                "checksum": report.checksum,
                "requests": report.request_count,
            }
            for report in reports
        ],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return path
