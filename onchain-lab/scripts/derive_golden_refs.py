"""Derive golden-day metrics from blockchain.com public charts.

This script fetches total supply deltas and transaction counts for a curated set
of reference days and materialises them into ``src/ingest/golden_refs.json``.

Usage:
    python scripts/derive_golden_refs.py [output_path]

The resulting JSON embeds provenance URLs for transparency. Re-run whenever the
upstream data source publishes corrections.
"""

from __future__ import annotations

import json
import sys
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

API_BASE = "https://api.blockchain.info/charts"


@dataclass(frozen=True)
class GoldenDay:
    date: str
    tolerance_pct: float
    manual: dict | None = None


REFERENCE_DAYS: Tuple[GoldenDay, ...] = (
    GoldenDay(
        date="2009-01-03",
        tolerance_pct=0.1,
        manual={
            "blocks": 1,
            "transactions": 1,
            "coinbase_sats": 50 * 100_000_000,
            "sources": ["Bitcoin Core genesis block (height 0)"],
        },
    ),
    GoldenDay(date="2017-08-01", tolerance_pct=0.5),
    GoldenDay(date="2020-05-11", tolerance_pct=0.5),
    GoldenDay(date="2024-04-20", tolerance_pct=0.5),
)

REWARD_SCHEDULE: Tuple[Tuple[datetime, float], ...] = (
    (datetime(2009, 1, 3, tzinfo=timezone.utc), 50.0),
    (datetime(2012, 11, 28, tzinfo=timezone.utc), 25.0),
    (datetime(2016, 7, 9, tzinfo=timezone.utc), 12.5),
    (datetime(2020, 5, 11, 19, 23, tzinfo=timezone.utc), 6.25),
    (datetime(2024, 4, 20, 0, 9, tzinfo=timezone.utc), 3.125),
)


def chart_url(chart: str, start: str, timespan: str) -> str:
    return f"{API_BASE}/{chart}?format=json&timespan={timespan}&start={start}"


def fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url) as resp:
        return json.load(resp)


def day_bounds(day_str: str) -> Tuple[int, int]:
    day = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(day.timestamp()), int((day + timedelta(days=1)).timestamp())


def slice_period(values: List[dict], start_ts: int, end_ts: int) -> List[dict]:
    return [entry for entry in values if start_ts <= entry["x"] <= end_ts]


def reward_for_timestamp(ts: int) -> float:
    instant = datetime.fromtimestamp(ts, tz=timezone.utc)
    reward = REWARD_SCHEDULE[0][1]
    for threshold, subsidy in REWARD_SCHEDULE:
        if instant >= threshold:
            reward = subsidy
    return reward


def derive_supply(day: str) -> Tuple[float, int]:
    start_ts, end_ts = day_bounds(day)
    data = fetch_json(chart_url("total-bitcoins", day, "2days"))
    values = sorted(slice_period(data["values"], start_ts, end_ts), key=lambda item: item["x"])
    if len(values) < 2:
        raise RuntimeError(f"Insufficient total-bitcoins data for {day}")

    minted_btc = values[-1]["y"] - values[0]["y"]
    block_count = 0
    for prev, cur in zip(values, values[1:]):
        diff = round(cur["y"] - prev["y"], 9)
        if diff <= 0:
            continue
        subsidy = reward_for_timestamp(cur["x"])
        blocks = round(diff / subsidy)
        block_count += blocks
    return minted_btc, block_count


def derive_transactions(day: str) -> int:
    start_ts, _ = day_bounds(day)
    data = fetch_json(chart_url("n-transactions", day, "1days"))
    for entry in data.get("values", []):
        if entry["x"] == start_ts:
            return int(entry["y"])
    raise RuntimeError(f"Transaction aggregate missing for {day}")


def format_sources(day: str) -> List[str]:
    return [
        chart_url("total-bitcoins", day, "2days"),
        chart_url("n-transactions", day, "1days"),
    ]


def derive_day(day: GoldenDay) -> Dict[str, object]:
    if day.manual is not None:
        result = dict(day.manual)
        result.setdefault("tolerance_pct", day.tolerance_pct)
        return result

    minted_btc, block_count = derive_supply(day.date)
    tx_count = derive_transactions(day.date)
    coinbase_sats = int(round(minted_btc * 100_000_000))
    return {
        "blocks": block_count,
        "transactions": tx_count,
        "coinbase_sats": coinbase_sats,
        "tolerance_pct": day.tolerance_pct,
        "sources": format_sources(day.date),
    }


def build_payload(days: Iterable[GoldenDay]) -> Dict[str, Dict[str, object]]:
    payload: Dict[str, Dict[str, object]] = {}
    for day in days:
        payload[day.date] = derive_day(day)
    return payload


def main() -> None:
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("src/ingest/golden_refs.json")
    payload = build_payload(REFERENCE_DAYS)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote {output} with {len(payload)} reference days.")


if __name__ == "__main__":
    main()
