from __future__ import annotations

import glob
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import MetricsConfig
from .registry import MetricDefinition, MetricBadgeView, load_metric_definitions

_SATS_PER_BTC = 100_000_000


@dataclass(frozen=True)
class InspectionSummary:
    metric: MetricDefinition
    target_date: date
    badge: MetricBadgeView
    price_rows: list[dict[str, Any]]
    snapshot_rows: list[dict[str, Any]]
    spent_rows: list[dict[str, Any]]
    totals: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric": {
                "name": self.metric.name,
                "description": self.metric.description,
                "dependencies": list(self.metric.dependencies),
                "docs_path": str(self.metric.docs_path) if self.metric.docs_path else None,
            },
            "target_date": self.target_date.isoformat(),
            "badge": {
                "status": self.badge.status,
                "version": self.badge.version,
                "coverage_pct": self.badge.coverage_pct,
                "null_ratio": self.badge.null_ratio,
                "golden_checks_passed": self.badge.golden_checks_passed,
                "deflated_sharpe_score": self.badge.deflated_sharpe_score,
                "no_lookahead": self.badge.no_lookahead,
                "reproducible": self.badge.reproducible,
                "utxo_snapshot_commit": self.badge.utxo_snapshot_commit,
                "price_root_commit": self.badge.price_root_commit,
                "formulas_version": self.badge.formulas_version,
            },
            "price_rows": self.price_rows,
            "snapshot_rows": self.snapshot_rows,
            "spent_rows": self.spent_rows,
            "totals": self.totals,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


def inspect_metric(
    cfg: MetricsConfig,
    metric_name: str,
    *,
    target_date: date,
    registry_path: Optional[Path] = None,
    limit: int = 10,
    offset: int = 0,
) -> InspectionSummary:
    definitions = (
        load_metric_definitions(str(registry_path))
        if registry_path is not None
        else load_metric_definitions()
    )
    definition = _select_definition(metric_name, definitions)
    price_rows: list[dict[str, Any]] = []
    snapshot_rows: list[dict[str, Any]] = []
    spent_rows: list[dict[str, Any]] = []
    totals: dict[str, Any] = {}

    deps = set(definition.dependencies)

    if "price_oracle" in deps:
        price_rows, price_totals = _inspect_price(cfg, target_date, limit, offset)
        totals.update(price_totals)

    if any(dep.startswith("utxo.snapshots") or dep == "utxo.snapshots" for dep in deps):
        snapshot_rows, snap_totals = _inspect_snapshots(cfg, target_date, limit, offset)
        totals.update(snap_totals)

    if any(dep.startswith("utxo.spent") or dep == "utxo.spent" for dep in deps):
        spent_rows, spent_totals = _inspect_spent(cfg, target_date, limit, offset)
        totals.update(spent_totals)

    return InspectionSummary(
        metric=definition,
        target_date=target_date,
        badge=definition.badge,
        price_rows=price_rows,
        snapshot_rows=snapshot_rows,
        spent_rows=spent_rows,
        totals=totals,
    )


def _select_definition(metric_name: str, definitions: Iterable[MetricDefinition]) -> MetricDefinition:
    for definition in definitions:
        if definition.name == metric_name:
            return definition
    raise ValueError(f"Metric '{metric_name}' is not registered")


def _inspect_price(
    cfg: MetricsConfig, target_date: date, limit: int, offset: int
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    pattern = cfg.data.price_glob
    matches = sorted(glob.glob(pattern, recursive=True))
    if not matches:
        raise FileNotFoundError(f"No price files found for pattern '{pattern}'")
    tables = [pq.read_table(path, columns=["symbol", "freq", "ts", "close"]) for path in matches]
    table = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
    frame = table.to_pandas()
    frame = frame[(frame["symbol"] == cfg.data.symbol) & (frame["freq"] == cfg.data.frequency)]
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    cutoff = pd.Timestamp(datetime.combine(target_date, time.max, tzinfo=timezone.utc))
    window = frame[frame["ts"] <= cutoff].sort_values("ts")
    total_rows = int(len(window))
    last_close = window["close"].iloc[-1] if total_rows else None
    start = max(offset, 0)
    stop = start + limit
    page = window.iloc[start:stop]
    records = _records(page, ["ts", "close"])
    return records, {
        "price_rows": total_rows,
        "price_close": float(last_close) if last_close is not None else None,
    }


def _inspect_snapshots(
    cfg: MetricsConfig, target_date: date, limit: int, offset: int
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    pattern = cfg.data.lifecycle.snapshots_glob
    matches = sorted(glob.glob(pattern, recursive=True))
    if not matches:
        raise FileNotFoundError(f"No snapshot files found for pattern '{pattern}'")
    columns = [
        "snapshot_date",
        "group_key",
        "age_bucket",
        "balance_sats",
        "balance_btc",
        "cost_basis_usd",
        "market_value_usd",
    ]
    tables = [pq.read_table(path, columns=columns) for path in matches]
    table = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
    frame = table.to_pandas()
    frame["snapshot_date"] = pd.to_datetime(frame["snapshot_date"]).dt.date
    day_rows = frame[frame["snapshot_date"] == target_date].copy()
    day_rows.sort_values("balance_sats", ascending=False, inplace=True)
    window = day_rows.iloc[max(offset, 0) : max(offset, 0) + limit]
    records = _records(
        window,
        ["snapshot_date", "group_key", "age_bucket", "balance_btc", "cost_basis_usd", "market_value_usd"],
    )
    totals = {
        "snapshot_rows": int(len(day_rows)),
        "snapshot_supply_btc": float(day_rows["balance_btc"].sum()),
        "snapshot_cost_basis_usd": float(day_rows["cost_basis_usd"].sum()),
        "snapshot_market_value_usd": float(day_rows["market_value_usd"].sum()),
    }
    return records, totals


def _inspect_spent(
    cfg: MetricsConfig, target_date: date, limit: int, offset: int
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    path = cfg.data.lifecycle.spent
    if not path.exists():
        raise FileNotFoundError(f"Spent dataset missing at {path}")
    columns = [
        "source_txid",
        "source_vout",
        "spend_txid",
        "value_sats",
        "spend_time",
        "holding_days",
        "realized_value_usd",
        "realized_profit_usd",
        "spend_price_close",
        "creation_price_close",
    ]
    table = pq.read_table(path, columns=columns)
    frame = table.to_pandas()
    frame["spend_time"] = pd.to_datetime(frame["spend_time"], utc=True)
    day_rows = frame[frame["spend_time"].dt.date == target_date].copy()
    day_rows.sort_values("value_sats", ascending=False, inplace=True)
    window = day_rows.iloc[max(offset, 0) : max(offset, 0) + limit]
    records = _records(
        window,
        [
            "spend_time",
            "source_txid",
            "value_sats",
            "realized_value_usd",
            "realized_profit_usd",
            "holding_days",
            "spend_price_close",
            "creation_price_close",
        ],
    )
    totals = {
        "spent_rows": int(len(day_rows)),
        "spent_value_btc": float(day_rows["value_sats"].sum() / _SATS_PER_BTC),
        "spent_realized_value_usd": float(day_rows["realized_value_usd"].sum()),
        "spent_realized_profit_usd": float(day_rows["realized_profit_usd"].sum()),
    }
    return records, totals


def _records(frame: pd.DataFrame, columns: Iterable[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for _, row in frame[list(columns)].iterrows():
        record: dict[str, Any] = {}
        for column in columns:
            value = row[column]
            if isinstance(value, pd.Timestamp):
                record[column] = value.isoformat()
            elif isinstance(value, (datetime, date)):
                record[column] = value.isoformat()
            elif isinstance(value, float):
                record[column] = float(value)
            elif isinstance(value, pd.Series):
                record[column] = value.to_list()
            else:
                record[column] = value
        rows.append(record)
    return rows


__all__ = ["InspectionSummary", "inspect_metric"]
