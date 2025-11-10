from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .config import EngineConfig

_SATS_PER_BTC = 100_000_000
_PIPELINE_VERSION = "metrics.v1"


def pipeline_version() -> str:
    """Return the semantic version for the metrics engine."""

    return _PIPELINE_VERSION


@dataclass
class MetricsComputationResult:
    frame: pd.DataFrame
    hodl_columns: List[str]
    lineage_id: str


def compute_metrics(
    price_df: pd.DataFrame,
    snapshot_df: pd.DataFrame,
    spent_df: pd.DataFrame,
    engine: EngineConfig,
) -> MetricsComputationResult:
    """Compute the full daily metrics frame from upstream datasets."""

    price_daily = _price_by_date(price_df)
    snapshot_daily = _aggregate_snapshots(snapshot_df)
    spent_daily = _aggregate_spent(spent_df)

    merged = price_daily.merge(snapshot_daily, on="date", how="left")
    merged = merged.merge(spent_daily, on="date", how="left")
    merged.sort_values("date", inplace=True)

    for column in [
        "supply_btc",
        "supply_sats",
        "realized_value_usd",
        "market_value_usd",
        "realized_profit_usd",
        "realized_loss_usd",
        "realized_profit_loss_ratio",
        "sopr",
        "asopr",
        "cost_basis_spent_usd",
        "spent_value_btc",
        "cdd",
        "adjusted_cdd",
        "asopr_realized_value",
        "asopr_cost_basis",
    ]:
        if column not in merged:
            merged[column] = 0.0

    merged.rename(columns={"close": "price_close"}, inplace=True)
    merged["price_close"] = merged["price_close"].ffill()

    numeric_fill_columns = [
        "market_value_usd",
        "realized_value_usd",
        "realized_profit_usd",
        "realized_loss_usd",
        "spent_value_btc",
        "cdd",
        "adjusted_cdd",
    ]
    for column in numeric_fill_columns:
        if column in merged:
            merged[column] = pd.to_numeric(merged[column], errors="coerce")

    market_value_fallback = merged["supply_btc"] * merged["price_close"]
    merged["market_value_usd"] = merged["market_value_usd"].fillna(market_value_fallback)
    merged["realized_value_usd"] = merged["realized_value_usd"].fillna(0.0)
    merged["realized_profit_usd"] = merged["realized_profit_usd"].fillna(0.0)
    merged["realized_loss_usd"] = merged["realized_loss_usd"].fillna(0.0)
    merged["spent_value_btc"] = merged["spent_value_btc"].fillna(0.0)
    merged["cdd"] = merged["cdd"].fillna(0.0)
    merged["adjusted_cdd"] = merged["adjusted_cdd"].fillna(0.0)

    merged["realized_profit_loss_ratio"] = merged.apply(
        lambda row: _safe_div(row["realized_profit_usd"], row["realized_loss_usd"]), axis=1
    )

    merged["dormancy_flow"] = merged["market_value_usd"] / (
        merged["cdd"].rolling(engine.dormancy_window_days, min_periods=1).mean().replace(0.0, np.nan)
    )

    merged["drawdown_pct"] = _compute_drawdown(merged["price_close"], engine.drawdown_window_days)

    profit_share = _profit_share(snapshot_df)
    merged = merged.merge(profit_share, on="date", how="left")
    merged["utxo_profit_share"] = merged["utxo_profit_share"].fillna(0.0)

    hodl_series, hodl_columns = _hodl_shares(snapshot_df)
    for column, series in hodl_series.items():
        merged = merged.merge(series, on="date", how="left")
        merged[column] = merged[column].fillna(0.0)

    merged["sopr"] = merged.apply(
        lambda row: _safe_div(row["realized_value_usd"], row["cost_basis_spent_usd"]), axis=1
    )
    merged["asopr"] = merged.apply(
        lambda row: _safe_div(row["asopr_realized_value"], row["asopr_cost_basis"]), axis=1
    )

    merged["adjusted_cdd"] = merged.apply(
        lambda row: _safe_div(row["cdd"], row["spent_value_btc"]), axis=1
    )

    merged.rename(columns={"cost_basis_snap_usd": "supply_cost_basis_usd"}, inplace=True)
    merged["supply_cost_basis_usd"] = merged["supply_cost_basis_usd"].ffill()

    merged["mvrv"] = merged.apply(
        lambda row: _safe_div(row["market_value_usd"], row["supply_cost_basis_usd"]), axis=1
    )

    delta = merged["market_value_usd"] - merged["supply_cost_basis_usd"]
    rolling_mean = delta.rolling(engine.mvrv_window_days, min_periods=1).mean()
    rolling_std = delta.rolling(engine.mvrv_window_days, min_periods=1).std(ddof=0)
    merged["mvrv_zscore"] = (delta - rolling_mean) / rolling_std.replace(0.0, np.nan)

    merged["nupl"] = merged.apply(
        lambda row: _safe_div(
            row["market_value_usd"] - row["supply_cost_basis_usd"],
            row["market_value_usd"],
        ),
        axis=1,
    )

    core_columns = [
        "date",
        "price_close",
        "market_value_usd",
        "realized_value_usd",
        "supply_cost_basis_usd",
        "realized_profit_usd",
        "realized_loss_usd",
        "realized_profit_loss_ratio",
        "sopr",
        "asopr",
        "mvrv",
        "mvrv_zscore",
        "nupl",
        "cdd",
        "adjusted_cdd",
        "dormancy_flow",
        "utxo_profit_share",
        "drawdown_pct",
        "supply_btc",
        "supply_sats",
    ]

    missing_columns = [column for column in core_columns + hodl_columns if column not in merged]
    for column in missing_columns:
        merged[column] = np.nan

    frame = merged[core_columns + hodl_columns].copy()

    lineage = _compute_lineage_hash(price_df, snapshot_df, spent_df)
    frame["pipeline_version"] = pipeline_version()
    frame["lineage_id"] = lineage

    return MetricsComputationResult(frame=frame, hodl_columns=hodl_columns, lineage_id=lineage)


def _price_by_date(price_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate raw price observations to one close per day."""

    if price_df.empty:
        return pd.DataFrame(columns=["date", "close"])
    df = price_df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["date"] = df["ts"].dt.date
    df.sort_values("ts", inplace=True)
    return df.groupby("date", as_index=False).agg({"close": "last"})


def _aggregate_snapshots(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate UTXO daily snapshots to supply-level totals."""

    if snapshot_df.empty:
        return pd.DataFrame(
            columns=["date", "supply_btc", "supply_sats", "realized_value_usd", "market_value_usd"]
        )
    df = snapshot_df.copy()
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    grouped = df.groupby("snapshot_date", as_index=False).agg(
        {
            "balance_btc": "sum",
            "balance_sats": "sum",
            "cost_basis_usd": "sum",
            "market_value_usd": "sum",
        }
    )
    grouped.rename(
        columns={
            "snapshot_date": "date",
            "balance_btc": "supply_btc",
            "balance_sats": "supply_sats",
            "cost_basis_usd": "cost_basis_snap_usd",
        },
        inplace=True,
    )
    return grouped


def _aggregate_spent(spent_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the spent UTXO table to day-level realized metrics."""

    if spent_df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "spent_value_btc",
                "realized_value_usd",
                "realized_profit_usd",
                "realized_loss_usd",
                "cost_basis_spent_usd",
                "sopr",
                "asopr",
                "cdd",
                "adjusted_cdd",
                "asopr_realized_value",
                "asopr_cost_basis",
            ]
        )

    df = spent_df.copy()
    df["spend_time"] = pd.to_datetime(df["spend_time"], utc=True)
    df["date"] = df["spend_time"].dt.date
    df["value_btc"] = df["value_sats"] / _SATS_PER_BTC
    spend_value_fallback = df["value_btc"] * df["spend_price_close"]
    df["realized_value_usd"] = df["realized_value_usd"].fillna(spend_value_fallback)
    df["realized_value_usd"] = df["realized_value_usd"].fillna(0.0)
    df["realized_profit_usd"] = df["realized_profit_usd"].fillna(0.0)
    df["creation_price_close"] = df["creation_price_close"].fillna(0.0)
    df["holding_days"] = df["holding_days"].fillna(0.0)
    df["cost_basis_usd"] = df["value_btc"] * df["creation_price_close"]

    df["loss_component"] = df["realized_profit_usd"].where(df["realized_profit_usd"] < 0, 0).abs()
    df["cdd_component"] = df["value_btc"] * df["holding_days"].clip(lower=0)

    grouped = df.groupby("date", as_index=False).agg(
        {
            "value_btc": "sum",
            "realized_value_usd": "sum",
            "realized_profit_usd": "sum",
            "loss_component": "sum",
            "cost_basis_usd": "sum",
            "cdd_component": "sum",
        }
    )
    grouped.rename(
        columns={
            "value_btc": "spent_value_btc",
            "loss_component": "realized_loss_usd",
            "cost_basis_usd": "cost_basis_spent_usd",
            "cdd_component": "cdd",
        },
        inplace=True,
    )

    # Apply a one-hour minimum holding period for aSOPR adjustments.
    asopr_subset = df[df["holding_days"] >= (1.0 / 24.0)]
    if asopr_subset.empty:
        asopr_grouped = pd.DataFrame(columns=["date", "asopr_realized_value", "asopr_cost_basis"])
    else:
        asopr_grouped = asopr_subset.groupby("date", as_index=False).agg(
            {
                "realized_value_usd": "sum",
                "cost_basis_usd": "sum",
            }
        )
        asopr_grouped.rename(
            columns={
                "realized_value_usd": "asopr_realized_value",
                "cost_basis_usd": "asopr_cost_basis",
            },
            inplace=True,
        )

    grouped = grouped.merge(asopr_grouped, on="date", how="left")
    grouped["asopr_realized_value"] = grouped["asopr_realized_value"].fillna(0.0)
    grouped["asopr_cost_basis"] = grouped["asopr_cost_basis"].fillna(0.0)

    grouped["adjusted_cdd"] = grouped.apply(
        lambda row: _safe_div(row["cdd"], row["spent_value_btc"]), axis=1
    )

    return grouped


def _profit_share(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    """Compute the share of supply in profit for each day."""

    if snapshot_df.empty:
        return pd.DataFrame(columns=["date", "utxo_profit_share"])
    df = snapshot_df.copy()
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date

    totals = df.groupby("snapshot_date")["balance_sats"].sum()
    profitable = (
        df[df["market_value_usd"] > df["cost_basis_usd"]]
        .groupby("snapshot_date")["balance_sats"].sum()
    )
    share = (profitable / totals).fillna(0.0)
    share.name = "utxo_profit_share"
    return share.reset_index().rename(columns={"snapshot_date": "date"})


def _hodl_shares(snapshot_df: pd.DataFrame) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    """Return normalized HODL share columns derived from age buckets."""

    if snapshot_df.empty:
        return {}, []
    df = snapshot_df.copy()
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    pivot = df.pivot_table(
        index="snapshot_date",
        columns="age_bucket",
        values="balance_sats",
        aggfunc="sum",
        fill_value=0.0,
    )
    totals = pivot.sum(axis=1).replace(0.0, np.nan)
    shares = pivot.div(totals, axis=0).fillna(0.0)

    hodl_series: Dict[str, pd.DataFrame] = {}
    hodl_columns: List[str] = []
    for raw_bucket in shares.columns:
        sanitized = _sanitize_bucket(raw_bucket)
        column_name = f"hodl_share_{sanitized}"
        hodl_columns.append(column_name)
        series = shares[[raw_bucket]].reset_index().rename(
            columns={raw_bucket: column_name, "snapshot_date": "date"}
        )
        hodl_series[column_name] = series

    return hodl_series, hodl_columns


def _sanitize_bucket(bucket: str) -> str:
    sanitized = bucket.lower()
    for old, new in [
        ("+", "_plus"),
        ("-", "_"),
        (" ", ""),
        (">", "_gt"),
        ("<", "_lt"),
        ("/", "_"),
        ("(", ""),
        (")", ""),
    ]:
        sanitized = sanitized.replace(old, new)
    sanitized = sanitized.replace("__", "_")
    return sanitized


def _compute_drawdown(series: pd.Series, window: int) -> pd.Series:
    rolling_max = series.rolling(window, min_periods=1).max()
    return ((series - rolling_max) / rolling_max) * 100.0


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator in (0, 0.0) or np.isnan(denominator):
        return 0.0 if numerator == 0 else float("nan")
    return numerator / denominator


def _compute_lineage_hash(
    price_df: pd.DataFrame,
    snapshot_df: pd.DataFrame,
    spent_df: pd.DataFrame,
) -> str:
    payload = {
        "price_rows": int(len(price_df)),
        "price_start": _series_min(price_df.get("ts")),
        "price_end": _series_max(price_df.get("ts")),
        "snapshot_rows": int(len(snapshot_df)),
        "snapshot_start": _series_min(snapshot_df.get("snapshot_date")),
        "snapshot_end": _series_max(snapshot_df.get("snapshot_date")),
        "spent_rows": int(len(spent_df)),
        "spent_start": _series_min(spent_df.get("spend_time")),
        "spent_end": _series_max(spent_df.get("spend_time")),
    }
    serialized = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()[:16]


def _series_min(series: pd.Series | None) -> str | None:
    if series is None or series.empty:
        return None
    values = pd.to_datetime(series, errors="coerce") if series.dtype == object else series
    values = values.dropna()
    if values.empty:
        return None
    return str(values.min())


def _series_max(series: pd.Series | None) -> str | None:
    if series is None or series.empty:
        return None
    values = pd.to_datetime(series, errors="coerce") if series.dtype == object else series
    values = values.dropna()
    if values.empty:
        return None
    return str(values.max())


__all__ = ["compute_metrics", "MetricsComputationResult", "pipeline_version"]
