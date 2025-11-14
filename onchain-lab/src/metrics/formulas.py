from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .config import EngineConfig

_SATS_PER_BTC = 100_000_000
_PIPELINE_VERSION = "metrics.v2"
_AGE_BUCKET_REALIZED_CAP_MAP = {
    "000-001d": "realized_cap_0_1d",
    "001-007d": "realized_cap_1_7d",
    "007-030d": "realized_cap_7_30d",
    "030-180d": "realized_cap_30_180d",
    "180-365d": "realized_cap_180_365d",
    "365d+": "realized_cap_365d_plus",
}
_WHALE_THRESHOLD_BTC = 1_000
_WHALE_THRESHOLD_SATS = int(_WHALE_THRESHOLD_BTC * _SATS_PER_BTC)
_LONG_TERM_THRESHOLD_DAYS = 155
_EXCHANGE_ENTITY_TYPES = {"exchange"}

def _numeric_series(df: pd.DataFrame, column: str, *, fill_value: float | None = 0.0) -> pd.Series:
    """Return a numeric Series for ``column`` with a fallback when the column is missing."""

    if column in df.columns:
        series = pd.to_numeric(df[column], errors="coerce")
    else:
        default = float("nan") if fill_value is None else float(fill_value)
        series = pd.Series(default, index=df.index, dtype="float64")

    if fill_value is not None:
        series = series.fillna(fill_value)
    return series

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
    entity_supply = _snapshot_entity_totals(snapshot_df)
    realized_caps = _snapshot_realized_cap_segments(snapshot_df)
    profit_share = _profit_share(snapshot_df)
    hodl_series, hodl_columns = _hodl_shares(snapshot_df)

    spent_prepared = _prepare_spent_frame(spent_df)
    spent_daily = _aggregate_prepared_spent(spent_prepared)
    sopr_entity_adjusted = _compute_sopr_entity_adjusted(spent_prepared)
    sopr_long_short = _compute_sopr_long_short(spent_prepared)
    whale_realized_pl = _compute_whale_realized_pl(spent_prepared)

    merged = price_daily.merge(snapshot_daily, on="date", how="left")
    for dataset in [
        spent_daily,
        entity_supply,
        realized_caps,
        sopr_entity_adjusted,
        sopr_long_short,
        whale_realized_pl,
        profit_share,
    ]:
        merged = merged.merge(dataset, on="date", how="left")

    for column, series in hodl_series.items():
        merged = merged.merge(series, on="date", how="left")

    merged.sort_values("date", inplace=True)
    merged.rename(columns={"close": "price_close", "cost_basis_snap_usd": "supply_cost_basis_usd"}, inplace=True)

    merged["price_close"] = merged["price_close"].ffill()
    merged["supply_cost_basis_usd"] = merged["supply_cost_basis_usd"].ffill()

    required_columns = [
        "supply_btc",
        "supply_sats",
        "market_value_usd",
        "realized_value_usd",
        "realized_profit_usd",
        "realized_loss_usd",
        "spent_value_btc",
        "cdd",
        "adjusted_cdd",
        "asopr_realized_value",
        "asopr_cost_basis",
        "cost_basis_spent_usd",
        "exchange_balance_sats",
        "exchange_weighted_age_days",
        "exchange_cost_basis_usd",
        "whale_balance_sats",
        "whale_weighted_age_days",
        "whale_cost_basis_usd",
        "sopr_entity_adjusted",
        "sopr_long_short_delta",
        "whale_realized_pl_usd",
        *list(_AGE_BUCKET_REALIZED_CAP_MAP.values()),
        "realized_cap_exchange",
        "realized_cap_whale",
    ]
    for column in required_columns:
        if column not in merged:
            merged[column] = 0.0

    numeric_columns = [
        "supply_btc",
        "supply_sats",
        "market_value_usd",
        "realized_value_usd",
        "realized_profit_usd",
        "realized_loss_usd",
        "spent_value_btc",
        "cdd",
        "adjusted_cdd",
        "asopr_realized_value",
        "asopr_cost_basis",
        "cost_basis_spent_usd",
        "exchange_balance_sats",
        "exchange_weighted_age_days",
        "exchange_cost_basis_usd",
        "whale_balance_sats",
        "whale_weighted_age_days",
        "whale_cost_basis_usd",
        "sopr_entity_adjusted",
        "sopr_long_short_delta",
        "whale_realized_pl_usd",
    ] + list(_AGE_BUCKET_REALIZED_CAP_MAP.values()) + [
        "realized_cap_exchange",
        "realized_cap_whale",
    ]
    for column in numeric_columns:
        if column in merged:
            merged[column] = pd.to_numeric(merged[column], errors="coerce")

    for column in [
        "supply_btc",
        "supply_sats",
        "market_value_usd",
        "realized_value_usd",
        "realized_profit_usd",
        "realized_loss_usd",
        "spent_value_btc",
        "cdd",
        "adjusted_cdd",
        "asopr_realized_value",
        "asopr_cost_basis",
        "cost_basis_spent_usd",
        "exchange_balance_sats",
        "exchange_weighted_age_days",
        "exchange_cost_basis_usd",
        "whale_balance_sats",
        "whale_weighted_age_days",
        "whale_cost_basis_usd",
        "sopr_entity_adjusted",
        "sopr_long_short_delta",
        "whale_realized_pl_usd",
    ] + list(_AGE_BUCKET_REALIZED_CAP_MAP.values()) + [
        "realized_cap_exchange",
        "realized_cap_whale",
    ]:
        if column in merged:
            merged[column] = merged[column].fillna(0.0)

    if "utxo_profit_share" not in merged:
        merged["utxo_profit_share"] = 0.0
    else:
        merged["utxo_profit_share"] = merged["utxo_profit_share"].fillna(0.0)
    for column in hodl_columns:
        if column not in merged:
            merged[column] = 0.0
        else:
            merged[column] = merged[column].fillna(0.0)

    market_value_fallback = merged.get("supply_btc", 0.0) * merged.get("price_close", 0.0)
    merged["market_value_usd"] = merged.get("market_value_usd", 0.0).fillna(market_value_fallback)

    merged["realized_profit_loss_ratio"] = merged.apply(
        lambda row: _safe_div(row.get("realized_profit_usd", 0.0), row.get("realized_loss_usd", 0.0)),
        axis=1,
    )

    merged["sopr"] = merged.apply(
        lambda row: _safe_div(row.get("realized_value_usd", 0.0), row.get("cost_basis_spent_usd", 0.0)),
        axis=1,
    )
    merged["asopr"] = merged.apply(
        lambda row: _safe_div(row.get("asopr_realized_value", 0.0), row.get("asopr_cost_basis", 0.0)),
        axis=1,
    )

    merged["dormancy_flow"] = merged["market_value_usd"] / (
        merged["cdd"].rolling(engine.dormancy_window_days, min_periods=1).mean().replace(0.0, np.nan)
    )

    merged["drawdown_pct"] = _compute_drawdown(merged["price_close"], engine.drawdown_window_days)

    merged["exchange_supply_btc"] = merged["exchange_balance_sats"] / _SATS_PER_BTC
    merged["exchange_supply_pct"] = merged.apply(
        lambda row: _safe_div(row.get("exchange_balance_sats", 0.0), row.get("supply_sats", 0.0)),
        axis=1,
    )
    merged["exchange_net_flow_btc"] = merged["exchange_supply_btc"].diff().fillna(0.0)
    merged["exchange_net_flow_usd"] = merged["exchange_net_flow_btc"] * merged["price_close"].fillna(0.0)
    merged["exchange_dormancy"] = merged["exchange_weighted_age_days"].fillna(0.0)

    merged["whale_supply_btc"] = merged["whale_balance_sats"] / _SATS_PER_BTC
    merged["whale_dormancy"] = merged["whale_weighted_age_days"].fillna(0.0)
    merged["whale_realized_pl_usd"] = merged["whale_realized_pl_usd"].fillna(0.0)

    merged["sopr_entity_adjusted"] = merged["sopr_entity_adjusted"].fillna(0.0)
    merged["sopr_long_short_delta"] = merged["sopr_long_short_delta"].fillna(0.0)
    merged["spent_profit_delta_usd"] = merged["realized_profit_usd"] - merged["realized_loss_usd"]

    merged["mvrv"] = merged.apply(
        lambda row: _safe_div(row.get("market_value_usd", 0.0), row.get("supply_cost_basis_usd", 0.0)),
        axis=1,
    )

    delta = merged["market_value_usd"] - merged["supply_cost_basis_usd"]
    rolling_mean = delta.rolling(engine.mvrv_window_days, min_periods=1).mean()
    rolling_std = delta.rolling(engine.mvrv_window_days, min_periods=1).std(ddof=0)
    merged["mvrv_zscore"] = (delta - rolling_mean) / rolling_std.replace(0.0, np.nan)

    merged["nupl"] = merged.apply(
        lambda row: _safe_div(
            row.get("market_value_usd", 0.0) - row.get("supply_cost_basis_usd", 0.0),
            row.get("market_value_usd", 0.0),
        ),
        axis=1,
    )

    drop_columns = [
        "exchange_balance_sats",
        "exchange_weighted_age_days",
        "exchange_cost_basis_usd",
        "whale_balance_sats",
        "whale_weighted_age_days",
        "whale_cost_basis_usd",
    ]
    for column in drop_columns:
        if column in merged:
            merged.drop(columns=column, inplace=True)

    realized_cap_columns = list(_AGE_BUCKET_REALIZED_CAP_MAP.values()) + [
        "realized_cap_exchange",
        "realized_cap_whale",
    ]

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
        "sopr_entity_adjusted",
        "sopr_long_short_delta",
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
        "spent_value_btc",
        "exchange_supply_btc",
        "exchange_supply_pct",
        "exchange_net_flow_btc",
        "exchange_net_flow_usd",
        "exchange_dormancy",
        "whale_supply_btc",
        "whale_dormancy",
        "whale_realized_pl_usd",
        "spent_profit_delta_usd",
    ] + realized_cap_columns

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
    prepared = _prepare_spent_frame(spent_df)
    return _aggregate_prepared_spent(prepared)


def _prepare_spent_frame(spent_df: pd.DataFrame) -> pd.DataFrame:
    if spent_df.empty:
        columns = [
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
            "value_btc",
            "cost_basis_usd",
            "loss_component",
            "profit_component",
            "cdd_component",
            "entity_id",
            "entity_type",
        ]
        return pd.DataFrame(columns=columns)

    df = spent_df.copy()
    df["spend_time"] = pd.to_datetime(df["spend_time"], utc=True)
    df["created_time"] = pd.to_datetime(df["created_time"], utc=True)
    df["holding_days"] = _numeric_series(df, "holding_days")
    df["value_sats"] = pd.to_numeric(df["value_sats"], errors="coerce").fillna(0).astype("int64")
    df["value_btc"] = df["value_sats"] / _SATS_PER_BTC
    df["spend_price_close"] = _numeric_series(df, "spend_price_close", fill_value=None)
    df["creation_price_close"] = _numeric_series(df, "creation_price_close")
    spend_value_fallback = df["value_btc"] * df["spend_price_close"].fillna(0.0)
    df["realized_value_usd"] = _numeric_series(df, "realized_value_usd", fill_value=None)
    df["realized_value_usd"] = df["realized_value_usd"].fillna(spend_value_fallback)
    df["realized_value_usd"] = df["realized_value_usd"].fillna(0.0)
    df["realized_profit_usd"] = _numeric_series(df, "realized_profit_usd")
    df["cost_basis_usd"] = df["value_btc"] * df["creation_price_close"]
    df["loss_component"] = df["realized_profit_usd"].where(df["realized_profit_usd"] < 0, 0).abs()
    df["profit_component"] = df["realized_profit_usd"].where(df["realized_profit_usd"] > 0, 0.0)
    df["cdd_component"] = df["value_btc"] * df["holding_days"].clip(lower=0)
    if "entity_type" not in df.columns:
        df["entity_type"] = pd.NA
    if "entity_id" not in df.columns:
        df["entity_id"] = pd.NA
    df["entity_type"] = df["entity_type"].apply(
        lambda value: str(value).strip().lower() if pd.notna(value) else None
    )
    df.loc[df["entity_type"] == "", "entity_type"] = None
    df["entity_id"] = df["entity_id"].apply(lambda value: str(value) if pd.notna(value) else None)
    return df


def _aggregate_prepared_spent(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the spent UTXO table to day-level realized metrics."""

    if df.empty:
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

    df = df.copy()
    df["date"] = df["spend_time"].dt.date

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


def _snapshot_entity_totals(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "date",
        "exchange_balance_sats",
        "exchange_weighted_age_days",
        "exchange_cost_basis_usd",
        "whale_balance_sats",
        "whale_weighted_age_days",
        "whale_cost_basis_usd",
    ]
    if snapshot_df.empty or "entity_type" not in snapshot_df.columns:
        return pd.DataFrame(columns=columns)

    df = snapshot_df.copy()
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    df["entity_type"] = df["entity_type"].apply(
        lambda value: str(value).strip().lower() if pd.notna(value) else None
    )
    df.loc[df["entity_type"] == "", "entity_type"] = None
    df["balance_sats"] = _numeric_series(df, "balance_sats")
    df["avg_age_days"] = _numeric_series(df, "avg_age_days")
    df["cost_basis_usd"] = _numeric_series(df, "cost_basis_usd")
    df["cluster_balance_sats"] = _numeric_series(df, "cluster_balance_sats")
    df["balance_age_product"] = df["balance_sats"] * df["avg_age_days"].clip(lower=0.0)

    records: list[dict[str, float]] = []
    for snapshot_date, group in df.groupby("snapshot_date"):
        exchange_rows = group[group["entity_type"].isin(_EXCHANGE_ENTITY_TYPES)]
        exchange_balance = float(exchange_rows["balance_sats"].sum())
        exchange_age = _safe_div(exchange_rows["balance_age_product"].sum(), exchange_balance)
        exchange_cost = float(exchange_rows["cost_basis_usd"].sum())

        whale_rows = group[
            (group["entity_type"] == "whale")
            | (group["cluster_balance_sats"] >= _WHALE_THRESHOLD_SATS)
        ]
        whale_balance = float(whale_rows["balance_sats"].sum())
        whale_age = _safe_div(whale_rows["balance_age_product"].sum(), whale_balance)
        whale_cost = float(whale_rows["cost_basis_usd"].sum())

        records.append(
            {
                "date": snapshot_date,
                "exchange_balance_sats": exchange_balance,
                "exchange_weighted_age_days": exchange_age,
                "exchange_cost_basis_usd": exchange_cost,
                "whale_balance_sats": whale_balance,
                "whale_weighted_age_days": whale_age,
                "whale_cost_basis_usd": whale_cost,
            }
        )

    return pd.DataFrame.from_records(records, columns=columns)


def _snapshot_realized_cap_segments(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "date",
        *list(_AGE_BUCKET_REALIZED_CAP_MAP.values()),
        "realized_cap_exchange",
        "realized_cap_whale",
    ]
    if snapshot_df.empty:
        return pd.DataFrame(columns=columns)

    df = snapshot_df.copy()
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    if "age_bucket" in df.columns:
        df["age_bucket"] = df["age_bucket"].astype(str)
    else:
        df["age_bucket"] = ""
    df["cost_basis_usd"] = _numeric_series(df, "cost_basis_usd")
    if "entity_type" in df.columns:
        df["entity_type"] = df["entity_type"].apply(
            lambda value: str(value).strip().lower() if pd.notna(value) else None
        )
        df.loc[df["entity_type"] == "", "entity_type"] = None
    else:
        df["entity_type"] = None
    df["cluster_balance_sats"] = _numeric_series(df, "cluster_balance_sats")
    df["is_whale"] = (df["entity_type"] == "whale") | (
        df["cluster_balance_sats"] >= _WHALE_THRESHOLD_SATS
    )

    age_pivot = (
        df.pivot_table(
            index="snapshot_date",
            columns="age_bucket",
            values="cost_basis_usd",
            aggfunc="sum",
            fill_value=0.0,
        )
        .reindex(_AGE_BUCKET_REALIZED_CAP_MAP.keys(), axis=1, fill_value=0.0)
        .rename(columns=_AGE_BUCKET_REALIZED_CAP_MAP)
    )
    result = age_pivot.reset_index().rename(columns={"snapshot_date": "date"})

    exchange_cap = (
        df[df["entity_type"].isin(_EXCHANGE_ENTITY_TYPES)]
        .groupby("snapshot_date")["cost_basis_usd"]
        .sum()
    )
    whale_cap = df[df["is_whale"]].groupby("snapshot_date")["cost_basis_usd"].sum()

    exchange_cap = exchange_cap.rename("realized_cap_exchange").reset_index()
    exchange_cap.rename(columns={"snapshot_date": "date"}, inplace=True)
    whale_cap = whale_cap.rename("realized_cap_whale").reset_index()
    whale_cap.rename(columns={"snapshot_date": "date"}, inplace=True)

    result = result.merge(exchange_cap, on="date", how="left")
    result = result.merge(whale_cap, on="date", how="left")

    for column in columns[1:]:
        if column in result:
            result[column] = result[column].fillna(0.0)

    missing_columns = [column for column in columns if column not in result.columns]
    for column in missing_columns:
        result[column] = 0.0

    return result[columns]


def _compute_sopr_entity_adjusted(spent_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["date", "sopr_entity_adjusted"]
    if spent_df.empty:
        return pd.DataFrame(columns=columns)

    df = spent_df.copy()
    df["date"] = df["spend_time"].dt.date
    filtered = df[~df["entity_type"].isin(_EXCHANGE_ENTITY_TYPES)]
    grouped = filtered.groupby("date", as_index=False).agg(
        {
            "realized_value_usd": "sum",
            "cost_basis_usd": "sum",
        }
    )
    grouped["sopr_entity_adjusted"] = grouped.apply(
        lambda row: _safe_div(row["realized_value_usd"], row["cost_basis_usd"]), axis=1
    )
    return grouped[columns]


def _compute_sopr_long_short(spent_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["date", "sopr_long_short_delta"]
    if spent_df.empty:
        return pd.DataFrame(columns=columns)

    df = spent_df.copy()
    df["date"] = df["spend_time"].dt.date
    df["holding_days"] = _numeric_series(df, "holding_days")

    long_mask = df["holding_days"] >= _LONG_TERM_THRESHOLD_DAYS
    short_mask = ~long_mask

    def _ratio(subset: pd.DataFrame) -> pd.DataFrame:
        if subset.empty:
            return pd.DataFrame(columns=["date", "ratio"])
        grouped = subset.groupby("date", as_index=False).agg(
            {
                "realized_value_usd": "sum",
                "cost_basis_usd": "sum",
            }
        )
        grouped["ratio"] = grouped.apply(
            lambda row: _safe_div(row["realized_value_usd"], row["cost_basis_usd"]), axis=1
        )
        return grouped[["date", "ratio"]]

    long_ratio = _ratio(df[long_mask])
    short_ratio = _ratio(df[short_mask])

    merged = long_ratio.merge(short_ratio, on="date", how="outer", suffixes=("_long", "_short"))
    for column in ("ratio_long", "ratio_short"):
        if column in merged.columns:
            merged[column] = pd.to_numeric(merged[column], errors="coerce")
    numeric_fill = {column: 0.0 for column in ("ratio_long", "ratio_short") if column in merged.columns}
    merged = merged.fillna(numeric_fill)
    merged["sopr_long_short_delta"] = merged["ratio_long"] - merged["ratio_short"]
    return merged[["date", "sopr_long_short_delta"]]


def _compute_whale_realized_pl(spent_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["date", "whale_realized_pl_usd"]
    if spent_df.empty:
        return pd.DataFrame(columns=columns)

    df = spent_df.copy()
    df["date"] = df["spend_time"].dt.date
    whales = df[df["entity_type"] == "whale"].copy()
    if whales.empty:
        return pd.DataFrame(columns=columns)

    grouped = whales.groupby("date", as_index=False).agg(
        {
            "profit_component": "sum",
            "loss_component": "sum",
        }
    )
    grouped["whale_realized_pl_usd"] = grouped["profit_component"] - grouped["loss_component"]
    return grouped[["date", "whale_realized_pl_usd"]]


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
