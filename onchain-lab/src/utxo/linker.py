from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd

from .datasets import pipeline_version


def _ensure_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if value is None:
        return []
    if isinstance(value, tuple):
        return [str(item) for item in value]
    return [str(value)]


def compute_lineage_id(*parts: str) -> str:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(part.encode("utf-8"))
    return digest.hexdigest()


@dataclass
class SourceFrames:
    txout: pd.DataFrame
    txin: pd.DataFrame
    transactions: pd.DataFrame
    prices: pd.DataFrame
    entity_lookup: Optional[pd.DataFrame] = None


@dataclass
class LifecycleFrames:
    created: pd.DataFrame
    spent: pd.DataFrame


def build_lifecycle_frames(frames: SourceFrames) -> LifecycleFrames:
    txout = frames.txout.copy()
    txout.rename(columns={"idx": "vout"}, inplace=True)
    txout["addresses"] = txout["addresses"].apply(_ensure_list)

    transactions = frames.transactions.copy()
    transactions["time_utc"] = pd.to_datetime(transactions["time_utc"], utc=True)

    txout_enriched = txout.merge(
        transactions[["txid", "height", "time_utc"]],
        on="txid",
        how="left",
        validate="many_to_one",
    )
    txout_enriched.rename(columns={"height": "created_height", "time_utc": "created_time"}, inplace=True)
    txout_enriched["created_time"] = pd.to_datetime(txout_enriched["created_time"], utc=True)
    txout_enriched["created_date"] = txout_enriched["created_time"].dt.date

    prices = frames.prices.copy()
    prices["ts"] = pd.to_datetime(prices["ts"], utc=True)
    prices.sort_values("ts", inplace=True)
    prices["price_date"] = prices["ts"].dt.date
    daily_prices = (
        prices.drop_duplicates(subset=["price_date"], keep="last")
        .set_index("price_date")
        [["close", "ts", "source", "raw_file_hash", "pipeline_version"]]
    )

    created = txout_enriched.merge(
        daily_prices.reset_index(),
        left_on="created_date",
        right_on="price_date",
        how="left",
    )
    created.rename(
        columns={
            "close": "creation_price_close",
            "ts": "creation_price_ts",
            "source": "creation_price_source",
            "raw_file_hash": "creation_price_hash",
            "pipeline_version": "creation_price_pipeline",
        },
        inplace=True,
    )
    created.drop(columns=["price_date"], inplace=True)
    created = attach_entity_metadata(created, frames.entity_lookup)

    spends = frames.txin.copy()
    spends = spends[~spends["coinbase"]].copy()
    spends.rename(
        columns={"txid": "spend_txid", "prev_txid": "source_txid", "prev_vout": "source_vout"},
        inplace=True,
    )
    spends = spends.merge(
        transactions[["txid", "height", "time_utc"]],
        left_on="spend_txid",
        right_on="txid",
        how="left",
        validate="many_to_one",
    )
    spends.drop(columns=["txid"], inplace=True)
    spends.rename(columns={"height": "spend_height", "time_utc": "spend_time"}, inplace=True)
    spends["spend_time"] = pd.to_datetime(spends["spend_time"], utc=True)
    spends["spend_date"] = spends["spend_time"].dt.date

    spends = spends.merge(
        daily_prices.reset_index(),
        left_on="spend_date",
        right_on="price_date",
        how="left",
    )
    spends.rename(
        columns={
            "close": "spend_price_close",
            "ts": "spend_price_ts",
            "source": "spend_price_source",
        },
        inplace=True,
    )
    spends.drop(columns=["price_date"], inplace=True)

    created = created.merge(
        spends[["source_txid", "source_vout", "spend_txid", "spend_height", "spend_time"]],
        left_on=["txid", "vout"],
        right_on=["source_txid", "source_vout"],
        how="left",
    )
    created.rename(
        columns={
            "spend_txid": "spend_txid_hint",
            "spend_height": "spend_height_hint",
            "spend_time": "spend_time_hint",
        },
        inplace=True,
    )
    created.drop(columns=["source_txid", "source_vout"], inplace=True)
    created["is_spent"] = created["spend_txid_hint"].notna()
    created["is_spent"] = created["is_spent"].astype(bool)

    created["lineage_id"] = created.apply(
        lambda row: compute_lineage_id(row["txid"], str(row["vout"])), axis=1
    )
    created["pipeline_version"] = pipeline_version()

    spend_join = spends.merge(
        created[
            [
                "txid",
                "vout",
                "value_sats",
                "created_height",
                "created_time",
                "creation_price_close",
                "creation_price_ts",
                "creation_price_source",
                "entity_id",
                "entity_type",
            ]
        ],
        left_on=["source_txid", "source_vout"],
        right_on=["txid", "vout"],
        how="left",
        indicator="creation_match",
    )

    spend_join.rename(columns={"txid": "created_txid", "vout": "created_vout"}, inplace=True)
    spend_join["value_sats"] = spend_join["value_sats"].fillna(0).astype("int64")
    spend_join["is_orphan"] = spend_join["creation_match"] == "left_only"
    spend_join["is_orphan"] = spend_join["is_orphan"].astype(bool)

    spend_join["holding_seconds"] = (
        spend_join["spend_time"] - spend_join["created_time"]
    ).dt.total_seconds()
    spend_join.loc[spend_join["holding_seconds"].isna(), "holding_seconds"] = 0.0
    spend_join["holding_days"] = spend_join["holding_seconds"] / 86400.0

    spend_join["realized_value_usd"] = (
        (spend_join["value_sats"] / 1e8) * spend_join["spend_price_close"]
    )
    spend_join["realized_profit_usd"] = (
        (spend_join["value_sats"] / 1e8)
        * (spend_join["spend_price_close"] - spend_join["creation_price_close"])
    )

    if spend_join.empty:
        spend_join["lineage_id"] = pd.Series(dtype="object", index=spend_join.index)
    else:
        spend_join["lineage_id"] = spend_join.apply(
            lambda row: compute_lineage_id(
                row["source_txid"], str(row["source_vout"]), row["spend_txid"]
            ),
            axis=1,
        )
    spend_join["pipeline_version"] = pipeline_version()

    spent_columns = [
        "source_txid",
        "source_vout",
        "spend_txid",
        "value_sats",
        "created_height",
        "created_time",
        "spend_height",
        "spend_time",
        "holding_seconds",
        "holding_days",
        "creation_price_close",
        "creation_price_ts",
        "creation_price_source",
        "spend_price_close",
        "spend_price_ts",
        "spend_price_source",
        "realized_value_usd",
        "realized_profit_usd",
        "is_orphan",
        "entity_id",
        "entity_type",
        "lineage_id",
        "pipeline_version",
    ]

    spent = spend_join[spent_columns].copy()
    spent.sort_values(["spend_height", "spend_txid", "source_vout"], inplace=True)

    created_columns = [
        "txid",
        "vout",
        "value_sats",
        "script_type",
        "addresses",
        "created_height",
        "created_time",
        "created_date",
        "creation_price_close",
        "creation_price_ts",
        "creation_price_source",
        "creation_price_hash",
        "creation_price_pipeline",
        "spend_txid_hint",
        "spend_height_hint",
        "spend_time_hint",
        "is_spent",
        "entity_id",
        "entity_type",
        "lineage_id",
        "pipeline_version",
    ]
    created = created[created_columns].copy()
    created.sort_values(["created_height", "txid", "vout"], inplace=True)

    return LifecycleFrames(created=created, spent=spent)


def attach_entity_metadata(
    created: pd.DataFrame, entity_lookup: Optional[pd.DataFrame]
) -> pd.DataFrame:
    result = created.copy()
    result["entity_id"] = pd.NA
    result["entity_type"] = pd.NA
    if entity_lookup is None or entity_lookup.empty:
        return result

    lookup = entity_lookup.copy()
    lookup["address"] = lookup["address"].astype(str).str.strip()
    lookup = lookup[lookup["address"] != ""]
    if lookup.empty:
        return result

    lookup["_addr_key"] = lookup["address"].str.lower()
    lookup = lookup.drop_duplicates("_addr_key", keep="last")
    id_map = lookup.set_index("_addr_key")["entity_id"].to_dict()
    type_map = lookup.set_index("_addr_key")["entity_type"].astype(str).str.lower().to_dict()

    def resolve(addresses: list[str]) -> tuple[Optional[str], Optional[str]]:
        for raw in addresses:
            if raw is None:
                continue
            key = str(raw).strip().lower()
            if not key:
                continue
            entity_id = id_map.get(key)
            entity_type = type_map.get(key)
            if entity_id is not None or entity_type is not None:
                return entity_id, entity_type
        return None, None

    resolved = result["addresses"].apply(resolve)
    result["entity_id"] = resolved.apply(lambda x: x[0])
    result["entity_type"] = resolved.apply(lambda x: x[1])
    return result


__all__ = [
    "SourceFrames",
    "LifecycleFrames",
    "attach_entity_metadata",
    "build_lifecycle_frames",
    "compute_lineage_id",
]
