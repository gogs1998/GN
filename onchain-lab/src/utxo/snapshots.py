from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
import glob
from typing import Dict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import LifecycleConfig
from .datasets import SNAPSHOT_SCHEMA, pipeline_version, write_snapshot


class SnapshotError(RuntimeError):
    """Raised when snapshots fail to build."""


class SnapshotBuilder:
    def __init__(self, config: LifecycleConfig) -> None:
        self._config = config

    def build(
        self,
        created: pa.Table,
        spent: pa.Table,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        persist: bool = True,
    ) -> Dict[date, pa.Table]:
        created_df = created.to_pandas()
        spent_df = spent.to_pandas()

        if created_df.empty:
            return {}

        created_df["addresses"] = created_df["addresses"].apply(_normalize_addresses)
        created_df["created_time"] = pd.to_datetime(created_df["created_time"], utc=True)
        created_df["created_date"] = pd.to_datetime(created_df["created_date"], utc=True).dt.date

        if "spend_time_hint" in created_df.columns:
            created_df["spend_time_hint"] = pd.to_datetime(
                created_df["spend_time_hint"], utc=True
            )

        spent_df["spend_time"] = pd.to_datetime(spent_df["spend_time"], utc=True)

        spend_map = (
            spent_df.set_index(["source_txid", "source_vout"])["spend_time"].to_dict()
        )

        created_df["actual_spend_time"] = created_df.apply(
            lambda row: spend_map.get((row["txid"], row["vout"]), row.get("spend_time_hint")),
            axis=1,
        )
        created_df["actual_spend_time"] = pd.to_datetime(created_df["actual_spend_time"], utc=True)

        if start_date is None:
            start_date = created_df["created_date"].min()
        if end_date is None:
            end_date_candidates = [created_df["created_date"].max()]
            if not spent_df.empty:
                spend_dates = spent_df["spend_time"].dropna()
                if not spend_dates.empty:
                    end_date_candidates.append(spend_dates.dt.date.max())
            end_date = max(end_date_candidates)

        price_daily = self._load_daily_prices(start_date, end_date)

        zone = self._config.snapshot.zoneinfo()
        close_time = self._config.snapshot.close_time()

        snapshots: Dict[date, pa.Table] = {}
        for current_date in pd.date_range(start_date, end_date, freq="D"):
            day = current_date.date()
            closing_local = datetime.combine(day, close_time, tzinfo=zone) + timedelta(days=1)
            boundary_utc = closing_local.astimezone(timezone.utc)

            active_mask = (created_df["created_time"] < boundary_utc) & (
                created_df["actual_spend_time"].isna()
                | (created_df["actual_spend_time"] > boundary_utc)
            )
            active = created_df[active_mask].copy()

            if active.empty:
                table = SNAPSHOT_SCHEMA.empty_table()
                snapshots[day] = table
                if persist:
                    write_snapshot(
                        table,
                        self._config.data.lifecycle_root,
                        day,
                        compression=self._config.writer.compression,
                        compression_level=self._config.writer.zstd_level,
                    )
                continue

            active["age_days"] = (
                boundary_utc - active["created_time"]
            ).dt.total_seconds() / 86400.0

            bucketed_records = self._aggregate_active(active, day, price_daily.get(day))
            table = pa.Table.from_pandas(bucketed_records, schema=SNAPSHOT_SCHEMA, preserve_index=False)
            snapshots[day] = table

            if persist:
                write_snapshot(
                    table,
                    self._config.data.lifecycle_root,
                    day,
                    compression=self._config.writer.compression,
                    compression_level=self._config.writer.zstd_level,
                )

        return snapshots

    def _load_daily_prices(self, start_date: date, end_date: date) -> Dict[date, dict]:
        price_cfg = self._config.data.price
        matches = sorted(glob.glob(price_cfg.parquet, recursive=True))
        if not matches:
            return {}
        tables = [pq.read_table(path) for path in matches]
        table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        df = table.to_pandas()
        df = df[
            (df["symbol"] == price_cfg.symbol)
            & (df["freq"] == price_cfg.freq)
        ].copy()
        if df.empty:
            return {}
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df["price_date"] = df["ts"].dt.date
        df.sort_values("ts", inplace=True)
        df = df[(df["price_date"] >= start_date) & (df["price_date"] <= end_date)]

        result: Dict[date, dict] = {}
        for price_date, group in df.groupby("price_date"):
            latest = group.iloc[-1]
            result[price_date] = {
                "close": latest.get("close"),
                "ts": latest.get("ts"),
            }
        return result

    def _aggregate_active(
        self,
        active: pd.DataFrame,
        snapshot_date: date,
        price_info: dict | None,
    ) -> pd.DataFrame:
        if price_info is None:
            price_close = None
            price_ts = None
        else:
            price_close = price_info.get("close")
            price_ts = price_info.get("ts")

        records: list[dict] = []
        grouped: Dict[tuple[str, str], dict] = defaultdict(lambda: {
            "output_count": 0,
            "balance_sats": 0,
            "balance_btc": 0.0,
            "age_days_total": 0.0,
            "cost_basis_usd": 0.0,
        })

        def bucket(age: float) -> str:
            if age < 1:
                return "000-001d"
            if age < 7:
                return "001-007d"
            if age < 30:
                return "007-030d"
            if age < 180:
                return "030-180d"
            if age < 365:
                return "180-365d"
            return "365d+"

        for _, row in active.iterrows():
            addresses = _normalize_addresses(row.get("addresses"))
            group_key = _derive_group_key(addresses, row["script_type"])
            age_bucket = bucket(row["age_days"])
            key = (group_key, age_bucket)
            entry = grouped[key]
            entry["output_count"] += 1
            value_sats = int(row["value_sats"])
            entry["balance_sats"] += value_sats
            entry["balance_btc"] += value_sats / 1e8
            entry["age_days_total"] += row["age_days"]
            price_close_creation = row.get("creation_price_close")
            if pd.notna(price_close_creation):
                entry["cost_basis_usd"] += (value_sats / 1e8) * float(price_close_creation)

        for (group_key, age_bucket), entry in grouped.items():
            output_count = entry["output_count"]
            avg_age = entry["age_days_total"] / output_count if output_count else 0.0
            market_value = (
                entry["balance_btc"] * price_close if price_close is not None else None
            )
            records.append(
                {
                    "snapshot_date": snapshot_date,
                    "group_key": group_key,
                    "age_bucket": age_bucket,
                    "output_count": output_count,
                    "balance_sats": entry["balance_sats"],
                    "balance_btc": entry["balance_btc"],
                    "avg_age_days": avg_age,
                    "cost_basis_usd": entry["cost_basis_usd"],
                    "market_value_usd": market_value,
                    "price_close": price_close,
                    "price_ts": price_ts,
                    "pipeline_version": pipeline_version(),
                    "lineage_id": f"{snapshot_date.isoformat()}::{group_key}::{age_bucket}",
                }
            )

        return pd.DataFrame.from_records(records, columns=SNAPSHOT_SCHEMA.names)


def _normalize_addresses(raw: object) -> list[str]:
    if hasattr(raw, "to_pylist"):
        try:
            return _normalize_addresses(raw.to_pylist())
        except TypeError:
            pass
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        flattened: list[str] = []
        for item in raw:
            flattened.extend(_normalize_addresses(item))
        return flattened
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []
    if isinstance(raw, str):
        text = raw.strip()
        if text.startswith("[") and text.endswith("]"):
            try:
                import ast

                parsed = ast.literal_eval(text)
                if isinstance(parsed, list):
                    return [str(item) for item in parsed]
            except (SyntaxError, ValueError):
                pass
        return [text]
    return [str(raw)]


def _derive_group_key(addresses: list[str], script_type: str) -> str:
    if not addresses:
        return f"script:{script_type}"
    candidate = str(addresses[0])
    text = candidate.strip()
    if text.startswith("[") and text.endswith("]"):
        try:
            import ast

            parsed = ast.literal_eval(text)
            if isinstance(parsed, list) and parsed:
                return str(parsed[0])
        except (SyntaxError, ValueError):
            text = text.strip("[]'\"")
            return text or f"script:{script_type}"
    return text


__all__ = ["SnapshotBuilder", "SnapshotError"]
