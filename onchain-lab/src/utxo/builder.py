from __future__ import annotations

import ast
import glob
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import LifecycleConfig
from .datasets import (
    CREATED_SCHEMA,
    SPENT_SCHEMA,
    LifecycleArtifacts,
    pipeline_version,
    write_created,
    write_spent,
)
from .linker import (
    LifecycleFrames,
    SourceFrames,
    attach_entity_metadata,
    build_lifecycle_frames,
    compute_lineage_id,
)


class SourceDataError(RuntimeError):
    """Raised when lifecycle source data is missing or inconsistent."""


@dataclass
class LifecycleBuildResult:
    artifacts: LifecycleArtifacts
    frames: LifecycleFrames


class LifecycleBuilder:
    def __init__(self, config: LifecycleConfig) -> None:
        self._config = config

    def build(self, *, persist: bool = True) -> LifecycleBuildResult:
        use_legacy = os.getenv("UTXO_LIFECYCLE_LEGACY", "0") == "1"
        if use_legacy:
            frames = self._load_source_frames()
            lifecycle = build_lifecycle_frames(frames)
        else:
            lifecycle = self._build_streaming_frames()
        created_table = pa.Table.from_pandas(
            lifecycle.created, schema=CREATED_SCHEMA, preserve_index=False
        )
        spent_table = pa.Table.from_pandas(
            lifecycle.spent, schema=SPENT_SCHEMA, preserve_index=False
        )
        artifacts = LifecycleArtifacts(created=created_table, spent=spent_table)

        if persist:
            writer = self._config.writer
            root = self._config.data.lifecycle_root
            write_created(
                artifacts.created,
                root,
                compression=writer.compression,
                compression_level=writer.zstd_level,
            )
            write_spent(
                artifacts.spent,
                root,
                compression=writer.compression,
                compression_level=writer.zstd_level,
            )

        return LifecycleBuildResult(artifacts=artifacts, frames=lifecycle)

    def _build_streaming_frames(self) -> LifecycleFrames:
        self._ensure_dataset_exists(self._config.data.ingest.blocks)
        assembler = _StreamingLifecycleAssembler(self._config, self._load_entity_lookup)
        return assembler.run()

    def _load_source_frames(self) -> SourceFrames:
        ingest = self._config.data.ingest
        price_cfg = self._config.data.price

        # Ensure block dataset exists even though the lifecycle pipeline does not yet consume it.
        self._ensure_dataset_exists(ingest.blocks)
        txout = self._read_dataset(ingest.txout)
        txin = self._read_dataset(ingest.txin)
        transactions = self._read_dataset(ingest.transactions)
        prices = self._read_dataset(price_cfg.parquet)
        entities = self._load_entity_lookup()

        price_df = prices.to_pandas()
        price_df = price_df[
            (price_df["symbol"] == price_cfg.symbol)
            & (price_df["freq"] == price_cfg.freq)
        ].copy()
        if price_df.empty:
            raise SourceDataError(
                f"Price dataset for symbol={price_cfg.symbol} freq={price_cfg.freq} is empty"
            )

        return SourceFrames(
            txout=txout.to_pandas(),
            txin=txin.to_pandas(),
            transactions=transactions.to_pandas(),
            prices=price_df,
            entity_lookup=entities,
        )

    def _read_dataset(self, pattern: str) -> pa.Table:
        matches = sorted(glob.glob(pattern, recursive=True))
        if not matches:
            raise SourceDataError(f"No parquet files matched pattern: {pattern}")
        tables = [pq.ParquetFile(match).read() for match in matches]
        table = pa.concat_tables(tables, promote=True) if len(tables) > 1 else tables[0]
        return table.combine_chunks()

    def _ensure_dataset_exists(self, pattern: str) -> None:
        if not any(glob.iglob(pattern, recursive=True)):
            raise SourceDataError(f"No parquet files matched pattern: {pattern}")

    def _load_entity_lookup(self) -> pd.DataFrame | None:
        entities_cfg = getattr(self._config.data, "entities", None)
        if not entities_cfg or entities_cfg.lookup is None:
            return None
        path = entities_cfg.lookup
        if not path.exists():
            raise SourceDataError(f"Entity lookup dataset missing at {path}")
        table = pq.read_table(path)
        df = table.to_pandas()
        required = {"address", "entity_id", "entity_type"}
        missing = required.difference(df.columns)
        if missing:
            raise SourceDataError(
                f"Entity lookup dataset at {path} missing columns: {sorted(missing)}"
            )
        return df


class _StreamingLifecycleAssembler:
    _CREATED_QUERY = """
        SELECT
            c.txid,
            c.vout,
            c.value_sats,
            c.script_type,
            c.addresses,
            c.created_height,
            c.created_time,
            c.created_date,
            c.creation_price_close,
            c.creation_price_ts,
            c.creation_price_source,
            c.creation_price_hash,
            c.creation_price_pipeline,
            h.spend_txid AS spend_txid_hint,
            h.spend_height AS spend_height_hint,
            h.spend_time AS spend_time_hint,
            h.spend_txid IS NOT NULL AS is_spent
        FROM created_join c
        LEFT JOIN spend_events h
            ON c.txid = h.source_txid AND c.vout = h.source_vout
    """

    _SPENT_QUERY = """
        SELECT
            s.source_txid,
            s.source_vout,
            s.spend_txid,
            COALESCE(c.value_sats, 0) AS value_sats,
            c.created_height,
            c.created_time,
            s.spend_height,
            s.spend_time,
            CAST(COALESCE(DATEDIFF('second', c.created_time, s.spend_time), 0) AS DOUBLE) AS holding_seconds,
            CAST(COALESCE(DATEDIFF('second', c.created_time, s.spend_time), 0) AS DOUBLE) / 86400.0 AS holding_days,
            c.creation_price_close,
            c.creation_price_ts,
            c.creation_price_source,
            s.spend_price_close,
            s.spend_price_ts,
            s.spend_price_source,
            CASE
                WHEN c.value_sats IS NULL OR s.spend_price_close IS NULL THEN NULL
                ELSE (CAST(c.value_sats AS DOUBLE) / 1e8) * s.spend_price_close
            END AS realized_value_usd,
            CASE
                WHEN c.value_sats IS NULL
                     OR s.spend_price_close IS NULL
                     OR c.creation_price_close IS NULL THEN NULL
                ELSE (CAST(c.value_sats AS DOUBLE) / 1e8) * (s.spend_price_close - c.creation_price_close)
            END AS realized_profit_usd,
            c.txid IS NULL AS is_orphan
        FROM spend_with_price s
        LEFT JOIN created_join c
            ON s.source_txid = c.txid AND s.source_vout = c.vout
    """

    def __init__(
        self,
        config: LifecycleConfig,
        entity_loader: Callable[[], Optional[pd.DataFrame]],
    ) -> None:
        self._config = config
        self._entity_loader = entity_loader

    @staticmethod
    def _escape(value: str) -> str:
        return value.replace("'", "''")

    @staticmethod
    def _path_literal(path: str | Path) -> str:
        normalized = Path(path).as_posix()
        return normalized.replace("'", "''")

    def run(self) -> LifecycleFrames:
        conn = duckdb.connect(database=":memory:")
        try:
            self._register_views(conn)
            created_arrow = conn.execute(self._CREATED_QUERY).arrow()
            created_df = created_arrow.read_all().to_pandas()
            spent_arrow = conn.execute(self._SPENT_QUERY).arrow()
            spent_df = spent_arrow.read_all().to_pandas()
        except duckdb.Error as exc:  # pragma: no cover - passthrough
            raise SourceDataError(f"DuckDB lifecycle assembly failed: {exc}") from exc
        finally:
            conn.close()
        created_df = self._finalize_created(created_df)
        spent_df = self._finalize_spent(spent_df, created_df)
        sort_created = ["created_height", "txid", "vout"]
        existing_created = [col for col in sort_created if col in created_df.columns]
        if existing_created:
            created_df = created_df.sort_values(existing_created).reset_index(drop=True)
        sort_spent = ["spend_height", "source_txid", "source_vout", "spend_txid"]
        existing_spent = [col for col in sort_spent if col in spent_df.columns]
        if existing_spent:
            spent_df = spent_df.sort_values(existing_spent).reset_index(drop=True)
        return LifecycleFrames(created=created_df, spent=spent_df)

    def _register_views(self, conn: duckdb.DuckDBPyConnection) -> None:
        ingest = self._config.data.ingest
        price_cfg = self._config.data.price
        conn.execute("SET TimeZone='UTC'")
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW transactions AS
            SELECT
                txid,
                height,
                time_utc,
                CAST(DATE_TRUNC('day', time_utc) AS DATE) AS time_date
            FROM read_parquet('{self._path_literal(ingest.transactions)}')
            """
        )
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW txout_view AS
            SELECT
                txid,
                idx AS vout,
                value_sats,
                script_type,
                addresses
            FROM read_parquet('{self._path_literal(ingest.txout)}')
            """
        )
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW daily_prices AS
            WITH ranked AS (
                SELECT
                    symbol,
                    freq,
                    ts,
                    close,
                    source,
                    raw_file_hash,
                    pipeline_version,
                    CAST(DATE_TRUNC('day', ts) AS DATE) AS price_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY CAST(DATE_TRUNC('day', ts) AS DATE)
                        ORDER BY ts DESC
                    ) AS rn
                FROM read_parquet('{self._path_literal(price_cfg.parquet)}')
                WHERE symbol = '{self._escape(price_cfg.symbol)}' AND freq = '{self._escape(price_cfg.freq)}'
            )
            SELECT
                symbol,
                freq,
                ts,
                close,
                source,
                raw_file_hash,
                pipeline_version,
                price_date
            FROM ranked
            WHERE rn = 1
            """
        )
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW spend_events AS
            SELECT
                i.prev_txid AS source_txid,
                i.prev_vout AS source_vout,
                i.txid AS spend_txid,
                t.height AS spend_height,
                t.time_utc AS spend_time,
                CAST(DATE_TRUNC('day', t.time_utc) AS DATE) AS spend_date
            FROM read_parquet('{self._path_literal(ingest.txin)}') i
            LEFT JOIN transactions t ON i.txid = t.txid
            WHERE NOT i.coinbase
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE VIEW spend_with_price AS
            SELECT
                e.*,
                dp.close AS spend_price_close,
                dp.ts AS spend_price_ts,
                dp.source AS spend_price_source
            FROM spend_events e
            LEFT JOIN daily_prices dp ON e.spend_date = dp.price_date
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE VIEW created_join AS
            SELECT
                o.txid,
                o.vout,
                o.value_sats,
                o.script_type,
                o.addresses,
                t.height AS created_height,
                t.time_utc AS created_time,
                t.time_date AS created_date,
                dp.close AS creation_price_close,
                dp.ts AS creation_price_ts,
                dp.source AS creation_price_source,
                dp.raw_file_hash AS creation_price_hash,
                dp.pipeline_version AS creation_price_pipeline
            FROM txout_view o
            LEFT JOIN transactions t ON o.txid = t.txid
            LEFT JOIN daily_prices dp ON t.time_date = dp.price_date
            """
        )

    def _finalize_created(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            df["addresses"] = pd.Series(dtype=object)
            df["entity_id"] = pd.Series(dtype=object)
            df["entity_type"] = pd.Series(dtype=object)
            df["lineage_id"] = pd.Series(dtype=object)
            df["pipeline_version"] = pd.Series(dtype=object)
            return df

        df["addresses"] = df["addresses"].apply(self._normalize_addresses)
        df["vout"] = pd.to_numeric(df["vout"], errors="coerce").fillna(0).astype("int32")
        df["value_sats"] = pd.to_numeric(df["value_sats"], errors="coerce").fillna(0).astype("int64")
        for column in ("created_height", "spend_height_hint"):
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
        df["created_time"] = pd.to_datetime(df["created_time"], utc=True, errors="coerce")
        df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce").dt.date
        df["creation_price_ts"] = pd.to_datetime(df["creation_price_ts"], utc=True, errors="coerce")
        df["spend_time_hint"] = pd.to_datetime(df["spend_time_hint"], utc=True, errors="coerce")
        df["is_spent"] = (
            df["is_spent"].fillna(False).apply(lambda value: bool(value)).astype(object)
        )

        lookup = self._entity_loader()
        df = attach_entity_metadata(df, lookup)
        df["addresses"] = df["addresses"].apply(self._normalize_addresses)
        df["lineage_id"] = df.apply(
            lambda row: compute_lineage_id(row["txid"], str(row["vout"])),
            axis=1,
        )
        df["pipeline_version"] = pipeline_version()
        return df

    def _finalize_spent(self, df: pd.DataFrame, created_df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            df["entity_id"] = pd.Series(dtype=object)
            df["entity_type"] = pd.Series(dtype=object)
            df["lineage_id"] = pd.Series(dtype=object)
            df["pipeline_version"] = pd.Series(dtype=object)
            return df

        df["source_vout"] = pd.to_numeric(df["source_vout"], errors="coerce").fillna(0).astype("int32")
        df["value_sats"] = pd.to_numeric(df["value_sats"], errors="coerce").fillna(0).astype("int64")
        for column in ("created_height", "spend_height"):
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
        df["created_time"] = pd.to_datetime(df["created_time"], utc=True, errors="coerce")
        df["spend_time"] = pd.to_datetime(df["spend_time"], utc=True, errors="coerce")
        df["holding_seconds"] = pd.to_numeric(df["holding_seconds"], errors="coerce").fillna(0.0)
        df["holding_days"] = pd.to_numeric(df["holding_days"], errors="coerce").fillna(0.0)
        for column in (
            "creation_price_close",
            "spend_price_close",
            "realized_value_usd",
            "realized_profit_usd",
        ):
            df[column] = pd.to_numeric(df[column], errors="coerce")
        df["is_orphan"] = df["is_orphan"].fillna(False).astype(bool)

        entity_cols = created_df[["txid", "vout", "entity_id", "entity_type"]].copy()
        entity_cols.rename(columns={"txid": "source_txid", "vout": "source_vout"}, inplace=True)
        if not entity_cols.empty:
            df = df.merge(entity_cols, how="left", on=["source_txid", "source_vout"])
        else:
            df["entity_id"] = pd.NA
            df["entity_type"] = pd.NA

        df["entity_id"] = df["entity_id"].astype(object)
        df["entity_type"] = df["entity_type"].astype(object)
        df["lineage_id"] = df.apply(
            lambda row: compute_lineage_id(row["source_txid"], str(row["source_vout"]), row["spend_txid"]),
            axis=1,
        )
        df["pipeline_version"] = pipeline_version()
        return df

    @staticmethod
    def _normalize_addresses(value: object) -> list[str]:
        if hasattr(value, "to_pylist"):
            try:
                return _StreamingLifecycleAssembler._normalize_addresses(value.to_pylist())
            except TypeError:  # pyarrow scalars without to_pylist
                pass
        if hasattr(value, "tolist") and not isinstance(value, str):
            try:
                as_list = value.tolist()
                return _StreamingLifecycleAssembler._normalize_addresses(as_list)
            except TypeError:
                pass
        if value in (None, ""):
            return []
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            queue: list[str] = [stripped]
            if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in ("'", '"'):
                inner = stripped[1:-1].strip()
                if inner:
                    queue.append(inner)
            seen: set[str] = set()
            while queue:
                candidate = queue.pop(0)
                if candidate in seen:
                    continue
                seen.add(candidate)
                try:
                    parsed = ast.literal_eval(candidate)
                except (ValueError, SyntaxError):
                    parsed = None
                if isinstance(parsed, (list, tuple, set)):
                    return _StreamingLifecycleAssembler._normalize_addresses(list(parsed))
                if isinstance(parsed, str) and parsed != candidate:
                    literal = parsed.strip()
                    if literal:
                        queue.append(literal)
                        continue
                if candidate:
                    return [candidate]
            return []
        if isinstance(value, (list, tuple, set)):
            result: list[str] = []
            for item in value:
                result.extend(_StreamingLifecycleAssembler._normalize_addresses(item))
            return result
        return [str(value)]


__all__ = ["LifecycleBuilder", "LifecycleBuildResult", "SourceDataError"]
