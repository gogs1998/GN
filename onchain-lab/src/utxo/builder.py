from __future__ import annotations

import glob
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import LifecycleConfig
from .datasets import (
    CREATED_SCHEMA,
    SPENT_SCHEMA,
    LifecycleArtifacts,
    write_created,
    write_spent,
)
from .linker import LifecycleFrames, SourceFrames, build_lifecycle_frames


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
        frames = self._load_source_frames()
        lifecycle = build_lifecycle_frames(frames)
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

    def _load_source_frames(self) -> SourceFrames:
        ingest = self._config.data.ingest
        price_cfg = self._config.data.price

        # Ensure block dataset exists even though the lifecycle pipeline does not yet consume it.
        self._read_dataset(ingest.blocks)
        txout = self._read_dataset(ingest.txout)
        txin = self._read_dataset(ingest.txin)
        transactions = self._read_dataset(ingest.transactions)
        prices = self._read_dataset(price_cfg.parquet)

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
        )

    def _read_dataset(self, pattern: str) -> pa.Table:
        matches = sorted(glob.glob(pattern, recursive=True))
        if not matches:
            raise SourceDataError(f"No parquet files matched pattern: {pattern}")
        tables = [pq.ParquetFile(match).read() for match in matches]
        table = pa.concat_tables(tables, promote=True) if len(tables) > 1 else tables[0]
        return table.combine_chunks()


__all__ = ["LifecycleBuilder", "LifecycleBuildResult", "SourceDataError"]
