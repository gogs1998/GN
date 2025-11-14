from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.utxo.builder import LifecycleBuilder
from src.utxo.config import LifecycleConfig


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _sample_config(tmp_path: Path) -> LifecycleConfig:
    ingest_root = tmp_path / "ingest"
    blocks_path = ingest_root / "blocks" / "blocks.parquet"
    tx_path = ingest_root / "tx" / "tx.parquet"
    txout_path = ingest_root / "txout" / "txout.parquet"
    txin_path = ingest_root / "txin" / "txin.parquet"
    price_path = tmp_path / "prices" / "prices.parquet"
    entities_path = tmp_path / "entities" / "entities.parquet"

    _write_parquet(pd.DataFrame({"height": [0]}), blocks_path)

    tx_df = pd.DataFrame(
        {
            "txid": ["tx1", "tx2"],
            "height": [100, 101],
            "time_utc": [
                pd.Timestamp("2025-01-01T00:00:00Z"),
                pd.Timestamp("2025-01-02T00:00:00Z"),
            ],
        }
    )
    _write_parquet(tx_df, tx_path)

    txout_df = pd.DataFrame(
        {
            "txid": ["tx1"],
            "idx": [0],
            "value_sats": [500_000_000],
            "script_type": ["p2pkh"],
            "addresses": [["addr1"]],
        }
    )
    _write_parquet(txout_df, txout_path)

    txin_df = pd.DataFrame(
        {
            "txid": ["tx2"],
            "prev_txid": ["tx1"],
            "prev_vout": [0],
            "coinbase": [False],
        }
    )
    _write_parquet(txin_df, txin_path)

    price_df = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "freq": ["1d"],
            "ts": [pd.Timestamp("2025-01-01T00:00:00Z")],
            "close": [45000.0],
            "source": ["binance"],
            "raw_file_hash": ["hash"],
            "pipeline_version": ["prices.v1"],
        }
    )
    _write_parquet(price_df, price_path)

    entities_df = pd.DataFrame(
        {
            "address": ["addr1"],
            "entity_id": ["entity-1"],
            "entity_type": ["exchange"],
        }
    )
    _write_parquet(entities_df, entities_path)

    config = LifecycleConfig.model_validate(
        {
            "data": {
                "ingest": {
                    "blocks": str(blocks_path),
                    "transactions": str(tx_path),
                    "txin": str(txin_path),
                    "txout": str(txout_path),
                },
                "price": {
                    "parquet": str(price_path),
                    "symbol": "BTCUSDT",
                    "freq": "1d",
                },
                "entities": {"lookup": str(entities_path)},
                "lifecycle_root": str(tmp_path / "lifecycle"),
            },
            "snapshot": {"timezone": "UTC", "daily_close_hhmm": "00:00"},
            "writer": {"compression": "zstd", "zstd_level": 3},
            "qa": {
                "price_coverage_min_pct": 99.0,
                "supply_tolerance_sats": 1,
                "lifespan_max_days": 3650,
                "max_snapshot_gap_pct": 0.0,
            },
        }
    )
    return config

def test_streaming_builder_generates_frames(tmp_path, monkeypatch):
    config = _sample_config(tmp_path)
    monkeypatch.delenv("UTXO_LIFECYCLE_LEGACY", raising=False)
    builder = LifecycleBuilder(config)

    result = builder.build(persist=False)
    created = result.frames.created
    spent = result.frames.spent

    assert len(created) == 1
    assert created.iloc[0]["is_spent"] is True
    assert created.iloc[0]["entity_id"] == "entity-1"
    assert not spent.empty
    assert spent["is_orphan"].eq(False).all()
