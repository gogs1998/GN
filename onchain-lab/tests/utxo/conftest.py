from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.utxo.config import LifecycleConfig


@pytest.fixture()
def sample_config(tmp_path):
    ingest_dir = tmp_path / "ingest"
    price_dir = tmp_path / "prices"
    lifecycle_dir = tmp_path / "utxo"
    ingest_dir.mkdir(parents=True, exist_ok=True)
    price_dir.mkdir(parents=True, exist_ok=True)
    lifecycle_dir.mkdir(parents=True, exist_ok=True)

    blocks = pa.table(
        {
            "height": [100, 101],
            "hash": ["block100", "block101"],
            "time_utc": [
                datetime(2024, 1, 1, 0, 4, tzinfo=timezone.utc),
                datetime(2024, 1, 2, 0, 2, tzinfo=timezone.utc),
            ],
            "version": [2, 2],
            "merkleroot": ["root100", "root101"],
            "nonce": [0, 0],
            "bits": ["1d00ffff", "1d00ffff"],
            "size": [1_000, 1_050],
            "weight": [4_000, 4_200],
            "tx_count": [1, 1],
        }
    )
    pq.write_table(blocks, ingest_dir / "blocks.parquet")

    transactions = pa.table(
        {
            "txid": ["txA", "txB"],
            "height": [100, 101],
            "time_utc": [
                datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc),
                datetime(2024, 1, 2, 0, 3, tzinfo=timezone.utc),
            ],
            "size": [250, 180],
            "weight": [900, 720],
            "version": [2, 2],
            "locktime": [0, 0],
            "vin_count": [1, 1],
            "vout_count": [2, 1],
        }
    )
    pq.write_table(transactions, ingest_dir / "transactions.parquet")

    txout = pa.table(
        {
            "txid": ["txA", "txA"],
            "idx": [0, 1],
            "value_sats": [100_000_000, 50_000_000],
            "script_type": ["p2pkh", "p2pkh"],
            "addresses": pa.array([["addr1"], ["addr2"]], type=pa.list_(pa.string())),
            "is_spent": [False, False],
        }
    )
    pq.write_table(txout, ingest_dir / "txout.parquet")

    txin = pa.table(
        {
            "txid": ["txA", "txB"],
            "idx": [0, 0],
            "coinbase": [True, False],
            "prev_txid": [None, "txA"],
            "prev_vout": [None, 1],
            "sequence": [0, 0],
        }
    )
    pq.write_table(txin, ingest_dir / "txin.parquet")

    prices = pa.table(
        {
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "freq": ["1d", "1d"],
            "ts": [
                datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc),
            ],
            "open": [30_000.0, 31_000.0],
            "high": [31_000.0, 32_500.0],
            "low": [29_500.0, 30_500.0],
            "close": [30_500.0, 32_000.0],
            "volume": [1_000.0, 1_200.0],
            "source": ["primary", "primary"],
            "raw_file_hash": ["hash1", "hash2"],
            "ingested_at": [
                datetime(2024, 1, 1, 2, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 2, 2, 0, tzinfo=timezone.utc),
            ],
            "pipeline_version": ["price.v1", "price.v1"],
            "schema_version": [1, 1],
        }
    )
    pq.write_table(prices, price_dir / "prices.parquet")

    return LifecycleConfig.model_validate(
        {
            "data": {
                "ingest": {
                    "blocks": str(ingest_dir / "blocks.parquet"),
                    "transactions": str(ingest_dir / "transactions.parquet"),
                    "txin": str(ingest_dir / "txin.parquet"),
                    "txout": str(ingest_dir / "txout.parquet"),
                },
                "price": {
                    "parquet": str(price_dir / "prices.parquet"),
                    "symbol": "BTCUSDT",
                    "freq": "1d",
                },
                "lifecycle_root": str(lifecycle_dir),
            },
            "snapshot": {"timezone": "UTC", "daily_close_hhmm": "00:00"},
            "writer": {"compression": "zstd", "zstd_level": 3},
            "qa": {
                "price_coverage_min_pct": 99.0,
                "supply_tolerance_sats": 10,
                "lifespan_max_days": 4000,
                "max_snapshot_gap_pct": 0.0,
            },
        }
    )
