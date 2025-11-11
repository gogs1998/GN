from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.utxo.builder import LifecycleBuilder
from src.utxo.qa import LifecycleQA
from src.utxo.snapshots import SnapshotBuilder


def _build_and_snapshot(config) -> None:
    builder = LifecycleBuilder(config)
    build_result = builder.build(persist=True)
    snapshot_builder = SnapshotBuilder(config)
    snapshot_builder.build(
        build_result.artifacts.created, build_result.artifacts.spent, persist=True
    )


def test_qa_passes_on_clean_data(sample_config):
    _build_and_snapshot(sample_config)
    qa_runner = LifecycleQA(sample_config)
    results = qa_runner.run()
    assert all(result.passed for result in results)


def test_qa_flags_orphan_spend(sample_config):
    txin_path = Path(sample_config.data.ingest.txin)
    existing = pq.read_table(txin_path)
    orphan = pa.table(
        {
            "txid": ["txC"],
            "idx": [0],
            "coinbase": [False],
            "prev_txid": ["missing"],
            "prev_vout": [0],
            "sequence": [0],
        }
    )
    pq.write_table(pa.concat_tables([existing, orphan]), txin_path)

    tx_path = Path(sample_config.data.ingest.transactions)
    tx_table = pq.read_table(tx_path)
    extra_tx = pa.table(
        {
            "txid": ["txC"],
            "height": [102],
            "time_utc": [datetime(2024, 1, 3, 0, 1, tzinfo=timezone.utc)],
            "size": [190],
            "weight": [760],
            "version": [2],
            "locktime": [0],
            "vin_count": [1],
            "vout_count": [1],
        }
    )
    pq.write_table(pa.concat_tables([tx_table, extra_tx]), tx_path)

    _build_and_snapshot(sample_config)
    qa_runner = LifecycleQA(sample_config)
    results = qa_runner.run()
    orphan_result = next(result for result in results if result.name == "orphan_spends")
    assert not orphan_result.passed
    assert orphan_result.details["orphan_count"] >= 1
