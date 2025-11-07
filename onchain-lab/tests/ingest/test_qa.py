from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
import sys

import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.append(str(ROOT / "src"))

from ingest.config import IngestConfig, LimitsConfig, QAConfig, RPCConfig  # type: ignore  # noqa: E402
from ingest.qa import QAError, run_golden_day_checks  # type: ignore  # noqa: E402
from ingest.schemas import (  # type: ignore  # noqa: E402
    Block,
    Transaction,
    TxIn,
    TxOut,
    block_schema,
    record_batch_from_models,
    transaction_schema,
    txin_schema,
    txout_schema,
)


@pytest.fixture()
def sample_config(tmp_path: Path) -> IngestConfig:
    return IngestConfig(
        data_root=tmp_path,
        partitions={
            "blocks": "blocks/height={height_bucket}/",
            "transactions": "tx/height={height_bucket}/",
            "txin": "txin/height={height_bucket}/",
            "txout": "txout/height={height_bucket}/",
        },
        height_bucket_size=10000,
        compression="zstd",
        zstd_level=6,
        rpc=RPCConfig(host="localhost", port=8332, user_env="USER", pass_env="PASS"),
        limits=LimitsConfig(max_blocks_per_run=1000, io_batch_size=50),
        qa=QAConfig(golden_days=[date(2020, 5, 11)], tolerance_pct=0.1),
    )


@pytest.fixture()
def golden_ref_path(tmp_path: Path) -> Path:
    path = tmp_path / "golden_refs.json"
    payload = {
        "2020-05-11": {
            "blocks": 2,
            "transactions": 3,
            "coinbase_sats": 1250000000,
            "tolerance_pct": 0.1,
        }
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


@pytest.fixture()
def sample_lake(tmp_path: Path, sample_config: IngestConfig) -> Path:
    ts0 = datetime(2020, 5, 11, 0, 0, tzinfo=timezone.utc)
    ts1 = datetime(2020, 5, 11, 0, 10, tzinfo=timezone.utc)

    blocks = [
        Block(height=630000, hash="hash0", time_utc=ts0, version=1, merkleroot="root0", nonce=0, bits="1d00ffff", size=1000, weight=4000, tx_count=2),
        Block(height=630001, hash="hash1", time_utc=ts1, version=1, merkleroot="root1", nonce=1, bits="1d00ffff", size=900, weight=3600, tx_count=1),
        TxIn(txid="coinbase1", idx=0, coinbase=True, prev_txid=None, prev_vout=None, sequence=0),
        # Coinbase input from a different day that must be ignored
        TxIn(txid="prior_coinbase", idx=0, coinbase=True, prev_txid=None, prev_vout=None, sequence=0),
    ]

    txs = [
        Transaction(txid="coinbase0", height=630000, time_utc=ts0, size=150, weight=600, version=2, locktime=0, vin_count=1, vout_count=1),
        Transaction(txid="tx-normal", height=630000, time_utc=ts0, size=200, weight=800, version=2, locktime=0, vin_count=1, vout_count=2),
        Transaction(txid="coinbase1", height=630001, time_utc=ts1, size=155, weight=620, version=2, locktime=0, vin_count=1, vout_count=1),
    ]

    txins = [
        TxIn(txid="coinbase0", idx=0, coinbase=True, prev_txid=None, prev_vout=None, sequence=0),
        TxIn(txid="tx-normal", idx=0, coinbase=False, prev_txid="coinbase0", prev_vout=0, sequence=0),
        TxIn(txid="coinbase1", idx=0, coinbase=True, prev_txid=None, prev_vout=None, sequence=0),
    ]

    txouts = [
        TxOut(txid="coinbase0", idx=0, value_sats=625000000, script_type="pubkeyhash", addresses=["addr0"], is_spent=False),
        TxOut(txid="tx-normal", idx=0, value_sats=1000, script_type="pubkeyhash", addresses=["addr1"], is_spent=False),
        TxOut(txid="tx-normal", idx=1, value_sats=624999000, script_type="pubkeyhash", addresses=["addr2"], is_spent=False),
        TxOut(txid="coinbase1", idx=0, value_sats=625000000, script_type="pubkeyhash", addresses=["addr3"], is_spent=False),
        TxOut(txid="prior_coinbase", idx=0, value_sats=5000000000, script_type="pubkeyhash", addresses=["addr4"], is_spent=False),
    ]

    root = sample_config.data_root
    (root / "blocks" / "height=0").mkdir(parents=True, exist_ok=True)
    (root / "tx" / "height=0").mkdir(parents=True, exist_ok=True)
    (root / "txin" / "height=0").mkdir(parents=True, exist_ok=True)
    (root / "txout" / "height=0").mkdir(parents=True, exist_ok=True)

    pq.write_table(record_batch_from_models(blocks, block_schema()), root / "blocks" / "height=0" / "part-test.parquet")
    pq.write_table(record_batch_from_models(txs, transaction_schema()), root / "tx" / "height=0" / "part-test.parquet")
    pq.write_table(record_batch_from_models(txins, txin_schema()), root / "txin" / "height=0" / "part-test.parquet")
    pq.write_table(record_batch_from_models(txouts, txout_schema()), root / "txout" / "height=0" / "part-test.parquet")

    return root


def test_golden_day_matches_reference(
    sample_config: IngestConfig, sample_lake: Path, golden_ref_path: Path
) -> None:
    metrics = run_golden_day_checks(
        target=date(2020, 5, 11),
        config=sample_config,
        references_path=golden_ref_path,
    )
    assert metrics == {
        "blocks": 2,
        "transactions": 3,
        "coinbase_sats": 1250000000,
    }


def test_golden_day_out_of_tolerance(
    sample_config: IngestConfig, sample_lake: Path, golden_ref_path: Path
) -> None:
    # Rewrite txout partition with incorrect coinbase totals to trigger QA failure
    out_dir = sample_config.data_root / "txout" / "height=0"
    for file in out_dir.glob("*.parquet"):
        file.unlink()

    pq.write_table(
        record_batch_from_models(
            [
                TxOut(txid="coinbase0", idx=0, value_sats=100, script_type="pubkeyhash", addresses=["addr0"], is_spent=False),
                TxOut(txid="coinbase1", idx=0, value_sats=100, script_type="pubkeyhash", addresses=["addr3"], is_spent=False),
            ],
            txout_schema(),
        ),
        out_dir / "part-mod.parquet",
    )

    with pytest.raises(QAError):
        run_golden_day_checks(
            target=date(2020, 5, 11),
            config=sample_config,
            references_path=golden_ref_path,
        )
