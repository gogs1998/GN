from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Dict

import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

from ingest.config import IngestConfig, LimitsConfig, QAConfig, RPCConfig  # type: ignore  # noqa: E402
from ingest.pipeline import ProcessedHeightIndex, sync_range  # type: ignore  # noqa: E402


@dataclass
class FakeBlock:
    height: int
    hash: str
    previous_hash: str | None
    tx_suffix: str

    def as_dict(self) -> Dict[str, object]:
        block_time = datetime(2020, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=self.height)
        txid = f"tx-{self.tx_suffix}"
        return {
            "hash": self.hash,
            "previousblockhash": self.previous_hash,
            "time": block_time,
            "version": 1,
            "merkleroot": f"root-{self.tx_suffix}",
            "nonce": self.height,
            "bits": "1d00ffff",
            "size": 400,
            "weight": 1600,
            "tx": [
                {
                    "txid": txid,
                    "time": block_time,
                    "size": 200,
                    "weight": 800,
                    "version": 1,
                    "locktime": 0,
                    "vin": [
                        {
                            "coinbase": "00",
                            "sequence": 0,
                        }
                    ],
                    "vout": [
                        {
                            "value": 1.0,
                            "scriptPubKey": {
                                "type": "pubkeyhash",
                                "addresses": [f"addr-{self.tx_suffix}"],
                            },
                            "spent": False,
                        }
                    ],
                }
            ],
        }


class FakeBitcoinRPCClient:
    def __init__(self, chain: Dict[int, FakeBlock]) -> None:
        self._chain: Dict[int, FakeBlock] = chain
        self._refresh_lookup()
        self.closed = False

    def _refresh_lookup(self) -> None:
        self._hash_lookup = {block.hash: block.as_dict() for block in self._chain.values()}

    def update_chain(self, chain: Dict[int, FakeBlock]) -> None:
        self._chain = chain
        self._refresh_lookup()

    def get_block_hash(self, height: int) -> str:
        return self._chain[height].hash

    def get_block(self, block_hash: str, verbosity: int = 2) -> Dict[str, object]:
        return self._hash_lookup[block_hash]

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def ingest_config(tmp_path: Path) -> IngestConfig:
    partitions = {
        "blocks": "blocks/height={height_bucket}",
        "transactions": "transactions/height={height_bucket}",
        "txin": "txin/height={height_bucket}",
        "txout": "txout/height={height_bucket}",
    }
    return IngestConfig(
        data_root=tmp_path,
        partitions=partitions,
        height_bucket_size=1024,
        compression="zstd",
        zstd_level=3,
        rpc=RPCConfig(host="localhost", port=8332, user_env="BTC_USER", pass_env="BTC_PASS", timeout_seconds=120.0),
        limits=LimitsConfig(max_blocks_per_run=500, io_batch_size=16),
        qa=QAConfig(golden_days=[], tolerance_pct=1.0),
    )


def _block(height: int, previous: str | None, variant: str) -> FakeBlock:
    return FakeBlock(height=height, hash=f"block-{variant}-{height}", previous_hash=previous, tx_suffix=f"{variant}-{height}")


def _dataset_file(root: Path, dataset: str, height: int) -> Path:
    bucket = 0  # heights in tests stay within first bucket
    marker = f"{dataset}-h{height:012d}"
    partition_dir = root / dataset / f"height={bucket}"
    return partition_dir / f"part-{marker}.parquet"


def test_sync_range_uses_deterministic_files(tmp_path: Path, ingest_config: IngestConfig) -> None:
    chain = {
        0: _block(0, None, "a"),
        1: _block(1, "block-a-0", "a"),
        2: _block(2, "block-a-1", "a"),
    }
    client = FakeBitcoinRPCClient(chain)

    counts = sync_range(0, 2, config=ingest_config, client=client)
    assert counts["blocks"] == 3
    block_dir = ingest_config.data_root / "blocks" / "height=0"
    files = sorted(p.name for p in block_dir.glob("*.parquet"))
    assert files == [
        "part-blocks-h000000000000.parquet",
        "part-blocks-h000000000001.parquet",
        "part-blocks-h000000000002.parquet",
    ]

    # Run again with identical data to ensure files are replaced, not duplicated.
    counts_again = sync_range(0, 2, config=ingest_config, client=client)
    assert counts_again["blocks"] == 0
    files_again = sorted(p.name for p in block_dir.glob("*.parquet"))
    assert files_again == files


def test_sync_range_recovers_from_reorg(tmp_path: Path, ingest_config: IngestConfig) -> None:
    original_chain = {
        0: _block(0, None, "a"),
        1: _block(1, "block-a-0", "a"),
        2: _block(2, "block-a-1", "a"),
    }
    client = FakeBitcoinRPCClient(original_chain)
    sync_range(0, 2, config=ingest_config, client=client)

    reorg_chain = {
        0: original_chain[0],
        1: _block(1, "block-a-0", "b"),
        2: _block(2, "block-b-1", "b"),
        3: _block(3, "block-b-2", "b"),
    }
    client.update_chain(reorg_chain)

    sync_range(3, 3, config=ingest_config, client=client)

    index = ProcessedHeightIndex(ingest_config.data_root)
    for height in range(4):
        expected_hash = reorg_chain[height].hash
        assert index.hash_for(height) == expected_hash

    block_path_h1 = _dataset_file(ingest_config.data_root, "blocks", 1)
    assert block_path_h1.exists()
    table_h1 = pq.ParquetFile(block_path_h1).read()
    assert table_h1.to_pydict()["hash"] == [reorg_chain[1].hash]

    block_path_h2 = _dataset_file(ingest_config.data_root, "blocks", 2)
    table_h2 = pq.ParquetFile(block_path_h2).read()
    assert table_h1.schema == table_h2.schema