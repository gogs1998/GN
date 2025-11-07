from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import DefaultDict, Dict, List, MutableMapping, Tuple

from rich.console import Console
from pydantic import BaseModel

from .config import IngestConfig, load_config
from .rpc import BitcoinRPCClient, RPCError
from .schemas import Block, Transaction, TxIn, TxOut
from .writer import WriterError, append_models, bucket_height

console = Console()


class ProcessedHeightIndex:
    def __init__(self, data_root: Path) -> None:
        self._marker_dir = data_root / "_markers"
        self._marker_dir.mkdir(parents=True, exist_ok=True)
        self._state_path = self._marker_dir / "state.json"
        self._state = self._read_state()

    def _read_state(self) -> Dict[str, int]:
        if not self._state_path.exists():
            return {"max_height": -1}
        with self._state_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _write_state(self) -> None:
        with self._state_path.open("w", encoding="utf-8") as handle:
            json.dump(self._state, handle)

    def is_done(self, height: int) -> bool:
        return self._marker_path(height).exists()

    def mark_done(self, height: int) -> None:
        marker = self._marker_path(height)
        marker.write_text("", encoding="utf-8")
        if height > self._state.get("max_height", -1):
            self._state["max_height"] = height
            self._write_state()

    def max_height(self) -> int:
        return self._state.get("max_height", -1)

    def _marker_path(self, height: int) -> Path:
        return self._marker_dir / f"{height}.done"


def _create_rpc_client(config: IngestConfig) -> BitcoinRPCClient:
    user, password = config.rpc.credentials()
    return BitcoinRPCClient(
        config.rpc.host,
        config.rpc.port,
        user,
        password,
    )


def _btc_to_sats(value: Decimal | float | str | int) -> int:
    decimal_value = Decimal(str(value))
    return int((decimal_value * Decimal("100000000")).to_integral_value())


def _ensure_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    raise ValueError(f"Expected datetime in UTC, received {value!r}")


def _extract_addresses(script_pub_key: Dict[str, object]) -> List[str]:
    """Return all addresses parsed from a scriptPubKey object."""
    addresses: List[str] = []
    raw_addresses = script_pub_key.get("addresses")
    if isinstance(raw_addresses, list):
        addresses.extend([str(addr) for addr in raw_addresses])
    single = script_pub_key.get("address")
    if isinstance(single, str):
        addresses.append(single)
    return addresses


def _parse_block(height: int, block: Dict[str, object]) -> Tuple[
    Block,
    List[Transaction],
    List[TxIn],
    List[TxOut],
]:
    block_time = _ensure_datetime(block["time"])
    block_record = Block(
        height=height,
        hash=str(block["hash"]),
        time_utc=block_time,
        version=int(block.get("version", 0)),
        merkleroot=str(block.get("merkleroot", "")),
        nonce=int(block.get("nonce", 0)),
        bits=str(block.get("bits", "")),
        size=int(block.get("size", 0)),
        weight=int(block.get("weight", block.get("size", 0) * 4)),
        tx_count=len(block.get("tx", [])),
    )

    transactions: List[Transaction] = []
    txins: List[TxIn] = []
    txouts: List[TxOut] = []

    for tx in block.get("tx", []):
        tx_time = tx.get("time", block_time)
        txid = str(tx.get("txid") or tx.get("hash"))
        transactions.append(
            Transaction(
                txid=txid,
                height=height,
                time_utc=tx_time,
                size=int(tx.get("size", 0)),
                weight=int(tx.get("weight", tx.get("size", 0) * 4)),
                version=int(tx.get("version", 0)),
                locktime=int(tx.get("locktime", 0)),
                vin_count=len(tx.get("vin", [])),
                vout_count=len(tx.get("vout", [])),
            )
        )

        for vin_idx, vin in enumerate(tx.get("vin", [])):
            coinbase = "coinbase" in vin
            prev_txid = vin.get("txid")
            prev_vout = vin.get("vout")
            txins.append(
                TxIn(
                    txid=txid,
                    idx=vin_idx,
                    coinbase=coinbase,
                    prev_txid=str(prev_txid) if prev_txid is not None else None,
                    prev_vout=int(prev_vout) if prev_vout is not None else None,
                    sequence=int(vin.get("sequence", 0)),
                )
            )

        for vout_idx, vout in enumerate(tx.get("vout", [])):
            script_pub_key = vout.get("scriptPubKey", {})
            if not isinstance(script_pub_key, dict):
                script_pub_key = {}
            addresses = _extract_addresses(script_pub_key)
            txouts.append(
                TxOut(
                    txid=txid,
                    idx=vout_idx,
                    value_sats=_btc_to_sats(vout.get("value", 0)),
                    script_type=str(script_pub_key.get("type", "unknown")),
                    addresses=addresses,
                    is_spent=bool(vout.get("spent", False)),
                )
            )

    return block_record, transactions, txins, txouts


def _flush_buffer(
    *,
    key: Tuple[str, int],
    buffer: List[BaseModel],
    config: IngestConfig,
    counts: MutableMapping[str, int],
) -> None:
    dataset, bucket = key
    if not buffer:
        return
    append_models(
        dataset,
        buffer,
        root=config.data_root,
        partition_template=config.partitions[dataset],
        height_bucket=bucket,
        compression=config.compression,
        zstd_level=config.zstd_level,
    )
    counts[dataset] += len(buffer)
    buffer.clear()


def sync_range(
    start_height: int,
    end_height: int,
    *,
    config: IngestConfig | None = None,
    client: BitcoinRPCClient | None = None,
) -> Dict[str, int]:
    if start_height > end_height:
        raise ValueError("start_height must be <= end_height")
    cfg = config or load_config()
    cfg.data_root.mkdir(parents=True, exist_ok=True)
    max_blocks = cfg.limits.max_blocks_per_run
    if (end_height - start_height + 1) > max_blocks:
        end_height = start_height + max_blocks - 1

    created_client = client or _create_rpc_client(cfg)
    own_client = client is None
    height_index = ProcessedHeightIndex(cfg.data_root)
    counts: Dict[str, int] = {name: 0 for name in ("blocks", "transactions", "txin", "txout")}
    buffers: DefaultDict[Tuple[str, int], List[BaseModel]] = defaultdict(list)

    try:
        for height in range(start_height, end_height + 1):
            if height_index.is_done(height):
                console.log(f"Skipping height {height} (already processed)")
                continue

            block_hash = created_client.get_block_hash(height)
            block = created_client.get_block(block_hash, verbosity=2)
            block_record, tx_records, txin_records, txout_records = _parse_block(height, block)
            bucket = bucket_height(height, cfg.height_bucket_size)

            buffers[("blocks", bucket)].append(block_record)
            buffers[("transactions", bucket)].extend(tx_records)
            buffers[("txin", bucket)].extend(txin_records)
            buffers[("txout", bucket)].extend(txout_records)

            _flush_buffer(
                key=("blocks", bucket),
                buffer=buffers[("blocks", bucket)],
                config=cfg,
                counts=counts,
            )
            _flush_buffer(
                key=("transactions", bucket),
                buffer=buffers[("transactions", bucket)],
                config=cfg,
                counts=counts,
            )

            if len(buffers[("txin", bucket)]) >= cfg.limits.io_batch_size:
                _flush_buffer(
                    key=("txin", bucket),
                    buffer=buffers[("txin", bucket)],
                    config=cfg,
                    counts=counts,
                )

            if len(buffers[("txout", bucket)]) >= cfg.limits.io_batch_size:
                _flush_buffer(
                    key=("txout", bucket),
                    buffer=buffers[("txout", bucket)],
                    config=cfg,
                    counts=counts,
                )

            height_index.mark_done(height)
            console.log(
                f"Processed height {height}: blocks=1 tx={len(tx_records)} vin={len(txin_records)} vout={len(txout_records)}"
            )

        # Final flush
        for key, buffer in list(buffers.items()):
            _flush_buffer(key=key, buffer=buffer, config=cfg, counts=counts)

    except (RPCError, WriterError) as exc:
        console.log(f"Ingestion halted: {exc}")
        raise
    finally:
        if own_client:
            created_client.close()

    return counts


def sync_from_tip(
    max_blocks: int,
    *,
    config: IngestConfig | None = None,
    client: BitcoinRPCClient | None = None,
) -> Dict[str, int]:
    if max_blocks <= 0:
        raise ValueError("max_blocks must be positive")
    cfg = config or load_config()
    created_client = client or _create_rpc_client(cfg)
    own_client = client is None

    try:
        tip = created_client.get_block_count()
        height_index = ProcessedHeightIndex(cfg.data_root)
        start_height = height_index.max_height() + 1
        if start_height > tip:
            console.log("Ledger already fully synced to tip.")
            return {name: 0 for name in ("blocks", "transactions", "txin", "txout")}

        budget = min(cfg.limits.max_blocks_per_run, max_blocks)
        end_height = min(tip, start_height + budget - 1)
        console.log(f"Syncing heights [{start_height}, {end_height}] (tip={tip})")
        return sync_range(start_height, end_height, config=cfg, client=created_client)
    finally:
        if own_client:
            created_client.close()
