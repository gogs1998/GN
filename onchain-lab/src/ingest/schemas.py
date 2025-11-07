from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

import pyarrow as pa
from pydantic import BaseModel, Field, field_validator

SCHEMA_VERSION = "ingest.v1"
SCHEMA_METADATA = {b"schema_version": SCHEMA_VERSION.encode("utf-8")}


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise ValueError("Datetime must be timezone aware in UTC.")
    if dt.tzinfo != timezone.utc:
        return dt.astimezone(timezone.utc)
    return dt


class Block(BaseModel):
    height: int
    hash: str
    time_utc: datetime
    version: int
    merkleroot: str
    nonce: int
    bits: str
    size: int
    weight: int
    tx_count: int

    @field_validator("time_utc")
    @classmethod
    def _ensure_time_utc(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


class Transaction(BaseModel):
    txid: str
    height: int
    time_utc: datetime
    size: int
    weight: int
    version: int
    locktime: int
    vin_count: int
    vout_count: int

    @field_validator("time_utc")
    @classmethod
    def _ensure_time_utc(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


class TxIn(BaseModel):
    txid: str
    idx: int
    coinbase: bool
    prev_txid: Optional[str]
    prev_vout: Optional[int]
    sequence: int


class TxOut(BaseModel):
    txid: str
    idx: int
    value_sats: int
    script_type: str
    addresses: List[str] = Field(default_factory=list)
    is_spent: bool = False


def block_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("height", pa.int64()),
            pa.field("hash", pa.string()),
            pa.field("time_utc", pa.timestamp("us", tz="UTC")),
            pa.field("version", pa.int32()),
            pa.field("merkleroot", pa.string()),
            pa.field("nonce", pa.uint32()),
            pa.field("bits", pa.string()),
            pa.field("size", pa.int32()),
            pa.field("weight", pa.int32()),
            pa.field("tx_count", pa.int32()),
        ],
        metadata=SCHEMA_METADATA,
    )


def transaction_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("txid", pa.string()),
            pa.field("height", pa.int64()),
            pa.field("time_utc", pa.timestamp("us", tz="UTC")),
            pa.field("size", pa.int32()),
            pa.field("weight", pa.int32()),
            pa.field("version", pa.int32()),
            pa.field("locktime", pa.int32()),
            pa.field("vin_count", pa.int32()),
            pa.field("vout_count", pa.int32()),
        ],
        metadata=SCHEMA_METADATA,
    )


def txin_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("txid", pa.string()),
            pa.field("idx", pa.int32()),
            pa.field("coinbase", pa.bool_()),
            pa.field("prev_txid", pa.string()).with_nullable(True),
            pa.field("prev_vout", pa.int32()).with_nullable(True),
            pa.field("sequence", pa.int64()),
        ],
        metadata=SCHEMA_METADATA,
    )


def txout_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("txid", pa.string()),
            pa.field("idx", pa.int32()),
            pa.field("value_sats", pa.int64()),
            pa.field("script_type", pa.string()),
            pa.field("addresses", pa.list_(pa.string())),
            pa.field("is_spent", pa.bool_()),
        ],
        metadata=SCHEMA_METADATA,
    )


SCHEMA_REGISTRY: Dict[str, pa.Schema] = {
    "blocks": block_schema(),
    "transactions": transaction_schema(),
    "txin": txin_schema(),
    "txout": txout_schema(),
}


def record_batch_from_models(models: Iterable[BaseModel], schema: pa.Schema) -> pa.Table:
    """Convert a sequence of pydantic models into an Arrow table enforcing schema."""
    rows = [model.model_dump() for model in models]
    return pa.Table.from_pylist(rows, schema=schema)
