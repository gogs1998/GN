from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Iterable, Sequence

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.lib import ArrowException

from pydantic import BaseModel

from .schemas import SCHEMA_REGISTRY, record_batch_from_models


class WriterError(RuntimeError):
    """Raised when parquet writing fails."""


def bucket_height(height: int, bucket_size: int) -> int:
    if bucket_size <= 0:
        raise WriterError("bucket_size must be positive")
    return (height // bucket_size) * bucket_size


def partition_path(root: Path, template: str, *, height_bucket: int) -> Path:
    relative = template.format(height_bucket=height_bucket)
    return (root / relative).resolve()


def ensure_schema(dataset: str) -> pa.Schema:
    try:
        return SCHEMA_REGISTRY[dataset]
    except KeyError as exc:
        raise WriterError(f"Unknown dataset '{dataset}'") from exc


def models_to_table(dataset: str, models: Iterable[BaseModel]) -> pa.Table:
    schema = ensure_schema(dataset)
    return record_batch_from_models(models, schema)


def write_table(
    dataset: str,
    table: pa.Table,
    *,
    output_dir: Path,
    file_stem: str,
    compression: str = "zstd",
    zstd_level: int = 6,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{file_stem}.parquet"
    target = output_dir / filename
    temp_name = f".{filename}.{uuid.uuid4().hex}.tmp"
    temp_path = output_dir / temp_name

    try:
        pq.write_table(
            table,
            temp_path,
            compression=compression,
            compression_level=zstd_level,
            use_dictionary=True,
            coerce_timestamps="us",
        )
        os.replace(temp_path, target)
    except (OSError, ArrowException) as exc:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
        raise WriterError(f"Failed to write parquet for dataset '{dataset}': {exc}") from exc

    return target


def append_models(
    dataset: str,
    models: Sequence[BaseModel],
    *,
    root: Path,
    partition_template: str,
    height_bucket: int,
    compression: str,
    zstd_level: int,
    marker: str | None = None,
) -> Path:
    if not models:
        raise WriterError("No records to write.")
    table = models_to_table(dataset, models)
    output_dir = partition_path(root, partition_template, height_bucket=height_bucket)
    token = marker or uuid.uuid4().hex[:10]
    file_stem = f"part-{token}"
    return write_table(
        dataset,
        table,
        output_dir=output_dir,
        file_stem=file_stem,
        compression=compression,
        zstd_level=zstd_level,
    )
