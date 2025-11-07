from datetime import datetime, timezone
from pathlib import Path
import sys

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.append(str(ROOT / "src"))

from ingest import schemas  # type: ignore  # noqa: E402


def test_block_schema_metadata_version() -> None:
    schema = schemas.block_schema()
    assert schema.metadata is not None
    assert schema.metadata.get(b"schema_version") == schemas.SCHEMA_VERSION.encode("utf-8")


def test_record_batch_from_models_respects_schema() -> None:
    model = schemas.Block(
        height=1,
        hash="hash",
        time_utc=datetime.now(tz=timezone.utc),
        version=1,
        merkleroot="merkleroot",
        nonce=0,
        bits="1d00ffff",
        size=285,
        weight=600,
        tx_count=1,
    )

    table = schemas.record_batch_from_models([model], schemas.block_schema())
    assert isinstance(table, pa.Table)
    assert table.schema == schemas.block_schema()
    assert table.num_rows == 1
    assert table.column("height").to_pylist() == [1]
