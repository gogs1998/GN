from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from price_oracle.normalize import PIPELINE_VERSION, PriceRecord  # type: ignore  # noqa: E402
from price_oracle.sources import load_records  # type: ignore  # noqa: E402


def test_load_binance_records(tmp_path: Path) -> None:
    csv_path = tmp_path / "binance.csv"
    csv_path.write_text(
        "open_time,open,high,low,close,volume,close_time\n"
        "1,100,110,90,105,12,3600\n",
        encoding="utf-8",
    )
    records = load_records("binance", csv_path, "1h", "BTCUSDT")
    assert len(records) == 1
    record = records[0]
    assert isinstance(record, PriceRecord)
    assert record.close == 105.0
    assert record.ts == datetime.fromtimestamp(3600, tz=timezone.utc)
    assert record.pipeline_version == PIPELINE_VERSION
    with csv_path.open("rb") as handle:
        expected_hash = hashlib.sha256(handle.read()).hexdigest()
    assert record.raw_file_hash == expected_hash
    assert record.ingested_at.tzinfo is not None
    assert datetime.now(timezone.utc) - record.ingested_at < timedelta(seconds=5)


def test_load_unknown_source(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        load_records("unknown", tmp_path / "missing.csv", "1h", "BTCUSDT")


def test_load_binance_malformed_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "binance.csv"
    csv_path.write_text("a,b,c\n1,2,3\n", encoding="utf-8")
    with pytest.raises(ValueError) as exc:
        load_records("binance", csv_path, "1h", "BTCUSDT")
    assert "binance.csv" in str(exc.value)


def test_load_empty_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "binance.csv"
    csv_path.write_text("open_time,open,high,low,close,volume,close_time\n", encoding="utf-8")
    records = load_records("binance", csv_path, "1h", "BTCUSDT")
    assert records == []
