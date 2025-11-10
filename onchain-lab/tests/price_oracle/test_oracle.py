from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from price_oracle.config import AlignmentConfig, ConfigError, PriceOracleConfig, QAConfig  # type: ignore  # noqa: E402
from price_oracle.normalize import PIPELINE_VERSION  # type: ignore  # noqa: E402
from price_oracle.oracle import PriceOracle, QAError  # type: ignore  # noqa: E402
from price_oracle.store import PriceStore  # type: ignore  # noqa: E402


def _write_binance_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        headers = ["open_time", "open", "high", "low", "close", "volume", "close_time"]
        handle.write(",".join(headers) + "\n")
        for row in rows:
            handle.write(",".join(row[h] for h in headers) + "\n")


def _write_coinbase_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        headers = ["time", "open", "high", "low", "close", "volume"]
        handle.write(",".join(headers) + "\n")
        for row in rows:
            handle.write(",".join(row[h] for h in headers) + "\n")


def _config(tmp_path: Path) -> PriceOracleConfig:
    data_root = tmp_path / "prices"
    alignment = AlignmentConfig(timezone="UTC", daily_close_hhmm="00:00")
    qa = QAConfig(max_gap_hours=6, max_basis_diff_pct=5.0)
    return PriceOracleConfig(
        data_root=data_root,
        symbols=["BTCUSDT"],
        freqs=["1h"],
        primary="binance",
        fallback="coinbase",
        alignment=alignment,
        qa=qa,
    )


def test_price_oracle_build_writes_parquet(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    raw_root = cfg.data_root.parent / "raw"
    binance_path = raw_root / "binance" / "BTCUSDT-1h.csv"
    coinbase_path = raw_root / "coinbase" / "BTCUSDT-1h.csv"

    binance_rows = [
        {
            "open_time": "1710000000000",
            "open": "100",
            "high": "110",
            "low": "95",
            "close": "105",
            "volume": "10",
            "close_time": "1710000000000",
        },
        {
            "open_time": "1710003600000",
            "open": "105",
            "high": "115",
            "low": "100",
            "close": "112",
            "volume": "8",
            "close_time": "1710003600000",
        },
    ]
    fallback_rows = [
        {
            "time": "2024-03-09T16:00:00Z",
            "open": "105",
            "high": "115",
            "low": "100",
            "close": "107",
            "volume": "9",
        }
    ]

    _write_binance_csv(binance_path, binance_rows)
    _write_coinbase_csv(coinbase_path, fallback_rows)

    oracle = PriceOracle(cfg)
    summaries = oracle.build()
    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.symbol == "BTCUSDT"
    assert summary.freq == "1h"
    assert summary.records_written == 2
    assert summary.qa.ok

    store = PriceStore(cfg.data_root)
    stored = store.read("BTCUSDT", "1h")
    assert len(stored) == 2
    assert stored[0].close == 105.0
    assert stored[1].source == "binance"
    assert stored[0].pipeline_version == PIPELINE_VERSION
    assert stored[0].raw_file_hash == stored[1].raw_file_hash
    assert stored[0].ingested_at.tzinfo is not None
    assert stored[0].ingested_at <= datetime.now(timezone.utc)
    delta = stored[0].ingested_at - stored[1].ingested_at
    assert abs(delta.total_seconds()) < 1


def test_price_oracle_gap_trigger(tmp_path: Path) -> None:
    cfg = _config(tmp_path).model_copy(
        update={"qa": QAConfig(max_gap_hours=1, max_basis_diff_pct=5.0)}
    )

    raw_root = cfg.data_root.parent / "raw"
    binance_path = raw_root / "binance" / "BTCUSDT-1h.csv"

    rows = [
        {
            "open_time": "1710000000000",
            "open": "100",
            "high": "110",
            "low": "95",
            "close": "105",
            "volume": "10",
            "close_time": "1710000000000",
        },
        {
            "open_time": "1710014400000",
            "open": "106",
            "high": "116",
            "low": "100",
            "close": "110",
            "volume": "9",
            "close_time": "1710014400000",
        },
    ]
    _write_binance_csv(binance_path, rows)

    oracle = PriceOracle(cfg)
    with pytest.raises(QAError):
        oracle.build()


def test_alignment_config_rejects_unknown_timezone() -> None:
    with pytest.raises(ConfigError):
        AlignmentConfig(timezone="Fake/Zone", daily_close_hhmm="00:00")


def test_price_oracle_fallback_only_triggers_qa(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    raw_root = cfg.data_root.parent / "raw"
    fallback_path = raw_root / "coinbase" / "BTCUSDT-1h.csv"

    _write_coinbase_csv(
        fallback_path,
        [
            {
                "time": "2024-03-09T16:00:00Z",
                "open": "105",
                "high": "115",
                "low": "100",
                "close": "107",
                "volume": "9",
            }
        ],
    )

    oracle = PriceOracle(cfg)
    with pytest.raises(QAError):
        oracle.build()


def test_price_oracle_primary_empty_csv_fails(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    raw_root = cfg.data_root.parent / "raw"
    binance_path = raw_root / "binance" / "BTCUSDT-1h.csv"
    fallback_path = raw_root / "coinbase" / "BTCUSDT-1h.csv"

    _write_binance_csv(binance_path, [])
    _write_coinbase_csv(
        fallback_path,
        [
            {
                "time": "2024-03-09T16:00:00Z",
                "open": "105",
                "high": "115",
                "low": "100",
                "close": "107",
                "volume": "9",
            }
        ],
    )

    oracle = PriceOracle(cfg)
    with pytest.raises(QAError):
        oracle.build()


def test_price_oracle_basis_diff_threshold(tmp_path: Path) -> None:
    cfg = _config(tmp_path).model_copy(
        update={"qa": QAConfig(max_gap_hours=6, max_basis_diff_pct=1.0)}
    )
    raw_root = cfg.data_root.parent / "raw"
    binance_path = raw_root / "binance" / "BTCUSDT-1h.csv"
    coinbase_path = raw_root / "coinbase" / "BTCUSDT-1h.csv"

    _write_binance_csv(
        binance_path,
        [
            {
                "open_time": "1710000000000",
                "open": "100",
                "high": "110",
                "low": "95",
                "close": "105",
                "volume": "10",
                "close_time": "1710000000000",
            }
        ],
    )
    _write_coinbase_csv(
        coinbase_path,
        [
            {
                "time": "2024-03-09T16:00:00Z",
                "open": "105",
                "high": "115",
                "low": "100",
                "close": "110",
                "volume": "9",
            }
        ],
    )

    oracle = PriceOracle(cfg)
    with pytest.raises(QAError):
        oracle.build()


def test_price_oracle_errors_when_all_sources_missing(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    oracle = PriceOracle(cfg)

    with pytest.raises(QAError):
        oracle.build()


def test_price_oracle_missing_fallback_records_fails(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    raw_root = cfg.data_root.parent / "raw"
    binance_path = raw_root / "binance" / "BTCUSDT-1h.csv"

    _write_binance_csv(
        binance_path,
        [
            {
                "open_time": "1710000000000",
                "open": "100",
                "high": "110",
                "low": "95",
                "close": "105",
                "volume": "10",
                "close_time": "1710000000000",
            }
        ],
    )

    oracle = PriceOracle(cfg)
    with pytest.raises(QAError):
        oracle.build()


def test_price_oracle_boundary_mismatch(tmp_path: Path) -> None:
    cfg = _config(tmp_path).model_copy(
        update={
            "freqs": ["1d"],
            "qa": QAConfig(max_gap_hours=6, max_basis_diff_pct=5.0),
        }
    )
    raw_root = cfg.data_root.parent / "raw"
    binance_path = raw_root / "binance" / "BTCUSDT-1d.csv"

    _write_binance_csv(
        binance_path,
        [
            {
                "open_time": "1710000000000",
                "open": "100",
                "high": "110",
                "low": "95",
                "close": "105",
                "volume": "10",
                "close_time": "1710000000000",
            }
        ],
    )

    oracle = PriceOracle(cfg)
    with pytest.raises(ValueError):
        oracle.build()
