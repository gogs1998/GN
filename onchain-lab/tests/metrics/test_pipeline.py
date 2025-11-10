from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from metrics.compute import BuildResult, build_daily_metrics  # type: ignore  # noqa: E402
from metrics.config import (  # type: ignore  # noqa: E402
    DataConfig,
    EngineConfig,
    GoldenDay,
    LifecyclePaths,
    MetricsConfig,
    QAConfig,
    WriterConfig,
)
from metrics.datasets import METRICS_SCHEMA  # type: ignore  # noqa: E402
from metrics.formulas import MetricsComputationResult, compute_metrics, pipeline_version  # type: ignore  # noqa: E402
from metrics.qa import MetricsQAError, run_qa_checks  # type: ignore  # noqa: E402


def _write_price(path: Path) -> None:
    table = pa.table(
        {
            "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT", "BTCUSDT"],
            "freq": ["1d", "1d", "1d", "1d"],
            "ts": [
                datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 1, 23, tzinfo=timezone.utc),
                datetime(2024, 1, 2, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 2, 23, tzinfo=timezone.utc),
            ],
            "close": [50000.0, 50500.0, 51500.0, 52000.0],
        }
    )
    pq.write_table(table, path)


def _write_snapshots(root: Path) -> None:
    day1 = pa.table(
        {
            "snapshot_date": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            "group_key": ["g1", "g2"],
            "age_bucket": ["000-001d", "030-180d"],
            "balance_sats": [60_000_000, 40_000_000],
            "balance_btc": [0.6, 0.4],
            "cost_basis_usd": [24_000.0, 16_000.0],
            "market_value_usd": [30_300.0, 20_200.0],
        }
    )
    day2 = pa.table(
        {
            "snapshot_date": [datetime(2024, 1, 2), datetime(2024, 1, 2)],
            "group_key": ["g1", "g2"],
            "age_bucket": ["000-001d", "030-180d"],
            "balance_sats": [55_000_000, 45_000_000],
            "balance_btc": [0.55, 0.45],
            "cost_basis_usd": [23_100.0, 18_900.0],
            "market_value_usd": [28_600.0, 23_400.0],
        }
    )
    pq.write_table(day1, root / "2024-01-01.parquet")
    pq.write_table(day2, root / "2024-01-02.parquet")


def _write_spent(path: Path) -> None:
    table = pa.table(
        {
            "source_txid": ["aaa"],
            "source_vout": [0],
            "spend_txid": ["bbb"],
            "value_sats": [50_000_000],
            "created_time": [datetime(2023, 12, 30, tzinfo=timezone.utc)],
            "spend_time": [datetime(2024, 1, 2, 12, tzinfo=timezone.utc)],
            "holding_days": [2.0],
            "realized_value_usd": [26_000.0],
            "realized_profit_usd": [2_000.0],
            "spend_price_close": [52_000.0],
            "creation_price_close": [48_000.0],
        }
    )
    pq.write_table(table, path)


def _config(tmp_path: Path) -> MetricsConfig:
    price_root = tmp_path / "price"
    price_root.mkdir(parents=True, exist_ok=True)
    price_path = price_root / "price.parquet"
    _write_price(price_path)

    snapshot_root = tmp_path / "snapshots"
    snapshot_root.mkdir(parents=True, exist_ok=True)
    _write_snapshots(snapshot_root)

    lifecycle_root = tmp_path / "lifecycle"
    lifecycle_root.mkdir(parents=True, exist_ok=True)
    spent_path = lifecycle_root / "spent.parquet"
    _write_spent(spent_path)

    created_path = lifecycle_root / "created.parquet"
    empty_created = pa.table(
        {
            "txid": pa.array([], pa.string()),
        }
    )
    pq.write_table(empty_created, created_path)

    data_config = DataConfig(
        price_glob=str(price_root / "*.parquet"),
        lifecycle=LifecyclePaths(
            created=created_path,
            spent=spent_path,
            snapshots_glob=str(snapshot_root / "*.parquet"),
        ),
        output_root=tmp_path / "metrics-output",
        symbol="BTCUSDT",
        frequency="1d",
    )

    engine_config = EngineConfig(mvrv_window_days=2, dormancy_window_days=2, drawdown_window_days=2)
    golden = GoldenDay(
        date=date(2024, 1, 2),
        metrics={
            "price_close": 52000.0,
            "sopr": 26_000.0 / 24_000.0,
        },
        tolerance_pct=5.0,
    )
    qa_config = QAConfig(
        golden_days=[golden],
        max_drawdown_pct=95.0,
        min_price=0.0,
        lookahead_tolerance_days=0,
    )
    writer_config = WriterConfig(compression="zstd", compression_level=3)

    return MetricsConfig(data=data_config, engine=engine_config, qa=qa_config, writer=writer_config)


def test_build_daily_metrics_writes_parquet_and_passes_qa(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    result: BuildResult = build_daily_metrics(config=cfg)

    assert result.rows == 2
    assert result.qa_report.ok
    assert all(golden.passed for golden in result.qa_report.golden_days)
    assert result.output_path.exists()
    assert len(result.provenance.price_hash) == 12
    assert len(result.provenance.snapshot_hash) == 12
    assert len(result.provenance.spent_hash) == 12
    assert result.provenance.formulas_version.startswith("metrics-formulas@metrics.v1+")

    table = pq.read_table(result.output_path)
    assert table.num_rows == 2
    assert table.schema.metadata.get(b"schema_version") == METRICS_SCHEMA.metadata.get(b"schema_version")
    hodl_columns = {name for name in table.column_names if name.startswith("hodl_share_")}
    assert hodl_columns == set(result.hodl_columns)


def test_run_qa_checks_detects_lookahead(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg = cfg.model_copy(update={"qa": cfg.qa.model_copy(update={"lookahead_tolerance_days": 0})})

    frame = pd.DataFrame(
        {
            "date": [date(2024, 1, 3), date(2024, 1, 5)],
            "price_close": [100.0, 100.0],
            "market_value_usd": [1.0, 1.0],
            "realized_value_usd": [1.0, 1.0],
            "supply_cost_basis_usd": [1.0, 1.0],
            "realized_profit_usd": [0.0, 0.0],
            "realized_loss_usd": [0.0, 0.0],
            "realized_profit_loss_ratio": [float("nan"), float("nan")],
            "sopr": [1.0, 1.0],
            "asopr": [1.0, 1.0],
            "mvrv": [1.0, 1.0],
            "mvrv_zscore": [0.0, 0.0],
            "nupl": [0.0, 0.0],
            "cdd": [0.0, 0.0],
            "adjusted_cdd": [0.0, 0.0],
            "dormancy_flow": [0.0, 0.0],
            "utxo_profit_share": [0.0, 0.0],
            "drawdown_pct": [0.0, 0.0],
            "supply_btc": [1.0, 1.0],
            "supply_sats": [100_000_000.0, 100_000_000.0],
            "pipeline_version": [pipeline_version(), pipeline_version()],
            "lineage_id": ["lookahead", "lookahead"],
        }
    )
    result = MetricsComputationResult(frame=frame, hodl_columns=[], lineage_id="lookahead")
    price_df = pd.DataFrame(
        {
            "ts": [datetime(2024, 1, 3, tzinfo=timezone.utc)],
            "close": [100.0],
        }
    )

    with pytest.raises(MetricsQAError):
        run_qa_checks(result, price_df=price_df, config=cfg)


def test_run_qa_checks_flags_nan_golden_metric(tmp_path: Path) -> None:
    cfg = _config(tmp_path)

    price_root = tmp_path / "price"
    snapshot_root = tmp_path / "snapshots"
    lifecycle_root = tmp_path / "lifecycle"

    price_df = pq.read_table(price_root / "price.parquet").to_pandas()
    snapshot_tables = [pq.read_table(path) for path in snapshot_root.glob("*.parquet")]
    snapshot_df = pa.concat_tables(snapshot_tables).to_pandas()
    spent_df = pq.read_table(lifecycle_root / "spent.parquet").to_pandas()

    result = compute_metrics(price_df, snapshot_df, spent_df, cfg.engine)
    result.frame.loc[result.frame["date"] == date(2024, 1, 2), "sopr"] = float("nan")

    with pytest.raises(MetricsQAError):
        run_qa_checks(result, price_df=price_df, config=cfg)


def test_build_metrics_with_relative_config_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = tmp_path
    data_dir = project_root / "data"
    price_dir = data_dir / "price"
    snapshot_dir = data_dir / "snapshots"
    lifecycle_dir = data_dir / "lifecycle"
    output_dir = data_dir / "output"
    price_dir.mkdir(parents=True)
    snapshot_dir.mkdir(parents=True)
    lifecycle_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)

    _write_price(price_dir / "price.parquet")
    _write_snapshots(snapshot_dir)
    _write_spent(lifecycle_dir / "spent.parquet")

    created_path = lifecycle_dir / "created.parquet"
    empty_created = pa.table({"txid": pa.array([], pa.string())})
    pq.write_table(empty_created, created_path)

    config_dir = project_root / "config"
    config_dir.mkdir(parents=True)
    config_path = config_dir / "metrics.yaml"

    with config_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            {
                "data": {
                    "price_glob": "../data/price/*.parquet",
                    "lifecycle": {
                        "created": "../data/lifecycle/created.parquet",
                        "spent": "../data/lifecycle/spent.parquet",
                        "snapshots_glob": "../data/snapshots/*.parquet",
                    },
                    "output_root": "../data/output",
                    "symbol": "BTCUSDT",
                    "frequency": "1d",
                },
                "engine": {
                    "mvrv_window_days": 2,
                    "dormancy_window_days": 2,
                    "drawdown_window_days": 2,
                },
                "qa": {
                    "golden_days": [],
                    "max_drawdown_pct": 95.0,
                    "min_price": 0.0,
                    "lookahead_tolerance_days": 0,
                },
                "writer": {
                    "compression": "zstd",
                    "compression_level": 3,
                },
            },
            handle,
        )

    monkeypatch.chdir(project_root.parent)
    result = build_daily_metrics(config_path=config_path)
    assert result.output_path.exists()
    assert Path(result.output_path).is_absolute()


def test_build_metrics_updates_registry_with_provenance(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    registry_source = ROOT / "config" / "metrics_registry.yaml"
    registry_copy = tmp_path / "metrics_registry.yaml"
    registry_copy.write_text(registry_source.read_text(encoding="utf-8"), encoding="utf-8")

    result = build_daily_metrics(config=cfg, registry_path=registry_copy)

    loaded = yaml.safe_load(registry_copy.read_text(encoding="utf-8"))
    metadata = loaded.get("metadata", {})
    assert metadata.get("generator") == "onchain-metrics.build"
    assert metadata.get("generated_at", "").endswith("Z")

    sopr_badge = loaded["metrics"]["sopr"]["badge"]
    mvrv_badge = loaded["metrics"]["mvrv"]["badge"]

    assert sopr_badge["utxo_snapshot_commit"] == result.provenance.spent_commit
    assert mvrv_badge["utxo_snapshot_commit"] == result.provenance.snapshot_commit
    assert sopr_badge["price_root_commit"] == result.provenance.price_root_commit
    assert sopr_badge["formulas_version"] == result.provenance.formulas_version
