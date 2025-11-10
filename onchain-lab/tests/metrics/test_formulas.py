from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from metrics.config import EngineConfig  # type: ignore  # noqa: E402
from metrics.formulas import MetricsComputationResult, compute_metrics, pipeline_version  # type: ignore  # noqa: E402


def _price_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"ts": datetime(2024, 1, 1, 0, tzinfo=timezone.utc), "close": 50000.0},
            {"ts": datetime(2024, 1, 1, 23, tzinfo=timezone.utc), "close": 50500.0},
            {"ts": datetime(2024, 1, 2, 0, tzinfo=timezone.utc), "close": 51500.0},
            {"ts": datetime(2024, 1, 2, 23, tzinfo=timezone.utc), "close": 52000.0},
        ]
    )


def _snapshot_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "snapshot_date": datetime(2024, 1, 1),
                "group_key": "g1",
                "age_bucket": "000-001d",
                "balance_sats": 60_000_000,
                "balance_btc": 0.6,
                "cost_basis_usd": 24_000.0,
                "market_value_usd": 30_300.0,
            },
            {
                "snapshot_date": datetime(2024, 1, 1),
                "group_key": "g2",
                "age_bucket": "030-180d",
                "balance_sats": 40_000_000,
                "balance_btc": 0.4,
                "cost_basis_usd": 16_000.0,
                "market_value_usd": 20_200.0,
            },
            {
                "snapshot_date": datetime(2024, 1, 2),
                "group_key": "g1",
                "age_bucket": "000-001d",
                "balance_sats": 55_000_000,
                "balance_btc": 0.55,
                "cost_basis_usd": 23_100.0,
                "market_value_usd": 28_600.0,
            },
            {
                "snapshot_date": datetime(2024, 1, 2),
                "group_key": "g2",
                "age_bucket": "030-180d",
                "balance_sats": 45_000_000,
                "balance_btc": 0.45,
                "cost_basis_usd": 18_900.0,
                "market_value_usd": 23_400.0,
            },
        ]
    )


def _spent_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "source_txid": "aaa",
                "source_vout": 0,
                "spend_txid": "bbb",
                "value_sats": 50_000_000,
                "created_time": datetime(2023, 12, 30, tzinfo=timezone.utc),
                "spend_time": datetime(2024, 1, 2, 12, tzinfo=timezone.utc),
                "holding_days": 2.0,
                "realized_value_usd": 26_000.0,
                "realized_profit_usd": 2_000.0,
                "spend_price_close": 52_000.0,
                "creation_price_close": 48_000.0,
            }
        ]
    )


def test_compute_metrics_generates_expected_columns() -> None:
    engine = EngineConfig(mvrv_window_days=2, dormancy_window_days=2, drawdown_window_days=2)
    result: MetricsComputationResult = compute_metrics(
        _price_frame(), _snapshot_frame(), _spent_frame(), engine
    )

    assert len(result.hodl_columns) == 2
    assert set(result.hodl_columns) == {"hodl_share_000_001d", "hodl_share_030_180d"}
    assert len(result.lineage_id) == 16

    frame = result.frame
    assert set(result.hodl_columns).issubset(frame.columns)
    assert frame["pipeline_version"].unique().tolist() == [pipeline_version()]

    day1 = frame.iloc[0]
    shares_sum = day1[result.hodl_columns].sum()
    assert pytest.approx(shares_sum, rel=1e-6) == 1.0
    assert pytest.approx(day1["hodl_share_000_001d"], rel=1e-6) == 0.6

    day2 = frame.iloc[1]
    expected_sopr = 26_000.0 / 24_000.0
    assert pytest.approx(day2["sopr"], rel=1e-6) == expected_sopr
    assert pytest.approx(day2["asopr"], rel=1e-6) == expected_sopr
    assert np.isnan(day2["realized_profit_loss_ratio"])

    expected_mvrv = (28_600.0 + 23_400.0) / (23_100.0 + 18_900.0)
    assert pytest.approx(day2["mvrv"], rel=1e-6) == expected_mvrv

    assert frame["drawdown_pct"].min() <= 0.0


def test_supply_cost_basis_forward_fill_without_realized_backfill() -> None:
    engine = EngineConfig(mvrv_window_days=2, dormancy_window_days=2, drawdown_window_days=2)
    snapshots = _snapshot_frame().iloc[:2].copy()
    result = compute_metrics(_price_frame(), snapshots, _spent_frame(), engine)

    frame = result.frame
    day1 = frame.loc[frame["date"] == date(2024, 1, 1)].iloc[0]
    day2 = frame.loc[frame["date"] == date(2024, 1, 2)].iloc[0]

    assert pytest.approx(day2["supply_cost_basis_usd"], rel=1e-9) == day1["supply_cost_basis_usd"]
    assert abs(day2["supply_cost_basis_usd"] - day2["realized_value_usd"]) > 1e-6
    assert day1["adjusted_cdd"] == 0.0