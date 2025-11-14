from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from models.backtest import run_backtest  # type: ignore[import]
from models.config import (  # type: ignore[import]
    BorutaConfig,
    CNNLSTMConfig,
    CostsConfig,
    DataConfig,
    DecisionConfig,
    FeatureTransformsConfig,
    FeaturesConfig,
    LogRegConfig,
    ModelConfig,
    ModelsConfig,
    QAConfig,
    RegistryConfig,
    SizingConfig,
    SplitAnchorsConfig,
    SplitConfig,
    TargetConfig,
    XGBoostConfig,
)


def _make_config() -> ModelConfig:
    return ModelConfig(
        data=DataConfig(
            metrics_parquet=Path("metrics.parquet"),
            out_root=Path("artifacts"),
            artifacts_root=Path("artifacts_cache"),
        ),
        target=TargetConfig(label_horizon_days=1, lookback_days=2, min_history_days=0),
        features=FeaturesConfig(
            include=["feature_a"],
            transforms=FeatureTransformsConfig(scale="none", diffs=[], lags=[]),
        ),
        splits=SplitConfig(
            scheme="forward_chaining",
            anchors=SplitAnchorsConfig(train_end="2021-01-01", val_end="2021-01-03", test_start="2021-01-04"),
            n_splits=1,
            seed=42,
        ),
        boruta=BorutaConfig(enabled=False),
        models=ModelsConfig(
            enabled=["logreg"],
            logreg=LogRegConfig(),
            xgboost=XGBoostConfig(),
            cnn_lstm=CNNLSTMConfig(),
        ),
        decision=DecisionConfig(prob_threshold=0.6, side="long_flat"),
        costs=CostsConfig(fee_bps=0.0, slippage_bps=0.0, execution_timing="next_close"),
        sizing=SizingConfig(mode="fixed", fixed_weight=1.0, kelly_cap=0.25),
        qa=QAConfig(forbid_future_lookahead=True, min_oos_start="2021-01-02", tolerance_pct=0.5),
        registry=RegistryConfig(path=Path("registry.json")),
    )


def _make_predictions() -> pd.DataFrame:
    dates = pd.date_range("2021-01-01", periods=5, freq="D")
    data = pd.DataFrame(
        {
            "date": dates,
            "split": ["train", "val", "val", "test", "test"],
            "forward_return": [0.0, 0.01, -0.005, 0.02, -0.01],
            "prob_up": [0.7, 0.8, 0.4, 0.75, 0.2],
        }
    )
    data["label"] = (data["forward_return"] > 0).astype(int)
    return data


def test_backtest_shifts_signals_without_lookahead() -> None:
    config = _make_config()
    result = run_backtest("logreg", _make_predictions(), config)
    equity = result.equity
    assert float(equity.loc[1, "position"]) == pytest.approx(0.0)
    assert float(equity.loc[2, "position"]) == pytest.approx(equity.loc[1, "signal"])
    assert equity.loc[0, "position"] == 0.0


def test_backtest_enforces_min_oos_start() -> None:
    config = _make_config()
    updated = config.model_copy(
        update={"qa": QAConfig(forbid_future_lookahead=True, min_oos_start="2021-01-04", tolerance_pct=0.5)}
    )
    with pytest.raises(ValueError):
        run_backtest("logreg", _make_predictions(), updated)


def test_backtest_uses_model_specific_threshold() -> None:
    base = _make_config()
    overrides = base.decision.model_copy(
        update={"prob_threshold": 0.9, "model_thresholds": {"logreg": 0.6}}
    )
    config = base.model_copy(update={"decision": overrides})
    result = run_backtest("logreg", _make_predictions(), config)
    assert result.equity["signal"].abs().sum() > 0
