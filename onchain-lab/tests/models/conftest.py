from __future__ import annotations

from pathlib import Path
from typing import Iterator
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from models.config import (
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


@pytest.fixture
def sample_metrics_df() -> pd.DataFrame:
    dates = pd.date_range("2021-01-01", periods=32, freq="D")
    frame = pd.DataFrame(
        {
            "date": dates,
            "price_close": np.linspace(10000.0, 11000.0, len(dates)),
            "feature_a": np.linspace(0.0, 1.0, len(dates)),
            "feature_b": np.linspace(1.0, 2.0, len(dates)),
            "hodl_share_short": np.linspace(0.05, 0.15, len(dates)),
        }
    )
    return frame


@pytest.fixture
def model_config(tmp_path: Path, sample_metrics_df: pd.DataFrame) -> Iterator[ModelConfig]:
    metrics_path = tmp_path / "metrics.parquet"
    sample_metrics_df.to_parquet(metrics_path, index=False)
    out_root = tmp_path / "models"
    artifacts_root = tmp_path / "artifacts"
    registry_path = out_root / "registry.json"
    config = ModelConfig(
        data=DataConfig(
            metrics_parquet=metrics_path,
            out_root=out_root,
            artifacts_root=artifacts_root,
        ),
        target=TargetConfig(label_horizon_days=1, lookback_days=5, min_history_days=0),
        features=FeaturesConfig(
            include=["price_close", "feature_a", "feature_b"],
            hodl_pattern="hodl_share_*",
            transforms=FeatureTransformsConfig(scale="none", diffs=[], lags=[1, 2], clip_pct=0.0),
        ),
        splits=SplitConfig(
            scheme="forward_chaining",
            anchors=SplitAnchorsConfig(
                train_end="2021-01-15",
                val_end="2021-01-23",
                test_start="2021-01-24",
            ),
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
        qa=QAConfig(forbid_future_lookahead=True, min_oos_start="2021-01-10", tolerance_pct=0.5),
        registry=RegistryConfig(path=registry_path),
    )
    yield config