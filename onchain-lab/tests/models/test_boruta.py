from __future__ import annotations

import json
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd
import pytest
from sklearn.preprocessing import StandardScaler

from models.boruta import BORUTA_FILE, BorutaRunner
from models.config import BorutaConfig, ModelConfig
from models.frame import (
    DATE_COLUMN,
    FORWARD_RETURN_COLUMN,
    LABEL_COLUMN,
    PRICE_COLUMN,
    SPLIT_COLUMN,
    FrameBundle,
)


def _make_frame(feature_names: Sequence[str]) -> FrameBundle:
    dates = pd.date_range("2021-01-01", periods=4, freq="D")
    tabular = pd.DataFrame(
        {
            DATE_COLUMN: dates,
            SPLIT_COLUMN: ["train", "train", "val", "test"],
            LABEL_COLUMN: [0.0, 1.0, 0.0, 1.0],
            FORWARD_RETURN_COLUMN: [0.0, 0.01, -0.01, 0.02],
            PRICE_COLUMN: [100.0, 101.0, 102.0, 103.0],
        }
    )
    for offset, name in enumerate(feature_names):
        tabular[name] = np.linspace(offset, offset + 1.0, len(dates))
    sequence_tensor = np.zeros((len(dates), 2, len(feature_names)))
    scaler = StandardScaler()
    scaler.fit(np.zeros((1, len(feature_names))))
    return FrameBundle(
        tabular=tabular,
        feature_columns=list(feature_names),
        sequence_tensor=sequence_tensor,
        sequence_features=list(feature_names),
        lookback=2,
        scaler=scaler,
        clip_bounds={name: (0.0, 1.0) for name in feature_names},
    )


def test_boruta_runner_persists_selected_features(monkeypatch: pytest.MonkeyPatch, model_config: ModelConfig) -> None:
    config = model_config.model_copy(update={"boruta": BorutaConfig(enabled=True)})
    frame = _make_frame(["feature_a", "feature_b", "feature_c"])

    class DummyBoruta:
        def __init__(self, *args, **kwargs) -> None:
            self.support_ = None
            self.ranking_ = None

        def fit(self, x, y) -> None:  # noqa: ANN001
            self.support_ = np.array([True, False, True])
            self.ranking_ = np.array([1, 3, 1])

    monkeypatch.setattr("models.boruta.BorutaPy", DummyBoruta)

    runner = BorutaRunner(config, frame)
    result = runner.run()
    assert result.selected_features == ["feature_a", "feature_c"]

    output_path = config.data.out_root / BORUTA_FILE
    assert output_path.exists()
    payload = json.loads(output_path.read_text())
    assert payload["selected_features"] == result.selected_features


def test_boruta_runner_falls_back_to_top_rank(monkeypatch: pytest.MonkeyPatch, model_config: ModelConfig) -> None:
    config = model_config.model_copy(update={"boruta": BorutaConfig(enabled=True)})
    frame = _make_frame(["feature_x", "feature_y", "feature_z"])

    class RejectingBoruta:
        def __init__(self, *args, **kwargs) -> None:
            self.support_ = None
            self.ranking_ = None

        def fit(self, x, y) -> None:  # noqa: ANN001
            self.support_ = np.array([False, False, False])
            self.ranking_ = np.array([2, 1, 3])

    monkeypatch.setattr("models.boruta.BorutaPy", RejectingBoruta)

    runner = BorutaRunner(config, frame)
    result = runner.run()
    assert result.selected_features == ["feature_y"]
