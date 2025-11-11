from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from models.frame import (
    DATE_COLUMN,
    LABEL_COLUMN,
    SPLIT_COLUMN,
    TABULAR_FILE,
    FrameBuilder,
)


def test_frame_builder_constructs_sequences(model_config, sample_metrics_df: pd.DataFrame) -> None:
    builder = FrameBuilder(model_config)
    bundle = builder.build()

    assert bundle.sequence_tensor.shape[1] == model_config.target.lookback_days
    assert bundle.sequence_tensor.shape[2] == len(bundle.sequence_features)

    raw = sample_metrics_df.sort_values("date").reset_index(drop=True)
    warmup = max(
        model_config.target.lookback_days,
        max(model_config.features.transforms.lags or [0]),
        model_config.target.min_history_days,
    )
    first_pos = warmup + model_config.target.lookback_days - 1
    first_expected = raw.loc[first_pos, "date"]
    assert bundle.tabular.loc[0, DATE_COLUMN] == first_expected
    last_expected = raw.loc[len(raw) - model_config.target.label_horizon_days - 1, "date"]
    assert bundle.tabular[DATE_COLUMN].iloc[-1] == last_expected

    lag_column = "feature_a_lag1"
    prev_date = first_expected - pd.Timedelta(days=1)
    expected_lag = raw.set_index("date").loc[prev_date, "feature_a"]
    assert bundle.tabular.loc[0, lag_column] == pytest.approx(expected_lag)

    train_mask = bundle.tabular[SPLIT_COLUMN] == "train"
    train_labels = bundle.tabular.loc[train_mask, LABEL_COLUMN]
    assert set(train_labels.unique()) <= {0.0, 1.0}

    builder.persist(bundle)
    tabular_path = model_config.data.out_root / TABULAR_FILE
    assert tabular_path.exists()