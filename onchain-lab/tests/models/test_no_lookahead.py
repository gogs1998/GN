from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from models.frame import DATE_COLUMN, FORWARD_RETURN_COLUMN, FrameBuilder


def test_frame_builder_prevents_future_leakage(model_config, sample_metrics_df: pd.DataFrame) -> None:
    builder = FrameBuilder(model_config)
    bundle = builder.build()

    raw = sample_metrics_df.sort_values("date").reset_index(drop=True).set_index("date")

    # Final day from metrics should not appear because label requires future price
    assert raw.index.max() not in set(bundle.tabular[DATE_COLUMN])

    # Forward returns must match direct computation from raw prices
    horizon = model_config.target.label_horizon_days
    computed = []
    for ts in bundle.tabular[DATE_COLUMN]:
        current = raw.loc[ts, "price_close"]
        future = raw.loc[ts + pd.Timedelta(days=horizon), "price_close"]
        computed.append(future / current - 1.0)
    assert np.allclose(computed, bundle.tabular[FORWARD_RETURN_COLUMN])

    # Sequence tensors must end with same-day feature values (no lookahead)
    feature_idx = bundle.sequence_features.index("feature_a")
    for row_idx, ts in enumerate(bundle.tabular[DATE_COLUMN]):
        expected_value = raw.loc[ts, "feature_a"]
        actual_value = bundle.sequence_tensor[row_idx, -1, feature_idx]
        assert actual_value == pytest.approx(expected_value)
