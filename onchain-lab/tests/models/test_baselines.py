from __future__ import annotations

import json
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from models.baselines_core import train_model
from models.frame import (
    DATE_COLUMN,
    FORWARD_RETURN_COLUMN,
    LABEL_COLUMN,
    SPLIT_COLUMN,
    FrameBuilder,
)
from models.utils import feature_hash


def _ensure_mixed_classes(bundle) -> None:
    train_idx = bundle.tabular.index[bundle.tabular[SPLIT_COLUMN] == "train"]
    if len(train_idx) == 0:
        raise AssertionError("expected non-empty training split")
    first = train_idx[0]
    bundle.tabular.loc[first, LABEL_COLUMN] = 0.0
    bundle.tabular.loc[first, FORWARD_RETURN_COLUMN] = -abs(
        float(bundle.tabular.loc[first, FORWARD_RETURN_COLUMN])
    )


def test_train_model_persists_artifacts(model_config) -> None:
    builder = FrameBuilder(model_config)
    bundle = builder.build()
    _ensure_mixed_classes(bundle)

    result = train_model("logreg", model_config, bundle)

    assert result.model == "logreg"
    assert result.model_path.exists()
    assert result.predictions_path.exists()
    assert result.signals_path.exists()
    assert result.baseline_signals_path.exists()
    assert result.metadata_path is not None and result.metadata_path.exists()

    predictions_disk = pd.read_parquet(result.predictions_path)
    assert {"prob_up", "decision"}.issubset(predictions_disk.columns)
    assert predictions_disk["prob_up"].between(0.0, 1.0).all()

    oos_rows = predictions_disk[predictions_disk[SPLIT_COLUMN] != "train"]
    signals_disk = pd.read_parquet(result.signals_path)
    assert len(signals_disk) == len(oos_rows)
    assert (signals_disk["model"] == "logreg").all()

    baseline_signals = pd.read_parquet(result.baseline_signals_path)
    assert len(baseline_signals) == len(oos_rows)
    assert set(baseline_signals.columns) == {
        DATE_COLUMN,
        SPLIT_COLUMN,
        "model",
        "prob_up",
        "decision",
        "threshold",
        "featureset_hash",
    }

    metadata = json.loads(result.metadata_path.read_text())
    assert metadata["model"] == "logreg"
    assert metadata["feature_hash"] == result.feature_hash
    assert metadata["feature_count"] == len(result.features)
    assert metadata["feature_source"] == "all"
    assert metadata["artifacts"]["baseline_signals"] == str(result.baseline_signals_path)


def test_train_model_with_selected_features_records_boruta_source(model_config) -> None:
    builder = FrameBuilder(model_config)
    bundle = builder.build()
    _ensure_mixed_classes(bundle)

    selected = bundle.feature_columns[:2]
    result = train_model("logreg", model_config, bundle, selected_features=selected)

    assert result.features == list(selected)
    assert result.feature_hash == feature_hash(selected)
    assert result.metadata_path is not None and result.metadata_path.exists()

    metadata = json.loads(result.metadata_path.read_text())
    assert metadata["feature_source"] == "boruta"
    assert metadata["feature_hash"] == feature_hash(selected)
    assert metadata["feature_count"] == len(selected)

    baseline_signals = pd.read_parquet(result.baseline_signals_path)
    assert (baseline_signals["featureset_hash"] == feature_hash(selected)).all()