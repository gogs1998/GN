from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple, cast

import numpy as np
import pandas as pd
from joblib import dump, load
from sklearn.preprocessing import StandardScaler
import sklearn

if TYPE_CHECKING:
    from numpy.typing import NDArray
else:  # pragma: no cover - typing fallback
    NDArray = np.ndarray  # type: ignore[attr-defined]

from .config import ModelConfig
from .utils import (
    ensure_directory,
    feature_hash,
    resolve_feature_list,
    save_json,
    save_numpy,
)

LABEL_COLUMN = "label"
SPLIT_COLUMN = "split"
FORWARD_RETURN_COLUMN = "forward_return"
DATE_COLUMN = "date"
PRICE_COLUMN = "price_close"

TABULAR_FILE = "frame/tabular.parquet"
SEQUENCE_FILE = "frame/sequence.npy"
METADATA_FILE = "frame/metadata.json"
SCALER_FILE = "frame/scaler.joblib"
CLIP_FILE = "frame/clip_bounds.json"


class FrameArtifactError(RuntimeError):
    """Raised when persisted frame artifacts need regeneration."""


@dataclass
class FrameBundle:
    tabular: pd.DataFrame
    feature_columns: List[str]
    sequence_tensor: NDArray[Any]
    sequence_features: List[str]
    lookback: int
    scaler: StandardScaler
    clip_bounds: Dict[str, Tuple[float, float]]

    def features(
        self, split: str, columns: Optional[Sequence[str]] = None
    ) -> Tuple[NDArray[Any], NDArray[Any]]:
        feats = list(columns) if columns is not None else self.feature_columns
        mask = self.tabular[SPLIT_COLUMN] == split
        x = cast(NDArray[Any], self.tabular.loc[mask, feats].to_numpy(dtype=float))
        y = cast(NDArray[Any], self.tabular.loc[mask, LABEL_COLUMN].to_numpy(dtype=float))
        return x, y

    def sequences(self, split: str, columns: Optional[Sequence[str]] = None) -> NDArray[Any]:
        feats = list(columns) if columns is not None else self.sequence_features
        indices = [self.sequence_features.index(name) for name in feats]
        mask = self.tabular[SPLIT_COLUMN].to_numpy() == split
        return cast(NDArray[Any], self.sequence_tensor[mask][:, :, indices])

    def forward_returns(self, split: str) -> NDArray[Any]:
        mask = self.tabular[SPLIT_COLUMN] == split
        return cast(
            NDArray[Any], self.tabular.loc[mask, FORWARD_RETURN_COLUMN].to_numpy(dtype=float)
        )

    def dates(self, split: Optional[str] = None) -> pd.Series:
        if split is None:
            return self.tabular[DATE_COLUMN]
        mask = self.tabular[SPLIT_COLUMN] == split
        return self.tabular.loc[mask, DATE_COLUMN]


class FrameBuilder:
    def __init__(self, config: ModelConfig):
        self.config = config

    def build(self, *, start: Optional[str] = None, end: Optional[str] = None) -> FrameBundle:
        raw = self._load_metrics(start=start, end=end)
        base_features = resolve_feature_list(
            raw.columns,
            self.config.features.include,
            self.config.features.hodl_pattern,
        )
        enriched = self._attach_targets(raw)
        enriched = self._apply_transforms(enriched, base_features)
        trimmed = self._trim_history(enriched)
        trimmed = self._apply_splits(trimmed)
        working, scaler, clip_bounds = self._clip_and_scale(trimmed)
        tensor, keep_indices = self._build_sequence_tensor(working, base_features)
        tabular = working.iloc[keep_indices].reset_index(drop=True)
        numeric_tabular = tabular.select_dtypes(include=["number"])
        feature_columns = [
            col
            for col in numeric_tabular.columns
            if col
            not in {
                DATE_COLUMN,
                FORWARD_RETURN_COLUMN,
                LABEL_COLUMN,
                SPLIT_COLUMN,
            }
        ]
        sequence_features = [name for name in base_features if name in working.columns]
        bundle = FrameBundle(
            tabular=tabular,
            feature_columns=feature_columns,
            sequence_tensor=tensor,
            sequence_features=sequence_features,
            lookback=self.config.target.lookback_days,
            scaler=scaler,
            clip_bounds=clip_bounds,
        )
        self._validate_splits(bundle)
        return bundle

    def _validate_splits(self, bundle: FrameBundle) -> None:
        counts = bundle.tabular[SPLIT_COLUMN].value_counts()
        for split in ("train", "val", "test"):
            if counts.get(split, 0) == 0:
                raise ValueError(f"split '{split}' has no rows after frame construction")
        if bundle.sequence_tensor.shape[0] != len(bundle.tabular):
            raise ValueError("sequence tensor rows do not align with tabular frame")
        if bundle.tabular[DATE_COLUMN].isna().any():
            raise ValueError("frame contains null dates")

    def persist(self, bundle: FrameBundle) -> None:
        out_root = self.config.data.out_root
        ensure_directory(out_root / "frame")
        bundle.tabular.to_parquet(out_root / TABULAR_FILE)
        save_numpy(out_root / SEQUENCE_FILE, bundle.sequence_tensor)
        metadata = {
            "feature_columns": bundle.feature_columns,
            "sequence_features": bundle.sequence_features,
            "lookback": bundle.lookback,
            "label_column": LABEL_COLUMN,
            "split_column": SPLIT_COLUMN,
            "forward_return_column": FORWARD_RETURN_COLUMN,
            "feature_hash": feature_hash(bundle.feature_columns),
            "rows": len(bundle.tabular),
            "start_date": str(bundle.tabular[DATE_COLUMN].min().date()),
            "end_date": str(bundle.tabular[DATE_COLUMN].max().date()),
            "sklearn_version": sklearn.__version__,
        }
        save_json(out_root / METADATA_FILE, metadata)
        save_json(
            out_root / CLIP_FILE,
            {key: [float(low), float(high)] for key, (low, high) in bundle.clip_bounds.items()},
        )
        dump(bundle.scaler, out_root / SCALER_FILE)

    def load(self) -> FrameBundle:
        out_root = self.config.data.out_root
        tabular_path = out_root / TABULAR_FILE
        if not tabular_path.exists():
            raise FileNotFoundError("tabular frame not built; run 'onchain-models build-frame' first")
        tabular = pd.read_parquet(tabular_path)
        metadata = json.loads((out_root / METADATA_FILE).read_text())
        recorded_version = metadata.get("sklearn_version")
        current_version = sklearn.__version__
        if recorded_version != current_version:
            raise FrameArtifactError(
                "frame artifacts built with scikit-learn "
                f"{recorded_version or 'unknown'}; current runtime {current_version}"
            )
        sequence_tensor = np.load(out_root / SEQUENCE_FILE, allow_pickle=False)
        scaler = load(out_root / SCALER_FILE)
        clip_bounds_raw = json.loads((out_root / CLIP_FILE).read_text())
        clip_bounds = {key: (float(values[0]), float(values[1])) for key, values in clip_bounds_raw.items()}
        return FrameBundle(
            tabular=tabular,
            feature_columns=list(metadata["feature_columns"]),
            sequence_tensor=sequence_tensor,
            sequence_features=list(metadata["sequence_features"]),
            lookback=int(metadata["lookback"]),
            scaler=scaler,
            clip_bounds=clip_bounds,
        )

    def _load_metrics(self, *, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
        path = self.config.data.metrics_parquet
        if not path.exists():
            raise FileNotFoundError(f"metrics dataset missing at {path}")
        df = pd.read_parquet(path)
        if DATE_COLUMN not in df.columns:
            raise ValueError("metrics dataset missing 'date' column")
        df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN])
        df = df.sort_values(DATE_COLUMN)
        if start:
            df = df[df[DATE_COLUMN] >= pd.to_datetime(start)]
        if end:
            df = df[df[DATE_COLUMN] <= pd.to_datetime(end)]
        df = df.reset_index(drop=True)
        if len(df) < self.config.target.min_history_days:
            raise ValueError("insufficient history to build frame")
        return df

    def _attach_targets(self, df: pd.DataFrame) -> pd.DataFrame:
        horizon = self.config.target.label_horizon_days
        future_price = df[PRICE_COLUMN].shift(-horizon)
        df[FORWARD_RETURN_COLUMN] = future_price / df[PRICE_COLUMN] - 1.0
        df[LABEL_COLUMN] = (df[FORWARD_RETURN_COLUMN] > 0).astype(float)
        return df

    def _apply_transforms(self, df: pd.DataFrame, features: Sequence[str]) -> pd.DataFrame:
        transforms = self.config.features.transforms
        result = df.copy()
        for feature in features:
            if transforms.diffs and feature in transforms.diffs:
                result[f"{feature}_diff1"] = result[feature].diff()
            for lag in transforms.lags:
                result[f"{feature}_lag{lag}"] = result[feature].shift(lag)
        return result

    def _trim_history(self, df: pd.DataFrame) -> pd.DataFrame:
        transforms = self.config.features.transforms
        max_lag = max(transforms.lags) if transforms.lags else 0
        warmup = max(self.config.target.lookback_days, max_lag, self.config.target.min_history_days)
        trimmed = df.iloc[warmup : len(df) - self.config.target.label_horizon_days]
        if trimmed.empty:
            raise ValueError("not enough rows after applying lookback and horizon constraints")
        return trimmed

    def _apply_splits(self, df: pd.DataFrame) -> pd.DataFrame:
        anchors = self.config.splits.anchors
        train_end = pd.to_datetime(anchors.train_end) if anchors.train_end else None
        val_end = pd.to_datetime(anchors.val_end) if anchors.val_end else None
        test_start = pd.to_datetime(anchors.test_start) if anchors.test_start else None
        splits: List[str] = []
        for ts in df[DATE_COLUMN]:
            if train_end and ts <= train_end:
                splits.append("train")
            elif val_end and ts <= val_end:
                splits.append("val")
            else:
                if test_start and ts < test_start:
                    splits.append("val")
                else:
                    splits.append("test")
        df = df.copy()
        df[SPLIT_COLUMN] = splits
        for split in {"train", "val", "test"}:
            if split not in df[SPLIT_COLUMN].values:
                raise ValueError(f"split '{split}' has no rows; adjust anchors")
        return df

    def _clip_and_scale(
        self,
        df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, StandardScaler, Dict[str, Tuple[float, float]]]:
        transforms = self.config.features.transforms
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        feature_cols = [
            col
            for col in numeric_cols
            if col
            not in {
                DATE_COLUMN,
                FORWARD_RETURN_COLUMN,
                LABEL_COLUMN,
                SPLIT_COLUMN,
            }
        ]
        working = df.copy()
        train_mask = working[SPLIT_COLUMN] == "train"
        if feature_cols:
            drop_cols = [col for col in feature_cols if working.loc[train_mask, col].isna().all()]
            if drop_cols:
                working = working.drop(columns=drop_cols)
                feature_cols = [col for col in feature_cols if col not in drop_cols]
                numeric_cols = [col for col in numeric_cols if col not in drop_cols]
                train_mask = working[SPLIT_COLUMN] == "train"
            if feature_cols:
                valid_row_mask = working[feature_cols].notna().all(axis=1)
                working = working.loc[valid_row_mask].copy()
                train_mask = working[SPLIT_COLUMN] == "train"
        clip_bounds: Dict[str, Tuple[float, float]] = {}
        if transforms.clip_pct > 0 and feature_cols:
            q_low = working.loc[train_mask, feature_cols].quantile(transforms.clip_pct)
            q_high = working.loc[train_mask, feature_cols].quantile(1 - transforms.clip_pct)
            for col in feature_cols:
                clip_bounds[col] = (float(q_low[col]), float(q_high[col]))
            working[feature_cols] = working[feature_cols].clip(q_low, q_high, axis=1)
        if transforms.scale == "standard" and feature_cols:
            scaler = StandardScaler()
            scaler.fit(working.loc[train_mask, feature_cols].to_numpy(dtype=float))
            working[feature_cols] = scaler.transform(working[feature_cols].to_numpy(dtype=float))
        else:
            scaler = StandardScaler(with_mean=False, with_std=False)
            scaler.fit(np.zeros((1, max(len(feature_cols), 1))))
        return working, scaler, clip_bounds

    def _build_sequence_tensor(
        self,
        df: pd.DataFrame,
        base_features: Sequence[str],
    ) -> Tuple[NDArray[Any], List[int]]:
        lookback = self.config.target.lookback_days
        indices: List[int] = []
        windows: List[NDArray[Any]] = []
        sequence_features = [name for name in base_features if name in df.columns]
        values = df[sequence_features].to_numpy(dtype=float)
        labels = df[LABEL_COLUMN].to_numpy()
        for idx in range(len(df)):
            if np.isnan(labels[idx]):
                continue
            start = idx - lookback + 1
            if start < 0:
                continue
            window = values[start : idx + 1]
            if window.shape[0] != lookback:
                continue
            windows.append(window)
            indices.append(idx)
        if not windows:
            raise ValueError("unable to build sequence tensor; check lookback configuration")
        tensor = np.stack(windows, axis=0)
        return tensor, indices


def load_frame_bundle(config: ModelConfig) -> FrameBundle:
    builder = FrameBuilder(config)
    return builder.load()


__all__ = [
    "FrameBundle",
    "FrameBuilder",
    "FrameArtifactError",
    "load_frame_bundle",
]
