from __future__ import annotations

from dataclasses import dataclass
import inspect
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import joblib
import numpy as np
import pandas as pd
import torch
from sklearn.linear_model import LogisticRegression
from torch import Tensor, nn
from torch.utils.data import DataLoader, TensorDataset
from xgboost import XGBClassifier, callback

from .config import CNNLSTMConfig, ModelConfig
from .frame import (
    DATE_COLUMN,
    FORWARD_RETURN_COLUMN,
    LABEL_COLUMN,
    PRICE_COLUMN,
    SPLIT_COLUMN,
    FrameBundle,
)
from .utils import (
    compute_trade_signal,
    ensure_directory,
    feature_hash,
    load_json,
    save_json,
    set_random_seed,
)

MODEL_FILENAME: Dict[str, str] = {
    "logreg": "model.joblib",
    "xgboost": "model.xgb.joblib",
    "cnn_lstm": "model.pt",
}

PREDICTIONS_FILE = "predictions.parquet"
METADATA_FILE = "run_metadata.json"
SIGNALS_FILE = "signals.parquet"
BASELINE_SIGNALS_FILE = "baseline_signals.parquet"


@dataclass
class ModelRunResult:
    model: str
    features: List[str]
    feature_hash: str
    predictions: pd.DataFrame
    model_path: Path
    predictions_path: Path
    signals_path: Path
    baseline_signals_path: Path
    metadata_path: Optional[Path] = None


def train_model(
    model_name: str,
    config: ModelConfig,
    frame: FrameBundle,
    *,
    selected_features: Optional[Sequence[str]] = None,
) -> ModelRunResult:
    if model_name not in MODEL_FILENAME:
        raise ValueError(f"unsupported model '{model_name}'")
    set_random_seed(config.splits.seed)
    feature_list = list(selected_features) if selected_features else list(frame.feature_columns)
    if not feature_list:
        raise ValueError("no features provided for training")
    data_dir = config.data.out_root / model_name
    artifacts_dir = config.data.artifacts_root / model_name
    ensure_directory(data_dir)
    ensure_directory(artifacts_dir)
    if model_name == "logreg":
        run = _train_logistic(config, frame, feature_list, data_dir, artifacts_dir)
    elif model_name == "xgboost":
        run = _train_xgboost(config, frame, feature_list, data_dir, artifacts_dir)
    else:
        run = _train_cnn_lstm(config, frame, feature_list, data_dir, artifacts_dir)
    feature_source = "boruta" if selected_features is not None else "all"
    run.metadata_path = _persist_run_metadata(config, frame, run, feature_source)
    return run


def load_predictions(model_name: str, config: ModelConfig) -> pd.DataFrame:
    path = config.data.out_root / model_name / PREDICTIONS_FILE
    if not path.exists():
        raise FileNotFoundError(f"predictions missing for model '{model_name}' at {path}")
    df = pd.read_parquet(path)
    if DATE_COLUMN in df.columns:
        df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN])
    return df


def load_run_metadata(model_name: str, config: ModelConfig) -> Dict[str, object]:
    path = config.data.artifacts_root / model_name / METADATA_FILE
    if not path.exists():
        raise FileNotFoundError(f"metadata missing for model '{model_name}' at {path}")
    return load_json(path)


def _train_logistic(
    config: ModelConfig,
    frame: FrameBundle,
    features: Sequence[str],
    data_dir: Path,
    artifacts_dir: Path,
) -> ModelRunResult:
    params = config.models.logreg
    model = LogisticRegression(
        C=params.C,
        penalty=params.penalty,
        max_iter=params.max_iter,
        class_weight=params.class_weight,
        solver="lbfgs",
    )
    x_train, y_train = frame.features("train", features)
    if x_train.size == 0:
        raise ValueError("training split empty; cannot fit logistic regression")
    model.fit(x_train, y_train)
    probs = _predict_probabilities(model, frame, features)
    predictions = _build_predictions_dataframe(frame, probs)
    model_path = artifacts_dir / MODEL_FILENAME["logreg"]
    joblib.dump(model, model_path)
    feature_hash_value = feature_hash(features)
    (
        predictions,
        predictions_path,
        signals_path,
        baseline_signals_path,
    ) = _persist_predictions("logreg", config, feature_hash_value, predictions, data_dir)
    return ModelRunResult(
        model="logreg",
        features=list(features),
        feature_hash=feature_hash_value,
        predictions=predictions,
        model_path=model_path,
        predictions_path=predictions_path,
        signals_path=signals_path,
        baseline_signals_path=baseline_signals_path,
    )


def _train_xgboost(
    config: ModelConfig,
    frame: FrameBundle,
    features: Sequence[str],
    data_dir: Path,
    artifacts_dir: Path,
) -> ModelRunResult:
    params = config.models.xgboost
    model = XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        n_estimators=params.n_estimators,
        learning_rate=params.learning_rate,
        max_depth=params.max_depth,
        subsample=params.subsample,
        colsample_bytree=params.colsample_bytree,
        reg_lambda=params.reg_lambda,
        tree_method="hist",
        random_state=config.splits.seed,
        n_jobs=-1,
    )
    x_train, y_train = frame.features("train", features)
    if x_train.size == 0:
        raise ValueError("training split empty; cannot fit xgboost classifier")
    x_val, y_val = frame.features("val", features)
    eval_set: List[Tuple[np.ndarray, np.ndarray]] = []
    if len(y_val):
        eval_set.append((x_val, y_val))
    fit_kwargs: Dict[str, object] = {"verbose": False}
    if eval_set:
        fit_kwargs["eval_set"] = eval_set
        if params.early_stopping_rounds:
            fit_signature = inspect.signature(model.fit)
            if "callbacks" in fit_signature.parameters:
                fit_kwargs["callbacks"] = [
                    callback.EarlyStopping(
                        rounds=params.early_stopping_rounds,
                        metric_name="logloss",
                        save_best=True,
                        maximize=False,
                    )
                ]
            elif "early_stopping_rounds" in fit_signature.parameters:
                fit_kwargs["early_stopping_rounds"] = params.early_stopping_rounds
    model.fit(
        x_train,
        y_train,
        **fit_kwargs,
    )
    probs = _predict_probabilities(model, frame, features)
    predictions = _build_predictions_dataframe(frame, probs)
    model_path = artifacts_dir / MODEL_FILENAME["xgboost"]
    joblib.dump(model, model_path)
    feature_hash_value = feature_hash(features)
    (
        predictions,
        predictions_path,
        signals_path,
        baseline_signals_path,
    ) = _persist_predictions("xgboost", config, feature_hash_value, predictions, data_dir)
    return ModelRunResult(
        model="xgboost",
        features=list(features),
        feature_hash=feature_hash_value,
        predictions=predictions,
        model_path=model_path,
        predictions_path=predictions_path,
        signals_path=signals_path,
        baseline_signals_path=baseline_signals_path,
    )


def _train_cnn_lstm(
    config: ModelConfig,
    frame: FrameBundle,
    features: Sequence[str],
    data_dir: Path,
    artifacts_dir: Path,
) -> ModelRunResult:
    params = config.models.cnn_lstm
    seq_features = [name for name in features if name in frame.sequence_features]
    if not seq_features:
        # Fall back to full sequence feature set when Boruta selects only tabular lags
        seq_features = list(frame.sequence_features)
    indices = [frame.sequence_features.index(name) for name in seq_features]
    sequences = frame.sequence_tensor[:, :, indices]
    mask_train = frame.tabular[SPLIT_COLUMN] == "train"
    mask_val = frame.tabular[SPLIT_COLUMN] == "val"
    x_train = torch.from_numpy(sequences[mask_train.to_numpy()].astype(np.float32))
    y_train = torch.from_numpy(
        frame.tabular.loc[mask_train, LABEL_COLUMN].to_numpy(dtype=np.float32)
    )
    x_val = torch.from_numpy(sequences[mask_val.to_numpy()].astype(np.float32))
    y_val = torch.from_numpy(
        frame.tabular.loc[mask_val, LABEL_COLUMN].to_numpy(dtype=np.float32)
    )
    x_all = torch.from_numpy(sequences.astype(np.float32))
    dataset = TensorDataset(x_train, y_train)
    loader = DataLoader(dataset, batch_size=params.batch_size, shuffle=True)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = _build_cnn_lstm(seq_features, params).to(device)
    criterion = nn.BCELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=params.lr)
    best_state: Optional[Dict[str, Tensor]] = None
    best_loss = float("inf")
    patience_counter = 0
    has_val = x_val.numel() > 0
    for _ in range(params.epochs):
        model.train()
        for batch_x, batch_y in loader:
            batch_x = batch_x.to(device)
            batch_y = batch_y.to(device)
            optimizer.zero_grad()
            preds = model(batch_x)
            loss = criterion(preds, batch_y)
            loss.backward()
            optimizer.step()
        if has_val:
            val_loss = _evaluate_loss(model, x_val, y_val, criterion, device)
            if val_loss < best_loss - 1e-4:
                best_loss = val_loss
                best_state = {key: val.detach().cpu().clone() for key, val in model.state_dict().items()}
                patience_counter = 0
            else:
                patience_counter += 1
                if patience_counter >= params.patience:
                    break
    if best_state is not None:
        model.load_state_dict(best_state)
    model.eval()
    probs = _predict_sequence_model(model, x_all, device, params.batch_size)
    predictions = _build_predictions_dataframe(frame, probs)
    model_path = artifacts_dir / MODEL_FILENAME["cnn_lstm"]
    ensure_directory(model_path.parent)
    torch.save(model.state_dict(), model_path)
    feature_hash_value = feature_hash(seq_features)
    (
        predictions,
        predictions_path,
        signals_path,
        baseline_signals_path,
    ) = _persist_predictions("cnn_lstm", config, feature_hash_value, predictions, data_dir)
    return ModelRunResult(
        model="cnn_lstm",
        features=list(seq_features),
        feature_hash=feature_hash_value,
        predictions=predictions,
        model_path=model_path,
        predictions_path=predictions_path,
        signals_path=signals_path,
        baseline_signals_path=baseline_signals_path,
    )


def _predict_probabilities(model: object, frame: FrameBundle, features: Sequence[str]) -> np.ndarray:
    x_full = frame.tabular[features].to_numpy(dtype=float)
    if hasattr(model, "predict_proba"):
        probs = model.predict_proba(x_full)[:, 1]
    elif hasattr(model, "predict"):
        preds = model.predict(x_full)
        probs = np.clip(preds, 1e-6, 1 - 1e-6)
    else:
        raise ValueError("model does not support probability predictions")
    return probs.astype(float)


def _predict_sequence_model(
    model: nn.Module,
    inputs: Tensor,
    device: torch.device,
    batch_size: int,
) -> np.ndarray:
    data = TensorDataset(inputs)
    loader = DataLoader(data, batch_size=batch_size, shuffle=False)
    preds: List[np.ndarray] = []
    model.eval()
    with torch.no_grad():
        for (batch_x,) in loader:
            batch_x = batch_x.to(device)
            batch_probs = model(batch_x).detach().cpu().numpy()
            preds.append(batch_probs)
    return np.concatenate(preds, axis=0)


def _build_predictions_dataframe(frame: FrameBundle, probs: np.ndarray) -> pd.DataFrame:
    data = frame.tabular[[DATE_COLUMN, SPLIT_COLUMN, LABEL_COLUMN, FORWARD_RETURN_COLUMN, PRICE_COLUMN]].copy()
    data = data.reset_index(drop=True)
    data["prob_up"] = probs
    return data


def _persist_predictions(
    model_name: str,
    config: ModelConfig,
    feature_hash_value: str,
    predictions: pd.DataFrame,
    data_dir: Path,
) -> Tuple[pd.DataFrame, Path, Path, Path]:
    updated = predictions.copy()
    decisions = compute_trade_signal(
        updated["prob_up"],
        threshold=config.decision.prob_threshold,
        side=config.decision.side,
    )
    updated["decision"] = decisions
    predictions_path = data_dir / PREDICTIONS_FILE
    ensure_directory(predictions_path.parent)
    updated.to_parquet(predictions_path, index=False)
    signals = _build_signals_dataframe(
        updated,
        model_name=model_name,
        feature_hash_value=feature_hash_value,
        threshold=config.decision.prob_threshold,
    )
    signals_path = data_dir / SIGNALS_FILE
    signals.to_parquet(signals_path, index=False)
    baseline_signals_path = config.data.out_root / BASELINE_SIGNALS_FILE
    _upsert_baseline_signals(baseline_signals_path, signals)
    return updated, predictions_path, signals_path, baseline_signals_path


def _build_signals_dataframe(
    predictions: pd.DataFrame,
    *,
    model_name: str,
    feature_hash_value: str,
    threshold: float,
) -> pd.DataFrame:
    oos = predictions[predictions[SPLIT_COLUMN] != "train"].copy()
    oos = oos[[DATE_COLUMN, SPLIT_COLUMN, "prob_up", "decision"]]
    oos["model"] = model_name
    oos["threshold"] = threshold
    oos["featureset_hash"] = feature_hash_value
    ordered = oos[
        [
            DATE_COLUMN,
            SPLIT_COLUMN,
            "model",
            "prob_up",
            "decision",
            "threshold",
            "featureset_hash",
        ]
    ]
    ordered[DATE_COLUMN] = pd.to_datetime(ordered[DATE_COLUMN])
    return ordered.sort_values(DATE_COLUMN).reset_index(drop=True)


def _upsert_baseline_signals(path: Path, signals: pd.DataFrame) -> None:
    ensure_directory(path.parent)
    if path.exists():
        existing = pd.read_parquet(path)
        existing = existing[existing["model"] != signals["model"].iloc[0]]
        combined = pd.concat([existing, signals], ignore_index=True)
    else:
        combined = signals
    combined = combined.sort_values(DATE_COLUMN).reset_index(drop=True)
    combined.to_parquet(path, index=False)


def _persist_run_metadata(
    config: ModelConfig,
    frame: FrameBundle,
    run: ModelRunResult,
    feature_source: str,
) -> Path:
    metadata_path = config.data.artifacts_root / run.model / METADATA_FILE
    ensure_directory(metadata_path.parent)
    counts = {
        split: int((frame.tabular[SPLIT_COLUMN] == split).sum())
        for split in ("train", "val", "test")
    }
    payload = {
        "model": run.model,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "feature_source": feature_source,
        "feature_hash": run.feature_hash,
        "feature_count": len(run.features),
        "features": run.features,
        "splits": counts,
        "artifacts": {
            "model": str(run.model_path),
            "predictions": str(run.predictions_path),
            "signals": str(run.signals_path),
            "baseline_signals": str(run.baseline_signals_path),
        },
        "config": {
            "lookback_days": config.target.lookback_days,
            "label_horizon_days": config.target.label_horizon_days,
            "decision_threshold": config.decision.prob_threshold,
            "decision_side": config.decision.side,
        },
    }
    save_json(metadata_path, payload)
    return metadata_path


def _build_cnn_lstm(sequence_features: Sequence[str], config: CNNLSTMConfig) -> nn.Module:
    class CNNLSTMNet(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.conv = nn.Conv1d(
                in_channels=len(sequence_features),
                out_channels=config.conv_filters,
                kernel_size=config.conv_kernel,
                padding="same",
            )
            self.lstm = nn.LSTM(
                input_size=config.conv_filters,
                hidden_size=config.lstm_units,
                batch_first=True,
            )
            self.dropout = nn.Dropout(config.dropout)
            self.fc = nn.Linear(config.lstm_units, 1)

        def forward(self, x: Tensor) -> Tensor:  # type: ignore[override]
            x = x.transpose(1, 2)
            x = torch.relu(self.conv(x))
            x = x.transpose(1, 2)
            output, _ = self.lstm(x)
            x = output[:, -1, :]
            x = self.dropout(x)
            x = torch.sigmoid(self.fc(x))
            return x.squeeze(-1)

    return CNNLSTMNet()


def _evaluate_loss(
    model: nn.Module,
    inputs: Tensor,
    targets: Tensor,
    criterion: nn.Module,
    device: torch.device,
) -> float:
    if inputs.numel() == 0:
        return float("inf")
    model.eval()
    with torch.no_grad():
        preds = model(inputs.to(device))
        loss = criterion(preds, targets.to(device))
    model.train()
    return float(loss.detach().cpu())


__all__ = [
    "ModelRunResult",
    "train_model",
    "load_predictions",
    "load_run_metadata",
]
