from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.calibration import calibration_curve
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
    roc_curve,
)

from .frame import LABEL_COLUMN, SPLIT_COLUMN
from .utils import ensure_directory, save_json


def compute_split_metrics(predictions: pd.DataFrame, threshold: float) -> Dict[str, Dict[str, float]]:
    metrics: Dict[str, Dict[str, float]] = {}
    for split in sorted(predictions[SPLIT_COLUMN].unique()):
        split_df = predictions[predictions[SPLIT_COLUMN] == split]
        metrics[split] = _compute_metrics_for_split(split_df, threshold)
    return metrics


def compute_confusion_matrices(
    predictions: pd.DataFrame, threshold: float
) -> Dict[str, Dict[str, int]]:
    matrices: Dict[str, Dict[str, int]] = {}
    for split in sorted(predictions[SPLIT_COLUMN].unique()):
        split_df = predictions[predictions[SPLIT_COLUMN] == split]
        if split_df.empty:
            continue
        matrices[split] = _confusion_counts(split_df, threshold)
    oos_mask = predictions[SPLIT_COLUMN] != "train"
    oos_df = predictions.loc[oos_mask]
    if not oos_df.empty:
        matrices["oos"] = _confusion_counts(oos_df, threshold)
    return matrices


def _compute_metrics_for_split(predictions: pd.DataFrame, threshold: float) -> Dict[str, float]:
    y_true = predictions[LABEL_COLUMN].to_numpy(dtype=float)
    prob = predictions["prob_up"].to_numpy(dtype=float)
    pred = (prob >= threshold).astype(int)
    results: Dict[str, float] = {}
    try:
        results["auc_roc"] = float(roc_auc_score(y_true, prob))
    except ValueError:
        results["auc_roc"] = float("nan")
    try:
        results["pr_auc"] = float(average_precision_score(y_true, prob))
    except ValueError:
        results["pr_auc"] = float("nan")
    results["accuracy"] = float(accuracy_score(y_true, pred))
    results["precision"] = float(precision_score(y_true, pred, zero_division=0))
    results["recall"] = float(recall_score(y_true, pred, zero_division=0))
    results["f1"] = float(f1_score(y_true, pred, zero_division=0))
    try:
        results["brier"] = float(brier_score_loss(y_true, prob))
    except ValueError:
        results["brier"] = float("nan")
    results["ece"] = float(_expected_calibration_error(y_true, prob))
    return results


def _confusion_counts(predictions: pd.DataFrame, threshold: float) -> Dict[str, int]:
    y_true = predictions[LABEL_COLUMN].to_numpy(dtype=int)
    pred = (predictions["prob_up"].to_numpy(dtype=float) >= threshold).astype(int)
    cm = confusion_matrix(y_true, pred, labels=[0, 1])
    tn, fp, fn, tp = cm.ravel() if cm.size == 4 else (0, 0, 0, 0)
    return {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)}


def _expected_calibration_error(y_true: np.ndarray, prob: np.ndarray, *, bins: int = 10) -> float:
    if len(y_true) == 0:
        return float("nan")
    bin_edges = np.linspace(0.0, 1.0, bins + 1)
    ece = 0.0
    for i in range(bins):
        lower = bin_edges[i]
        upper = bin_edges[i + 1]
        mask = (prob >= lower) & ((prob < upper) if i < bins - 1 else (prob <= upper))
        if not np.any(mask):
            continue
        bin_prob = prob[mask]
        bin_true = y_true[mask]
        gap = abs(np.mean(bin_prob) - np.mean(bin_true))
        ece += gap * (len(bin_true) / len(prob))
    return float(ece)


def generate_plots(predictions: pd.DataFrame, out_dir: Path, model_name: str) -> None:
    ensure_directory(out_dir)
    for split in sorted(predictions[SPLIT_COLUMN].unique()):
        split_df = predictions[predictions[SPLIT_COLUMN] == split]
        if split_df.empty:
            continue
        y_true = split_df[LABEL_COLUMN].to_numpy(dtype=float)
        prob = split_df["prob_up"].to_numpy(dtype=float)
        fig, ax = plt.subplots(figsize=(6, 4))
        try:
            fpr, tpr, _ = roc_curve(y_true, prob)
            ax.plot(fpr, tpr, label=f"ROC AUC {roc_auc_score(y_true, prob):.3f}")
        except ValueError:
            ax.plot([0, 1], [0, 1], linestyle="--", color="grey")
        ax.plot([0, 1], [0, 1], linestyle="--", color="grey")
        ax.set_title(f"{model_name} ROC — {split}")
        ax.set_xlabel("False Positive Rate")
        ax.set_ylabel("True Positive Rate")
        ax.legend()
        fig.tight_layout()
        fig.savefig(out_dir / f"roc_{split}.png", dpi=150)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(6, 4))
        try:
            precision, recall, _ = precision_recall_curve(y_true, prob)
            ax.plot(recall, precision, label="PR Curve")
        except ValueError:
            ax.plot([0, 1], [np.mean(y_true)] * 2, linestyle="--", color="grey")
        ax.set_title(f"{model_name} Precision-Recall — {split}")
        ax.set_xlabel("Recall")
        ax.set_ylabel("Precision")
        ax.legend()
        fig.tight_layout()
        fig.savefig(out_dir / f"pr_{split}.png", dpi=150)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(6, 4))
        try:
            frac_pos, mean_pred = calibration_curve(y_true, prob, n_bins=10, strategy="uniform")
            ax.plot(mean_pred, frac_pos, marker="o")
        except ValueError:
            ax.plot([0, 1], [0, 1], linestyle="--", color="grey")
        ax.plot([0, 1], [0, 1], linestyle="--", color="grey")
        ax.set_title(f"{model_name} Calibration — {split}")
        ax.set_xlabel("Predicted Probability")
        ax.set_ylabel("Observed Frequency")
        fig.tight_layout()
        fig.savefig(out_dir / f"calibration_{split}.png", dpi=150)
        plt.close(fig)


def save_metrics(metrics: Dict[str, Dict[str, float]], path: Path) -> None:
    ensure_directory(path.parent)
    save_json(path, metrics)


__all__ = [
    "compute_confusion_matrices",
    "compute_split_metrics",
    "generate_plots",
    "save_metrics",
]
