from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import pyarrow.parquet as pq

from .config import GoldenDay
from .registry import MetricDefinition

matplotlib.use("Agg", force=True)


@dataclass(frozen=True)
class GoldenArtifact:
    metric: str
    target_date: date
    output_path: Path


def _artifact_filename(metric: str, target_date: date) -> str:
    return f"{metric}_{target_date.isoformat()}_golden.png"


def _resolve_metric_lookup(definitions: Iterable[MetricDefinition]) -> Mapping[str, MetricDefinition]:
    return {definition.name: definition for definition in definitions}


def _ensure_images_root(docs_root: Path) -> Path:
    images_root = docs_root / "images"
    images_root.mkdir(parents=True, exist_ok=True)
    return images_root


def _prepare_frame(metrics_path: Path) -> pd.DataFrame:
    table = pq.read_table(metrics_path)
    frame = table.to_pandas()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    return frame


def _render_chart(
    frame: pd.DataFrame,
    metric: str,
    target_date: date,
    expected_value: float,
    output_path: Path,
    window: int,
) -> None:
    window_start = target_date - timedelta(days=window)
    window_end = target_date + timedelta(days=window)
    subset = frame[(frame["date"] >= window_start) & (frame["date"] <= window_end)].copy()
    if subset.empty:
        return

    dates_numeric = [mdates.date2num(d) for d in subset["date"]]
    values = subset[metric].astype(float).to_numpy()
    target_num = mdates.date2num(target_date)

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(dates_numeric, values, label=metric, linewidth=2)
    ax.axvline(target_num, color="tab:red", linestyle="--", label="Golden day")
    if metric in subset.columns:
        golden_point = subset[subset["date"] == target_date]
        if not golden_point.empty:
            actual_value = float(golden_point[metric].astype(float).iloc[0])
            ax.scatter([target_num], [actual_value], color="black", zorder=5)
    ax.scatter([target_num], [expected_value], color="gold", edgecolor="black", zorder=6)
    ax.set_title(f"{metric} around {target_date.isoformat()}")
    ax.set_ylabel(metric)
    ax.set_xlabel("date")
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    fig.autofmt_xdate()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def generate_golden_artifacts(
    definitions: Iterable[MetricDefinition],
    metrics_path: Path,
    docs_root: Path,
    golden_days: Sequence[GoldenDay],
    window: int = 7,
) -> list[GoldenArtifact]:
    if not metrics_path.exists():
        raise FileNotFoundError(f"Metrics dataset missing at {metrics_path}")

    frame = _prepare_frame(metrics_path)
    lookup = _resolve_metric_lookup(definitions)
    images_root = _ensure_images_root(docs_root)
    artifacts: list[GoldenArtifact] = []

    for golden in golden_days:
        for metric, expected in golden.metrics.items():
            definition = lookup.get(metric)
            if definition is None:
                continue
            if metric not in frame.columns:
                continue
            output_path = images_root / _artifact_filename(metric, golden.date)
            _render_chart(frame, metric, golden.date, expected, output_path, window)
            if output_path.exists():
                artifacts.append(GoldenArtifact(metric=metric, target_date=golden.date, output_path=output_path))
    return artifacts


__all__ = ["GoldenArtifact", "generate_golden_artifacts"]
