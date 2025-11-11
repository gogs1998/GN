from __future__ import annotations

from .backtest import BacktestResult, persist_backtest, register_backtest, run_backtest
from .baselines_core import ModelRunResult, load_predictions, load_run_metadata, train_model
from .boruta import BorutaResult, BorutaRunner, load_selected_features
from .config import ModelConfig, load_model_config
from .eval import compute_split_metrics, generate_plots, save_metrics
from .frame import FrameBuilder, FrameBundle, load_frame_bundle
from .utils import append_registry_entry

__all__ = [
    "BacktestResult",
    "ModelRunResult",
    "BorutaResult",
    "BorutaRunner",
    "FrameBuilder",
    "FrameBundle",
    "append_registry_entry",
    "compute_split_metrics",
    "generate_plots",
    "load_frame_bundle",
    "load_model_config",
    "load_predictions",
    "load_run_metadata",
    "persist_backtest",
    "register_backtest",
    "run_backtest",
    "save_metrics",
    "train_model",
]
