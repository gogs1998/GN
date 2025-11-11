from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

import numpy as np
import pandas as pd

from .config import ModelConfig
from .frame import DATE_COLUMN, FORWARD_RETURN_COLUMN, SPLIT_COLUMN
from .utils import append_registry_entry, compute_trade_signal, ensure_directory, save_json

BACKTEST_DIR = "backtest"
EQUITY_FILE = "equity.parquet"
SUMMARY_FILE = "summary.json"

PERIODS_PER_YEAR = 365


@dataclass
class BacktestResult:
    model: str
    equity: pd.DataFrame
    summary: Dict[str, Dict[str, float]]
    equity_path: Optional[str] = None
    summary_path: Optional[str] = None


def run_backtest(model_name: str, predictions: pd.DataFrame, config: ModelConfig) -> BacktestResult:
    if {DATE_COLUMN, SPLIT_COLUMN, FORWARD_RETURN_COLUMN, "prob_up"} - set(predictions.columns):
        missing = {DATE_COLUMN, SPLIT_COLUMN, FORWARD_RETURN_COLUMN, "prob_up"} - set(
            predictions.columns
        )
        raise ValueError(f"predictions missing required columns: {sorted(missing)}")
    data = predictions.copy()
    data[DATE_COLUMN] = pd.to_datetime(data[DATE_COLUMN])
    data = data.sort_values(DATE_COLUMN).reset_index(drop=True)
    data["signal"] = pd.Series(
        compute_trade_signal(
            data["prob_up"], threshold=config.decision.prob_threshold, side=config.decision.side
        ),
        index=data.index,
    )
    data.loc[data[SPLIT_COLUMN] == "train", "signal"] = 0.0
    data["position"] = data["signal"].shift(1).fillna(0.0)
    data.loc[data[SPLIT_COLUMN] == "train", "position"] = 0.0
    data["turnover"] = data["position"].diff().abs().fillna(0.0)
    cost_rate = (config.costs.fee_bps + config.costs.slippage_bps) / 10_000.0
    data["cost"] = data["turnover"] * cost_rate
    data["gross_return"] = data["position"] * data[FORWARD_RETURN_COLUMN]
    data["net_return"] = data["gross_return"] - data["cost"]
    data["equity_curve"] = (1.0 + data["net_return"]).cumprod()
    _run_quality_checks(data, config)
    summary = _summarize_backtest(data, config)
    return BacktestResult(model=model_name, equity=data, summary=summary)


def persist_backtest(result: BacktestResult, config: ModelConfig) -> BacktestResult:
    model_dir = config.data.out_root / result.model / BACKTEST_DIR
    ensure_directory(model_dir)
    equity_path = model_dir / EQUITY_FILE
    summary_path = model_dir / SUMMARY_FILE
    result.equity.to_parquet(equity_path, index=False)
    save_json(summary_path, result.summary)
    result.equity_path = str(equity_path)
    result.summary_path = str(summary_path)
    return result


def register_backtest(result: BacktestResult, config: ModelConfig) -> None:
    registry_entry = {
        "model": result.model,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "equity_path": result.equity_path,
        "summary_path": result.summary_path,
        "total_return": result.summary.get("overall", {}).get("total_return", float("nan")),
        "sharpe": result.summary.get("overall", {}).get("sharpe", float("nan")),
    }
    append_registry_entry(config.registry.path, registry_entry)
def _summarize_backtest(equity: pd.DataFrame, config: ModelConfig) -> Dict[str, Dict[str, float]]:
    results: Dict[str, Dict[str, float]] = {}
    mask = equity[SPLIT_COLUMN] != "train"
    oos_equity = equity.loc[mask]
    results["overall"] = _compute_metrics(oos_equity)
    for split in sorted(oos_equity[SPLIT_COLUMN].dropna().unique()):
        split_df = oos_equity[oos_equity[SPLIT_COLUMN] == split]
        results[split] = _compute_metrics(split_df)
    return results


def _compute_metrics(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty:
        return {
            "total_return": float("nan"),
            "annual_return": float("nan"),
            "annual_vol": float("nan"),
            "sharpe": float("nan"),
            "max_drawdown": float("nan"),
            "win_rate": float("nan"),
            "trades": 0.0,
            "avg_trade_return": float("nan"),
            "exposure": float("nan"),
        }
    net_returns = df["net_return"].to_numpy(dtype=float)
    equity_curve = (1.0 + net_returns).cumprod()
    total_return = float(equity_curve[-1] - 1.0)
    periods = len(net_returns)
    annual_return = float((1.0 + total_return) ** (PERIODS_PER_YEAR / max(periods, 1)) - 1.0)
    annual_vol = float(np.std(net_returns, ddof=0) * np.sqrt(PERIODS_PER_YEAR))
    sharpe = float(annual_return / annual_vol) if annual_vol > 0 else float("nan")
    running_max = np.maximum.accumulate(equity_curve)
    drawdowns = equity_curve / running_max - 1.0
    max_drawdown = float(drawdowns.min()) if len(drawdowns) else float("nan")
    win_rate = float(np.mean(net_returns > 0))
    position = df["position"].to_numpy(dtype=float)
    trades = float(np.sum(np.abs(np.diff(position)) > 1e-8))
    active_mask = np.abs(position) > 1e-8
    avg_trade_return = float(np.mean(net_returns[active_mask])) if np.any(active_mask) else float("nan")
    exposure = float(np.mean(np.abs(position)))
    return {
        "total_return": total_return,
        "annual_return": annual_return,
        "annual_vol": annual_vol,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "win_rate": win_rate,
        "trades": trades,
        "avg_trade_return": avg_trade_return,
        "exposure": exposure,
    }


def _run_quality_checks(df: pd.DataFrame, config: ModelConfig) -> None:
    qa = config.qa
    if qa.forbid_future_lookahead:
        misaligned = df.loc[df["signal"].shift(1).isna() & (df["position"].abs() > 1e-8)]
        if not misaligned.empty:
            raise ValueError("detected position before sufficient history; potential lookahead")
    if qa.min_oos_start:
        oos = df.loc[df[SPLIT_COLUMN] != "train", DATE_COLUMN]
        if not oos.empty:
            min_oos = oos.min().date()
            required = pd.to_datetime(qa.min_oos_start).date()
            if min_oos < required:
                raise ValueError(
                    f"out-of-sample period starts at {min_oos} before required minimum {required}"
                )


__all__ = [
    "BacktestResult",
    "persist_backtest",
    "register_backtest",
    "run_backtest",
]
