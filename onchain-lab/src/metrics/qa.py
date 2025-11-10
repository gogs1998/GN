from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Sequence

import pandas as pd

from .config import MetricsConfig
from .formulas import MetricsComputationResult


class MetricsQAError(RuntimeError):
    """Raised when metrics QA checks fail."""


@dataclass(frozen=True)
class GoldenDayResult:
    target_date: date
    deltas_pct: Dict[str, float]
    tolerance_pct: float
    missing_metrics: tuple[str, ...]
    passed: bool


@dataclass(frozen=True)
class QAReport:
    golden_days: tuple[GoldenDayResult, ...]
    warnings: tuple[str, ...]
    violations: tuple[str, ...]
    latest_metric_date: date | None
    latest_price_date: date | None

    @property
    def ok(self) -> bool:
        return not self.violations


def _pct_delta(actual: float, expected: float) -> float:
    if expected == 0:
        return 0.0 if abs(actual) < 1e-9 else float("inf")
    return abs(actual - expected) / abs(expected) * 100.0


def _series_dates(frame: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(frame["date"]).dt.date


def _check_order(dates: pd.Series, *, violations: List[str]) -> None:
    if not dates.is_monotonic_increasing:
        violations.append("Metrics dates are not sorted ascending")
    if dates.duplicated().any():
        violations.append("Duplicate metric dates detected")


def _check_price_floor(frame: pd.DataFrame, *, min_price: float, violations: List[str]) -> None:
    if frame["price_close"].min() < min_price:
        violations.append(f"Observed price falls below configured floor {min_price}")


def _check_drawdown(frame: pd.DataFrame, *, max_drawdown_pct: float, violations: List[str]) -> None:
    if frame.empty:
        return
    minimum = frame["drawdown_pct"].min()
    if pd.notna(minimum) and abs(minimum) > max_drawdown_pct:
        violations.append(
            f"Drawdown {minimum:.2f}% exceeds max allowed {max_drawdown_pct:.2f}%"
        )


def _evaluate_golden_days(
    frame: pd.DataFrame,
    *,
    golden_days: Sequence,
    violations: List[str],
) -> List[GoldenDayResult]:
    if not golden_days:
        return []
    indexed = frame.copy()
    indexed["__date_index"] = pd.to_datetime(indexed["date"]).dt.date
    lookup = indexed.set_index("__date_index")
    results: List[GoldenDayResult] = []
    for golden in golden_days:
        deltas: Dict[str, float] = {}
        missing: List[str] = []
        passed = True
        if golden.date not in lookup.index:
            violations.append(f"Golden day {golden.date.isoformat()} missing from metrics output")
            results.append(
                GoldenDayResult(
                    target_date=golden.date,
                    deltas_pct=deltas,
                    tolerance_pct=golden.tolerance_pct,
                    missing_metrics=tuple(golden.metrics.keys()),
                    passed=False,
                )
            )
            continue
        row = lookup.loc[golden.date]
        if isinstance(row, pd.DataFrame):
            # Multiple rows indicate duplicate dates; already flagged but choose first.
            row = row.iloc[0]
        for metric, expected in golden.metrics.items():
            if metric not in row:
                missing.append(metric)
                passed = False
                violations.append(
                    f"Golden day {golden.date.isoformat()} missing metric '{metric}'"
                )
                continue
            actual = row[metric]
            if pd.isna(actual):
                missing.append(metric)
                passed = False
                violations.append(
                    f"Golden day {golden.date.isoformat()} metric '{metric}' is NaN"
                )
                continue
            actual = float(actual)
            delta = _pct_delta(actual, float(expected))
            deltas[metric] = delta
            if delta > golden.tolerance_pct:
                passed = False
                violations.append(
                    (
                        f"Golden day {golden.date.isoformat()} metric '{metric}' delta {delta:.2f}% "
                        f"exceeds tolerance {golden.tolerance_pct:.2f}% (expected {expected}, got {actual})"
                    )
                )
        results.append(
            GoldenDayResult(
                target_date=golden.date,
                deltas_pct=deltas,
                tolerance_pct=golden.tolerance_pct,
                missing_metrics=tuple(missing),
                passed=passed,
            )
        )
    return results


def _check_lookahead(
    *,
    dates: pd.Series,
    price_df: pd.DataFrame,
    tolerance_days: int,
    violations: List[str],
    warnings: List[str],
) -> tuple[date | None, date | None]:
    if price_df.empty:
        warnings.append("Price input frame is empty; skipping lookahead check")
        return None, None
    price_ts = pd.to_datetime(price_df["ts"], utc=True, errors="coerce").dropna()
    if price_ts.empty:
        warnings.append("Price timestamps could not be parsed; skipping lookahead check")
        return None, None
    latest_price_date = price_ts.max().date()
    latest_metric_date = dates.max()
    delta_days = (latest_metric_date - latest_price_date).days
    if delta_days > tolerance_days:
        violations.append(
            (
                f"Metrics extend {delta_days} day(s) beyond available price data "
                f"(latest metric {latest_metric_date}, latest price {latest_price_date})"
            )
        )
    return latest_metric_date, latest_price_date


def run_qa_checks(
    result: MetricsComputationResult,
    *,
    price_df: pd.DataFrame,
    config: MetricsConfig,
    raise_on_failure: bool = True,
) -> QAReport:
    frame = result.frame.copy()
    violations: List[str] = []
    warnings: List[str] = []

    dates = _series_dates(frame)
    _check_order(dates, violations=violations)
    _check_price_floor(frame, min_price=config.qa.min_price, violations=violations)
    _check_drawdown(
        frame,
        max_drawdown_pct=config.qa.max_drawdown_pct,
        violations=violations,
    )
    golden_results = _evaluate_golden_days(frame, golden_days=config.qa.golden_days, violations=violations)
    latest_metric_date, latest_price_date = _check_lookahead(
        dates=dates,
        price_df=price_df,
        tolerance_days=config.qa.lookahead_tolerance_days,
        violations=violations,
        warnings=warnings,
    )

    report = QAReport(
        golden_days=tuple(golden_results),
        warnings=tuple(warnings),
        violations=tuple(violations),
        latest_metric_date=latest_metric_date,
        latest_price_date=latest_price_date,
    )

    if raise_on_failure and not report.ok:
        raise MetricsQAError("; ".join(report.violations))

    return report


__all__ = ["MetricsQAError", "QAReport", "GoldenDayResult", "run_qa_checks"]
