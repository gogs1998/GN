from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa

from .config import LifecycleConfig
from .datasets import read_created, read_snapshots, read_spent


@dataclass
class QAResult:
    name: str
    passed: bool
    details: Dict[str, object]


class LifecycleQA:
    def __init__(self, config: LifecycleConfig) -> None:
        self._config = config

    def run(
        self,
        *,
        created: Optional[pa.Table] = None,
        spent: Optional[pa.Table] = None,
        snapshots: Optional[Iterable[tuple[date, pa.Table]]] = None,
    ) -> List[QAResult]:
        root = self._config.data.lifecycle_root
        created_table = created or read_created(root)
        spent_table = spent or read_spent(root)
        snapshot_tables = list(snapshots) if snapshots is not None else read_snapshots(root)

        created_df = created_table.to_pandas()
        spent_df = spent_table.to_pandas()

        snapshot_frames: Dict[date, pd.DataFrame] = {}
        for snapshot_date, table in snapshot_tables:
            snapshot_frames[snapshot_date] = table.to_pandas()

        results = [
            self._check_orphan_spends(spent_df),
            self._check_price_coverage(created_df, spent_df),
            self._check_supply(created_df, spent_df, snapshot_frames),
            self._check_lifespan(spent_df),
            self._check_snapshot_completeness(created_df, spent_df, snapshot_frames),
        ]
        return results

    def _check_orphan_spends(self, spent_df: pd.DataFrame) -> QAResult:
        if "is_orphan" in spent_df:
            is_orphan = spent_df["is_orphan"].astype(bool)
        else:
            is_orphan = pd.Series([False] * len(spent_df))
        orphans = spent_df[is_orphan]
        passed = orphans.empty
        details = {
            "orphan_count": int(len(orphans)),
            "examples": orphans[["source_txid", "source_vout"]].head(5).to_dict(orient="records"),
        }
        return QAResult(name="orphan_spends", passed=passed, details=details)

    def _check_price_coverage(self, created_df: pd.DataFrame, spent_df: pd.DataFrame) -> QAResult:
        required = self._config.qa.price_coverage_min_pct
        created_pct = (
            created_df["creation_price_close"].notna().mean() * 100.0 if not created_df.empty else 100.0
        )
        spent_pct = (
            spent_df["spend_price_close"].notna().mean() * 100.0 if not spent_df.empty else 100.0
        )
        passed = created_pct >= required and spent_pct >= required
        details = {
            "min_required_pct": required,
            "created_pct": created_pct,
            "spent_pct": spent_pct,
        }
        return QAResult(name="price_coverage", passed=passed, details=details)

    def _check_supply(
        self,
        created_df: pd.DataFrame,
        spent_df: pd.DataFrame,
        snapshot_frames: Dict[date, pd.DataFrame],
    ) -> QAResult:
        created_total = int(created_df["value_sats"].sum()) if "value_sats" in created_df else 0
        spent_total = int(spent_df["value_sats"].sum()) if "value_sats" in spent_df else 0
        outstanding_expected = created_total - spent_total

        latest_snapshot_value = 0
        latest_date = None
        if snapshot_frames:
            latest_date = max(snapshot_frames)
            latest_snapshot_value = int(snapshot_frames[latest_date]["balance_sats"].sum())

        diff = abs(outstanding_expected - latest_snapshot_value)
        tolerance = self._config.qa.supply_tolerance_sats
        passed = diff <= tolerance
        details = {
            "created_total_sats": created_total,
            "spent_total_sats": spent_total,
            "outstanding_expected_sats": outstanding_expected,
            "snapshot_total_sats": latest_snapshot_value,
            "tolerance_sats": tolerance,
            "difference_sats": diff,
            "latest_snapshot_date": latest_date,
        }
        return QAResult(name="supply_reconciliation", passed=passed, details=details)

    def _check_lifespan(self, spent_df: pd.DataFrame) -> QAResult:
        lifespan_days = spent_df.get("holding_days")
        invalid_negative = lifespan_days[lifespan_days < 0].dropna() if lifespan_days is not None else []
        too_long = (
            lifespan_days[lifespan_days > self._config.qa.lifespan_max_days].dropna()
            if lifespan_days is not None
            else []
        )
        passed = len(invalid_negative) == 0 and len(too_long) == 0
        details = {
            "negative_durations": invalid_negative.tolist() if hasattr(invalid_negative, "tolist") else [],
            "exceeds_max": too_long.tolist() if hasattr(too_long, "tolist") else [],
            "max_days": self._config.qa.lifespan_max_days,
        }
        return QAResult(name="lifespan_bounds", passed=passed, details=details)

    def _check_snapshot_completeness(
        self,
        created_df: pd.DataFrame,
        spent_df: pd.DataFrame,
        snapshot_frames: Dict[date, pd.DataFrame],
    ) -> QAResult:
        created_outputs = int(len(created_df))
        if "is_orphan" in spent_df:
            spent_outputs = int(len(spent_df[~spent_df["is_orphan"].astype(bool)]))
        else:
            spent_outputs = int(len(spent_df))
        outstanding_outputs = created_outputs - spent_outputs

        latest_snapshot_outputs = 0
        if snapshot_frames:
            latest_snapshot_outputs = int(
                snapshot_frames[max(snapshot_frames)]["output_count"].sum()
            )

        diff_outputs = outstanding_outputs - latest_snapshot_outputs

        if snapshot_frames:
            span_days = (max(snapshot_frames) - min(snapshot_frames)).days + 1
            gap_pct = (span_days - len(snapshot_frames)) / span_days * 100 if span_days else 0.0
        else:
            gap_pct = 100.0 if created_outputs else 0.0

        passed = diff_outputs == 0 and gap_pct <= self._config.qa.max_snapshot_gap_pct
        details = {
            "created_outputs": created_outputs,
            "spent_outputs": spent_outputs,
            "outstanding_outputs": outstanding_outputs,
            "snapshot_outputs": latest_snapshot_outputs,
            "difference_outputs": diff_outputs,
            "gap_pct": gap_pct,
            "max_gap_pct": self._config.qa.max_snapshot_gap_pct,
        }
        return QAResult(name="snapshot_completeness", passed=passed, details=details)


__all__ = ["LifecycleQA", "QAResult"]
