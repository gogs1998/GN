from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Sequence

from .normalize import FREQ_MINUTES, PriceRecord


class QAError(RuntimeError):
    """Raised when QA checks fail."""


@dataclass(frozen=True)
class QAResult:
    symbol: str
    freq: str
    gap_warnings: List[str]
    basis_warnings: List[str]

    @property
    def ok(self) -> bool:
        return not self.gap_warnings and not self.basis_warnings


def _hours_between(first: datetime, second: datetime) -> float:
    delta = second - first
    return delta.total_seconds() / 3600.0


def gap_checks(records: Sequence[PriceRecord], *, max_gap_hours: float) -> List[str]:
    if len(records) < 2:
        return []
    warnings: List[str] = []
    expected_hours = FREQ_MINUTES.get(records[0].freq, 0) / 60
    for prev, curr in zip(records, records[1:]):
        gap = _hours_between(prev.ts, curr.ts)
        missing = gap - expected_hours
        if missing > max_gap_hours:
            warnings.append(
                f"Gap {gap:.2f}h exceeds allowed {expected_hours + max_gap_hours:.2f}h between "
                f"{prev.ts.isoformat()} and {curr.ts.isoformat()}"
            )
    return warnings


def basis_checks(
    primary: Sequence[PriceRecord],
    fallback: Sequence[PriceRecord],
    *,
    max_basis_diff_pct: float,
) -> List[str]:
    primary_map = {(rec.ts, rec.symbol, rec.freq): rec for rec in primary}
    if not primary_map:
        return [
            "Primary source provided no data; cannot perform basis comparison"
        ] if fallback else []
    fallback_map = {(rec.ts, rec.symbol, rec.freq): rec for rec in fallback}
    warnings: List[str] = []
    for key, fb in fallback_map.items():
        ref = primary_map.get(key)
        if ref is None:
            warnings.append(
                f"No primary price available to validate {fb.symbol} {fb.freq} at {fb.ts.isoformat()}"
            )
            continue
        if ref.close == 0:
            continue
        diff_pct = abs(ref.close - fb.close) / ref.close * 100
        if diff_pct > max_basis_diff_pct:
            warnings.append(
                f"Basis diff {diff_pct:.4f}% exceeds {max_basis_diff_pct:.4f}% at {fb.ts.isoformat()}"
            )
    return warnings


def run_checks(
    *,
    symbol: str,
    freq: str,
    merged: Sequence[PriceRecord],
    primary: Sequence[PriceRecord],
    fallback: Sequence[PriceRecord],
    fallback_expected: bool,
    max_gap_hours: float,
    max_basis_diff_pct: float,
) -> QAResult:
    gaps = gap_checks(merged, max_gap_hours=max_gap_hours) if len(merged) >= 2 else []
    basis = basis_checks(primary, fallback, max_basis_diff_pct=max_basis_diff_pct)

    if not merged:
        gaps.append("Merged price series is empty; cannot publish price data")

    if not primary:
        gaps.append("Primary source produced no records for the build window")

    if fallback_expected and not fallback:
        basis.append("Fallback source produced no records; redundancy guarantees unmet")

    if not primary and not fallback:
        basis.append("Neither primary nor fallback sources provided data")

    return QAResult(symbol=symbol, freq=freq, gap_warnings=gaps, basis_warnings=basis)
