from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence

from zoneinfo import ZoneInfo

from .config import PriceOracleConfig, load_config
from .normalize import PriceRecord, apply_alignment, merge_sources
from .qa import QAError, QAResult, run_checks
from .sources import load_records
from .store import PriceStore

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class BuildSummary:
    symbol: str
    freq: str
    records_written: int
    qa: QAResult
    fallback_points: int = 0
    fallback_discarded: int = 0


class PriceOracle:
    def __init__(self, config: PriceOracleConfig | None = None) -> None:
        self.config = config or load_config()
        self.store = PriceStore(self.config.data_root)
        self.raw_root = self._resolve_raw_root()

    def _resolve_raw_root(self) -> Path:
        root = self.config.data_root
        parent = root.parent if root.parent != root else root
        return parent / "raw"

    def _csv_path(self, source: str, symbol: str, freq: str) -> Path:
        filename = f"{symbol}-{freq}.csv"
        return self.raw_root / source / filename

    def _load_source(self, source: str, symbol: str, freq: str) -> List[PriceRecord]:
        path = self._csv_path(source, symbol, freq)
        if not path.exists():
            logger.warning(
                "Missing %s export for %s %s: %s",
                source,
                symbol,
                freq,
                path,
            )
            return []
        return load_records(source, path, freq, symbol)

    def _alignment_context(self, freq: str) -> tuple[str | None, ZoneInfo | None]:
        if freq == "1d":
            return self.config.alignment.daily_close_hhmm, ZoneInfo(self.config.alignment.timezone)
        return None, ZoneInfo("UTC")

    def build(
        self,
        *,
        symbols: Sequence[str] | None = None,
        freqs: Sequence[str] | None = None,
    ) -> List[BuildSummary]:
        symbol_list = list(symbols) if symbols is not None else list(self.config.symbols)
        freq_list = list(freqs) if freqs is not None else list(self.config.freqs)
        summaries: List[BuildSummary] = []
        for symbol in symbol_list:
            for freq in freq_list:
                summary = self._build_pair(symbol, freq)
                summaries.append(summary)
        return summaries

    def build_all(self) -> List[BuildSummary]:
        return self.build()

    def _build_pair(self, symbol: str, freq: str) -> BuildSummary:
        logger.info("Building price series for %s %s", symbol, freq)
        primary_records = self._load_source(self.config.primary, symbol, freq)
        fallback_records: List[PriceRecord] = []
        if self.config.fallback and self.config.fallback != self.config.primary:
            fallback_records = self._load_source(self.config.fallback, symbol, freq)
        boundary, tzinfo = self._alignment_context(freq)
        aligned_primary = apply_alignment(primary_records, freq, boundary, tzinfo=tzinfo)
        aligned_fallback = apply_alignment(fallback_records, freq, boundary, tzinfo=tzinfo)

        filtered_fallback: List[PriceRecord] = []
        discarded_fallback = 0
        if aligned_fallback:
            primary_lookup = {
                (rec.ts, rec.symbol, rec.freq): rec for rec in aligned_primary
            }
            for rec in aligned_fallback:
                primary_rec = primary_lookup.get((rec.ts, rec.symbol, rec.freq))
                if primary_rec and primary_rec.close != 0:
                    diff_pct = abs(primary_rec.close - rec.close) / primary_rec.close * 100
                    if diff_pct > self.config.qa.max_basis_diff_pct:
                        discarded_fallback += 1
                        continue
                filtered_fallback.append(rec)
        else:
            filtered_fallback = aligned_fallback

        if discarded_fallback:
            logger.warning(
                "Discarded %d fallback records for %s %s exceeding %.4f%% basis threshold",
                discarded_fallback,
                symbol,
                freq,
                self.config.qa.max_basis_diff_pct,
            )

        aligned_fallback = filtered_fallback

        source_priority: List[str] = [self.config.primary]
        if self.config.fallback and self.config.fallback not in source_priority:
            source_priority.append(self.config.fallback)

        merged = merge_sources(aligned_primary + aligned_fallback, source_priority)

        qa_result = run_checks(
            symbol=symbol,
            freq=freq,
            merged=merged,
            primary=aligned_primary,
            fallback=aligned_fallback,
            fallback_expected=bool(self.config.fallback and self.config.fallback != self.config.primary),
            max_gap_hours=self.config.qa.max_gap_hours,
            max_basis_diff_pct=self.config.qa.max_basis_diff_pct,
        )

        missing_primary_basis = [
            warning
            for warning in qa_result.basis_warnings
            if warning.startswith("No primary price available")
        ]
        blocking_basis = [
            warning
            for warning in qa_result.basis_warnings
            if warning not in missing_primary_basis
        ]
        blocking_gaps = list(qa_result.gap_warnings)

        if blocking_gaps or blocking_basis:
            issues = blocking_gaps + blocking_basis
            raise QAError("; ".join(issues))

        if missing_primary_basis:
            logger.warning(
                "Primary gaps covered by fallback for %s %s at %d timestamps",
                symbol,
                freq,
                len(missing_primary_basis),
            )

        qa_clean = QAResult(
            symbol=qa_result.symbol,
            freq=qa_result.freq,
            gap_warnings=[],
            basis_warnings=[],
        )

        fallback_points = sum(1 for rec in merged if rec.source != self.config.primary)

        self.store.upsert(merged)
        logger.info("%s %s: wrote %s records", symbol, freq, len(merged))
        return BuildSummary(
            symbol=symbol,
            freq=freq,
            records_written=len(merged),
            qa=qa_clean,
            fallback_points=fallback_points,
            fallback_discarded=discarded_fallback,
        )

    def latest(self, symbol: str, freq: str, limit: int = 5) -> Sequence[PriceRecord]:
        records = self.store.read(symbol, freq)
        return records[-limit:]


__all__ = ["PriceOracle", "BuildSummary"]
