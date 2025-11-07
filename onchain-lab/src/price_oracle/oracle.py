from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence

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

    def _alignment_boundary(self, freq: str) -> str | None:
        if freq == "1d":
            return self.config.alignment.daily_close_hhmm
        return None

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

        boundary = self._alignment_boundary(freq)
        aligned_primary = apply_alignment(primary_records, freq, boundary)
        aligned_fallback = apply_alignment(fallback_records, freq, boundary)

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
            max_gap_hours=self.config.qa.max_gap_hours,
            max_basis_diff_pct=self.config.qa.max_basis_diff_pct,
        )

        if not qa_result.ok:
            issues = qa_result.gap_warnings + qa_result.basis_warnings
            raise QAError("; ".join(issues))

        self.store.upsert(merged)
        logger.info("%s %s: wrote %s records", symbol, freq, len(merged))
        return BuildSummary(
            symbol=symbol,
            freq=freq,
            records_written=len(merged),
            qa=qa_result,
        )

    def latest(self, symbol: str, freq: str, limit: int = 5) -> Sequence[PriceRecord]:
        records = self.store.read(symbol, freq)
        return records[-limit:]


__all__ = ["PriceOracle", "BuildSummary"]
