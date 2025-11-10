from __future__ import annotations

"""Helpers for working with the metrics registry."""

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Set

from .config import MetricRegistryEntry, MetricsRegistry, load_registry


@dataclass(frozen=True)
class MetricBadgeView:
    version: str
    status: str
    coverage_pct: float
    null_ratio: float
    golden_checks_passed: bool
    deflated_sharpe_score: float
    no_lookahead: bool
    reproducible: bool
    utxo_snapshot_commit: str
    price_root_commit: str
    formulas_version: str


@dataclass(frozen=True)
class MetricDefinition:
    name: str
    description: str
    dependencies: tuple[str, ...]
    badge: MetricBadgeView
    docs_path: Optional[Path]

    @property
    def qa_status(self) -> str:
        return self.badge.status


def load_metric_definitions(path: str | None = None) -> tuple[MetricDefinition, ...]:
    registry: MetricsRegistry = load_registry(Path(path)) if path else load_registry()
    definitions: list[MetricDefinition] = []
    for name, entry in registry.metrics.items():
        badge = entry.badge
        badge_view = MetricBadgeView(
            version=badge.version,
            status=badge.status,
            coverage_pct=badge.coverage_pct,
            null_ratio=badge.null_ratio,
            golden_checks_passed=badge.golden_checks_passed,
            deflated_sharpe_score=badge.deflated_sharpe_score,
            no_lookahead=badge.no_lookahead,
            reproducible=badge.reproducible,
            utxo_snapshot_commit=badge.utxo_snapshot_commit,
            price_root_commit=badge.price_root_commit,
            formulas_version=badge.formulas_version,
        )
        definitions.append(
            MetricDefinition(
                name=name,
                description=entry.description,
                dependencies=tuple(entry.dependencies),
                badge=badge_view,
                docs_path=entry.docs_path,
            )
        )
    return tuple(definitions)


def validate_metric_names(metrics: Iterable[str], registry: Iterable[MetricDefinition]) -> None:
    """Ensure every metric in *metrics* is defined in the registry."""

    defined: Set[str] = {definition.name for definition in registry}
    missing = sorted(set(metrics) - defined)
    if missing:
        raise ValueError(f"Metrics missing from registry: {', '.join(missing)}")


__all__ = [
    "MetricDefinition",
    "MetricBadgeView",
    "load_metric_definitions",
    "validate_metric_names",
]
