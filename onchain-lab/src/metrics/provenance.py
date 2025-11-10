from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

import yaml

from .config import ConfigError


@dataclass(frozen=True)
class MetricProvenance:
    """Deterministic provenance fingerprints for metrics inputs and formulas."""

    price_hash: str
    snapshot_hash: str
    spent_hash: str
    formulas_tag: str

    @property
    def price_root_commit(self) -> str:
        return f"price-oracle@{self.price_hash}"

    @property
    def snapshot_commit(self) -> str:
        return f"utxo-snapshots@{self.snapshot_hash}"

    @property
    def spent_commit(self) -> str:
        return f"utxo-spent@{self.spent_hash}"

    @property
    def formulas_version(self) -> str:
        return f"metrics-formulas@{self.formulas_tag}"


def _normalize_paths(paths: Iterable[Path]) -> Sequence[Path]:
    return tuple(sorted({path.resolve() for path in paths}))


def fingerprint_paths(paths: Iterable[Path]) -> str:
    """Return a stable 12-character hex digest for a set of filesystem paths."""

    resolved = _normalize_paths(paths)
    hasher = hashlib.sha256()
    for path in resolved:
        if not path.exists():
            continue
        stat = path.stat()
        hasher.update(path.as_posix().encode("utf-8"))
        hasher.update(str(stat.st_size).encode("utf-8"))
        hasher.update(str(int(stat.st_mtime_ns)).encode("utf-8"))
    digest = hasher.hexdigest() if resolved else ""
    return digest[:12] if digest else "0" * 12


def update_registry_provenance(registry_path: Path, provenance: MetricProvenance) -> None:
    """Persist provenance fingerprints to the metrics registry file."""

    registry_path = registry_path.resolve()
    if not registry_path.exists():
        raise ConfigError(f"Metrics registry not found: {registry_path}")

    with registry_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    if raw is None:
        raise ConfigError(f"Metrics registry file {registry_path} is empty")
    if not isinstance(raw, dict):
        raise ConfigError("Metrics registry invalid: expected mapping at top level")

    metrics = raw.get("metrics")
    if not isinstance(metrics, dict):
        raise ConfigError("Metrics registry invalid: 'metrics' must be a mapping")

    metadata = raw.get("metadata") or {}
    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    metadata["generated_at"] = timestamp.isoformat().replace("+00:00", "Z")
    metadata["generator"] = "onchain-metrics.build"
    raw["metadata"] = metadata

    for entry in metrics.values():
        if not isinstance(entry, dict):
            continue
        badge = entry.get("badge") or {}
        dependencies = entry.get("dependencies", []) or []
        utxo_reference = (
            provenance.spent_commit if "utxo.spent" in dependencies else provenance.snapshot_commit
        )
        badge["utxo_snapshot_commit"] = utxo_reference
        badge["price_root_commit"] = provenance.price_root_commit
        badge["formulas_version"] = provenance.formulas_version
        entry["badge"] = badge

    with registry_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(raw, handle, sort_keys=False)


__all__ = ["MetricProvenance", "fingerprint_paths", "update_registry_provenance"]
