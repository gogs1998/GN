from __future__ import annotations

from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from metrics.config import load_registry  # type: ignore  # noqa: E402
from metrics.registry import load_metric_definitions  # type: ignore  # noqa: E402


def _write_registry(path: Path) -> None:
    content = {
        "schema_version": "metrics.registry.v2",
        "metrics": {
            "foo": {
                "description": "Example metric",
                "dependencies": ["price_oracle"],
                "docs_path": "docs/foo.md",
                "badge": {
                    "version": "1.0.0",
                    "status": "verified",
                    "coverage_pct": 100.0,
                    "null_ratio": 0.0,
                    "golden_checks_passed": True,
                    "deflated_sharpe_score": 0.25,
                    "no_lookahead": True,
                    "reproducible": True,
                    "utxo_snapshot_commit": "utxo@abc123",
                    "price_root_commit": "price@def456",
                    "formulas_version": "metrics-formulas@ghi789",
                },
            }
        },
    }
    path.write_text(yaml.safe_dump(content), encoding="utf-8")


def test_load_registry_resolves_relative_docs_path(tmp_path: Path) -> None:
    registry_path = tmp_path / "metrics_registry.yaml"
    _write_registry(registry_path)

    registry = load_registry(registry_path)
    assert registry.schema_version == "metrics.registry.v2"
    assert "foo" in registry.metrics
    entry = registry.metrics["foo"]
    assert entry.docs_path is not None
    assert entry.docs_path.is_absolute()
    assert entry.badge.reproducible is True
    assert entry.badge.coverage_pct == 100.0


def test_load_metric_definitions_exposes_badge_metadata(tmp_path: Path) -> None:
    registry_path = tmp_path / "metrics_registry.yaml"
    _write_registry(registry_path)

    definitions = load_metric_definitions(str(registry_path))
    assert len(definitions) == 1
    definition = definitions[0]
    assert definition.name == "foo"
    assert definition.badge.status == "verified"
    assert definition.badge.utxo_snapshot_commit == "utxo@abc123"
    assert definition.docs_path is not None