from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import sys

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from metrics.config import GoldenDay  # type: ignore  # noqa: E402
from metrics.compute import build_daily_metrics  # type: ignore  # noqa: E402
from metrics.docs import generate_metric_docs  # type: ignore  # noqa: E402
from metrics.golden import generate_golden_artifacts  # type: ignore  # noqa: E402
from metrics.registry import load_metric_definitions  # type: ignore  # noqa: E402
from .test_pipeline import _config  # type: ignore  # noqa: E402


def test_generate_metric_docs_creates_expected_artifacts(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    build_result = build_daily_metrics(config=cfg)

    registry_path = ROOT / "config" / "metrics_registry.yaml"
    definitions = tuple(
        replace(definition, docs_path=None)
        for definition in load_metric_definitions(str(registry_path))
    )
    docs_root = tmp_path / "docs"
    mkdocs_file = tmp_path / "mkdocs.yml"

    artifacts = generate_golden_artifacts(
        definitions,
        build_result.output_path,
        docs_root,
        cfg.qa.golden_days,
    )
    expected_image = f"sopr_{cfg.qa.golden_days[0].date.isoformat()}_golden.png"
    assert any(artifact.output_path.name == expected_image for artifact in artifacts)
    assert (docs_root / "images" / expected_image).exists()

    summary = generate_metric_docs(definitions, docs_root, mkdocs_file)

    assert summary.index_path.exists()
    assert mkdocs_file.exists()
    assert (docs_root / "images" / ".gitkeep").exists()

    sopr_doc = next(path for path in summary.written_paths if path.name == "sopr.md")
    content = sopr_doc.read_text(encoding="utf-8")

    assert "SOPR" in content
    assert "SOPR = realized_value_usd / cost_basis_spent_usd" in content
    assert "Golden Day Snapshot" in content
    assert f"![Golden day chart](../images/{expected_image})" in content

    mkdocs_text = mkdocs_file.read_text(encoding="utf-8")
    assert "site_name" in mkdocs_text
    assert "Metrics:" in mkdocs_text


def test_generate_golden_artifacts_uses_unique_filenames_per_date(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    build_result = build_daily_metrics(config=cfg)

    table = pq.read_table(build_result.output_path)
    frame = table.to_pandas()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    sopr_values = {
        day: float(frame.loc[frame["date"] == day, "sopr"].iloc[0])
        for day in sorted(frame["date"].unique())
    }

    registry_path = ROOT / "config" / "metrics_registry.yaml"
    definitions = tuple(
        replace(definition, docs_path=None)
        for definition in load_metric_definitions(str(registry_path))
    )

    docs_root = tmp_path / "docs"
    golden_days = [
        GoldenDay(date=day, metrics={"sopr": value}, tolerance_pct=cfg.qa.golden_days[0].tolerance_pct)
        for day, value in sopr_values.items()
    ]

    artifacts = generate_golden_artifacts(
        definitions,
        build_result.output_path,
        docs_root,
        golden_days,
    )

    sopr_artifacts = [artifact for artifact in artifacts if artifact.metric == "sopr"]
    names = [artifact.output_path.name for artifact in sopr_artifacts]

    assert len(names) >= 2
    assert len(names) == len(set(names))
    for artifact in sopr_artifacts:
        assert artifact.output_path.exists()
        assert artifact.output_path.parent == docs_root / "images"
