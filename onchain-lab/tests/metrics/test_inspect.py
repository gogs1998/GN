from __future__ import annotations

import json
from datetime import date
from pathlib import Path
import sys

import yaml
from typer.testing import CliRunner

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from metrics.cli import app as metrics_cli_app  # type: ignore  # noqa: E402
from metrics.compute import build_daily_metrics  # type: ignore  # noqa: E402
from metrics.config import MetricsConfig  # type: ignore  # noqa: E402
from metrics.inspect import inspect_metric  # type: ignore  # noqa: E402
from .test_pipeline import _config  # type: ignore  # noqa: E402


def _write_config_file(cfg: MetricsConfig, path: Path) -> Path:
    data = cfg.model_dump()

    def _coerce(value):
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {k: _coerce(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_coerce(item) for item in value]
        return value

    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(_coerce(data), handle)
    return path


def test_inspect_metric_returns_upstream_records(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    registry_path = ROOT / "config" / "metrics_registry.yaml"

    summary = inspect_metric(
        cfg,
        "sopr",
        target_date=date(2024, 1, 2),
        registry_path=registry_path,
        limit=5,
        offset=0,
    )

    assert summary.metric.name == "sopr"
    assert summary.badge.status == "verified"
    assert summary.totals["spent_rows"] == 1
    assert summary.totals["price_rows"] == 4
    assert summary.totals["price_close"] == 52000.0
    assert summary.spent_rows[0]["source_txid"] == "aaa"
    assert len(summary.price_rows) == 4
    assert not summary.snapshot_rows
    assert "snapshot_rows" not in summary.totals


def test_inspect_metric_includes_snapshot_rows_for_snapshot_metric(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    registry_path = ROOT / "config" / "metrics_registry.yaml"

    summary = inspect_metric(
        cfg,
        "mvrv",
        target_date=date(2024, 1, 2),
        registry_path=registry_path,
        limit=5,
        offset=0,
    )

    assert summary.metric.name == "mvrv"
    assert summary.price_rows
    assert summary.snapshot_rows
    assert not summary.spent_rows
    assert summary.totals["snapshot_rows"] >= len(summary.snapshot_rows)


def test_cli_show_supports_json_output(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    registry_source = ROOT / "config" / "metrics_registry.yaml"
    registry_copy = tmp_path / "registry.yaml"
    registry_copy.write_text(registry_source.read_text(encoding="utf-8"), encoding="utf-8")

    build_daily_metrics(config=cfg, registry_path=registry_copy)

    config_path = _write_config_file(cfg, tmp_path / "metrics.yaml")

    runner = CliRunner()
    result = runner.invoke(
        metrics_cli_app,
        [
            "show",
            "sopr",
            "--date",
            "2024-01-02",
            "--config",
            str(config_path),
            "--registry",
            str(registry_copy),
            "--json",
            "--offset",
            "0",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["metric"]["name"] == "sopr"
    assert payload["badge"]["status"] == "verified"
    assert payload["totals"]["spent_rows"] == 1
    assert payload["totals"]["price_rows"] == 4
    assert payload["spent_rows"][0]["source_txid"] == "aaa"
    assert payload["price_rows"]
    assert payload["snapshot_rows"] == []


def test_cli_show_writes_payload_to_disk(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    registry_source = ROOT / "config" / "metrics_registry.yaml"
    registry_copy = tmp_path / "registry.yaml"
    registry_copy.write_text(registry_source.read_text(encoding="utf-8"), encoding="utf-8")

    build_daily_metrics(config=cfg, registry_path=registry_copy)

    config_path = _write_config_file(cfg, tmp_path / "metrics.yaml")
    output_path = tmp_path / "inspection.json"

    runner = CliRunner()
    result = runner.invoke(
        metrics_cli_app,
        [
            "show",
            "sopr",
            "--date",
            "2024-01-02",
            "--config",
            str(config_path),
            "--registry",
            str(registry_copy),
            "--limit",
            "1",
            "--output",
            str(output_path),
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert output_path.exists()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["metric"]["name"] == "sopr"
    assert payload["snapshot_rows"] == []


def test_inspect_metric_totals_ignore_pagination(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    registry_path = ROOT / "config" / "metrics_registry.yaml"

    first_page = inspect_metric(
        cfg,
        "sopr",
        target_date=date(2024, 1, 2),
        registry_path=registry_path,
        limit=2,
        offset=0,
    )

    second_page = inspect_metric(
        cfg,
        "sopr",
        target_date=date(2024, 1, 2),
        registry_path=registry_path,
        limit=2,
        offset=2,
    )

    assert first_page.totals == second_page.totals