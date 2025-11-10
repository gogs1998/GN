from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from .registry import MetricDefinition

_FORMULA_TEXT: dict[str, str] = {
    "price_close": "Closing price in USD sourced from the approved oracle snapshot at 23:00 UTC.",
    "market_value_usd": "market_value_usd = supply_btc × price_close.",
    "realized_value_usd": "realized_value_usd = Σ cost_basis_usd across all unspent outputs.",
    "realized_profit_usd": "realized_profit_usd = Σ max(realized_value_usd − cost_basis_usd, 0) over daily spends.",
    "realized_loss_usd": "realized_loss_usd = Σ abs(min(realized_value_usd − cost_basis_usd, 0)) over daily spends.",
    "realized_profit_loss_ratio": "realized_profit_loss_ratio = realized_profit_usd / realized_loss_usd.",
    "sopr": "SOPR = realized_value_usd / cost_basis_spent_usd (spent outputs).",
    "asopr": "aSOPR = adjusted_realized_value / adjusted_cost_basis for spends held ≥ 1 hour.",
    "mvrv": "MVRV = market_value_usd / supply_cost_basis_usd.",
    "mvrv_zscore": "MVRV Z-Score = (market_value_usd − supply_cost_basis_usd − rolling_mean) / rolling_std.",
    "nupl": "NUPL = (market_value_usd − supply_cost_basis_usd) / market_value_usd.",
    "cdd": "CDD = Σ (value_btc × holding_days) for all spends on the metric date.",
    "adjusted_cdd": "Adjusted CDD = CDD / spent_value_btc.",
    "dormancy_flow": "Dormancy Flow = market_value_usd / rolling_mean(CDD).",
    "utxo_profit_share": "UTXO Profit Share = share of outstanding supply with cost_basis < price_close.",
    "drawdown_pct": "Drawdown % = (price_close − rolling_max(price_close)) / rolling_max(price_close).",
    "supply_btc": "Supply BTC = Σ balance_btc across current UTXO set.",
    "supply_sats": "Supply Sats = supply_btc × 100,000,000.",
    "supply_cost_basis_usd": "Supply Cost Basis = Σ cost_basis_usd across current UTXO set.",
}

_CITATIONS: dict[str, list[str]] = {
    "sopr": [
        "Willy Woo, \"On-chain metrics: SOPR\" (2019)",
        "Glassnode Academy, \"Spent Output Profit Ratio\"",
    ],
    "mvrv": [
        "David Puell, \"Market-Value-to-Realized-Value (MVRV) Ratio\"",
        "Glassnode Academy, \"MVRV Ratio\"",
    ],
    "mvrv_zscore": [
        "LookIntoBitcoin, \"MVRV Z-Score\"",
    ],
    "nupl": [
        "LookIntoBitcoin, \"Net Unrealized Profit/Loss\"",
    ],
    "cdd": [
        "ByteTree, \"Coin Days Destroyed\"",
    ],
    "dormancy_flow": [
        "Glassnode Academy, \"Dormancy Flow\"",
    ],
    "utxo_profit_share": [
        "Onchain Lab Stage 4 Metrics Specification",
    ],
}

_DEFAULT_CITATIONS: list[str] = [
    "Onchain Lab Stage 4 Metrics Specification",
]

_SQL_TEMPLATE = (
    "SELECT date, {metric}\n"
    "FROM read_parquet('data/metrics/daily/metrics.parquet')\n"
    "WHERE date BETWEEN '2024-01-01' AND '2024-01-10';"
)


@dataclass
class DocGenerationSummary:
    written_paths: list[Path]
    index_path: Path
    mkdocs_path: Path


def _title(name: str) -> str:
    return name.replace("_", " ").title()


def _formula_text(metric: str) -> str:
    return _FORMULA_TEXT.get(metric, "Formula documentation pending.")


def _citations(metric: str) -> list[str]:
    return _CITATIONS.get(metric, _DEFAULT_CITATIONS)


def _render_badge_table(product) -> str:
    return (
        "| Field | Value |\n"
        "| --- | --- |\n"
        f"| Status | {product.status} |\n"
        f"| Version | {product.version} |\n"
        f"| Coverage % | {product.coverage_pct:.2f} |\n"
        f"| Null Ratio | {product.null_ratio:.4f} |\n"
        f"| Deflated Sharpe | {product.deflated_sharpe_score:.2f} |\n"
        f"| Golden Checks Passed | {product.golden_checks_passed} |\n"
        f"| No Lookahead | {product.no_lookahead} |\n"
        f"| Reproducible | {product.reproducible} |\n"
    )


def _render_provenance(product) -> str:
    return (
        "- UTXO snapshot commit: {utxo}\n"
        "- Price root commit: {price}\n"
        "- Formulas version: {formulas}"
    ).format(
        utxo=product.utxo_snapshot_commit,
        price=product.price_root_commit,
        formulas=product.formulas_version,
    )


def _find_latest_golden_image(images_root: Path, metric: str) -> Optional[Path]:
    candidates = sorted(images_root.glob(f"{metric}_*_golden.png"))
    if candidates:
        return candidates[-1]
    legacy = images_root / f"{metric}_golden.png"
    return legacy if legacy.exists() else None


def _render_metric_page(
    definition: MetricDefinition,
    destination: Path,
    docs_root: Path,
    images_root: Path,
) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    badge = definition.badge
    image_path = _find_latest_golden_image(images_root, definition.name)
    golden_section: list[str]
    if image_path is not None and image_path.exists():
        try:
            rel_path = image_path.relative_to(destination.parent)
        except ValueError:
            try:
                rel_path = Path(os.path.relpath(image_path, destination.parent))
            except ValueError:
                rel_path = image_path
        golden_section = [
            f"![Golden day chart]({rel_path.as_posix()})",
            "",
            f"Image source: {rel_path.as_posix()}",
        ]
    else:
        golden_section = [
            (
                "Capture the golden day chart or table and store it at "
                f"`{(images_root / f'{definition.name}_<YYYY-MM-DD>_golden.png').relative_to(docs_root).as_posix()}` "
                "to replace this note."
            )
        ]
    lines = [
        f"# {_title(definition.name)}",
        "",
        "## Definition",
        definition.description or "Description pending upstream documentation.",
        "",
        "## Formula",
        "```",
        _formula_text(definition.name),
        "```",
        "",
        "## QA Badge",
        _render_badge_table(badge),
        "",
        "## Provenance",
        _render_provenance(badge),
        "",
        "## Known Caveats",
        "- Pending deeper statistical audit for extreme market regimes.",
        "",
        "## Reproduction",
        "```sql",
        _SQL_TEMPLATE.format(metric=definition.name),
        "```",
        "",
        "## Golden Day Snapshot",
    ]
    lines.extend(golden_section)
    lines.extend([
        "",
        "## Citations",
    ])
    for citation in _citations(definition.name):
        lines.append(f"- {citation}")
    content = "\n".join(lines).strip() + "\n"
    destination.write_text(content, encoding="utf-8")
    return destination


def _write_index(
    definitions: Iterable[MetricDefinition], docs_root: Path, doc_paths: dict[str, Path]
) -> Path:
    docs_root.mkdir(parents=True, exist_ok=True)
    rows = [
        "| Metric | Status | Coverage % | Documentation |",
        "| --- | --- | --- | --- |",
    ]
    for definition in sorted(definitions, key=lambda d: d.name):
        badge = definition.badge
        target = doc_paths[definition.name]
        try:
            rel_path = target.relative_to(docs_root)
        except ValueError:
            rel_path = Path("metrics") / f"{definition.name}.md"
        link = f"[View]({rel_path.as_posix()})"
        rows.append(
            f"| {_title(definition.name)} | {badge.status} | {badge.coverage_pct:.2f} | {link} |"
        )
    body = "\n".join(rows)
    text = (
        "# Metric Registry\n\n"
        "This catalog is generated from the Stage 6 transparency pipeline. Each metric links "
        "to a badge report and reproducibility evidence.\n\n"
        "Use `onchain-metrics show <metric> --date YYYY-MM-DD` to inspect the exact upstream "
        "records contributing to any metric value. Append `--json` for machine-readable output.\n\n"
        f"Generated on {datetime.utcnow().isoformat()}Z.\n\n"
        f"{body}\n"
    )
    index_path = docs_root / "index.md"
    index_path.write_text(text, encoding="utf-8")
    return index_path


def _write_mkdocs_config(
    definitions: Iterable[MetricDefinition],
    mkdocs_path: Path,
    doc_paths: dict[str, Path],
    docs_root: Path,
) -> Path:
    nav_lines: list[str] = []
    docs_root = docs_root.resolve()
    for definition in sorted(definitions, key=lambda d: d.name):
        target = doc_paths[definition.name]
        try:
            rel = target.relative_to(docs_root)
        except ValueError:
            rel = target
        nav_lines.append(f"    - {_title(definition.name)}: {rel.as_posix()}")
    nav_entries = "\n".join(nav_lines)
    config = (
        "site_name: Onchain Lab Metrics\n"
        "docs_dir: docs\n"
        "theme:\n"
        "  name: readthedocs\n"
        "nav:\n"
        "  - Overview: index.md\n"
        "  - Metrics:\n"
        f"{nav_entries}\n"
    )
    mkdocs_path.write_text(config, encoding="utf-8")
    return mkdocs_path


def generate_metric_docs(
    definitions: Iterable[MetricDefinition],
    docs_root: Path,
    mkdocs_path: Path,
    *,
    images_root: Path | None = None,
) -> DocGenerationSummary:
    docs_root = docs_root.resolve()
    metrics_root = docs_root / "metrics"
    metrics_root.mkdir(parents=True, exist_ok=True)
    images_root = (images_root or (docs_root / "images")).resolve()
    images_root.mkdir(parents=True, exist_ok=True)
    keep_file = images_root / ".gitkeep"
    if not keep_file.exists():
        keep_file.write_text("", encoding="utf-8")
    written: list[Path] = []
    doc_paths: dict[str, Path] = {}
    for definition in definitions:
        target = Path(definition.docs_path) if definition.docs_path else metrics_root / f"{definition.name}.md"
        if not target.is_absolute():
            target = docs_root / target
        target = target.resolve()
        doc_paths[definition.name] = target
        written.append(_render_metric_page(definition, target, docs_root, images_root))
    index_path = _write_index(definitions, docs_root, doc_paths)
    mkdocs_out = _write_mkdocs_config(definitions, mkdocs_path, doc_paths, docs_root)
    return DocGenerationSummary(written_paths=written, index_path=index_path, mkdocs_path=mkdocs_out)


__all__ = ["DocGenerationSummary", "generate_metric_docs"]
