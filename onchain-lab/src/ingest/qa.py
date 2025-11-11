from __future__ import annotations

import glob
import json
import string
from dataclasses import dataclass
from datetime import date, datetime, time, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import duckdb

from .config import IngestConfig, load_config


class QAError(RuntimeError):
    """Raised when QA checks fail."""


@dataclass(frozen=True)
class GoldenReference:
    blocks: int
    transactions: int
    coinbase_sats: int
    tolerance_pct: Optional[float] = None


def _load_golden_refs(path: Path) -> Dict[date, GoldenReference]:
    if not path.exists():
        raise QAError(f"Golden reference file missing: {path}")
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    references: Dict[date, GoldenReference] = {}
    for key, value in raw.items():
        try:
            parsed_date = datetime.strptime(key, "%Y-%m-%d").date()
        except ValueError as exc:
            raise QAError(f"Invalid golden reference date: {key}") from exc
        tolerance = value.get("tolerance_pct")
        references[parsed_date] = GoldenReference(
            blocks=int(value["blocks"]),
            transactions=int(value["transactions"]),
            coinbase_sats=int(value["coinbase_sats"]),
            tolerance_pct=float(tolerance) if tolerance is not None else None,
        )
    return references


def _day_bounds(target: date) -> tuple[datetime, datetime]:
    start = datetime.combine(target, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _duckdb_connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=":memory:")


def _format_pct(delta: float) -> str:
    return f"{delta:.4f}%"


def _template_glob(template: str) -> str:
    formatter = string.Formatter()
    parts: List[str] = []
    for literal, field_name, _, _ in formatter.parse(template):
        parts.append(literal)
        if field_name is not None:
            parts.append("*")
    return "".join(parts)


def _partition_files(config: IngestConfig, key: str) -> List[str]:
    template = config.partitions[key]
    fragment = _template_glob(template)
    base = Path(fragment)
    if not base.is_absolute():
        base = config.data_root / base

    if str(base).lower().endswith(".parquet"):
        pattern = str(base)
    else:
        pattern = str(base / "**" / "*.parquet")

    matches = glob.glob(pattern, recursive=True)
    return sorted(matches)


def _format_timestamp(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc).isoformat()
    normalized = normalized.replace("T", " ")
    return normalized.replace("'", "''")


def _register_view(
    connection: duckdb.DuckDBPyConnection,
    *,
    name: str,
    files: List[str],
    start: datetime | None = None,
    end: datetime | None = None,
) -> None:
    relation = connection.read_parquet(files)
    if start is not None and end is not None:
        start_text = _format_timestamp(start)
        end_text = _format_timestamp(end)
        predicate = (
            f"time_utc >= TIMESTAMPTZ '{start_text}' "
            f"AND time_utc < TIMESTAMPTZ '{end_text}'"
        )
        relation = relation.filter(predicate)
    relation.create_view(name, replace=True)


def run_golden_day_checks(
    *,
    target: date,
    config: IngestConfig | None = None,
    references_path: Path | None = None,
) -> Dict[str, int]:
    cfg = config or load_config()
    data_root = cfg.data_root
    if not data_root.exists():
        raise QAError(f"Data root does not exist: {data_root}")

    ref_path = references_path or Path(__file__).with_name("golden_refs.json")
    references = _load_golden_refs(ref_path)
    if target not in references:
        raise QAError(f"No golden reference stored for {target.isoformat()}")
    reference = references[target]

    start, end = _day_bounds(target)
    con = _duckdb_connect()
    block_files = _partition_files(cfg, "blocks")
    tx_files = _partition_files(cfg, "transactions")
    txin_files = _partition_files(cfg, "txin")
    txout_files = _partition_files(cfg, "txout")

    if not block_files or not tx_files or not txin_files or not txout_files:
        raise QAError("Parquet datasets incomplete for QA check.")

    metrics = None

    try:
        _register_view(con, name="day_blocks", files=block_files, start=start, end=end)
        _register_view(
            con, name="day_transactions", files=tx_files, start=start, end=end
        )
        _register_view(con, name="all_txin", files=txin_files)
        _register_view(con, name="all_txout", files=txout_files)

        con.execute(
            """
            CREATE OR REPLACE VIEW coinbase_txids AS
            SELECT DISTINCT t.txid
            FROM day_transactions AS t
            INNER JOIN all_txin AS vin
                ON t.txid = vin.txid
            WHERE vin.coinbase = TRUE;
            """
        )

        con.execute(
            """
            CREATE OR REPLACE VIEW day_coinbase AS
            SELECT o.value_sats
            FROM all_txout AS o
            INNER JOIN coinbase_txids AS c ON o.txid = c.txid;
            """
        )

        metrics = con.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM day_blocks) AS block_count,
                (SELECT COUNT(*) FROM day_transactions) AS tx_count,
                COALESCE((SELECT SUM(value_sats) FROM day_coinbase), 0) AS coinbase_sats
            """
        ).fetchone()
    finally:
        con.close()

    if metrics is None:
        raise QAError("Failed to compute golden day metrics.")

    block_count, tx_count, coinbase_sats = metrics

    effective_tolerance = reference.tolerance_pct or cfg.qa.tolerance_pct

    deltas = {
        "blocks": _delta_pct(block_count, reference.blocks),
        "transactions": _delta_pct(tx_count, reference.transactions),
        "coinbase_sats": _delta_pct(coinbase_sats, reference.coinbase_sats),
    }

    violations = [name for name, delta in deltas.items() if delta > effective_tolerance]

    if violations:
        formatted = ", ".join(
            f"{name} Δ={_format_pct(deltas[name])}" for name in violations
        )
        raise QAError(
            f"Golden day {target.isoformat()} outside tolerance ({effective_tolerance}%): {formatted}"
        )

    return {
        "blocks": int(block_count),
        "transactions": int(tx_count),
        "coinbase_sats": int(coinbase_sats),
    }


def _delta_pct(measured: int, reference: int) -> float:
    if reference == 0:
        return 0.0 if measured == 0 else 100.0
    return abs(measured - reference) / reference * 100.0


def verify_date(arg: str, *, config: IngestConfig | None = None) -> Dict[str, int]:
    target_date = datetime.strptime(arg, "%Y-%m-%d").date()
    return run_golden_day_checks(target=target_date, config=config)
