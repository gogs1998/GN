from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timezone, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

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
    blocks_path = (data_root / "blocks").glob("**/*.parquet")
    tx_path = (data_root / "tx").glob("**/*.parquet")
    txin_path = (data_root / "txin").glob("**/*.parquet")
    txout_path = (data_root / "txout").glob("**/*.parquet")

    def _expand(paths: Iterable[Path]) -> List[str]:
        return [str(p) for p in paths]

    block_files = _expand(blocks_path)
    tx_files = _expand(tx_path)
    txin_files = _expand(txin_path)
    txout_files = _expand(txout_path)

    if not block_files or not tx_files or not txin_files or not txout_files:
        raise QAError("Parquet datasets incomplete for QA check.")

    metrics = None

    try:
        con.execute(
            """
            CREATE OR REPLACE VIEW day_blocks AS
            SELECT * FROM read_parquet($1)
            WHERE time_utc >= $2 AND time_utc < $3;
            """,
            [block_files, start, end],
        )

        con.execute(
            """
            CREATE OR REPLACE VIEW day_transactions AS
            SELECT * FROM read_parquet($1)
            WHERE time_utc >= $2 AND time_utc < $3;
            """,
            [tx_files, start, end],
        )

        con.execute(
            """
            CREATE OR REPLACE VIEW coinbase_txids AS
            SELECT DISTINCT t.txid
            FROM day_transactions AS t
            INNER JOIN read_parquet($1) AS vin
                ON t.txid = vin.txid
            WHERE vin.coinbase = TRUE;
            """,
            [txin_files],
        )

        con.execute(
            """
            CREATE OR REPLACE VIEW day_coinbase AS
            SELECT o.value_sats
            FROM read_parquet($1) AS o
            INNER JOIN coinbase_txids AS c ON o.txid = c.txid;
            """,
            [txout_files],
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
