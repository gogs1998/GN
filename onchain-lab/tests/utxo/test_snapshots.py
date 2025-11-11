from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import pyarrow as pa

import pytest

from src.utxo.builder import LifecycleBuilder
from src.utxo.snapshots import SnapshotBuilder
from src.utxo.datasets import CREATED_SCHEMA, SPENT_SCHEMA


def test_snapshot_builder_groups_active_outputs(sample_config):
    builder = LifecycleBuilder(sample_config)
    build_result = builder.build(persist=False)

    snapshot_builder = SnapshotBuilder(sample_config)
    snapshots = snapshot_builder.build(
        build_result.artifacts.created, build_result.artifacts.spent, persist=False
    )

    assert date(2024, 1, 1) in snapshots
    assert date(2024, 1, 2) in snapshots

    jan02 = snapshots[date(2024, 1, 2)].to_pandas()
    assert jan02["output_count"].sum() == 1
    assert jan02["balance_sats"].sum() == 100_000_000
    assert jan02.iloc[0]["market_value_usd"] == pytest.approx(32_000.0)
    assert jan02.iloc[0]["cost_basis_usd"] == pytest.approx(30_500.0)
    assert jan02.iloc[0]["group_key"] == "addr1"


def test_snapshot_builder_excludes_boundary_creations(sample_config):
    builder = LifecycleBuilder(sample_config)
    build_result = builder.build(persist=False)

    snapshot_builder = SnapshotBuilder(sample_config)
    baseline = snapshot_builder.build(
        build_result.artifacts.created, build_result.artifacts.spent, persist=False
    )
    baseline_jan01 = baseline[date(2024, 1, 1)].to_pandas()["output_count"].sum()
    baseline_jan02 = baseline[date(2024, 1, 2)].to_pandas()["output_count"].sum()

    created_df = build_result.artifacts.created.to_pandas()
    boundary_time = datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc)
    boundary_row = created_df.iloc[0].copy()
    boundary_row["txid"] = "boundary_tx"
    boundary_row["vout"] = 42
    boundary_row["value_sats"] = 200_000_000
    boundary_row["addresses"] = ["boundary_addr"]
    boundary_row["script_type"] = "p2pkh"
    boundary_row["created_time"] = boundary_time
    boundary_row["created_date"] = boundary_time.date()
    boundary_row["creation_price_close"] = 32_000.0
    boundary_row["creation_price_ts"] = boundary_time
    boundary_row["pipeline_version"] = created_df.iloc[0]["pipeline_version"]
    boundary_row["lineage_id"] = created_df.iloc[0]["lineage_id"]

    extended = pd.concat([created_df, pd.DataFrame([boundary_row])], ignore_index=True)
    created_table = pa.Table.from_pandas(extended, schema=CREATED_SCHEMA, preserve_index=False)

    snapshots = snapshot_builder.build(
        created_table,
        build_result.artifacts.spent,
        persist=False,
    )

    jan01 = snapshots[date(2024, 1, 1)].to_pandas()
    jan02 = snapshots[date(2024, 1, 2)].to_pandas()

    assert jan01["output_count"].sum() == baseline_jan01
    assert jan02["output_count"].sum() == baseline_jan02 + 1


def test_snapshot_builder_excludes_spends_at_boundary(sample_config):
    builder = LifecycleBuilder(sample_config)
    build_result = builder.build(persist=False)

    snapshot_builder = SnapshotBuilder(sample_config)
    baseline = snapshot_builder.build(
        build_result.artifacts.created, build_result.artifacts.spent, persist=False
    )
    baseline_jan01 = baseline[date(2024, 1, 1)].to_pandas()["output_count"].sum()

    created_df = build_result.artifacts.created.to_pandas()
    spent_df = build_result.artifacts.spent.to_pandas()

    boundary_time = pd.Timestamp("2024-01-02T00:00:00Z")
    creation_time = pd.Timestamp("2024-01-01T12:00:00Z")

    template_created = created_df.iloc[0].copy()
    template_spent = spent_df.iloc[0].copy()

    boundary_created = template_created.copy()
    boundary_created["txid"] = "boundary_spend_tx"
    boundary_created["vout"] = 99
    boundary_created["value_sats"] = 75_000_000
    boundary_created["addresses"] = ["boundary_spend_addr"]
    boundary_created["script_type"] = "p2pkh"
    boundary_created["created_time"] = creation_time
    boundary_created["created_date"] = creation_time.date()
    boundary_created["creation_price_close"] = template_created["creation_price_close"]
    boundary_created["creation_price_ts"] = template_created["creation_price_ts"]
    boundary_created["spend_txid_hint"] = "boundary_spend_txid"
    boundary_created["spend_height_hint"] = template_created.get("spend_height_hint")
    boundary_created["spend_time_hint"] = boundary_time
    boundary_created["is_spent"] = True
    boundary_created["pipeline_version"] = template_created["pipeline_version"]
    boundary_created["lineage_id"] = "boundary-spend::99"

    boundary_spent = template_spent.copy()
    boundary_spent["source_txid"] = boundary_created["txid"]
    boundary_spent["source_vout"] = boundary_created["vout"]
    boundary_spent["spend_txid"] = "boundary_spend_txid"
    boundary_spent["value_sats"] = boundary_created["value_sats"]
    boundary_spent["created_time"] = creation_time
    boundary_spent["spend_time"] = boundary_time
    holding_seconds = (boundary_time - creation_time).total_seconds()
    boundary_spent["holding_seconds"] = holding_seconds
    boundary_spent["holding_days"] = holding_seconds / 86400.0
    boundary_spent["creation_price_close"] = float(boundary_created["creation_price_close"])
    boundary_spent["creation_price_ts"] = boundary_created["creation_price_ts"]
    boundary_spent["creation_price_source"] = template_spent["creation_price_source"]
    boundary_spent["spend_height"] = template_spent["spend_height"]
    boundary_spent["spend_price_close"] = template_spent["spend_price_close"]
    boundary_spent["spend_price_ts"] = template_spent["spend_price_ts"]
    boundary_spent["spend_price_source"] = template_spent["spend_price_source"]
    value_btc = boundary_spent["value_sats"] / 1e8
    spend_price = float(boundary_spent["spend_price_close"])
    creation_price = float(boundary_spent["creation_price_close"])
    boundary_spent["realized_value_usd"] = value_btc * spend_price
    boundary_spent["realized_profit_usd"] = value_btc * (spend_price - creation_price)
    boundary_spent["is_orphan"] = False
    boundary_spent["lineage_id"] = boundary_created["lineage_id"]
    boundary_spent["pipeline_version"] = template_spent["pipeline_version"]

    extended_created = pd.concat(
        [created_df, pd.DataFrame([boundary_created])], ignore_index=True
    )
    extended_spent = pd.concat([spent_df, pd.DataFrame([boundary_spent])], ignore_index=True)

    created_table = pa.Table.from_pandas(
        extended_created, schema=CREATED_SCHEMA, preserve_index=False
    )
    spent_table = pa.Table.from_pandas(
        extended_spent, schema=SPENT_SCHEMA, preserve_index=False
    )

    snapshots = snapshot_builder.build(created_table, spent_table, persist=False)
    jan01 = snapshots[date(2024, 1, 1)].to_pandas()

    assert jan01["output_count"].sum() == baseline_jan01
    assert "boundary_spend_addr" not in jan01["group_key"].tolist()
