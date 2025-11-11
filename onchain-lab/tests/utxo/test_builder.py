from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.utxo.builder import LifecycleBuilder


def test_builder_constructs_created_and_spent(sample_config):
    builder = LifecycleBuilder(sample_config)
    result = builder.build(persist=False)

    created = result.artifacts.created
    spent = result.artifacts.spent

    assert created.num_rows == 2
    assert spent.num_rows == 1

    created_df = created.to_pandas()
    spent_df = spent.to_pandas()

    addr1_row = created_df[created_df["vout"] == 0].iloc[0]
    assert addr1_row["creation_price_close"] == pytest.approx(30_500.0)
    assert not bool(addr1_row["is_spent"])

    addr2_row = created_df[created_df["vout"] == 1].iloc[0]
    assert bool(addr2_row["is_spent"])
    assert addr2_row["spend_txid_hint"] == "txB"

    spent_row = spent_df.iloc[0]
    assert spent_row["source_txid"] == "txA"
    assert spent_row["source_vout"] == 1
    assert spent_row["spend_txid"] == "txB"
    assert spent_row["holding_days"] > 0
    assert spent_row["realized_value_usd"] == pytest.approx((50_000_000 / 1e8) * 32_000.0)
    assert not bool(spent_row["is_orphan"])
