from datetime import datetime, timezone, timedelta
from pathlib import Path
import pandas as pd
import pyarrow as pa

from src.utxo.builder import LifecycleBuilder
from src.utxo.snapshots import SnapshotBuilder
from src.utxo.datasets import CREATED_SCHEMA
import tempfile
from tests.utxo import conftest

def main():
    tmpdir = Path(tempfile.mkdtemp())
    config = conftest.sample_config.__wrapped__(tmpdir)
    builder = LifecycleBuilder(config)
    build_result = builder.build(persist=False)
    snapshot_builder = SnapshotBuilder(config)
    baseline = snapshot_builder.build(build_result.artifacts.created, build_result.artifacts.spent, persist=False)
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
    print("extended created_date unique", extended["created_date"].unique())
    print("created_table created_date unique", created_table.to_pandas()["created_date"].unique())
    created_pdf = created_table.to_pandas()
    print("created_table created_time", created_pdf["created_time"])
    print("created_table entity_id", created_pdf["entity_id"])
    created_pdf = created_table.to_pandas()
    spent_pdf = build_result.artifacts.spent.to_pandas()
    spend_map = (
        spent_pdf.set_index(["source_txid", "source_vout"])["spend_time"].to_dict()
        if not spent_pdf.empty
        else {}
    )
    created_pdf["actual_spend_time"] = created_pdf.apply(
        lambda row: spend_map.get((row["txid"], row["vout"]), row.get("spend_time_hint")),
        axis=1,
    )
    created_pdf["actual_spend_time"] = pd.to_datetime(created_pdf["actual_spend_time"], utc=True)
    created_pdf["created_time"] = pd.to_datetime(created_pdf["created_time"], utc=True)
    created_pdf["created_date"] = pd.to_datetime(created_pdf["created_date"]).dt.date
    print("actual_spend_time", created_pdf["actual_spend_time"])
    from zoneinfo import ZoneInfo

    zone = ZoneInfo("UTC")
    close_time = snapshot_builder._config.snapshot.close_time()
    for day in [datetime(2024, 1, 1).date(), datetime(2024, 1, 2).date()]:
        closing_local = datetime.combine(day, close_time, tzinfo=zone) + timedelta(days=1)
        boundary_utc = closing_local.astimezone(timezone.utc)
        active_mask = (created_pdf["created_time"] < boundary_utc) & (
            created_pdf["actual_spend_time"].isna()
            | (created_pdf["actual_spend_time"] > boundary_utc)
        )
        print("day", day, "boundary", boundary_utc, "active mask", active_mask.tolist())
    snapshots = snapshot_builder.build(created_table, build_result.artifacts.spent, persist=False)
    print("snapshot keys", list(snapshots.keys()))
    jan02 = snapshots[datetime(2024, 1, 2).date()].to_pandas()
    jan01 = snapshots[datetime(2024, 1, 1).date()].to_pandas()
    print("baseline Jan02 sum", baseline[datetime(2024, 1, 2).date()].to_pandas()["output_count"].sum())
    print("Jan02 sum", jan02["output_count"].sum())
    print(jan02)
    print("Jan01 sum", jan01["output_count"].sum())
    print("active rows", jan02.shape[0])


if __name__ == "__main__":
    main()
