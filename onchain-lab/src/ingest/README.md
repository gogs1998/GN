# Ingest Module Overview

## Purpose
Establish a verifiable, reproducible pipeline that transforms raw Bitcoin blockchain data into structured artifacts suitable for downstream UTXO normalization, metric computation, and ML experimentation.

## Scope (MVP)
- Support Bitcoin mainnet data (blocks, transactions, UTXOs) from trusted archival sources.
- Provide daily batch ingestion with deterministic replay and audit logs.
- Persist raw artifacts to `data/raw/` and curated Parquet outputs to `data/parquet/` with schema versioning.
- Emit QA manifests documenting source checksums, record counts, and validation status.

## Data Sources
- Primary: Bitcoin Core full node with transaction index enabled (`-txindex`).
- Secondary: Public archival APIs (Blockstream, MemPool.space) for redundancy and checksum comparison.
- Metadata: Chain state snapshots (e.g., UTXO set dumps), exchange rate feeds for USD normalization.

## Pipeline Stages
1. **Extraction**: Pull blocks and transactions via RPC and/or SQL export, capturing raw JSON + binary payloads.
2. **Normalization**: Convert raw payloads into tabular schemas (blocks, transactions, inputs, outputs) with explicit typing and timestamp alignment.
3. **Validation**: Run block height continuity checks, transaction count reconciliations, Merkle root verification, and duplicate detection.
4. **Publishing**: Write partitioned Parquet files, register schema versions, and update data catalog manifests.
5. **Lineage Logging**: Record job parameters, source hashes, record counts, and downstream consumers in an append-only ledger.

## Storage Layout
- `data/raw/bitcoin/<yyyy-mm-dd>/blocks/*.json`
- `data/raw/bitcoin/<yyyy-mm-dd>/transactions/*.json`
- `data/parquet/bitcoin/blocks/part_dt=<yyyy-mm-dd>/*.parquet`
- `data/parquet/bitcoin/transactions/part_dt=<yyyy-mm-dd>/*.parquet`
- `data/parquet/bitcoin/utxo/part_dt=<yyyy-mm-dd>/*.parquet`
- `data/parquet/bitcoin/metadata/schema_versions.json`

## Interfaces & Artifacts
- **Config**: YAML files capturing data source endpoints, batch size, retry policies.
- **Manifests**: JSON documents logged per run with checksums, counts, and validation results.
- **Metrics Hooks**: Emit ingestion latency, throughput, and failure rates to observability stack.

## Operational Requirements
- Support deterministic replays for specific block ranges.
- Enforce idempotent writes via job run IDs and object locks.
- Alert on missing blocks, validation failures, or schema drift.
- Maintain retention policy: raw data (365 days), curated Parquet (full history).

## QA Checklist
- Compare block hash sequences against reference node.
- Reconcile transaction counts between raw and normalized tables.
- Validate UTXO snapshot completeness versus expected supply.
- Spot-check random blocks for Merkle root fidelity.
- Record QA sign-off in shared ledger before downstream consumption.

## Backlog (Phase 1)
- Implement checksum registry for raw and normalized artifacts.
- Build deterministic job scheduler (pref. Airflow or Dagster) integration.
- Add exchange rate enrichment hook (BTC/USD) for same-day normalization.
- Automate data catalog entry generation per Parquet dataset.
- Prototype public dataset export for free tier (last 30 days).

## Open Questions
- Preferred data replication strategy: ZFS snapshots vs. cloud object storage versioning?
- Hosting strategy for archival full node (self-managed vs. managed providers)?
- Compression format standard (Snappy vs. ZSTD) for Parquet outputs?
- SLA targets for ingestion completion after block finality?

## Next Steps
- Confirm orchestration platform and infrastructure footprint.
- Finalize schema definitions shared with `src/utxo` module.
- Draft detailed runbook and incident response checklist.
