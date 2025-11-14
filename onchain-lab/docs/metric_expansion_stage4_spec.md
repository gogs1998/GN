# Metric Expansion Initiative — Stage 4 Specification

## 1. Purpose
Define the schema, formulas, and pipeline changes required to extend the Bitcoin daily metrics catalog with exchange flow, SOPR derivative, whale balance, and realized cap segmentation features. This spec is the implementation contract for Stage 4 of the Metric Expansion Initiative.

## 2. Scope
- Update the Stage 4 metrics engine (`src/metrics`) to compute the new indicators on daily cadence.
- Persist the new columns in `data/metrics/daily/metrics.parquet` with deterministic naming and lineage metadata.
- Enrich upstream UTXO feature extraction to provide exchange/entity tagging and cohort lookups required by the new metrics.
- Instrument QA controls covering formulas, tolerances, and regression checks.

Out of scope for Stage 4:
- Stage 5 modeling retrains (handled in the next stage).
- Public API or dashboard exposure.
- Non-Bitcoin chains.

## 3. Metric Catalog Additions (v1)
| Metric Name | Description | Formula (High Level) | Data Inputs |
|-------------|-------------|-----------------------|-------------|
| `exchange_net_flow_btc` | Net BTC transferred to exchanges per day. | `inflow_btc - outflow_btc` aggregated across tagged exchange entities. | Daily UTXO spend events tagged as exchange; entity lookup table; BTC conversion. |
| `exchange_net_flow_usd` | Net USD value transferred to exchanges per day. | `(inflow_sats - outflow_sats) * price_close / 1e8`. | Same as above plus price close. |
| `exchange_supply_pct` | Share of circulating supply held by exchange entities. | `exchange_balance_sats / supply_sats`. | Snapshot balances by entity; total supply. |
| `exchange_dormancy` | Average age of coins deposited to exchanges (days). | `weighted_avg(age_days, deposit_sats)` for exchange inflows. | Spend events with age metadata. |
| `sopr_entity_adjusted` | SOPR adjusted for entity consolidation heuristics. | `sum(realized_profit_usd_adj) / sum(realized_loss_usd_adj)`. | Entity-level UTXO spends with adjusted clustering. |
| `sopr_long_short_delta` | Differential between long- and short-term holder SOPR. | `sopr_long_term - sopr_short_term`. | SOPR computed separately for age buckets (`>=155d`, `<155d`). |
| `spent_profit_delta_usd` | Net USD profit realized on-chain (profits minus losses). | `realized_profit_usd - realized_loss_usd`. | Same as existing realized metrics. |
| `whale_supply_btc` | BTC held by addresses/entities >= 1,000 BTC. | Sum of balances where entity balance >=1,000 BTC. | Snapshot balances with entity clustering. |
| `whale_dormancy` | Average age of whale-held coins. | `weighted_avg(age_days, whale_balance_sats)`. | Snapshot with age metadata. |
| `whale_realized_pl_usd` | Realized profit minus loss for whales. | Whale entity spend profit minus loss. | UTXO spend events tagged as whale. |
| `realized_cap_0_1d` ... `realized_cap_1y_plus` | Realized cap segmented by age cohorts. | Sum cost basis USD for spends within cohort. | Snapshot/ spend data by age bucket. |
| `realized_cap_exchange` | Realized cap attributable to exchange entities. | Sum cost basis USD for exchange entities. | Exchange entity tagging. |
| `realized_cap_whale` | Realized cap attributable to whale entities. | Sum cost basis USD for whale entities. | Whale entity tagging. |

## 4. Data Dependencies
1. **Entity tagging**
   - Maintain or ingest an `entity_lookup.parquet` mapping address clusters to entity types (`exchange`, `whale`, etc.).
   - Extend UTXO enrichment job to join entity type on each spend/create event.
2. **Age buckets**
   - Reuse existing lifespan band definitions; ensure they are applied consistently to realized cap segmentation.
3. **Balance snapshots**
   - Daily snapshots must include entity type and cumulative balance for exchange/whale cohorts.
4. **Price data**
   - Continue using Binance close price as USD conversion; ensure joins remain on UTC day boundary.

## 5. Pipeline Changes
- **UTXO enrichment layer** (`src/utxo`)
  - Add optional foreign key to `entity_lookup` when producing `created.parquet` and `spent.parquet`.
  - Emit new columns: `entity_type`, `entity_id`, `age_days` (existing), `cluster_balance_sats` (for whales).
- **Metrics engine** (`src/metrics`)
  - Update configuration with new metric definitions and QA tolerances.
  - Implement calculator modules for:
    - Exchange flows (aggregating spend events into inflow/outflow).
    - SOPR derivatives (entity-adjusted, cohort delta).
    - Whale balance stack (snapshot-based aggregations).
    - Realized cap segmentation (age/entity-based sums).
  - Ensure outputs append to existing parquet with schema version bump (`pipeline_version = "metrics.v2"`).
  - Backfill historical metrics by re-running engine across full date range once calculations pass QA.
- **Lineage metadata**
  - Extend `lineage_id` to encode metric version (`metrics.v2`) and entity tagging version.
  - Document dependencies in QA manifests.

## 6. QA & Validation Plan
- Golden-day checks for each new metric (at least 3 reference dates with expected values and tolerance <= 2%).
- Drift alerts comparing previous metric version vs new version to detect regressions.
- Sanity rules:
  - `exchange_supply_pct` in [0,1].
  - `whale_supply_btc <= supply_btc`.
  - SOPR metrics finite, no negative denominators.
- Unit tests covering aggregation edge cases (no exchange tags, partial data, zero denominators).
- Integration test pipeline run using fixture snapshots to validate parquet schema and lineage metadata.

## 7. Backfill & Deployment
1. Run metrics engine in staging mode to produce `metrics_v2.parquet`.
2. Compare aggregates with v1 to ensure existing metrics remain unchanged within tolerance.
3. Promote v2 artifacts to `data/metrics/daily/metrics.parquet` once QA passes.
4. Archive v1 parquet for reproducibility (`data/metrics/daily/archive/metrics_v1.parquet`).
5. Update documentation and changelog.

## 8. Modeling Impact Preview (Stage 5)
- Feature allowlist must include new columns for frame builder.
- Retrain baseline models with updated dataset after backfill.
- Capture feature importance / SHAP for new metrics to confirm predictive contribution.
- Update model QA thresholds to account for new feature distributions.

## 9. Deliverables
- Updated configs (`config/metrics.yaml`, `config/utxo.yaml` if needed).
- New entity lookup ingestion script/notebook (if not already present).
- Code changes in `src/utxo` and `src/metrics` implementing calculations.
- New tests under `tests/metrics/` covering calculators and QA.
- QA manifest documenting golden-day expectations.
- Backfill command output with audit log.

## 10. Timeline (Target)
| Week | Milestone |
|------|-----------|
| W1 | Finalize formulas, entity lookup schema, and QA tolerances (this spec). |
| W2 | Implement UTXO enrichment + entity tagging integration. |
| W3 | Implement metrics calculators + unit tests. |
| W3 | Backfill historical data and execute QA. |
| W4 | Publish v2 metrics, handoff to Stage 5 modeling. |

## 11. Open Questions
- Source of entity tagging: internal clustering vs third-party dataset? Need reproducible source reference.
- Do we expose gross exchange inflow/outflow separately or only net? (Current plan: only net; backlog item for gross).
- Whale threshold configurable? Default to 1,000 BTC but may expose config parameter.
- Storage footprint impact negligible but confirm parquet partition size post-backfill.

---
Prepared by Copilot on 2025-11-11. Submit feedback via Stage 3 review thread before implementation begins.
