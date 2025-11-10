# Utxo Profit Share

## Definition
Share of outstanding supply currently in profit relative to cost basis.

## Formula
```
UTXO Profit Share = share of outstanding supply with cost_basis < price_close.
```

## QA Badge
| Field | Value |
| --- | --- |
| Status | verified |
| Version | 1.0.0 |
| Coverage % | 100.00 |
| Null Ratio | 0.0000 |
| Deflated Sharpe | 0.21 |
| Golden Checks Passed | True |
| No Lookahead | True |
| Reproducible | True |


## Provenance
- UTXO snapshot commit: utxo-snapshots@0000000
- Price root commit: price-oracle@0000000
- Formulas version: metrics-formulas@v0.1.0

## Known Caveats
- Pending deeper statistical audit for extreme market regimes.

## Reproduction
```sql
SELECT date, utxo_profit_share
FROM read_parquet('data/metrics/daily/metrics.parquet')
WHERE date BETWEEN '2024-01-01' AND '2024-01-10';
```

## Golden Day Snapshot
Capture the golden day chart or table and store it at `images/utxo_profit_share_golden.png` to replace this note.

## Citations
- Onchain Lab Stage 4 Metrics Specification
