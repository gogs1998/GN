# Asopr

## Definition
Adjusted SOPR excluding UTXOs with holding period under one day.

## Formula
```
aSOPR = adjusted_realized_value / adjusted_cost_basis for spends held ≥ 1 hour.
```

## QA Badge
| Field | Value |
| --- | --- |
| Status | verified |
| Version | 1.0.0 |
| Coverage % | 99.00 |
| Null Ratio | 0.0000 |
| Deflated Sharpe | 0.40 |
| Golden Checks Passed | True |
| No Lookahead | True |
| Reproducible | True |


## Provenance
- UTXO snapshot commit: utxo-spent@0000000
- Price root commit: price-oracle@0000000
- Formulas version: metrics-formulas@v0.1.0

## Known Caveats
- Pending deeper statistical audit for extreme market regimes.

## Reproduction
```sql
SELECT date, asopr
FROM read_parquet('data/metrics/daily/metrics.parquet')
WHERE date BETWEEN '2024-01-01' AND '2024-01-10';
```

## Golden Day Snapshot
Capture the golden day chart or table and store it at `images/asopr_golden.png` to replace this note.

## Citations
- Onchain Lab Stage 4 Metrics Specification
