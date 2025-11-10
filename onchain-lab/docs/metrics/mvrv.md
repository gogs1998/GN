# Mvrv

## Definition
Market value to realized value ratio for outstanding supply.

## Formula
```
MVRV = market_value_usd / supply_cost_basis_usd.
```

## QA Badge
| Field | Value |
| --- | --- |
| Status | verified |
| Version | 1.0.0 |
| Coverage % | 100.00 |
| Null Ratio | 0.0000 |
| Deflated Sharpe | 0.28 |
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
SELECT date, mvrv
FROM read_parquet('data/metrics/daily/metrics.parquet')
WHERE date BETWEEN '2024-01-01' AND '2024-01-10';
```

## Golden Day Snapshot
Capture the golden day chart or table and store it at `images/mvrv_golden.png` to replace this note.

## Citations
- David Puell, "Market-Value-to-Realized-Value (MVRV) Ratio"
- Glassnode Academy, "MVRV Ratio"
