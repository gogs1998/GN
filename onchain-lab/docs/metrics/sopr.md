# Sopr

## Definition
Spent Output Profit Ratio computed as realized value divided by cost basis for the day.

## Formula
```
SOPR = realized_value_usd / cost_basis_spent_usd (spent outputs).
```

## QA Badge
| Field | Value |
| --- | --- |
| Status | verified |
| Version | 1.0.0 |
| Coverage % | 99.50 |
| Null Ratio | 0.0000 |
| Deflated Sharpe | 0.43 |
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
SELECT date, sopr
FROM read_parquet('data/metrics/daily/metrics.parquet')
WHERE date BETWEEN '2024-01-01' AND '2024-01-10';
```

## Golden Day Snapshot
Capture the golden day chart or table and store it at `images/sopr_golden.png` to replace this note.

## Citations
- Willy Woo, "On-chain metrics: SOPR" (2019)
- Glassnode Academy, "Spent Output Profit Ratio"
