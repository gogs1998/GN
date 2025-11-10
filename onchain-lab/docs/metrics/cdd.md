# Cdd

## Definition
Coin Days Destroyed aggregated for spends on the metric date.

## Formula
```
CDD = Σ (value_btc × holding_days) for all spends on the metric date.
```

## QA Badge
| Field | Value |
| --- | --- |
| Status | verified |
| Version | 1.0.0 |
| Coverage % | 99.50 |
| Null Ratio | 0.0000 |
| Deflated Sharpe | 0.00 |
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
SELECT date, cdd
FROM read_parquet('data/metrics/daily/metrics.parquet')
WHERE date BETWEEN '2024-01-01' AND '2024-01-10';
```

## Golden Day Snapshot
Capture the golden day chart or table and store it at `images/cdd_golden.png` to replace this note.

## Citations
- ByteTree, "Coin Days Destroyed"
