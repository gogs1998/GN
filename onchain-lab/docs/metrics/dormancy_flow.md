# Dormancy Flow

## Definition
Dormancy flow calculated as market capitalization divided by rolling Coin Days Destroyed.

## Formula
```
Dormancy Flow = market_value_usd / rolling_mean(CDD).
```

## QA Badge
| Field | Value |
| --- | --- |
| Status | verified |
| Version | 1.0.0 |
| Coverage % | 99.00 |
| Null Ratio | 0.0010 |
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
SELECT date, dormancy_flow
FROM read_parquet('data/metrics/daily/metrics.parquet')
WHERE date BETWEEN '2024-01-01' AND '2024-01-10';
```

## Golden Day Snapshot
Capture the golden day chart or table and store it at `images/dormancy_flow_golden.png` to replace this note.

## Citations
- Glassnode Academy, "Dormancy Flow"
