# Nupl

## Definition
Net Unrealized Profit/Loss computed as (market - realized)/market.

## Formula
```
NUPL = (market_value_usd − supply_cost_basis_usd) / market_value_usd.
```

## QA Badge
| Field | Value |
| --- | --- |
| Status | verified |
| Version | 1.0.0 |
| Coverage % | 100.00 |
| Null Ratio | 0.0000 |
| Deflated Sharpe | 0.22 |
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
SELECT date, nupl
FROM read_parquet('data/metrics/daily/metrics.parquet')
WHERE date BETWEEN '2024-01-01' AND '2024-01-10';
```

## Golden Day Snapshot
Capture the golden day chart or table and store it at `images/nupl_golden.png` to replace this note.

## Citations
- LookIntoBitcoin, "Net Unrealized Profit/Loss"
