# Supply Sats

## Definition
Outstanding UTXO supply expressed in satoshis.

## Formula
```
Supply Sats = supply_btc × 100,000,000.
```

## QA Badge
| Field | Value |
| --- | --- |
| Status | verified |
| Version | 1.0.0 |
| Coverage % | 100.00 |
| Null Ratio | 0.0000 |
| Deflated Sharpe | 0.00 |
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
SELECT date, supply_sats
FROM read_parquet('data/metrics/daily/metrics.parquet')
WHERE date BETWEEN '2024-01-01' AND '2024-01-10';
```

## Golden Day Snapshot
Capture the golden day chart or table and store it at `images/supply_sats_golden.png` to replace this note.

## Citations
- Onchain Lab Stage 4 Metrics Specification
