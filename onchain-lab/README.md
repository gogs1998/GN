ONCHAIN LAB is a transparent, ML-native platform for verifiable Bitcoin on-chain metrics. We transform raw blockchain data into auditable insights with reproducible pipelines and published methodologies. This repo tracks the MVP buildout, roadmap, and supporting assets for our BTC-focused launch.

- Copy `.env.example` to `.env` and set `BTC_RPC_USER`/`BTC_RPC_PASS`, then install dependencies with `poetry install`.
- Start the local Bitcoin node via `make up`, tail logs with `make logs`, and stop it with `make down`.
- Run `poetry run onchain ingest catchup --max-blocks 1000` (or `make ingest`) once the node is synced to hydrate the Parquet lake under `data/parquet`.
- After new batches, execute `poetry run onchain ingest verify --date 2020-05-11` (or `make verify`) to assert golden-day QA checks. Reference metrics in `src/ingest/golden_refs.json` are derived from blockchain.com charts; regenerate via `python scripts/derive_golden_refs.py` if upstream sources change.

This repository is automatically mirrored to `https://github.com/gogs1998/Onchain_lab.git` on every push to the `main` branch via a GitHub Actions workflow.
