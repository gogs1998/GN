# ONCHAIN LAB — Implementation Plan

**Status**: Stage 6 Complete | Next: Stage 6.5+
**Last Updated**: 2025-11-08

---

## Completed Stages (Weeks 1-6)

### ✅ Stage 0-5: Foundation Complete
- **Stage 0**: Project scaffolding
- **Stage 1-2**: Ingest Module (Bitcoin blockchain data ingestion)
- **Stage 3**: Price Oracle Implementation
- **Stage 3.5**: UTXO Lifecycle (created/spent/snapshots)
- **Stage 4**: Metrics Engine (18 core metrics)
- **Stage 5**: Model Baselines (LogReg, XGBoost, CNN-LSTM)

### ✅ Stage 6: Metric QA Badges + Evidence Docs
**Completed**: 2025-11-08
- Cryptographic provenance tracking (SHA256 fingerprints)
- CLI inspector (`onchain-metrics show`)
- Evidence documentation (19 Markdown pages)
- Static docs site (mkdocs)
- Badge registry with QA stats

**Artifacts**:
- `src/metrics/provenance.py` - Deterministic fingerprinting
- `src/metrics/inspect.py` - Upstream record inspection
- `src/metrics/docs.py` - Documentation generation
- `docs/metrics/*.md` - Evidence pages
- `config/metrics_registry.yaml` - Badge registry

---

## Stage 6.5 — Docs & QA Badges Publisher

**Goal**: Turn Stage 6 artifacts into browsable docs site + machine-readable badges.

**Timeline**: Week 7 (3-5 days)

### Files to Create
```
docs/badges/<name>.json         # Machine-readable badge JSON per metric
src/docsgen/__init__.py
src/docsgen/badges.py           # Badge JSON exporter
src/docsgen/render.py           # Enhanced doc rendering
src/docsgen/cli.py              # Documentation CLI commands
mkdocs.yml                      # Enhanced MkDocs config (already exists)
```

### Behavior
1. Read `data/metrics/daily/metrics.parquet` + `config/metrics_registry.yaml`
2. Emit per-metric pages (`docs/metrics/<name>.md`) with:
   - Definition, formula, inputs
   - **Golden-day screenshots** (generate plots from QA data)
   - QA badge table (coverage, null_ratio, no_lookahead, etc.)
   - Tiny DuckDB snippet to reproduce a single day
3. Write badges JSON: `docs/badges/<name>.json` with schema:
   ```json
   {
     "metric": "sopr",
     "version": "1.0.0",
     "status": "verified",
     "coverage_pct": 99.5,
     "null_ratio": 0.0,
     "golden_checks_passed": true,
     "deflated_sharpe_score": 0.43,
     "no_lookahead": true,
     "reproducible": true,
     "provenance": {
       "utxo_snapshot_commit": "utxo-spent@abc123def456",
       "price_root_commit": "price-oracle@789ghi",
       "formulas_version": "metrics-formulas@v1.0.0+jkl"
     },
     "last_updated": "2025-11-08T12:34:56Z"
   }
   ```
4. Generate MkDocs navigation from registry

### CLI Commands
```bash
onchain docs build       # Generate docs + badges + plots
onchain docs serve       # Serve locally (mkdocs serve)
onchain docs validate    # Check all badges + links
```

### Acceptance Criteria
- [ ] `onchain docs build` generates complete site locally
- [ ] Badges JSON exists for all 18+ metrics in `docs/badges/`
- [ ] Golden-day plots rendered in evidence pages
- [ ] MkDocs site builds with `--strict` mode (no broken links)
- [ ] Badge JSON validates against schema

### Dependencies
- matplotlib/plotly for golden-day chart generation
- jsonschema for badge validation
- Existing mkdocs.yml and docs/ structure

---

## Stage 7 — Public API + Auth + Rate Limits

**Goal**: Serve metrics & timeseries via minimal, fast API.

**Timeline**: Week 7-8 (5-7 days)

### Files to Create
```
src/api/__init__.py
src/api/app.py              # FastAPI application
src/api/auth.py             # HMAC API key authentication
src/api/ratelimit.py        # In-memory token bucket
src/api/routers/metrics.py  # Metrics endpoints
src/api/routers/catalog.py  # Catalog endpoint
src/api/routers/health.py   # Health checks
src/api/schemas.py          # Pydantic response models
config/api.yaml             # API configuration
tests/api/test_*.py         # API tests
```

### Routes
1. **Health**: `GET /healthz` → `{ok: true, version: "1.0.0"}`
2. **Catalog**: `GET /catalog` → List of metrics with versions, QA badges (from registry)
3. **Single Metric**: `GET /metrics/{name}?start=YYYY-MM-DD&end=YYYY-MM-DD&fields=date,value&format=json|parquet|csv`
   - Returns timeseries from `data/metrics/daily/metrics.parquet`
4. **Multi-Metric**: `GET /metrics?names=sopr,mvrv,nupl&start=2024-01-01&end=2024-12-31`

### Authentication
- **Header**: `X-API-Key` + `X-API-Sign` (HMAC-SHA256 of `path+query+timestamp`)
- **Storage**: API keys in SQLite with metadata (plan, rate limits, created_at)
- **Validation**: HMAC signature with 60-second timestamp tolerance

### Rate Limiting
- **Algorithm**: Token bucket (in-memory, per API key)
- **Default**: 120 requests/minute per key
- **Headers**: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`
- **Response**: 429 Too Many Requests with `Retry-After` header

### Acceptance Criteria
- [ ] `curl` returns JSON/CSV for authenticated requests
- [ ] 401 Unauthorized without API key
- [ ] 429 Too Many Requests when rate limit exceeded
- [ ] HMAC signature verification works
- [ ] Parquet format download works for bulk exports
- [ ] All 4 routes tested with pytest

---

## Stage 8 — SDKs (Python + JS) & CLI

**Goal**: Make it stupid-easy to consume the API.

**Timeline**: Week 8 (3-4 days)

### Files to Create
```
sdk/python/onchain_client/__init__.py
sdk/python/onchain_client/client.py
sdk/python/onchain_client/auth.py
sdk/python/onchain_client/exceptions.py
sdk/python/pyproject.toml
sdk/python/README.md
sdk/python/tests/test_client.py

sdk/js/src/index.ts
sdk/js/src/client.ts
sdk/js/src/auth.ts
sdk/js/package.json
sdk/js/README.md
sdk/js/tests/client.test.ts

src/cli/__init__.py
src/cli/main.py             # Unified CLI (onchain fetch)
src/cli/config.py           # CLI config management
```

### Python Client
```python
from onchain_client import OnchainClient

client = OnchainClient(
    base_url="https://api.onchainlab.io",
    api_key="your-key-here"
)

# Fetch single metric
df = client.timeseries("sopr", start="2019-01-01", end="2024-12-31")

# Fetch multiple metrics
df = client.timeseries(["sopr", "mvrv", "nupl"], start="2020-01-01")

# Get catalog
catalog = client.catalog()
```

### JavaScript Client
```typescript
import { OnchainClient } from 'onchain-client';

const client = new OnchainClient({
  baseUrl: 'https://api.onchainlab.io',
  apiKey: 'your-key-here'
});

// Fetch single metric
const data = await client.timeseries('mvrv', {
  start: '2020-01-01',
  end: '2024-12-31'
});

// Fetch catalog
const catalog = await client.catalog();
```

### CLI
```bash
# Configure API key
onchain config set-key YOUR_API_KEY

# Fetch metrics
onchain fetch sopr --start 2019-01-01 --end 2024-12-31 --format csv > sopr.csv
onchain fetch sopr,mvrv,nupl --start 2020-01-01 --format parquet -o metrics.parquet

# List available metrics
onchain catalog

# Show metric details
onchain show sopr
```

### Acceptance Criteria
- [ ] Python SDK fetches, parses, and verifies HMAC
- [ ] JavaScript SDK fetches, parses, and verifies HMAC
- [ ] CLI can fetch metrics and write to files
- [ ] All SDKs handle rate limiting gracefully (retry with backoff)
- [ ] Unit tests for auth, parsing, error handling
- [ ] README with examples for both SDKs

---

## Stage 9 — MCP Tools (LLM Integration)

**Goal**: Expose API to AI agents in safe, read-only way via Model Context Protocol.

**Timeline**: Week 9 (2-3 days)

### Files to Create
```
mcp/onchain/tools.json          # MCP manifest
mcp/onchain/handlers.py         # Tool implementations
mcp/onchain/schemas.py          # Input/output schemas
mcp/onchain/README.md
mcp/onchain/tests/test_handlers.py
```

### MCP Tools
1. **`onchain.catalog`**: Returns catalog + badges
   ```json
   {
     "name": "onchain.catalog",
     "description": "List all available Bitcoin metrics with QA badges",
     "inputSchema": {},
     "outputSchema": { "type": "array", "items": {...} }
   }
   ```

2. **`onchain.metric_timeseries`**: Fetch metric data
   ```json
   {
     "name": "onchain.metric_timeseries",
     "description": "Fetch timeseries for one or more metrics",
     "inputSchema": {
       "name": "string | string[]",
       "start": "string (YYYY-MM-DD)",
       "end": "string (YYYY-MM-DD)",
       "format": "json | csv"
     }
   }
   ```

3. **`onchain.metric_explain`**: Get metric documentation
   ```json
   {
     "name": "onchain.metric_explain",
     "description": "Get full documentation for a metric",
     "inputSchema": { "name": "string" }
   }
   ```

### Guardrails
- **Row limits**: Cap at 3,000 rows unless `allow_large=true` and key tier=Pro
- **Read-only**: Never let LLM write/modify data
- **Rate limits**: Inherit from API key tier
- **Error handling**: Clear error messages for invalid inputs

### Acceptance Criteria
- [ ] MCP test harness calls all 3 tools successfully
- [ ] Tools return real data from API
- [ ] Row limit enforced (3k default)
- [ ] Documentation tool returns formatted Markdown
- [ ] Integration with Claude Desktop tested

---

## Stage 10 — AutoMetrics Lab (1,000 Experiments Agent)

**Goal**: Sandbox for discovering new metric recipes via automated experimentation.

**Timeline**: Week 9-10 (5-7 days)

### Files to Create
```
labs/autometrics/__init__.py
labs/autometrics/recipes.py     # Declarative metric DSL
labs/autometrics/search.py      # Random/grid/Bayesian search
labs/autometrics/evaluate.py    # OOS accuracy, Sharpe, deflated SR
labs/autometrics/leakage.py     # Lookahead detection
labs/autometrics/cli.py
labs/autometrics/templates/*.yaml
config/autometrics.yaml
tests/labs/test_recipes.py
```

### Metric DSL
```yaml
name: sopr_ratio_sma
description: "SOPR divided by 7-day SMA of SOPR"
expr: (sopr / sma(sopr, 7))
label: next_day_up              # Binary: price[t+1] > price[t]
thresholding: top_quantile:0.6  # Top 60% → long signal
lookback_days: 30
```

### Search Strategies
- **Random**: Sample N random param combinations
- **Grid**: Exhaustive grid search (small param spaces)
- **Bayesian**: Hyperopt/Optuna for efficient search

### Evaluation Metrics
- **Classification**: AUC-ROC, accuracy, F1, Brier score
- **Trading**: Sharpe ratio, deflated Sharpe, max drawdown, CAGR
- **Leakage Tests**: Ensure no future data in features

### Walk-Forward Splits
- **Train**: 3 years
- **Validation**: 1 year
- **Test**: 1 year (out-of-sample)

### CLI
```bash
# Run experiments
onchain lab run --recipes labs/recipes/*.yaml --max 1000 --strategy bayes

# View leaderboard
onchain lab top --k 20 --sort sharpe

# Inspect experiment
onchain lab inspect sopr_ratio_sma_v42

# Generate report
onchain lab report --output labs/report.html
```

### Outputs
- **Leaderboard**: `labs/autometrics/leaderboard.parquet`
  - Columns: `experiment_id`, `recipe`, `params`, `train_auc`, `test_auc`, `sharpe`, `max_dd`, `leakage_score`, `timestamp`
- **Artifacts**: `labs/artifacts/{experiment_id}/`
  - Model weights, equity curves, feature importances

### Acceptance Criteria
- [ ] DSL parser validates recipe YAML
- [ ] Leakage detector flags future-looking features
- [ ] Walk-forward splits enforce no overlap
- [ ] Leaderboard parquet written with all experiments
- [ ] Top-20 report shows deflated Sharpe and OOS metrics
- [ ] CLI runs 100+ experiments in reasonable time

---

## Stage 11 — Scheduler, Backfill & Monitoring

**Goal**: Reliable daily jobs + observability.

**Timeline**: Week 10 (4-5 days)

### Files to Create
```
ops/scheduler.py            # APScheduler for cron jobs
ops/monitor.py              # Prometheus metrics exporter
ops/checks.py               # Smoke tests (QA subset)
ops/backfill.py             # Historical data backfill
ops/.env.sample
ops/config.yaml
docker/Dockerfile.scheduler
docker/Dockerfile.api
docker/docker-compose.yml
docker/prometheus.yml
```

### Daily Jobs (UTC 00:30)
1. **Ingest Delta**: Fetch new Bitcoin blocks → parquet
2. **UTXO Lifecycle**: Link transactions → created/spent
3. **Snapshots**: Generate daily UTXO snapshots
4. **Price Oracle**: Hourly sync → daily resample (00:00 UTC)
5. **Metrics Build**: Compute all metrics → QA checks
6. **Publish**: Update badges, regenerate docs
7. **API Cache Warmup**: Pre-load popular metrics

### Monitoring (Prometheus Exporter)
**Endpoint**: `/metrics`

**Metrics**:
- `onchain_job_duration_seconds{job="ingest|utxo|metrics"}`
- `onchain_job_rows_processed{job="..."}`
- `onchain_qa_checks_passed{job="metrics"}`
- `onchain_qa_checks_failed{job="metrics"}`
- `onchain_api_requests_total{endpoint="/metrics", status="200"}`
- `onchain_api_latency_seconds{endpoint="/metrics"}`

### Smoke Tests
```bash
ops/checks.py --mode quick   # Run 3 golden days
ops/checks.py --mode full    # Run all QA checks
```

### Docker Compose Services
```yaml
services:
  api:
    build: ./docker/Dockerfile.api
    ports: ["8000:8000"]
    volumes: ["./data:/data"]

  scheduler:
    build: ./docker/Dockerfile.scheduler
    environment:
      - JOBS_ENABLED=ingest,utxo,metrics
    volumes: ["./data:/data"]

  prometheus:
    image: prom/prometheus
    ports: ["9090:9090"]
    volumes: ["./docker/prometheus.yml:/etc/prometheus/prometheus.yml"]

  grafana:
    image: grafana/grafana
    ports: ["3000:3000"]
```

### Acceptance Criteria
- [ ] `docker-compose up` starts API + scheduler + monitoring stack
- [ ] Manual trigger runs end-to-end pipeline (ingest → metrics → publish)
- [ ] Prometheus `/metrics` endpoint exports job stats
- [ ] Grafana dashboard shows job durations and QA pass rates
- [ ] Backfill script can rebuild historical data
- [ ] Smoke tests catch regressions

---

## Stage 12 — Billing & Plans (Stripe Integration)

**Goal**: Gate API access by subscription plan.

**Timeline**: Week 11 (3-4 days)

### Files to Create
```
billing/stripe_webhooks.py      # Handle Stripe events
billing/plans.yaml              # Plan definitions
billing/keys.py                 # API key issuance
src/api/middleware/plans.py     # Plan enforcement middleware
src/api/models/users.py         # User/subscription models
migrations/001_create_users.sql
frontend/portal/index.html      # Basic portal (optional)
tests/billing/test_webhooks.py
```

### Plans
```yaml
plans:
  free:
    price: $0/mo
    features:
      - last_30_days: true
      - max_metrics: 5
      - rate_limit: 60/min
      - bulk_download: false

  research:
    price: $49/mo
    features:
      - full_history: true
      - max_metrics: 50
      - rate_limit: 300/min
      - bulk_download: true

  pro:
    price: $199/mo
    features:
      - full_history: true
      - max_metrics: unlimited
      - rate_limit: 1000/min
      - bulk_download: true
      - multi_metric_query: true

  enterprise:
    price: custom
    features:
      - sla: 99.9%
      - dedicated_support: true
      - custom_metrics: true
```

### Stripe Webhook Flow
1. **Checkout Session Complete**: Issue API key with plan metadata
2. **Subscription Updated**: Update plan limits for existing key
3. **Subscription Deleted**: Revoke/downgrade key to free tier
4. **Payment Failed**: Suspend key (grace period)

### Middleware Enforcement
```python
@app.middleware("http")
async def enforce_plan(request: Request, call_next):
    api_key = request.headers.get("X-API-Key")
    user = get_user_by_key(api_key)

    # Check plan limits
    if not user.plan.can_access(request.url.path):
        return JSONResponse(status_code=403, content={"error": "Plan upgrade required"})

    if not user.plan.can_query_date_range(request.query_params.get("start")):
        return JSONResponse(status_code=403, content={"error": "Historical data requires paid plan"})

    return await call_next(request)
```

### Acceptance Criteria
- [ ] Stripe checkout creates user + API key
- [ ] Webhook updates plan in real-time
- [ ] Free tier limited to 30 days, 5 metrics, 60 req/min
- [ ] Research tier gets full history + higher limits
- [ ] 403 error when accessing restricted features
- [ ] Test webhooks verified with Stripe CLI

---

## Stage 13 — ETH Adapter (Account-Based Chain)

**Goal**: Add Ethereum with equivalent metric classes where meaningful.

**Timeline**: Week 11-12 (5-7 days)

### Files to Create
```
chains/eth/ingest.py            # Geth JSON-RPC or public dumps
chains/eth/state.py             # Account balance snapshots
chains/eth/age.py               # Age approximations (first-seen heuristic)
chains/eth/metrics.py           # Realized-like proxies
chains/eth/qa.py                # ETH-specific QA checks
config/chains.yaml              # Multi-chain config
tests/chains/eth/test_*.py
```

### Notes
- **No UTXO**: Approximate "realized" via first-seen price per token unit flow
- **Document caveats**: Account-based != UTXO; metrics are proxies
- **Implement**:
  - Activity/dormancy proxies (age-banded balances by address cohort)
  - MVRV-like metrics using account creation date heuristics
  - Supply distribution by age buckets
- **Keep namespaced**: `mvrv_eth`, `sopr_eth_approx` (mark experimental)

### Metrics for ETH
- `supply_eth`: Total ETH supply
- `mvrv_eth`: Market-to-realized value (approximated)
- `dormancy_eth`: Weighted average coin age
- `hodl_waves_eth`: Supply by age buckets (0-1mo, 1-3mo, etc.)
- `realized_cap_eth`: Sum of ETH × price at first movement

### Acceptance Criteria
- [ ] ETH metrics appear in catalog with `status: experimental`
- [ ] Evidence docs clearly state methodology differences vs BTC
- [ ] Ingest pipeline fetches daily account snapshots
- [ ] QA checks validate ETH-specific constraints
- [ ] Multi-chain config allows selecting BTC or ETH

---

## Stage 14 — Backtests → Live (Execution Adapter)

**Goal**: Turn signals into executable strategies (sim & live).

**Timeline**: Week 12 (4-5 days)

### Files to Create
```
trading/strategies/binary_direction.py   # Long/flat strategy from Stage 5
trading/strategies/multi_signal.py       # Combine multiple signals
trading/execution/paper.py               # CCXT paper trading (no real orders)
trading/execution/live_ccxt.py           # Real execution (guarded)
trading/fees.py                          # Fee schedules per exchange
trading/risk.py                          # Risk controls
trading/cli.py
config/trading.yaml
tests/trading/test_*.py
```

### Behavior
1. Read `data/models/baseline_signals.parquet` (from Stage 5)
2. Cost-aware simulation:
   - Fees: 0.05% per trade (configurable)
   - Slippage: 0.05% (configurable)
   - Borrow costs for short (if enabled)
3. Risk controls:
   - Max drawdown halt (e.g., 25%)
   - Max position size (e.g., 100% of capital)
   - Cooldown period after losses
4. (Optional) Live execution:
   - CCXT integration (Binance, Coinbase Pro, etc.)
   - Requires `--i-know-what-im-doing` flag
   - Only in paper mode by default

### CLI
```bash
# Backtest
onchain trade backtest \
  --signal data/models/baseline_signals.parquet \
  --model xgboost \
  --start 2021-01-01 \
  --end 2024-12-31 \
  --fees 0.05 \
  --slippage 0.05

# Paper trading (live data, fake orders)
onchain trade live \
  --exchange binance \
  --pair BTC/USDT \
  --signal latest \
  --mode paper

# Real trading (requires explicit flag + config)
onchain trade live \
  --exchange binance \
  --pair BTC/USDT \
  --signal latest \
  --mode live \
  --i-know-what-im-doing
```

### Outputs
- **Backtest tear sheet**: PDF with equity curve, metrics, trades
- **Paper trading log**: JSON with positions, orders, PnL
- **Live trading log**: Encrypted log with real orders (audit trail)

### Acceptance Criteria
- [ ] Backtest produces tear sheet with Sharpe, CAGR, max DD
- [ ] Paper trading logs positions without placing real orders
- [ ] Live trading blocked without explicit flag
- [ ] Risk controls halt trading on max DD breach
- [ ] All trades logged with timestamp, price, fees

---

## Stage 15 — Security, Provenance & Licensing

**Goal**: Button up security and data provenance.

**Timeline**: Week 13 (2-3 days)

### Files to Create
```
security/sbom.md                # Software Bill of Materials
security/hardening.md           # Security best practices
security/key_rotation.md        # API key rotation procedures
provenance/manifest.json        # Checksums of datasets per build
LICENSES/DATA_LICENSE.md        # Data usage license
LICENSES/CODE_LICENSE.md        # Software license (MIT/Apache)
.github/dependabot.yml          # Automated dependency updates
tests/security/test_auth.py     # Security tests
```

### Provenance Manifest
```json
{
  "build_id": "metrics-2025-11-08-1234",
  "timestamp": "2025-11-08T12:34:56Z",
  "inputs": {
    "price": {
      "files": ["data/price/2024-11-07.parquet"],
      "sha256": "abc123..."
    },
    "utxo_snapshots": {
      "files": ["data/utxo/snapshots/2024-11-07.parquet"],
      "sha256": "def456..."
    },
    "utxo_spent": {
      "file": "data/utxo/lifecycle/spent.parquet",
      "sha256": "ghi789..."
    }
  },
  "outputs": {
    "metrics": {
      "file": "data/metrics/daily/metrics.parquet",
      "sha256": "jkl012...",
      "rows": 2450
    }
  },
  "formulas_version": "metrics-formulas@v1.0.0+abc",
  "pipeline_version": "onchain-lab@v0.5.0"
}
```

### SBOM (Software Bill of Materials)
- List all dependencies with versions
- Pin exact versions in `requirements.txt` / `poetry.lock`
- Run `pip-audit` / `safety check` in CI

### Data License
```markdown
# Data Usage License

The Bitcoin on-chain metrics provided by Onchain Lab are licensed for:
- ✅ Research and analysis
- ✅ Internal business use
- ✅ Derived works (models, strategies)

Restrictions:
- ❌ No redistribution of raw data
- ❌ No resale of metrics
- ❌ No transfer to third parties without permission

Attribution required for public use.
```

### Acceptance Criteria
- [ ] Build writes provenance manifest matching docs
- [ ] SBOM lists all dependencies with CVE checks
- [ ] API key rotation documented with zero-downtime procedure
- [ ] Data license published in docs
- [ ] Security tests cover auth, rate limiting, input validation

---

## Stage 16 — Site & Landing (Docs + Signup)

**Goal**: Minimal but professional web presence.

**Timeline**: Week 13-14 (5-7 days)

### Files to Create
```
website/nextjs/*
website/pages/index.tsx         # Landing page
website/pages/catalog.tsx       # Metric catalog
website/pages/docs/[metric].tsx # Metric docs
website/pages/pricing.tsx       # Pricing plans
website/pages/signup.tsx        # Signup flow
website/components/BadgeCard.tsx
website/lib/api.ts              # API client for SSR
website/public/images/
website/styles/
next.config.js
```

### Features
1. **Landing Page**:
   - Hero: "Transparent, Verifiable Bitcoin Metrics for Data Scientists"
   - Feature grid: Provenance, QA Badges, API, SDKs
   - Sample chart (SOPR with golden day markers)
   - CTA: "Start Free" → Signup

2. **Catalog Page**:
   - Table/grid of all metrics with QA badges
   - Filter by: status (verified/experimental), category, Sharpe score
   - Click metric → detail page

3. **Metric Detail Pages**:
   - Fetched from `/catalog` API
   - Formula, description, citations
   - QA badge display
   - Sample code snippets (Python, JS, CLI)
   - Interactive chart (optional)

4. **Pricing Page**:
   - Plan comparison table
   - Stripe checkout buttons
   - FAQ

5. **Signup Flow**:
   - Email → Stripe checkout → API key issuance
   - Email API key to user
   - Dashboard: show key, usage stats, plan

### Tech Stack
- **Framework**: Next.js (SSR/SSG)
- **Styling**: Tailwind CSS
- **Charts**: Recharts or Plotly
- **Auth**: NextAuth.js (optional)

### Acceptance Criteria
- [ ] Local run shows catalog fed from `/catalog` API
- [ ] Metric pages render with badges and code snippets
- [ ] Stripe checkout works for paid plans
- [ ] API key delivered after successful payment
- [ ] Site deployed to Vercel/Netlify

---

## Success Metrics (Overall)

**Week 14+ (Post-Launch)**:
- [ ] 100+ API keys issued
- [ ] 10+ Research/Pro subscriptions
- [ ] 10,000+ API requests/day
- [ ] 5+ community-contributed metric recipes
- [ ] 99.5% uptime for API
- [ ] Average query latency < 100ms
- [ ] Stage 15 provenance manifest published weekly

---

## Notes

- **Parallel Work**: Stages 7-9 can run in parallel (API, SDKs, MCP)
- **Optional Stages**: 13 (ETH), 14 (Live Trading), 16 (Website) can be deferred
- **Core Path**: 6.5 → 7 → 8 → 11 → 12 → 15 (bare minimum for launch)
- **Testing**: All stages require >= 80% test coverage
- **Documentation**: Each stage updates SOT.md with implementation review

---

**Last Updated**: 2025-11-08
**Next Review**: After Stage 6.5 completion
