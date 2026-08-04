# Nadobro Stats Dashboard — Build Spec

Implementation spec for Cursor. Architecture rationale and API verification live in
[`stats_dashboard_design.md`](./stats_dashboard_design.md) — read §1 of that doc once, then work
from this one.

**Deliverable:** an admin dashboard showing **Total Transactions** (bar chart), **Total Volume**,
**Total Users**, and **NLP Deposits**, sourced from the on-chain `BuilderFeePayment` ledger for
builder ID 2900 plus Nadobro's own Postgres.

---

## 0. Non-negotiable repo rules

These are enforced by tests that will fail CI. Read before writing code.

| Rule | Enforced by |
|---|---|
| All env reads go through `utils/env.py` (`env_str/env_int/env_float/env_bool`). No raw `os.environ` parsing — values carry inline `#` comments. | `tests/test_env_read_hygiene_static.py` (AST-based, alias-proof) |
| Package import edges may only shrink. **This spec adds none** — see §3. | `tests/lint/test_architecture_layers.py` |
| Never call blocking IO inside a coroutine body. `scheduler` is `AsyncIOScheduler`; offload via `core/async_utils.run_blocking_sdk` / `run_blocking_db`. | `project_event_loop_blocking` — starves the loop, APScheduler starts skipping jobs |
| New tables follow the existing DDL pattern in `db.py` (`_NETWORK_*_DDL` + `for net in ("testnet","mainnet")`, or a plain block for network-agnostic tables). | `tests/test_schema_migrations_static.py` |
| Never log tokens/keys; output passes `core/log_redaction`. | `tests/test_log_redaction.py` |
| `mypy src/nadobro/engine` is a blocking CI job. This spec touches `venue/`, `quant/`, `db.py`, `runtime/` — not `engine/` — but keep annotations clean anyway. | `ci.yml` |

Full suite must stay green: `.venv/bin/python -m pytest -q`

---

## 1. Locked definitions

Do not re-litigate these in code review; they are decided.

**Total Transactions** — the headline activity metric, reported to chains and ecosystem partners.

> **One `BuilderFeePayment` log = one transaction.**

Count the events. Do **not** deduplicate by `tx_hash`: Nado's off-chain sequencer batches many
matches into a single L2 submission, so counting L2 tx hashes undercounts real trading activity by
one to two orders of magnitude. We store `tx_hash` anyway, so `count(DISTINCT tx_hash)` stays
available as a free query if a specific chain program ever asks for settlement-level counts — but it
is **not** displayed and **not** the headline number.

Use the word **"transactions"** in every column name, API field, and label. Not "fills". Not "orders".

**Total Volume** — `Σ (feeAmount / feeRate)` over builder-attributed events, in USD.
Confirmed correct: NadoExplorer publishes $674.39K volume against $67.44 generated at 1 bp.

**Total Users** — `count(*)` from our `users` table. Builder events give *wallets*, not accounts, and
isolated margin mints a subaccount per position, so on-chain wallet counts overcount. Store
`active_wallets` from the chain as a separate cross-check column.

**NLP Deposits** — from `vault_lp_events_<net>`. Report **gross mint** in USDT0 as the headline, with
burn stored alongside so net is derivable. Not builder-attributable; this is our ledger.

**Network** — mainnet only for on-chain data. `config.py:304-312` bypasses builder routing on
testnet entirely, so `BuilderFeePayment` events do not exist there. Testnet rows carry NULL in every
on-chain column.

---

## 2. Ground truth to hit

Our ingest must reproduce these public figures for builder 2900 (observed 2026-08-01 at
`nadoexplorer.com/builders/2900/cohorts`). Treat as an acceptance oracle, not a spec.

| Metric | Target |
|---|---|
| Volume lifetime | ~$674.39K |
| Builder revenue lifetime | ~$67.44 |
| Transactions lifetime | ~2.5K |
| Active wallets | 4 |
| Markets | 21 |
| Fee rate | 1.00 bps |

If Phase 1 output is materially off these, the ingest is wrong. Stop and debug before Phase 2.

---

## 3. File plan

Zero new package import edges. Everything lands in packages that already have the edges it needs.

```
src/nadobro/
  venue/builder_ledger.py      NEW  eth_getLogs reader + persistence
                                    imports: config, core, db, utils  (all allowed for venue)
  quant/builder_stats.py       NEW  pure aggregation math, no IO
                                    imports: utils only (quant is a leaf)
  db.py                        EDIT add DDL for 2 new tables
  runtime/scheduler.py         EDIT register 2 jobs (runtime→venue already allowed)
  config.py                    EDIT add NADO_BUILDER_STATS_* constants

stats/                         NEW  read-only FastAPI service, sibling to relay/
  main.py                           not under src/nadobro/, so not subject to layering test
  queries.py
  static/index.html
  fly.toml
  requirements.txt
  Dockerfile

tests/
  venue/test_builder_ledger.py      NEW
  quant/test_builder_stats.py       NEW
  test_stats_schema.py              NEW

scripts/verify_builder_ledger.py    NEW  Phase 0 spikes, runnable standalone
docs/stats_dashboard_build.md       this file
```

**Do not** create an `analytics/` package. It would require editing the layering allowlist, and
CLAUDE.md states the edge set may only shrink.

---

## 4. Schema

Append to `db.py` inside the existing `init_db()` DDL section. Both tables are network-agnostic
(network is a column, not a table suffix) — follow the plain-block pattern, not `_NETWORK_*_DDL`.

```sql
-- Append-only on-chain builder ledger. Mainnet only; testnet never populates.
CREATE TABLE IF NOT EXISTS builder_fee_events (
    tx_hash        TEXT          NOT NULL,
    log_index      INT           NOT NULL,
    block_number   BIGINT        NOT NULL,
    block_ts       TIMESTAMPTZ,
    network        TEXT          NOT NULL DEFAULT 'mainnet',
    subaccount     TEXT          NOT NULL,
    wallet         TEXT,
    product_id     INT           NOT NULL,
    order_digest   TEXT          NOT NULL,
    fee_amount_x18 NUMERIC(78,0) NOT NULL,
    fee_rate_raw   NUMERIC(78,0) NOT NULL,
    volume_usd     DOUBLE PRECISION,
    PRIMARY KEY (tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_bfe_block   ON builder_fee_events (block_number);
CREATE INDEX IF NOT EXISTS idx_bfe_digest  ON builder_fee_events (order_digest);
CREATE INDEX IF NOT EXISTS idx_bfe_ts      ON builder_fee_events (block_ts);
CREATE INDEX IF NOT EXISTS idx_bfe_wallet  ON builder_fee_events (wallet);

-- Serving read model. Rebuildable from builder_fee_events + trades_<net> + users at any time.
CREATE TABLE IF NOT EXISTS stats_daily (
    day                  DATE   NOT NULL,
    network              TEXT   NOT NULL,
    transactions         BIGINT,              -- on-chain; NULL on testnet
    volume_usd           DOUBLE PRECISION,
    builder_revenue_usd  DOUBLE PRECISION,
    active_wallets       INT,
    markets              INT,
    users_total          BIGINT,
    users_new            INT,
    users_active         INT,
    nlp_mint_usdt0       DOUBLE PRECISION,
    nlp_burn_usdt0       DOUBLE PRECISION,
    db_transactions      BIGINT,              -- our ledger, for recon
    db_volume_usd        DOUBLE PRECISION,
    recon_delta_pct      DOUBLE PRECISION,
    unmatched_digests    INT,
    updated_at           TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (day, network)
);
CREATE INDEX IF NOT EXISTS idx_stats_daily_day ON stats_daily (network, day DESC);
```

Recon columns live on `stats_daily` rather than a second table — one row per day per network keeps
the dashboard a single query.

---

## 5. `venue/builder_ledger.py`

### Constants (into `config.py`)

```python
NADO_BUILDER_STATS_ENABLED_ENV = "NADO_BUILDER_STATS_ENABLED"
# BuilderFeePayment(bytes32,uint32,uint32,bytes32,int128,int128) — derived locally, MUST be
# confirmed against a live log by scripts/verify_builder_ledger.py V1 before trusting it.
BUILDER_FEE_PAYMENT_TOPIC0 = "0x1d708c8f826da6e14f2e8e350d8bcad13cabb0a9e9498a029000f0e269377fe2"
```

### Env vars (all via `utils/env.py`)

| Var | Default | Purpose |
|---|---|---|
| `NADO_BUILDER_STATS_ENABLED` | `false` | Master switch. Ship dark, enable after Phase 0 passes. |
| `INK_RPC_URL` | *(exists, `config.py:22`)* | Ink L2 RPC |
| `NADO_BUILDER_LOG_CHUNK_BLOCKS` | `2000` | `eth_getLogs` range per call; tune to the provider's cap |
| `NADO_BUILDER_LOG_FINALITY_LAG` | `64` | Ingest only to `head − lag`; reorg safety |
| `NADO_BUILDER_LOG_START_BLOCK` | `0` | Backfill floor; set to our builder registration block once known |
| `NADO_BUILDER_STATS_INTERVAL_SECONDS` | `300` | Ingest cadence |
| `NADO_BUILDER_ROLLUP_INTERVAL_SECONDS` | `900` | Rollup cadence |
| `NADO_BUILDER_ROLLUP_TRAILING_DAYS` | `3` | Days recomputed each rollup run |

### Public functions

```python
def get_builder_info() -> dict | None:
    """SDK NadoContracts.get_builder_info(builder_id) -> owner, fee tier, rate bounds.
    Returns None when the SDK/RPC is unreachable. Used by the health endpoint to prove
    our builder ID is registered."""

def fetch_builder_logs(from_block: int, to_block: int) -> list[dict]:
    """eth_getLogs against offchain_exchange_addr, filtered server-side on our builder ID.

    topics = [BUILDER_FEE_PAYMENT_TOPIC0, None, <builder_id as 32-byte topic>, None]
      topic1 = subaccount (bytes32)
      topic2 = builder    (uint32, left-padded)
      topic3 = productId  (uint32, left-padded)
      data   = (bytes32 digest, int128 feeAmount, int128 feeRate)

    Decode data with eth_abi.decode(["bytes32","int128","int128"], log["data"]).
    Address comes from load_deployment("mainnet").offchain_exchange_addr — never hardcode
    the value in the Nado docs.
    Raises on RPC error; caller handles retry/backoff."""

def ingest_builder_logs(*, network: str = "mainnet") -> dict:
    """Blocking. Cursor-driven incremental ingest. Returns
    {"from_block", "to_block", "rows", "skipped"}.

    1. cursor = get_bot_state("builder_log_cursor") or NADO_BUILDER_LOG_START_BLOCK
    2. head = w3.eth.block_number; to_block = head - FINALITY_LAG
    3. walk [cursor, to_block] in CHUNK_BLOCKS steps
    4. UPSERT ON CONFLICT (tx_hash, log_index) DO NOTHING  -> replay-safe
    5. advance cursor ONLY after a chunk commits

    volume_usd = fee_amount / fee_rate, using the scale pinned by Phase 0 V2.
    Leave block_ts NULL here; resolve_block_timestamps() fills it."""

def resolve_block_timestamps(limit: int = 500) -> int:
    """Fill block_ts where NULL. Prefer joining order_digest -> trades_mainnet.filled_at
    (free); fall back to eth_getBlockByNumber with a per-block cache. Returns rows filled."""
```

The `wallet` column is the first 20 bytes of `subaccount`. Populate it on insert — it is what makes
per-user joins and isolated-subaccount collapsing possible later.

### `quant/builder_stats.py`

Pure functions, no DB, no network, injectable `now` — mirror the style of `quant/user_analytics.py`.

```python
def volume_from_fee(fee_amount_x18: Decimal, fee_rate_raw: Decimal) -> Decimal: ...
def daily_buckets(rows: Iterable[Mapping], *, tz_utc: bool = True) -> dict[date, dict]: ...
def recon_delta_pct(onchain: Decimal, db: Decimal) -> Decimal | None: ...  # None when onchain == 0
def detect_self_matches(rows: Iterable[Mapping]) -> int:
    """Two of our events sharing a tx_hash with opposing sides = one economic trade counted twice."""
```

---

## 6. Scheduler wiring

In `runtime/scheduler.py::start_scheduler()`, follow the `sync_pending_fills` precedent — the
scheduler is `AsyncIOScheduler`, so jobs are coroutines and blocking work is offloaded.

```python
if builder_stats_enabled():
    from src.nadobro.venue.builder_ledger import (
        tick_builder_ingest, tick_builder_rollup,
    )
    scheduler.add_job(
        tick_builder_ingest, "interval",
        seconds=builder_stats_interval_seconds(),
        id="builder_stats_ingest", replace_existing=True,
        max_instances=1, misfire_grace_time=120, coalesce=True,
    )
    scheduler.add_job(
        tick_builder_rollup, "interval",
        seconds=builder_rollup_interval_seconds(),
        id="builder_stats_rollup", replace_existing=True,
        max_instances=1, misfire_grace_time=120, coalesce=True,
    )
```

```python
async def tick_builder_ingest() -> None:
    from src.nadobro.core.async_utils import run_blocking_sdk
    try:
        result = await run_blocking_sdk(ingest_builder_logs)
        await run_blocking_sdk(resolve_block_timestamps)
        logger.info("builder ingest: %s rows to block %s", result["rows"], result["to_block"])
    except Exception:
        logger.exception("builder ingest failed")   # never let a tick kill the scheduler
```

`max_instances=1` is required: a slow RPC must not stack overlapping ingests.

---

## 7. Phase 0 — verification spikes

Write `scripts/verify_builder_ledger.py` with a subcommand per check. **Every one must pass before
Phase 1.** Print results; write nothing.

| ID | Check | Pass condition |
|---|---|---|
| **V0** | `SELECT is_taker, count(*), sum(builder_fee), sum(fill_price*fill_size) FROM trades_mainnet WHERE builder_fee > 0 GROUP BY 1;` | Rows exist for `is_taker = false`. If maker fills never carry a builder fee, on-chain volume undercounts and §1 needs revisiting. |
| **V1** | `eth_getLogs` by address only over a recent range where we know we traded; list distinct `topic0`. | `0x1d708c8f…` appears. If not, the deployed event signature differs from the docs — recompute topic0 from the real ABI before continuing. |
| **V2** | Pick one log, find its fill in `trades_mainnet` via `order_digest`. Compute `feeAmount/feeRate` treating `feeRate` as (a) x18 fraction, (b) raw 0.1bp units. | One interpretation reproduces `fill_price × fill_size` within rounding. Hardcode that scale. |
| **V3** | Full-history sum: `Σ volume_usd` and `count(*)`. | Within ~2% of $674.39K and ~2.5K transactions (§2). |
| **V4** | Archive `events` with `event_types: ["mint_lp"]` vs `["mint_nlp"]` for a subaccount known to have minted. | Exactly one returns rows. `nado_archive.py:945` currently sends `mint_nlp` — fix if wrong. |
| **V5** | `get_builder_info(2900)` | Returns our owner address and fee-rate bounds; confirms the ID is registered. |

---

## 8. HTTP service

`stats/` — read-only FastAPI on Fly, modelled on `relay/` (own `fly.toml`, own Dockerfile, deployed
independently). It **only reads** `stats_daily`. All writes belong to the bot's scheduler; a single
writer avoids advisory locks and a second venue client.

```
GET  /api/summary?network=mainnet
     -> {transactions, volume_usd, builder_revenue_usd, users_total, users_active,
         active_wallets, nlp_mint_usdt0, nlp_burn_usdt0, recon_delta_pct, updated_at}

GET  /api/timeseries?metric=transactions|volume_usd|nlp_mint_usdt0
                    &from=YYYY-MM-DD&to=YYYY-MM-DD&network=mainnet
     -> {points: [{day, value}, ...]}

GET  /healthz -> {ok, cursor_block, head_block, lag_blocks, last_rollup_at}
```

**Auth:** admin only. Bearer token from `STATS_ADMIN_TOKEN` (via `utils/env.py`), checked on every
`/api/*` route. No public URL, no per-user rows in v1 — aggregates only. This surface exposes
aggregate user financial data; treat it accordingly.

**UI:** `static/index.html`, self-contained, no CDN. Four KPI cards (Total Transactions, Total
Volume, Total Users, NLP Deposits) above a daily bar chart of transactions with a 30/90/all range
toggle. A recon banner turns amber when `|recon_delta_pct| > 1`.

---

## 9. Tests

| File | Covers |
|---|---|
| `tests/venue/test_builder_ledger.py` | topic construction for a known builder ID; `eth_abi` decode of a fixture log; cursor advances only after commit; re-ingesting the same logs inserts 0 rows (ON CONFLICT); ingest stops at `head − FINALITY_LAG` |
| `tests/quant/test_builder_stats.py` | `volume_from_fee` under both rate scales; daily bucketing across a UTC midnight boundary; `recon_delta_pct` returns None on zero denominator; `detect_self_matches` on a two-event tx |
| `tests/test_stats_schema.py` | both tables created idempotently by `init_db()`; PK conflict is a no-op |

Use `NADO_TEST_DATABASE_URL` for DB-backed tests (`compose.postgres-test.yaml`, port 5433). conftest
scrubs non-local `DATABASE_URL`, so these never touch prod.

Network calls must be mocked — no test may hit Ink RPC or the archive.

---

## 10. Task order for Cursor

Work top to bottom. Each phase has a gate; do not start the next until it passes.

**Phase 0 — verify** *(gate: V0–V5 all pass; write results back into this file)*
1. `scripts/verify_builder_ledger.py` with the six subcommands from §7
2. Run against mainnet, record actual numbers and the pinned `feeRate` scale

**Phase 1 — ingest** *(gate: V3 reconciles within 2% of §2)*
3. `config.py` constants + env accessors
4. `db.py` DDL for both tables
5. `venue/builder_ledger.py`: `fetch_builder_logs`, `ingest_builder_logs`, `resolve_block_timestamps`
6. `tests/venue/test_builder_ledger.py`
7. Full backfill from `NADO_BUILDER_LOG_START_BLOCK`

**Phase 2 — rollups** *(gate: `recon_delta_pct` < 1% for 7 consecutive days)*
8. `quant/builder_stats.py` + tests
9. `tick_builder_rollup` — idempotent UPSERT, recompute trailing 3 days
10. Scheduler registration behind `NADO_BUILDER_STATS_ENABLED`

**Phase 3 — surface**
11. `stats/` FastAPI service + auth + `fly.toml`
12. `static/index.html` — 4 KPI cards + transactions bar chart
13. Deploy to Fly, admin-gated

Ship Phase 2 before Phase 3 if time is short — a `psql` query against `stats_daily` already answers
every question the dashboard will. The UI is presentation, not capability.

---

## 11. Acceptance

- [ ] `.venv/bin/python -m pytest -q` green
- [ ] `.venv/bin/python -m mypy src/nadobro/engine` clean
- [ ] Lifetime totals within 2% of NadoExplorer's published figures (§2)
- [ ] Re-running ingest over an already-ingested range inserts 0 rows
- [ ] Killing the process mid-ingest and restarting loses no events and duplicates none
- [ ] `NADO_BUILDER_STATS_ENABLED=false` leaves scheduler behaviour byte-identical to today
- [ ] Every metric on the dashboard is labelled with its provenance: **on-chain verified**
      (transactions, volume) vs **Nadobro ledger** (users, NLP deposits)
