# Nadobro Bot

Nadobro is a Telegram-first trading copilot for **Nado DEX** (a CLOB perp venue on Ink L2).
It lets users ask market questions, inspect live Nado/Ink context, link a 1CT signer wallet,
place natural-language trades, monitor live PnL, set alerts, and run automated strategies —
all from chat.

## Core features

- Natural-language trade parsing (for example: "long ETH 0.1 at 10x") with confirmation flow
- Wallet linking with encrypted 1CT signer keys (never logged, redaction enforced)
- Live market data, positions, portfolio and PnL views (edit-in-place card UI)
- Automated strategies (Engine v2): Grid, Reverse Grid, Dynamic Grid, Market Making (Mid),
  Volume Bot, Delta Neutral — maker-first limit orders, session SL/TP rails judged net of fees
- Desk: text-to-trade plans with digest-tagged closes and full fill attribution
- Copy trading (poll-based live path in `trading/copy_service.py`)
- Price/funding alerts and vault deposit-capacity watches
- AI layer through a single LLM gateway (NanoGPT; per-task model env vars): AI chat,
  Night HOWL analyst, edge scanner, signals, morning brief, knowledge/vector retrieval
- Cost-aware backtester (fees + funding + slippage) driving the same controllers as live

## Repo layout

```
main.py                  # entry point: boot, transport (polling/webhook), handler wiring
src/nadobro/
  config.py              # env + product/market constants
  db.py                  # psycopg2 pool + statement helpers (disconnect hygiene)
  i18n.py                # translations; reply-keyboard label round-trip
  utils/                 # stdlib-only leaves (env parsing with inline-# tolerance, x18)
  core/                  # infra leaves: thread pools, caches, rate limits, HTTP session,
                         # log redaction, perf/SLI, feature flags
  quant/                 # pure trading math: margin, fill attribution/PnL pairing,
                         # quote math, POV participation
  models/ + migrations/  # raw-SQL CRUD (per-network tables), schema migrations
  connectors/            # market-data/news connectors, provider catalog + env resolution,
                         # source freshness registry
  venue/                 # everything that talks to Nado: REST/WS clients, fill sync,
                         # archive indexer, product catalog, market feed, gateway budgets
  market_data/           # non-venue data: CMC/HL/X clients, news, scanners, price tracker
  llm/                   # LLM gateway + providers, AI chat, knowledge/vector store,
                         # analysts (HOWL, edge, signals, briefs), managed agent
  engine/                # Engine v2: orchestrator, controllers, executors, risk, backtester
  trading/               # order/trade domain: placement, closes, digest intents,
                         # attribution persistence, desk, copy trading, readiness/budgets
  strategy/              # strategy lifecycle + runtime: bot_runtime, engine_runtime,
                         # registry, FSM, schedulers, MM overlay + dashboard
  users/                 # accounts: users, settings, onboarding, invites/referrals/points,
                         # admin, audit log, wallet flows
  portfolio/             # portfolio views, PnL cards
  vault/                 # Nado NLP vault: metrics, deposit watching
  notify/                # outbound delivery: rate-limited Telegram sender, alerts
  runtime/               # APScheduler job wiring, runtime supervisor
  handlers/              # Telegram UI: commands, callback router, free-text flow,
                         # keyboards, cards and views
relay/                   # separate FastAPI/Telethon microservice (@lowiqpts lookups)
tests/                   # full suite incl. DB-backed tests and lint/architecture guards
docs/ARCHITECTURE.md     # layering rules, package map, and the guards that enforce them
```

The dependency direction is enforced by `tests/lint/test_architecture_layers.py`:
domain packages never import the `handlers/` UI layer, `engine/` reaches only
`venue`/`quant`/`utils`, and the package import graph can only shrink.

## Quick start

1. Install dependencies:
   - `pip install -r requirements.txt`
2. Configure environment variables (values may carry inline `# comments`; parsing is
   comment-tolerant via `src/nadobro/utils/env.py`):
   - `TELEGRAM_TOKEN`, `DATABASE_URL`, `ENCRYPTION_KEY`
   - `NANOGPT_API_KEY` (AI layer), `XAI_API_KEY` (Grok X-search), `DMIND_API_KEY` (optional)
   - Market-data provider keys as needed: `CMC_API_KEY`, `COINGECKO_API_KEY`, `FMP_API_KEY`, …
   - `TELEGRAM_TRANSPORT` (`polling` default, `webhook` for prod),
     `TELEGRAM_WEBHOOK_URL`/`_PATH`/`_SECRET` for webhook mode
3. Start the bot:
   - `python3.11 main.py`

## Testing and gates

- Full suite: `.venv/bin/python -m pytest -q` (CI runs all of it and must stay green)
- DB-backed tests need a local postgres:
  `docker compose -f compose.postgres-test.yaml up -d`, then run pytest with
  `NADO_TEST_DATABASE_URL=postgresql://nadobro:nadobro@127.0.0.1:5433/nadobro_test`
- Type-check (blocking in CI): `.venv/bin/python -m mypy src/nadobro/engine`
- Strategy gate (required before merging any strategy / SL-TP / config change):
  `PYTHON=.venv/bin/python bash scripts/self_review.sh`

## Hard product rules

- Redeploys NEVER auto-resume any trade, plan, or strategy. Boot = stand-down;
  resuming is strictly user-initiated.
- The venue reports no per-fill realized PnL — all PnL comes from local fill
  attribution; close orders must be digest-tagged so they reconcile.
- Maker-first limit orders for all strategy opens/closes.

## Deployment

Production runs on Fly.io. See [`deploy.md`](deploy.md) for the step-by-step guide.

---

Docs: [nadobro.gitbook.io/docs](https://nadobro.gitbook.io/docs)
