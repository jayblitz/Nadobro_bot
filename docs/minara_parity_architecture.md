# Nadobro Minara-Parity Architecture

## Core Layers

### DMind Financial Expert Layer

`src/nadobro/services/dmind_service.py` is the finance-native expert layer. It is responsible for financial data structuring, market summarization, signal scoring, workflow planning, and strategy recommendation. If `DMIND_API_KEY` is absent, Nadobro must disclose degraded mode for market recommendations and autonomous decisions.

### Source Registry

`src/nadobro/services/source_registry.py` records provider metadata:

- provider
- fetched timestamp
- TTL
- confidence
- latency
- source URL
- license tier
- allowed use
- stale/fresh status

This prevents generic stale answers from looking as trustworthy as provider-backed data.

### Provider Connectors

Provider adapters live under `src/nadobro/connectors/`.

Initial provider targets mirror Minara’s public docs:

- DMind
- n8n
- Ink RPC/WebSocket
- Arkham
- CoinMarketCap/CoinGecko
- CoinGlass
- DeFiLlama
- Glassnode
- NFTGo
- Virtuals/Pump.fun/Bonk.fun
- RootData
- X/social
- global news
- Polymarket
- GoPlus
- xStocks
- OpenAI/Grok search
- FMP

### Ink Intelligence Service

`src/nadobro/services/ink_intelligence_service.py` combines the existing Nado scanner, CMC, X/social, Pinecone, archive fills, provider status, and DMind analysis into one snapshot for the Telegram copilot.

### n8n Workflow Layer

`src/nadobro/services/workflow_service.py` builds workflow drafts from natural language and deploys them to n8n when configured.

n8n is allowed to orchestrate workflow nodes, but safe actions remain behind Nadobro APIs:

- analyze
- recommend
- simulate
- notify
- start strategy
- pause strategy
- stop strategy
- flatten
- update risk settings

n8n must never hold raw signing keys or bypass wallet readiness, budget guard, admin pause, or strategy runtime checks.

### Strategy Runtime Reliability

`src/nadobro/services/strategy_fsm.py` adds a common phase vocabulary:

- idle
- starting
- scanning
- placing
- waiting_fill
- reconciling
- closing
- paused
- failed
- stopped

`src/nadobro/services/order_intents.py` adds idempotent order intent envelopes so retries can be associated with a stable intent before/after a Nado digest is available.

Strategy status responses now include `strategy_phase` so Telegram and workflows can show recoverable states and allowed actions.

## Required Secrets

Credentials are read from environment variables only:

- `DMIND_API_KEY`
- `N8N_BASE_URL`, `N8N_API_KEY`, `N8N_WEBHOOK_SECRET`
- `INK_RPC_URL`, `INK_WS_URL`
- `ARKHAM_API_KEY`
- `CMC_API_KEY`, `COINGECKO_API_KEY`
- `COINGLASS_API_KEY`
- `DEFILLAMA_API_KEY`
- `GLASSNODE_API_KEY`
- `NFTGO_API_KEY`
- `ROOTDATA_API_KEY`
- `X_API_BEARER_TOKEN`
- `GLOBAL_NEWS_API_KEY`
- `POLYMARKET_API_KEY`
- `GOPLUS_API_KEY`
- `FMP_API_KEY`
- `OPENAI_API_KEY`, `XAI_API_KEY`

## Failure Modes

- DMind missing: show degraded mode.
- Provider missing: omit provider-backed claims and show missing provider in health.
- n8n missing: workflows remain drafts in Nadobro.
- Archive fill lag: strategy/trade status remains submitted/reconciling until fill sync resolves.
- Duplicate order retry: order intent suppresses duplicate live submission where an active intent already exists.
