# Nadobro Scalable System Architecture

Nadobro is now prioritized as a Telegram-first trading product with a platform-grade backend. The Mini App remains archived until the bot, execution layer, and portfolio read models are reliable under real trading load.

## Architecture

The product should be organized around a few durable backend capabilities rather than UI-specific flows.

1. Telegram Bot UX
   - Primary user surface for trading, portfolio, strategy control, alerts, and AI assistance.
   - Should call service-layer read models and command handlers, not assemble exchange data inline.

2. Read Model Services
   - Portfolio, positions, order history, account health, strategy status, and market intelligence.
   - These services merge live Nado data, archive/indexer data, local DB state, and cached snapshots.

3. Execution Services
   - Order validation, placement, cancellation, strategy lifecycle, and signer/key safety.
   - Must remain idempotent and recoverable after process restarts.

4. Intelligence and Workflow Services
   - NanoGPT for finance-aware reasoning.
   - n8n for event workflows that call stable Nadobro backend commands instead of directly owning trading state.

5. Persistence
   - PostgreSQL for durable user, trade, strategy, fill-sync, and audit data.
   - Redis can be added for hot snapshots, strategy queues, and short-lived market/account state once traffic grows.

6. Nado Integration Layer
   - SDK and REST gateway for live state.
   - Archive indexer for isolated subaccounts, fills, historical orders, funding, and reconciliation.

## Component Structure

Current active backend modules should move toward this ownership model:

- `src/nadobro/handlers/`: Telegram presentation and interaction routing only.
- `src/nadobro/services/portfolio_service.py`: portfolio snapshot read model.
- `src/nadobro/services/trade_service.py`: trade commands and local trade ledger helpers.
- `src/nadobro/services/nado_client.py`: Nado SDK/REST client, product and subaccount live state.
- `src/nadobro/services/nado_archive.py`: archive/indexer query adapter.
- `src/nadobro/services/bot_runtime.py`: strategy orchestration and lifecycle.
- `src/nadobro/services/workflow_service.py`: n8n workflow generation/deployment.
- `src/nadobro/services/dmind_service.py` and `nanogpt_client.py`: financial LLM routing.
- `miniapp_api/` and `miniapp_web/`: archived until explicitly re-enabled.

## Data Flow

Portfolio read path:

1. User taps Portfolio in Telegram.
2. Handler calls `get_portfolio_snapshot()`.
3. Portfolio service resolves active network and read-only Nado client.
4. Live Nado positions are fetched from default and isolated subaccounts.
5. Live market prices are fetched in bulk.
6. Open orders are fetched from product catalog plus products observed in recent local trades.
7. Local trade ledger supplies conservative position hints if live isolated discovery misses a product.
8. Formatter renders the read model for Telegram.

Trade execution path:

1. Handler parses intent and validates readiness.
2. `trade_service` validates product, balance, price, leverage, isolated-only rules, and builder routing config.
3. `nado_client` submits the order.
4. Trade row is inserted/updated in PostgreSQL.
5. Archive fill sync resolves final fills, fees, slippage, and realized PnL.
6. Strategy/runtime state is reconciled from DB and exchange state on future cycles.

Workflow path:

1. User asks for an automation or agent workflow.
2. `workflow_service` generates n8n JSON using NanoGPT or fallback templates.
3. n8n invokes stable Nadobro API/command endpoints.
4. Backend owns all validation, execution, and audit logging.

## API Design

The active Telegram code should be treated as one API consumer. Future HTTP/n8n endpoints should expose the same service-layer contracts.

Recommended internal APIs:

- `GET /v1/portfolio/snapshot`
  - Returns positions, open orders, prices, stats, source freshness, and cache metadata.
- `GET /v1/account/readiness`
  - Returns onboarding, wallet, signer, pause, and network status.
- `POST /v1/orders`
  - Creates a validated order intent and submits to Nado.
- `POST /v1/orders/{digest}/cancel`
  - Cancels an order after ownership and network validation.
- `POST /v1/strategies/{strategy}/start`
  - Starts a strategy session from normalized settings.
- `POST /v1/strategies/{session_id}/stop`
  - Stops a strategy and invokes centralized cleanup.
- `POST /v1/workflows`
  - Builds/deploys an n8n workflow from prompt or template.

Public endpoints should be thin. They should call services and return typed response models; they should not duplicate Nado, DB, or strategy logic.

## Database Schema Direction

Existing tables should remain the source of truth, but the next schema layer should make reconciliation first-class.

Recommended additions:

- `portfolio_snapshots`
  - `id`, `user_id`, `network`, `payload_json`, `source_versions_json`, `created_at`
  - Optional durable snapshot table for support/debugging and daily analytics.

- `account_reconciliation_runs`
  - `id`, `user_id`, `network`, `status`, `started_at`, `finished_at`, `error_message`
  - Tracks background exchange vs DB reconciliation.

- `exchange_positions`
  - `user_id`, `network`, `subaccount_hex`, `product_id`, `amount`, `side`, `entry_price`, `unrealized_pnl`, `source`, `observed_at`
  - Stores latest observed live positions for fast reads and incident review.

- `exchange_open_orders`
  - `user_id`, `network`, `subaccount_hex`, `product_id`, `digest`, `side`, `size`, `price`, `status`, `observed_at`
  - Stores latest observed open orders and supports diff-based notifications.

- `order_intents`
  - Already introduced conceptually. It should become the durable idempotency boundary for all execution paths.

## Caching Strategy

Use cache close to ownership:

- Portfolio snapshot cache
  - Short TTL, per user/network.
  - Prevents repeated Telegram refreshes from fanning out to Nado and Postgres.
  - Current implementation is in-memory; Redis is the next step for multi-machine deploys.

- Product catalog cache
  - Short TTL with dynamic catalog refresh.
  - Must support new Nado products like xStocks without code deploys.

- Market price cache
  - Very short TTL, bulk fetch preferred.
  - Consumer code should tolerate missing prices and fall back to position prices for display.

- Open-order cache
  - Very short TTL.
  - Force refresh should bypass cache when the user taps Refresh or after order mutation.

- Strategy state cache
  - Runtime hot state can be cached, but DB/exchange reconciliation must rebuild it after restart.

## Implementation Code Added

`src/nadobro/services/portfolio_service.py` is the first service-layer read model:

- `get_portfolio_snapshot()` merges live positions, market prices, trade analytics, open orders, and local ledger hints.
- `PortfolioSnapshot` provides a stable contract for Telegram and future API/n8n consumers.
- A bounded TTL cache prevents duplicate refresh bursts from hitting Nado and Postgres.
- `clear_portfolio_snapshot_cache()` gives tests and future mutation paths a way to invalidate snapshots.

The Telegram Portfolio card now consumes this read model instead of assembling portfolio data inline.

## Rollout Plan

1. Keep Mini App archived and ship bot-first improvements.
2. Use `portfolio_service` as the canonical Portfolio read model.
3. Move account readiness, order submit, and strategy status toward the same read/command service split.
4. Add a reconciliation worker that periodically persists `exchange_positions` and `exchange_open_orders`.
5. Promote in-memory portfolio/product/order caches to Redis if running more than one app machine.
6. Expose thin HTTP endpoints only after service contracts are stable.
