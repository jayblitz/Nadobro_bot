# Nadobro — Replit Project Guide

## Latest Updates (March 2026)

- **Copy Trading frontend UX**: Full Telegram bot UI for copy trading wired into Strategy Hub. "🔁 Copy Trading" button in strategy hub → copy hub with trader list, My Copies dashboard, custom wallet input, admin trader management. Setup flow: trader selection → budget ($50-$1000 presets) → risk factor (0.5x-5x) → max leverage (5x-40x) → confirm with passphrase auth. Dashboard shows active mirrors with stop buttons, filled/failed trade counts. Custom wallet input validates 0x address format. Admin sub-menu: add/remove traders (curated). All buttons have i18n labels (zh, fr, ar, ru, ko). Files: keyboards.py (copy_hub_kb, copy_trader_preview_kb, copy_budget_kb, copy_risk_kb, copy_leverage_kb, copy_confirm_kb, copy_dashboard_kb, copy_admin_menu_kb), callbacks.py (_handle_copy dispatcher), messages.py (start_copy action + wallet text handlers).
- **Copy Trading backend infrastructure**: Monitors Hyperliquid traders via WebSocket (`wss://api.hyperliquid.xyz/ws`) and mirrors their trades on Nado DEX. Components: `hl_client.py` (REST client for HL info API), `copy_asset_map.py` (HL coin → Nado product ID mapping), `copy_service.py` (mirror trade execution with proportional sizing), `hl_websocket.py` (persistent WS with auto-reconnect and dynamic wallet subscription). DB tables: `copy_traders` (wallet registry), `copy_mirrors` (per-user copy configs with budget/risk_factor/max_leverage), `copy_trades` (execution log). Up to 5 simultaneous copied traders per user. Size calculation: `hl_size * (user_budget / leader_equity) * risk_factor`. Admin functions in `admin_service.py` for trader management and stats. Lifecycle integrated in `main.py` (start on boot, clean shutdown). Controlled by `NADO_COPY_TRADING` env var (default: true).
- **i18n localization wired into all bot flows**: The existing i18n system (`i18n.py`) is now connected across all handler entry points (callbacks, messages, commands), trade flows (trade card, intent handlers), service notifications (bot_runtime, scheduler), and AI chat (knowledge_service). `language_context()` wraps every handler to set the user's preferred language from DB. `localize_text()` and `localize_markup()` are applied at all output chokepoints. AI system prompts (casual, synthesizer, X/Twitter) include language instruction for non-English users. `resolve_reply_button_text()` maps translated keyboard buttons back to English for routing. Translation dictionary in `_TEXTS` and `_LABELS` covers 5 languages (zh, fr, ar, ru, ko).
- **`/status` command UX overhaul**: Removed raw Perf Snapshot (p50/p95/avg ms developer metrics) from user-facing output. Redesigned status display: shows live uptime, next scan countdown, last action with detail, Bro Mode trade count/PnL/open positions, error streak warnings, and cleaner onboarding section that only shows incomplete steps. Strategy states `last_action` and `last_action_detail` now persisted in bot state for both Bro Mode and non-bro strategies. Status no longer shows confusing "COMPLETE"/"IDLE" — shows "NOT RUNNING" when stopped (with last action and stop reason) or "RUNNING" with live cycle info when active.
- **Security audit & hardening**: Token redaction filter added to main.py (`_TokenRedactFilter`) — all log output is now sanitized so bot tokens, API keys in URLs are never logged in plaintext. `attached_assets/` directory added to `.gitignore` and removed from git tracking. Git history purged of leaked Telegram Bot Token using `git filter-repo`. Force-pushed clean history to GitHub. Additional `.gitignore` entries: `.env.*`, `*.pem`, `*.key`, `secrets.json`, `.secrets/`.
- Bro Mode silent hold UX bug fixed: users now receive Telegram notifications for every Bro Mode cycle (hold reasoning, trade actions, blocked states). Periodic updates every 6th consecutive hold.
- Intensive codebase review: 12 bug fixes across all major components — Bro Mode max_positions enforcement + position collision guard, routing bypass for Ink/Nado questions, source filter cap increased (1→3 links), dead search_web tool removed, volume bot stale order cleanup, MM bot duplicate order prevention via price tolerance, bot_runtime state desync guard, HOWL 48h expiry on stale pending suggestions, LLM confidence normalization (handles 0-100 scale), execution_queue dedupe cleanup throttled, budget_guard margin uses real leverage per position, DB index on users.last_active.
- CoinMarketCap AI Skills integrated: 3 new agent tools (get_crypto_info, get_trending_cryptos, get_global_market_data) powered by CMC API. Provides market cap, volume, price changes, trending coins, gainers/losers, BTC dominance, total market cap. CMC Fear & Greed Index replaces api.alternative.me as primary source. Agent tools now 7 total (4 base + 3 CMC). CMC tools conditionally loaded only when CMC_API_KEY is set. Caching: 2min for quotes/trending/global, 5min for sentiment/news.
- Strategy dashboard simplified: removed 5-section layout (Setup Flow, How It Works, Settings, Config, Analytics), replaced with compact title+explainer, Settings block, key Analytics (margin, volume, max loss, net estimate). Onboarding messages shortened. Intro video sent to new users on first /start.
- AI agent transformed into conversational trading companion ("mini Alexa"): conversation memory (8-msg buffer, 15min TTL), personalized greetings using Telegram name, casual chat handling, trade format suggestions.
- Added live price tool: get_live_price fetches real-time bid/ask/mid from Nado API via NadoClient.
- Enhanced market sentiment: Fear & Greed Index (CMC primary, api.alternative.me fallback) + broader crypto news from WatcherGuru, CoinDesk, Cointelegraph, whale_alert via xAI search.
- Knowledge base expanded to ~350 lines with FAQ, getting started guide, fee tiers, supported markets, security.
- Agent tools now 7: search_knowledge_base, get_live_price, get_market_sentiment, search_x_twitter, get_crypto_info, get_trending_cryptos, get_global_market_data. Strict source filtering enforces official URLs + coinmarketcap.com.
- Trade close tracking: close_position/close_all_positions now record close price, PnL, closed_at, and status=closed in DB. find_open_trade helper links closes to opens. Trade history and analytics include close data. ALTER TABLE migration adds close_price and closed_at columns.
- Trade rate limit reduced from 60s to 5s; only counts filled trades (not pending/closed).
- Webhook mode deployed on Fly.io Amsterdam; single machine with min/max_machines_running=1.
- Improved strategy UX and execution reliability for smoother bot operation.
- Added Volume Bot to the strategy hub and settings flow.
- Fixed strategy runtime signing and execution flow issues.

## Overview

Nadobro is a Telegram bot that serves as a trading interface for the Nado DEX (a CLOB-based perpetuals and spot trading exchange on the Ink L2 blockchain). Users interact with the bot via natural language and keyboard commands to view live market data, manage wallets, place trades, set price alerts, and run automated trading strategies — all without leaving Telegram.

**Core capabilities:**
- Natural language trade parsing (e.g., "long ETH 0.1 at 10x")
- Live market data, positions, PnL, and funding rates from Nado DEX
- Linked Signer wallet management (no raw private key import — bot generates a signer key, user authorizes it on Nado)
- Automated trading strategies: Market Making (Grid/RGRID), Delta Neutral, Volume Bot
- Price and funding alerts via background scheduler
- Admin controls (pause trading, view stats)
- Testnet and mainnet support

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Entry Point
`main.py` starts the bot. It validates the `ENCRYPTION_KEY` before anything else, then boots the Telegram polling loop. The app uses **python-telegram-bot** with long polling (not webhooks) as the default deployment model.

### Package Layout
```
src/nadobro/
  config.py            — all env vars + product/market constants
  db.py                — PostgreSQL connection pool (psycopg2 + DATABASE_URL)
  models/database.py   — enums + all PostgreSQL table CRUD (raw SQL, no ORM)
  handlers/            — Telegram update handlers (commands, callbacks, messages, trade card, home card)
  services/            — business logic (user, trade, alert, crypto, knowledge, scheduler, bot_runtime)
  strategies/          — MM bot, delta neutral, volume bot cycle logic
  data/                — static knowledge base text for the AI assistant
```

### Frontend (Telegram UI)
- **Commands** (`/start`, `/help`, `/status`, `/revoke`, `/stop_all`) handled in `handlers/commands.py`
- **Inline keyboards** for navigation and trade flows (`handlers/keyboards.py`)
- **Reply keyboards** for step-by-step trade card flow (persistent bottom menu)
- **Callback query router** in `handlers/callbacks.py` dispatches all button taps
- **Message handler** (`handlers/messages.py`) handles free-text input including natural language trade intents
- A "home card" pattern (`handlers/home_card.py`) acts as a persistent dashboard message that gets edited in place rather than sending new messages
- Trade flow runs as an inline "trade card" (`handlers/trade_card.py`) with session state stored in `context.user_data`

### Backend Services
| Service | Purpose |
|---|---|
| `user_service.py` | Create/fetch users, manage linked signer wallets, NadoClient per user |
| `trade_service.py` | Validate and execute market/limit orders, rate limiting |
| `alert_service.py` | CRUD for price alerts, check triggered alerts |
| `settings_service.py` | Per-user settings (leverage, slippage, strategy params) stored in bot_state |
| `onboarding_service.py` | Multi-step onboarding state machine (language → ToS → dashboard) |
| `bot_runtime.py` | Background strategy bot lifecycle (start/stop per user, APScheduler tasks) |
| `scheduler.py` | APScheduler AsyncIO scheduler; runs alert checks on interval |
| `crypto.py` | Passphrase-based encryption (PBKDF2 600k + Fernet) for linked signer keys |
| `knowledge_service.py` | Grok-style conversational AI: witty/opinionated crypto personality, not limited to KB Q&A. Router LLM dispatches to 7 tools (search_knowledge_base, get_live_price, get_market_sentiment, search_x_twitter, get_crypto_info, get_trending_cryptos, get_global_market_data). CMC tools conditionally available when CMC_API_KEY set. Per-user conversation memory (12-msg, 30min TTL). General chat handled by LLM's own knowledge with Grok personality. Smart points distribution detection (weekly Friday epochs, X search for announcements). Temperature 0.5-0.6 for natural responses. xAI primary + OpenAI fallback. |
| `cmc_client.py` | CoinMarketCap API client: crypto quotes, global metrics, Fear & Greed, trending/gainers/losers, news. 2-5min caching. Requires CMC_API_KEY env var. |
| `nado_client.py` | HTTP wrapper around Nado REST API (prices, orders, positions, balance) |
| `admin_service.py` | Admin-only controls: pause trading, view stats, audit log, copy trader management |
| `hl_client.py` | Async HTTP client for Hyperliquid info API (clearinghouseState, userFills, allMids) |
| `hl_websocket.py` | Persistent WebSocket to HL (`wss://api.hyperliquid.xyz/ws`), subscribes to userFills per wallet, auto-reconnect |
| `copy_service.py` | Copy trading logic: mirror HL fills to Nado trades, proportional sizing, user notifications |
| `copy_asset_map.py` | HL coin name → Nado product ID mapping (BTC→2, ETH→4, SOL→8, etc.) |

### Data Layer
- **Replit PostgreSQL** via `DATABASE_URL` environment variable. Connection pooling via `psycopg2.pool.ThreadedConnectionPool` in `src/nadobro/db.py`.
- Raw SQL queries (no ORM). Helper functions: `query_one`, `query_all`, `execute`, `execute_returning`, `query_count`.
- Tables: `users`, `trades_testnet`, `trades_mainnet`, `alerts_testnet`, `alerts_mainnet`, `bot_state`, `admin_logs`, `copy_traders`, `copy_mirrors`, `copy_trades` — auto-created by `init_db()` on startup.
- **Network-separated tables**: Trades and alerts use separate tables per network (`trades_testnet`/`trades_mainnet`, `alerts_testnet`/`alerts_mainnet`) to prevent cross-network data leakage. Legacy unified `trades`/`alerts` tables are retained as backup; all new reads/writes go to network-specific tables. Migration from legacy tables runs automatically on first startup.
- The `bot_state` table acts as a flexible key-value store for settings, onboarding state, strategy state, etc.
- A short in-memory LRU-style cache (`_user_cache`, `_price_cache`) with TTLs reduces DB round-trips for hot data.

### Wallet — 1-Click Trading (1CT) Model
- **No raw main-wallet key import.** Users link wallets via Nado's 1CT (1-Click Trading) flow:
  1. Bot generates a new Ethereum keypair (the "1CT signer")
  2. Bot shows the 1CT **private key** to the user
  3. User pastes the key into Nado's web UI (Settings → 1-Click Trading → Advanced 1CT → "1CT Private Key" field), enables the toggle, and saves (1 USDT0 fee, signed by their connected browser wallet)
  4. User sends their main wallet address back to the bot
  5. User sets a strong passphrase; bot encrypts the 1CT key with PBKDF2 600k iterations + Fernet
  6. Encrypted key + salt stored in `users` table; main wallet address stored separately
- The 1CT key can only sign trades — it cannot withdraw funds from the user's account
- The `ENCRYPTION_KEY` env var provides a Fernet key for general encryption validation at startup
- Users can revoke/unlink at any time via the Wallet button or `/revoke` command

#### Read-Only vs Signing Client Pattern
- **`get_user_readonly_client(telegram_id)`** — creates a `NadoClient.from_address(main_address, network)` with no private key; used for all READ operations (balance, prices, positions, market data). Cached in `_readonly_cache` by address+network.
- **`get_user_nado_client(telegram_id, passphrase)`** — decrypts the 1CT key with the user's passphrase; required ONLY for trade SIGNING (placing/closing orders). Returns `None` if passphrase is missing or wrong.
- Trade validation (`validate_trade`) uses `ensure_active_wallet_ready` + readonly client for balance checks; passphrase is only needed at execution time.

#### Per-Trade Passphrase Collection Flow
- When a trade is confirmed (via any path — trade flow, text intent, callback button, trade card), the bot prompts: "🔑 Enter your passphrase to sign this trade:"
- The passphrase message is immediately deleted from chat for security after the bot reads it.
- Passphrase is passed to `execute_market_order`/`execute_limit_order`/`close_position`/`close_all_positions` which decrypt the 1CT key on demand.
- The passphrase is NOT cached in memory — each trade requires fresh entry for maximum security.
- The `PENDING_PASSPHRASE_ACTION` key in `context.user_data` stores the pending action details while waiting for passphrase input.
- The `_prompt_passphrase()` and `_handle_passphrase_input()` functions in `messages.py` handle the entire flow centrally.
- Cancelling (via nav:main button) clears the pending passphrase state.

### Natural Language Trade Parsing
- `handlers/intent_parser.py` uses regex to extract product, direction, size, leverage, and order type from free-text messages.
- `handlers/intent_handlers.py` enriches parsed intents with user settings and shows a confirmation preview before executing.

### Trading Strategies (Background Bots)
- `bot_runtime.py` manages per-user asyncio tasks that run strategy cycles on a timer.
- Strategy state (running, parameters, last tick) is persisted in the `bot_state` table under a `strategy_bot:{user_id}:{network}` key.
- `_dispatch_strategy()` routes: mm/grid → `strategies/mm_bot.py`, dn → `strategies/delta_neutral.py`, vol → `strategies/volume_bot.py`, bro → `strategies/bro_mode.py`
- SUPPORTED_STRATEGIES = ("mm", "grid", "dn", "vol", "bro")

### Bro Mode — Autonomous LLM Quant Agent
- **Architecture**: Grok-3 structured JSON decision engine (`services/bro_llm.py`), market scanner (`services/market_scanner.py`), budget guard (`services/budget_guard.py`), price tracker (`services/price_tracker.py`)
- **Strategy cycle** (`strategies/bro_mode.py`): 5-min cycle scans all assets, builds market snapshots (technicals + funding + CMC + X sentiment), sends to Grok-3 for structured decisions (open/close/hold/emergency_flatten), manages full position lifecycle
- **Product**: Uses "MULTI" as product (not single-asset); `start_user_bot` has dedicated bro early-return that skips product_id validation
- **Budget Guard**: 3 risk profiles (conservative/balanced/aggressive), exposure tracking, emergency flatten, min margin buffer
- **Price Tracker**: Rolling price history, RSI-14, EMA-9/21/50, MACD, Bollinger Bands, 1hr/4hr signals, 60s tick
- **HOWL** (nightly optimization): `services/howl_service.py` runs at 02:00 UTC via scheduler, sends suggestions to Telegram with approve/reject/dismiss keyboard
- **Settings**: `strategies.bro` in user settings — budget_usd, risk_level, min_confidence, leverage_cap, max_positions, tp_pct, sl_pct, max_loss_pct, cycle_seconds, products
- **UI**: Bro Mode card in strategy hub, config keyboard, risk preset picker, HOWL approval flow, pending bro input handler in messages.py

### Configuration
All configuration lives in `src/nadobro/config.py` and is read from environment variables. Key flags:
- `DUAL_MODE_CARD_FLOW` — enables the inline trade card UI pattern
- `RATE_LIMIT_SECONDS`, `MAX_LEVERAGE`, `MIN_TRADE_SIZE_USD` — trading safety constants
- `EST_FEE_RATE`, `EST_FILL_EFFICIENCY` — estimation constants for strategies
- Product IDs and aliases for BTC, ETH, SOL, XRP, BNB, LINK, DOGE, AVAX are hardcoded here

## External Dependencies

### Required Services
| Service | Purpose | Config Keys |
|---|---|---|
| **Telegram Bot API** | All user interaction (python-telegram-bot library) | `TELEGRAM_TOKEN` |
| **Replit PostgreSQL** | Persistent storage for users, trades, alerts, settings | `DATABASE_URL` (auto-provided) |
| **Nado DEX REST API** | Market data, order placement, position management | No key needed; testnet/mainnet URLs in config |

### Optional Services
| Service | Purpose | Config Keys |
|---|---|---|
| **xAI (Grok)** | AI-powered "Ask Nado" Q&A feature (primary) | `XAI_API_KEY` |
| **OpenAI** | Fallback AI for Q&A | `OPENAI_API_KEY` |
| **CoinMarketCap** | Market data, quotes, trending, Fear & Greed, global metrics | `CMC_API_KEY` |

### Key Python Packages
- `python-telegram-bot` — Telegram bot framework
- `psycopg2-binary` — PostgreSQL driver for Replit PG
- `cryptography` — Fernet encryption for linked signer keys
- `eth-account` — Ethereum key derivation and signing
- `nado_protocol` — Official Nado DEX SDK (order building, signing, subaccount utils)
- `apscheduler` — Background job scheduler for alerts and strategy bots
- `openai` — OpenAI/xAI API client
- `requests` — HTTP calls to Nado REST API
- `pysocks` — SOCKS5 proxy support for requests library
- `python-dotenv` — Local `.env` loading for development

### Deployment
- Production: Fly.io Amsterdam (`nadobro-bot.fly.dev`), single machine (`max_machines_running=1`), 1GB RAM, `shared-cpu-1x`
- Transport: webhook mode (`TELEGRAM_TRANSPORT=webhook`), webhook URL set to Fly.io endpoint
- Replit used for development; Fly.io for production to bypass geo restrictions
- Secrets injected as environment variables via Replit Secrets panel and `fly secrets set`
- Dockerfile uses requirements.txt; `python-telegram-bot[webhooks]==22.6` required for webhook mode

### Project Structure (root)
```
main.py          — entry point
pyproject.toml   — Python dependencies
uv.lock          — locked dependency versions
replit.md        — this file
.replit           — Replit workspace config
.gitignore       — Git ignore rules
src/nadobro/     — all source code
attached_assets/ — reference images/docs from user
```
