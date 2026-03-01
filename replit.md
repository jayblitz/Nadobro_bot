# Nadobro — Replit Project Guide

## Overview

Nadobro is a Telegram bot that serves as a trading interface for the Nado DEX (a CLOB-based perpetuals and spot trading exchange on the Ink L2 blockchain). Users interact with the bot via natural language and keyboard commands to view live market data, manage wallets, place trades, set price alerts, and run automated trading strategies — all without leaving Telegram.

**Core capabilities:**
- Natural language trade parsing (e.g., "long ETH 0.1 at 10x")
- Live market data, positions, PnL, and funding rates from Nado DEX
- Encrypted wallet management (private keys stored encrypted in Supabase)
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
  supabase_client.py   — singleton Supabase client
  models/database.py   — enums + all Supabase table CRUD (no ORM)
  handlers/            — Telegram update handlers (commands, callbacks, messages, trade card, home card)
  services/            — business logic (user, trade, alert, crypto, knowledge, scheduler, bot_runtime)
  strategies/          — MM bot, delta neutral, volume bot cycle logic (currently stubs)
  data/                — static knowledge base text for the AI assistant
```

### Frontend (Telegram UI)
- **Commands** (`/start`, `/help`, `/status`, etc.) handled in `handlers/commands.py`
- **Inline keyboards** for navigation and trade flows (`handlers/keyboards.py`)
- **Reply keyboards** for step-by-step trade card flow (persistent bottom menu)
- **Callback query router** in `handlers/callbacks.py` dispatches all button taps
- **Message handler** (`handlers/messages.py`) handles free-text input including natural language trade intents
- A "home card" pattern (`handlers/home_card.py`) acts as a persistent dashboard message that gets edited in place rather than sending new messages
- Trade flow runs as an inline "trade card" (`handlers/trade_card.py`) with session state stored in `context.user_data`

### Backend Services
| Service | Purpose |
|---|---|
| `user_service.py` | Create/fetch users, manage wallets, NadoClient per user |
| `trade_service.py` | Validate and execute market/limit orders, rate limiting |
| `alert_service.py` | CRUD for price alerts, check triggered alerts |
| `settings_service.py` | Per-user settings (leverage, slippage, strategy params) stored in Supabase bot_state |
| `onboarding_service.py` | Multi-step onboarding state machine (language → ToS → dashboard) |
| `bot_runtime.py` | Background strategy bot lifecycle (start/stop per user, APScheduler tasks) |
| `scheduler.py` | APScheduler AsyncIO scheduler; runs alert checks on interval |
| `crypto.py` | Fernet-based encryption/decryption of private keys; passphrase-based key derivation |
| `knowledge_service.py` | AI Q&A using xAI (Grok) or OpenAI, with local knowledge base fallback and live URL fetching |
| `nado_client.py` | HTTP wrapper around Nado REST API (prices, orders, positions, balance) |
| `admin_service.py` | Admin-only controls: pause trading, view stats, audit log |

### Data Layer
- **No ORM, no local database.** All persistence goes through **Supabase** (PostgreSQL-as-a-service) via the `supabase-py` client.
- Tables expected in Supabase: `users`, `trades`, `alerts`, `bot_state`, `admin_logs` (tables must be created via Supabase dashboard migrations — they are not auto-created by the app).
- The `bot_state` table acts as a flexible key-value store for settings, onboarding state, strategy state, etc. — avoids needing many small tables.
- A short in-memory LRU-style cache (`_user_cache`, `_price_cache`) with TTLs reduces Supabase round-trips for hot data.

### Wallet Encryption
- Each user's private key is encrypted with Fernet symmetric encryption before storage in Supabase.
- The `ENCRYPTION_KEY` environment variable must be a valid Fernet key (or a raw string that gets SHA-256 derived into one).
- Keys are validated at startup; the process exits if the key is missing or invalid.
- Optional passphrase-based encryption (`encrypt_with_passphrase`) is available for additional protection.

### Natural Language Trade Parsing
- `handlers/intent_parser.py` uses regex to extract product, direction, size, leverage, and order type from free-text messages.
- `handlers/intent_handlers.py` enriches parsed intents with user settings and shows a confirmation preview before executing.

### Trading Strategies (Background Bots)
- `bot_runtime.py` manages per-user asyncio tasks that run strategy cycles on a timer.
- Strategy state (running, parameters, last tick) is persisted in the Supabase `bot_state` table under a `strategy_bot:{user_id}:{network}` key.
- Three strategy skeletons exist in `src/nadobro/strategies/`: MM Bot, Delta Neutral, Volume Bot. Currently placeholders pending full implementation.

### Configuration
All configuration lives in `src/nadobro/config.py` and is read from environment variables. Key flags:
- `DUAL_MODE_CARD_FLOW` — enables the inline trade card UI pattern
- `RATE_LIMIT_SECONDS`, `MAX_LEVERAGE`, `MIN_TRADE_SIZE_USD` — trading safety constants
- Product IDs and aliases for BTC, ETH, SOL, XRP, BNB, LINK, DOGE, AVAX are hardcoded here

## External Dependencies

### Required Services
| Service | Purpose | Config Keys |
|---|---|---|
| **Telegram Bot API** | All user interaction (python-telegram-bot library) | `TELEGRAM_TOKEN` |
| **Supabase** | Persistent storage for users, trades, alerts, settings | `SUPABASE_URL`, `SUPABASE_KEY` |
| **Nado DEX REST API** | Market data, order placement, position management | No key needed; testnet/mainnet URLs in config |

### Optional Services
| Service | Purpose | Config Keys |
|---|---|---|
| **xAI (Grok)** | AI-powered "Ask Nado" Q&A feature | `XAI_API_KEY` |
| **OpenAI** | Fallback AI for Q&A | `OPENAI_API_KEY` |

### Key Python Packages
- `python-telegram-bot` — Telegram bot framework
- `supabase` — Supabase Python client
- `cryptography` — Fernet encryption for private keys
- `eth-account` — Ethereum key derivation and signing
- `nado_protocol` — Official Nado DEX SDK (order building, signing, subaccount utils)
- `apscheduler` — Background job scheduler for alerts and strategy bots
- `openai` — OpenAI/xAI API client
- `requests` — HTTP calls to Nado REST API
- `python-dotenv` — Local `.env` loading for development

### Deployment
- Designed to run as a single Python process with long polling
- Documented deployment targets: Fly.io (via `fly.toml` + Docker), Oracle Cloud Always Free, or any VPS with a process manager
- Secrets are injected as environment variables (never committed); a helper script `run_setup_secrets.py` prompts securely and writes to `.env`
- No webhook server needed; long polling works without a public URL