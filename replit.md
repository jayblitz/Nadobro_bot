# Nadobro - AI Trading Companion for Nado DEX

## Overview
Nadobro is a Telegram trading bot for Nado DEX (perpetual futures on Ink blockchain). It provides a dual-interface UX: a persistent reply keyboard (grid button) for quick actions and a blue Menu button for slash commands. All free-text messages route to Ask Nado AI chat for natural language Q&A about Nado DEX. Per-user encrypted wallet management, real-time market data, position management, price alerts, and AI-powered knowledge Q&A.

## Recent Changes
- 2026-02-12: Intensive dev review cleanup. Removed dead ai_parser.py module (318 lines). Fixed debug_logger.py hardcoded macOS path → /tmp. Fixed keyboard inconsistency (all commands now use persistent_menu_kb). Moved _fmt_strategy_update to formatters.py. Cleaned up stale debug_log IDs.
- 2026-02-12: UX overhaul to dual-interface pattern. Added persistent reply keyboard (ReplyKeyboardMarkup, is_persistent=True) with action buttons. Added blue Menu button via set_my_commands (/start, /help, /status, /import_key, /stop_all). All free text now routes to Ask Nado AI chat. Removed AI intent parser flow.
- 2026-02-10: Added AI-powered "Ask Nado" knowledge Q&A feature. Created knowledge_service.py with xAI Grok + comprehensive Nado docs knowledge base (nado_knowledge.txt).
- 2026-02-10: Fixed limit order flow, enhanced fmt_positions with PnL and mark price display, hardened MarkdownV2 escaping.
- 2026-02-10: Rebuilt as Trojan-style pure bot. Removed Flask web UI/Mini App entirely.
- 2026-02-09: Production hardening - Encryption key validation, AI parser sanitization, comprehensive error handling
- 2026-02-09: Initial build - Full project structure, database models, Nado SDK integration, wallet encryption

## Architecture
```
main.py                          # Entry point - pure bot polling (no Flask)
src/nadobro/
  config.py                      # Environment vars, product definitions, constants
  data/
    nado_knowledge.txt           # Comprehensive Nado DEX documentation knowledge base
  models/
    database.py                  # SQLAlchemy models (User, Trade, Alert, AdminLog, BotState)
  services/
    crypto.py                    # Wallet generation, AES-256 encryption, mnemonic recovery
    nado_client.py               # Nado SDK wrapper - market data, orders, positions
    knowledge_service.py         # AI-powered Nado Q&A using docs knowledge base + xAI Grok
    user_service.py              # User CRUD, wallet management, network switching
    trade_service.py             # Order execution, validation, rate limiting, history
    alert_service.py             # Price alerts CRUD and trigger checking
    admin_service.py             # Admin stats, pause trading, logs
    scheduler.py                 # APScheduler for background alert checking
    settings_service.py          # User settings CRUD (leverage, slippage, risk profiles)
    onboarding_service.py        # Multi-step onboarding flow state machine
    bot_runtime.py               # Bot runtime state management (start/stop strategies)
    debug_logger.py              # Structured debug logging to /tmp and stdout
  handlers/
    formatters.py                # MarkdownV2 message formatting (dashboard, positions, strategy updates, etc.)
    keyboards.py                 # Persistent reply keyboard + inline keyboards for sub-flows
    commands.py                  # Slash commands: /start, /help, /status, /import_key, /stop_all
    callbacks.py                 # Inline button callback handler for all sub-flows
    messages.py                  # Free-text → Ask Nado AI, pending input handlers, reply keyboard dispatch
```

## Key Technologies
- Python 3.11
- python-telegram-bot (polling mode, persistent reply keyboard + inline keyboards, MarkdownV2)
- Nado Protocol SDK (nado-protocol)
- PostgreSQL (SQLAlchemy ORM)
- AES-256 Fernet encryption for private keys
- APScheduler for background alert checking
- xAI Grok API + OpenAI API (optional) for Nado knowledge Q&A

## Bot Interface
### Dual-Interface Pattern
1. **Blue Menu button** (Telegram native): /start, /help, /status, /import_key, /stop_all
2. **Persistent reply keyboard** (grid button): Trade Long/Short, Limit Buy/Sell, Wallet, Positions, Strategies, Markets, Status, Stop Bot, Alerts, Settings, Setup, Help
3. **Free text**: All free-text messages route directly to Ask Nado AI chat
4. **Inline keyboards**: Used for sub-flows (product picker, size presets, leverage selector, confirmations, etc.)

### Sub-Flows (inline keyboards)
- Trade: Product picker → Size presets → Leverage selector → Preview → Confirm
- Positions: View all → Close individual or close all
- Wallet: Balance, import key, rotate, remove, network switching, faucet
- Markets: Prices grid, funding rates, live price ticker
- Alerts: Set/view/delete price alerts
- Settings: Default leverage, slippage, risk profiles
- Strategies: Market Maker, Grid, Delta Neutral previews and configuration
- Onboarding: Mode → Key import → Funding → Risk → Template

## Environment Variables Required
- TELEGRAM_TOKEN: Bot token from @BotFather
- XAI_API_KEY: xAI API key for Grok AI
- OPENAI_API_KEY: OpenAI API key for higher-intelligence support answers (optional, recommended)
- ENCRYPTION_KEY: For wallet encryption
- DATABASE_URL: Auto-configured by Replit PostgreSQL

### Optional AI Routing Variables
- NADO_AI_PROVIDER: `auto` (default), `xai`, or `openai`
- NADO_AI_ESCALATE_ON_COMPLEX: `true`/`false` (default `true`, used in `auto` mode)
- XAI_SUPPORT_MODEL: Override xAI model (default: `grok-3-mini-fast`)
- OPENAI_SUPPORT_MODEL: Override OpenAI model (default: `gpt-4.1-mini`)

## Supported Products
BTC-PERP, ETH-PERP, SOL-PERP, XRP-PERP, BNB-PERP, LINK-PERP, DOGE-PERP, AVAX-PERP

## Deployment
- Development: `python main.py` (bot polling, console output)
- Production: Replit Deployments (VM type, always-on)

## User Preferences
- Production-grade, 24/7 uptime
- Per-user encrypted wallets with mnemonic recovery
- Rate limit: 1 trade per minute per user
- Max leverage: 50x
- Testnet and mainnet support with network switching
- Dual-interface: persistent reply keyboard + blue Menu button
- All free text → Ask Nado AI (no intent parsing)
