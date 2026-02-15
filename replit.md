# Nadobro - AI Trading Companion for Nado DEX

## Overview
Nadobro is a Telegram trading bot for Nado DEX (perpetual futures on Ink blockchain). It provides a dual-interface UX: a persistent reply keyboard (grid button) for quick actions and a blue Menu button for slash commands. All free-text messages route to Ask Nado AI chat for natural language Q&A about Nado DEX. Per-user encrypted wallet management, real-time market data, position management, price alerts, and AI-powered knowledge Q&A.

## Recent Changes
- 2026-02-15: Visual polish — colored buttons via api_kwargs style (green for confirm/start/long, red for cancel/close/stop/short). Streaming AI responses via sendMessageDraft (progressive text rendering like ChatGPT). Requires python-telegram-bot v22.6+ / Bot API 9.3+.
- 2026-02-15: Performance optimizations — added ChatAction.TYPING indicators for immediate feedback, moved ~12 lazy imports to top-level, added 10s in-memory user cache (with invalidation on state changes), added 5s market price cache. Webhook hardened with WEBHOOK_SECRET auth and strict price validation.
- 2026-02-15: Added Whale Strategy (Hybrid Whale Engine). New whale_strategy.py service with 3-state engine (long/short/neutral). Strategy Hub now shows Whale Engine as top option with preview, pair selection, size config, manual signal buttons, and status display. Added aiohttp webhook server on port 8099 for TradingView signal automation (POST /webhook with action+price+telegram_id JSON). Beginner-friendly explanations for every signal.
- 2026-02-12: Complete UX workflow refactor. Replaced Help button with Mode button for quick testnet/mainnet switching. Fixed all inline Back button flows — sub-flow dismissals now show clean status instead of dead-end dashboard loop. State-aware button validation prevents out-of-order trade steps. All 8 home buttons (Trade, Positions, Wallet, Markets, Strategies, Alerts, Settings, Mode) work end-to-end.
- 2026-02-12: Dynamic reply keyboard UX refactor. Trade flow now uses dynamic keyboard swaps (direction → order type → product → leverage → size → TP/SL → confirm) with Back navigation at each step. Home keyboard simplified to 8 buttons. Removed main_menu_kb() inline keyboard entirely. Sub-flows (Wallet, Positions, Markets, Alerts, Settings, Strategies) still use inline keyboards in chat while home keyboard stays visible. Trade flow state machine in messages.py with full custom size, limit price, and TP/SL input handling.
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
    whale_strategy.py            # Hybrid Whale Engine - 3-state strategy (long/short/neutral)
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
2. **Dynamic reply keyboard** (bottom panel): Home keyboard has 8 buttons (Trade, Positions, Wallet, Markets, Strategies, Alerts, Settings, Mode). Trade flow dynamically swaps keyboards through each step.
3. **Free text**: All free-text messages route directly to Ask Nado AI chat
4. **Inline keyboards**: Used for sub-flow data displays in chat (positions, wallet, markets, alerts, settings, strategies)

### Trade Flow (dynamic reply keyboards)
Home → Trade → Direction (Long/Short) → Order Type (Market/Limit) → Product → Leverage → Size → TP/SL (optional) → Confirm
- Each step swaps the reply keyboard and sends brief status to chat
- Back navigation returns to previous step at every point
- Custom size and limit price via free-text input
- TP/SL edit sub-flow with Set TP / Set SL / Done keyboard

### Sub-Flows (inline keyboards in chat)
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
