# Nadobro - AI Trading Companion for Nado DEX

## Overview
Nadobro is a Telegram trading bot for Nado DEX (perpetual futures on Ink blockchain). It provides a rich, button-driven Trojan-style interface with inline keyboards for all interactions, per-user encrypted wallet management, real-time market data, position management, price alerts, AI-powered natural language command parsing, and AI-powered Nado knowledge Q&A.

## Recent Changes
- 2026-02-10: Added AI-powered "Ask Nado" knowledge Q&A feature. Created knowledge_service.py with xAI Grok + comprehensive Nado docs knowledge base (nado_knowledge.txt). Updated ai_parser to detect nado_question intent. Added Ask Nado button to main menu. Users can ask any question about Nado DEX and get AI-powered answers.
- 2026-02-10: Fixed limit order flow (pending_trade step set correctly in _handle_product), enhanced fmt_positions with PnL and mark price display, hardened MarkdownV2 escaping (backslash handling).
- 2026-02-10: Rebuilt as Trojan-style pure bot. Removed Flask web UI/Mini App entirely. New button-driven interface with MarkdownV2 formatting, inline keyboards for multi-step trade flows, position management, wallet ops, alerts, and settings. Added formatters.py for rich message formatting.
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
    ai_parser.py                 # xAI Grok natural language trade intent parser (+ nado_question)
    knowledge_service.py         # AI-powered Nado Q&A using docs knowledge base + xAI Grok
    user_service.py              # User CRUD, wallet management, network switching
    trade_service.py             # Order execution, validation, rate limiting, history
    alert_service.py             # Price alerts CRUD and trigger checking
    admin_service.py             # Admin stats, pause trading, logs
    scheduler.py                 # APScheduler for background alert checking
  handlers/
    formatters.py                # MarkdownV2 message formatting (dashboard, positions, etc.)
    keyboards.py                 # Trojan-style inline keyboard layouts (incl. Ask Nado button)
    commands.py                  # /start (dashboard) and /help commands
    callbacks.py                 # Button-driven trade/position/wallet/alert/settings/ask_nado flows
    messages.py                  # AI-powered natural language handler + pending input + Q&A
```

## Key Technologies
- Python 3.11
- python-telegram-bot (polling mode, inline keyboards, MarkdownV2)
- Nado Protocol SDK (nado-protocol)
- PostgreSQL (SQLAlchemy ORM)
- AES-256 Fernet encryption for private keys
- APScheduler for background alert checking
- xAI Grok API for natural language trade parsing and Nado knowledge Q&A

## Bot Interface Flows
- Trade: Product picker -> Size presets -> Leverage selector -> Preview -> Confirm
- Positions: View all -> Close individual or close all
- Wallet: Balance, address, network switching, faucet
- Markets: Prices grid, funding rates
- Alerts: Set/view/delete price alerts
- Ask Nado: AI-powered Q&A about Nado DEX (margin, liquidation, fees, order types, NLP, etc.)
- Settings: Default leverage, slippage
- AI: Natural language commands parsed via xAI Grok (also detects Nado questions)

## Environment Variables Required
- TELEGRAM_TOKEN: Bot token from @BotFather
- XAI_API_KEY: xAI API key for Grok AI
- ENCRYPTION_KEY: For wallet encryption
- DATABASE_URL: Auto-configured by Replit PostgreSQL

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
- Trojan-style button-driven bot interface
