# Nadobro - AI Trading Companion for Nado DEX

## Overview
Nadobro is a production-grade Telegram bot for trading on Nado DEX (perpetual futures and spot exchange on Ink blockchain). It supports natural language AI-powered trading, per-user encrypted wallet management, real-time position monitoring, and comprehensive risk management.

## Recent Changes
- 2026-02-09: Initial build - Full project structure, database models, Nado SDK integration, xAI Grok AI parser, Telegram bot with all commands, admin system, alert scheduler

## Architecture
```
main.py                          # Entry point - bot startup with polling
src/nadobro/
  config.py                      # Environment vars, product definitions, constants
  models/
    database.py                  # SQLAlchemy models (User, Trade, Alert, AdminLog, BotState)
  services/
    crypto.py                    # Wallet generation, AES-256 encryption, mnemonic recovery
    nado_client.py               # Nado SDK wrapper - market data, orders, positions
    ai_parser.py                 # xAI Grok natural language trade intent parser
    user_service.py              # User CRUD, wallet management, network switching
    trade_service.py             # Order execution, validation, rate limiting, history
    alert_service.py             # Price alerts CRUD and trigger checking
    admin_service.py             # Admin stats, pause trading, logs
    scheduler.py                 # APScheduler for background alert checking
  handlers/
    commands.py                  # All /command handlers for Telegram
    messages.py                  # Natural language message handler
```

## Key Technologies
- Python 3.11, python-telegram-bot (polling mode)
- Nado Protocol SDK (nado-protocol)
- xAI Grok (via OpenAI-compatible API)
- PostgreSQL (SQLAlchemy ORM)
- AES-256 Fernet encryption for private keys
- APScheduler for background tasks

## Environment Variables Required
- TELEGRAM_TOKEN: Bot token from @BotFather
- XAI_API_KEY: xAI API key for Grok AI
- ENCRYPTION_KEY: Auto-generated, used for wallet encryption
- ADMIN_USER_IDS: Comma-separated Telegram user IDs for admin access
- DATABASE_URL: Auto-configured by Replit PostgreSQL

## Supported Products
BTC-PERP, ETH-PERP, SOL-PERP, ARB-PERP, OP-PERP, DOGE-PERP, LINK-PERP, AVAX-PERP

## Bot Commands
Trading: /long, /short, /limit_long, /limit_short, /close, /close_all
Info: /positions, /balance, /price, /funding, /history, /analytics
Alerts: /alert, /my_alerts, /del_alert
Account: /wallet, /mode, /recover
Admin: /admin_stats, /admin_pause, /admin_logs

## Deployment
- Development: Polling mode via `python main.py`
- Production: Replit Deployments (VM type, always-on)

## User Preferences
- Production-grade, 24/7 uptime
- Per-user encrypted wallets with mnemonic recovery
- Rate limit: 1 trade per minute per user
- Max leverage: 50x
- Testnet and mainnet support with network switching
- No Builder ID integration (Phase 3)
- Multi-language support planned (Phase 2)
