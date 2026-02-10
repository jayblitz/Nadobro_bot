# Nadobro - AI Trading Companion for Nado DEX

## Overview
Nadobro is a Telegram Mini App for trading on Nado DEX (perpetual futures on Ink blockchain). It provides a full web-based trading interface inside Telegram, with per-user encrypted wallet management, real-time market data, position management, and price alerts.

## Recent Changes
- 2026-02-10: Pivoted from Telegram bot UI to Telegram Mini App. Built Flask web server serving a modern dark-themed trading interface. Bot now sends "Open Nadobro" button that launches the Mini App inside Telegram. All trading functionality available through the web UI.
- 2026-02-09: Production hardening - Encryption key validation, AI parser sanitization, comprehensive error handling
- 2026-02-09: Initial build - Full project structure, database models, Nado SDK integration, wallet encryption

## Architecture
```
main.py                          # Entry point - runs Flask + bot polling together
templates/
  index.html                     # Mini App HTML (single-page app)
static/
  css/app.css                    # Dark trading theme CSS
  js/app.js                      # Frontend logic (tabs, trading, positions, wallet)
src/nadobro/
  config.py                      # Environment vars, product definitions, constants
  api.py                         # Flask REST API endpoints
  webapp_auth.py                 # Telegram WebApp initData validation
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
    commands.py                  # /start and /help with Mini App launch button
    messages.py                  # Redirects text to Mini App
    callbacks.py                 # Legacy callback handlers
    keyboards.py                 # Legacy inline keyboards
```

## Key Technologies
- Python 3.11, Flask (web server on port 5000)
- Telegram Mini App (WebApp SDK for auth + theming)
- python-telegram-bot (polling mode for notifications)
- Nado Protocol SDK (nado-protocol)
- PostgreSQL (SQLAlchemy ORM)
- AES-256 Fernet encryption for private keys
- APScheduler for background alert checking

## API Endpoints
- GET /api/user - Get/create user
- GET /api/balance - Wallet balance
- GET /api/positions - Open positions
- GET /api/prices - All market prices
- POST /api/trade - Execute trade (market/limit)
- POST /api/close - Close position(s)
- GET /api/history - Trade history
- GET /api/analytics - Trading stats
- GET /api/wallet - Wallet info
- POST /api/network - Switch testnet/mainnet
- GET/POST/DELETE /api/alerts - Price alerts
- GET /api/products - Available products

## Environment Variables Required
- TELEGRAM_TOKEN: Bot token from @BotFather
- XAI_API_KEY: xAI API key for Grok AI
- ENCRYPTION_KEY: For wallet encryption
- DATABASE_URL: Auto-configured by Replit PostgreSQL

## Supported Products
BTC-PERP, ETH-PERP, SOL-PERP, XRP-PERP, BNB-PERP, LINK-PERP, DOGE-PERP, AVAX-PERP

## Deployment
- Development: `python main.py` (Flask on port 5000 + bot polling)
- Production: Replit Deployments (VM type, always-on)
- Mini App URL must be set in BotFather: use the Replit deployment URL

## User Preferences
- Production-grade, 24/7 uptime
- Per-user encrypted wallets with mnemonic recovery
- Rate limit: 1 trade per minute per user
- Max leverage: 50x
- Testnet and mainnet support with network switching
- Telegram Mini App (not chat-based bot UI)
