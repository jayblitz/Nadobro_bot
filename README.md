# Nadobro Bot

Nadobro is a Telegram trading bot for Nado DEX that lets users manage wallets, place trades, monitor markets, and run automated strategies from chat.

## Latest Updates (March 2026)

- Added webhook transport mode (`TELEGRAM_TRANSPORT=webhook`) for lower update latency.
- Added internal execution queues and dedicated strategy/alert workers for better responsiveness under load.
- Improved runtime performance with latency instrumentation (p50/p95 snapshots in status).
- Expanded strategy controls (Grid range/levels, MM threshold/close offset, DN maintenance auto-close).
- Added DCA Engine strategy with guided parameters and preview integration.

## Core Features

- Natural-language trade parsing (for example: "long ETH 0.1 at 10x")
- Wallet linking with secure signer-key encryption
- Live market data, positions, and PnL views
- Automated strategies: Market Making, Grid, Delta Neutral, Volume Bot, and DCA
- Price and funding alerts
- Admin controls for trading safety and diagnostics

## Quick Start

1. Install dependencies:
   - `pip install -r requirements.txt`
2. Configure environment variables:
   - `TELEGRAM_TOKEN`
   - `DATABASE_URL`
   - `ENCRYPTION_KEY`
   - `XAI_API_KEY` (optional)
   - `OPENAI_API_KEY` (optional)
   - `TELEGRAM_TRANSPORT` (`polling` or `webhook`, default `polling`)
   - `TELEGRAM_WEBHOOK_URL` (required for webhook mode)
   - `TELEGRAM_WEBHOOK_PATH` (optional, default `/telegram/webhook`)
   - `TELEGRAM_WEBHOOK_SECRET` (recommended in webhook mode)
3. Start the bot:
   - `python main.py`

## Deployment

For Fly.io deployment instructions, see `deploy.md`.
