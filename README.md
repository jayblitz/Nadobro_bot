# Nadobro Bot

Nadobro is a Telegram-first Ink financial copilot for Nado DEX. It lets users ask market questions, inspect live Nado/Ink context, receive strategy recommendations, manage wallets, place trades, monitor sessions, and run automated strategies from chat.

## Latest Updates (March 2026)

- Added webhook transport mode (`TELEGRAM_TRANSPORT=webhook`) for lower update latency.
- Added internal execution queues and dedicated strategy/alert workers for better responsiveness under load.
- Improved runtime performance with latency instrumentation (p50/p95 snapshots in status).
- Expanded strategy controls (Grid range/levels, MM threshold/close offset, DN maintenance auto-close).

## Core Features

- Natural-language trade parsing (for example: "long ETH 0.1 at 10x")
- Wallet linking with secure signer-key encryption
- Live market data, positions, and PnL views
- Automated strategies: Market Making, Grid, Dynamic Grid, Delta Neutral, Volume Bot, and Bro Mode
- Price and funding alerts
- Admin controls for trading safety and diagnostics
- DMind financial expert layer for finance-native structuring, signal scoring, and strategy recommendations
- Source freshness tracking for provider-backed market answers
- n8n workflow layer for Telegram-visible automations backed by safe Nadobro action APIs
- Strategy phase/status surface for recovery after failed or stuck sessions
- Dynamic GRID (`dgrid`) that switches between GRID and Reverse GRID based on volatility regime

## Quick Start

1. Install dependencies:
   - `pip install -r requirements.txt`
2. Configure environment variables:
   - `TELEGRAM_TOKEN`
   - `DATABASE_URL`
   - `ENCRYPTION_KEY`
   - `XAI_API_KEY` (optional)
   - `OPENAI_API_KEY` (optional)
   - `DMIND_API_KEY` (recommended for Minara-parity financial reasoning)
   - `N8N_BASE_URL`, `N8N_API_KEY`, `N8N_WEBHOOK_SECRET` (optional workflow engine)
   - Market-data provider keys such as `CMC_API_KEY`, `COINGECKO_API_KEY`, `COINGLASS_API_KEY`, `ARKHAM_API_KEY`, `GLASSNODE_API_KEY`, `ROOTDATA_API_KEY`, `GOPLUS_API_KEY`, `FMP_API_KEY`
   - `TELEGRAM_TRANSPORT` (`polling` or `webhook`, default `polling`)
   - `TELEGRAM_WEBHOOK_URL` (required for webhook mode)
   - `TELEGRAM_WEBHOOK_PATH` (optional, default `/telegram/webhook`)
   - `TELEGRAM_WEBHOOK_SECRET` (recommended in webhook mode)
3. Start the bot:
   - `python3.11 main.py`

## Deployment

For Fly.io deployment instructions, see `deploy.md`.

## Minara-Parity Architecture

See:

- `docs/minara_for_ink_product_spec.md`
- `docs/minara_parity_architecture.md`
- `docs/dynamic_grid_strategy.md`

- Docs: [nadobro.gitbook.io/docs](https://nadobro.gitbook.io/docs)
