# Nadobro Bot

Nadobro is a Telegram trading bot for Nado DEX that lets users manage wallets, place trades, monitor markets, and run automated strategies from chat.

## Latest Updates (March 2026)

- Added webhook transport mode (`TELEGRAM_TRANSPORT=webhook`) for lower update latency.
- Added internal execution queues and dedicated strategy/alert workers for better responsiveness under load.
- Improved runtime performance with latency instrumentation (p50/p95 snapshots in status).
- Expanded strategy controls (Grid range/levels, MM threshold/close offset, DN maintenance auto-close).

## Core Features

- Natural-language trade parsing (for example: "long ETH 0.1 at 10x")
- Wallet linking with secure signer-key encryption
- Live market data, positions, and PnL views
- Automated strategies: Market Making, Grid, Delta Neutral, Volume Bot, and Bro Mode
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
   - `python3.11 main.py`

## Deployment

For Fly.io deployment instructions, see `deploy.md`.

## Telegram Mini App

The repo includes `miniapp_web/` (Vite + React) and `miniapp_api/` (FastAPI) for a Telegram Web App: trading UI, portfolio, and **Speak with Bro** (Gemini Live) voice.

Mini App is currently archived in production while bot stability is prioritized:

- Bot deploy runs in bot-only mode.
- `BOT_DISABLE_MINIAPP=true` disables Mini App buttons in Telegram UI.
- Mini App code remains in-repo for future re-enable.

- Set `MINIAPP_URL` to your public HTTPS origin (e.g. `https://<app>.fly.dev/`).
- Set `GEMINI_API_KEY` for voice.
- Docs: [nadobro.gitbook.io/docs](https://nadobro.gitbook.io/docs)

Local dev: `cd miniapp_web && npm ci && npm run dev` (proxies `/api` and `/ws` to `localhost:8081`); run `uvicorn miniapp_api.main:app --host 127.0.0.1 --port 8081` from the repo root with `PYTHONPATH=.` and the same env vars as the bot.
