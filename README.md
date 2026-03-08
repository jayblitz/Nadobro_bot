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

**The bot runs on Fly.io only.** Do not run it locally in production; use deployments.

1. Install the [Fly CLI](https://fly.io/docs/flyctl/install/)
2. Log in: `fly auth login`
3. Deploy: `fly deploy -a nadobro-bot`

Secrets (e.g. `TELEGRAM_TOKEN`, `DATABASE_URL`, `ENCRYPTION_KEY`) are set via `fly secrets set`. See `deploy.md` for full setup and configuration.
