# Nadobro Bot

Nadobro is a Telegram trading bot for Nado DEX that lets users manage wallets, place trades, monitor markets, and run automated strategies from chat.

## Latest Updates (March 2026)

- Improved strategy UX and execution reliability for smoother bot operation.
- Added Volume Bot to the strategy hub and settings flow.
- Fixed strategy runtime signing and execution flow issues.
- Improved Fly deployment stability for strategy execution workloads.
- Updated Fly VM sizing for better deployment compatibility.

## Core Features

- Natural-language trade parsing (for example: "long ETH 0.1 at 10x")
- Wallet linking with secure signer-key encryption
- Live market data, positions, and PnL views
- Automated strategies: Market Making, Delta Neutral, and Volume Bot
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
3. Start the bot:
   - `python main.py`

## Deployment

For Fly.io deployment instructions, see `deploy.md`.
