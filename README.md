# Nadobro

**The Telegram trading copilot for [Nado](https://nado.xyz) on Ink L2.**

Trade perps and spot, deposit into the NLP vault, run automated strategies, and stay on top of the market — all from a single Telegram chat. Nadobro plugs straight into Nado's gateway, keeps your wallet keys encrypted, and turns every Nado feature into a button.

→ **Try the bot:** [@NBdotbot](https://t.me/NBdotbot) · **Docs:** [nadobro.gitbook.io/docs](https://nadobro.gitbook.io/docs) · **Follow:** [x.com/NBdotbot](https://x.com/NBdotbot)

---

## What you can do

### 🤖 Trade Console
Natural-language and button-driven perp and spot trades on Nado. Market or limit, custom leverage, optional TP / SL, time limits, and a confirmation card before anything signs. Examples: *"long ETH 0.1 at 10x"*, *"buy 50 BTC at 65000 with 0.5% SL"*.

### 💰 Nado Vault
One-tap deposit and withdraw on Nado's [NLP](https://docs.nado.xyz/nlp) liquidity vault. USDT0 in, share of Nado's institutional MM yield out. The bot enforces the 4-day post-mint lockup, the 20,000 USDT0 Private Alpha cap, and shows you live NAV / fees / lockup countdown before you sign. See [`docs/nado_vault.md`](docs/nado_vault.md).

### 🧠 Strategy Lab
Automated trading strategies, all maker-only by default:

- **GRID / Reverse GRID / Dynamic GRID** — classic and adaptive grid market making.
- **Mid Mode** — directional-biased mid-quoting for one-sided markets.
- **Mirror Delta-Neutral** — spot long + perp short hedged pairs with funding-mode controls.
- **Volume Engine** — spot-only post-only buy → sell loop that builds Nado maker volume against a session margin + SL + target-volume budget. See [`docs/volume_bot.md`](docs/volume_bot.md).
- **Copy Trading** — mirror curated Nado wallets with per-user budget, risk factor, and leverage caps.

### 📚 Resources
A built-in menu of every link a Nado user needs: Nado Docs, NLP Vault docs, Nado dev resources (API, CLI/MCP, TypeScript SDK), NadoBro docs, and the @NBdotbot X account.

### Other essentials
Wallet linking with encrypted 1CT signer keys, live PnL / positions / open orders, price + funding alerts, multi-language UI (EN / ZH / FR / AR / RU / KO), an i18n-aware AI assistant for market questions, and admin / safety controls.

---

## How it works

1. Open Telegram and message [@NBdotbot](https://t.me/NBdotbot).
2. Run `/start`, follow the onboarding to link a Nado main wallet and a session signer.
3. Pick a feature from the Home card — Trade, Vault, Strategy Lab, Resources.
4. Confirm any signing action on the confirmation card before it goes on-chain.

Your linked signer key is encrypted with a server-side keyfile (`ENCRYPTION_KEY`); the main wallet stays cold. Every order, mint, burn, and strategy cycle is signed off-chain by the 1CT signer via Nado's gateway. Studio / autonomous trading layers have been retired in favor of explicit user-driven flows.

---

## Developer quickstart

Python 3.11+, PostgreSQL, and a Nado main wallet.

```bash
pip install -r requirements.txt
python3.11 main.py
```

Required environment variables:

| Variable | Purpose |
| --- | --- |
| `TELEGRAM_TOKEN` | Telegram BotFather token |
| `DATABASE_URL` | PostgreSQL connection string |
| `ENCRYPTION_KEY` | 32-byte key used to encrypt linked-signer private keys |

Optional (each unlocks an extra feature):

| Variable | Effect |
| --- | --- |
| `XAI_API_KEY` / `OPENAI_API_KEY` | LLM provider for the AI assistant and trade-intent parsing |
| `NADO_AI_PROVIDER` | `auto`, `xai`, or `openai` |
| `DMIND_API_KEY` | DMind financial-reasoning provider |
| `CMC_API_KEY`, `COINGECKO_API_KEY`, `COINGLASS_API_KEY`, `ARKHAM_API_KEY`, `GLASSNODE_API_KEY`, `ROOTDATA_API_KEY`, `GOPLUS_API_KEY`, `FMP_API_KEY` | Market-data providers |
| `TELEGRAM_TRANSPORT` | `polling` (default) or `webhook` |
| `TELEGRAM_WEBHOOK_URL`, `TELEGRAM_WEBHOOK_PATH`, `TELEGRAM_WEBHOOK_SECRET` | Webhook mode settings |
| `NADO_COPY_TRADING` | Enable the copy-trading polling loop (default `true`) |

---

## Architecture, in one paragraph

Nadobro is a single Python service plus a sidecar relay. The bot process owns Telegram I/O, an APScheduler that runs strategy cycles and alert ticks, an execution queue with dedicated workers per strategy family, an encrypted wallet vault, and the Nado SDK client wrapper that signs every order, mint, and burn. State lives in PostgreSQL. The `relay/` microservice (FastAPI + Telethon) handles the LOWIQPTS reward relay.

Deep-dives:

- [`docs/scalable_system_architecture.md`](docs/scalable_system_architecture.md) — service-layer overview.
- [`docs/nado_vault.md`](docs/nado_vault.md) — NLP deposit / withdraw integration.
- [`docs/volume_bot.md`](docs/volume_bot.md) — spot Volume strategy spec.
- [`docs/mm_strategy_design.md`](docs/mm_strategy_design.md) — market-making strategy design (GRID, RGRID, DGRID, Mid).
- [`docs/dgrid_intelligence_upgrade.md`](docs/dgrid_intelligence_upgrade.md) — Dynamic GRID adaptive logic.
- [`docs/managed-ai-agent-integration.md`](docs/managed-ai-agent-integration.md) — opt-in AI assistant layer.
- [`docs/referral_system_integration.md`](docs/referral_system_integration.md) — referral codes and rewards.

---

## Deployment

Production runs on Fly.io. See [`deploy.md`](deploy.md) for the step-by-step guide.

---

## Repo layout

```
main.py            # Telegram bot entrypoint
src/nadobro/       # Application source
  handlers/          # Telegram callbacks, keyboards, message routing
  services/          # Nado client, vault service, strategy lifecycle, scheduler, etc.
  strategies/        # MM, GRID family, DN, Volume bot
  models/            # PostgreSQL ORM
  migrations/        # SQL migrations
relay/             # LOWIQPTS reward relay (FastAPI + Telethon)
docs/              # Architecture and feature docs
tests/             # pytest suite
deploy/            # Deployment scripts and configs
scripts/           # Operational and migration scripts
```

---

## Links

- Bot — [@NBdotbot](https://t.me/NBdotbot)
- Nadobro docs — [nadobro.gitbook.io/docs](https://nadobro.gitbook.io/docs)
- Nadobro on X — [x.com/NBdotbot](https://x.com/NBdotbot)
- Nado — [nado.xyz](https://nado.xyz) · [docs.nado.xyz](https://docs.nado.xyz)
