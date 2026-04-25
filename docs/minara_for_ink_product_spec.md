# Nadobro: Telegram-First Minara For Ink

## Product Promise

Nadobro is the Ink-native financial copilot for Nado traders inside Telegram. A user should be able to ask what is happening on Ink, get source-grounded market intelligence, receive a strategy recommendation, simulate it, approve execution, and monitor or recover the session without leaving chat.

## Product Modes

### Ask Bro

Source-grounded Q&A about Nado, Ink, positions, markets, workflows, and risk. Answers must show whether they came from Nado/Ink truth, DMind, external market providers, social/news feeds, the knowledge base, or a degraded model fallback.

### Market Intel

Fresh market intelligence for Nado-tradable assets:

- Nado prices, orderbook context, funding, open orders, fills, and PnL.
- Ink RPC and event signals.
- CoinMarketCap/CoinGecko market data.
- Arkham/Glassnode-style wallet and on-chain intelligence.
- CoinGlass derivatives context.
- DeFiLlama protocol and stablecoin flow context.
- X/social, global news, RootData, Polymarket, and listing/delisting feeds.

### Strategy Copilot

Recommendation layer for Grid, RGRID, Volume Bot, Delta Neutral, and Bro Mode. Every recommendation must include:

- Strategy and suggested parameters.
- Confidence score.
- Source bundle and data age.
- Reasons and risks.
- Simulation/paper-trade option.
- Confirmation step before any live execution.

### Workflow Builder

n8n-backed workflow creation from natural language. Nadobro exposes safe action APIs to n8n while retaining wallet checks, budget/risk guards, admin pause, and strategy runtime control.

Initial workflow templates:

- Price monitor -> notify.
- Funding threshold -> recommend strategy.
- Edge found -> ask DMind for summary -> notify.
- Risk-off sentiment -> pause strategy.
- Strategy session failed -> recovery card.

### Session Desk

Live strategy desk in Telegram:

- State/phase.
- Last action and next action.
- Open orders and pending digests.
- Fill sync status.
- Realized/unrealized PnL, volume, fees, funding.
- Stop reason and recovery actions.

## Feature Tiers

### V1: Trust And Product Clarity

- Source freshness and provider labels in AI answers.
- DMind financial expert service with degraded-mode fallback.
- Session status that explains what happened after each strategy cycle.
- Product docs for users and developers.

### V2: Minara-Equivalent Data Layer

- Provider adapter pattern for Minara-equivalent sources.
- Health/rate-limit diagnostics.
- Normalized schemas for quotes, news, social posts, whale flows, liquidations, funding, OI, project data, safety checks, and prediction odds.

### V3: n8n Workflow Copilot

- Self-hosted n8n connected to safe Nadobro action APIs.
- Telegram-visible workflow cards.
- Workflow run logs and status.

### V4: Strategy Reliability

- Explicit durable state machines.
- Startup reconciliation.
- Idempotent order intents.
- Expanded end reasons and recovery paths.

## Non-Negotiables

- Do not hardcode API credentials.
- Do not let n8n hold signing keys or bypass risk controls.
- Do not let generic web search be the primary source of actionable trading data.
- Keep execution in the existing Python/Nado SDK path unless a verified Nado API gap requires another path.
- Always disclose degraded mode when DMind or provider-backed data is unavailable.
