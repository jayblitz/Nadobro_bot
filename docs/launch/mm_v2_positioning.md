# Nadobro MM v2 — Launch Positioning

Source brief for the CMO. Nado-native messaging only — no Tread Fi clone copy,
no fixed multiplier promises, no third-party data references. Every claim
below maps to a Nado endpoint or Phase 0–4 code change in this branch.

---

## One-liner

> **Nadobro MM v2 — Tread Fi parity, built natively on Nado.**
> Mid + Grid + RGrid + DGrid modes, real per-pair venue minimums from Nado's
> `symbols` endpoint, and a smart Tiny Budget Preset that auto-fits leverage
> to the venue floor so $50 wallets actually place quotes.

## Pillars

### 1. Nado-native, not a port

Every input the engine reads is sourced from Nado:

- **Per-pair venue minimum** (`min_size`) — `gateway.{prod,test}.nado.xyz/v1/symbols`.
- **Maker rebate / taker rate** (`maker_fee_rate_x18`, `taker_fee_rate_x18`) —
  same endpoint. Negative on most majors; the bot earns the rebate.
- **Builder fee** — Nadobro-locked at 1 bps via the canonical
  `get_nado_builder_routing_config()` constant. The pre-trade card renders
  exactly that — no Tread fee mirroring.
- **24h volume for participation pacing** —
  `archive.{prod,test}.nado.xyz/v1/market_snapshots` with hourly granularity.

No CMC. No CoinGecko. No external reference exchanges. No RSI signal feeds.

### 2. Tiny Budget Preset

The single feature that lets a $50 wallet quote on majors that would
otherwise reject the order at the venue floor.

- Reads the live `min_size` from Nado.
- Computes `required_leverage = ceil(min_size / collateral)`.
- Clamps to per-asset max leverage from the same `symbols` payload.
- Pins a tight `mm_collateral_safety_factor` (1.10) so the bot doesn't
  over-reserve margin.
- Surfaces actionable error guidance if the pair's max leverage cannot lift
  the user's collateral to the floor — "pick a pair with a smaller min_size,
  or add collateral to $X+".

### 3. Modes

| Mode | What it is | When to use |
| --- | --- | --- |
| **GRID** | Symmetric maker grid around an EMA reference | Calm range markets |
| **Reverse GRID** | Anchor follows fills + momentum-aware reversal | Trending pairs you want to participate in |
| **Dynamic GRID** | Auto-switches GRID ↔ RGRID by realized variance | Set-and-forget on volatile majors |
| **Mid Mode** | Pure `mid ± spread × level`, no anchor | When you want a clean Tread-style maker without the soft-reset complexity |

Every mode goes through the same risk preflight, fee model, and margin-based
SL/TP — the engine is one strategy with four shapes, not four engines.

### 4. POV / Participation pacing

Three documented per-minute participation rates against the pair's rolling
Nado 24h volume:

- **Aggressive 10%/min** — fastest legal completion of the user's notional.
- **Normal 5%/min** — default.
- **Passive 1%/min** — slowest schedule.

Higher participation also raises the per-quote margin requirement (Tread perp
formula: `× {2.0, 1.0, 0.5}`), so an Aggressive run reserves more collateral
per resting quote — the dashboard shows this breakdown before the user starts.

### 5. Margin-denominated SL/TP

Stops are applied to **margin** (`notional / leverage`), not raw notional.

- Matches Tread's documented perp formula.
- Matches existing log text (`"of margin"`) — the previous code applied to
  notional, contradicting its own log lines.
- Means leveraged users hit SL at smaller absolute drawdowns than v1 did —
  this is intentional and surfaces in the pre-trade card so users can see
  their actual max-loss number before they start.

### 6. Lifetime analytics (`/mm_status`, `/mm_fills`)

- `/mm_status` — live snapshot: cumulative volume, net fees (1 bps builder +
  signed maker rate), realized + unrealized PnL via Nado positions, fill rate,
  current spread / reference, open-orders count, inventory soft/hard gauges,
  active POV preset and its computed duration / cycle slice. Refresh button
  re-renders in place.
- `/mm_fills` (or `/mm_fills 25`) — last N executions across both sides,
  newest first.

State persists across restarts via the bot's existing JSON state blob; the
Phase 4 resume reconcile pass (against the Nado archive) confirms there are
no orphan tracker entries before the first cycle re-quotes.

### 7. Reliability

- Bounded retry-with-backoff around `get_market_price` and `get_open_orders`
  so a transient 429 / blip from the gateway does not kill a cycle. Retry
  diagnostics surface in `/mm_status` so users see when the gateway is rough.
- Skipped levels (post-only retry exhausted) surface as a dashboard row
  rather than dropping silently.
- Resume reconcile stamps `mm_resume_reconciled_at` on the first cycle of
  each `strategy_session_id` so an operator can verify the reconcile pass ran
  before the bot starts re-quoting.

---

## What we are NOT promising

- **No fixed volume multiplier.** The plan-based "$50 → $5,000+ in 4 hours"
  number is a Phase 5 empirical target, not a marketing promise. The launch
  copy reports the **observed range** from the soak, not a single number.
- **No "X% APY"** — Nadobro MM is a market-making / inventory strategy. PnL
  depends on market regime, fee rebates, and inventory drift. We surface the
  numbers; we don't promise them.
- **No claims of better execution than Tread.** The wins are: native Nado
  feeds, Tiny Budget Preset for sub-$100 wallets, Telegram-first UX, and
  builder fee discount (1 bps vs 2 bps).

---

## Soak-derived numbers (filled in by the launch report)

> Replace these placeholders from the Phase 5 soak log before publishing.

- **Testnet 4h soak** — observed `mm_session_notional_done_usd`: `$_____`.
  Fill rate: `__%`. Net fee per $1k notional: `$_____`. Skipped levels:
  `__`. Gateway retries: `__`.
- **Mainnet 4h soak** — `mm_session_notional_done_usd`: `$_____`. Fill rate:
  `__%`. Net fee per $1k notional: `$_____`. Skipped levels: `__`. Gateway
  retries: `__`.

## Channels (suggested launch sequence)

1. **In-bot announcement** — pinned `/start` card with the one-liner + a
   "Try $50 Tiny Budget" deep link to the strategy hub.
2. **Telegram channel post** — pillars 1–4 with the soak-derived numbers.
3. **X / Twitter thread** — visual: pre-trade card screenshot, `/mm_status`
   snapshot from the mainnet soak. No promises, just observed.
4. **Docs update** — link `docs/launch/mm_v2_positioning.md` and the soak
   log from `README.md`.

## Sign-off

- [ ] CMO reviewed positioning copy.
- [ ] Soak numbers replaced (no `$_____` left in the published version).
- [ ] No third-party data references in the published copy.
- [ ] No fixed multiplier or APY promises.
- [ ] Builder fee correctly stated as **1 bps** (not 2 bps).
