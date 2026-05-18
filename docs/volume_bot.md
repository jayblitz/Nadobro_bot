# Volume Bot (Spot)

Volume Bot is Nadobro's automated maker-volume strategy. It rotates a fixed USDT0 amount through a single Nado spot product with a `buy → wait → sell` loop, every order posted as `post_only` so the user only pays (or earns) the maker fee. The bot stops when the cumulative executed volume reaches the user's target or when realized session PnL hits the configured stop-loss.

This document describes the canonical 2026-05 spec. The earlier perp / signal-filtered variant has been retired.

## At a glance

| Property | Value |
| --- | --- |
| Markets | Nado spot products (KBTC, WETH, USDC, …) |
| Leverage | 1x (spot) |
| Order type | `post_only` limit, both legs |
| Direction | Round-trip buy → sell, one open position at a time |
| Stop condition | Target volume hit, session SL hit, or manual stop |
| Per-cycle notional | Session margin (single configurable value) |

Supported products are pulled live from `list_volume_spot_product_names()` in `src/nadobro/market_categories.py`.

## The loop

```text
idle
  ├─ place_buy   (post_only limit at the bid)
  ├─ wait_buy_fill
  ├─ wait_close_timer  (60s default hold between buy fill and sell post)
  ├─ place_sell  (post_only limit at the ask)
  ├─ wait_sell_fill
  └─ session checks:
       cumulative volume >= target_volume_usd  → stop (target_volume_hit)
       realized PnL <= -margin * SL%           → stop (sl_hit)
       otherwise                                → idle (next cycle)
```

Every cycle pushes `2 × session_margin` of executed volume into the user's stats — once on the buy leg, once on the sell leg.

## Configuration

Three knobs, exposed in the Volume strategy card under **Advanced** and in Telegram presets:

### Session margin (USD)
The per-cycle notional. Also the SL denominator. A `$500` session margin means each round-trip cycles $500 through the market, and a `0.5%` SL halts the bot at `-$2.50` of session realized PnL.

Stored as `state["session_margin_usd"]`. Mirrors into legacy `target_notional_usd` / `fixed_margin_usd` for backward-compat readers (copy-trade, preview cards from older builds).

Presets: `$100`, `$500`, `$1000`, plus `✍️ Custom Margin`.

### Stop loss %
Applied to **session realized PnL**, not per-trade. The bot halts when `session_realized_pnl_usd ≤ -session_margin × SL%`.

Stored as `state["sl_pct"]`. Presets: `0.5%`, `1.0%`, `2.0%`, plus `✍️ Custom SL`.

### Target volume (USD)
Cumulative executed volume target across all cycles (buy + sell legs combined). The bot stops once `session_executed_volume_usd ≥ target_volume_usd`.

Stored as `state["target_volume_usd"]`. Presets: `$10k`, `$25k`, `$100k`, plus `✍️ Custom Target`.

## Pre-flight analytics card

Before the user presses **Start**, the Volume dashboard shows:

```text
Market:           {ASSET} SPOT
Session margin:   $500.00
Stop loss:        0.50%
Target volume:    $25,000.00

Pre-flight analytics
  Est. cycles to target: 25
  Est. fees (maker 3.0bp): $7.50
  Est. PnL if SL hits:    -$2.50
  Slippage:               post-only (maker fills only)
```

Math used by `_build_strategy_preview_text` in `src/nadobro/handlers/callbacks.py`:

```python
est_cycles      = ceil(target_volume / (2 * session_margin))
est_fees_usd    = target_volume * (maker_fee_bp / 10_000)
est_pnl_at_sl   = -session_margin * (sl_pct / 100)
slippage        = 0  # post-only orders fill at the maker price by construction
```

`maker_fee_bp` defaults to `EST_FEE_RATE × 10_000` from `src/nadobro/config.py` (3 bps as of writing) and can be overridden per-user via `vol_maker_fee_bp`. Slippage in the traditional sense is zero, but **drift risk** between the buy fill and the sell post is real and gets surfaced in the live statistics block (`vol_spread_bp`).

## Live statistics

After Start, the same card switches to a live view:

```text
Live statistics
  Volume done:    $X,XXX / $25,000
  Volume remaining: $X,XXX
  Fees paid:      $X.XX
  Realized PnL:   +/-$X.XX
  Phase:          IDLE | PLACE_BUY | WAIT_BUY_FILL | WAIT_CLOSE_TIMER | PLACE_SELL | WAIT_SELL_FILL
```

`session_realized_pnl_usd`, `session_executed_volume_usd`, `vol_phase`, and `session_fees_usd` are updated by `_run_volume_spot_cycle` on every fill.

## Stop conditions

| Reason | Trigger | Result |
| --- | --- | --- |
| `target_volume_hit` | `volume_done_usd >= target_volume_usd` | Strategy stops cleanly, banner: `✅ Target volume reached`. |
| `sl_hit` | `session_realized_pnl_usd <= -margin × SL%` | Strategy stops cleanly, banner: `🛑 SL hit`. Any open sell leg is force-closed via IOC. |
| `user_stop` | User taps Stop in the strategy card | Same as SL cleanup. |
| `insufficient_margin` | USDT0 balance < session_margin at cycle start | One-shot error, strategy stays running and retries next tick. |

## What was retired in 2026-05

- **Volume perp mode** — the Perp/Spot toggle, dual LONG/SHORT entry, per-asset MAX-leverage sizing.
- **Signal-filter gating** — EMA, RSI, edge-bp, and regime-classifier checks that used to skip cycles ("VOL spot skipped entry: long signal filter did not confirm setup"). The new spec is deliberately dumb: every cycle attempts a maker buy.
- **Take-profit %** — TP is no longer a Volume concept; target volume drives session end.
- **LONG / SHORT direction** — round-trip buy → sell only.

Operator-callable env flags for any of those features have been removed.

## Code map

| File | Role |
| --- | --- |
| `src/nadobro/strategies/volume_bot.py` | `run_cycle`, `_run_volume_spot_cycle`, `_resolve_target_notional`, fill bookkeeping. |
| `src/nadobro/handlers/keyboards.py` | `strategy_action_kb` for Volume (single Start button, no perp toggle), `_strategy_config_section_kb` (margin / SL / target presets + custom). |
| `src/nadobro/handlers/callbacks.py` | `_build_strategy_preview_text` Volume branch (analytics card), input validator + limits for `session_margin_usd` / `sl_pct` / `target_volume_usd`. |
| `src/nadobro/services/bot_runtime.py` | Start banner, cycle-zero alert, `_market_label_for_strategy` that returns `{PRODUCT} SPOT` for Volume. |
| `src/nadobro/services/trade_service.py` | `execute_spot_limit_order` (post-only buys/sells). |
| `src/nadobro/market_categories.py` | `list_volume_spot_product_names`, `normalize_volume_spot_symbol`. |

## Worked example

User picks `KBTC` on mainnet, sets margin `$500`, SL `0.5%`, target `$25,000`.

- Estimated cycles: `25,000 / (2 × 500) = 25`.
- Estimated fees at 3 bps maker: `25,000 × 0.0003 = $7.50`.
- Estimated SL PnL: `-500 × 0.005 = -$2.50`.

The bot starts, posts a maker buy at the KBTC bid for `$500 / bid` size, waits for it to fill, waits 60s, posts a maker sell at the ask, waits for it to fill, then loops. After roughly 25 successful round-trips, the bot stops with a target-volume notice. If the cumulative realized PnL drops to `-$2.50` first, the bot halts with an SL notice instead.

## See also

- [docs.nado.xyz/products](https://docs.nado.xyz/products) — Nado spot product list.
- [docs.nado.xyz/fees-and-rebates](https://docs.nado.xyz/fees-and-rebates) — fee schedule.
- [`docs/mm_strategy_design.md`](mm_strategy_design.md) — the perp market-making family that complements Volume.
