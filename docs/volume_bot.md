# Volume Bot (Spot) — v3

Volume Bot is Nadobro's automated spot maker-volume strategy: a fast
buy → sell ping-pong of a fixed USDT0 amount through one Nado spot product,
**limit orders on both legs**, repeating until the cumulative executed volume
reaches the user's target. v3 (2026-07) replaced the v2 mechanics after a
production audit showed v2 had never completed a target (best session: $101
of a $10,000 target; one sell rested unfilled 8.5 hours).

## At a glance

| Property | Value |
| --- | --- |
| Markets | Nado spot products (KBTC, WETH, WNVDAX, …) |
| Leverage | 1x (spot) |
| Order type | post-only limit both legs; marketable LIMIT on the cross deadline |
| Direction | Round-trip buy → sell, one cycle at a time |
| Stop conditions | Target volume, max cycles, session SL rail, manual stop |
| Maker fee (mainnet, measured) | ~1.8 bp per leg |

Implementation: `src/nadobro/engine/controllers/volume_bot.py` (engine v2
controller — the legacy `src/nadobro/strategies/` module this doc used to
reference is gone). Config mapping: `engine_runtime.map_strategy_config` /
risk caps: `map_risk_limits` (dedicated `vol` branch sized off
`session_margin_usd`).

## The v3 loop

```text
place buy   post-only AT the touch (join best bid; improve 1 tick if spread allows)
  │   unfilled vol_requote_seconds (20s) → cancel, re-place at fresh touch
  │   unfilled vol_cross_after_seconds (75s) → marketable LIMIT through the
  │   touch by vol_cross_slippage_bp (15bp) — price-bounded, fills as taker
  ▼
buy filled → place sell   post-only AT the ask, same requote/cross treatment,
  │   floored at breakeven − vol_max_cycle_loss_bp (20bp): a volume bot pays a
  │   BOUNDED cost per cycle instead of demanding per-cycle profit
  ▼
sell filled → book volume (both legs) → next cycle | target hit → stop
```

No live book (best bid/ask missing — RWA spots outside US market hours) puts
the controller in a `market_closed` wait state: no orders, no failure, resumes
automatically when the book returns.

## Configuration

User-facing (strategy card): **Session margin** (`session_margin_usd`, the
per-cycle notional), **Stop loss %** (`sl_pct`, session-PnL rail), **Target
volume** (`target_volume_usd`).

Engine knobs (settings passthrough, sane defaults): `vol_buy_offset_bp` (0 =
join the touch), `vol_max_cycle_loss_bp` (20), `vol_requote_seconds` (20),
`vol_cross_after_seconds` (75, 0 = pure maker), `vol_cross_slippage_bp` (15),
`vol_max_cycles` (100). Tick cadence: 5s (fast-cadence set).

## Cost model

Target volume × maker fee is the floor cost (e.g. $10,000 × 1.8bp ≈ $1.80 per
side that rests). Each crossed leg pays the taker fee instead, and each cycle
may additionally cost up to `vol_max_cycle_loss_bp` of the cycle notional in
adverse moves — bounded per cycle, with the session SL rail as the hard stop.

## What v3 fixed (2026-07 audit)

| Audit ID | v2 defect |
| --- | --- |
| VOL-RISK-CAP | No `vol` branch in `map_risk_limits` → $100 caps for every session; lot-rounded closes rejected (prod: sell refused 1.4s after the buy fill, spot stranded) |
| VOL-SELL-NO-CHASE | Sell leg had no requote/timeout at all (8.5h stall) |
| VOL-SELL-PROFIT-FLOOR | Sell ≥ max(breakeven+edge, ask) demanded per-cycle profit |
| VOL-BUY-DEAD-BAND | Buy at mid−5bp with ~25bp requote dead band (56-min fills) |
| VOL-CROSS-REGRESSION | 8bf08d0 cross-on-timeout + tests lost in the d10e6f1 merge |
| VOL-NO-MARKET-HOURS | Quoted into closed RWA books all night |
| VOL-TICK-CADENCE | 10–20s reaction latency |

Guardrails: `tests/engine/controllers/test_volume_bot.py` (20 tests).
