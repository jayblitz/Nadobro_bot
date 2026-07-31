# Volume Bot (Spot) — v4.1 taker mode

v4 (2026-07-31) converts the spot Volume Bot from **maker-first limit** quoting
to **taker on both legs**, adds a **$100–$500 margin band**, and gates the
start behind an **explicit fee-agreement card**. v4.1 (same day) removes the
profit gate entirely: PnL is not the objective, and the session stop — 5% of
margin, net of fees — is what bounds a run. Read the v4.1 section first.

This supersedes the v3 loop in `docs/volume_bot.md` for the taker path. The v3
maker machinery stays in the controller behind `vol_taker_mode=0` as a
reversible kill-switch — it is the tested fallback, not dead code.

## Why taker

v3's purpose was cheap volume: rest post-only, pay ~1.8 bp/leg. But resting
orders don't fill on demand — v3's own post-mortem records a sell resting 8.5
hours and a session reaching $101 of a $10,000 target. A volume product that
cannot *complete* its volume target is not a volume product.

Taker trades fee for determinism: every leg fills immediately, so the target
completes in predictable wall-clock time. The user pays for that, and v4's job
is to state the price **before** they start.

### This does not violate the maker-only policy

`docs/mm_volume_tuning.md` makes MM strategies maker-only partly to avoid
self-matching (a marketable order sweeping the bot's own resting quote = wash
trading, which Nado's terms penalize). That policy already carves out this
bot verbatim:

> The Volume bot's spot cross-on-deadline is a separate, sequential single-leg
> design that cancels its own order before crossing — it cannot self-match.

v4 keeps that property and strengthens it: in taker mode the bot holds **at
most one live order at a time** and never has a resting quote to sweep. Buy
completes → then sell is placed. No two-sided book, no self-match surface.

It *does* supersede the standing "maker-first limit orders for all strategy
opens/closes" preference for this one strategy, by explicit product decision.

## v4.1 — no profit gate (user directive, 2026-07-31)

**PnL is explicitly not the objective; volume is.** Both legs fire back to
back as taker with no wait for a favourable price. A cycle is buy-then-sell,
so the bot is **flat between cycles**.

This supersedes the v4 "patient taker" design, which held inventory until the
bid cleared a fee-inclusive profit target. That gate is gone, and with it the
whole class of problems it created (an unreachable concession floor, an
unwatched held bag, hold deadlines).

### The consequence, stated plainly

A market buy fills at the **ask** and a market sell at the **bid**, so **every
round trip costs spread + both legs' fees by construction**. There is no
mechanism by which a cycle profits. That is the accepted trade for
deterministic, immediate volume — and it makes the session stop, not the
volume target, the binding constraint on a run.

Measured defaults (KBTC, 3.5 bp venue + 1 bp builder, $100 margin, 5% stop):

| Book spread | Cost / cycle | Cycles to the stop | Volume before it trips |
| --- | --- | --- | --- |
| 2 bp | $0.11 | ~45 | ~$9,100 |
| 5 bp | $0.14 | ~36 | ~$7,100 |
| 20 bp | $0.29 | ~17 | ~$3,400 |

The agreement card states the stop in dollars and warns when the configured
target is unreachable before it trips.

### Session stop: 5% of margin, net of fees

The bot **stops and closes everything** once cumulative realized PnL net of
fees reaches `-sl_pct%` of margin — the default 5% is `-$5` on a $100 margin.

It is enforced **in the controller**, off its own fill accounting
(`session_realized_pnl_usd` = `sold_quote − entry_quote − entry_fee −
sold_fee`, already net of both legs' fees), because the live-session SL rail
is structurally blind to spot: `strategy_sessions.product_id` is resolved by
the perp-only `get_product_id`, so it is NULL for a spot session and
unrealized PnL short-circuits to 0. The controller knows exactly what it
bought and sold, so its book is the authoritative stop for this strategy.

The check runs in `_after_cycle`, i.e. **after a completed round trip**, so
the book is flat when it fires — nothing is left to close.

### Nothing left exposed

- Selling immediately means the only exposure window is between a buy fill and
  its sell fill, within a single tick.
- A partial buy is round-tripped for exactly what was filled, never abandoned.
- Exhausting sell attempts on a real (non-dust) position **stops the session**
  rather than zeroing `entry_base` and buying on top of it.
- Any stop that lands while base is held exposes the size through
  `volume_metrics`, so `_volume_spot_managed_size` can size the spot sweep;
  the vol counters are now merged into state **before** the rail can fire, so
  a stop on the same cycle as the first buy fill no longer sweeps against a
  stale zero and silently sells nothing.
- The sweep stays **capped at what the bot bought** — it must never sell a
  user's pre-existing spot holdings.

### Execution is a marketable limit, not a naked MARKET

Both legs cross the book and fill immediately as taker, but they are placed as
**LIMIT orders priced through the touch** by `vol_taker_slippage_bp` (15 bp).
`NadoClient.place_market_order` is an IOC priced ±`slippage_pct` through the
touch with a **1% default that the adapter never overrides**, so a bare MARKET
could fill up to 99 bp away from the book and overspend the risk-approved
cycle size (audit 2026-07-31, F1). This is an execution-safety bound, not a
profit gate.

## Fee model

### Volume accounting — both legs count

`session_volume_usd` accumulates the buy notional (`entry_quote`) *and* the
sell notional (`sold_quote`) — see `volume_bot.py` `_finish_cycle` and the
requote/terminal paths. So the user's target volume maps **1:1 onto
fee-bearing notional**, which makes the estimate a clean product:

```
estimated_fee = target_volume_usd × (venue_taker_rate + builder_rate)
cycles ≈ target_volume_usd / (2 × margin_usd)
```

No 2× correction is needed or allowed: quoting `2 × volume × rate` would
double-count, quoting only the sell leg would halve it.

### Rate provenance (measured, not guessed)

Rates resolve live-first, with a measured fallback:

1. `product_catalog.get_spot_taker_fee_rate(pair, network)` — the venue value
   when the catalog carries it (the `all_products` gateway query frequently
   omits fee fields, so this is often `None`).
2. Fallback `DEFAULT_SPOT_TAKER_FEE_RATE = 4.3 bp` — **measured from
   production fills**: `engine_executors` taker rows (MARKET and crossed
   LIMIT) sit at 4.30–4.34 bp of notional across BTC-PERP, KBTC and
   BTC-USDT0, versus 1.80 bp for resting post-only makers.
3. Builder fee `1.0 bp` from `config.get_nado_builder_routing_config` (locked
   to 1 bps by policy; testnet returns 0).

The estimate quotes `venue + builder` per leg.

> **Open question — is the builder share already inside the venue fee?**
> `engine_persistence.py` states as established fact that the venue match
> `fee` **already includes** the builder portion and splits it out for
> attribution (`fill_fee = venue_fee − builder_fee`). This contradicts the
> first draft of this doc, which inferred "charged on top" from
> `trades_mainnet.builder_fee` measuring exactly 1.000 bp — but that column is
> *computed* as `notional × 0.0001`, so it reads 1.000 bp tautologically and
> is **not evidence either way** (audit 2026-07-31). Exactly one of the two
> readings is right; settle it against a single production fill by comparing
> the venue's reported `fee` against `notional × venue_rate`.
>
> Until it is settled the estimate adds them, which **over**-quotes by 1 bp if
> the venue fee is already all-in. For a card whose button means "I agree to
> these charges", over-quoting is the only acceptable direction to be wrong.
> The same addition appears in the controller's breakeven, where it makes the
> sell gate ~2 bp harder and therefore holds ~marginally longer — noted as a
> real (if small) cost of the ambiguity.

### Slippage disclaimer (required)

Spot market orders walk the book. The card states plainly that slippage can
push the **total cost above the displayed estimate**, because the estimate
prices fees only — not the price you actually fill at.

## Workflow

```text
Vol preview card
  ├─ Margin: $100 / $250 / $500  (band enforced 100–500, custom input too)
  ├─ Target volume: $10k / $25k / $100k (or custom)
  └─ ▶ Start VOL (Spot)
        ▼
   [all existing preflight: onboarding, wallet, collateral budget]
        ▼
   💳 Fee agreement card  ← NEW
      Margin, target volume, estimated cycles
      Venue taker X bp + builder 1.0 bp = Y bp per leg
      Estimated total fee ≈ $Z
      ⚠️ Spot slippage can make the total exceed this estimate
      [ ✅ Agree & Start ]  [ ◀ Back ]
        ▼ (strategy:startok:vol:PRODUCT)
   start_strategy  → records the agreement (rate, estimate, volume, margin, ts)
```

The agreement is **per start**, not once-ever: the estimate depends on the
margin/target/rates in force at that moment, so each run re-quotes and
re-consents. What the user agreed to is persisted alongside the session for
audit (`vol_fee_ack_*`).

## Config keys (all mapped, none dead)

| Key | Default | Meaning |
| --- | --- | --- |
| `vol_taker_mode` | `1` | 1 = taker both legs (v4.1); 0 = v3 maker path |
| `vol_taker_slippage_bp` | `15.0` | Price bound on the marketable-limit legs |
| `sl_pct` | `5.0` | Session stop, % of margin, net of fees (-$5 on $100) |
| `vol_max_cycle_loss_bp` | `20.0` | v3-only (maker sell floor); unused in taker mode |
| `session_margin_usd` | `100` | Per-cycle size; **band 100–500** |
| `target_volume_usd` | `10000` | Cumulative both-leg volume target |
| `vol_taker_fee_rate` | live/4.3bp | Venue taker rate used for breakeven + estimate |
| `vol_builder_fee_rate` | 1.0bp | Builder rate used for breakeven + estimate |

## Files

- `src/nadobro/quant/vol_fee_estimator.py` — pure fee math (no venue, unit-tested)
- `src/nadobro/engine/controllers/volume_bot.py` — taker buy/hold/sell path
- `src/nadobro/strategy/engine_runtime.py` — vol config mapping
- `src/nadobro/strategy/strategy_registry.py` — defaults + margin band
- `src/nadobro/handlers/strategy_handler.py` — margin buttons, fee card, agreement gate
- `src/nadobro/handlers/messages.py` — custom-input bounds
