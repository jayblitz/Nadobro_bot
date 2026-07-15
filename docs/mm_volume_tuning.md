# MM volume tuning — how to print more volume with grid / rgrid / dgrid / mid

Volume = **fills/hour × order size × 2** (a round trip books both the open and
the close). Every lever below multiplies one of those factors. The defaults are
tuned for capital preservation, not throughput — ~$5k/hour at stock settings is
the model working as configured, not a limitation of the venue.

## The one-tap path: 🚀 Turbo Volume

Strategy → Configure → **Turbo Volume** writes the coherent high-throughput set
for the selected strategy in one tap:

| setting | mid | grid | rgrid | dgrid |
|---|---|---|---|---|
| leverage | min(10x, pair max) | same | same | same |
| spread | 2bp + **touch-join** | 3bp | 3bp | 3bp |
| cycle interval | 5s | 5s | 5s | 5s |
| inventory allowance | auto (= deployed) | unchanged | unchanged | unchanged |
| net-exposure cap | 100% (one full fill) | unchanged | unchanged | unchanged |
| session SL | 10% of margin | 10% | 10% | 10% |
| session TP | off (volume mode) | unchanged | unchanged | unchanged |

Margin stays whatever you configured — Turbo multiplies it. $500 margin at 10x
quotes **$5,000 per side**; every round trip is ~$10k of volume.

## Why these values (the math you should know)

- **Leverage multiplies volume per margin dollar, but also uPnL as % of
  margin** — the unit the session SL/TP rail judges. At 10x, a 1% adverse move
  on full one-sided inventory = 10% of margin = the Turbo SL. At 40x the same
  SL trips on a 0.25% wiggle: the bot auto-stops constantly and **a stopped bot
  prints zero volume**. That's why Turbo picks 10x even where the pair allows
  40-50x. Raise leverage only if you also widen `sl_pct` proportionally — and
  understand that widening SL is real money at risk per session.
- **Touch-join (mid only)** quotes AT the best bid/ask instead of mid ± spread,
  so fills arrive at the rate organic flow trades, not at the rate price drifts
  a spread-width. The cost: thinner (sometimes negative) per-fill edge. The
  session SL bounds the downside; watch **Cost/$1M** on the Performance card.
- **Caps must fit the order** — an inventory cap below the order size
  suppresses a side after every single fill (the old $60 default under a $1,000
  quote). Turbo sets the cap to the deployed size and the net-exposure
  allowance to 100%, which produces the strict buy→sell alternation a volume
  bot wants.
- **Fees floor the cost**: pure-maker at ~1.8bp/side ≈ **$180 per $1M** of
  volume before adverse selection (plan for 2-5× that all-in). There is no
  configuration that makes volume free.

## The physics you cannot configure away

A passive maker's volume ceiling is **its share of the organic taker flow** on
that product. Quote the most liquid perps (BTC, ETH) and expect volume to track
the market's own activity — a quiet hour is a quiet hour. Rough expectations on
a liquid pair with Turbo + $500-1,000 margin: **$100k-500k/hour** in normal
conditions; more when flow is heavy. Beyond that ceiling the only doors are
paying to cross the spread (taker participation — not part of Turbo) or
self-matching, which we deliberately do not do.

## Manual tuning (if you skip the preset)

1. Margin × leverage sets the per-side quote (`mid` uses the full deployed
   size per side; ladders split it across levels).
2. `spread_bp` 2-3 on liquid pairs; wider on thin books or you will be run over.
3. `interval_seconds` 5 (floor 3 — venue rate limits; both enforced globally).
4. Keep `sl_pct` ≈ leverage × (price move you can tolerate). 10x and 10% SL
   means a 1% move stops the session.
5. If you raise order size, raise `inventory_soft_limit_usd` (or set 0 = auto
   on mid) and `max_net_exposure_pct` with it — otherwise the book goes
   one-sided after the first fill.

Wallet execute budget (600 weight/min) comfortably fits Turbo cadence; the
shared per-IP query budget is the fleet-level constraint the team monitors.

## P2: reacting at fill speed

- **Fill-nudge (on by default, `NADO_FILL_NUDGE=false` to disable):** the
  venue's fill stream wakes the strategy runtime the moment one of your orders
  fills, so the controller re-quotes within ~a second instead of waiting out
  the tick interval. Round-trip cadence stops being tick-bound. Overlaps are
  safe (cycles are serialized per user and coalesced); bursts are debounced.

**MM strategies are maker-only — policy (2026-07-15).** Every mid/grid order
is a post-only limit: users pay the lowest fee tier on every fill, and the bot
can never take liquidity. A cross-on-deadline taker flatten was designed,
audited, and deliberately REMOVED: taker fills cost more, and with two-sided
quoting a marketable order risks sweeping the bot's own resting quote — wash
trading, which Nado's terms explicitly penalize. Stale inventory unwinds
through the maker quotes re-centering every tick, bounded by the session SL
rail. (The Volume bot's spot cross-on-deadline is a separate, sequential
single-leg design that cancels its own order before crossing — it cannot
self-match.)

The volume machine, assembled: Turbo preset (size × leverage × touch quotes)
→ fill-nudge (instant re-quote after every fill). Both layers are independent
and independently disableable.

## Not yet: one user, many products at once

Running mid on BTC + ETH + SOL simultaneously would multiply volume linearly,
and the engine layer could hold the controllers — but the user runtime (state
store, scheduler, /status, stop paths, session SL rails, session accounting)
assumes ONE strategy+product per user per network throughout. Making that
plural is the largest structural change since the services decomposition and
gets its own design + review cycle rather than riding along here. Until then:
one product per session, pick the most liquid book.
