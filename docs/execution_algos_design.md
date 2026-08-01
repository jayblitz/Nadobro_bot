# Execution Algos for the Spot Volume Bot — architecture

Design only. Every capability below was verified against the installed Nado
SDK v2 and probed live against mainnet on 2026-07-31; nothing here is assumed.
Where a requested algo cannot be built, it says so.

---

## 1. Verified ground truth

### 1.1 What the Nado SDK v2 actually gives us

| Capability | Symbol | Verified |
| --- | --- | --- |
| Order types | `OrderType.{DEFAULT, IOC, FOK, POST_ONLY}` | `utils/expiration.py:5` |
| Place limit (typed) | `market.execute.place_order(order_type=…)` | `apis/market/execute.py:258` |
| Place market | `place_market_order` — IOC priced ±`slippage_pct` through touch | `execute.py:81` |
| Batch cancel | `cancel_orders(digests)` | `execute.py:96` |
| Cancel-all per product | `cancel_product_orders` | `execute.py:111` |
| **Atomic replace** | **`cancel_and_place`** | `execute.py:128` |
| Order expiry | `get_expiration_timestamp(seconds_from_now)` | `utils/expiration.py:12` |
| Max order size | `get_max_order_size` | `market/query.py:178` |
| **Full book depth** | **`get_market_liquidity(product_id, depth)`** | probed: 20 levels returned |
| **Public trade tape** | **`GET /v2/trades?ticker_id=…`** | probed: live, `trade_id/price/base_filled/quote_filled/timestamp/trade_type` |
| 24h volume per market | `GET /v2/tickers` | probed: `base_volume`, `quote_volume` |
| 1m OHLCV + volume | `get_candlesticks` | already in use |
| Trigger orders | `trigger_client`, `get/cancel_trigger_orders` | present |

**Private** WS streams (Nadobro's own `venue/nado_ws.py`, `/v1/subscribe`):
`order_update`, `fill`, `position_change`, `funding_payment`.

### 1.2 What does NOT exist — do not design around these

- **No native iceberg / hidden / display-size order type.** Grep of the whole
  SDK for `iceberg|hidden|display_size` returns nothing. The order-type enum is
  exactly four values.
- **No venue-side algos.** No native TWAP/VWAP/POV. Everything is client-side.
- **No public book or trade websocket.** The WS streams are account-scoped
  only. Book depth and the trade tape are **poll-only**, which floors our
  market-reaction latency at the poll cadence and spends gateway budget.
- `reduce_only` is documented as working only with IOC & FOK
  (`engine_client/types/execute.py:75`) — relevant to any maker *exit*.

### 1.3 Measured market reality (KBTC spot, mainnet, 2026-07-31)

```
spread                     5.7 bp     (bid 63,002 / ask 63,038)
top-of-book depth         $5,998
cumulative, 8 levels    $751,127
24h traded volume        $13,737     (WETH spot: $3,299)
```

Price impact of a market buy, walking the real book:

| Notional | Fill VWAP vs ask |
| --- | --- |
| $100 | **0.00 bp** |
| $500 | **0.00 bp** |
| $5,000 | **0.00 bp** |
| $25,000 | 0.72 bp |

**Two conclusions that decide this entire design:**

1. **Price impact is not our problem.** At the product's $100–$500 band we do
   not clear even the first level. Impact-minimisation machinery is solving a
   problem we measurably do not have.
2. **The spread and the fee tier are the whole cost.** Measured taker 4.3 bp
   vs maker 1.8 bp, plus a 5.7 bp spread that a taker crosses twice.

### 1.4 The actual prize

Round trip on $100 notional, against the user's $5 (5%-of-margin) stop:

| Execution style | Cost/cycle | Cycles to the stop | Volume before it trips |
| --- | --- | --- | --- |
| Pure taker (today) | 14.3 bp / $0.143 | 35 | **$6,993** |
| Maker buy, taker sell | 8.9 bp / $0.089 | 56 | $11,173 |
| Pure maker (both legs rest) | 3.6 bp / $0.036 | 139 | **$27,778** |

> **Maker execution is worth ~4× the achievable volume for the same stop
> budget.** That — not impact — is why this work is worth doing, and it is
> directly denominated in the safety limit the user already set.

### 1.5 The tension that must be designed for

The Volume Bot's objective is **turnover**, not price. Maker orders are cheap
but *may not fill*; taker orders always fill but cost 4× more. So the algo
choice is a **throughput ⇄ cost dial**, and the honest framing is:

- Taker: guaranteed fill rate, 4× the cost, ~$7k of volume per $5 of stop.
- Maker: cheap, but throughput depends on someone crossing to us — on a
  $13.7k/day book that is not guaranteed.

The placement ladder in §3 exists precisely to resolve this: rest first, chase,
and only cross when the schedule would otherwise fall behind.

---

## 2. Architecture

Separate **schedule** (how much, when) from **placement** (where, what order
type). All viable algos differ *only* in the schedule; they share one placement
engine. This is the single most important structural decision — it is what
stops five algos becoming five copies of the same order-management bugs.

```
                    VolumeBotController  (session: margin band, 5% loss stop, cycle loop)
                              │  emits a PARENT ORDER per leg
                              ▼
        ┌─────────────────────────────────────────────────────────┐
        │                  ExecutionAlgoRunner                     │
        │  owns one parent order to completion; reports benchmarks │
        └───────────────┬──────────────────────┬──────────────────┘
                        │                      │
              ┌─────────▼────────┐   ┌─────────▼──────────┐
              │  SCHEDULER       │   │  PLACEMENT ENGINE  │
              │  (the algo)      │   │  (shared by all)   │
              │                  │   │                    │
              │  TWAP  (default) │   │  maker-first       │
              │  VWAP            │──▶│  chase ladder      │
              │  POV             │   │  bounded crossing  │
              │  Chase(passthru) │   │  cancel_and_place  │
              └─────────┬────────┘   └─────────┬──────────┘
                        │                      │
                        │            ┌─────────▼──────────┐
                        │            │  ChildOrderManager │
                        │            │  place/replace/    │
                        │            │  cancel + fills    │
                        │            └─────────┬──────────┘
                        │                      │
              ┌─────────▼──────────────────────▼──────────┐
              │        MarketContext (shared, cached)      │
              │  book depth · trade tape · tickers ·       │
              │  candles · own-fill WS · gateway budget    │
              └───────────────────────────────────────────┘
```

### 2.1 Component contracts

**`MarketContext`** — one polled snapshot per (product, network), shared by all
users, budget-aware. This must be a singleton like the existing
`venue/market_feed.py`, or N users × M algos will exhaust the gateway budget.

```
book(product)        -> BookSnapshot(bids[], asks[], ts)     # get_market_liquidity
tape(product, since) -> list[Trade]                          # /v2/trades, deduped
adv(product)         -> 24h base/quote volume                # /v2/tickers
profile(product)     -> intraday volume profile              # candles (1m)
```

Two non-obvious requirements, both forced by §1.3:
- **Dedupe the tape by `trade_id`.** The endpoint returns each match **twice**
  (once as `trade_type:"buy"`, once as `"sell"`) — verified in the live probe.
  Counting both double-counts market volume.
- **Exclude our own prints** from any benchmark or participation measurement.
  At a $10k target on a $13.7k/day book we are 40–70% of the tape; measuring
  "market volume" that is mostly us is a feedback loop, not a signal. Our own
  fills are identifiable from the `fill` WS stream / our order digests.

**`Scheduler` (the algo)** — pure, testable, no venue access:

```
plan(parent, now, ctx) -> TargetState(qty_that_should_be_done_by_now,
                                      urgency,          # drives the ladder
                                      max_child_qty)
```

Pure functions here mean each algo is unit-testable against a synthetic tape
with no venue and no mocks — the same discipline as `quant/`.

**`PlacementEngine`** — turns "I am X behind schedule" into orders (§3).

**`ChildOrderManager`** — owns live child orders: place, `cancel_and_place`,
cancel, reconcile fills. One place for the order-lifecycle bugs, which this
codebase has already paid for twice (a marketable LIMIT rests; an IOC does not).

### 2.2 Reporting — what makes it an OEMS rather than a slicer

Per parent order, persisted and shown in `/status`:

- **Arrival price** (mid at parent start) and slippage vs it, in bp.
- **Interval VWAP from the tape, excluding our own prints**, and slippage vs it.
  This is the honest benchmark; without the exclusion it is self-referential.
- **Maker fill ratio** and **effective fee bp** — the number that proves the
  4× claim in §1.4 is being realised.
- Chases, crossings, and *why* each crossing happened.

---

## 3. The placement ladder (shared by every algo)

For each child slice, in order:

1. **Post-only at the touch.** `OrderType.POST_ONLY`, join best bid/ask, improve
   by one tick when the spread leaves ≥2 ticks. Rejected-as-crossing is a
   *success signal* (the book moved to us) — re-price, do not treat as an error.
2. **Chase.** Unfilled after `chase_interval`: `cancel_and_place` at the new
   touch. Atomic, so no naked window. Bounded by `max_chases_per_slice`.
3. **Escalate on schedule debt.** If the slice's deadline passes and the parent
   is behind schedule beyond `catchup_tolerance`, cross with a **bounded
   marketable limit** (never `place_market_order` — it is an IOC at a 1%
   default the adapter does not override; this repo has already been bitten).
4. **Never cross more than `max_taker_fraction`** of the parent (default ~25%).
   Beyond that, fall behind schedule deliberately and report it — the volume
   target is not worth silently reverting to the 14.3 bp cost curve.

Two hard-won invariants from the v4.1 work, which apply to every algo:

- A marketable LIMIT **rests** if it cannot fully sweep. Every child order needs
  its own fill timeout, or a thin book strands a partial fill forever.
- Any stop must size its sweep from **what is actually held right now**, never
  from a prior slice's size, or the sweep can reach the user's own balance.

---

## 4. Algo-by-algo verdict

### 4.1 maker TWAP — **BUILD. Ship as the default.** ✅

Even time-slicing of the parent over a horizon; each slice through the ladder.

- Buildable today: `TWAPExecutor` already exists with a `mode: "MAKER"|"TAKER"`
  and slice/deadline mechanics.
- Correct default because it makes **no claim about the market** — it needs no
  volume signal, so the reflexivity problem in §1.3 cannot corrupt it.
- Delivers the entire 4× prize from §1.4.

### 4.2 chase — **BUILD.** ✅

Not really a scheduling algo — it is the *fill-certainty* mode of the ladder,
exposed as a user-facing choice ("fill fast, still maker"). Aggressive chase
interval, improve-by-a-tick, high `max_chases`, quick escalation.

- `LIMIT_CHASER` already exists in `OrderExecutor`.
- Upgrade: use `cancel_and_place` instead of cancel-then-spawn, removing the
  window where the parent has no live order.

### 4.3 maker POV (percentage of volume) — **BUILD, but gated.** ⚠️

Mechanically fine: the tape and `adv` are verified and real.

**But the signal is reflexive on this venue.** With a $10k target against
$13.7k/day, we are the majority of the volume we would be measuring — POV
would pace against its own shadow and spiral.

Therefore: only enable when the parent is a **small fraction of real ADV**.
Concretely, require `target_volume ≤ 10% × adv_excluding_our_prints`; otherwise
degrade to TWAP **and tell the user why**. On today's spot books that gate
means POV is effectively off for KBTC/WETH — which is the correct answer, not
a failure.

### 4.4 maker VWAP "(impact minimisation)" — **BUILD the pacing; REJECT the premise.** ⚠️

The volume-profile pacing is buildable (1m candles + tape).

**The impact-minimisation rationale does not hold here.** Measured impact at
$100/$500/$5,000 is **0.00 bp** — we never clear the first level. There is no
impact to minimise, so VWAP cannot beat TWAP on that axis at this size. It is
a *different pacing curve*, not a cheaper one.

Recommendation: ship it as **"volume-weighted pacing"**, with the same ADV gate
as POV, and do not market it as impact minimisation at these sizes. It becomes
genuinely useful when either (a) clips reach ~$25k, where impact first appears
(0.72 bp), or (b) it is pointed at a deep perp book (BTC-PERP does $33M/day —
1,000× the spot book, where a real intraday profile exists).

### 4.5 iceberg — **DO NOT BUILD.** ❌

Two independent reasons, either sufficient:

1. **Not supported.** There is no hidden / iceberg / display-size order type in
   the SDK — the enum is `{DEFAULT, IOC, FOK, POST_ONLY}`. A "synthetic
   iceberg" is just slicing with a re-post, i.e. TWAP wearing a different name.
   Shipping it as a distinct algo would be marketing a capability the venue
   does not have.
2. **Nothing to conceal.** An iceberg hides size from a book that would
   otherwise move against you. Our clip is $100–$500 against $5,998 at the
   touch — **1.7–8% of the first level**. No participant is re-pricing because
   of us.

If order sizes ever reach a meaningful fraction of top-of-book, revisit — but
it would still be synthetic slicing, and TWAP already provides that.

---

## 4b. What shipped (2026-07-31)

**maker TWAP (default)** and **chase**, per §4.1/§4.2. POV, volume-weighted
pacing and iceberg were **not** built — see §4.3–4.5.

- `quant/twap_schedule.py` — pure pacing math (target, debt, `should_cross`,
  slice planning under the venue minimum).
- `VolumeBotController.execution_algo` — `"twap"` (default) | `"chase"` |
  `"taker"`. The legacy `vol_taker_mode` remains an explicit per-user
  kill-switch and is deliberately **not** defaulted, or it would pin every run
  to taker and the new default could never apply.
- Crossing is now driven by **schedule debt**, not a wall-clock deadline: a leg
  only pays the taker fee once it is materially behind, and never beyond
  `vol_max_taker_frac` (25%) of the leg.
- Telemetry: `vol_execution_algo`, `vol_maker_fills`, `vol_taker_fills`,
  `vol_maker_fill_ratio`, `vol_chases`, `vol_crossed_quote`.

**Deliberately not built: slice-splitting.** The venue minimum is $100 and the
product band starts at $100, so a default clip cannot be split at all and a
$500 clip supports at most 4 pieces. With measured impact of 0.00 bp at these
sizes there is nothing for slicing to buy. `plan_slices` exists and is tested
so the pacing is ready if sizes ever grow, but the controller places one order
per leg today. The schedule governs *patience*, not slice count.

**Simulated against the measured book** (5.7 bp spread, maker 1.8 bp, taker
4.3 bp, $100 clip, idealised fills):

| Algo | Cost per round trip | Maker ratio |
| --- | --- | --- |
| taker | **14.30 bp** | 0% |
| maker TWAP | **−2.10 bp** (i.e. a small gain) | 100% |

The maker number is positive because resting both sides captures the 5.7 bp
spread against 3.6 bp of maker fees. **Treat it as an upper bound, not a
forecast:** it assumes every resting order fills at its price. Real maker fills
require someone to cross to us, and on a $13.7k/day book that is exactly the
uncertainty the chase ladder and the `max_taker_frac` brake exist to manage.
The honest claim is the *swing* — ~16 bp per round trip between always-crossing
and always-resting — with fill probability as the risk being taken.

## 5. Build order

| Phase | Scope | Why first |
| --- | --- | --- |
| 1 | `MarketContext` (book/tape/tickers, deduped, own-prints excluded) + benchmark reporting | Everything else needs it; reporting is what proves the 4× |
| 2 | `PlacementEngine` + `ChildOrderManager` with the §3 ladder | The shared risk surface; all algos inherit its correctness |
| 3 | **maker TWAP** as the vol bot default, replacing the v4.1 taker legs | Delivers the whole measured prize |
| 4 | **chase** mode | Small delta on the ladder; the throughput dial |
| 5 | POV + volume-weighted pacing, **behind the ADV gate** | Correct to build, honest to keep off for thin spot |
| — | iceberg | Not building — see §4.5 |

Phase 3 is where the user-visible win lands. Phases 4–5 are refinements, and
Phase 5's main deliverable may well be *the gate that keeps it switched off*.

## 6. Risks

- **Fill risk is the real cost of maker.** If maker legs do not fill, turnover
  collapses and the product fails at its actual job. The ladder's escalation
  and `max_taker_fraction` are the control; both must be surfaced in `/status`,
  not silent.
- **Gateway budget.** Book+tape polling per product per user is new load.
  `MarketContext` must be a shared singleton with a TTL, and the existing
  `venue/gateway_budget.py` throttle must gate it.
- **Self-matching — precisely.** A buy cannot match a buy, so multiple live
  child orders on the *same* side are safe: a TWAP resting slice N while N−1
  still works is fine. The exposure is **only** a live buy and a live sell at
  once, i.e. overlapping legs. Today's bot is sequential (buy completes, then
  sell), which is exactly the carve-out `docs/mm_volume_tuning.md` relies on to
  keep the vol bot clear of Nado's wash-trading rule.

  So the invariant to preserve is narrow and cheap: **never run the buy and
  sell parents concurrently on the same product.** Keep legs sequential and the
  carve-out holds unchanged. If a future version ever wants overlapping legs
  for throughput, that needs an explicit self-match guard *and* a policy
  decision — do not let it arrive by accident as a side effect of adding
  algos.
