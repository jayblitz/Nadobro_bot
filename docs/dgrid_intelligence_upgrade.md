# Nadobro D-Grid Upgrade Plan

**Goal:** Turn D-Grid from a passive layer-placer into an inventory- and regime-aware market maker that scales positions intelligently, manages open inventory after the order fires, and stops handing money to better-informed counterparties.

**Scope:** This plan is centered on Dynamic Grid (`dgrid`). Because D-Grid in `mm_bot.py` runs through the shared GRID/RGRID engine, every fix here will automatically also harden the classic Grid and Reverse Grid paths.

**Repo focus:** `src/nadobro/strategies/mm_bot.py` (the entire grid family), with new helper modules added alongside.

---

## 1. Executive Summary

The attached trade log (`f14288_default_inkMainnet_trades_*.csv`, 12 May 2026) shows the diagnosis you already feel: **178 fills, net −$12.41**, with **49 wins averaging +$0.045** and **41 losses averaging −$0.272**. Losses are roughly **6× the size of wins** per fill. A single WTI close at −$3.62 burned 29% of the day's PnL in one go.

The root cause is not the regime-switching logic; the regime detector in `_apply_dgrid_controls` works as designed. The root cause is that **everything downstream of the regime decision is static**:

- Every quote is sized at exactly `min_order_notional_usd / mid`. There is no scaling for inventory, regime, conviction, or recent fill performance.
- "Inventory awareness" only re-weights which side gets more *quote slots*, not how much each slot is worth.
- The only exit logic is a per-cycle PnL stop/take that fires on the entire session and shuts the bot off. There is no per-position TP, no partial close, no scale-out on profits, no scale-in on conviction.
- There is no re-entry policy after a stop. If the bot stops, it stays stopped.
- The variance-ratio regime signal is computed from 4-vs-12 mid points; on a fast-moving market like WTI that signal is too lagging to size down before the directional move chews through layers.

This plan adds three intelligence layers on top of the existing engine: **(A) Adaptive Layer Sizing**, **(B) Multi-Signal Regime Classifier**, and **(C) Active Position Manager**. Each is implemented as a new module that the existing `run_cycle` calls at three well-defined hook points — no rewrite, just promotion of the static defaults to data-driven decisions.

---

## 2. What the CSV Tells Us

Per-market breakdown for 12 May 2026:

| Market | Fills | Closes | Realized PnL | Fees | Net | Notes |
|---|---|---|---|---|---|---|
| QQQ | 42 | 16 | −$1.31 | −$0.86 | **−$2.17** | Choppy, range +/-14 bp |
| SPY | 45 | 25 | +$0.26 | −$0.76 | **−$0.50** | Best behavior of the three |
| WTI | 91 | 49 | −$7.88 | −$1.86 | **−$9.74** | 169 bp range, one −$3.62 close |
| **Total** | **178** | **90** | **−$8.94** | **−$3.48** | **−$12.41** | |

Three telling patterns:

1. **Uniform 0.142 / 0.136 / 0.12 base unit sizing across every fill** — confirming the static `min_order_notional_usd / mid` size in `mm_bot.py:1631-1633`. Every quote was ~$100 of notional regardless of context.
2. **Asymmetric tail** — the win distribution is bounded above by the spread (which D-Grid keeps tight in `grid` phase: ~8 bp via `GRID_MIN_SPREAD_BP`), but the loss distribution is bounded only by when the position manually got closed. The 6:1 asymmetric ratio is the signature of "tight wins, ungated losses."
3. **WTI burned 78% of the day's losses on 51% of the volume.** WTI moved 169 bp intraday — three to four standard deviations of what the dgrid_min_spread_bp=2 / max=50 envelope expected. The regime detector did flip phases, but the *position size never came down*, so each adverse layer cost the same as each ranging-market layer.

The bot is not broken on entries. The bot is broken on **size selection** and **position management after fill**.

---

## 3. Current D-Grid: How It Works Today

The full pipeline for one D-Grid cycle (referenced to `mm_bot.py` line numbers):

| Step | Function | What it does | Decision quality |
|---|---|---|---|
| 1. Mid history | `_update_mid_history` (75) | Rolling buffer of up to 40 mid prices. | Adequate. |
| 2. Regime select | `_apply_dgrid_controls` (137) | Variance-ratio of returns over 4 vs 12 points. ≥1.25 → rgrid phase; ≤1.15 → grid phase; hysteresis band keeps current. | Lagging on fast moves. Single-signal. |
| 3. Spread sizing | same | Spread = realized vol clamped to [`dgrid_min_spread_bp`, `dgrid_max_spread_bp`]. In rgrid phase, spread is forced to 0. | OK in range, blind in trend. |
| 4. Soft reset threshold | same | Snapped to {5, 12.5, 25, 50, 100} bp; ~4× spread. | OK. |
| 5. Inventory read | run_cycle (1183) | Sum of net units across positions on the product. | OK. |
| 6. Slot-side multipliers | `_resolve_side_multipliers` (255) | `long_bias → buy×1.15, sell×0.85`; soft-cap breach → `buy×0.55, sell×1.45`; hard-cap → `pause_flatten_only`. **Only changes slot counts, not sizes.** | **The big miss.** |
| 7. Slot allocation | `_mm_allocate_quote_levels` (333) | Splits max_resting_quotes by buy/sell mults. | OK. |
| 8. Grid prices | `_compute_grid_prices` (518) | For levels 1..N: `buy = ref * (1 − spread×i)`, `sell = ref * (1 + spread×i)`. Post-only clamped to top of book. | OK shape, no size variation per level. |
| 9. **Quote sizing** | run_cycle (1631-1633) | `size = min_order_notional_usd / mid` — **every level, every cycle, every regime gets the same notional.** | **The other big miss.** |
| 10. Stale-quote cancel | `_cancel_stale_orders` (566) | Cancels orders that drifted > `STALE_DRIFT_FLOOR_BP`. | OK. |
| 11. SL/TP | run_cycle (1285-1346) | Per-cycle PnL ≤ −max_loss → halt; ≥ +max_profit → halt. | **Halts everything, no partial close, no re-entry.** |
| 12. Drawdown breaker | run_cycle (845-853) | Cumulative PnL drawdown > `mm_max_drawdown_pct` → halt. | Hard kill only. |

The shape is right. The decisions are too static.

---

## 4. Root Cause Analysis

The losses in the CSV map cleanly to five concrete code-level issues:

**Issue 1 — Constant quote notional.** `mm_bot.py:1631-1633` sizes every quote at exactly the venue minimum (`$100` on these pairs). No scaling for inventory, vol regime, recent win-rate, or conviction. On WTI in a 169 bp trend, the bot kept placing $100 longs into a falling market at the same size as if it were a 5 bp range.

**Issue 2 — Inventory pressure expressed only as slot count.** `_resolve_side_multipliers` (line 255) modifies `buy_mult`/`sell_mult`, but those multipliers feed `_mm_allocate_quote_levels` to redistribute **slots**, not size. With `max_orders = 6` and 3-4 levels per side, you only have integer slots; a 0.55/1.45 multiplier rounds to one slot of difference, which is too coarse to matter on a 91-fill WTI session.

**Issue 3 — Single-signal, lagging regime detector.** `_compute_variance_ratio` uses 4 vs 12 mid points (line 99). At `interval_seconds = 60` that's a 4-minute vs 12-minute look-back. A WTI move from $95.55 to $97.17 took longer than 12 minutes, so the variance ratio stayed in the hysteresis band and the bot kept its `grid` phase. The regime detector also ignores: directional drift, depth imbalance, funding rate, ATR, fill-distance asymmetry — all signals that would have flagged WTI as trending earlier.

**Issue 4 — Session-level SL/TP, no per-position management.** Lines 1285-1346 stop the **entire cycle** when cumulative PnL hits the limit. There is no logic for: closing a single losing layer, scaling out a winning layer in pieces, trailing a TP, or re-quoting at a better level after a partial close. The single WTI close at −$3.62 happened because a position was held until it was forced shut by a session-level mechanism, not because the bot intelligently exited it.

**Issue 5 — No re-entry / cool-down policy.** When `grid_stop_loss_hit` or `grid_take_profit_hit` fires, `run_cycle` returns and the bot expects a higher-level scheduler to start it back up unchanged. There's no concept of "the regime that caused this loss is still here, so quote wider / size smaller / wait 5 minutes before re-engaging."

A sixth, secondary issue: **min-spread floor is too low for the fee structure.** `GRID_MIN_SPREAD_BP = 8.0` round-trip is 16 bp, which on the Ink venue net of fees still doesn't clear the adverse-selection cost on a directional name like WTI. The MM design doc (`docs/mm_strategy_design.md`) already proposes 1.5 bp per side as the *floor*, but assumes spread will widen above it from vol. D-Grid's `dgrid_min_spread_bp = 2` is below what the unit economics support on volatile pairs.

---

## 5. The Upgrade: Three Intelligence Layers

The plan adds three new modules. Each is small, has a single responsibility, and is called from a defined hook in `run_cycle`. The existing engine logic stays.

### A. Adaptive Layer Sizing — `strategies/_layer_sizing.py`

**Purpose:** Decide how much notional each level should carry, given current inventory, regime, recent fill performance, and conviction. Replace the constant `min_order_notional_usd / mid` with a sized-per-level vector.

**Inputs:**

- `net_units`, `inv_usd`, `inv_soft_limit_usd` — current inventory.
- `regime` (from layer B) — `range_tight`, `range_wide`, `trend_up`, `trend_down`, `chop_high_vol`.
- `realized_vol_bp`, `dynamic_spread_bp` — already computed.
- `recent_fill_stats` — rolling 50-fill win-rate and average PnL per side.
- `level_index` — which level of the ladder (1..N).
- `base_notional_usd` — the user's configured per-cycle notional.
- `min_order_notional_usd` — venue floor.

**Sizing policy (per side, per level):**

```
size_usd = base_notional_usd                # start from user notional
        × side_inventory_brake              # 0.25 .. 1.0 based on |q|/Q_soft
        × regime_size_mult[regime, side]    # 0.4 .. 1.5 per regime
        × level_taper(level_index)          # 1.0 / 0.7 / 0.5 / 0.35 geometric
        × fill_performance_mult[side]       # 0.7 .. 1.3 based on rolling PnL
        × volatility_scaling                # min(1.0, target_vol_bp/realized_vol_bp)
size_usd = clamp(size_usd, min_order_notional_usd, max_per_level_usd)
```

**Five multipliers, each capped:**

1. **`side_inventory_brake`** — once `|inv| > 0.3 × Q_soft`, brake the same-side adds: `1.0 − 0.75 × (|inv| − 0.3·Q_soft) / (Q_soft − 0.3·Q_soft)`. At `|inv| = Q_soft`, same-side adds drop to 0.25× notional. Today this is binary (suppress entire side at hard cap); we replace it with a smooth ramp.
2. **`regime_size_mult`** — a 5×2 table (regime × side). In `trend_up`: counter-trend (sell) side gets 0.5× and trend (buy) side gets 1.0×. In `range_tight`: both sides 1.5× (capture more of the round-trip). In `chop_high_vol`: both sides 0.4× — quote, but small.
3. **`level_taper`** — outer levels get less size, not more. A geometric `1.0 / 0.7 / 0.5 / 0.35` for levels 1-4 means the level closest to mid carries the most weight; outer levels are reconnaissance. Today every level is identical.
4. **`fill_performance_mult`** — rolling 50-fill side P&L. If the buy side has been losing money for the last 50 fills, scale buy size by 0.7×; if winning, 1.3×. This is the "stop adding to a losing thesis" lever.
5. **`volatility_scaling`** — when realized vol is double the configured target, halve size. This is what would have caught the WTI 169-bp move: target 30-50 bp daily, realized 169 bp → scale to 0.18-0.3×.

**Inventory cap behavior:**

The existing `inv_soft_limit_usd` and 1.8× hard cap stay in place as outside-the-formula stops. The layer-sizing module adds a smoother *brake* between 0.3× and 1.0× of the soft cap, plus a hard suppress at hard cap (the current behavior).

**Pseudocode:**

```python
# strategies/_layer_sizing.py

def size_quote_level(
    side: str,                    # "buy" or "sell"
    level: int,
    base_notional_usd: float,
    inv_usd: float,
    net_units: float,
    inv_soft_usd: float,
    regime: str,
    realized_vol_bp: float,
    target_vol_bp: float,
    recent_fill_stats: dict,
    min_notional: float,
    max_per_level: float,
) -> float:
    # 1) inventory brake
    inv_dir = "buy" if net_units < 0 else ("sell" if net_units > 0 else side)
    same_side = (side == ("buy" if net_units > 0 else "sell"))
    brake = 1.0
    if inv_soft_usd > 0:
        ratio = abs(inv_usd) / inv_soft_usd
        if same_side and ratio > 0.3:
            brake = max(0.25, 1.0 - 0.75 * (ratio - 0.3) / 0.7)

    # 2) regime multiplier
    regime_mult = _REGIME_TABLE.get(regime, {}).get(side, 1.0)

    # 3) level taper
    taper = (0.7 ** (level - 1))  # 1.0, 0.7, 0.49, 0.343
    taper = max(taper, 0.35)

    # 4) fill performance
    side_stats = recent_fill_stats.get(side, {})
    perf_mult = _fill_perf_mult(side_stats)  # 0.7..1.3

    # 5) vol scaling
    vol_mult = min(1.0, target_vol_bp / max(realized_vol_bp, 1.0))

    size_usd = base_notional_usd * brake * regime_mult * taper * perf_mult * vol_mult
    return max(min_notional, min(size_usd, max_per_level))
```

**Default regime table (tune in backtest):**

```
                buy_mult   sell_mult
range_tight     1.50       1.50
range_wide      1.00       1.00
trend_up        1.00       0.50
trend_down      0.50       1.00
chop_high_vol   0.40       0.40
```

**Integration point in `run_cycle`:** Replace lines 1631-1633 (the static `min_quote_size`) with a per-level call to `size_quote_level()` inside the `for order_spec in grid_orders` loop (around line 1660). The `per_level_buy_size` and `per_level_sell_size` variables go away; size is computed per order spec.

### B. Multi-Signal Regime Classifier — `strategies/_regime.py`

**Purpose:** Promote regime detection from a single variance-ratio (range vs trend) to a five-state classifier that the sizing module and the position manager both consume.

**Output regimes:**

- `range_tight` — variance ratio low AND realized vol bp below `dgrid_min_spread_bp × 2`.
- `range_wide` — variance ratio low AND realized vol bp normal.
- `trend_up` — variance ratio high AND directional drift positive AND ATR rising.
- `trend_down` — same with negative drift.
- `chop_high_vol` — variance ratio high but directional drift small (whipsaw).

**Signals consumed (composite score):**

1. **Variance ratio** — keep the existing `_compute_variance_ratio`. Weight: 1.0.
2. **Directional drift** — `(mid_now − mid_short_window_ago) / mid_now × 1e4` in bp. Positive → trend_up bias. Weight: 1.0.
3. **EMA crossover** — `_detect_ema_crossover` is already implemented (line 395). Reuse. Weight: 0.5.
4. **ATR-like range expansion** — rolling max-min of the last `dgrid_long_window_points` mids divided by mid, in bp. Rising = trend. Weight: 0.5.
5. **Fill-distance asymmetry** — rolling-window asymmetry of fills (how far on the buy side they filled vs the sell side). One-sided heavily filled side → adverse selection / trend. Weight: 0.5. New telemetry needed; track in `state["mm_recent_fill_distances"]`.
6. **Funding sign and shift** — `_detect_funding_shift` already exists (line 448). Reuse with current sign as a tie-breaker for trend direction. Weight: 0.3.

**Output:**

```python
{
  "regime": "trend_down",
  "confidence": 0.72,         # 0..1, used to dampen size_mult away from base when low
  "variance_ratio": 1.47,
  "drift_bp": -38.0,
  "ema_div_bp": 12.5,
  "range_expansion_bp": 95.0,
  "fill_asymmetry": 0.31,
  "funding_bp": 1.2,
  "regime_changed": True,
}
```

**Hysteresis:** keep the existing `dgrid_trend_on_variance_ratio = 1.25` and `dgrid_range_on_variance_ratio = 1.15` bands as the *primary* gate, but the secondary signals can override only on strong agreement (≥3 of the 5 secondary signals pointing the same way) to avoid flip-flopping.

**Integration point in `run_cycle`:** Right after `_apply_dgrid_controls` (line 1098), call `classify_regime(state, history, mid, client, product_id)`. Use its `regime` field for both Layer A (sizing) and Layer C (position manager). Keep `dgrid_state["phase"]` as the spread/order-shape selector — that part still works.

### C. Active Position Manager — `strategies/_position_manager.py`

**Purpose:** Manage **inventory after fill**, independently of the quote-placement loop. This is the biggest behavioral change: the bot stops being "place orders and hope" and starts being "every cycle, decide what to do with current inventory."

**The PM runs at the *top* of every cycle, before quote placement.** It reads:

- Current positions (`live_position_rows`).
- Entry VWAP per side (already tracked via `_rolling_vwap_recent_fraction`).
- Realized + unrealized PnL.
- Time-in-position.
- Current regime (from Layer B).
- Recent fill stats.

**Actions the PM can take:**

1. **Partial scale-out on profit.** If a side's unrealized PnL ≥ `pm_partial_tp_bp` (default 8 bp on inventory cost basis), submit a reduce-only market or post-only IOC for 25-50% of that side's inventory. Re-quote the harvested level wider. *This is what would have prevented the WTI run-up from being given back.*

2. **Aggressive close on regime flip against inventory.** If `regime` flips to trend-against-inventory with confidence > 0.65 AND inventory is non-trivial AND unrealized PnL is negative, close 50% of the adverse side via reduce-only IOC. Don't wait for the session SL. *This is the "cut losers" lever; today there is none.*

3. **Trailing TP on extended winners.** Track high-water-mark unrealized PnL per side; if current PnL falls below `0.5 × hwm` AND `hwm > pm_trail_arm_bp`, close the side. Lets winners run, locks before reversal.

4. **Stale-inventory flatten.** If a position has been held for `> pm_stale_hold_minutes` (default 30) AND realized vol is rising AND we're still in inventory, flatten reduce-only IOC. Long-held inventory in a vol-expanding regime is the worst expected-value position.

5. **Re-quote anchor reset.** After any PM-driven close, reset `grid_anchor_price` to mid so the next set of quotes doesn't keep bracketing the closed inventory's VWAP.

6. **Post-stop cool-down.** When a session SL/TP fires (existing path), instead of just halting, the PM enters a `cooldown` state for `pm_cooldown_seconds` (default 300). On exit it re-engages with: (a) sizing multiplier 0.5× for the first 10 minutes, (b) min_spread floor doubled. Earned back to normal over the next 30 minutes if PnL is positive.

**The PM does *not* place new quotes itself.** It only closes (reduce-only). Quote placement stays in the existing engine, which now consumes the PM's "skip add-side, only post reduce" signal.

**Pseudocode:**

```python
# strategies/_position_manager.py

def manage_positions(state, client, product_id, mid, regime_info, positions, fill_stats):
    actions = []
    inv_long, inv_short = _split_inventory(positions, product_id)
    pnl_long, pnl_short = _unrealized_per_side(positions, product_id, mid)

    # Update HWM
    state.setdefault("pm_hwm", {"long": 0.0, "short": 0.0})
    state["pm_hwm"]["long"] = max(state["pm_hwm"]["long"], pnl_long)
    state["pm_hwm"]["short"] = max(state["pm_hwm"]["short"], pnl_short)

    cfg = _pm_config(state)

    # 1) Partial TP
    for side, inv_u, pnl, vwap in [("long", inv_long, pnl_long, state.get("grid_buy_exposure_price")),
                                    ("short", inv_short, pnl_short, state.get("grid_sell_exposure_price"))]:
        if inv_u <= 0 or vwap is None or vwap <= 0:
            continue
        pnl_bp = (pnl / (inv_u * vwap)) * 1e4 if inv_u * vwap > 0 else 0
        if pnl_bp >= cfg["partial_tp_bp"]:
            close_size = inv_u * cfg["partial_tp_fraction"]
            actions.append({"type": "partial_close", "side": side, "size": close_size,
                            "reason": f"partial_tp {pnl_bp:.1f}bp"})

    # 2) Regime-against-inventory cut
    if regime_info["confidence"] >= cfg["cut_confidence_threshold"]:
        if regime_info["regime"] == "trend_down" and inv_long > 0 and pnl_long < 0:
            actions.append({"type": "adverse_cut", "side": "long",
                            "size": inv_long * 0.5, "reason": "regime_against_long"})
        if regime_info["regime"] == "trend_up" and inv_short > 0 and pnl_short < 0:
            actions.append({"type": "adverse_cut", "side": "short",
                            "size": inv_short * 0.5, "reason": "regime_against_short"})

    # 3) Trailing TP
    for side, inv_u, pnl, hwm in [("long", inv_long, pnl_long, state["pm_hwm"]["long"]),
                                   ("short", inv_short, pnl_short, state["pm_hwm"]["short"])]:
        if inv_u > 0 and hwm > cfg["trail_arm_usd"] and pnl < 0.5 * hwm:
            actions.append({"type": "trail_close", "side": side, "size": inv_u,
                            "reason": f"trail hwm={hwm:.2f} pnl={pnl:.2f}"})

    # 4) Stale flatten
    last_fill_ts = state.get("grid_last_fill_ts", 0)
    if last_fill_ts and (time.time() - last_fill_ts) > cfg["stale_hold_seconds"]:
        if regime_info["range_expansion_bp"] > 2 * cfg["stale_vol_normal_bp"]:
            if inv_long > 0:
                actions.append({"type": "stale_flatten", "side": "long", "size": inv_long,
                                "reason": "stale+vol_expand"})
            if inv_short > 0:
                actions.append({"type": "stale_flatten", "side": "short", "size": inv_short,
                                "reason": "stale+vol_expand"})

    # Execute
    for a in actions:
        _execute_reduce_only_ioc(client, product_id, a["side"], a["size"], mid, source="dgrid_pm")
        logger.info("PM action: %s %s size=%.6f reason=%s", a["type"], a["side"], a["size"], a["reason"])

    return actions
```

**Integration point in `run_cycle`:** Insert right after the position read at line 1199, before the cancel-stale-orders block. Pass the regime classification from Layer B. PM actions are reduce-only IOCs through the existing `execute_market_order(reduce_only=True)` path used by `volume_bot` (see `mm_strategy_design.md` §5).

---

## 6. New Config Parameters

These are added to whatever config struct `mm_bot.py` reads `state[...]` from. All have safe defaults so an existing user's running session doesn't need to be reconfigured.

| Param | Default | Range | Purpose |
|---|---|---|---|
| `pm_enabled` | `True` | bool | Master switch for position manager (Layer C). |
| `pm_partial_tp_bp` | `8.0` | 3-30 | Per-side unrealized PnL bp threshold to trigger partial close. |
| `pm_partial_tp_fraction` | `0.33` | 0.1-0.75 | Fraction of side inventory to close on partial TP. |
| `pm_cut_confidence_threshold` | `0.65` | 0.5-0.9 | Regime confidence required to fire an adverse cut. |
| `pm_trail_arm_usd` | `0.5` | 0.1-5.0 | Unrealized PnL HWM threshold to arm trailing TP. |
| `pm_stale_hold_minutes` | `30` | 5-180 | Time-in-inventory after which the stale-flatten rule activates. |
| `pm_cooldown_seconds` | `300` | 30-1800 | Pause after session SL/TP before re-engaging. |
| `layer_target_vol_bp` | `30.0` | 5-200 | Target realized vol the sizing module scales toward. |
| `layer_max_per_level_usd` | `notional × 3` | — | Per-level notional ceiling. |
| `layer_taper_ratio` | `0.7` | 0.4-1.0 | Geometric taper across the ladder. |
| `regime_min_signal_agreement` | `3` | 2-5 | Secondary signals needed to override variance-ratio regime. |
| `dgrid_min_spread_bp` | `3.0` (was 2.0) | 1-20 | Raised to clear the Ink fee structure (1.5 bp/side + slippage buffer). |

Existing parameters kept verbatim: `dgrid_trend_on_variance_ratio`, `dgrid_range_on_variance_ratio`, `dgrid_max_spread_bp`, `dgrid_short_window_points`, `dgrid_long_window_points`, `inv_soft_limit_usd`, `mm_max_drawdown_pct`, `grid_stop_loss_pct`, `grid_take_profit_pct`.

---

## 7. Integration: Surgical Edits to `mm_bot.py`

The goal is minimum invasive change. Three new module files, four edits in `mm_bot.py`:

**New files:**
- `src/nadobro/strategies/_layer_sizing.py` (Layer A)
- `src/nadobro/strategies/_regime.py` (Layer B)
- `src/nadobro/strategies/_position_manager.py` (Layer C)

**Edits in `mm_bot.py`:**

1. **At line 1098** (after `_apply_dgrid_controls` call): add `regime_info = classify_regime(state, history, mid, client, product_id, dgrid_state)`. Persist `state["regime_info"] = regime_info`.

2. **At line 1199** (after `live_position_rows` is built, before stale-cancel): add `if configured_strategy == "dgrid" and state.get("pm_enabled", True): pm_actions = manage_positions(state, client, product_id, mid, regime_info, positions, fill_stats)`. The `fill_stats` helper reads `grid_buy_fills` / `grid_sell_fills` (already tracked) and computes a 50-fill rolling per-side win-rate and avg PnL.

3. **At lines 1631-1633** (replace static `min_quote_size`): remove the two-line constant. Inside the `for order_spec in grid_orders` loop at line 1660, compute `size_to_use = size_quote_level(...)` per spec using Layer A. Keep the existing `size_to_use = max(size_to_use, min_order_notional_usd / mid)` floor in case the formula produces sub-min sizes.

4. **At lines 1315-1346** (the session SL/TP halt branches): instead of returning immediately, set `state["pm_cooldown_until"] = time.time() + cfg["pm_cooldown_seconds"]` and let the next cycle's PM logic handle re-engagement at reduced size.

5. **At line 1098** (after `_apply_dgrid_controls`): bump `dgrid_min_spread_bp` default from 2.0 to 3.0 (line 43, constant `DGRID_MIN_SPREAD_BP` is not defined yet — the doc lists default 2 in the state lookup at 1057 via `GRID_MIN_SPREAD_BP=8.0`; this is one of those places where the design doc and code disagree, surface it in a comment).

That's the entire integration surface. The new modules are pure functions of `state` plus injected client/positions, so they're unit-testable in isolation.

---

## 8. Rollout Plan & KPIs

**Phase 0 — Parameterize and ship dead-code (1-2 days).**
Land the three new module files with full pseudocode → implementation, but gated behind `state.get("dgrid_intelligence_enabled", False)`. Default off. Wire the call sites. Run the existing test suite to confirm no regressions when the flag is off.

**Phase 1 — Layer A (sizing) only, testnet (3-5 days).**
Turn on `dgrid_intelligence_enabled = True` but stub Layer B to always return `range_wide` and Layer C to no-op. Verify on testnet that quote sizes vary per level and per inventory state. Goal: replay the WTI move and confirm size drops at least 50% by mid-move.

**Phase 2 — Layer B (regime) + Layer A, testnet (3-5 days).**
Activate the real regime classifier. Verify regime label transitions are stable (no flip-flopping within 60s) and that sizing responds. Reproduce 24h of historical mid history per pair offline and label regimes.

**Phase 3 — Full stack, mainnet small size (1 week).**
Cap `notional` at 50% of current and run Layers A+B+C live on one pair (start with SPY — your least-bad market in the CSV). Compare 7-day net PnL to the same parameters with intelligence disabled.

**KPIs to clear before scaling to all markets:**

| KPI | Today | Target after upgrade |
|---|---|---|
| Win / loss size ratio | 1 : 6.0 | ≥ 1 : 2.5 |
| Net PnL on representative day | −$12.41 | ≥ −$3 (then ≥ +$0) |
| Largest single losing close as % of day | 29% (WTI) | ≤ 12% |
| Fees as % of gross PnL | 39% | ≤ 25% (sizing should reduce fill churn) |
| Median time-in-inventory before close | unbounded | ≤ 20 min |
| Session SL hits per week | (target a baseline) | drop ≥ 50% as PM cuts earlier |

**Phase 4 — Roll out to QQQ and WTI.**
Once SPY clears two consecutive winning weeks, promote to the other markets. WTI keeps a tighter `layer_max_per_level_usd` cap (`notional × 1.5` vs `× 3`) until the regime classifier has 30 days of WTI-specific labels.

---

## 9. What This Does *Not* Do

To keep scope bounded, the following are deliberately deferred:

- **No new entry strategy.** Quote prices still come from `_compute_grid_prices`. The only change is what *size* and *whether* to place each level.
- **No Avellaneda-Stoikov.** That's the separate `AS_MM` strategy proposed in `mm_strategy_design.md`. This plan upgrades D-Grid in place.
- **No multi-pair correlation.** Each market is managed independently. WTI's regime doesn't inform SPY's sizing.
- **No order-book microstructure.** We don't read depth, queue position, or trade-tape adverse selection. Phase 4 candidates.
- **No funding-driven directional bet.** Funding sign is read as a tie-breaker for regime direction only; we don't size up the same-side as funding pays.

---

## Appendix A — Where Each Fix Maps to the CSV Pain

| CSV symptom | Today's behavior | Fix in this plan |
|---|---|---|
| Every fill uniformly $100 | `min_order_notional_usd / mid` constant | **Layer A: `size_quote_level`** scales per inventory, regime, level, vol. |
| 49 wins × $0.045 vs 41 losses × $0.272 | TP/SL only at session level; no per-position trail | **Layer C: partial TP, trailing TP, adverse cut** sized by side. |
| Single WTI close at −$3.62 | Position held until session SL; no per-position cut | **Layer C: adverse_cut** fires when regime flips against inventory with confidence > 0.65. |
| 91 WTI fills for −$9.74 (high churn) | Static sizing keeps quoting through a directional move | **Layer A: `regime_size_mult` × `volatility_scaling`** cuts WTI fill rate in trend. |
| Fees = 28% of gross loss | Constant min-notional quotes churn the fee meter | Layer A's level taper + Layer B's regime-aware quote count reduce churn ≥ 30%. |
| No re-engagement logic | Bot halts on SL; same parameters next start | **Layer C: cooldown + reduced-size re-engagement** for 10-30 min after stop. |

## Appendix B — File Tree After Upgrade

```
src/nadobro/strategies/
├── __init__.py
├── bro_mode.py           (unchanged)
├── delta_neutral.py      (unchanged)
├── mm_bot.py             (~50 line diff: 4 call-site edits + 1 constant bump)
├── volume_bot.py         (unchanged)
├── _layer_sizing.py      (NEW, ~200 lines incl. regime table + tests)
├── _regime.py            (NEW, ~250 lines incl. all signal computations)
└── _position_manager.py  (NEW, ~300 lines incl. all 4 PM actions + cooldown)
```

Plus tests under `tests/strategies/test_layer_sizing.py`, `test_regime.py`, `test_position_manager.py`. Suggested unit-test scenarios: replay the CSV mids and confirm (1) sizing drops on WTI by mid-move, (2) regime label flips to `trend_down` within 5 minutes of the WTI run, (3) PM fires an `adverse_cut` on the WTI long inventory before the −$3.62 close would have occurred.

---

*Plan written 2026-05-12. Built against `jayblitz/Nadobro_bot` `main` at the time of writing (commit referenced via README's "Latest Updates (March 2026)"). Trade evidence: `f14288_default_inkMainnet_trades_1778457600000_1778630399999.csv`.*
