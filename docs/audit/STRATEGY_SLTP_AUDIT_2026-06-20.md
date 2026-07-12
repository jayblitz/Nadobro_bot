# Nadobro Strategy & SL/TP Audit — 2026-06-20

**Branch:** `audit/strategy-sltp-self-review`
**Scope:** MM (grid / rgrid / dgrid / mid), Volume (vol), Copy Trading, Delta Neutral (dn), plus the shared SL/TP rail, engine/risk, and backtester.
**Method:** Four parallel read-only auditors traced each path end-to-end; every finding cites `file:line` and quotes the offending code. Top findings independently re-verified by the orchestrator (see "Verification" column). No code was changed in this pass.

**Legend:** `[VERIFIED]` = code path traced and confirmed (including a second independent check for the top items). `[SUSPECTED]` = looks wrong but not fully confirmed; treat as a lead, not a fact.

---

## 0. Executive summary

The **execution layer is strong** — the grid executor, DN leg-integrity machinery, orchestrator orphan handling, order lifecycle, and inventory repo are well-hardened and were marked clean. The damage is concentrated in three places:

1. **SL/TP semantics** — the user's single SL/TP number is interpreted inconsistently (price-move barrier vs % of margin), measured on a **gross-of-fees** PnL, and for **Delta Neutral it is not enforced at all**.
2. **Strategy config plumbing** — user inputs silently fail to reach the controller (vol margin, vol SL, dead dgrid keys), so the bot trades different parameters than the UI shows.
3. **No safety net** — the `backtester/` package is empty stubs, so "does this strategy bleed money?" cannot be answered before going live.

### Cross-agent convergence (highest confidence)
Three of four independent auditors landed on the **same root cause** from different entry points: **`session_pnl` excludes fees** (`live_session.py:247`), and that value feeds the SL/TP rail (`bot_runtime.py:2193`) and the DN share card. This convergence, plus a direct re-read, makes it the most trustworthy systemic finding.

### Top 10 ranked (by money impact × confidence)

| # | ID | Sev | Verif | One-line |
|---|-----|-----|-------|----------|
| 1 | DN-RAIL | Critical | VERIFIED | Delta Neutral has **no** session SL/TP enforcement — user stops silently ignored. |
| 2 | SLTP-GROSS | High | VERIFIED | SL/TP rail fires on gross-of-fees PnL → stops trip late by the accumulated fee drag (all strategies). |
| 3 | GRID-DUAL-UNIT | High | VERIFIED | Grid/rgrid/dgrid apply the user's SL **twice** with conflicting units (price-move vs % margin); the tighter price barrier wins → premature stop-outs. |
| 4 | DN-PNL-FEES | High | VERIFIED | DN headline PnL = realized + funding, **fees never subtracted**; a test locks the overstatement in as "correct". |
| 5 | VOL-MARGIN | High | VERIFIED | Vol ignores the user's `session_margin_usd`; trades the $100 default. |
| 6 | VOL-LOOP | High | VERIFIED | Vol stops after one round-trip; never loops to the target volume; never marks itself stopped. |
| 7 | COPY-SIZE | High | VERIFIED | Copy sizing ignores the leader's size **and** leverage; every mirror is fixed-notional at max leverage. |
| 8 | DGRID-BOOK-RACE | High | VERIFIED | dgrid profit-booking fires uncoordinated MARKET orders that race the executor's resting close legs → inventory drift + taker fees. |
| 9 | DGRID-RECENTER | High | VERIFIED | Re-center sizes a fresh full ladder without subtracting held notional → deployed size ratchets above the risk-approved amount. |
| 10 | GRID-TP-DEAD | High | VERIFIED | Executor-level take-profit is dead for the whole grid family (`take_profit` passed but never read). |

---

## 1. SL/TP — the priority area

### How it actually works (traced ground truth)
The user's one SL/TP number drives **two independent mechanisms with different units**:

- **Session PnL rail** — `bot_runtime._evaluate_session_pnl_rail` (`bot_runtime.py:2144`), reads SL/TP via `effective_sl_tp_pct` and compares against `session_pnl` as **% of margin**. This is what the UI promises ("SL 1% of margin").
- **Grid executor triple-barrier** — `map_strategy_config` (`engine_runtime.py:610`) passes `stop_loss=sl` into `TripleBarrierConfig`, where `GridExecutor._stop_breached` (`grid_executor.py:372`) treats it as a **price-move fraction**.

The key-name wiring (grid/mid → `sl_pct`/`tp_pct`; rgrid/dgrid → `rgrid_stop_loss_pct`/`rgrid_take_profit_pct`) is now **correct** via `effective_sl_tp_pct` (`strategy_registry.py:46`). The earlier key-mismatch is genuinely fixed — **not a current bug**. The live bugs are in semantics, coverage, and double-application.

### DN-RAIL — Delta Neutral has no session SL/TP at all — **Critical [VERIFIED]**
`bot_runtime.py:2474` puts `dn` in the *skip* list for the legacy rail, and the post-dispatch rails fire only for grid/rgrid/dgrid/mid (`:2654`) and vol (`:2668`). There is **no `_evaluate_session_pnl_rail` call for `dn`**. The DN per-leg barriers are off by default (`dn_leg_tp_pct`/`dn_leg_sl_pct` = 0.0 → `barriers=None`, `engine_runtime.py:504-516`), and the DN controller has no PnL guard.
*Re-verified:* lines 2654 and 2668 only name grid/rgrid/dgrid/mid and vol; dn appears nowhere as a rail caller.
**Symptom:** A user setting DN `sl_pct`/`tp_pct` (defaults `tp 0.8 / sl 0.6`, shown at `strategy_handler.py:856`) gets **zero** stop behavior. The DN dashboard advertises "Session TP/SL" that nothing enforces.
**Money impact:** Hedge drift / one-dead-leg can bleed unbounded while the user believes a stop is active.
**Fix:** Add a DN post-dispatch rail using `effective_sl_tp_pct("dn", state)` on net two-leg PnL; close via `close_delta_neutral_legs` (flatten **both** legs), not `close_all_positions`.

### SLTP-GROSS — SL/TP fires on gross-of-fees PnL — **High [VERIFIED]**
`live_session.py:247`: `session_pnl = realized + unrealized - funding_paid` — fees deliberately excluded (`:27` "Fees are a standalone metric, never in PnL"; `database.py:1043` realized is "gross of fees"). The rail reads this directly (`bot_runtime.py:2193`).
*Re-verified:* exact line confirmed; `realized` documented gross of fees.
**Symptom:** True net loss = `session_pnl − cumulative_fees`, so a −1% stop is actually hit at −1% **plus** fees. The stop trips **late** by the fee drag — worst on high-turnover grid/vol/DN.
**Money impact:** Real losses always exceed the configured stop by the accumulated fees. Small per trip, compounding over many cycles.
**Fix:** Subtract live cumulative fees in `session_pnl` (single change benefits every strategy), or add a fee-aware SL basis. If gross is intentional, the UI must say "gross of fees".

### GRID-DUAL-UNIT — user SL applied twice, two unit systems — **High [VERIFIED]**
The same `sl_pct` is used as a **price-move barrier** (`engine_runtime.py:599,610` → `grid_executor.py:363,372`) **and** as **% of margin** in the session rail. At leverage > 1 these are very different thresholds and the tighter price barrier wins.
**Symptom:** "I set SL 0.5% of margin but the grid stops out on a tiny 0.5% wick." Stops fire far earlier than intended; grid never gets room to mean-revert → repeated realized losses + fee churn.
**Fix:** Pick one semantic. Either pass `stop_loss=None` to the grid barrier and keep only the margin rail, or convert margin-% to a price fraction (`sl_price_frac = sl_margin_pct / leverage`) before building the barrier.

### GRID-TP-DEAD — executor take-profit never read — **High [VERIFIED]**
`engine_runtime.py:611` passes `take_profit=tp`, but `GridExecutor` never reads it (re-verified: **zero** `take_profit` references in `grid_executor.py`). TP is realized only via the per-level closing leg and the session rail.
**Fix:** Either implement a TP branch in `_stop_breached`, or stop passing `take_profit` into the grid barrier and document TP as session-level only.

### Secondary SL/TP findings
- **DGRID-SHADOW-KEYS — Med [VERIFIED]** — dgrid defaults define both `tp_pct`/`sl_pct` **and** `rgrid_*` (`strategy_registry.py:148,263`); the UI only writes `rgrid_*`. The `sl_pct`/`tp_pct` copies are dead but dangerous: any code reading `sl_pct` directly uses the stale default (e.g. the legacy preview `:1546`). *Fix:* drop them or mirror-write.
- **SLTP-MARGIN-BASIS — Med [SUSPECTED]** — `_resolve_margin` uses `notional_usd` as the denominator (`live_session.py:56`) while Nado leverage is account-level (`adapter/nado.py:15`). At >1x the "% of margin" stop is scaled by leverage. *Fix:* divide basis by effective leverage or relabel "% of notional".
- **Mid mode — [VERIFIED] clean** — Mid passes no barrier; its SL/TP is purely the margin rail, internally consistent (still subject to SLTP-GROSS).

---

## 2. MM / grid family

- **DGRID-BOOK-RACE — High [VERIFIED]** (`dynamic_grid.py:328-376`) — `_maybe_book_profit` fires a naked MARKET reduce-only **bypassing the executor**, while the executor's LIMIT_MAKER close legs rest against the same inventory. The two views diverge: controller inventory shows the booked reduction, executor still thinks it holds `filled_base`. Produces orphaned/rejected close legs, inventory-vs-venue drift, and pays taker fees that negate the maker edge. *Fix:* route booking through the executor (reduce tracked size + re-size close legs) or make booking and close-legs mutually exclusive.
- **DGRID-RECENTER — High [VERIFIED]** (`grid_executor.py:326-339`) — re-center sizes `fresh` levels at `total_amount_quote / max_open_orders` without subtracting notional already committed in `kept` (inventory-holding) legs. Repeated re-centers ratchet deployed notional above the risk-approved size. *Fix:* size fresh against `total_amount_quote − notional(kept)`.
- **GRID-MIN-NOTIONAL-INFLATE — Med [VERIFIED]** (`grid_executor.py:132` + `nado_client.py:2680`) — per-level size is bumped up to venue min-notional, so a small/many-level grid deploys more than `total_amount_quote` (and more than the risk gate approved). Round-trip stays balanced (no orphan), but exposure exceeds intent. *Fix:* cap level count so `total/levels ≥ min_notional`.
- **DGRID-TREND-BLEED — Med [VERIFIED]** (`variance_regime.py:76-121`) — a slow steady decline keeps the variance ratio < 1, so the **only** flip trigger is `trend_drift_pct` (0.30%). Below that, a long dgrid keeps buying the dip indefinitely (bounded only by the 30% exposure cap). This is the classic "re-buy into a losing trend" bleed. *Fix:* add a cumulative-drift / consecutive-down-candle confirm.
- **DGRID-NO-GATE — Med [VERIFIED]** (`dynamic_grid.py:216`) — dgrid disables breakout/trend gating; its only trend defense is the (weak) VR/drift flip + exposure cap. *Fix:* keep `pause_on_breakout=True`.
- **MM-SPREAD-FLOOR — Med [SUSPECTED]** (`market_making.py:106` vs `mm_strategy_design.md:71`) — manual spread has no fee floor; a sub-1.5bp user spread quotes a book that loses after fees. The auto-spread path floors at 1.5bp, the manual path does not. *Fix:* floor `spread_*_pct` at `spread_floor_half_pct` regardless of mode.
- **RGRID-GATE — High-if-confirmed [SUSPECTED]** (`reverse_grid.py:16` vs `engine_runtime.py:419`) — the rgrid docstring claims `regime_gate_enabled=0` in production, but the default is `1.0` and no rgrid override was found. If the gate is live, rgrid (no `adverse_trend`) pauses on **both** trends → sits out the very downtrends it exists to trade. **Action: confirm whether the production override exists.**
- **Doc-vs-code:** `market_making.py` is a fixed-spread inventory-gated quoter; the GLFT/Avellaneda math in `mm_strategy_design.md` is **not implemented**. The doc oversells what ships.
- **Clean:** grid/reverse-grid executors (partial-fill capture, external-cancel re-issue, fee passthrough, sign handling), `mm_quote_math`, controller bounds-rebuild, gate/exposure hysteresis, inventory repo.

---

## 3. Volume bot

- **VOL-MARGIN — High [VERIFIED]** (`engine_runtime.py:438`) — vol reads `cycle_notional_usd`/`notional_usd`, never the user's `session_margin_usd` (collected/validated at `strategy_handler.py:449`). User sets $500, bot trades $100. *Fix:* source notional from `session_margin_usd` first.
- **VOL-LOOP — High [VERIFIED]** (`volume_bot.py:73-96`) — after buy-TWAP → sell-TWAP, `phase="done"` and the controller idles forever; never re-cycles toward target volume and never calls `_set_stopped()`. The documented session loop is absent. *Fix:* implement the cycle-until-target loop + terminal signal.
- **VOL-DEAD-SL — Med [VERIFIED]** (`engine_runtime.py:528-539`) — `sl_pct` and `target_volume_usd` are never read by vol code; the SL the user sets does nothing at the controller level. (Vol IS covered by the session rail, but the controller passes no barriers.) *Fix:* wire or remove the knobs.
- **VOL-NO-CAP — Med [VERIFIED]** (`volume_bot.py:6-8`) — docstring claims a daily-volume cap "enforced upstream by the Risk Engine" that **does not exist** (no volume/wash cap in `engine/risk.py`). Today blast radius is small (one round-trip), but the moment VOL-LOOP is fixed there's no ceiling on wash-fee bleed. *Fix:* add a cumulative-volume / fee budget guard.
- **Clean:** sell-leg sizes off `buy_ex.filled_quote` (handles partial buy correctly); spot, leverage forced to 1; sequential phases avoid self-trade.
- **Doc drift:** `docs/volume_bot.md` references a `src/nadobro/strategies/volume_bot.py` and knobs that don't exist.

---

## 4. Copy trading (live path: `trading/copy_service.py`)

- **COPY-SIZE — High [VERIFIED]** (`copy_service.py:719-723`) — follower size = `margin_per_trade × leverage / leader_entry`; the leader's actual position size (available at `:758`) is never used. No proportional ratio. A leader's tiny probe and max-conviction position are copied at identical notional. *Fix:* scale by `leader_size × (allocation / leader_equity)`.
- **COPY-LEVERAGE — Med-High [VERIFIED]** (`copy_service.py:720`) — always opens at the user's max leverage; the leader's actual leverage is never captured. A leader at 2x is mirrored at 10x. *Fix:* `min(leader_leverage, max_leverage)`.
- **COPY-NO-SLIPPAGE — Med [VERIFIED]** (`copy_service.py:52,716,728`) — 30s poll interval + a flat 1.5% market slippage, no max-deviation gate; follower enters up to ~30s late at materially worse prices. (The dead `CopyController` *has* a slippage gate the live path lacks.) *Fix:* add a deviation gate; shorten poll for active mirrors.
- **COPY-VENUE-RECONCILE — Med [VERIFIED]** (`copy_service.py:628,741-760`) — decisions trust the `copy_positions` DB table; the follower's real on-venue position is never queried. A fill that succeeds on-chain but fails the DB insert (`:744`) leaves an untracked live position the bot never closes. *Fix:* reconcile against `get_all_positions()` + idempotency key.
- **COPY-DEDUP — Low-Med [SUSPECTED]** — no DB unique constraint on open `(mirror, product)`; safe within one serialized poller, unguarded if two pollers ever run. *Fix:* unique constraint / advisory lock.
- **Clean:** leader-close → follower reduce-only close + side-flip handling; privacy/ownership checks; `risk_factor`/budget → `margin_per_trade` mapping.
- **Note:** `engine/controllers/copy_trading.py` (`CopyController`) is registered but **not** in `ENGINE_MAPPED_STRATEGIES` and is never wired — dead code that diverges from the live path. The two tests referenced in the brief (`test_fill_sync_claiming`, `test_copy_trading_privacy`) do not exist.

---

## 5. Delta Neutral (economic, beyond DN-RAIL)

- **DN-PNL-FEES — High [VERIFIED]** (`pnl_card_builder.py:218-219`) — DN headline = `realized (gross) + funding`, fees never subtracted; `test_pnl_card_builder.py:204-234` asserts +$3.50 when true net is $3.30. DN fires 4 taker MARKET orders/cycle, yet funding-vs-fees is never compared anywhere — the funding-capture thesis is **never verified net-positive**. *Fix:* DN PnL = `realized + funding − fees`; warn when cumulative fees > funding.
- **DN-FUNDING-WINDOW — Low [SUSPECTED]** (`nado.py:728-731`) — funding rows with an unparseable timestamp are summed regardless of date → pre-run funding can leak into the run total (compounds DN-PNL-FEES). *Fix:* skip rows where `ts is None`.
- **Clean:** DN execution/leg-integrity — atomic two-leg open with rollback, one-dead-leg → close both, retried idempotent stops, inventory-verified residual sweep, base-matched hedge sizing. The naked-leg failure mode is properly guarded.

---

## 6. Engine / risk

- **FUNDING-SIGN — Med [VERIFIED]** — two contradictory funding sign conventions: `live_session.py:247` / `bot_runtime.py:1346` treat funding paid-positive (cost); `pnl_card_builder.py:125-152` treats it received-positive (added to PnL). The same run can report different funding economics across `/mm_status`, the share card, and the stop summary. *Fix:* one signed convention in one helper, called everywhere.
- **NO-LIQ-CHECK — Low [SUSPECTED]** — no engine-side liquidation-distance gate; relies on venue auto-liq + the 1.20 margin safety multiplier. Probably adequate for 1x DN / isolated grid, but no defense-in-depth. *Fix:* add a liquidation-distance gate.
- **Clean:** `engine/risk.py` gates, `orchestrator.py` orphan handling, `order_lifecycle.py` terminal-state safety, `engine/portfolio.py` (engine-side net = realized − fees + unrealized is **correct** — note the divergence from what the user sees), `quant/margin.py`.

---

## 7. Backtester readiness

- **BT-EMPTY — High capability gap [VERIFIED]** — the entire `backtester/` package is docstring-only stubs (`engine.py`, `executor_sim.py`, `candle_ingest.py`, `report.py` ≤5 lines each, all tagged "Phase 5"). No strategy can be simulated; "does this bleed money?" cannot be answered pre-flight.
- The only working sim is a bespoke per-test loop driving **MM only** (`tests/engine/test_gate_backtest.py`); `MockNadoAdapter` defaults fees to 0, models no funding accrual, no slippage, no candle ingestion.
- The repo CSV (`f14288_*_trades_*.csv`) is a **trade/fill export**, not OHLC — usable as a price/fee tape but must be resampled.

**What it takes to wire per-strategy money-bleed regressions** (controllers need no changes — they already run against any `NadoAdapterBase`):
1. `candle_ingest.py` — load/resample OHLC (or drive off `adapter.candles`, already implemented at `nado.py:677`).
2. `executor_sim.py` — candle-driven fills **with fees + funding accrual + slippage** (the costs the live system under-reports; a fees=0 backtest would falsely show DN/grid profitable — the exact A1/SLTP-GROSS trap).
3. `engine.py` — time loop: advance clock → `orchestrator.tick_controller` → sim fill → accrue funding.
4. `report.py` — equity curve, max drawdown, **net-of-fees PnL** per strategy.
5. Per-strategy regression: instantiate each of the 5 controllers against the same candle path, assert net-of-fees PnL ≥ threshold.

A starter test that encodes the SL/TP invariants (including the known-broken ones as `xfail`) ships in `tests/engine/test_sltp_invariants.py` (see the self-review workflow). It is the executable expression of this audit.

---

## 8. What is explicitly NOT a bug (anti-hallucination ledger)

To keep this report honest, the following were checked and found **correct**: `effective_sl_tp_pct` key resolution for rgrid/dgrid; dgrid tiered-profit-booking margin basis; Mid executor (no double-application); grid/rgrid BUY/SELL barrier sign handling; `desk.py` legacy SL/TP sign math; grid/reverse-grid executor core; orchestrator orphan teardown; order-lifecycle terminal-state monotonicity; engine-side portfolio net PnL; `margin.py` isolated-margin math; DN leg-integrity/rollback; vol sell-leg sizing; copy-trading privacy/ownership and budget mapping.
