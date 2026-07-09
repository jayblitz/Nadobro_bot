# Nadobro Strategy Self-Review Workflow

A repeatable loop that keeps every strategy (grid, rgrid, dgrid, mid, vol, copy,
dn) correct, honors the user's SL/TP, and stops it bleeding money. Built from the
2026-06-20 audit (`docs/audit/STRATEGY_SLTP_AUDIT_2026-06-20.md`).

The principle: **the audit becomes executable.** Every finding either becomes a
guardrail test (`tests/engine/test_sltp_invariants.py`) or a checklist item below.
A fix is only "done" when its guardrail flips from xfail to pass.

---

## The loop

```
        ┌──────────────────────────────────────────────────────────┐
        │ 1. CHANGE      edit a strategy / config / SL-TP path      │
        │ 2. AUDIT       fan out read-only agents (1 per strategy   │
        │                + sltp-tracer) — file:line evidence only   │
        │ 3. GUARD       run scripts/self_review.sh (mypy + the     │
        │                invariant tests + targeted strategy tests) │
        │ 4. TRIAGE      new [VERIFIED] finding? -> add an xfail     │
        │                guardrail referencing the audit ID         │
        │ 5. FIX         make the xfail XPASS, then delete the      │
        │                marker (strict=True enforces this)         │
        │ 6. BACKTEST    once the harness exists, assert net-of-fee │
        │                PnL >= threshold per strategy              │
        └──────────────────────────────────────────────────────────┘
```

### Run the agents (step 2)
The two reusable agent definitions live in `docs/self_review/agents/`. Copy them
to `.claude/agents/` (a protected dir this tooling can't write to) once:

```bash
mkdir -p .claude/agents && cp docs/self_review/agents/*.md .claude/agents/
```

Then, before merging any strategy change, launch them in parallel — one
`strategy-auditor` per touched strategy plus one `sltp-tracer` if the change
touches SL/TP, fees, session PnL, or `map_strategy_config`. They are READ-ONLY
and must cite `file:line` for every claim; they are instructed never to invent
bugs.

### Run the guardrails (step 3)
```bash
bash scripts/self_review.sh
```

---

## Strategy correctness checklist

Each item is an invariant the bot must satisfy. Status reflects the 2026-06-20
audit. `[test]` = covered by `tests/engine/test_sltp_invariants.py`. Fix the
Critical/High items first.

Every open item has a ready-to-paste `/goal` prompt with a deterministic exit
criterion in [`GOAL_LOOPS.md`](GOAL_LOOPS.md) — run them as goal loops instead
of babysitting fix sessions. Recurring drift detection (nightly attribution
reconciliation via `scripts/reconcile_attribution.py`, PR babysitting, the
alpha-brief prototype) lives in [`SCHEDULED_LOOPS.md`](SCHEDULED_LOOPS.md).

### SL/TP (priority)
- [x] **DN-RAIL** (Critical) — *FIXED 2026-06-20:* DN now gets a post-dispatch session SL/TP rail (`bot_runtime.py`, dn block) that flattens both legs via `close_delta_neutral_legs`. Tested in `tests/services/test_session_safety_rails.py`.
- [x] **SLTP-GROSS** (High) — *FIXED 2026-06-20:* the snapshot exposes `session_pnl_net`/`session_pnl_pct_net` (gross minus fees) and the rail judges the stop on the net basis; displayed gross PnL unchanged. `live_session.py`, `bot_runtime.py`. Tested.
- [x] **GRID-DUAL-UNIT** (High) — *FIXED 2026-06-20:* re-examined with the actual rail basis (margin = **notional**, not notional/leverage), so the price barrier and the rail are the same magnitude — there was no leverage-scaled double-stop. The real defect was the fill-blind, mid-referenced `limit_price` stop firing on a wick before the grid filled. Disabled it (`engine_runtime.py`, `grid_trading.py`, `dynamic_grid.py` set `limit_price=0`); SL is now the avg-entry barrier + the fee-aware rail. `[test]` `test_grid_does_not_set_fill_blind_limit_price_stop`.
- [x] **GRID-TP-DEAD** (High) — *FIXED 2026-06-20:* the executor now enforces `take_profit` (avg-entry referenced, mirrors the stop). `grid_executor._take_profit_breached`. `[test]` `test_take_profit_breach_triggers_take_profit`.
- [ ] **DGRID-SHADOW-KEYS** (Med) — dgrid defaults don't carry dead `sl_pct`/`tp_pct` copies that shadow the live `rgrid_*` values. `strategy_registry.py:148,263`.
- [ ] **SLTP-MARGIN-BASIS** (Med) — "% of margin" is measured against true posted margin (notional/leverage), or the UI says "% of notional". `live_session.py:56`.
- [x] **SLTP-KEYS** — rgrid/dgrid resolve SL/TP from the keys the UI writes. *Fixed.* `[test]` green `test_user_sltp_is_resolved_...`.

### Volume bot
- [x] **VOL-MARGIN** (High) — *FIXED 2026-06-20:* the vol branch of `map_strategy_config` now prefers `session_margin_usd` (then `cycle_notional_usd`/`notional_usd`). `[test]` green `test_vol_uses_user_session_margin`.
- [x] **VOL-LOOP** (High) — *FIXED 2026-06-20:* the controller now loops buy→sell until the user's `target_volume_usd` is met (single round-trip when unset), then signals completion; `run_engine_cycle` surfaces `result["done"]` and bot_runtime finalizes the session (no more idling "running"). `volume_bot.py`, `engine_runtime.py`, `bot_runtime.py`. `[test]` `tests/engine/controllers/test_volume_bot.py`.
- [x] **VOL-DEAD-SL** (Med) — *NOT DEAD (reframed) 2026-06-20:* the vol SL is enforced by the session SL/TP rail (`effective_sl_tp_pct('vol', state)`, now fee-aware), not the controller config. `[test]` `test_vol_stop_loss_is_enforced_by_the_session_rail`.
- [x] **VOL-NO-CAP** (Med) — *FIXED 2026-06-20:* a hard `max_cycles` ceiling bounds fee burn if the target is mis-set; docstring corrected (the claimed Risk-Engine cap never existed). `[test]` `test_max_cycles_caps_runaway_loop`.

### Copy trading
- [x] **COPY-SIZE** (High) — *FIXED 2026-06-20:* mirror size scales with the leader's conviction (position notional as a fraction of the leader's largest position), capped by the user's per-trade budget — a probe is copied small, max-conviction copied full. `copy_service._compute_copy_sizing`. `[test]` `tests/services/test_copy_sizing.py`.
- [x] **COPY-LEVERAGE** (Med-High) — *FIXED 2026-06-20:* leverage mirrors the leader's, capped by the user's max + product max; falls back to `min(max, product_max)` when the venue doesn't report it. Same helper/tests.
- [x] **COPY-NO-SLIPPAGE** (Med) — *FIXED 2026-06-20:* a max-deviation gate (`_entry_deviation_too_far`, default 1.5%) skips a late entry that's drifted too far from `leader_entry` (retried next poll). `copy_service.py`. `[test]` `test_entry_deviation_gate_*`.
- [x] **COPY-VENUE-RECONCILE** (Med) — *FIXED 2026-06-20:* before opening, the follower's REAL on-venue positions are read once and a product already held untracked is skipped (no duplicate/orphan stacking). Best-effort; degrades to DB-only if the client is unavailable. `copy_service._sync_mirror_positions`.
- [ ] **COPY-DEDUP** (Low-Med) — a DB unique constraint / lock prevents double-open per (mirror, product). 

### MM / grid family
- [x] **DGRID-BOOK-RACE** (High) — *FIXED 2026-06-20:* profit-booking is routed through the executor's new `reduce_position` (records the fill in shared inventory + advances per-level close accounting + cancels fully-booked close legs), with a direct reduce-only MARKET fallback only when no executor reduce-path exists. `grid_executor.reduce_position`, `dynamic_grid._maybe_book_profit`. `[test]` `test_reduce_position_books_through_executor_and_advances_accounting`.
- [x] **DGRID-RECENTER** (High) — *NOT A BUG (false positive), verified 2026-06-20:* `recenter` already sizes fresh levels as `fresh_count = max_open − len(kept)` at `total/max_open`, so total committed notional stays bounded by `total_amount_quote`. Confirmed empirically (held+resting held at the 1000 budget across repeated re-centers). No change made.
- [x] **GRID-MIN-NOTIONAL-INFLATE** (Med) — *FIXED 2026-06-20:* `run_engine_cycle` caps the grid-family level count so `total/levels >= venue min-notional` (only ever reduces levels), preventing the silent exposure inflation from venue min-notional bumps. `engine_runtime.py`.
- [ ] **DGRID-TREND-BLEED** (Med, tuning — not changed) — lowering the 0.30% drift default risks whipsaw; the exposure cap + fee-aware SL rail already bound a slow-decline bleed. Left to deliberate tuning rather than a blind default change. `variance_regime.py`.
- [x] **DGRID-NO-GATE** (Med) — *BY DESIGN (not changed), verified 2026-06-20:* dgrid's variance-ratio selector chooses GRID/RGRID for every regime incl. breakout, so it deliberately doesn't sit out (documented in `dynamic_grid.on_tick`). Changing it would break the intended flip behavior.
- [x] **MM-SPREAD-FLOOR** (Med) — *FIXED 2026-06-20:* the manual per-side spread is floored at `spread_floor_half_pct` (same as the auto path) so a sub-fee book can't be quoted. `market_making.py`. `[test]` `test_manual_spread_is_floored_at_fee_clearing_minimum`.
- [ ] **RGRID-GATE** (verify) — confirm whether the production `regime_gate_enabled=0` override for rgrid exists; if not, rgrid is gated out of its own downtrends. `reverse_grid.py:16` vs `engine_runtime.py:419`.

### Delta Neutral (economic)
- [x] **DN-CYCLES** (High) — *FIXED 2026-06-20:* DN cycle count + funding are restored from persisted progress on rebuild (`engine_runtime.py` injects `restore_cycles_completed`/`restore_funding_usd`, gated on `runs>0`; `delta_neutral.py` resumes the count and won't open a cycle past `total_cycles`). So a restart/worker-handoff no longer ignores the configured cycle count. Tested in `tests/engine/controllers/test_delta_neutral.py`.
- [x] **DN-CUSTOM-ASSETS** (High) — *FIXED 2026-06-20:* wrapped RWA spots (wQQQX/wSPYX) now pair with their perps so DN offers more than BTC/ETH (`product_catalog._dn_pair_candidates` + candidate fallback in `_build_dn_pair_catalog`). Tested in `tests/services/test_dn_pairing.py`.
- [ ] **DN-HOLD-CLOCK-ON-REBUILD** (Med, remaining) — the hold timer (`opened_at`) is still memory-only; on a rebuild mid-hold the controller re-opens a fresh cycle and restarts the clock rather than ADOPTING the open legs. A full fix needs `opened_at` persisted (schema field) + venue-position adoption on rebuild + integration testing.
- [x] **DN-PNL-FEES** (High) — *FIXED 2026-06-20:* DN headline PnL is now `realized + funding − fees` (was gross, overstating DN profit / hiding net losses). `pnl_card_builder.py`. `[test]` updated `test_delta_neutral_folds_funding_into_pnl` (+$3.30 net).
- [x] **DN-FUNDING-WINDOW** (Low) — *FIXED 2026-06-20:* funding rows with an unparseable timestamp are now excluded from the run total. `nado.py funding_since`. `[test]` `test_funding_since_excludes_undated_rows`.

### Engine / risk
- [ ] **FUNDING-SIGN** (Med, needs live-data verification — not changed) — the live-session path (`total_funding_paid`, paid-positive: `- funding_paid`) and the share card (`_net_funding_usd`, received-positive: `+ funding`) express funding differently but appear to net consistently (both add received funding). Confirming requires the live sign of the DB column vs the funding feed; flipping a sign blind would risk a real PnL-display bug, so left for data-verified change. `live_session.py` vs `pnl_card_builder.py`.
- [ ] **NO-LIQ-CHECK** (Low) — consider an engine-side liquidation-distance gate as defense-in-depth. `engine/risk.py`.

### Backtester (money-bleed proof harness)
- [x] **BT-EMPTY** (High capability gap) — *BUILT 2026-06-20:* the `backtester/` package is implemented — `candle_ingest` (OHLC / price-path / CSV resample), cost-aware `executor_sim` (taker/maker fees + funding accrual on perps only + slippage), `engine` time loop (no look-ahead), net-of-fees `report` (equity curve + max drawdown). `run_backtest(strategy, configs, candles, costs=...)` drives the SAME controllers the live engine builds. Tests in `tests/engine/backtester/` prove the harness is honest (fees flip a winner to a loser) and that grid/rgrid/vol/dn run end-to-end — incl. the DN thesis check (net positive only when funding > fees).

  Run a quick money-bleed check::

      from src.nadobro.engine.backtester import run_backtest, resample_trades_csv, SimCosts
      candles = resample_trades_csv("f14288_*_trades_*.csv", interval_s=3600, market="WTI")
      print(run_backtest("grid", grid_cfg, candles, costs=SimCosts()).summary())

---

## Anti-hallucination contract

This workflow exists because a *wrong* bug report is worse than a missed one — it
burns a fix cycle and erodes trust in the whole process. Therefore:

1. Auditors are read-only and must quote `file:line` for every claim.
2. Every finding is tagged `[VERIFIED]` or `[SUSPECTED]`; only `[VERIFIED]` items get a fix.
3. Top findings are re-checked by a second pass (the orchestrator) before they enter the report.
4. Each fixed bug leaves behind a guardrail test, so it can never silently regress.
