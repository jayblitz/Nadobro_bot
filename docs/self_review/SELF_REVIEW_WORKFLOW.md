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
- [ ] **VOL-LOOP** (High) — vol cycles until target volume, then marks itself stopped. *Today: one round-trip then idles forever.* `volume_bot.py:73-96`.
- [ ] **VOL-DEAD-SL** (Med) — the vol SL the user sets reaches the controller (or is removed from the UI). `engine_runtime.py:528-539`. `[test]` xfail `test_vol_config_carries_user_stop_loss`.
- [ ] **VOL-NO-CAP** (Med) — a cumulative-volume / fee budget guard exists before the loop is enabled. `volume_bot.py:6-8` (docstring claims a cap that doesn't exist).

### Copy trading
- [x] **COPY-SIZE** (High) — *FIXED 2026-06-20:* mirror size scales with the leader's conviction (position notional as a fraction of the leader's largest position), capped by the user's per-trade budget — a probe is copied small, max-conviction copied full. `copy_service._compute_copy_sizing`. `[test]` `tests/services/test_copy_sizing.py`.
- [x] **COPY-LEVERAGE** (Med-High) — *FIXED 2026-06-20:* leverage mirrors the leader's, capped by the user's max + product max; falls back to `min(max, product_max)` when the venue doesn't report it. Same helper/tests.
- [ ] **COPY-NO-SLIPPAGE** (Med) — a max-deviation gate skips/queues entries too far from `leader_entry`; consider a shorter poll. `copy_service.py:52,716`.
- [ ] **COPY-VENUE-RECONCILE** (Med) — open/close decisions reconcile against the follower's real on-venue position + an idempotency key. `copy_service.py:628,741`.
- [ ] **COPY-DEDUP** (Low-Med) — a DB unique constraint / lock prevents double-open per (mirror, product). 

### MM / grid family
- [x] **DGRID-BOOK-RACE** (High) — *FIXED 2026-06-20:* profit-booking is routed through the executor's new `reduce_position` (records the fill in shared inventory + advances per-level close accounting + cancels fully-booked close legs), with a direct reduce-only MARKET fallback only when no executor reduce-path exists. `grid_executor.reduce_position`, `dynamic_grid._maybe_book_profit`. `[test]` `test_reduce_position_books_through_executor_and_advances_accounting`.
- [x] **DGRID-RECENTER** (High) — *NOT A BUG (false positive), verified 2026-06-20:* `recenter` already sizes fresh levels as `fresh_count = max_open − len(kept)` at `total/max_open`, so total committed notional stays bounded by `total_amount_quote`. Confirmed empirically (held+resting held at the 1000 budget across repeated re-centers). No change made.
- [ ] **GRID-MIN-NOTIONAL-INFLATE** (Med) — level count is capped so `total/levels >= venue min-notional`, preventing silent exposure inflation. `grid_executor.py:132`.
- [ ] **DGRID-TREND-BLEED** (Med) — a slow steady decline triggers a trend flip (cumulative-drift / consecutive-down-candle), not just the 0.30% per-window threshold. `variance_regime.py:76-121`.
- [ ] **DGRID-NO-GATE** (Med) — dgrid keeps breakout gating on. `dynamic_grid.py:216`.
- [ ] **MM-SPREAD-FLOOR** (Med) — manual MM spread is floored at the fee-clearing half-spread, like the auto path. `market_making.py:106`.
- [ ] **RGRID-GATE** (verify) — confirm whether the production `regime_gate_enabled=0` override for rgrid exists; if not, rgrid is gated out of its own downtrends. `reverse_grid.py:16` vs `engine_runtime.py:419`.

### Delta Neutral (economic)
- [x] **DN-CYCLES** (High) — *FIXED 2026-06-20:* DN cycle count + funding are restored from persisted progress on rebuild (`engine_runtime.py` injects `restore_cycles_completed`/`restore_funding_usd`, gated on `runs>0`; `delta_neutral.py` resumes the count and won't open a cycle past `total_cycles`). So a restart/worker-handoff no longer ignores the configured cycle count. Tested in `tests/engine/controllers/test_delta_neutral.py`.
- [x] **DN-CUSTOM-ASSETS** (High) — *FIXED 2026-06-20:* wrapped RWA spots (wQQQX/wSPYX) now pair with their perps so DN offers more than BTC/ETH (`product_catalog._dn_pair_candidates` + candidate fallback in `_build_dn_pair_catalog`). Tested in `tests/services/test_dn_pairing.py`.
- [ ] **DN-HOLD-CLOCK-ON-REBUILD** (Med, remaining) — the hold timer (`opened_at`) is still memory-only; on a rebuild mid-hold the controller re-opens a fresh cycle and restarts the clock rather than ADOPTING the open legs. A full fix needs `opened_at` persisted (schema field) + venue-position adoption on rebuild + integration testing.
- [x] **DN-PNL-FEES** (High) — *FIXED 2026-06-20:* DN headline PnL is now `realized + funding − fees` (was gross, overstating DN profit / hiding net losses). `pnl_card_builder.py`. `[test]` updated `test_delta_neutral_folds_funding_into_pnl` (+$3.30 net).
- [x] **DN-FUNDING-WINDOW** (Low) — *FIXED 2026-06-20:* funding rows with an unparseable timestamp are now excluded from the run total. `nado.py funding_since`. `[test]` `test_funding_since_excludes_undated_rows`.

### Engine / risk
- [ ] **FUNDING-SIGN** (Med) — one signed funding convention in one helper, used by live_session, the share card, and the stop summary. `live_session.py:247` vs `pnl_card_builder.py:125`.
- [ ] **NO-LIQ-CHECK** (Low) — consider an engine-side liquidation-distance gate as defense-in-depth. `engine/risk.py`.

### Backtester (no money-bleed proof exists today)
- [ ] **BT-EMPTY** (High capability gap) — implement the 5-piece harness so each strategy can be simulated. See `docs/audit/STRATEGY_SLTP_AUDIT_2026-06-20.md` §7 for the concrete plan: `candle_ingest` → cost-aware `executor_sim` (fees + funding + slippage) → `engine` time loop → net-of-fees `report` → per-strategy regression. The controllers need no changes.

---

## Anti-hallucination contract

This workflow exists because a *wrong* bug report is worse than a missed one — it
burns a fix cycle and erodes trust in the whole process. Therefore:

1. Auditors are read-only and must quote `file:line` for every claim.
2. Every finding is tagged `[VERIFIED]` or `[SUSPECTED]`; only `[VERIFIED]` items get a fix.
3. Top findings are re-checked by a second pass (the orchestrator) before they enter the report.
4. Each fixed bug leaves behind a guardrail test, so it can never silently regress.
