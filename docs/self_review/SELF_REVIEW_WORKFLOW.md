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
- [ ] **DN-RAIL** (Critical) — Delta Neutral has a session SL/TP rail that fires on net two-leg PnL. *Today: no rail; user stops ignored.* `bot_runtime.py:2654,2668` (dn absent). Add integration test on fix.
- [ ] **SLTP-GROSS** (High) — the SL/TP rail measures PnL **net of fees**. *Today: `session_pnl = realized + unrealized - funding_paid`, fees excluded.* `live_session.py:247`.
- [ ] **GRID-DUAL-UNIT** (High) — a user's SL is applied with ONE unit semantic, not both a price-move barrier and a % -of-margin rail. `engine_runtime.py:610` + `grid_executor.py:372`. `[test]` xfail `test_grid_barrier_does_not_double_apply_margin_pct_stop`.
- [ ] **GRID-TP-DEAD** (High) — if `take_profit` is passed to the grid barrier it is actually enforced (or stop passing it). *Today: `grid_executor.py` never reads `take_profit`.*
- [ ] **DGRID-SHADOW-KEYS** (Med) — dgrid defaults don't carry dead `sl_pct`/`tp_pct` copies that shadow the live `rgrid_*` values. `strategy_registry.py:148,263`.
- [ ] **SLTP-MARGIN-BASIS** (Med) — "% of margin" is measured against true posted margin (notional/leverage), or the UI says "% of notional". `live_session.py:56`.
- [x] **SLTP-KEYS** — rgrid/dgrid resolve SL/TP from the keys the UI writes. *Fixed.* `[test]` green `test_user_sltp_is_resolved_...`.

### Volume bot
- [ ] **VOL-MARGIN** (High) — vol sizes the run from the user's `session_margin_usd`. `engine_runtime.py:438`. `[test]` xfail `test_vol_uses_user_session_margin`.
- [ ] **VOL-LOOP** (High) — vol cycles until target volume, then marks itself stopped. *Today: one round-trip then idles forever.* `volume_bot.py:73-96`.
- [ ] **VOL-DEAD-SL** (Med) — the vol SL the user sets reaches the controller (or is removed from the UI). `engine_runtime.py:528-539`. `[test]` xfail `test_vol_config_carries_user_stop_loss`.
- [ ] **VOL-NO-CAP** (Med) — a cumulative-volume / fee budget guard exists before the loop is enabled. `volume_bot.py:6-8` (docstring claims a cap that doesn't exist).

### Copy trading
- [ ] **COPY-SIZE** (High) — follower size scales with the leader's size (proportional ratio), not a fixed notional. `copy_service.py:719-723`.
- [ ] **COPY-LEVERAGE** (Med-High) — follower uses `min(leader_leverage, max_leverage)`, not always the max. `copy_service.py:720`.
- [ ] **COPY-NO-SLIPPAGE** (Med) — a max-deviation gate skips/queues entries too far from `leader_entry`; consider a shorter poll. `copy_service.py:52,716`.
- [ ] **COPY-VENUE-RECONCILE** (Med) — open/close decisions reconcile against the follower's real on-venue position + an idempotency key. `copy_service.py:628,741`.
- [ ] **COPY-DEDUP** (Low-Med) — a DB unique constraint / lock prevents double-open per (mirror, product). 

### MM / grid family
- [ ] **DGRID-BOOK-RACE** (High) — profit-booking is routed through the executor (or mutually exclusive with its close legs), never a naked MARKET that races them. `dynamic_grid.py:328-376`.
- [ ] **DGRID-RECENTER** (High) — re-center sizes fresh levels against `total_amount_quote - notional(kept)`, so deployed notional never ratchets above the approved size. `grid_executor.py:326-339`.
- [ ] **GRID-MIN-NOTIONAL-INFLATE** (Med) — level count is capped so `total/levels >= venue min-notional`, preventing silent exposure inflation. `grid_executor.py:132`.
- [ ] **DGRID-TREND-BLEED** (Med) — a slow steady decline triggers a trend flip (cumulative-drift / consecutive-down-candle), not just the 0.30% per-window threshold. `variance_regime.py:76-121`.
- [ ] **DGRID-NO-GATE** (Med) — dgrid keeps breakout gating on. `dynamic_grid.py:216`.
- [ ] **MM-SPREAD-FLOOR** (Med) — manual MM spread is floored at the fee-clearing half-spread, like the auto path. `market_making.py:106`.
- [ ] **RGRID-GATE** (verify) — confirm whether the production `regime_gate_enabled=0` override for rgrid exists; if not, rgrid is gated out of its own downtrends. `reverse_grid.py:16` vs `engine_runtime.py:419`.

### Delta Neutral (economic)
- [ ] **DN-PNL-FEES** (High) — DN headline PnL = `realized + funding - fees`; warn when cumulative fees > funding. `pnl_card_builder.py:218`. (Update `test_pnl_card_builder.py:204-234` — it currently locks the overstatement in.)
- [ ] **DN-FUNDING-WINDOW** (Low) — funding rows with unparseable timestamps are excluded from the run total. `nado.py:728-731`.

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
