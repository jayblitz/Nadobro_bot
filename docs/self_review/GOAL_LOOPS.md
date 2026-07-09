# Goal loops for the self-review backlog

Ready-to-paste `/goal` prompts. The xfail-guardrail protocol already gives every work item a
deterministic exit criterion (a named test node going green, `self_review.sh` exit 0), which is
exactly what goal-based loops need — so instead of babysitting a session, paste a goal and let the
evaluator hold Claude to the finish line.

State as of 2026-07-08: `tests/engine/test_sltp_invariants.py` has **zero live xfail markers**
(14/14 green — every audited bug fixed, markers deleted). The open checklist items in
`SELF_REVIEW_WORKFLOW.md` have **no guardrails yet**, so each goal below starts by writing one.

## Templates

**New `[VERIFIED]` bug** (from a strategy-auditor / sltp-tracer report):

```
/goal Fix <AUDIT-ID>: <one-line bug>. Protocol: (1) add a strict-xfail guardrail to
tests/engine/test_sltp_invariants.py referencing <AUDIT-ID> and confirm it XFAILS (bug reproduced);
(2) fix the code; (3) delete the xfail marker. Done = the new test passes un-marked,
PYTHON=.venv/bin/python bash scripts/self_review.sh exits 0, and .venv/bin/python -m pytest -q is
green. Stop after 6 tries.
```

**Tuning change** (spread / cadence / sizing / drift defaults):

```
/goal Evaluate <change> with the backtester (src/nadobro/engine/backtester, reference data
f14288_default_inkMainnet_trades_*.csv). Done = a sweep table of net-of-fee PnL + max drawdown for
current vs proposed values on trending AND ranging slices; change the default only if net PnL
improves on both, else report-only. Stop after 4 tries.
```

## Ready-to-paste goals for the open checklist items

Ranked: quick wins first. Every goal implicitly ends with: tick the item's checkbox in
`docs/self_review/SELF_REVIEW_WORKFLOW.md`, self_review.sh exit 0, full suite green.

**1. RGRID-GATE (verify → close; the code already looks right)** —
`engine_runtime.map_strategy_config` forces `regime_gate_enabled=0` for rgrid (and grid without an
explicit setting) around `engine_runtime.py:969`, matching the comment in `reverse_grid.py`. The
item just needs the verification locked in:

```
/goal Close RGRID-GATE: add a guardrail test to tests/engine/test_sltp_invariants.py asserting
map_strategy_config disables the regime gate for rgrid by default and that an explicit user
regime_gate_enabled=1 re-arms it. Done = new test green, self_review.sh exit 0, full pytest green,
RGRID-GATE checkbox ticked with a dated note. Stop after 3 tries.
```

**2. DGRID-SHADOW-KEYS (Med)** — dgrid defaults must not carry dead `sl_pct`/`tp_pct` copies that
shadow the live `rgrid_*` values (`services/strategy_registry.py`):

```
/goal Close DGRID-SHADOW-KEYS: add a guardrail asserting dgrid defaults carry no sl_pct/tp_pct
shadows of the rgrid_* keys and effective_sl_tp_pct('dgrid', ...) always resolves the rgrid_* value
when both exist. If the shadow exists, follow the xfail protocol (xfail → fix strategy_registry →
delete marker). Done = guardrail green un-marked, self_review.sh exit 0, full pytest green,
checkbox ticked. Stop after 5 tries.
```

**3. SLTP-MARGIN-BASIS (Med)** — "% of margin" must be measured against true posted margin
(notional/leverage) or the UI must say "% of notional" (`live_session.py` ~line 60). NOTE: changing
the math changes when live stops fire; the behavior-preserving move is relabeling. Decide which
before running — the goal below takes the safe default:

```
/goal Close SLTP-MARGIN-BASIS the behavior-preserving way: keep the rail math (basis = notional)
unchanged, relabel every UI/i18n string that says "% of margin" for session SL/TP to "% of
notional", and add a guardrail test pinning session_pnl_pct's denominator to the labeled basis.
Done = guardrail green, no rail-behavior test changes, self_review.sh exit 0, full pytest green,
checkbox ticked with a note that the true-margin-basis alternative was deliberately not taken.
Stop after 5 tries.
```

**4. NO-LIQ-CHECK (Low)** — defense-in-depth liquidation-distance gate in `engine/risk.py`:

```
/goal Close NO-LIQ-CHECK: add an engine-side liquidation-distance gate to engine/risk.py (block an
order that would put the position within a configurable buffer of its liquidation price; default
buffer conservative, off/permissive when leverage data is unavailable) + unit tests for long and
short sides. Done = new tests green, self_review.sh --full exit 0, full pytest green, checkbox
ticked. Stop after 5 tries.
```

**5. COPY-DEDUP (Low-Med)** — prevent double-open per (mirror, product). Needs the local test DB:
`docker compose -f compose.postgres-test.yaml up -d` first.

```
/goal Close COPY-DEDUP: inspect copy_service's open path and add a DB-level uniqueness guard (unique
constraint or advisory lock) preventing two concurrent opens for the same (mirror, product), with a
DB-backed test (NADO_TEST_DATABASE_URL=postgresql://nadobro:nadobro@127.0.0.1:5433/nadobro_test)
proving the second concurrent open is rejected/skipped cleanly. Done = test green locally,
self_review.sh exit 0, full pytest green, checkbox ticked. Stop after 5 tries.
```

**6. DN-HOLD-CLOCK-ON-REBUILD (Med, biggest)** — persist the DN hold timer and adopt open legs on
rebuild instead of opening a fresh cycle:

```
/goal Close DN-HOLD-CLOCK-ON-REBUILD: persist DN's opened_at (schema/bot_state field), and on
engine rebuild mid-hold make the controller ADOPT the existing venue legs and resume the hold clock
instead of re-opening. Add a controller test simulating stop → rebuild mid-hold that asserts no new
legs are opened and the original opened_at is honored. Done = new test green, existing DN tests
green, self_review.sh --full exit 0, full pytest green, checkbox ticked. Stop after 8 tries.
```

**7. DGRID-TREND-BLEED (tuning)** — use the tuning template above on the 0.30% drift default in
`engine/routines/variance_regime.py`; report-only unless the sweep wins on both regimes.

## Not goal-loop material

- **FUNDING-SIGN** — needs live venue data to confirm the DB column's sign convention; an offline
  evaluator can't verify it. Belongs to the nightly reconciliation loop (`SCHEDULED_LOOPS.md`,
  Loop A): the first triage session captures live funding rows, compares them against both display
  paths, then hands the confirmed sign back here as a normal fix goal.
