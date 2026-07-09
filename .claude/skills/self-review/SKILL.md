---
name: self-review
description: Run Nadobro's strategy self-review gate — SL/TP & config invariant tests, mypy, engine suite, and read-only audit agents. Use BEFORE merging any change that touches src/nadobro/engine (controllers/executors/risk), SL/TP handling, fees, session PnL, live_session.py, strategy_registry.py, or engine_runtime.map_strategy_config. Trigger on "self review", "run the guardrails", "audit this strategy change", or any strategy-behavior diff.
---

# Nadobro strategy self-review

The executable audit loop from `docs/self_review/SELF_REVIEW_WORKFLOW.md`: a strategy change is
only done when these gates pass. Guiding contract: a wrong bug report is worse than a missed one —
every claim needs `file:line` evidence, and only `[VERIFIED]` findings get fixes.

## Gates — quantitative pass criteria

1. **Invariants + type-check** (every run):

   ```bash
   PYTHON=.venv/bin/python bash scripts/self_review.sh
   ```

   PASS = exit 0. This runs `tests/engine/test_sltp_invariants.py` (known bugs are strict-xfail,
   so they pass until fixed) plus mypy on `src/nadobro/engine` (advisory — report issues, don't
   block on them).

2. **Engine suite** (any diff under `src/nadobro/engine/`):

   ```bash
   PYTHON=.venv/bin/python bash scripts/self_review.sh --full
   ```

   PASS = exit 0.

3. **Full suite** (before merge):

   ```bash
   .venv/bin/python -m pytest -q
   ```

   PASS = 0 failures. DB-backed tests skip without a local `NADO_TEST_DATABASE_URL` — that's
   expected locally; CI runs them against its postgres service.

## Audit fan-out — strategy changes

Before merging a change to strategy behavior, launch the read-only auditors in parallel via the
Agent tool (definitions in `.claude/agents/`, canonical copies in `docs/self_review/agents/`):

- one **strategy-auditor** per touched strategy (grid, rgrid, dgrid, mid, vol, copy, dn), told
  which strategy to audit
- plus one **sltp-tracer** if the diff touches SL/TP, fees, session PnL, `live_session.py`,
  `strategy_registry.py`, or `map_strategy_config`

Both are READ-ONLY and must cite `file:line` + a code quote for every claim, tagged
`[VERIFIED]` or `[SUSPECTED]`. Re-check top findings yourself before acting on them.

## Triage protocol — xfail guardrails

- New `[VERIFIED]` finding → add a **strict xfail** test to
  `tests/engine/test_sltp_invariants.py` referencing the audit ID (e.g. `GRID-TP-DEAD`).
  Do not fix silently.
- A fix makes its guardrail XPASS → strict mode fails the run → **delete the xfail marker in the
  same PR** as the fix. An XPASS failure is the cue, not a flake.
- `[SUSPECTED]` findings don't get fixes; record them in the checklist in
  `docs/self_review/SELF_REVIEW_WORKFLOW.md`.

## Money-bleed check — tuning changes (spread, cadence, sizing, fees)

The cost-aware backtest harness (`src/nadobro/engine/backtester/`) drives the SAME controllers as
the live engine, net of taker/maker fees + funding + slippage:

```python
from src.nadobro.engine.backtester import run_backtest, resample_trades_csv, SimCosts
candles = resample_trades_csv("<trades export>.csv", interval_s=3600, market="WTI")
print(run_backtest("grid", grid_cfg, candles, costs=SimCosts()).summary())
```

A tuning change should not flip net-of-fee PnL negative on the reference data.

## Done means

- `self_review.sh` exit 0 (and `--full` exit 0 for engine diffs)
- full pytest green (skips excepted)
- zero `[VERIFIED]` findings without either a fix or an xfail guardrail
- zero XPASS leftovers (every fixed bug's marker deleted)
