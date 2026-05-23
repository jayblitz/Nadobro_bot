# Phase 3 — Testnet Sanity Runbook

Operator-run 24h sanity soak for the four Phase 3 executors (Grid, Reverse
Grid, DCA, TWAP). Goal: each executor runs against **Nado testnet** with small
capital, takes real fills via the 1CT Linked Signer, and terminates with a
defined `close_type` and **no unhandled exceptions**.

> Prerequisites: testnet wallet linked via the 1CT Linked Signer, a funded
> testnet balance, `DATABASE_URL` pointing at a test Postgres, and the bot
> running on `feat/engine-v2` with Phase 0–3 merged. Use Python 3.11
> (`.venv/bin/python`). The system default `python3` (3.9) cannot run the app.

## 0. Setup

```bash
export NADO_NETWORK=testnet
export NADO_PORTFOLIO_HISTORY=true
# small capital caps for the soak
export ENGINE_SOAK_TOTAL_QUOTE=50      # USDC-equivalent per executor
```

Pick one liquid testnet pair (e.g. `KBTC-USDC`). Record the start mid price.

## 1. Per-executor checklist

For each of GridExecutor, ReverseGridExecutor, DCAExecutor, TWAPExecutor:

| Step | What to verify |
|------|----------------|
| Spawn | Executor reaches `ACTIVE`; orders appear on the testnet book. |
| Fills | At least one real fill is ingested; Inventory `engine_position_hold` row updates; fees recorded. |
| Barriers | Force a barrier (move size/price config so a TP or limit_price/SL triggers) and confirm the matching `close_type`. |
| activation_bounds (grid) | Move mid; confirm far levels cancel and re-place near mid. |
| Missed slice (TWAP MAKER) | Confirm an unfilled slice is cancelled and rolled forward (`lost_slices` increments), not retried. |
| Termination | Executor ends in `TERMINATED` with a defined `close_type` (never stuck `ACTIVE`). |
| Logs | No unhandled exceptions / tracebacks in the journal or process log. |

### Suggested minimal configs (small capital)

- **Grid**: `start_price`/`end_price` ±1% of mid, `total_amount_quote=50`,
  `min_spread_between_orders=0.002`, `max_open_orders=4`,
  `limit_price` 2% below mid, `keep_position=false`.
- **Reverse Grid**: same band, `side=SELL`, `limit_price` 2% above mid.
- **DCA**: `amounts_quote=[25,25]`, `prices=[mid*0.999, mid*0.99]`,
  `take_profit=0.004`, `stop_loss=0.02`, `mode=MAKER`.
- **TWAP**: `total_amount_quote=50`, `total_duration=3600`,
  `order_interval=300`, `mode=MAKER` (then repeat with `TAKER`).

## 2. 24h soak

Leave the four executors (sequentially or in parallel under distinct
`controller_id`s) running for 24h on testnet. Sample the journal hourly.

## 3. Exit report

Confirm and report back to the engineer:

- [ ] All four executors terminated with a defined `close_type`.
- [ ] No unhandled exceptions in logs/journals over 24h.
- [ ] `engine_position_hold` / `engine_executors` rows reconcile with testnet
      fills (no orphaned/dangling state).
- [ ] Portfolio history rows were sampled (`engine_portfolio_history`).

When all four boxes are checked, reply **"Phase 3 soak clean — Phase 4 go"**.
If any executor hangs, errors, or terminates with `FAILED`, capture the
journal + the executor `id` and report before proceeding.
