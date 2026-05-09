# Testnet Runtime Validation Checklist

Use this checklist when enabling `NADO_RUNTIME_MODE=multiprocess` on testnet.

## Preflight

- [ ] Set `NADO_RUNTIME_MODE=multiprocess`.
- [ ] Configure worker counts (example):
  - [ ] `NADO_MM_GRID_WORKERS=1`
  - [ ] `NADO_DN_WORKERS=1`
  - [ ] `NADO_VOL_WORKERS=1`
  - [ ] `NADO_BRO_WORKERS=1`
- [ ] Restart bot process and confirm startup logs include runtime supervisor start.

## DN Enter-Anyway

- [ ] Open DN preview and verify `Funding Entry` shows `ENTER ANYWAY`.
- [ ] Launch DN on testnet and confirm start message appears.
- [ ] Within one runtime tick, open status and verify:
  - [ ] `Worker Group` is present.
  - [ ] `Heartbeat` updates.
  - [ ] `Cycle` and cycle latency are visible.
  - [ ] `Funding Mode` and `Funding` are visible.
- [ ] Confirm DN can open hedge legs even when funding is unfavorable.

## Isolation & Reliability

- [ ] Run DN and MM simultaneously for at least 5 minutes.
- [ ] Confirm both continue cycling (`Cycles` count increases for each).
- [ ] Trigger repeated status callbacks and verify responsiveness.
- [ ] Confirm no continuous callback slow-path warnings attributable to strategy cycles.

## Recovery & Shutdown

- [ ] Restart bot with a running strategy and verify restore still schedules loops.
- [ ] Stop strategy via UI and ensure status flips to `NOT RUNNING`.
- [ ] Stop bot process and verify clean shutdown (workers + supervisor).

---

## $50 MM Tiny Budget (Phase 5 — Tread Fi parity soak)

Validates that the Phase 0–4 Tread Fi parity work lets a $50 wallet actually
place quotes on Nado, that the dashboard math matches the engine, and that the
bot survives a kill / resume cycle without orphan orders or double-quoting.
Run on `gateway.test.nado.xyz/v1` first; repeat on `gateway.prod.nado.xyz/v1`
mainnet with a real $50 wallet before the public announcement.

### Pre-soak preflight

- [ ] Funded testnet wallet shows ≥ $50 USDT0 collateral on `gateway.test.nado.xyz/v1`.
- [ ] `NADO_BUILDER_ID` and `NADO_BUILDER_FEE_RATE` env vars set; the start
      banner reports the builder fee at `1 bps` (Nadobro-locked, not Tread's 2 bps).
- [ ] `gateway.test.nado.xyz/v1/symbols` returns a `min_size` for BTC-PERP.
      Verify by running:
      ```
      curl -sX POST https://gateway.test.nado.xyz/v1/symbols \
          -H 'content-type: application/json' \
          -d '{}' | jq '.symbols[] | select(.symbol == "BTC-PERP") | .min_size'
      ```
- [ ] `archive.test.nado.xyz/v1/market_snapshots` returns ≥ 2 hourly buckets
      for BTC-PERP (POV engine needs 24 buckets to compute volume).

### Strategy setup ($50 collateral, DGRID, Tiny Budget Preset)

- [ ] From the Strategy Hub, select **⚡ Dynamic GRID**, pick **BTC**.
- [ ] Open **Configure → Core**.
- [ ] Set **Custom Margin** to `50` (USD).
- [ ] Tap **🎯 Tiny Budget Preset**.
  - [ ] Card response shows `$50 × Nx = $≥min_size notional ≥ pair minimum
        $min_size USDT0 — Cleared to quote.` (`N` is auto-derived to clear the
        venue floor).
  - [ ] `mm_leverage_override`, `min_order_notional_usd`,
        `mm_collateral_safety_factor` all written to settings (verify via
        `/status` or DB inspection).
- [ ] Open **Configure → Execution** (DGRID has no Execution tab — use Setup),
      tap **📈 POV Normal**.
- [ ] Tap **▶ Start**.

### Live observation (4-hour window)

Snapshot via `/mm_status` every ~15 minutes; record into the soak log.

- [ ] Pre-trade card shows a **Tread Breakdown** block with:
  - [ ] Required margin per quote (matches `(notional / leverage) × 1.10 ×
        participation_multiplier × bias_uplift`).
  - [ ] Builder fee `1.00 bps` (NOT 2 bps).
  - [ ] Maker rate signed (negative on BTC-PERP — typically `-3.00 bps`).
  - [ ] POV `Normal: 5%/min, ~X min duration`.
  - [ ] Max resting quotes ≥ 1.
- [ ] First `/mm_status` after start surfaces `Resume reconcile: tracked=0 →
      executed=0 @ ts=…` (a fresh session has nothing to reconcile but the
      marker still stamps because Phase 4 sees no persisted state and SKIPS;
      verify behavior matches `test_no_marker_when_no_persisted_state`).
- [ ] Throughout the 4 hours:
  - [ ] Zero `MM collateral budget too small` errors. (If one occurs, take the
        actionable guidance from the error, raise leverage, restart — count as
        an SLO miss.)
  - [ ] `Skipped this cycle: 0` on ≥ 95% of snapshots (low post-only retry
        exhaustion).
  - [ ] `Gateway retries last cycle: 0` on ≥ 99% of snapshots (transient 429
        handling not stressed).
  - [ ] `Spread` and `Reference` move with the market; `Open` quote count stays
        in `[1, max_open_orders]`.
  - [ ] `Session volume` is monotonically non-decreasing.

### Synthetic adverse move (margin-denominated SL behaviour)

Run after at least one successful fill cycle has been observed.

- [ ] Open the BTC-PERP UI on Nado testnet, take liquidity to push price
      adversely against the user's net inventory until cumulative position PnL
      ≈ `-(SL% / 100) × (notional / leverage)`.
- [ ] Within one cycle, `/mm_status` reports `STOPPED — grid_sl_hit` and the
      bot closes its remaining position.
- [ ] Verify the SL fired at MARGIN-denominated drawdown, not notional. Example
      for a $50 collateral × 5x leverage × 0.5% SL configuration: expect SL at
      `-($50 / 1) × 0.005 = -$0.25`, NOT `-($250 / 1) × 0.005 = -$1.25`. The
      Phase 0 fix means leveraged users hit SL at smaller absolute drawdowns
      than the pre-fix code did — confirm this is expected behaviour for the
      pair and the operator is aware.

### Kill / resume drill

Run after at least one fill cycle has been observed.

- [ ] Note the current `mm_session_notional_done_usd` and `Open quotes` count
      from `/mm_status`.
- [ ] Kill the bot process (`SIGTERM` or container stop).
- [ ] Wait 60 seconds.
- [ ] Restart the bot process; the strategy should auto-resume.
- [ ] First `/mm_status` after resume:
  - [ ] Shows `Resume reconcile: tracked=N → executed=M @ ts=…` where `N` is
        the pre-kill open-quote count.
  - [ ] `mm_session_notional_done_usd` is `≥` the pre-kill value (monotonic).
  - [ ] No duplicate prices in `Open` orders (no double-quoting).
  - [ ] No orders show up in `/mm_fills` that are also in the live open-orders
        list (no orphan tracker entries).

### Acceptance gate (testnet)

- [ ] `mm_session_notional_done_usd > $5,000` over the 4 hours **(empirical
      target, not a guarantee — record observed range)**.
- [ ] Zero `MM collateral budget too small` errors.
- [ ] ≥ 99% session uptime in healthy phases (paused-by-design while
      flatten-only is allowed; SL/TP triggered stops do NOT count against
      uptime).
- [ ] Margin-denominated SL fires correctly under the synthetic adverse move.
- [ ] Kill/resume drill passes all four sub-checks above.

### Mainnet repeat

- [ ] After testnet pass, repeat the entire `$50 MM Tiny Budget` section on
      `gateway.prod.nado.xyz/v1` with a real $50 wallet.
- [ ] Soak for at least 4 hours.
- [ ] Capture `/mm_status` snapshots and a final `/mm_fills 25` listing for the
      launch report.

### Post-soak deliverables

- [ ] Soak log committed to `docs/soak/phase5-<network>-<date>.md` containing:
  - [ ] Pair / leverage / preset / margin used.
  - [ ] Hourly `/mm_status` snapshots.
  - [ ] Final cumulative volume, fill rate, net fees, realized PnL.
  - [ ] Any skipped levels, gateway retries, or pause reasons observed.
- [ ] Launch positioning copy reviewed (`docs/launch/mm_v2_positioning.md`).
- [ ] CMO sign-off recorded in the soak log before public announcement.


