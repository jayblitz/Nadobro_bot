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

