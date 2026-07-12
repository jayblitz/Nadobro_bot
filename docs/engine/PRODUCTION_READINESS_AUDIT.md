# Engine v2 — Production Readiness Audit

Scope: every place where **production** (non-test) code uses a mock / in-memory /
no-op / placeholder stand-in instead of real DB or real Nado venue I/O. Unit-test
doubles (`MockNadoAdapter`, in-memory repos as fixtures) are intentionally excluded —
those are correct and stay.

**Headline:** Phases 1–4 delivered a complete, unit-tested engine, but it is **not
wired into the live bot at all**. Nothing in production constructs the
`ExecutorOrchestrator`, the controllers, the `NadoAdapter`, or a `RiskEngine`; the
strategy dispatch no-ops; and the engine DB tables are created but never written.
The engine works in isolation (tests green) — it is not yet connected.

Legend — **Can fix here**: I can implement in code now. **Needs live env**: final
correctness requires your Postgres / Nado testnet + 1CT signer.

---

## A. CRITICAL — the engine is not connected to the live runtime

### A1. No production wiring of the engine
- **Files:** `strategy/strategy_runtime.py`, `strategy/bot_runtime.py`
- **State:** `_dispatch_strategy` → `strategy_runtime.dispatch_cycle` returns
  `{"action":"skipped","reason":"engine_v2_controller_managed"}`. **No** code path
  constructs `ExecutorOrchestrator`, instantiates any controller, or calls
  `spawn_controller` / `tick_controller`. Strategies do not execute.
- **Real requires:** a runtime owner that, per running strategy session, builds the
  orchestrator (+ RiskEngine + NadoAdapter + DB inventory), spawns the matching
  controller, and ticks it on the existing cadence; start/stop wired to the bot's
  session lifecycle. **Can fix here** (code) / **Needs live env** (validate).

### A2. `NadoAdapter` is never constructed
- **File:** `engine/adapter/nado.py`
- **State:** class exists; **zero** production constructors. The engine has no live
  venue connection anywhere.
- **Real requires:** build it from the user's `NadoClient` + a real product-metadata
  map (`tick/lot/min_notional`, product_id per pair) in the runtime owner (A1).
  **Can fix here** / **Needs live env**.

---

## B. Venue adapter incomplete (`engine/adapter/nado.py`)

### B1. `order_status()` is a hard stub
- Line ~172: `raise AdapterError("order_status live polling not wired until Phase 6")`.
  Every executor polls `order_status` each tick → in production this raises. **Blocks
  all executors.** **Needs live env** to confirm the real status/poll shape; **can fix
  here** structurally (REST poll or fill-stream-driven state).

### B2. `cancel_order()` can never succeed
- `_product_for_digest()` raises, so `cancel_order` always hits the `except` and
  returns `False`. Cancels are silently no-ops. **Real requires** a digest→product
  registry (track product_id when placing). **Can fix here** / **Needs live env**.

### B3. Response/match/order-book parsing is guessed
- `_order_from_response`, `fill_stream`, `order_book` assume dict keys
  (`digest`/`filled_base`/`is_buy`/`bid`/`ask`/…) and `place_market_order` optimistically
  fabricates a fill if the venue didn't return one. Signatures were aligned against
  `NadoClient`, but the **response shapes were never verified** against a live call.
  **Needs live env** (capture a real response and map exactly).

---

## C. Persistence gaps (engine DB tables created but unused)

### C1. Inventory is in-memory only — `engine_position_hold` never written
- **File:** `engine/inventory.py` (`InventoryRepository` is a process dict).
- Executors route fills to this in-memory repo; nothing persists to
  `engine_position_hold`. Holds vanish on restart and are invisible cross-process.
- **Real requires:** a `DbInventoryRepository` (atomic upsert into
  `engine_position_hold`, read-back for `list_for_user`/`list_for_controller`).
  **Can fix here** / **Needs live env** (validate against your Postgres).

### C2. Executors never persisted to `engine_executors`
- The orchestrator keeps executors in a dict; no INSERT/UPDATE to `engine_executors`
  (id/state/close_type/metrics/timestamps). Restart loses all executor state.
- **Real requires:** orchestrator writes executor lifecycle rows. **Can fix here** /
  **Needs live env**.

### C3. Portfolio per-controller PnL reads empty in production
- **File:** `services/portfolio_history_worker.build_db_portfolio()` injects an
  **in-memory** `InventoryRepository()` placeholder, so `Portfolio.state().per_controller`
  is always empty in prod. Depends on C1. **Can fix here**.

### C4. Kill switch not persisted; RiskEngine not active in production
- **File:** `engine/risk.py` (only `InMemoryKillSwitchStore`). No DB-backed store, and
  **no production code builds a `RiskEngine`** or passes one to an orchestrator → risk
  gates are inert in prod and the kill switch does not survive restart (brief requires
  DB persistence). **Real requires** a DB-backed `KillSwitchStore` + a per-user
  `RiskState` provider reading real PnL/exposure. **Can fix here** / **Needs live env**.

---

## D. Market data has no real implementation (`engine/market_data.py`)

- **State:** 6-line docstring skeleton. The controllers/routines that need candles,
  funding, order book or mark price receive them via **injected providers**
  (`candle_provider`, `signal_source`, funding `rate_provider`, `AccountProvider`) that
  have **no production implementation** — only test fakes pass real-looking data.
- **Real requires:** a `MarketData` service backed by `NadoAdapter` (+ existing Nado
  candle/funding endpoints), cached, supplying the provider callables the controllers
  consume. **Can fix here** (code) / **Needs live env** (verify endpoints/shapes).

---

## E. Account provider is best-effort (`portfolio_history_worker.SnapshotAccountProvider`)

- Maps the legacy `portfolio_service.get_portfolio_snapshot` into engine `accounts`/marks
  using **guessed** attribute/key names (`positions`, `signed_amount`, `mark_price`, …).
  The account/total-value half of `/portfolio` works via the legacy path, but the engine
  normalization is unverified. **Can fix here** (read the real snapshot shape) / partly
  **Needs live env**.

---

## What is genuinely real today (not stubbed)

- `engine/journal.py` — real file-backed journaling.
- `engine/inventory.py` math, `engine/risk.py` gate logic, all executors/controllers/
  routines **logic** — real and unit-tested (just not fed real I/O yet).
- DB migrations `0007`/`0008` + `db.py` table DDL — real (pending validation on your DB).
- `quant/mm_quote_math.py` — real (extracted verbatim).
- The existing legacy `/portfolio` account/positions view — unchanged, still real.

---

## Recommended order to make it real

1. **C1 + C4 + C2** — DB-backed `InventoryRepository`, `KillSwitchStore`, executor
   persistence. (Validatable against the Postgres you provide — no venue needed.)
2. **B1–B3 + A2** — finish `NadoAdapter` (order_status, cancel registry, real response
   mapping) and build it from a real `NadoClient`. (Needs Nado testnet.)
3. **D + E** — `MarketData` service + real provider callables; verify `SnapshotAccountProvider`.
4. **A1** — runtime owner wiring `bot_runtime` ↔ orchestrator/controllers; start/stop/tick.
5. Re-run the Phase 3 testnet runbook end-to-end (real fills) + Phase 6 soak.

I can do (1) fully against the Postgres you point me at, plus the code for (2)–(4);
the live-venue and end-to-end validation in (2)/(5) is yours to run.
