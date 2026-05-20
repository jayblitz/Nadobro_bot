# Nadobro Engine v2 — Architecture

## Mission

Provide Nadobro with a clean, robust, testable strategy engine modeled on
Hummingbot V2 / Condor design patterns, executing natively on Nado DEX via
the existing 1CT Linked Signer. No Hummingbot dependency. No MCP. No
foreign wallets.

## Layered architecture

Telegram handlers → Strategy Controllers → Executor Orchestrator → Executors
→ Nado Execution Adapter → 1CT Linked Signer → Nado DEX (Ink L2).

Cross-cutting services: Risk Engine, Inventory, Portfolio, Market Data,
Routines, Backtester, Journal.

## Modules

- `engine/types.py` — TradeType, OrderType, ExecutionStrategy, CloseType,
  ExecutorState, TripleBarrierConfig, TrailingStop, RiskState, ExchangePair.
- `engine/executor_base.py` — Abstract Executor: lifecycle, metrics,
  controller_id, retry policy, keep_position.
- `engine/orchestrator.py` — Owns executor lifecycles; spawn/stop/list;
  event bus; batched cancel; filtering by controller_id.
- `engine/risk.py` — Pre-tick + per-executor gates; kill switch.
- `engine/inventory.py` — Position Hold keyed by (user_id, trading_pair,
  controller_id) with realized/unrealized PnL.
- `engine/portfolio.py` — state(), history(), distribution(),
  accounts_distribution().
- `engine/market_data.py` — Nado order book, candles, funding, mark; cached.
- `engine/journal.py` — Per-session journal + cross-session learnings.
- `engine/executors/*` — Order, Position, Grid, ReverseGrid, DCA, TWAP.
- `engine/controllers/*` — market_making, grid_trading, reverse_grid,
  dynamic_grid, delta_neutral, volume_bot, copy_trading.
- `engine/routines/*` — technical_analysis, support_resistance_ema,
  funding_scan, volatility_regime, market_scanner.
- `engine/backtester/*` — event-driven simulator + reports.
- `engine/adapter/nado.py` — ONLY venue-aware module. Wraps connectors/nado
  and the 1CT signer.

## Security invariants

1. Only `engine/adapter/nado.py` may import `connectors/nado`.
2. No private key ever leaves the user's device.
3. Risk Engine kill switch gates `orchestrator.spawn_executor()`.
4. No MCP, no Gateway, no foreign wallets, no Hummingbot dependency.

## Migration

Pre-launch clean cutover. Legacy `src/nadobro/strategies/` and
`tests/strategies/` are deleted at the end of Phase 4. No feature flags.
