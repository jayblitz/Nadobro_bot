"""Grid Trading controller — thin wrapper that spawns a single GridExecutor
across the configured price range and ticks it. Long-bias.

Implemented in Phase 4.
"""
from __future__ import annotations

from typing import Optional

from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.executors.grid_executor import GridExecutor, GridExecutorConfig
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.types import TradeType, TripleBarrierConfig, _dec


def build_grid_config(configs: dict, side: TradeType) -> GridExecutorConfig:
    return GridExecutorConfig(
        trading_pair=str(configs["trading_pair"]),
        side=side,
        start_price=_dec(configs["start_price"]),
        end_price=_dec(configs["end_price"]),
        limit_price=_dec(configs.get("limit_price", 0)),
        total_amount_quote=_dec(configs["total_amount_quote"]),
        min_spread_between_orders=_dec(configs.get("min_spread_between_orders", "0.005")),
        max_open_orders=int(configs.get("max_open_orders", 10)),
        max_orders_per_batch=int(configs.get("max_orders_per_batch", 10)),
        activation_bounds=(_dec(configs["activation_bounds"]) if configs.get("activation_bounds") is not None else None),
        triple_barrier_config=configs.get("triple_barrier_config"),
        leverage=int(configs.get("leverage", 1)),
        keep_position=bool(configs.get("keep_position", False)),
    )


class GridController(Controller):
    SIDE = TradeType.BUY
    EXECUTOR_CLS = GridExecutor

    def __init__(self, **kwargs: object) -> None:
        super().__init__(name=kwargs.pop("name", "grid_trading"), **kwargs)  # type: ignore[arg-type]
        self._executor_id: Optional[str] = None

    async def on_start(self) -> None:
        # Regime gate: never ARM a grid into a trending/breakout market —
        # the post-mortem failure was "reset re-armed just in time for the
        # next leg down". A paused start defers the spawn to a later tick.
        await self.evaluate_quote_gate(str(self.configs.get("trading_pair")))
        if self.gate_paused:
            return
        await self._spawn()

    async def _spawn(self) -> None:
        cfg = build_grid_config(self.configs, self.SIDE)
        ex = self.EXECUTOR_CLS(
            cfg, user_id=self.user_id, controller_id=self.id, adapter=self.adapter,
            inventory=self.inventory,
        )
        ok = await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=cfg.total_amount_quote,
                                position_size_quote=cfg.total_amount_quote)
        )
        if ok:
            self._executor_id = ex.id

    async def on_tick(self) -> None:
        pair = str(self.configs.get("trading_pair"))
        await self.evaluate_quote_gate(pair)
        active = self.my_executors()
        if not active and self._executor_id is None and not self.gate_paused:
            # Spawn was gate-deferred at on_start; the regime is now ranging.
            await self._spawn()
            active = self.my_executors()
        # Inventory cap: a long grid's only worsening side is its entries.
        try:
            mid = await self.adapter.mid_price(pair)
        except Exception:  # noqa: BLE001 - cap check degrades to gate-only
            mid = None
        exposure = self.exposure_allowed_sides(pair, mid) if mid else {"buy": True, "sell": True}
        entry_side_allowed = exposure["buy"] if self.SIDE is TradeType.BUY else exposure["sell"]
        for ex in active:
            # PAUSE / cap blocks NEW entry levels only; fills, close legs and
            # stops keep managing through the executor tick below.
            ex.suppress_new_entries = self.gate_paused or not entry_side_allowed
            await self.orchestrator.tick(ex.id)
