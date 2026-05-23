"""Delta Neutral controller — a long leg + a short leg held to a target
hedge ratio. Each leg is a PositionExecutor with its own Triple Barrier. If
the hedge drifts beyond ``max_drift_pct`` both legs are closed (EARLY_STOP).

Implemented in Phase 4.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional

from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.executors.order_executor import OrderExecutorConfig
from src.nadobro.engine.executors.position_executor import PositionExecutor, PositionExecutorConfig
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.types import ExecutionStrategy, TradeType, TripleBarrierConfig, _dec


class DeltaNeutralController(Controller):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(name="delta_neutral", **kwargs)  # type: ignore[arg-type]
        self.long_pair = str(self.cfg("trading_pair_long"))
        self.short_pair = str(self.cfg("trading_pair_short"))
        self.hedge_ratio = _dec(self.cfg("hedge_ratio", "1"))
        self.leg_amount_quote = _dec(self.cfg("leg_amount_quote", "50"))
        self.max_drift_pct = _dec(self.cfg("max_drift_pct", "0.05"))
        self.long_id: Optional[str] = None
        self.short_id: Optional[str] = None
        self.hedge_broken = False

    def _barriers(self) -> TripleBarrierConfig:
        b = self.cfg("barriers")
        return b if isinstance(b, TripleBarrierConfig) else TripleBarrierConfig()

    async def _spawn_leg(self, pair: str, side: TradeType, amount_quote: Decimal) -> Optional[str]:
        mid = await self.adapter.mid_price(pair)
        amount_base = amount_quote / mid
        oc = OrderExecutorConfig(pair, side, amount_base, ExecutionStrategy.MARKET)
        ex = PositionExecutor(
            PositionExecutorConfig(order_config=oc, barriers=self._barriers()),
            user_id=self.user_id, controller_id=self.id, adapter=self.adapter,
            inventory=self.inventory,
        )
        ok = await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=amount_quote, position_size_quote=amount_quote)
        )
        return ex.id if ok else None

    async def on_start(self) -> None:
        # BUG-DN-2 fix: spawn both legs atomically. If the short leg fails to
        # spawn, roll back the long leg so we don't carry unhedged exposure.
        self.long_id = await self._spawn_leg(self.long_pair, TradeType.BUY, self.leg_amount_quote)
        if self.long_id is None:
            return
        self.short_id = await self._spawn_leg(
            self.short_pair, TradeType.SELL, self.leg_amount_quote * self.hedge_ratio
        )
        if self.short_id is None:
            # Roll back the long leg: stop it with EARLY_STOP so its
            # PositionExecutor's on_stop unwinds the just-opened position.
            from src.nadobro.engine.types import CloseType

            await self.orchestrator.stop(self.long_id, CloseType.EARLY_STOP)
            self.long_id = None
            raise RuntimeError("delta_neutral: short leg failed to spawn; long leg rolled back")

    async def _leg_value(self, pair: str) -> Decimal:
        if self.inventory is None:
            return Decimal(0)
        mid = await self.adapter.mid_price(pair)
        hold = self.inventory.get(self.user_id, pair, self.id)
        return abs(hold.net_amount_base) * mid

    async def on_tick(self) -> None:
        for ex in self.my_executors(active_only=True):
            await self.orchestrator.tick(ex.id)
        long_value = await self._leg_value(self.long_pair)
        short_value = await self._leg_value(self.short_pair)
        if short_value <= 0 or long_value <= 0:
            return
        # BUG-DN-1 fix: on_start sizes short_value = leg_amount_quote * hedge_ratio
        # (so hedge_ratio is the short:long notional ratio). The target the
        # drift check should compare against is therefore:
        #   target_long_for_current_short = short_value / hedge_ratio
        # The previous formula (target_long = hedge_ratio * short_value)
        # implied the OPPOSITE relationship and tripped the drift gate on the
        # first tick for any hedge_ratio != 1.
        if self.hedge_ratio <= 0:
            return
        target_long = short_value / self.hedge_ratio
        drift = abs(long_value - target_long) / max(long_value, target_long)
        if drift > self.max_drift_pct:
            self.hedge_broken = True
            from src.nadobro.engine.types import CloseType

            for ex in self.my_executors(active_only=True):
                await self.orchestrator.stop(ex.id, CloseType.EARLY_STOP)
