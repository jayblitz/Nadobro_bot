"""Dynamic Grid controller — re-centers strategy by volatility regime.

Each tick (only when no executor is active — no mid-flight swap): run the
``volatility_regime`` routine on recent candles and spawn the matching
executor — GridExecutor for RANGING / TRENDING_UP, ReverseGridExecutor for
TRENDING_DOWN.

Implemented in Phase 4.
"""
from __future__ import annotations

import inspect
from typing import List, Optional

from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.controllers.grid_trading import build_grid_config
from src.nadobro.engine.executors.grid_executor import GridExecutor
from src.nadobro.engine.executors.reverse_grid_executor import ReverseGridExecutor
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.routines import volatility_regime
from src.nadobro.engine.types import TradeType


class DynamicGridController(Controller):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(name="dynamic_grid", **kwargs)  # type: ignore[arg-type]
        self.trading_pair = str(self.cfg("trading_pair"))
        self.last_regime: Optional[str] = None

    async def on_start(self) -> None:
        return None

    async def _candles(self) -> List[dict]:
        provider = self.cfg("candle_provider")
        if provider is None:
            return []
        result = provider(self.trading_pair)  # type: ignore[operator]
        if inspect.isawaitable(result):
            result = await result
        return list(result or [])

    async def on_tick(self) -> None:
        active = self.my_executors(active_only=True)
        if active:
            for ex in active:
                await self.orchestrator.tick(ex.id)
            return  # no mid-flight swap

        candles = await self._candles()
        if not candles:
            return
        regime_info = await volatility_regime.run(self.trading_pair, candles)
        regime = str(regime_info["regime"])
        self.last_regime = regime

        side: TradeType
        cls: type[GridExecutor]
        if regime == volatility_regime.TRENDING_DOWN:
            side, cls = TradeType.SELL, ReverseGridExecutor
        else:  # RANGING or TRENDING_UP -> long grid
            side, cls = TradeType.BUY, GridExecutor
        cfg = build_grid_config(self.configs, side)
        ex = cls(cfg, user_id=self.user_id, controller_id=self.id, adapter=self.adapter,
                 inventory=self.inventory)
        await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=cfg.total_amount_quote,
                                position_size_quote=cfg.total_amount_quote)
        )
