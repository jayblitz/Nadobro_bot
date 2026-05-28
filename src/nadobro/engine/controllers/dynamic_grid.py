"""Dynamic Grid controller — re-centers strategy by volatility regime.

Each tick (only when no executor is active — no mid-flight swap): run the
``volatility_regime`` routine on recent candles and spawn the matching
executor — GridExecutor for RANGING / TRENDING_UP, ReverseGridExecutor for
TRENDING_DOWN.

NO_ORDERS_AUDIT-FIX-R4: before spawning, this controller now rebuilds the
grid's ``start_price`` and ``end_price`` against the CURRENT mid and the
CHOSEN side. The configs emitted by ``engine_runtime.map_strategy_config``
include ``step_pct`` and ``levels_count`` for exactly this purpose. Without
this, a long grid would place levels on both sides of mid; the level above
mid would be rejected as a post-only crossing buy and the grid would
silently post fewer orders than configured.

Implemented in Phase 4.
"""
from __future__ import annotations

import inspect
from decimal import Decimal
from typing import List, Optional

from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.controllers.grid_trading import build_grid_config
from src.nadobro.engine.executors.grid_executor import GridExecutor
from src.nadobro.engine.executors.reverse_grid_executor import ReverseGridExecutor
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.routines import volatility_regime
from src.nadobro.engine.types import TradeType, _dec


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

    def _rebuild_bounds_for_side(self, side: TradeType, mid: Decimal) -> dict:
        """NO_ORDERS_AUDIT-FIX-R4: derive side-correct start/end + limit from
        the live mid + the step/levels knobs. Returns a shallow override
        dict layered onto ``self.configs`` for one ``build_grid_config``
        call. If ``step_pct``/``levels_count`` are absent we fall back to
        whatever ``self.configs`` already had (legacy callers).
        """
        if mid <= 0:
            return {}
        step = _dec(self.cfg("step_pct", 0) or 0)
        levels = int(self.cfg("levels_count", 0) or 0)
        if step <= 0 or levels < 1:
            return {}
        span = step * Decimal(max(levels - 1, 1))
        sl = _dec(self.cfg("sl_pct", 0) or 0)
        if side is TradeType.SELL:
            return {
                "start_price": mid,
                "end_price": mid * (Decimal(1) + span),
                "limit_price": (mid * (Decimal(1) + sl)) if sl > 0 else Decimal(0),
            }
        # BUY (long grid)
        return {
            "start_price": mid * (Decimal(1) - span),
            "end_price": mid,
            "limit_price": (mid * (Decimal(1) - sl)) if sl > 0 else Decimal(0),
        }

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

        # NO_ORDERS_AUDIT-FIX-R4: refresh the band against a fresh mid +
        # the side we just chose. Falls back to self.configs (no-op overlay)
        # for legacy callers that never set step_pct / levels_count.
        try:
            mid = await self.adapter.mid_price(self.trading_pair)
        except Exception:  # noqa: BLE001
            mid = Decimal(0)
        overlay = self._rebuild_bounds_for_side(side, _dec(mid))
        merged = {**self.configs, **overlay} if overlay else self.configs

        cfg = build_grid_config(merged, side)
        ex = cls(cfg, user_id=self.user_id, controller_id=self.id, adapter=self.adapter,
                 inventory=self.inventory)
        await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=cfg.total_amount_quote,
                                position_size_quote=cfg.total_amount_quote)
        )
