"""Reverse Grid controller — short counterpart of GridController using the
ReverseGridExecutor.

Implemented in Phase 4.
"""
from __future__ import annotations

from src.nadobro.engine.controllers.grid_trading import GridController
from src.nadobro.engine.executors.reverse_grid_executor import ReverseGridExecutor
from src.nadobro.engine.types import TradeType


class ReverseGridController(GridController):
    SIDE = TradeType.SELL
    EXECUTOR_CLS = ReverseGridExecutor
    # A Reverse Grid is a TREND strategy — it wins in trends (up or down),
    # re-centers, and keeps quoting. The regime gate exists to keep a *ranging*
    # grid out of trends, so it is turned OFF for rgrid in production via
    # ``regime_gate_enabled=0`` (see engine_runtime.map_strategy_config). No
    # per-side trend gating here: rgrid quotes in every regime.

    def __init__(self, **kwargs: object) -> None:
        kwargs.setdefault("name", "reverse_grid")
        super().__init__(**kwargs)
