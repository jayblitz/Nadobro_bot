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
    # A short reverse grid exists to trade DOWNtrends — only an UPtrend is
    # adverse. Previously it inherited the default (pause on either trend),
    # so the gate kept it sidelined in exactly the regime it is built for.
    GATE_ADVERSE_TREND = "trending_up"

    def __init__(self, **kwargs: object) -> None:
        kwargs.setdefault("name", "reverse_grid")
        super().__init__(**kwargs)
