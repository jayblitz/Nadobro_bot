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

    def __init__(self, **kwargs: object) -> None:
        kwargs.setdefault("name", "reverse_grid")
        super().__init__(**kwargs)
