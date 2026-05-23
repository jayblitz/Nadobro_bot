"""Reverse Grid Executor — the short-side mirror of the Grid Executor.

A reverse grid opens ``SELL`` orders across the range and takes profit with
``BUY`` orders one step lower; the per-level state machine and aggregate
Triple Barrier are identical to :class:`GridExecutor`, which is already
direction-generic on ``config.side``. Useful for trending-down regimes
(see the volatility_regime routine in Phase 4).

Implemented in Phase 3.
"""
from __future__ import annotations

from typing import Optional

from src.nadobro.engine.adapter.base import NadoAdapterBase
from src.nadobro.engine.executors.grid_executor import GridExecutor, GridExecutorConfig
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.types import TradeType


class ReverseGridExecutor(GridExecutor):
    """Short grid. Construct with ``side=SELL`` (enforced here)."""

    def __init__(
        self,
        config: GridExecutorConfig,
        *,
        user_id: int,
        controller_id: str,
        adapter: NadoAdapterBase,
        inventory: Optional[InventoryRepository] = None,
        executor_id: Optional[str] = None,
    ) -> None:
        if config.side is not TradeType.SELL:
            raise ValueError("ReverseGridExecutor requires config.side == SELL")
        super().__init__(
            config,
            user_id=user_id,
            controller_id=controller_id,
            adapter=adapter,
            inventory=inventory,
            executor_id=executor_id,
        )
