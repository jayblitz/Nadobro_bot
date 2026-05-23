"""Order Executor — places a single order using one of LIMIT, LIMIT_MAKER,
MARKET, or LIMIT_CHASER strategies.

LIMIT_CHASER posts ``distance`` off mid and re-posts when mid moves past
``refresh_threshold``, capped at ``max_refreshes`` (default 50) replacements.
Fills are routed into Inventory on terminate.

Implemented in Phase 1.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from src.nadobro.engine.adapter.base import Fill, NadoAdapterBase, NadoOrder, OrderState
from src.nadobro.engine.executor_base import Executor
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.types import (
    CloseType,
    ExecutionStrategy,
    LimitChaserConfig,
    OrderType,
    PositionAction,
    TradeType,
    _dec,
)


@dataclass
class OrderExecutorConfig:
    trading_pair: str
    side: TradeType
    amount_base: Decimal
    execution_strategy: ExecutionStrategy
    price: Optional[Decimal] = None
    chaser_config: Optional[LimitChaserConfig] = None
    leverage: int = 1
    position_action: PositionAction = PositionAction.OPEN
    connector_name: str = "nado"

    def __post_init__(self) -> None:
        self.amount_base = _dec(self.amount_base)
        if self.price is not None:
            self.price = _dec(self.price)
        if self.execution_strategy in (
            ExecutionStrategy.LIMIT,
            ExecutionStrategy.LIMIT_MAKER,
        ) and self.price is None:
            raise ValueError(f"{self.execution_strategy.value} requires a price")
        if self.execution_strategy is ExecutionStrategy.LIMIT_CHASER and self.chaser_config is None:
            raise ValueError("LIMIT_CHASER requires a chaser_config")


_STRATEGY_TO_ORDER_TYPE = {
    ExecutionStrategy.LIMIT: OrderType.LIMIT,
    ExecutionStrategy.LIMIT_MAKER: OrderType.LIMIT_MAKER,
    ExecutionStrategy.MARKET: OrderType.MARKET,
    ExecutionStrategy.LIMIT_CHASER: OrderType.LIMIT_MAKER,
}


class OrderExecutor(Executor):
    def __init__(
        self,
        config: OrderExecutorConfig,
        *,
        user_id: int,
        controller_id: str,
        adapter: NadoAdapterBase,
        inventory: Optional[InventoryRepository] = None,
        keep_position: bool = True,
        executor_id: Optional[str] = None,
    ) -> None:
        super().__init__(
            user_id=user_id,
            controller_id=controller_id,
            trading_pair=config.trading_pair,
            adapter=adapter,
            inventory=inventory,
            keep_position=keep_position,
            executor_id=executor_id,
        )
        self.config = config
        self.order: Optional[NadoOrder] = None
        self.last_fill: Optional[Fill] = None
        self.refreshes = 0
        self.orders_placed = 0
        self._placement_mid: Optional[Decimal] = None
        self._recorded_base = Decimal(0)
        self._recorded_quote = Decimal(0)
        self._recorded_fee = Decimal(0)

    # -- placement helpers ------------------------------------------------
    async def _chaser_price(self) -> Decimal:
        mid = await self._guard(
            lambda: self.adapter.mid_price(self.trading_pair), label="mid_price"
        )
        self._placement_mid = mid
        dist = self.config.chaser_config.distance  # type: ignore[union-attr]
        if self.config.side is TradeType.BUY:
            return mid * (Decimal(1) - dist)
        return mid * (Decimal(1) + dist)

    async def _place(self, order_type: OrderType, price: Optional[Decimal]) -> None:
        order = await self._guard(
            lambda: self.adapter.place_order(
                self.trading_pair,
                self.config.side,
                order_type,
                self.config.amount_base,
                price,
                self.config.leverage,
                self.config.position_action is PositionAction.CLOSE,
            ),
            label="place_order",
        )
        self.order = order
        self.orders_placed += 1
        self._ingest(order)

    def _ingest(self, order: NadoOrder) -> None:
        """Record any newly-filled quantity since the last poll."""
        delta_base = order.filled_base - self._recorded_base
        if delta_base > 0:
            delta_quote = order.filled_quote - self._recorded_quote
            delta_fee = order.fee_quote - self._recorded_fee
            price = (delta_quote / delta_base) if delta_base else Decimal(0)
            fill = Fill(
                order_id=order.id,
                trading_pair=self.trading_pair,
                side=self.config.side,
                amount_base=delta_base,
                price=price,
                fee_quote=delta_fee,
                timestamp=time.time(),
            )
            self._record_fill(fill)
            self.last_fill = fill
            self._recorded_base = order.filled_base
            self._recorded_quote = order.filled_quote
            self._recorded_fee = order.fee_quote

    # -- lifecycle --------------------------------------------------------
    async def on_create(self) -> None:
        self._activate()
        strat = self.config.execution_strategy
        if strat is ExecutionStrategy.MARKET:
            await self._place(OrderType.MARKET, None)
        elif strat is ExecutionStrategy.LIMIT_CHASER:
            price = await self._chaser_price()
            await self._place(OrderType.LIMIT_MAKER, price)
        else:
            await self._place(_STRATEGY_TO_ORDER_TYPE[strat], self.config.price)
        await self._check_complete()

    async def on_tick(self) -> None:
        if self.is_terminated or self.order is None:
            return
        order_ref = self.order
        order = await self._guard(
            lambda: self.adapter.order_status(order_ref.id), label="order_status"
        )
        self.order = order
        self._ingest(order)
        if await self._check_complete():
            return
        if self.config.execution_strategy is ExecutionStrategy.LIMIT_CHASER:
            await self._maybe_refresh()

    async def _check_complete(self) -> bool:
        if self.order is not None and self.order.state is OrderState.FILLED:
            self._terminate(CloseType.COMPLETED)
            return True
        if self.order is not None and self.order.state in (
            OrderState.CANCELLED,
            OrderState.REJECTED,
        ) and self.config.execution_strategy is not ExecutionStrategy.LIMIT_CHASER:
            self._terminate(CloseType.EARLY_STOP)
            return True
        return False

    async def _maybe_refresh(self) -> None:
        cfg = self.config.chaser_config
        assert cfg is not None
        if self.refreshes >= cfg.max_refreshes or self.order is None:
            return
        order_ref = self.order
        mid = await self._guard(
            lambda: self.adapter.mid_price(self.trading_pair), label="mid_price"
        )
        if self._placement_mid is None or self._placement_mid == 0:
            self._placement_mid = mid
            return
        move = abs(mid - self._placement_mid) / self._placement_mid
        if move > cfg.refresh_threshold:
            await self._guard(
                lambda: self.adapter.cancel_order(order_ref.id), label="cancel_order"
            )
            price = await self._chaser_price()
            await self._place(OrderType.LIMIT_MAKER, price)
            self.refreshes += 1

    async def on_stop(self, close_type: CloseType = CloseType.EARLY_STOP) -> None:
        # BUG-CC-1 fix: cancel-on-stop must CONFIRM the venue side actually
        # removed the order; previously any exception was swallowed and we
        # terminated with the order still live. Now: attempt cancel, then
        # poll order_status once. If the order is gone or terminal, ingest
        # any final fills and terminate. If it's still open, we cannot
        # cleanly terminate without leaking; surface FAILED so the
        # controller's stop loop / operator can intervene.
        order = self.order
        if order is None or order.state in (
            OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED,
        ):
            self._terminate(close_type)
            return

        try:
            await self._guard(
                lambda: self.adapter.cancel_order(order.id),
                label="cancel_order_stop",
            )
        except Exception:  # noqa: BLE001 - venue cancel failure handled by status probe
            pass

        # Verify by polling status. Capture any final fills, then check.
        try:
            refreshed = await self._guard(
                lambda: self.adapter.order_status(order.id),
                label="cancel_confirm_status",
            )
            self.order = refreshed
            self._ingest(refreshed)
        except Exception:  # noqa: BLE001
            # Lost network during confirmation. The order's state on the
            # venue is unknown; mark FAILED so a human / controller stop
            # loop reconciles it.
            self._terminate(CloseType.FAILED)
            return

        if self.order is None or self.order.state in (
            OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED,
        ):
            self._terminate(close_type)
            return

        # Order still resting after a cancel attempt + status confirm. This
        # is the leak vector — surface FAILED instead of silently terminating.
        self._terminate(CloseType.FAILED)
