"""Position Executor — places an entry order and manages the resulting
position using the Triple Barrier method (take_profit, stop_loss, time_limit,
optional trailing_stop with activation_price + trailing_delta).

Internal states: OPENING -> ACTIVE_POSITION -> CLOSING -> TERMINATED. Barrier
evaluation order on each tick: time_limit, stop_loss, trailing_stop (if
armed), take_profit. The first barrier hit wins; the close order uses that
barrier's order type and ``close_type`` is set accordingly.

The closing order is placed directly through the adapter with the barrier's
order type (Phase 1 keeps PositionExecutor self-contained rather than nesting
an OrderExecutor; the order *type* per barrier is honored).

Implemented in Phase 1.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Optional, Tuple

from src.nadobro.engine.adapter.base import Fill, NadoAdapterBase, NadoOrder, OrderState
from src.nadobro.engine.executor_base import Executor
from src.nadobro.engine.executors.order_executor import OrderExecutorConfig
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.types import (
    CloseType,
    ExecutionStrategy,
    OrderType,
    PositionAction,
    TradeType,
    TripleBarrierConfig,
)


class PositionExecState(Enum):
    OPENING = "OPENING"
    ACTIVE_POSITION = "ACTIVE_POSITION"
    CLOSING = "CLOSING"
    TERMINATED = "TERMINATED"


@dataclass
class PositionExecutorConfig:
    order_config: OrderExecutorConfig
    barriers: TripleBarrierConfig


_STRATEGY_TO_ORDER_TYPE = {
    ExecutionStrategy.LIMIT: OrderType.LIMIT,
    ExecutionStrategy.LIMIT_MAKER: OrderType.LIMIT_MAKER,
    ExecutionStrategy.MARKET: OrderType.MARKET,
    ExecutionStrategy.LIMIT_CHASER: OrderType.LIMIT_MAKER,
}


class PositionExecutor(Executor):
    def __init__(
        self,
        config: PositionExecutorConfig,
        *,
        user_id: int,
        controller_id: str,
        adapter: NadoAdapterBase,
        inventory: Optional[InventoryRepository] = None,
        keep_position: bool = False,
        executor_id: Optional[str] = None,
    ) -> None:
        super().__init__(
            user_id=user_id,
            controller_id=controller_id,
            trading_pair=config.order_config.trading_pair,
            adapter=adapter,
            inventory=inventory,
            keep_position=keep_position,
            executor_id=executor_id,
        )
        self.config = config
        self.barriers = config.barriers
        self.position_state = PositionExecState.OPENING
        self.entry_order: Optional[NadoOrder] = None
        self.close_order: Optional[NadoOrder] = None
        self.entry_price = Decimal(0)
        self.entry_base = Decimal(0)
        self.entry_quote = Decimal(0)
        self.exit_quote = Decimal(0)
        self.exit_base = Decimal(0)
        self.opened_at: Optional[float] = None
        self.max_price: Optional[Decimal] = None
        self.min_price: Optional[Decimal] = None
        self.trailing_armed = False
        self.trail_stop_level: Optional[Decimal] = None
        self._pending_close_type: Optional[CloseType] = None
        self._e_base = Decimal(0)
        self._e_quote = Decimal(0)
        self._e_fee = Decimal(0)
        self._x_base = Decimal(0)
        self._x_quote = Decimal(0)
        self._x_fee = Decimal(0)

    @property
    def is_long(self) -> bool:
        return self.config.order_config.side is TradeType.BUY

    @property
    def exit_side(self) -> TradeType:
        return self.config.order_config.side.opposite

    # -- fill ingestion ---------------------------------------------------
    def _ingest_entry(self, order: NadoOrder) -> None:
        delta_base = order.filled_base - self._e_base
        if delta_base <= 0:
            return
        delta_quote = order.filled_quote - self._e_quote
        delta_fee = order.fee_quote - self._e_fee
        price = delta_quote / delta_base
        self._record_fill(
            Fill(
                order.id,
                self.trading_pair,
                self.config.order_config.side,
                delta_base,
                price,
                delta_fee,
                time.time(),
            )
        )
        self._e_base, self._e_quote, self._e_fee = (
            order.filled_base,
            order.filled_quote,
            order.fee_quote,
        )
        self.entry_base = order.filled_base
        self.entry_quote = order.filled_quote

    def _ingest_exit(self, order: NadoOrder) -> None:
        delta_base = order.filled_base - self._x_base
        if delta_base <= 0:
            return
        delta_quote = order.filled_quote - self._x_quote
        delta_fee = order.fee_quote - self._x_fee
        price = delta_quote / delta_base
        self._record_fill(
            Fill(
                order.id,
                self.trading_pair,
                self.exit_side,
                delta_base,
                price,
                delta_fee,
                time.time(),
            )
        )
        self._x_base, self._x_quote, self._x_fee = (
            order.filled_base,
            order.filled_quote,
            order.fee_quote,
        )
        self.exit_base = order.filled_base
        self.exit_quote = order.filled_quote

    # -- lifecycle --------------------------------------------------------
    async def on_create(self) -> None:
        self._activate()
        oc = self.config.order_config
        order_type = (
            self.barriers.open_order_type
            if oc.execution_strategy
            in (ExecutionStrategy.LIMIT, ExecutionStrategy.LIMIT_MAKER)
            else _STRATEGY_TO_ORDER_TYPE[oc.execution_strategy]
        )
        order = await self._guard(
            lambda: self.adapter.place_order(
                self.trading_pair,
                oc.side,
                order_type,
                oc.amount_base,
                oc.price,
                oc.leverage,
                False,
            ),
            label="place_entry",
        )
        self.entry_order = order
        self._ingest_entry(order)
        if order.state is OrderState.FILLED:
            self._on_entry_filled()

    def _on_entry_filled(self) -> None:
        if self.entry_base <= 0:
            return
        self.entry_price = self.entry_quote / self.entry_base
        self.opened_at = time.time()
        self.max_price = self.entry_price
        self.min_price = self.entry_price
        self.position_state = PositionExecState.ACTIVE_POSITION

    async def on_tick(self) -> None:
        if self.is_terminated:
            return
        if self.position_state is PositionExecState.OPENING:
            if self.entry_order is None:
                return
            entry_ref = self.entry_order
            order = await self._guard(
                lambda: self.adapter.order_status(entry_ref.id),
                label="entry_status",
            )
            self.entry_order = order
            self._ingest_entry(order)
            if order.state is OrderState.FILLED:
                self._on_entry_filled()
            return
        if self.position_state is PositionExecState.ACTIVE_POSITION:
            price = await self._guard(
                lambda: self.adapter.mid_price(self.trading_pair), label="mid_price"
            )
            hit = self._evaluate_barriers(price)
            if hit is not None:
                close_type, order_type = hit
                self._pending_close_type = close_type
                await self._open_close(order_type)
            return
        if self.position_state is PositionExecState.CLOSING:
            await self._poll_close()

    # -- barriers ---------------------------------------------------------
    def _evaluate_barriers(
        self, price: Decimal
    ) -> Optional[Tuple[CloseType, OrderType]]:
        b = self.barriers
        e = self.entry_price
        if e <= 0:
            return None
        long = self.is_long
        now = time.time()
        if b.time_limit is not None and self.opened_at is not None:
            if (now - self.opened_at) >= b.time_limit:
                return CloseType.TIME_LIMIT, b.time_limit_order_type
        if b.stop_loss is not None:
            if long and price <= e * (Decimal(1) - b.stop_loss):
                return CloseType.STOP_LOSS, b.stop_loss_order_type
            if not long and price >= e * (Decimal(1) + b.stop_loss):
                return CloseType.STOP_LOSS, b.stop_loss_order_type
        if b.trailing_stop is not None:
            self._update_trailing(price)
            if self.trailing_armed and self.trail_stop_level is not None:
                if long and price <= self.trail_stop_level:
                    return CloseType.TRAILING_STOP, b.trailing_stop_order_type
                if not long and price >= self.trail_stop_level:
                    return CloseType.TRAILING_STOP, b.trailing_stop_order_type
        if b.take_profit is not None:
            if long and price >= e * (Decimal(1) + b.take_profit):
                return CloseType.TAKE_PROFIT, b.take_profit_order_type
            if not long and price <= e * (Decimal(1) - b.take_profit):
                return CloseType.TAKE_PROFIT, b.take_profit_order_type
        return None

    def _update_trailing(self, price: Decimal) -> None:
        ts = self.barriers.trailing_stop
        if ts is None:
            return
        e = self.entry_price
        if self.is_long:
            self.max_price = max(self.max_price or price, price)
            if not self.trailing_armed and (price - e) / e >= ts.activation_price:
                self.trailing_armed = True
            if self.trailing_armed:
                self.trail_stop_level = self.max_price * (Decimal(1) - ts.trailing_delta)
        else:
            self.min_price = min(self.min_price or price, price)
            if not self.trailing_armed and (e - price) / e >= ts.activation_price:
                self.trailing_armed = True
            if self.trailing_armed:
                self.trail_stop_level = self.min_price * (Decimal(1) + ts.trailing_delta)

    # -- closing ----------------------------------------------------------
    async def _open_close(self, order_type: OrderType) -> None:
        self.position_state = PositionExecState.CLOSING
        order = await self._guard(
            lambda: self.adapter.place_order(
                self.trading_pair,
                self.exit_side,
                order_type,
                self.entry_base,
                None if order_type is OrderType.MARKET else self.entry_price,
                self.config.order_config.leverage,
                True,
            ),
            label="place_close",
        )
        self.close_order = order
        self._ingest_exit(order)
        if order.state is OrderState.FILLED:
            self._finalize(self._pending_close_type or CloseType.COMPLETED)

    async def _poll_close(self) -> None:
        if self.close_order is None:
            return
        close_ref = self.close_order
        order = await self._guard(
            lambda: self.adapter.order_status(close_ref.id),
            label="close_status",
        )
        self.close_order = order
        self._ingest_exit(order)
        if order.state is OrderState.FILLED:
            self._finalize(self._pending_close_type or CloseType.COMPLETED)

    def _finalize(self, close_type: CloseType) -> None:
        gross = (
            (self.exit_quote - self.entry_quote)
            if self.is_long
            else (self.entry_quote - self.exit_quote)
        )
        self._net_pnl_quote = gross - self._fees_paid_quote
        self.position_state = PositionExecState.TERMINATED
        self._terminate(close_type)

    async def on_stop(self, close_type: CloseType = CloseType.EARLY_STOP) -> None:
        if self.is_terminated:
            return
        if self.position_state is PositionExecState.OPENING:
            entry = self.entry_order
            if entry is not None and entry.state is OrderState.OPEN:
                try:
                    await self._guard(
                        lambda: self.adapter.cancel_order(entry.id),
                        label="cancel_entry",
                    )
                except Exception:
                    pass
            self._terminate(close_type)
            return
        if self.position_state is PositionExecState.ACTIVE_POSITION:
            if self.keep_position:
                self.position_state = PositionExecState.TERMINATED
                self._terminate(close_type)
                return
            self._pending_close_type = close_type
            await self._open_close(OrderType.MARKET)
            if not self.is_terminated:
                self._terminate(close_type)
            return
        if self.position_state is PositionExecState.CLOSING:
            await self._poll_close()
            if not self.is_terminated:
                self._terminate(close_type)
            return
        self._terminate(close_type)
