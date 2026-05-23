"""Grid Executor — multi-level grid with a per-level state machine
(NOT_ACTIVE -> OPEN_ORDER_PLACED -> OPEN_ORDER_FILLED -> CLOSE_ORDER_PLACED ->
COMPLETE) and an aggregate Triple Barrier (limit_price hard stop / stop_loss).

Direction-generic: ``side=BUY`` is a long grid (buy a level, sell one step
up); ``side=SELL`` is a short grid (sell a level, buy one step down) — the
ReverseGridExecutor is the short configuration.

``activation_bounds`` keeps resting open orders within a fraction of mid. On a
limit_price / stop_loss breach the executor cancels every resting order and,
unless ``keep_position`` is set, flattens the net held inventory, terminating
with ``close_type=STOP_LOSS``.

Implemented in Phase 3.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import List, Optional

from src.nadobro.engine.adapter.base import Fill, NadoAdapterBase, NadoOrder, OrderState
from src.nadobro.engine.executor_base import Executor
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.types import (
    CloseType,
    OrderType,
    TradeType,
    TripleBarrierConfig,
    _dec,
)


class GridLevelState(Enum):
    NOT_ACTIVE = "NOT_ACTIVE"
    OPEN_ORDER_PLACED = "OPEN_ORDER_PLACED"
    OPEN_ORDER_FILLED = "OPEN_ORDER_FILLED"
    CLOSE_ORDER_PLACED = "CLOSE_ORDER_PLACED"
    COMPLETE = "COMPLETE"


@dataclass
class GridLevel:
    index: int
    open_price: Decimal
    close_price: Decimal
    amount_base: Decimal
    state: GridLevelState = GridLevelState.NOT_ACTIVE
    open_order_id: Optional[str] = None
    close_order_id: Optional[str] = None
    _open_recorded: Decimal = Decimal(0)
    _close_recorded: Decimal = Decimal(0)
    filled_base: Decimal = Decimal(0)
    filled_quote: Decimal = Decimal(0)


@dataclass
class GridExecutorConfig:
    trading_pair: str
    side: TradeType
    start_price: Decimal
    end_price: Decimal
    limit_price: Decimal
    total_amount_quote: Decimal
    min_spread_between_orders: Decimal
    min_order_amount_quote: Decimal = Decimal(0)
    max_open_orders: int = 10
    max_orders_per_batch: int = 10
    order_frequency: float = 0.0
    activation_bounds: Optional[Decimal] = None
    safe_extra_spread: Decimal = Decimal(0)
    triple_barrier_config: Optional[TripleBarrierConfig] = None
    leverage: int = 1
    keep_position: bool = False
    connector_name: str = "nado"

    def __post_init__(self) -> None:
        for f in ("start_price", "end_price", "limit_price", "total_amount_quote",
                  "min_spread_between_orders", "min_order_amount_quote", "safe_extra_spread"):
            setattr(self, f, _dec(getattr(self, f)))
        if self.activation_bounds is not None:
            self.activation_bounds = _dec(self.activation_bounds)


def generate_grid_levels(cfg: GridExecutorConfig) -> List[GridLevel]:
    lo, hi = sorted([cfg.start_price, cfg.end_price])
    spread = cfg.min_spread_between_orders
    if spread > 0 and hi > 0:
        max_by_spread = int((hi - lo) / (hi * spread))
    else:
        max_by_spread = cfg.max_open_orders
    n = max(1, min(cfg.max_open_orders, max_by_spread + 1))
    if n == 1:
        prices = [lo]
    else:
        step = (hi - lo) / (n - 1)
        prices = [lo + step * i for i in range(n)]
    cap = cfg.total_amount_quote / n
    step_frac = cfg.min_spread_between_orders + cfg.safe_extra_spread
    levels: List[GridLevel] = []
    for i, p in enumerate(prices):
        if cfg.side is TradeType.BUY:
            close_price = p * (Decimal(1) + step_frac)
        else:
            close_price = p * (Decimal(1) - step_frac)
        levels.append(GridLevel(index=i, open_price=p, close_price=close_price, amount_base=cap / p))
    return levels


class GridExecutor(Executor):
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
        super().__init__(
            user_id=user_id,
            controller_id=controller_id,
            trading_pair=config.trading_pair,
            adapter=adapter,
            inventory=inventory,
            keep_position=config.keep_position,
            executor_id=executor_id,
        )
        self.config = config
        self.levels: List[GridLevel] = generate_grid_levels(config)
        self._last_place_ts = 0.0

    @property
    def open_side(self) -> TradeType:
        return self.config.side

    @property
    def close_side(self) -> TradeType:
        return self.config.side.opposite

    # -- placement --------------------------------------------------------
    def _within_bounds(self, price: Decimal, mid: Decimal) -> bool:
        ab = self.config.activation_bounds
        if ab is None or mid <= 0:
            return True
        return abs(price - mid) / mid <= ab

    async def _place_open(self, level: GridLevel) -> None:
        order = await self._guard(
            lambda: self.adapter.place_order(
                self.trading_pair, self.open_side, OrderType.LIMIT_MAKER,
                level.amount_base, level.open_price, self.config.leverage, False,
            ),
            label="grid_open",
        )
        level.open_order_id = order.id
        level.state = GridLevelState.OPEN_ORDER_PLACED

    async def _place_close(self, level: GridLevel) -> None:
        order = await self._guard(
            lambda: self.adapter.place_order(
                self.trading_pair, self.close_side, OrderType.LIMIT_MAKER,
                level.filled_base, level.close_price, self.config.leverage, True,
            ),
            label="grid_close",
        )
        level.close_order_id = order.id
        level.state = GridLevelState.CLOSE_ORDER_PLACED

    def _ingest(self, level: GridLevel, order: NadoOrder, side: TradeType, *, opening: bool) -> None:
        recorded = level._open_recorded if opening else level._close_recorded
        delta_base = order.filled_base - recorded
        if delta_base <= 0:
            return
        # proportional quote/fee deltas
        prev_q = (recorded / order.filled_base) * order.filled_quote if order.filled_base else Decimal(0)
        delta_quote = order.filled_quote - prev_q
        price = delta_quote / delta_base if delta_base else Decimal(0)
        self._record_fill(
            Fill(order.id, self.trading_pair, side, delta_base, price, Decimal(0), time.time())
        )
        if opening:
            level._open_recorded = order.filled_base
            level.filled_base = order.filled_base
            level.filled_quote = order.filled_quote
        else:
            level._close_recorded = order.filled_base

    # -- lifecycle --------------------------------------------------------
    async def on_create(self) -> None:
        self._activate()
        await self._maybe_place_opens()

    async def _maybe_place_opens(self) -> None:
        if self.config.order_frequency > 0:
            if time.time() - self._last_place_ts < self.config.order_frequency:
                return
        mid = await self._guard(lambda: self.adapter.mid_price(self.trading_pair), label="mid")
        placed = 0
        for level in self.levels:
            if placed >= self.config.max_orders_per_batch:
                break
            if level.state is GridLevelState.NOT_ACTIVE and self._within_bounds(level.open_price, mid):
                await self._place_open(level)
                placed += 1
        if placed:
            self._last_place_ts = time.time()

    async def on_tick(self) -> None:
        if self.is_terminated:
            return
        mid = await self._guard(lambda: self.adapter.mid_price(self.trading_pair), label="mid")
        if self._stop_breached(mid):
            await self._stop_out(CloseType.STOP_LOSS)
            return
        for level in self.levels:
            await self._process_level(level, mid)
        await self._maybe_place_opens()
        if all(lv.state is GridLevelState.COMPLETE for lv in self.levels):
            self._terminate(CloseType.COMPLETED)

    def _stop_breached(self, mid: Decimal) -> bool:
        lp = self.config.limit_price
        if self.open_side is TradeType.BUY:
            if lp > 0 and mid <= lp:
                return True
        else:
            if lp > 0 and mid >= lp:
                return True
        tb = self.config.triple_barrier_config
        if tb is not None and tb.stop_loss is not None:
            avg = self._avg_entry()
            if avg is not None:
                if self.open_side is TradeType.BUY and mid <= avg * (Decimal(1) - tb.stop_loss):
                    return True
                if self.open_side is TradeType.SELL and mid >= avg * (Decimal(1) + tb.stop_loss):
                    return True
        return False

    def _avg_entry(self) -> Optional[Decimal]:
        base = sum((lv.filled_base for lv in self.levels), Decimal(0))
        quote = sum((lv.filled_quote for lv in self.levels), Decimal(0))
        return (quote / base) if base > 0 else None

    async def _process_level(self, level: GridLevel, mid: Decimal) -> None:
        if level.state is GridLevelState.OPEN_ORDER_PLACED:
            assert level.open_order_id is not None
            oid = level.open_order_id
            order = await self._guard(lambda: self.adapter.order_status(oid), label="grid_open_status")
            if order.state is OrderState.FILLED:
                self._ingest(level, order, self.open_side, opening=True)
                level.state = GridLevelState.OPEN_ORDER_FILLED
                await self._place_close(level)
            elif self.config.activation_bounds is not None and not self._within_bounds(level.open_price, mid):
                await self._guard(lambda: self.adapter.cancel_order(oid), label="grid_cancel")
                level.open_order_id = None
                level.state = GridLevelState.NOT_ACTIVE
        elif level.state is GridLevelState.CLOSE_ORDER_PLACED:
            assert level.close_order_id is not None
            cid = level.close_order_id
            order = await self._guard(lambda: self.adapter.order_status(cid), label="grid_close_status")
            if order.state is OrderState.FILLED:
                self._ingest(level, order, self.close_side, opening=False)
                level.state = GridLevelState.COMPLETE

    # -- stop / teardown --------------------------------------------------
    async def _cancel_all_resting(self) -> None:
        for level in self.levels:
            for oid in (level.open_order_id, level.close_order_id):
                if oid is not None:
                    cid = oid
                    try:
                        await self._guard(lambda: self.adapter.cancel_order(cid), label="grid_cancel_all")
                    except Exception:
                        pass

    def _net_base(self) -> Decimal:
        opened = sum((lv.filled_base for lv in self.levels), Decimal(0))
        closed = sum((lv._close_recorded for lv in self.levels), Decimal(0))
        return opened - closed

    async def _stop_out(self, close_type: CloseType) -> None:
        await self._cancel_all_resting()
        net = self._net_base()
        if not self.config.keep_position and net > 0:
            flat = await self._guard(
                lambda: self.adapter.place_order(
                    self.trading_pair, self.close_side, OrderType.MARKET, net, None,
                    self.config.leverage, True,
                ),
                label="grid_flatten",
            )
            if flat.filled_base > 0:
                price = flat.filled_quote / flat.filled_base
                self._record_fill(
                    Fill(flat.id, self.trading_pair, self.close_side, flat.filled_base, price, Decimal(0), time.time())
                )
        self._terminate(close_type)

    async def on_stop(self, close_type: CloseType = CloseType.EARLY_STOP) -> None:
        if self.is_terminated:
            return
        await self._stop_out(close_type)
