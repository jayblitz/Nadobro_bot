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

import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import List, Optional

logger = logging.getLogger(__name__)

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
    # Cumulative venue-side fills already recorded for this level. We track
    # base/quote/fee separately for both open and close legs so multi-price
    # fills (variable VWAP) can be ingested as exact deltas — BUG-GR-4 fix.
    _open_recorded: Decimal = Decimal(0)
    _open_quote_recorded: Decimal = Decimal(0)
    _open_fee_recorded: Decimal = Decimal(0)
    _close_recorded: Decimal = Decimal(0)
    _close_quote_recorded: Decimal = Decimal(0)
    _close_fee_recorded: Decimal = Decimal(0)
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
        await self._place_close_remaining(level, level.filled_base - level._close_recorded)

    async def _place_close_remaining(self, level: GridLevel, base_amount: Decimal) -> None:
        if base_amount <= 0:
            level.state = GridLevelState.COMPLETE
            return
        order = await self._guard(
            lambda: self.adapter.place_order(
                self.trading_pair, self.close_side, OrderType.LIMIT_MAKER,
                base_amount, level.close_price, self.config.leverage, True,
            ),
            label="grid_close",
        )
        level.close_order_id = order.id
        level.state = GridLevelState.CLOSE_ORDER_PLACED

    def _ingest(self, level: GridLevel, order: NadoOrder, side: TradeType, *, opening: bool) -> None:
        """Record the *delta* between the venue's current cumulative fill
        totals and what this level has already booked. BUG-GR-4 fix: track
        recorded base/quote/fee explicitly so multi-price (VWAP) fills give
        the right marginal price for each new chunk.
        BUG-GR-3 fix: pass the fee delta to the inventory book.
        """
        if opening:
            recorded_b = level._open_recorded
            recorded_q = level._open_quote_recorded
            recorded_f = level._open_fee_recorded
        else:
            recorded_b = level._close_recorded
            recorded_q = level._close_quote_recorded
            recorded_f = level._close_fee_recorded
        delta_base = order.filled_base - recorded_b
        if delta_base <= 0:
            return
        delta_quote = order.filled_quote - recorded_q
        delta_fee = order.fee_quote - recorded_f
        # Use the marginal price of this *chunk*, not the running VWAP.
        price = (delta_quote / delta_base) if delta_base > 0 else Decimal(0)
        self._record_fill(
            Fill(order.id, self.trading_pair, side, delta_base, price, delta_fee, time.time())
        )
        if opening:
            level._open_recorded = order.filled_base
            level._open_quote_recorded = order.filled_quote
            level._open_fee_recorded = order.fee_quote
            level.filled_base = order.filled_base
            level.filled_quote = order.filled_quote
        else:
            level._close_recorded = order.filled_base
            level._close_quote_recorded = order.filled_quote
            level._close_fee_recorded = order.fee_quote

    # -- lifecycle --------------------------------------------------------
    async def on_create(self) -> None:
        self._activate()
        await self._maybe_place_opens()

    async def _maybe_place_opens(self) -> None:
        # Regime gate / inventory cap: the owning controller sets this flag
        # to stop NEW entry orders while letting existing close legs, stops,
        # and fills keep managing. Pause is "stop digging", never "flatten".
        if self.suppress_new_entries:
            return
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
            # Always ingest any partial fills first so we don't lose inventory
            # records when the order is about to be cancelled (BUG-GR-1) or
            # was externally cancelled/rejected (BUG-GR-2).
            self._ingest(level, order, self.open_side, opening=True)
            if order.state is OrderState.FILLED:
                level.state = GridLevelState.OPEN_ORDER_FILLED
                await self._place_close(level)
                return
            if order.state in (OrderState.CANCELLED, OrderState.REJECTED):
                # BUG-GR-2 fix: external cancel/reject. If we got any fills,
                # close them via the close leg; otherwise return to NOT_ACTIVE
                # so the next placement cycle re-issues the open.
                level.open_order_id = None
                if level.filled_base > 0:
                    level.state = GridLevelState.OPEN_ORDER_FILLED
                    await self._place_close(level)
                else:
                    level.state = GridLevelState.NOT_ACTIVE
                return
            if self.config.activation_bounds is not None and not self._within_bounds(level.open_price, mid):
                # BUG-GR-1 fix: cancel, then RE-POLL to capture any fills that
                # arrived between status and cancel. If any base was filled,
                # treat the level as opened and book the close leg.
                try:
                    await self._guard(lambda: self.adapter.cancel_order(oid), label="grid_cancel")
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "grid %s: out-of-bounds cancel failed for %s — order may still be resting: %s",
                        self.id, oid, exc,
                    )
                try:
                    post_cancel = await self._guard(
                        lambda: self.adapter.order_status(oid), label="grid_open_post_cancel",
                    )
                    self._ingest(level, post_cancel, self.open_side, opening=True)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "grid %s: post-cancel status probe failed for %s — last fills may be unbooked: %s",
                        self.id, oid, exc,
                    )
                level.open_order_id = None
                if level.filled_base > 0:
                    level.state = GridLevelState.OPEN_ORDER_FILLED
                    await self._place_close(level)
                else:
                    level.state = GridLevelState.NOT_ACTIVE
        elif level.state is GridLevelState.CLOSE_ORDER_PLACED:
            assert level.close_order_id is not None
            cid = level.close_order_id
            order = await self._guard(lambda: self.adapter.order_status(cid), label="grid_close_status")
            self._ingest(level, order, self.close_side, opening=False)
            if order.state is OrderState.FILLED:
                level.state = GridLevelState.COMPLETE
                return
            if order.state in (OrderState.CANCELLED, OrderState.REJECTED):
                # BUG-GR-2 fix: re-place the close leg for the remaining base
                # if any. If the level is fully closed via partials, complete it.
                level.close_order_id = None
                remaining = level.filled_base - level._close_recorded
                if remaining <= 0:
                    level.state = GridLevelState.COMPLETE
                else:
                    # Drop back to OPEN_ORDER_FILLED so _place_close re-issues
                    # against the remaining inventory.
                    level.state = GridLevelState.OPEN_ORDER_FILLED
                    await self._place_close_remaining(level, remaining)

    # -- stop / teardown --------------------------------------------------
    async def _cancel_all_resting(self) -> None:
        for level in self.levels:
            for oid in (level.open_order_id, level.close_order_id):
                if oid is not None:
                    cid = oid
                    try:
                        await self._guard(lambda: self.adapter.cancel_order(cid), label="grid_cancel_all")
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "grid %s: cancel-all failed for %s — order may still be resting: %s",
                            self.id, cid, exc,
                        )

    def _net_base(self) -> Decimal:
        opened = sum((lv.filled_base for lv in self.levels), Decimal(0))
        closed = sum((lv._close_recorded for lv in self.levels), Decimal(0))
        return opened - closed

    async def _stop_out(self, close_type: CloseType) -> None:
        await self._cancel_all_resting()
        # After cancelling open orders, re-poll levels to absorb any last
        # partial fills (BUG-GR-1 again at stop-out time).
        for level in self.levels:
            for label, oid, opening in (
                ("grid_open_status_stop", level.open_order_id, True),
                ("grid_close_status_stop", level.close_order_id, False),
            ):
                if oid is None:
                    continue
                try:
                    captured = oid
                    refreshed = await self._guard(
                        lambda: self.adapter.order_status(captured), label=label,
                    )
                    self._ingest(level, refreshed, self.open_side if opening else self.close_side, opening=opening)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "grid %s: stop-out status refresh (%s) failed for %s — "
                        "flatten size may miss last fills: %s",
                        self.id, label, oid, exc,
                    )
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
                # BUG-GR-5 fix: use the venue's authoritative quote total
                # rather than re-deriving price * base (which silently loses
                # precision for multi-price market fills) and pass the real
                # venue fee through to inventory rather than zero.
                price = flat.filled_quote / flat.filled_base
                self._record_fill(
                    Fill(
                        flat.id, self.trading_pair, self.close_side,
                        flat.filled_base, price, flat.fee_quote, time.time(),
                    )
                )
        self._terminate(close_type)

    async def on_stop(self, close_type: CloseType = CloseType.EARLY_STOP) -> None:
        if self.is_terminated:
            return
        await self._stop_out(close_type)
