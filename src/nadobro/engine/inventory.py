"""Position Hold inventory — per-``(user_id, trading_pair, controller_id)``
aggregation of buy/sell base and quote amounts plus cumulative fees, with
derived breakeven, realized PnL (matched min of base in/out) and unrealized
PnL.

Phase 1 ships an in-memory :class:`InventoryRepository` behind the same
``apply_fill`` mutation seam the Phase 2 DB-backed repository will use; the
in-memory store guards mutations with a re-entrant lock so a fill is applied
atomically.

Implemented in Phase 1.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from src.nadobro.engine.types import PositionSide, TradeType, _dec

HoldKey = Tuple[int, str, str]


@dataclass
class PositionHold:
    user_id: int
    trading_pair: str
    controller_id: str
    buy_amount_base: Decimal = Decimal(0)
    buy_amount_quote: Decimal = Decimal(0)
    sell_amount_base: Decimal = Decimal(0)
    sell_amount_quote: Decimal = Decimal(0)
    cum_fees_quote: Decimal = Decimal(0)
    updated_at: float = 0.0

    @property
    def avg_buy_price(self) -> Optional[Decimal]:
        if self.buy_amount_base <= 0:
            return None
        return self.buy_amount_quote / self.buy_amount_base

    @property
    def avg_sell_price(self) -> Optional[Decimal]:
        if self.sell_amount_base <= 0:
            return None
        return self.sell_amount_quote / self.sell_amount_base

    @property
    def net_amount_base(self) -> Decimal:
        return self.buy_amount_base - self.sell_amount_base

    @property
    def side(self) -> PositionSide:
        net = self.net_amount_base
        if net > 0:
            return PositionSide.LONG
        if net < 0:
            return PositionSide.SHORT
        return PositionSide.FLAT

    @property
    def breakeven(self) -> Optional[Decimal]:
        s = self.side
        if s is PositionSide.LONG:
            return self.avg_buy_price
        if s is PositionSide.SHORT:
            return self.avg_sell_price
        return None

    @property
    def realized_pnl(self) -> Decimal:
        """Gross matched PnL: ``(avg_sell - avg_buy) * min(buy_base, sell_base)``."""
        matched = min(self.buy_amount_base, self.sell_amount_base)
        if matched <= 0:
            return Decimal(0)
        avg_b = self.avg_buy_price or Decimal(0)
        avg_s = self.avg_sell_price or Decimal(0)
        return (avg_s - avg_b) * matched

    @property
    def realized_pnl_after_fees(self) -> Decimal:
        return self.realized_pnl - self.cum_fees_quote

    def unrealized_pnl(self, mark_price: object) -> Decimal:
        mark = _dec(mark_price)
        s = self.side
        if s is PositionSide.LONG:
            be = self.avg_buy_price or Decimal(0)
            return self.net_amount_base * (mark - be)
        if s is PositionSide.SHORT:
            be = self.avg_sell_price or Decimal(0)
            short_qty = -self.net_amount_base
            return short_qty * (be - mark)
        return Decimal(0)


class InventoryRepository:
    """In-memory Position Hold store keyed by ``(user_id, trading_pair,
    controller_id)``. Holds on the same pair under different ``controller_id``
    are fully isolated."""

    def __init__(self) -> None:
        self._holds: Dict[HoldKey, PositionHold] = {}
        self._lock = threading.RLock()

    def _key(self, user_id: int, trading_pair: str, controller_id: str) -> HoldKey:
        return (user_id, trading_pair, controller_id)

    def get(self, user_id: int, trading_pair: str, controller_id: str) -> PositionHold:
        key = self._key(user_id, trading_pair, controller_id)
        with self._lock:
            hold = self._holds.get(key)
            if hold is None:
                hold = PositionHold(
                    user_id=user_id, trading_pair=trading_pair, controller_id=controller_id
                )
                self._holds[key] = hold
            return hold

    def apply_fill(
        self,
        user_id: int,
        trading_pair: str,
        controller_id: str,
        side: TradeType,
        base_qty: object,
        quote_qty: object,
        fee_quote: object = Decimal(0),
        timestamp: Optional[float] = None,
    ) -> PositionHold:
        base = _dec(base_qty)
        quote = _dec(quote_qty)
        fee = _dec(fee_quote)
        if base < 0 or quote < 0:
            raise ValueError("fill quantities must be non-negative")
        key = self._key(user_id, trading_pair, controller_id)
        with self._lock:
            hold = self._holds.get(key)
            if hold is None:
                hold = PositionHold(
                    user_id=user_id, trading_pair=trading_pair, controller_id=controller_id
                )
            if side is TradeType.BUY:
                hold.buy_amount_base += base
                hold.buy_amount_quote += quote
            else:
                hold.sell_amount_base += base
                hold.sell_amount_quote += quote
            hold.cum_fees_quote += fee
            hold.updated_at = timestamp if timestamp is not None else time.time()
            self._holds[key] = hold
            return hold

    def list_for_user(self, user_id: int) -> List[PositionHold]:
        with self._lock:
            return [h for h in self._holds.values() if h.user_id == user_id]

    def list_for_controller(self, user_id: int, controller_id: str) -> List[PositionHold]:
        with self._lock:
            return [
                h
                for h in self._holds.values()
                if h.user_id == user_id and h.controller_id == controller_id
            ]
