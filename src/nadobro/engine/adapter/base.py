"""Engine adapter base — the venue-agnostic Adapter interface (ABC) plus the
value objects exchanged with executors.

Both the live ``NadoAdapter`` (``engine/adapter/nado.py``) and the test
``MockNadoAdapter`` implement :class:`NadoAdapterBase`. This module imports
NOTHING from ``connectors/`` or ``services/nado_client`` so test doubles can
depend on it without touching the venue or the 1CT signer.

Implemented in Phase 1.
"""
from __future__ import annotations

import abc
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import AsyncIterator, List, Optional

from src.nadobro.engine.types import OrderType, TradeType


class OrderState(Enum):
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"

    @property
    def is_terminal(self) -> bool:
        return self in (OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED)


class AdapterError(Exception):
    """Venue error raised by an adapter. Executors retry on these per the
    executor retry policy (3 attempts, exponential backoff)."""


@dataclass
class NadoOrder:
    id: str
    trading_pair: str
    side: TradeType
    order_type: OrderType
    amount_base: Decimal
    price: Optional[Decimal] = None
    state: OrderState = OrderState.OPEN
    filled_base: Decimal = Decimal(0)
    filled_quote: Decimal = Decimal(0)
    fee_quote: Decimal = Decimal(0)

    @property
    def avg_fill_price(self) -> Optional[Decimal]:
        if self.filled_base <= 0:
            return None
        return self.filled_quote / self.filled_base


@dataclass
class Fill:
    order_id: str
    trading_pair: str
    side: TradeType
    amount_base: Decimal
    price: Decimal
    fee_quote: Decimal
    timestamp: float

    @property
    def amount_quote(self) -> Decimal:
        return self.amount_base * self.price


@dataclass
class OrderBookLevel:
    price: Decimal
    amount: Decimal


@dataclass
class OrderBookSnapshot:
    trading_pair: str
    bids: List[OrderBookLevel] = field(default_factory=list)
    asks: List[OrderBookLevel] = field(default_factory=list)
    timestamp: float = 0.0

    @property
    def best_bid(self) -> Optional[Decimal]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[Decimal]:
        return self.asks[0].price if self.asks else None

    @property
    def mid(self) -> Optional[Decimal]:
        bid, ask = self.best_bid, self.best_ask
        if bid is None or ask is None:
            return None
        return (bid + ask) / Decimal(2)


class NadoAdapterBase(abc.ABC):
    """Contract that every Nado adapter (live or simulated) must satisfy.

    All ``async`` methods may raise :class:`AdapterError` on transient venue
    failures; executors apply the retry policy around them.
    """

    connector_name: str = "nado"

    @abc.abstractmethod
    async def place_order(
        self,
        trading_pair: str,
        side: TradeType,
        order_type: OrderType,
        amount_base: Decimal,
        price: Optional[Decimal] = None,
        leverage: int = 1,
        reduce_only: bool = False,
    ) -> NadoOrder:
        ...

    @abc.abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order. Idempotent: cancelling an unknown/terminal order
        returns ``False`` rather than raising."""
        ...

    @abc.abstractmethod
    async def order_status(self, order_id: str) -> NadoOrder:
        ...

    @abc.abstractmethod
    def fill_stream(self, trading_pair: str) -> AsyncIterator[Fill]:
        """Async generator yielding fills for ``trading_pair`` in order."""
        ...

    @abc.abstractmethod
    async def order_book(self, trading_pair: str) -> OrderBookSnapshot:
        ...

    @abc.abstractmethod
    async def mid_price(self, trading_pair: str) -> Decimal:
        ...

    @abc.abstractmethod
    def tick_size(self, trading_pair: str) -> Decimal:
        ...

    @abc.abstractmethod
    def lot_size(self, trading_pair: str) -> Decimal:
        ...

    @abc.abstractmethod
    def min_notional(self, trading_pair: str) -> Decimal:
        ...

    # Market-data reads (concrete defaults so test doubles need not implement
    # them; the live adapter overrides). Consumed via the MarketData service.
    async def candles(
        self, trading_pair: str, timeframe: str = "1h", limit: int = 200
    ) -> List[dict]:
        raise NotImplementedError

    async def funding_rate(self, trading_pair: str) -> Optional[Decimal]:
        raise NotImplementedError
