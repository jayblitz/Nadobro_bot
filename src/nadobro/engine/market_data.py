"""Market Data service — cached reads of mid price, candles, and funding via
the engine adapter, plus the provider callables that controllers consume
(``candle_provider`` for DynamicGrid, ``funding_provider`` for DeltaNeutral).

Goes through the adapter (never the venue client directly), so the engine's
single venue boundary is preserved. A short TTL cache avoids hammering the
venue when several controllers/routines ask for the same data within a tick.

Implemented in Phase 4 / production hardening (D).
"""
from __future__ import annotations

import time
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from src.nadobro.engine.adapter.base import NadoAdapterBase


class MarketData:
    def __init__(self, adapter: NadoAdapterBase, ttl_seconds: float = 5.0) -> None:
        self.adapter = adapter
        self.ttl = ttl_seconds
        self._cache: Dict[Tuple[Any, ...], Tuple[float, Any]] = {}

    async def _cached(self, key: Tuple[Any, ...], fetch: Callable[[], Awaitable[Any]]) -> Any:
        now = time.time()
        hit = self._cache.get(key)
        if hit is not None and (now - hit[0]) < self.ttl:
            return hit[1]
        value = await fetch()
        self._cache[key] = (now, value)
        return value

    async def mid(self, trading_pair: str) -> Decimal:
        return await self._cached(("mid", trading_pair), lambda: self.adapter.mid_price(trading_pair))

    async def candles(
        self, trading_pair: str, timeframe: str = "1h", limit: int = 200
    ) -> List[dict]:
        return await self._cached(
            ("candles", trading_pair, timeframe, limit),
            lambda: self.adapter.candles(trading_pair, timeframe, limit),
        )

    async def funding(self, trading_pair: str) -> Optional[Decimal]:
        return await self._cached(
            ("funding", trading_pair), lambda: self.adapter.funding_rate(trading_pair)
        )

    def invalidate(self) -> None:
        self._cache.clear()

    # -- provider callables for controller configs ------------------------
    def candle_provider(
        self, timeframe: str = "1h", limit: int = 200
    ) -> Callable[[str], Awaitable[List[dict]]]:
        async def provider(trading_pair: str) -> List[dict]:
            return await self.candles(trading_pair, timeframe, limit)

        return provider

    def funding_provider(self) -> Callable[[str], Awaitable[Optional[Decimal]]]:
        async def provider(trading_pair: str) -> Optional[Decimal]:
            return await self.funding(trading_pair)

        return provider
