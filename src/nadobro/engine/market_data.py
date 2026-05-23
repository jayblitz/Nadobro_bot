"""Market Data service — cached reads of mid price, candles, and funding via
the engine adapter, plus the provider callables that controllers consume
(``candle_provider`` for DynamicGrid, ``funding_provider`` for DeltaNeutral).

Goes through the adapter (never the venue client directly), so the engine's
single venue boundary is preserved. A short TTL cache avoids hammering the
venue when several controllers/routines ask for the same data within a tick.

Implemented in Phase 4 / production hardening (D).
"""
from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from src.nadobro.engine.adapter.base import NadoAdapterBase


class MarketData:
    """LRU-bounded TTL cache for market data fetches.

    BUG-MD-1 fix: cache is now bounded (LRU eviction at ``max_entries``) so
    long-running deployments with many trading pairs don't accumulate stale
    entries forever.

    BUG-MD-2 fix: per-key locks ensure only one in-flight fetch per cache
    key. Without this, concurrent ticks that miss the cache simultaneously
    fan-out N identical requests to the venue (thundering-herd).
    """

    DEFAULT_MAX_ENTRIES = 1024

    def __init__(
        self,
        adapter: NadoAdapterBase,
        ttl_seconds: float = 5.0,
        *,
        max_entries: int = DEFAULT_MAX_ENTRIES,
    ) -> None:
        self.adapter = adapter
        self.ttl = ttl_seconds
        self.max_entries = max(1, int(max_entries))
        # OrderedDict gives O(1) LRU ordering via move_to_end / popitem.
        self._cache: "OrderedDict[Tuple[Any, ...], Tuple[float, Any]]" = OrderedDict()
        self._locks: Dict[Tuple[Any, ...], asyncio.Lock] = {}

    def _evict_if_needed(self) -> None:
        while len(self._cache) > self.max_entries:
            self._cache.popitem(last=False)  # evict oldest

    def _lock_for(self, key: Tuple[Any, ...]) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    async def _cached(self, key: Tuple[Any, ...], fetch: Callable[[], Awaitable[Any]]) -> Any:
        now = time.time()
        hit = self._cache.get(key)
        if hit is not None and (now - hit[0]) < self.ttl:
            self._cache.move_to_end(key)  # mark as recently used
            return hit[1]

        # Single-flight: only one coroutine performs the fetch per key.
        lock = self._lock_for(key)
        async with lock:
            # Re-check inside the lock — another waiter may have populated.
            hit = self._cache.get(key)
            now = time.time()
            if hit is not None and (now - hit[0]) < self.ttl:
                self._cache.move_to_end(key)
                return hit[1]
            value = await fetch()
            self._cache[key] = (time.time(), value)
            self._cache.move_to_end(key)
            self._evict_if_needed()
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
        # Drop stale per-key locks too; they will be lazily recreated on demand.
        self._locks.clear()

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
