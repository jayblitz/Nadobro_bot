"""Shared mark-price feed per network — one REST/WS source, many readers.

Phase 4: strategy cycles and alerts previously each called
``get_all_market_prices`` independently. This singleton caches one
snapshot per network with a short TTL so 1000 users share one price
fetch instead of N.
"""
from __future__ import annotations

import asyncio
import logging
import os

from src.nadobro.utils.env import env_float
import time
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

_TTL = env_float("NADO_MARKET_FEED_TTL_SECONDS", 3.0)
_lock = asyncio.Lock()
_cache: dict[str, dict[str, Any]] = {}
_ts: dict[str, float] = {}
_fetcher: Optional[Callable[[], Any]] = None


def bind_fetcher(fetcher: Callable[[], Any]) -> None:
    """Bind a callable that returns ``{product: {mid, bid, ask}}``."""
    global _fetcher
    _fetcher = fetcher


def update_from_ws(network: str, prices: dict[str, Any]) -> None:
    """Push WS-derived prices into the cache."""
    net = str(network or "mainnet").lower()
    if not prices:
        return
    _cache[net] = dict(prices)
    _ts[net] = time.monotonic()


async def get_prices(network: str = "mainnet", *, force_refresh: bool = False) -> dict[str, Any]:
    net = str(network or "mainnet").lower()
    async with _lock:
        now = time.monotonic()
        if not force_refresh and net in _cache and (now - _ts.get(net, 0)) < _TTL:
            return dict(_cache[net])
    if _fetcher is None:
        return dict(_cache.get(net, {}))
    try:
        from src.nadobro.services.async_utils import run_blocking_sdk
        prices = await run_blocking_sdk(_fetcher)
    except Exception as exc:
        logger.debug("market_feed fetch failed network=%s err=%s", net, exc)
        return dict(_cache.get(net, {}))
    async with _lock:
        _cache[net] = dict(prices or {})
        _ts[net] = time.monotonic()
        return dict(_cache[net])


def snapshot() -> dict:
    now = time.monotonic()
    return {
        net: {"age_s": round(now - ts, 2), "products": len(_cache.get(net, {}))}
        for net, ts in _ts.items()
    }
