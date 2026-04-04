"""Candle (OHLCV) data from Nado Indexer — native on-chain data."""

import logging
import time

from fastapi import APIRouter, Query

from miniapp_api.config import get_product_id
from miniapp_api.dependencies import AuthUser, UserClient
from src.nadobro.services.async_utils import run_blocking

logger = logging.getLogger(__name__)

router = APIRouter()

# Map frontend interval strings to Nado granularity (seconds)
_GRANULARITY_MAP: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "1d": 86400,
    "1w": 604800,
}

# In-memory cache: (product, granularity) -> (expires_at, data)
_cache: dict[tuple[str, int], tuple[float, list]] = {}
_CACHE_TTL: dict[str, int] = {
    "1m": 30,
    "5m": 60,
    "15m": 120,
    "1h": 300,
    "4h": 600,
    "1d": 900,
    "2h": 300,
    "1w": 1800,
}


@router.get("/products/{product}/candles")
async def get_candles(
    product: str,
    client: UserClient,
    user: AuthUser,
    interval: str = Query("1h", description="Candle interval: 1m, 5m, 15m, 1h, 2h, 4h, 1d, 1w"),
    limit: int = Query(200, ge=1, le=500),
):
    """Return OHLCV candle data from Nado Indexer for TradingView charts."""
    granularity = _GRANULARITY_MAP.get(interval)
    if granularity is None:
        return {
            "candles": [],
            "error": f"Invalid interval. Use: {', '.join(sorted(_GRANULARITY_MAP.keys()))}",
        }

    product_upper = product.upper()
    pid = get_product_id(product_upper, network=user.network)
    if pid is None:
        return {"candles": [], "error": f"Unknown product: {product}"}

    # Check cache
    cache_key = (product_upper, granularity)
    now = time.time()
    cached = _cache.get(cache_key)
    if cached and cached[0] > now:
        return {"candles": cached[1][:limit]}

    try:
        candles = await run_blocking(client.get_candlesticks, pid, granularity, limit)
    except Exception as exc:
        logger.warning("Nado candle fetch failed for %s: %s", product_upper, exc)
        # Return stale cache if available
        if cached:
            return {"candles": cached[1][:limit]}
        return {"candles": [], "error": "Failed to fetch candle data"}

    if not candles:
        if cached:
            return {"candles": cached[1][:limit]}
        return {"candles": []}

    ttl = _CACHE_TTL.get(interval, 60)
    _cache[cache_key] = (now + ttl, candles)

    return {"candles": candles[:limit]}
