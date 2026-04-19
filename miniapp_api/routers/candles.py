"""Candle (OHLCV) data from Nado Indexer — native on-chain data."""

import logging
import time

from fastapi import APIRouter, Query

from miniapp_api.candle_intervals import CACHE_TTL_SECONDS, GRANULARITY_SECONDS
from miniapp_api.config import get_product_id
from miniapp_api.dependencies import AuthUser, UserClient
from src.nadobro.services.async_utils import run_blocking

logger = logging.getLogger(__name__)

router = APIRouter()

# In-memory cache: (network, product, granularity) -> (expires_at, data)
_cache: dict[tuple[str, str, int], tuple[float, list]] = {}


@router.get("/products/{product}/candles")
async def get_candles(
    product: str,
    client: UserClient,
    user: AuthUser,
    interval: str = Query(
        "1h",
        description=f"Candle interval: {', '.join(sorted(GRANULARITY_SECONDS.keys()))}",
    ),
    limit: int = Query(200, ge=1, le=500),
):
    """Return OHLCV candle data from Nado Indexer for TradingView charts."""
    granularity = GRANULARITY_SECONDS.get(interval)
    if granularity is None:
        return {
            "candles": [],
            "error": f"Invalid interval. Use: {', '.join(sorted(GRANULARITY_SECONDS.keys()))}",
        }

    product_upper = product.upper()
    pid = get_product_id(product_upper, network=user.network)
    if pid is None:
        return {"candles": [], "error": f"Unknown product: {product}"}

    # Check cache
    cache_key = (str(user.network), product_upper, granularity)
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

    ttl = CACHE_TTL_SECONDS.get(interval, 60)
    _cache[cache_key] = (now + ttl, candles)

    return {"candles": candles[:limit]}
