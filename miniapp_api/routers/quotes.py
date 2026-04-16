"""Market data from Nado Indexer — tickers, funding rates, perp contracts."""

import logging
import time

from fastapi import APIRouter

from miniapp_api.config import get_perp_products, get_product_id, get_product_max_leverage
from miniapp_api.dependencies import AuthUser, UserClient
from src.nadobro.services.async_utils import run_blocking

logger = logging.getLogger(__name__)

router = APIRouter()


def _leverage_map_for_network(names: list[str], network: str) -> dict[str, int]:
    return {n: get_product_max_leverage(n, network=network) for n in names}


# Cache for tickers/contracts (refreshed every 30s)
_tickers_cache: dict[str, tuple[float, dict]] = {}
_TICKERS_TTL = 30


@router.get("/quotes")
async def get_quotes(client: UserClient, user: AuthUser):
    """Return 24h price changes, volume, and market data for all perp products.

    Uses Nado Indexer /v2/contracts which returns per-ticker:
    - last_price, price_change_percent_24h, base_volume, quote_volume
    - mark_price, index_price, funding_rate, open_interest
    """
    cache_key = f"contracts_{user.network}"
    now = time.time()
    cached = _tickers_cache.get(cache_key)
    if cached and (now - cached[0]) < _TICKERS_TTL:
        return {"quotes": cached[1]}

    try:
        contracts = await run_blocking(client.get_perp_contracts)
    except Exception as exc:
        logger.warning("Nado perp contracts fetch failed: %s", exc)
        if cached:
            return {"quotes": cached[1]}
        return {"quotes": {}, "error": str(exc)}

    if not contracts:
        if cached:
            return {"quotes": cached[1]}
        return {"quotes": {}}

    # Map ticker_id (e.g. "BTC-PERP_USDT") to our product names
    perp_names = await run_blocking(get_perp_products, user.network)
    leverages = await run_blocking(_leverage_map_for_network, perp_names, user.network)
    quotes: dict[str, dict] = {}

    for ticker_id, info in contracts.items():
        if not isinstance(info, dict):
            continue

        # Extract product name from ticker_id: "BTC-PERP_USDT" -> "BTC"
        base = ticker_id.split("-")[0].upper() if "-" in ticker_id else ticker_id.split("_")[0].upper()

        if base not in perp_names:
            continue

        max_lev = leverages.get(base, 20)
        quotes[base] = {
            "price": info.get("last_price"),
            "change_24h": info.get("price_change_percent_24h"),
            "volume_24h": info.get("quote_volume") or info.get("base_volume"),
            "mark_price": info.get("mark_price"),
            "index_price": info.get("index_price"),
            "funding_rate": info.get("funding_rate"),
            "open_interest": info.get("open_interest"),
            "open_interest_usd": info.get("open_interest_usd"),
            "max_leverage": max_lev,
        }

    _tickers_cache[cache_key] = (now, quotes)
    return {"quotes": quotes}


@router.get("/funding-rates")
async def get_funding_rates(client: UserClient, user: AuthUser):
    """Return current funding rates for all perp products from Nado Indexer."""
    try:
        perp_names = await run_blocking(get_perp_products, user.network)
        product_ids = []
        id_to_name = {}
        for name in perp_names:
            pid = get_product_id(name, network=user.network)
            if pid is not None:
                product_ids.append(pid)
                id_to_name[pid] = name

        rates = await run_blocking(client.get_indexer_funding_rates, product_ids)

        result = {}
        for pid, data in rates.items():
            name = id_to_name.get(pid)
            if name:
                result[name] = {
                    "funding_rate": data.get("funding_rate"),
                    "update_time": data.get("update_time"),
                }

        return {"funding_rates": result}
    except Exception as exc:
        logger.warning("Funding rates fetch failed: %s", exc)
        return {"funding_rates": {}, "error": str(exc)}


@router.get("/products/{product}/mark-price")
async def get_mark_price(product: str, client: UserClient, user: AuthUser):
    """Return mark and index prices for a specific product from Nado Indexer."""
    product_upper = product.upper()
    pid = get_product_id(product_upper, network=user.network)
    if pid is None:
        return {"error": f"Unknown product: {product}"}

    try:
        data = await run_blocking(client.get_perp_prices, pid)
        if data:
            return {
                "product": product_upper,
                "mark_price": data.get("mark_price"),
                "index_price": data.get("index_price"),
            }
        return {"product": product_upper, "mark_price": None, "index_price": None}
    except Exception as exc:
        logger.warning("Mark price fetch failed for %s: %s", product_upper, exc)
        return {"product": product_upper, "error": str(exc)}
