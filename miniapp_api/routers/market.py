"""Market data routes — products, prices."""

import logging
import time

from fastapi import APIRouter

from miniapp_api.config import (
    PRODUCTS,
    get_perp_products,
    get_product_id,
    get_product_max_leverage,
    get_product_name,
    is_product_isolated_only,
)
from miniapp_api.dependencies import AuthUser, UserClient
from miniapp_api.models.schemas import AllPricesResponse, PriceResponse, ProductInfo
from src.nadobro.services.async_utils import run_blocking

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/products", response_model=list[ProductInfo])
async def list_products(user: AuthUser):
    """Return all available perpetual products for the user's network."""
    network = user.network
    names = await run_blocking(get_perp_products, network)
    result = []
    for name in names:
        pid = get_product_id(name, network=network)
        info = PRODUCTS.get(name, {})
        result.append(ProductInfo(
            id=pid or info.get("id", 0),
            name=name,
            symbol=info.get("symbol", f"{name}-PERP"),
            type="perp",
            max_leverage=get_product_max_leverage(name, network=network),
            isolated_only=is_product_isolated_only(name, network=network),
        ))
    return result


@router.get("/products/{product}/price", response_model=PriceResponse)
async def get_product_price(product: str, client: UserClient, user: AuthUser):
    """Get current bid/ask/mid for a single product."""
    pid = get_product_id(product, network=user.network)
    if pid is None:
        return PriceResponse(product=product)

    price_data = await run_blocking(client.get_market_price, pid)
    return PriceResponse(
        product=product,
        bid=price_data.get("bid"),
        ask=price_data.get("ask"),
        mid=price_data.get("mid"),
        timestamp=time.time(),
    )


@router.get("/prices", response_model=AllPricesResponse)
async def get_all_prices(client: UserClient, user: AuthUser):
    """Get current prices for all products."""
    all_prices = await run_blocking(client.get_all_market_prices)
    prices: dict[str, PriceResponse] = {}
    now = time.time()
    for pid_key, pdata in (all_prices or {}).items():
        try:
            pid = int(pid_key) if isinstance(pid_key, str) else pid_key
        except (ValueError, TypeError):
            continue
        name = get_product_name(pid, network=user.network)
        if isinstance(pdata, dict):
            mid = pdata.get("mid")
            bid = pdata.get("bid")
            ask = pdata.get("ask")
        else:
            # Scalar value — treat as mid price.
            mid = float(pdata) if pdata is not None else None
            bid = None
            ask = None
        prices[name] = PriceResponse(
            product=name, bid=bid, ask=ask, mid=mid, timestamp=now,
        )
    return AllPricesResponse(prices=prices)
