"""Trade routes — market orders, limit orders, intent parsing."""

import logging

from fastapi import APIRouter, HTTPException

from miniapp_api.dependencies import AuthUser
from miniapp_api.models.schemas import (
    LimitOrderRequest,
    MarketOrderRequest,
    ParseIntentRequest,
    ParseIntentResponse,
    TradeResponse,
)
from src.nadobro.services.async_utils import run_blocking

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/trade/market", response_model=TradeResponse)
async def place_market_order(body: MarketOrderRequest, user: AuthUser):
    """Execute a market order (IOC with slippage tolerance)."""
    from src.nadobro.services.trade_service import execute_market_order

    is_long = body.side == "long"

    # TP/SL percentages are computed against a reference price at execution
    # time by the trade service; we pass None here and let the service handle it.
    # If the caller provided percentages, convert to absolute prices using
    # a current price estimate.
    tp_price = None
    sl_price = None
    if body.take_profit_pct is not None or body.stop_loss_pct is not None:
        from src.nadobro.services.user_service import get_user_nado_client as _get_client
        from miniapp_api.config import get_product_id
        client = await run_blocking(_get_client, user.telegram_id, user.network)
        if client:
            pid = get_product_id(body.product, network=user.network)
            if pid is not None:
                price_data = await run_blocking(client.get_market_price, pid)
                mid = price_data.get("mid") or price_data.get("ask") or 0
                if mid and body.take_profit_pct is not None:
                    tp_price = mid * (1 + body.take_profit_pct / 100) if is_long else mid * (1 - body.take_profit_pct / 100)
                if mid and body.stop_loss_pct is not None:
                    sl_price = mid * (1 - body.stop_loss_pct / 100) if is_long else mid * (1 + body.stop_loss_pct / 100)

    result = await run_blocking(
        execute_market_order,
        telegram_id=user.telegram_id,
        product=body.product,
        size=body.size_usd,
        is_long=is_long,
        leverage=body.leverage,
        tp_price=tp_price,
        sl_price=sl_price,
        enforce_rate_limit=True,
    )

    if not result.get("success"):
        return TradeResponse(ok=False, error=result.get("error", "Order failed"))

    return TradeResponse(
        ok=True,
        trade_id=result.get("trade_id"),
        digest=result.get("digest"),
        fill_price=result.get("fill_price"),
        size=result.get("size"),
        side=body.side,
        product=body.product,
    )


@router.post("/trade/limit", response_model=TradeResponse)
async def place_limit_order(body: LimitOrderRequest, user: AuthUser):
    """Place a limit order (post-only quote)."""
    from src.nadobro.services.trade_service import execute_limit_order

    is_long = body.side == "long"

    result = await run_blocking(
        execute_limit_order,
        telegram_id=user.telegram_id,
        product=body.product,
        size=body.size_usd,
        price=body.price,
        is_long=is_long,
        leverage=body.leverage,
        enforce_rate_limit=True,
    )

    if not result.get("success"):
        return TradeResponse(ok=False, error=result.get("error", "Order failed"))

    return TradeResponse(
        ok=True,
        trade_id=result.get("trade_id"),
        digest=result.get("digest"),
        fill_price=result.get("fill_price"),
        size=result.get("size"),
        side=body.side,
        product=body.product,
    )


@router.post("/trade/parse-intent", response_model=ParseIntentResponse)
async def parse_intent(body: ParseIntentRequest, user: AuthUser):
    """Parse a natural language trade command into structured intent."""
    from src.nadobro.handlers.intent_parser import parse_trade_intent

    result = await run_blocking(parse_trade_intent, body.text, user.network)

    if not result:
        return ParseIntentResponse()

    # parse_trade_intent returns: kind, direction, product, order_type,
    # size, leverage, limit_price, tp, sl, missing, raw
    direction = result.get("direction", "")
    side = "long" if direction and direction.lower() in ("long", "buy") else (
        "short" if direction and direction.lower() in ("short", "sell") else None
    )

    return ParseIntentResponse(
        intent=result.get("kind"),
        product=result.get("product"),
        side=side,
        size_usd=result.get("size"),
        price=result.get("limit_price"),
        leverage=result.get("leverage"),
        raw=result,
    )
