"""Position routes — list, close, close-all, trade history."""

import logging

from fastapi import APIRouter

from miniapp_api.config import get_product_name
from miniapp_api.dependencies import AuthUser, UserClient
from miniapp_api.models.schemas import (
    ClosePositionRequest,
    OkResponse,
    PortfolioSummary,
    PositionResponse,
    TradeHistoryItem,
    TradeResponse,
)
from src.nadobro.services.async_utils import run_blocking

logger = logging.getLogger(__name__)

router = APIRouter()


def _position_to_response(pos: dict) -> PositionResponse:
    """Convert a raw position dict from NadoClient into a response model.

    NadoClient.get_all_positions() returns dicts with keys:
      product_id, product_name, amount, signed_amount, price, side
    where side is "LONG" or "SHORT" (uppercase) and price is the entry price.
    """
    pid = pos.get("product_id", 0)
    side_raw = pos.get("side", "")
    side = side_raw.lower() if side_raw else ""

    return PositionResponse(
        product_id=int(pid),
        product_name=pos.get("product_name") or get_product_name(pid),
        side=side,
        size=abs(float(pos.get("amount", 0) or pos.get("signed_amount", 0))),
        entry_price=float(pos.get("price", 0)),
        # These fields are not available from the basic position data.
        # They require additional API calls (mark price, margin info).
        mark_price=None,
        unrealized_pnl=None,
        leverage=None,
        liquidation_price=None,
        margin=None,
    )


@router.get("/positions", response_model=list[PositionResponse])
async def list_positions(client: UserClient):
    """List all open positions."""
    positions = await run_blocking(client.get_all_positions)
    return [_position_to_response(p) for p in (positions or [])]


@router.get("/portfolio", response_model=PortfolioSummary)
async def get_portfolio(client: UserClient, user: AuthUser):
    """Get portfolio summary: equity, balance, positions."""
    positions = await run_blocking(client.get_all_positions) or []
    balance = await run_blocking(client.get_balance) or {}

    # get_balance returns {"exists": bool, "balances": {product_id: amount}}
    # The USDT0 balance (product_id 0) is the main trading balance.
    balances = balance.get("balances", {})
    usdt_balance = float(balances.get(0, 0) or 0)

    pos_responses = [_position_to_response(p) for p in positions]

    return PortfolioSummary(
        equity=usdt_balance,
        available_balance=usdt_balance,
        total_unrealized_pnl=0.0,  # requires mark price data
        total_margin_used=0.0,
        positions=pos_responses,
    )


@router.post("/positions/close", response_model=TradeResponse)
async def close_position(body: ClosePositionRequest, user: AuthUser):
    """Close a position (fully or partially)."""
    from src.nadobro.services.trade_service import close_position as _close

    # Compute size for partial closes.
    size = None
    if body.close_pct < 100:
        # Look up current position size to compute partial amount.
        from src.nadobro.services.user_service import get_user_nado_client as _get_client
        from miniapp_api.config import get_product_id
        client = await run_blocking(_get_client, user.telegram_id, user.network)
        if client:
            pid = get_product_id(body.product, network=user.network)
            positions = await run_blocking(client.get_all_positions) or []
            for p in positions:
                if p.get("product_id") == pid:
                    full_size = abs(float(p.get("amount", 0)))
                    size = full_size * (body.close_pct / 100)
                    break

    result = await run_blocking(
        _close,
        telegram_id=user.telegram_id,
        product=body.product,
        size=size,
        network=user.network,
    )

    if not result.get("success"):
        return TradeResponse(ok=False, error=result.get("error", "Close failed"))

    # close_position returns: {"success": True, "cancelled": close_size, "product": ...}
    return TradeResponse(
        ok=True,
        product=result.get("product", body.product),
        size=result.get("cancelled"),
    )


@router.post("/positions/close-all", response_model=OkResponse)
async def close_all_positions(user: AuthUser):
    """Close all open positions and cancel all resting orders."""
    from src.nadobro.services.trade_service import close_all_positions as _close_all

    result = await run_blocking(
        _close_all,
        telegram_id=user.telegram_id,
        network=user.network,
    )

    if not result.get("success"):
        return OkResponse(ok=False, message=result.get("error", "Close-all failed"))

    return OkResponse(message="All positions closed")


@router.get("/trades/history", response_model=list[TradeHistoryItem])
async def trade_history(user: AuthUser, limit: int = 20):
    """Return recent trade history."""
    from src.nadobro.services.trade_service import get_trade_history

    rows = await run_blocking(get_trade_history, user.telegram_id, limit)

    # get_trade_history returns dicts with keys:
    # id, product (not product_name), type, side, size, price, status,
    # pnl, close_price, network, created_at, closed_at
    return [
        TradeHistoryItem(
            id=r.get("id", 0),
            product_name=r.get("product", ""),
            side=r.get("side", ""),
            size=r.get("size", 0),
            price=r.get("price"),
            leverage=r.get("leverage", 1.0),
            status=r.get("status", ""),
            pnl=r.get("pnl"),
            fees=r.get("fees", 0),
            created_at=r.get("created_at"),
            filled_at=r.get("closed_at"),  # trade_service returns closed_at, not filled_at
        )
        for r in (rows or [])
    ]
