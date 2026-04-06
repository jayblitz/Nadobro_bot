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
from src.nadobro.models.database import find_open_trade
from src.nadobro.services.async_utils import run_blocking

logger = logging.getLogger(__name__)

router = APIRouter()


def _ref_mid(price_data: dict) -> float:
    bid = float(price_data.get("bid") or 0)
    ask = float(price_data.get("ask") or 0)
    mid = price_data.get("mid")
    if mid is not None:
        m = float(mid)
        if m > 0:
            return m
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    return max(bid, ask, 0.0)


def _signed_amount(pos: dict) -> float:
    raw = pos.get("signed_amount")
    if raw is not None:
        return float(raw)
    side = str(pos.get("side", "long")).lower()
    amt = abs(float(pos.get("amount", 0)))
    if side == "short":
        return -amt
    return amt


def _build_position_response(
    pos: dict,
    *,
    mark_price: float | None,
    unrealized_pnl: float | None,
    leverage: float | None,
    margin: float | None,
) -> PositionResponse:
    pid = pos.get("product_id", 0)
    side_raw = pos.get("side", "")
    side = side_raw.lower() if side_raw else ""

    return PositionResponse(
        product_id=int(pid),
        product_name=pos.get("product_name") or get_product_name(pid),
        side=side,
        size=abs(float(pos.get("amount", 0) or pos.get("signed_amount", 0))),
        entry_price=float(pos.get("price", 0)),
        mark_price=mark_price,
        unrealized_pnl=unrealized_pnl,
        leverage=leverage,
        liquidation_price=(
            float(pos["liquidation_price"])
            if pos.get("liquidation_price") is not None
            else None
        ),
        margin=margin,
    )


def _enrich_single_position(
    client,
    pos: dict,
    telegram_id: int,
    network: str,
) -> PositionResponse:
    pid = int(pos.get("product_id", 0))
    price_data = {}
    try:
        price_data = client.get_market_price(pid) or {}
    except Exception as e:
        logger.debug("get_market_price failed for %s: %s", pid, e)

    mark = _ref_mid(price_data) if price_data else 0.0
    mark_price = mark if mark > 0 else None

    raw_up = pos.get("unrealized_pnl")
    if raw_up is not None:
        unrealized_pnl = float(raw_up)
    elif mark_price is not None:
        signed = _signed_amount(pos)
        entry = float(pos.get("price", 0))
        unrealized_pnl = signed * (mark_price - entry)
    else:
        unrealized_pnl = None

    ot = find_open_trade(telegram_id, pid, network=network)
    lev: float | None = None
    if ot:
        try:
            lv = float(ot.get("leverage") or 0)
            if lv >= 1:
                lev = lv
        except (TypeError, ValueError):
            pass

    abs_amt = abs(float(pos.get("amount", 0)))
    entry = float(pos.get("price", 0) or 0)
    px = mark_price if (mark_price and mark_price > 0) else entry
    notional = abs_amt * max(px, 0.0)

    # Margin estimate: use DB leverage when the opening trade exists; else ~10x typical.
    _DEFAULT_LEV_EST = 10.0
    if lev is not None and notional > 0:
        margin = notional / lev
        leverage_out = lev
    elif notional > 0:
        margin = notional / _DEFAULT_LEV_EST
        leverage_out = None
    else:
        margin = None
        leverage_out = None

    return _build_position_response(
        pos,
        mark_price=mark_price,
        unrealized_pnl=unrealized_pnl,
        leverage=leverage_out,
        margin=margin,
    )


@router.get("/positions", response_model=list[PositionResponse])
async def list_positions(client: UserClient, user: AuthUser):
    """List all open positions."""
    positions = await run_blocking(client.get_all_positions) or []
    return [
        _enrich_single_position(client, p, user.telegram_id, user.network)
        for p in positions
    ]


@router.get("/portfolio", response_model=PortfolioSummary)
async def get_portfolio(client: UserClient, user: AuthUser):
    """Get portfolio summary: equity, balance, positions, margin (estimated)."""
    positions = await run_blocking(client.get_all_positions) or []
    balance = await run_blocking(client.get_balance) or {}

    balances = balance.get("balances", {})
    usdt_balance = float(balances.get(0, 0) or 0)

    pos_responses = [
        _enrich_single_position(client, p, user.telegram_id, user.network)
        for p in positions
    ]

    total_unrealized = 0.0
    for pr in pos_responses:
        if pr.unrealized_pnl is not None:
            total_unrealized += float(pr.unrealized_pnl)

    equity = usdt_balance + total_unrealized

    total_margin_used = 0.0
    for pr in pos_responses:
        if pr.margin is not None:
            total_margin_used += float(pr.margin)

    available = max(0.0, equity - total_margin_used)
    margin_util = None
    if equity > 1e-9:
        margin_util = min(1.0, max(0.0, total_margin_used / equity))

    open_orders: list = []
    try:
        open_orders = await run_blocking(client.get_all_open_orders, False) or []
    except Exception as e:
        logger.warning("get_all_open_orders failed: %s", e)

    return PortfolioSummary(
        equity=equity,
        balance_usd=usdt_balance,
        available_balance=available,
        total_unrealized_pnl=total_unrealized,
        unrealized_spot_pnl=0.0,
        total_margin_used=total_margin_used,
        margin_utilization=margin_util,
        total_volume_usd=float(user.total_volume_usd or 0),
        fee_tier_display="—",
        nlp_balance_usd=0.0,
        positions=pos_responses,
        open_orders_count=len(open_orders),
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
