"""Strategy bot control — same runtime as the Telegram bot."""

import logging

from fastapi import APIRouter, HTTPException

from miniapp_api.dependencies import AuthUser
from miniapp_api.models.schemas import StrategyActionResponse, StrategyStartRequest
from src.nadobro.config import list_volume_spot_product_names
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.bot_runtime import get_user_bot_status, start_user_bot, stop_user_bot
from src.nadobro.services.trading_readiness import check_trading_readiness
from src.nadobro.services.user_service import get_user, get_user_readonly_client

logger = logging.getLogger(__name__)

router = APIRouter()


def _require_trading_ready(telegram_id: int) -> None:
    readiness = check_trading_readiness(telegram_id)
    if readiness.ok:
        return
    status = 403 if readiness.code in ("trading_paused", "onboarding_incomplete") else 400
    raise HTTPException(status_code=status, detail=readiness.reason)


@router.get("/strategies/status")
async def strategies_status(user: AuthUser):
    """Return live strategy bot state for the user's current network."""
    return await run_blocking(get_user_bot_status, user.telegram_id)


@router.get("/strategies/volume-spot-products")
async def strategies_volume_spot_products(user: AuthUser):
    """KBTC/WETH/USDC spot symbols that resolve on the user's current network (for Volume spot UI)."""
    u = await run_blocking(get_user, user.telegram_id)
    network = u.network_mode.value if u else "mainnet"
    client = await run_blocking(get_user_readonly_client, user.telegram_id, network=network)
    names = await run_blocking(list_volume_spot_product_names, network, client)
    return {"names": list(names or [])}


@router.post("/strategies/start", response_model=StrategyActionResponse)
async def strategies_start(body: StrategyStartRequest, user: AuthUser):
    _require_trading_ready(user.telegram_id)

    product = body.product.strip().upper()
    if body.strategy == "bro":
        product = "MULTI"

    start_kw = {"direction": body.direction}
    if body.strategy == "vol":
        start_kw["vol_market"] = body.vol_market

    ok, message = await run_blocking(
        start_user_bot,
        user.telegram_id,
        body.strategy,
        product,
        body.leverage,
        body.slippage_pct,
        **start_kw,
    )
    if not ok:
        raise HTTPException(status_code=400, detail=message)
    return StrategyActionResponse(ok=True, message=message)


@router.post("/strategies/stop", response_model=StrategyActionResponse)
async def strategies_stop(user: AuthUser):
    ok, message = await run_blocking(stop_user_bot, user.telegram_id, True)
    if not ok:
        raise HTTPException(status_code=400, detail=message)
    return StrategyActionResponse(ok=True, message=message)
