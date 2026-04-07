"""Strategy bot control — same runtime as the Telegram bot."""

import logging

from fastapi import APIRouter, HTTPException

from miniapp_api.dependencies import AuthUser
from miniapp_api.models.schemas import StrategyActionResponse, StrategyStartRequest
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.bot_runtime import get_user_bot_status, start_user_bot, stop_user_bot
from src.nadobro.services.onboarding_service import is_new_onboarding_complete
from src.nadobro.services.user_service import ensure_active_wallet_ready

logger = logging.getLogger(__name__)

router = APIRouter()


def _require_trading_ready(telegram_id: int) -> None:
    if not is_new_onboarding_complete(telegram_id):
        raise HTTPException(status_code=403, detail="Complete onboarding and accept Terms of Service first.")
    ok, msg = ensure_active_wallet_ready(telegram_id)
    if not ok:
        raise HTTPException(status_code=400, detail=msg or "Wallet not ready for trading.")


@router.get("/strategies/status")
async def strategies_status(user: AuthUser):
    """Return live strategy bot state for the user's current network."""
    return await run_blocking(get_user_bot_status, user.telegram_id)


@router.post("/strategies/start", response_model=StrategyActionResponse)
async def strategies_start(body: StrategyStartRequest, user: AuthUser):
    _require_trading_ready(user.telegram_id)

    product = body.product.strip().upper()
    if body.strategy == "bro":
        product = "MULTI"

    ok, message = await run_blocking(
        start_user_bot,
        user.telegram_id,
        body.strategy,
        product,
        body.leverage,
        body.slippage_pct,
        direction=body.direction,
    )
    if not ok:
        raise HTTPException(status_code=400, detail=message)
    return StrategyActionResponse(ok=True, message=message)


@router.post("/strategies/stop", response_model=StrategyActionResponse)
async def strategies_stop(user: AuthUser):
    _require_trading_ready(user.telegram_id)

    ok, message = await run_blocking(stop_user_bot, user.telegram_id, True)
    if not ok:
        raise HTTPException(status_code=400, detail=message)
    return StrategyActionResponse(ok=True, message=message)
