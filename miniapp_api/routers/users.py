"""User routes — profile, settings, network switching."""

import json
import logging

from fastapi import APIRouter

from miniapp_api.dependencies import AuthUser
from miniapp_api.models.schemas import (
    OkResponse,
    SwitchNetworkRequest,
    UpdateSettingsRequest,
    UserResponse,
)
from src.nadobro.services.async_utils import run_blocking

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/me", response_model=UserResponse)
async def get_me(user: AuthUser):
    """Return the authenticated user's profile."""
    return UserResponse(
        telegram_id=user.telegram_id,
        username=user.username,
        language=user.language,
        network=user.network,
        main_address=user.main_address,
        tos_accepted=user.tos_accepted,
        is_new=user.is_new,
        total_trades=user.total_trades,
        total_volume_usd=user.total_volume_usd,
    )


@router.patch("/me/settings", response_model=OkResponse)
async def update_settings(body: UpdateSettingsRequest, user: AuthUser):
    """Update user preferences (language, tos_accepted)."""
    from src.nadobro.db import execute, query_one

    if body.language:
        await run_blocking(
            execute,
            "UPDATE users SET language = %s WHERE telegram_id = %s",
            (body.language, user.telegram_id),
        )

    if body.tos_accepted is not None:
        # tos_accepted lives in strategy_settings JSON column.
        row = await run_blocking(
            query_one,
            "SELECT strategy_settings FROM users WHERE telegram_id = %s",
            (user.telegram_id,),
        )
        settings = {}
        if row and row.get("strategy_settings"):
            raw = row["strategy_settings"]
            settings = json.loads(raw) if isinstance(raw, str) else (raw or {})
        settings["tos_accepted"] = body.tos_accepted
        await run_blocking(
            execute,
            "UPDATE users SET strategy_settings = %s WHERE telegram_id = %s",
            (json.dumps(settings), user.telegram_id),
        )

    from src.nadobro.services.user_service import invalidate_user_cache
    invalidate_user_cache(user.telegram_id)

    return OkResponse(message="Settings updated")


@router.post("/me/network", response_model=OkResponse)
async def switch_network(body: SwitchNetworkRequest, user: AuthUser):
    """Switch the user between testnet and mainnet."""
    from src.nadobro.services.user_service import switch_network as _switch

    success, msg = await run_blocking(_switch, user.telegram_id, body.network)
    if not success:
        return OkResponse(ok=False, message=msg)
    return OkResponse(message=msg)
