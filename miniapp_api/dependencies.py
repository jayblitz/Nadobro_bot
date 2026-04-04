"""FastAPI dependency functions for the Mini App API.

These provide the authenticated user and a per-request NadoClient to route
handlers, following the same patterns as the main bot.
"""

import logging
from typing import Annotated, Optional

from fastapi import Depends, HTTPException, Request

from miniapp_api.auth import AuthError, TelegramUser, validate_init_data
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.user_service import get_or_create_user
from src.nadobro.services.nado_client import NadoClient
from src.nadobro.models.database import NetworkMode

logger = logging.getLogger(__name__)


class CurrentUser:
    """Combines the Telegram identity with the DB user row."""

    __slots__ = ("telegram_user", "db_user", "is_new")

    def __init__(self, telegram_user: TelegramUser, db_user: dict, is_new: bool):
        self.telegram_user = telegram_user
        self.db_user = db_user
        self.is_new = is_new

    @property
    def telegram_id(self) -> int:
        return self.telegram_user.id

    @property
    def username(self) -> str:
        return self.telegram_user.username

    @property
    def network(self) -> str:
        nm = self.db_user.get("network_mode")
        if isinstance(nm, NetworkMode):
            return nm.value
        return str(nm or "mainnet")

    @property
    def language(self) -> str:
        return self.db_user.get("language") or "en"

    @property
    def main_address(self) -> Optional[str]:
        return self.db_user.get("main_address")

    @property
    def tos_accepted(self) -> bool:
        settings = self.db_user.get("strategy_settings") or {}
        if isinstance(settings, str):
            import json
            try:
                settings = json.loads(settings)
            except Exception:
                settings = {}
        return bool(settings.get("tos_accepted", False))

    @property
    def total_trades(self) -> int:
        return int(self.db_user.get("total_trades") or 0)

    @property
    def total_volume_usd(self) -> float:
        return float(self.db_user.get("total_volume_usd") or 0)


def _extract_init_data(request: Request) -> str:
    """Pull initData from the ``Authorization: tma <initData>`` header."""
    auth = request.headers.get("Authorization", "")
    if not auth.lower().startswith("tma "):
        raise HTTPException(status_code=401, detail="Missing Authorization: tma <initData>")
    return auth[4:]


async def get_current_user(request: Request) -> CurrentUser:
    """Validate initData and resolve the DB user.

    This is the primary authentication dependency for all protected routes.
    """
    init_data = _extract_init_data(request)

    try:
        tg_user = validate_init_data(init_data)
    except AuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc))

    # Resolve or create the user in the DB (synchronous call).
    user_row, is_new, _err = await run_blocking(
        get_or_create_user, tg_user.id, tg_user.username
    )

    if user_row is None:
        raise HTTPException(status_code=500, detail="Failed to resolve user")

    # UserRow stores attributes directly (telegram_id, main_address, network_mode, etc.)
    # and keeps the raw dict in _data.  Prefer _data for dict access; fall back to vars().
    if hasattr(user_row, "_data") and isinstance(user_row._data, dict):
        db_user = dict(user_row._data)
        # UserRow normalises network_mode to a NetworkMode enum; keep that.
        db_user["network_mode"] = getattr(user_row, "network_mode", db_user.get("network_mode"))
        db_user["strategy_settings"] = getattr(user_row, "strategy_settings", db_user.get("strategy_settings"))
        db_user["total_trades"] = getattr(user_row, "total_trades", 0)
        db_user["total_volume_usd"] = getattr(user_row, "total_volume_usd", 0.0)
    elif isinstance(user_row, dict):
        db_user = user_row
    else:
        db_user = {
            k: getattr(user_row, k, None)
            for k in (
                "telegram_id", "telegram_username", "main_address",
                "linked_signer_address", "language", "network_mode",
                "strategy_settings", "total_trades", "total_volume_usd",
            )
        }

    return CurrentUser(tg_user, db_user, is_new)


async def get_user_nado_client(
    user: Annotated[CurrentUser, Depends(get_current_user)],
) -> NadoClient:
    """Return a NadoClient configured for the authenticated user's wallet and network."""
    if not user.main_address:
        raise HTTPException(
            status_code=400,
            detail="Wallet not linked. Complete onboarding first.",
        )

    from src.nadobro.services.user_service import get_user_nado_client as _get_client

    client = await run_blocking(_get_client, user.telegram_id, user.network)
    if client is None:
        raise HTTPException(status_code=500, detail="Failed to initialise exchange client")
    return client


# Convenient type aliases for route signatures.
AuthUser = Annotated[CurrentUser, Depends(get_current_user)]
UserClient = Annotated[NadoClient, Depends(get_user_nado_client)]
