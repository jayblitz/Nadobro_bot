"""Shared trading readiness gates for Telegram, Mini App, and agents."""

from __future__ import annotations

from dataclasses import dataclass

from src.nadobro.services.admin_service import is_trading_paused
from src.nadobro.services.onboarding_service import is_new_onboarding_complete
from src.nadobro.services.user_service import ensure_active_wallet_ready


@dataclass(frozen=True)
class TradingReadiness:
    ok: bool
    reason: str = ""
    code: str = ""


def check_trading_readiness(
    telegram_id: int,
    *,
    require_onboarding: bool = True,
    require_wallet: bool = True,
    block_when_paused: bool = True,
) -> TradingReadiness:
    """Return the first user-actionable reason trading cannot proceed."""
    if block_when_paused and is_trading_paused():
        return TradingReadiness(False, "Trading is temporarily paused by admin controls.", "trading_paused")
    if require_onboarding and not is_new_onboarding_complete(telegram_id):
        return TradingReadiness(
            False,
            "Complete onboarding and accept Terms of Service first.",
            "onboarding_incomplete",
        )
    if require_wallet:
        wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
        if not wallet_ready:
            return TradingReadiness(False, wallet_msg or "Wallet not ready for trading.", "wallet_not_ready")
    return TradingReadiness(True)
