"""Helpers for binding runtime state to the exact strategy session row."""

from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _coerce_session_id(value: Any) -> Optional[int]:
    try:
        sid = int(value)
    except (TypeError, ValueError):
        return None
    return sid if sid > 0 else None


def resolve_current_strategy_session(
    telegram_id: int,
    network: str,
    strategy: str,
    *,
    state: dict | None = None,
    status: dict | None = None,
) -> Optional[dict]:
    """Resolve the session row for the current strategy run.

    Prefer the session id persisted in runtime state/status because it uniquely
    identifies the run whose fills are tagged in ``trades_<network>``. Only when
    that id is absent or no longer matches this user/network/strategy do we fall
    back to the newest *same-strategy* running row.
    """
    from src.nadobro.models.database import (
        get_active_strategy_session_for_strategy,
        get_strategy_session_by_id,
    )

    state = state or {}
    status = status or {}
    sid = _coerce_session_id(
        state.get("strategy_session_id") or status.get("strategy_session_id")
    )
    if sid is not None:
        try:
            sess = get_strategy_session_by_id(
                sid,
                user_id=int(telegram_id),
                network=str(network),
                strategy=str(strategy),
            )
        except Exception:  # noqa: BLE001 - display/guard callers degrade safely
            logger.warning(
                "strategy session lookup failed sid=%s user=%s network=%s strategy=%s",
                sid, telegram_id, network, strategy,
                exc_info=True,
            )
            sess = None
        if sess:
            return sess

    try:
        return get_active_strategy_session_for_strategy(
            int(telegram_id), str(network), str(strategy)
        )
    except Exception:  # noqa: BLE001 - display/guard callers degrade safely
        logger.warning(
            "active strategy session lookup failed user=%s network=%s strategy=%s",
            telegram_id, network, strategy,
            exc_info=True,
        )
        return None
