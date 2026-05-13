"""Persist pending text-trade confirmations across worker restarts and webhook hops.

`context.user_data` lives in-process, so a redeploy or a second machine routing
the user's `confirm`/`cancel` reply will lose the trade preview state and the
message silently falls through to the AI chat handler. Mirror the strategy-lab
pattern: keep user_data as the hot cache, but persist the payload into
`bot_state` so we can hydrate on the next message.
"""

from __future__ import annotations

import logging
import time

from src.nadobro.db import execute
from src.nadobro.models.database import get_bot_state, set_bot_state

logger = logging.getLogger(__name__)

TEXT_TRADE_PENDING_TTL_SECONDS = 600
TEXT_CLOSE_ALL_PENDING_TTL_SECONDS = 600


def _trade_key(telegram_user_id: int) -> str:
    return f"text_trade_pending:{int(telegram_user_id)}"


def _close_all_key(telegram_user_id: int) -> str:
    return f"text_close_all_pending:{int(telegram_user_id)}"


def persist_text_trade_pending(telegram_user_id: int, payload: dict) -> None:
    if not payload:
        return
    blob = dict(payload)
    blob["_ts"] = time.time()
    set_bot_state(_trade_key(telegram_user_id), blob)


def load_text_trade_pending(telegram_user_id: int) -> dict | None:
    row = get_bot_state(_trade_key(telegram_user_id))
    if not row or not isinstance(row, dict):
        return None
    ts = float(row.get("_ts") or 0)
    if ts and time.time() - ts > TEXT_TRADE_PENDING_TTL_SECONDS:
        clear_text_trade_pending(telegram_user_id)
        return None
    payload = {k: v for k, v in row.items() if k != "_ts"}
    return payload or None


def clear_text_trade_pending(telegram_user_id: int) -> None:
    try:
        execute("DELETE FROM bot_state WHERE key = %s", (_trade_key(telegram_user_id),))
    except Exception:
        logger.exception("Failed to clear text_trade_pending for uid=%s", telegram_user_id)


def persist_text_close_all_pending(telegram_user_id: int) -> None:
    set_bot_state(_close_all_key(telegram_user_id), {"_ts": time.time()})


def load_text_close_all_pending(telegram_user_id: int) -> bool:
    row = get_bot_state(_close_all_key(telegram_user_id))
    if not row or not isinstance(row, dict):
        return False
    ts = float(row.get("_ts") or 0)
    if ts and time.time() - ts > TEXT_CLOSE_ALL_PENDING_TTL_SECONDS:
        clear_text_close_all_pending(telegram_user_id)
        return False
    return True


def clear_text_close_all_pending(telegram_user_id: int) -> None:
    try:
        execute("DELETE FROM bot_state WHERE key = %s", (_close_all_key(telegram_user_id),))
    except Exception:
        logger.exception("Failed to clear text_close_all_pending for uid=%s", telegram_user_id)
