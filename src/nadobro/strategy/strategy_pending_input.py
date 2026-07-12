"""Persist strategy free-text prompts so config survives multi-worker webhook and restarts."""

import logging
import time

from src.nadobro.db import execute
from src.nadobro.models.database import get_bot_state, set_bot_state

logger = logging.getLogger(__name__)

STRATEGY_PENDING_TTL_SECONDS = 3600


def _key(telegram_user_id: int) -> str:
    return f"strategy_pending_input:{int(telegram_user_id)}"


def persist_strategy_pending_input(telegram_user_id: int, payload: dict) -> None:
    """Merge strategy | field | section into bot_state."""
    blob = {
        "strategy": str(payload.get("strategy", "")).strip(),
        "field": str(payload.get("field", "")).strip(),
        "section": payload.get("section"),
        "_ts": time.time(),
    }
    if not blob["strategy"] or not blob["field"]:
        logger.warning("Refusing to persist empty strategy_pending_input uid=%s", telegram_user_id)
        return
    set_bot_state(_key(telegram_user_id), blob)


def load_strategy_pending_input(telegram_user_id: int) -> dict | None:
    row = get_bot_state(_key(telegram_user_id))
    if not row or not isinstance(row, dict):
        return None
    ts = float(row.get("_ts") or row.get("ts") or 0)
    if ts and time.time() - ts > STRATEGY_PENDING_TTL_SECONDS:
        clear_strategy_pending_input(telegram_user_id)
        return None
    return {
        "strategy": row.get("strategy"),
        "field": row.get("field"),
        "section": row.get("section"),
    }


def clear_strategy_pending_input(telegram_user_id: int) -> None:
    execute("DELETE FROM bot_state WHERE key = %s", (_key(telegram_user_id),))
