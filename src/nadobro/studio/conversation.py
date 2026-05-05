"""Persistence helpers for Strategy Studio conversations."""

from __future__ import annotations

import json
from enum import StrEnum

from src.nadobro.models.database import (
    cancel_active_studio_sessions,
    get_active_studio_session,
    get_studio_session,
    insert_studio_session,
    update_studio_session,
)
from src.nadobro.studio.intent import TradingIntent, intent_from_json


class StudioState(StrEnum):
    EXTRACTING = "EXTRACTING"
    CLARIFYING = "CLARIFYING"
    CONFIRMING = "CONFIRMING"
    EXECUTING = "EXECUTING"
    DONE = "DONE"
    CANCELLED = "CANCELLED"


def _decode_json(value, default):
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def start_session(telegram_id: int, network: str, raw: str | None = None) -> dict:
    cancel_active_studio_sessions(telegram_id, network)
    history = [{"role": "user", "content": raw}] if raw else []
    session_id = insert_studio_session(
        {
            "telegram_id": telegram_id,
            "network": network,
            "state": StudioState.EXTRACTING.value,
            "intent_json": {},
            "history_json": history,
        }
    )
    return get_studio_session(int(session_id)) if session_id else {}


def active_session(telegram_id: int, network: str) -> dict | None:
    return get_active_studio_session(telegram_id, network)


def abandon_active_studio_sessions(telegram_id: int, network: str) -> None:
    """Cancel in-progress studio rows for this user/network (memory lost, home nav, etc.)."""
    cancel_active_studio_sessions(telegram_id, network)


def get_studio_session_row(session_id: int) -> dict | None:
    return get_studio_session(session_id)


def load_intent(row: dict) -> TradingIntent | None:
    payload = _decode_json(row.get("intent_json"), {})
    if not payload:
        return None
    return intent_from_json(payload, raw_fallback=payload.get("raw_input", ""))


def load_history(row: dict) -> list[dict]:
    history = _decode_json(row.get("history_json"), [])
    return history if isinstance(history, list) else []


def save_turn(session_id: int, state: StudioState, intent: TradingIntent | None, history: list[dict], strategy_session_id: int | None = None):
    payload: dict = {
        "state": state.value,
        "history_json": history,
    }
    if intent is not None:
        payload["intent_json"] = intent.dict()
    if strategy_session_id is not None:
        payload["strategy_session_id"] = strategy_session_id
    update_studio_session(session_id, payload)
