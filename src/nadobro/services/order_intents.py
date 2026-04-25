"""Idempotent order intent tracking.

Nado returns digests after submission, but retries can happen before archive
fills resolve. This module stores a durable intent envelope in bot_state so
strategy/runtime code can identify duplicate submits and surface recovery state.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any

from src.nadobro.models.database import get_bot_state, set_bot_state

ORDER_INTENT_PREFIX = "order_intent:"


def build_intent_id(
    *,
    user_id: int,
    network: str,
    strategy_session_id: int | None,
    source: str,
    product: str,
    side: str,
    size: float,
    reduce_only: bool = False,
) -> str:
    payload = {
        "user_id": int(user_id),
        "network": network,
        "strategy_session_id": strategy_session_id,
        "source": source,
        "product": product.upper(),
        "side": side.lower(),
        "size": round(float(size), 10),
        "reduce_only": bool(reduce_only),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:24]


def _key(intent_id: str) -> str:
    return f"{ORDER_INTENT_PREFIX}{intent_id}"


def get_order_intent(intent_id: str) -> dict[str, Any] | None:
    return get_bot_state(_key(intent_id))


def create_order_intent(intent_id: str, data: dict[str, Any]) -> dict[str, Any]:
    now = time.time()
    existing = get_order_intent(intent_id)
    if existing and existing.get("status") in {"submitted", "filled", "pending"}:
        return existing
    payload = dict(data)
    payload.update({
        "intent_id": intent_id,
        "status": "pending",
        "created_at_ts": now,
        "updated_at_ts": now,
    })
    set_bot_state(_key(intent_id), payload)
    return payload


def update_order_intent(intent_id: str, **updates) -> dict[str, Any]:
    payload = get_order_intent(intent_id) or {"intent_id": intent_id}
    payload.update(updates)
    payload["updated_at_ts"] = time.time()
    set_bot_state(_key(intent_id), payload)
    return payload


def should_skip_duplicate(intent_id: str, max_age_seconds: int = 120) -> tuple[bool, dict[str, Any] | None]:
    existing = get_order_intent(intent_id)
    if not existing:
        return False, None
    age = time.time() - float(existing.get("updated_at_ts") or existing.get("created_at_ts") or 0)
    if age <= max_age_seconds and existing.get("status") in {"pending", "submitted", "filled"}:
        return True, existing
    return False, existing
