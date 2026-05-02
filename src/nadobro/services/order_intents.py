"""Idempotent order intent tracking.

Nado returns digests after submission, but retries can happen before archive
fills resolve. This module stores a durable intent envelope in Postgres so
strategy/runtime code can atomically identify duplicate submits and surface
recovery state.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any

from src.nadobro.models.database import (
    get_bot_state,
    get_order_intent_row,
    reserve_order_intent_row,
    set_bot_state,
    update_order_intent_row,
)

ORDER_INTENT_PREFIX = "order_intent:"
ACTIVE_STATUSES = {"pending", "recorded", "submitted", "filled"}


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
    price: float | None = None,
    order_nonce: str | None = None,
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
    if price is not None:
        payload["price"] = round(float(price), 10)
    if order_nonce:
        payload["order_nonce"] = str(order_nonce)
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:24]


def _key(intent_id: str) -> str:
    return f"{ORDER_INTENT_PREFIX}{intent_id}"


def get_order_intent(intent_id: str) -> dict[str, Any] | None:
    try:
        row = get_order_intent_row(intent_id)
    except Exception:
        row = None
    if row:
        return row
    return get_bot_state(_key(intent_id))


def reserve_order_intent(
    intent_id: str,
    data: dict[str, Any],
    max_age_seconds: int = 120,
) -> tuple[bool, dict[str, Any]]:
    """Reserve an intent atomically.

    Returns (reserved, payload). reserved=False means another active submit owns
    the same intent id and the caller must not submit a second order.
    """
    now = time.time()
    payload = dict(data)
    payload.update({
        "intent_id": intent_id,
        "status": "pending",
        "created_at_ts": now,
        "updated_at_ts": now,
    })
    try:
        row = reserve_order_intent_row(intent_id, payload, stale_after_seconds=max_age_seconds)
    except Exception:
        existing = get_bot_state(_key(intent_id))
        if existing:
            age = time.time() - float(existing.get("updated_at_ts") or existing.get("created_at_ts") or 0)
            if age <= max_age_seconds and existing.get("status") in ACTIVE_STATUSES:
                return False, existing
        set_bot_state(_key(intent_id), payload)
        return True, payload
    if row:
        value = row.get("value") or {}
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except Exception:
                value = payload
        reserved = dict(value)
        reserved.setdefault("intent_id", row.get("intent_id") or intent_id)
        reserved["status"] = row.get("status") or reserved.get("status")
        return True, reserved
    existing = get_order_intent(intent_id) or {"intent_id": intent_id}
    return False, existing


def create_order_intent(intent_id: str, data: dict[str, Any]) -> dict[str, Any]:
    reserved, payload = reserve_order_intent(intent_id, data)
    if reserved:
        return payload
    if payload.get("status") in ACTIVE_STATUSES:
        return payload
    now = time.time()
    payload.update({"status": "pending", "updated_at_ts": now})
    set_bot_state(_key(intent_id), payload)
    return payload


def update_order_intent(intent_id: str, **updates) -> dict[str, Any]:
    payload = get_order_intent(intent_id) or {"intent_id": intent_id}
    payload.update(updates)
    payload["updated_at_ts"] = time.time()
    try:
        row = update_order_intent_row(intent_id, payload)
    except Exception:
        row = None
    if not row:
        set_bot_state(_key(intent_id), payload)
    return payload


def should_skip_duplicate(intent_id: str, max_age_seconds: int = 120) -> tuple[bool, dict[str, Any] | None]:
    existing = get_order_intent(intent_id)
    if not existing:
        return False, None
    age = time.time() - float(existing.get("updated_at_ts") or existing.get("created_at_ts") or 0)
    if age <= max_age_seconds and existing.get("status") in ACTIVE_STATUSES:
        return True, existing
    return False, existing
