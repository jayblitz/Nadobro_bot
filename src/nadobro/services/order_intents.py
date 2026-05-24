"""Patched: services/order_intents.py

Fixes applied (search for AUDIT-FIX):
  AUDIT-FIX-OI-1: The previous fallback path (when the DB-backed
                  reserve_order_intent_row raised) used
                  get_bot_state + set_bot_state, which is *not* atomic. Two
                  concurrent submits during a DB blip could both observe "no
                  existing intent", both write their own payload, and both
                  return reserved=True — meaning the bot would send the same
                  order to the venue twice. For a live trading system this is
                  a fund-safety risk (double size, double fees, drift between
                  what the user asked for and what they got). The fix is to
                  treat a DB failure as a hard reservation failure: callers
                  must wait and retry rather than fall back to a racy local
                  path. The original cache-only behavior is still available
                  via reserve_order_intent_unsafe_cache_only() for tests and
                  documented dev usage only.
  AUDIT-FIX-OI-2: reserve_order_intent now defends against the DB path
                  returning a row whose status is already an ACTIVE_STATUS
                  (e.g. 'submitted' or 'filled'). Even though the SQL WHERE
                  clause is supposed to prevent that, a stale_after_seconds
                  expiry can let an UPDATE through against a row that is
                  still active by business logic. We refuse to claim it.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from typing import Any

from src.nadobro.models.database import (
    get_bot_state,
    get_order_intent_row,
    reserve_order_intent_row,
    set_bot_state,
    update_order_intent_row,
)

logger = logging.getLogger(__name__)

ORDER_INTENT_PREFIX = "order_intent:"
ACTIVE_STATUSES = {"pending", "recorded", "submitted", "filled"}

# AUDIT-FIX-OI-1: a local lock prevents the in-process race when two coroutines
# call reserve_order_intent_unsafe_cache_only() with the same id. This does
# NOT solve the cross-process race — see the docstring on
# reserve_order_intent_unsafe_cache_only.
_cache_lock = threading.Lock()


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


class OrderIntentReservationError(RuntimeError):
    """Raised when the DB-backed reservation path cannot be used and the
    caller must NOT proceed with submitting the order. Wrapping callers
    should surface a user-visible "please retry" rather than guessing."""


def reserve_order_intent(
    intent_id: str,
    data: dict[str, Any],
    max_age_seconds: int = 120,
) -> tuple[bool, dict[str, Any]]:
    """Reserve an intent atomically using the Postgres row.

    Returns ``(reserved, payload)``. ``reserved=False`` means another active
    submit owns the same intent id — the caller must NOT submit a second
    order. On DB failure this raises :class:`OrderIntentReservationError`
    rather than falling back to a non-atomic cache path (the previous
    fallback could allow double-submission during a partial DB outage).
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
    except Exception as exc:
        # AUDIT-FIX-OI-1: do NOT fall back to bot_state. The cache fallback
        # was the historical behavior but it is not atomic across processes,
        # so during a DB hiccup it can yield two "reserved" results to two
        # concurrent submits — the bot would then place two orders for the
        # same intent. Fund safety > availability here.
        logger.error(
            "reserve_order_intent: DB reservation failed for intent %s: %s "
            "(refusing to fall back to non-atomic cache)",
            intent_id, exc,
        )
        raise OrderIntentReservationError(str(exc)) from exc

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

        # AUDIT-FIX-OI-2: if our INSERT/UPDATE somehow returned a row whose
        # status is already ACTIVE for some other reason (a race we missed
        # in the SQL guard), refuse to treat that as a fresh reservation.
        if str(reserved.get("status") or "").lower() in ACTIVE_STATUSES and (
            reserved.get("created_at_ts") != payload["created_at_ts"]
        ):
            return False, reserved
        return True, reserved

    existing = get_order_intent(intent_id) or {"intent_id": intent_id}
    return False, existing


def reserve_order_intent_unsafe_cache_only(
    intent_id: str,
    data: dict[str, Any],
    max_age_seconds: int = 120,
) -> tuple[bool, dict[str, Any]]:
    """Local-cache-only reservation (NOT cross-process safe).

    This function exists for test harnesses and dev sandboxes where the
    Postgres backing store isn't available. In production it MUST NOT be
    used because two concurrent calls in different worker processes can
    both observe "free" and both return reserved=True, leading to a
    double-submitted order. Use :func:`reserve_order_intent` instead.
    """
    now = time.time()
    payload = dict(data)
    payload.update({
        "intent_id": intent_id,
        "status": "pending",
        "created_at_ts": now,
        "updated_at_ts": now,
    })
    with _cache_lock:
        existing = get_bot_state(_key(intent_id))
        if existing:
            age = time.time() - float(existing.get("updated_at_ts") or existing.get("created_at_ts") or 0)
            if age <= max_age_seconds and existing.get("status") in ACTIVE_STATUSES:
                return False, existing
        set_bot_state(_key(intent_id), payload)
        return True, payload


def create_order_intent(intent_id: str, data: dict[str, Any]) -> dict[str, Any]:
    """Best-effort intent recorder.

    Unlike :func:`reserve_order_intent` (which protects the submit path and
    must refuse when the DB is down), this helper is used by non-critical
    bookkeeping paths and may fall back to bot_state if the DB is
    unavailable. For tracking / duplicate-detection only — never as the
    sole guard before placing an order.
    """
    try:
        reserved, payload = reserve_order_intent(intent_id, data)
    except OrderIntentReservationError:
        # AUDIT-FIX-OI-1: callers going through create_order_intent are
        # best-effort book-keepers. Fall back to the cache-only path so we
        # still record SOMETHING when the DB is down. The submit guard
        # remains strict via reserve_order_intent.
        reserved, payload = reserve_order_intent_unsafe_cache_only(intent_id, data)
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
