"""Phase C: WS-driven order lifecycle store (Redis-backed, cross-process).

The subscriptions ``order_update`` / ``fill`` streams (Phase A) tell us, in real
time, when an order is *placed*, *filled*, *partially filled*, or *cancelled*.
This module records that lifecycle per order so the engine can stop polling the
gateway for order status on every tick.

Design (deliberately conservative — "make no mistakes"):

  * The store records **state + a monotonic change sequence + freshness**. It
    does NOT compute filled quote/fee from the stream (the fill event's field
    scales are not something we want to guess at): authoritative amounts always
    come from the REST matches feed.
  * ``NadoAdapter.order_status`` uses this purely to decide *whether* it needs
    to hit the gateway:
      - a **terminal** state (filled/cancelled/rejected) is permanent, so a
        cached terminal snapshot can be returned forever with no gateway call;
      - while an order is **fresh** (touched by a WS event recently) and its
        change-seq hasn't advanced since the adapter last reconciled, the cached
        snapshot is returned with no gateway call;
      - otherwise the adapter falls back to the REST status path, which remains
        the source of truth.

Process-local store:
  Lifecycle state lives in an in-process ``OrderedDict`` (``_store``). A prior
  version mirrored every mutation to Upstash Redis so worker-pool executors
  could read the main process's WS-driven state; that cross-process mirror was
  removed (single-machine deployment). A worker that misses the local store
  falls back to the REST status path, which remains the source of truth.
"""
from __future__ import annotations

import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Optional

from src.nadobro.utils.env import env_float
from src.nadobro.engine import order_tags
from src.nadobro.engine.adapter.base import OrderState

_TRUST_TTL_SECONDS = env_float("NADO_WS_LIFECYCLE_TTL_SECONDS", 8.0)
_MAX_ENTRIES = 8192

_lock = threading.RLock()
_store: "OrderedDict[str, _Entry]" = OrderedDict()

# reason -> state for order_update events.
_REASON_STATE = {
    "placed": OrderState.OPEN,
    "open": OrderState.OPEN,
    "filled": OrderState.FILLED,
    "cancelled": OrderState.CANCELLED,
    "canceled": OrderState.CANCELLED,
    "rejected": OrderState.REJECTED,
}


@dataclass
class _Entry:
    digest: str
    state: OrderState = OrderState.OPEN
    seq: int = 0
    last_ws_event_ts: float = 0.0  # 0 = never touched by a real WS event
    tag: Optional[int] = None
    fresh: bool = False  # transient: computed on read, not part of stored state


def _copy(e: _Entry) -> _Entry:
    return _Entry(digest=e.digest, state=e.state, seq=e.seq,
                  last_ws_event_ts=e.last_ws_event_ts, tag=e.tag)


# ---- write path -----------------------------------------------------------

def _evict_if_needed() -> None:
    while len(_store) > _MAX_ENTRIES:
        _store.popitem(last=False)


def seed(digest: str, *, state: OrderState = OrderState.OPEN, tag: Optional[int] = None) -> None:
    """Create an entry when an order is placed. Written ``fresh=False`` — it is a
    seq baseline only, NOT a reason to skip polling (keeps the no-WS path on
    pure REST)."""
    if not digest:
        return
    with _lock:
        entry = _store.get(digest)
        if entry is None:
            entry = _Entry(digest=str(digest), state=state, tag=tag)
            _store[digest] = entry
        else:
            entry.state = state
            if tag is not None:
                entry.tag = tag
        _store.move_to_end(digest)
        _evict_if_needed()


def _touch_locked(digest: str, *, state: Optional[OrderState], tag: Optional[int]) -> _Entry:
    entry = _store.get(digest)
    if entry is None:
        entry = _Entry(digest=str(digest), tag=tag)
        _store[digest] = entry
    entry.seq += 1
    entry.last_ws_event_ts = time.time()
    if tag is not None:
        entry.tag = tag
    if state is not None and not entry.state.is_terminal:
        # Never regress out of a terminal state (a late event can't un-fill).
        entry.state = state
    _store.move_to_end(digest)
    _evict_if_needed()
    return _copy(entry)


def apply_order_update(*, digest: Optional[str], reason: Optional[str], tag: Optional[int] = None) -> None:
    """Handle an ``order_update`` stream event."""
    if not digest and tag is not None:
        meta = order_tags.resolve_tag(tag)
        digest = (meta or {}).get("digest")
    if not digest:
        return
    state = _REASON_STATE.get(str(reason or "").strip().lower())
    with _lock:
        _touch_locked(str(digest), state=state, tag=tag)
    if tag is not None:
        order_tags.bind_digest(tag, str(digest))


def apply_fill(*, tag: Optional[int] = None, digest: Optional[str] = None) -> None:
    """Handle a ``fill`` stream event. Fills carry only ``id`` (our tag), so we
    resolve the digest via the tag registry. A fill marks the order at least
    partially filled (a following ``order_update`` reason=filled finalises it)."""
    if not digest and tag is not None:
        meta = order_tags.resolve_tag(tag)
        digest = (meta or {}).get("digest")
    if not digest:
        return
    with _lock:
        entry = _store.get(str(digest))
        next_state = OrderState.PARTIALLY_FILLED
        if entry is not None and entry.state.is_terminal:
            next_state = entry.state
        _touch_locked(str(digest), state=next_state, tag=tag)


# ---- read path ------------------------------------------------------------

def get(digest: Optional[str]) -> Optional[_Entry]:
    """Return the known lifecycle entry with ``.fresh`` computed, or ``None``."""
    if not digest:
        return None
    now = time.time()
    with _lock:
        local = _store.get(str(digest))
        local_copy = _copy(local) if local is not None else None
    if local_copy is not None:
        local_copy.fresh = local_copy.state.is_terminal or (
            local_copy.last_ws_event_ts > 0
            and (now - local_copy.last_ws_event_ts) <= _TRUST_TTL_SECONDS
        )
    return local_copy


def seq(digest: Optional[str]) -> int:
    e = get(digest)
    return e.seq if e else -1


def is_fresh(digest: Optional[str], *, ttl: float = _TRUST_TTL_SECONDS, now: Optional[float] = None) -> bool:
    """Local-only freshness check (used by tests / same-process callers).
    Cross-process callers should use ``get(...).fresh``."""
    with _lock:
        e = _store.get(str(digest)) if digest else None
        ts = e.last_ws_event_ts if e else 0.0
    if ts <= 0:
        return False
    return ((now or time.time()) - ts) <= ttl


def forget(digest: Optional[str]) -> None:
    if not digest:
        return
    with _lock:
        _store.pop(str(digest), None)


def clear() -> None:
    with _lock:
        _store.clear()


def stats() -> dict:
    with _lock:
        terminal = sum(1 for e in _store.values() if e.state.is_terminal)
        return {"tracked": len(_store), "terminal": terminal}
