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

Cross-process (Redis):
  The WS client runs in the main process, but MM executors run in worker pools.
  Every lifecycle mutation is mirrored to Redis (Upstash) with a short TTL, so a
  worker's ``order_status`` can read the main process's WS-driven state instead
  of polling the Nado gateway. Only genuine WS events are written ``fresh=True``;
  the placement ``seed`` writes ``fresh=False`` (a seq baseline only), so a
  worker still REST-polls until the WS actually has something to say — i.e. the
  optimisation can never *invent* freshness, only propagate it. Redis is
  best-effort: if it's unconfigured or down, behaviour degrades to per-process
  REST polling (identical to pre-Phase-C). Presence in Redis implies "within
  TTL", which sidesteps cross-machine clock skew.
"""
from __future__ import annotations

import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Optional

from src.nadobro.engine import order_tags
from src.nadobro.engine.adapter.base import OrderState

_TRUST_TTL_SECONDS = float(os.environ.get("NADO_WS_LIFECYCLE_TTL_SECONDS", "8"))
_TERMINAL_TTL_SECONDS = int(os.environ.get("NADO_WS_LIFECYCLE_TERMINAL_TTL_SECONDS", "120"))
_REDIS_ENABLED = os.environ.get("NADO_WS_LIFECYCLE_REDIS", "1").strip().lower() in ("1", "true", "yes", "on")
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


# ---- Redis mirror (cross-process) ----------------------------------------

def _redis_key(digest: str) -> str:
    return f"olc:{digest}"


def _write_redis(entry: _Entry, *, fresh: bool) -> None:
    if not _REDIS_ENABLED:
        return
    try:
        from src.nadobro.services.upstash_redis import get_redis

        r = get_redis()
        if not r.enabled:
            return
        ttl = _TERMINAL_TTL_SECONDS if entry.state.is_terminal else max(1, int(round(_TRUST_TTL_SECONDS)))
        r.set_json(
            _redis_key(entry.digest),
            {"state": entry.state.value, "seq": int(entry.seq),
             "tag": entry.tag, "fresh": bool(fresh)},
            ttl_seconds=ttl,
        )
    except Exception:  # noqa: BLE001 - mirror is best-effort
        pass


def _read_redis(digest: str, *, now: float) -> Optional[_Entry]:
    if not _REDIS_ENABLED:
        return None
    try:
        from src.nadobro.services.upstash_redis import get_redis

        payload = get_redis().get_json(_redis_key(digest))
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(payload, dict):
        return None
    try:
        state = OrderState(payload.get("state"))
    except Exception:  # noqa: BLE001
        state = OrderState.OPEN
    # Presence in Redis ⇒ within TTL. Terminal is always trustworthy; otherwise
    # honour the ``fresh`` flag the writer set (only real WS events are fresh).
    is_fresh_flag = bool(payload.get("fresh")) or state.is_terminal
    return _Entry(
        digest=digest, state=state, seq=int(payload.get("seq") or 0),
        last_ws_event_ts=now if is_fresh_flag else 0.0,
        tag=payload.get("tag"), fresh=is_fresh_flag,
    )


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
        snapshot = _copy(entry)
    _write_redis(snapshot, fresh=False)


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
        snapshot = _touch_locked(str(digest), state=state, tag=tag)
    if tag is not None:
        order_tags.bind_digest(tag, str(digest))
    _write_redis(snapshot, fresh=True)


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
        snapshot = _touch_locked(str(digest), state=next_state, tag=tag)
    _write_redis(snapshot, fresh=True)


# ---- read path ------------------------------------------------------------

def get(digest: Optional[str]) -> Optional[_Entry]:
    """Return the freshest known lifecycle entry (local or Redis), with
    ``.fresh`` computed. ``None`` if nothing is known anywhere."""
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
        if local_copy.fresh:
            return local_copy
    # Local missing or stale → consult the cross-process Redis mirror.
    remote = _read_redis(str(digest), now=now)
    if remote is not None and (local_copy is None or remote.seq >= local_copy.seq):
        return remote
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
    if _REDIS_ENABLED:
        try:
            from src.nadobro.services.upstash_redis import get_redis

            get_redis().delete(_redis_key(str(digest)))
        except Exception:  # noqa: BLE001
            pass


def clear() -> None:
    with _lock:
        _store.clear()


def stats() -> dict:
    with _lock:
        terminal = sum(1 for e in _store.values() if e.state.is_terminal)
        return {"tracked": len(_store), "terminal": terminal}
