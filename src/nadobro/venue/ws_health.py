"""WebSocket health registry — skip REST poll when WS is live.

Tracks the last successful WS message per ``(user_id, network)``. When
``portfolio_ws_enabled()`` and the socket is healthy, ``nado_sync`` can
serve cached snapshots and only reconcile on a longer interval.
"""
from __future__ import annotations


from src.nadobro.utils.env import env_float
import threading
import time
from typing import Optional

_HEALTH_SECONDS = env_float("NADO_WS_HEALTH_SECONDS", 45.0)
_RECONCILE_SECONDS = env_float("NADO_WS_RECONCILE_SECONDS", 300.0)

_lock = threading.RLock()
_last_event: dict[tuple[int, str], float] = {}
_connected: dict[tuple[int, str], bool] = {}


def _key(user_id: int, network: str) -> tuple[int, str]:
    return int(user_id), str(network or "mainnet").lower()


def mark_connected(user_id: int, network: str) -> None:
    key = _key(user_id, network)
    with _lock:
        _connected[key] = True
        _last_event[key] = time.time()


def mark_disconnected(user_id: int, network: str) -> None:
    key = _key(user_id, network)
    with _lock:
        _connected[key] = False


def touch(user_id: int, network: str) -> None:
    key = _key(user_id, network)
    with _lock:
        _last_event[key] = time.time()
        _connected[key] = True


def is_healthy(user_id: int, network: str) -> bool:
    key = _key(user_id, network)
    now = time.time()
    with _lock:
        if not _connected.get(key):
            return False
        last = _last_event.get(key, 0.0)
        return (now - last) <= _HEALTH_SECONDS


def should_skip_poll(user_id: int, network: str, *, reason: str = "poll") -> bool:
    """True when WS is healthy and this tick doesn't need forced reconcile."""
    if reason != "poll":
        return False
    return is_healthy(user_id, network)


def reconcile_due(user_id: int, network: str, last_reconcile_monotonic: float) -> bool:
    return (time.monotonic() - last_reconcile_monotonic) >= _RECONCILE_SECONDS


def snapshot() -> dict:
    now = time.time()
    with _lock:
        healthy = sum(
            1 for key in _connected
            if _connected.get(key) and (now - _last_event.get(key, 0)) <= _HEALTH_SECONDS
        )
        return {
            "connected": sum(1 for v in _connected.values() if v),
            "healthy": healthy,
            "tracked": len(_last_event),
        }
