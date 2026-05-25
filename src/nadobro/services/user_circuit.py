"""Per-user error circuit breaker — auto-pause noisy strategies.

When a user accumulates ``NADO_USER_CIRCUIT_THRESHOLD`` consecutive gateway
or cycle errors within ``NADO_USER_CIRCUIT_WINDOW_SECONDS``, their strategy
cycles are skipped until the cooldown elapses. This prevents one user's
broken wallet or rate-limit spiral from consuming queue workers indefinitely.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

_THRESHOLD = int(os.environ.get("NADO_USER_CIRCUIT_THRESHOLD", "5"))
_WINDOW = float(os.environ.get("NADO_USER_CIRCUIT_WINDOW_SECONDS", "60"))
_COOLDOWN = float(os.environ.get("NADO_USER_CIRCUIT_COOLDOWN_SECONDS", "120"))
_MAX_TRACKED = int(os.environ.get("NADO_USER_CIRCUIT_MAX_TRACKED", "4096"))

_lock = threading.RLock()
_states: dict[tuple[int, str], "_UserCircuitState"] = {}


@dataclass
class _UserCircuitState:
    failures: list[float] = field(default_factory=list)
    open_until: float = 0.0
    last_error: str = ""


def _key(user_id: int, network: str) -> tuple[int, str]:
    return int(user_id), str(network or "mainnet").lower()


def record_failure(user_id: int, network: str, error: str = "") -> None:
    now = time.time()
    key = _key(user_id, network)
    with _lock:
        if len(_states) >= _MAX_TRACKED and key not in _states:
            oldest = min(_states, key=lambda k: _states[k].failures[-1] if _states[k].failures else 0)
            _states.pop(oldest, None)
        state = _states.get(key)
        if state is None:
            state = _UserCircuitState()
            _states[key] = state
        cutoff = now - _WINDOW
        state.failures = [t for t in state.failures if t >= cutoff]
        state.failures.append(now)
        if error:
            state.last_error = str(error)[:220]
        if len(state.failures) >= _THRESHOLD and now >= state.open_until:
            state.open_until = now + _COOLDOWN
            logger.warning(
                "User circuit OPEN user=%s network=%s cooldown=%.0fs err=%s",
                user_id,
                network,
                _COOLDOWN,
                state.last_error,
            )


def record_success(user_id: int, network: str) -> None:
    key = _key(user_id, network)
    with _lock:
        state = _states.get(key)
        if state is not None:
            state.failures.clear()
            state.open_until = 0.0


def is_open(user_id: int, network: str) -> bool:
    key = _key(user_id, network)
    with _lock:
        state = _states.get(key)
        if not state:
            return False
        if time.time() < state.open_until:
            return True
        return False


def last_error(user_id: int, network: str) -> str:
    key = _key(user_id, network)
    with _lock:
        state = _states.get(key)
        return state.last_error if state else ""


def snapshot() -> dict:
    now = time.time()
    with _lock:
        open_circuits = {
            f"{uid}:{net}": {
                "open_until": s.open_until,
                "recent_failures": len(s.failures),
                "last_error": s.last_error,
            }
            for (uid, net), s in _states.items()
            if now < s.open_until
        }
    return {"open": open_circuits, "tracked": len(_states)}
