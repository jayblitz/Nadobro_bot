"""Unified Nado gateway budget — the lasting rate-limit solution.

Every outbound call to Nado (REST via ``http_session`` or SDK via
``nado_client``) must pass through :func:`try_acquire` **before** hitting
the network and call :func:`release` in a ``finally`` block.

Layers (checked in order):

1. **Host circuit** — Cloudflare + Nado ``error_code=1000`` breakers.
2. **Host token bucket** — global egress (default **16 rps / burst 32**,
   reflecting Nado's 2× rate-limit increase).
3. **Per-user in-flight cap** — max ``NADO_USER_MAX_INFLIGHT`` (default 4)
   concurrent calls per ``user_id``.
4. **Per-user token bucket** — fair share (default **4 rps / burst 8**).

When :func:`try_acquire` returns ``False``, callers must **skip** the call
and serve cached data — never fan out to per-product fallbacks.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

_USER_RPS = float(os.environ.get("NADO_USER_GATEWAY_RPS", "4"))
_USER_BURST = float(os.environ.get("NADO_USER_GATEWAY_BURST", "8"))
_USER_MAX_WAIT = float(os.environ.get("NADO_USER_GATEWAY_MAX_WAIT_SECONDS", "2.0"))
_USER_MAX_INFLIGHT = int(os.environ.get("NADO_USER_MAX_INFLIGHT", "4"))
_USER_BUCKET_MAX = int(os.environ.get("NADO_USER_GATEWAY_BUCKETS_MAX", "8192"))

_RL_THRESHOLD = int(os.environ.get("NADO_GATEWAY_RL_THRESHOLD", "4"))
_RL_WINDOW = float(os.environ.get("NADO_GATEWAY_RL_WINDOW_SECONDS", "15"))
_RL_COOLDOWN = float(os.environ.get("NADO_GATEWAY_RL_COOLDOWN_SECONDS", "60"))


@dataclass
class _TokenBucket:
    rps: float
    burst: float
    tokens: float = 0.0
    last_refill: float = field(default_factory=time.monotonic)

    def __post_init__(self) -> None:
        if self.tokens <= 0:
            self.tokens = self.burst

    def try_acquire(self, *, max_wait: float) -> bool:
        deadline = time.monotonic() + max(0.0, max_wait)
        while True:
            now = time.monotonic()
            elapsed = max(0.0, now - self.last_refill)
            self.tokens = min(self.burst, self.tokens + elapsed * self.rps)
            self.last_refill = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            if now >= deadline:
                return False
            needed = (1.0 - self.tokens) / self.rps if self.rps > 0 else 0.25
            time.sleep(min(needed, 0.25, max(0.0, deadline - now)))


@dataclass
class _BreakerLite:
    failures: list[float]
    open_until: float


_lock = threading.RLock()
_user_buckets: dict[int, _TokenBucket] = {}
_user_inflight: dict[int, int] = {}
_gateway_rl: dict[str, _BreakerLite] = {}


def _host(url: str) -> str:
    try:
        from urllib.parse import urlsplit
        return urlsplit(url).netloc.lower()
    except Exception:
        return ""


def is_rate_limit_error(exc: BaseException | str) -> bool:
    msg = str(exc).lower()
    return (
        "too many requests" in msg
        or '"error_code":1000' in msg
        or "'error_code': 1000" in msg
        or "error_code=1000" in msg
    )


def record_gateway_failure(url: str, exc: BaseException | str | None = None) -> None:
    if exc is not None and not is_rate_limit_error(exc):
        return
    host = _host(url)
    if not host:
        return
    now = time.time()
    with _lock:
        state = _gateway_rl.get(host)
        if state is None:
            state = _BreakerLite(failures=[], open_until=0.0)
            _gateway_rl[host] = state
        cutoff = now - _RL_WINDOW
        state.failures = [t for t in state.failures if t >= cutoff]
        state.failures.append(now)
        if len(state.failures) >= _RL_THRESHOLD and now >= state.open_until:
            state.open_until = now + _RL_COOLDOWN
            logger.warning(
                "Nado gateway rate-limit circuit OPEN host=%s cooldown=%.0fs failures=%d",
                host,
                _RL_COOLDOWN,
                len(state.failures),
            )


def is_gateway_rate_limited(url: str) -> bool:
    host = _host(url)
    if not host:
        return False
    with _lock:
        state = _gateway_rl.get(host)
        if not state:
            return False
        return time.time() < state.open_until


def is_gateway_blocked(url: str) -> bool:
    try:
        from src.nadobro.services.http_session import is_circuit_open
        return is_circuit_open(url) or is_gateway_rate_limited(url)
    except Exception:
        return is_gateway_rate_limited(url)


def _user_bucket(user_id: int) -> _TokenBucket:
    with _lock:
        bucket = _user_buckets.get(user_id)
        if bucket is None:
            if len(_user_buckets) >= _USER_BUCKET_MAX:
                oldest = min(_user_buckets, key=lambda k: _user_buckets[k].last_refill)
                _user_buckets.pop(oldest, None)
            bucket = _TokenBucket(rps=_USER_RPS, burst=_USER_BURST)
            _user_buckets[user_id] = bucket
        return bucket


def try_acquire(
    url: str,
    *,
    user_id: Optional[int] = None,
    max_wait: Optional[float] = None,
) -> bool:
    """Reserve one gateway slot. Always pair with :func:`release`."""
    if is_gateway_blocked(url):
        return False
    wait = _USER_MAX_WAIT if max_wait is None else float(max_wait)
    uid: Optional[int] = int(user_id) if user_id is not None else None
    if uid is not None:
        with _lock:
            if _user_inflight.get(uid, 0) >= _USER_MAX_INFLIGHT:
                logger.debug("gateway budget: user %s in-flight cap", uid)
                return False
            _user_inflight[uid] = _user_inflight.get(uid, 0) + 1
    allowed = True
    try:
        from src.nadobro.services.http_session import throttle_host
        if not throttle_host(url, max_wait=wait):
            allowed = False
        elif uid is not None and not _user_bucket(uid).try_acquire(max_wait=wait):
            logger.debug("gateway budget: user %s token bucket starved", uid)
            allowed = False
    except Exception:
        pass
    if not allowed:
        release(uid)
        return False
    return True


def release(user_id: Optional[int] = None) -> None:
    if user_id is None:
        return
    uid = int(user_id)
    with _lock:
        current = _user_inflight.get(uid, 0)
        if current <= 1:
            _user_inflight.pop(uid, None)
        else:
            _user_inflight[uid] = current - 1


def snapshot() -> dict:
    with _lock:
        inflight = dict(_user_inflight)
        rl_open = {h: s.open_until for h, s in _gateway_rl.items() if time.time() < s.open_until}
    try:
        from src.nadobro.services.http_session import bucket_snapshot, breaker_snapshot
        host_buckets = bucket_snapshot()
        breakers = breaker_snapshot()
    except Exception:
        host_buckets = {}
        breakers = {}
    return {
        "user_buckets": len(_user_buckets),
        "user_inflight": inflight,
        "gateway_rl_open": rl_open,
        "host_buckets": host_buckets,
        "breakers": breakers,
        "limits": {
            "user_rps": _USER_RPS,
            "user_burst": _USER_BURST,
            "user_max_inflight": _USER_MAX_INFLIGHT,
        },
    }
