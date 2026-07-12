"""Lightweight per-user token-bucket rate limiter (abuse / cost control).

Trades already carry their own 5s cooldown (``trade_service.check_rate_limit``)
and the venue gateway has its own token bucket, but the *conversational / agent
LLM path* had no throttle — a single user could spam expensive Grok/Gemini calls
(and the DB/market lookups behind them), driving cost and load. This module adds
a tiny in-memory token bucket keyed by ``(telegram_id, action)``.

In-memory by design: it's a soft abuse guard, not a security boundary, so a
process restart resetting buckets is acceptable. Memory is bounded by evicting
buckets that have been idle longer than the refill window.
"""
from __future__ import annotations

import os

from src.nadobro.utils.env import env_float
import threading
import time
from dataclasses import dataclass

# Defaults tuned for the AI chat path: a burst of ~6 messages, then ~1 every
# 4 seconds (≈15/min sustained). Override per deployment via env.
_DEFAULT_CAPACITY = env_float("NADO_LLM_RL_CAPACITY", 6.0)
_DEFAULT_REFILL_PER_SEC = env_float("NADO_LLM_RL_REFILL_PER_SEC", 0.25)
_MAX_BUCKETS = 5000

_lock = threading.Lock()


@dataclass
class _Bucket:
    tokens: float
    last_refill: float


_buckets: dict[tuple[int, str], _Bucket] = {}


def _evict_stale_locked(now: float, window: float) -> None:
    if len(_buckets) < _MAX_BUCKETS:
        return
    stale = [k for k, b in _buckets.items() if now - b.last_refill > max(window, 60.0)]
    for k in stale:
        _buckets.pop(k, None)
    # Hard cap fallback: drop oldest if still oversized.
    while len(_buckets) >= _MAX_BUCKETS:
        oldest = min(_buckets, key=lambda k: _buckets[k].last_refill)
        _buckets.pop(oldest, None)


def check_rate_limit(
    telegram_id: int,
    action: str = "llm",
    *,
    capacity: float | None = None,
    refill_per_sec: float | None = None,
) -> tuple[bool, float]:
    """Consume one token for ``(telegram_id, action)``.

    Returns ``(allowed, retry_after_seconds)``. When ``allowed`` is False,
    ``retry_after_seconds`` estimates when the next token will be available.
    """
    cap = float(capacity if capacity is not None else _DEFAULT_CAPACITY)
    refill = float(refill_per_sec if refill_per_sec is not None else _DEFAULT_REFILL_PER_SEC)
    if cap <= 0 or refill <= 0:  # disabled
        return True, 0.0

    key = (int(telegram_id), str(action))
    now = time.monotonic()
    with _lock:
        _evict_stale_locked(now, cap / refill)
        bucket = _buckets.get(key)
        if bucket is None:
            bucket = _Bucket(tokens=cap, last_refill=now)
            _buckets[key] = bucket
        else:
            elapsed = now - bucket.last_refill
            bucket.tokens = min(cap, bucket.tokens + elapsed * refill)
            bucket.last_refill = now

        if bucket.tokens >= 1.0:
            bucket.tokens -= 1.0
            return True, 0.0
        retry_after = (1.0 - bucket.tokens) / refill
        return False, max(0.0, retry_after)


def reset(telegram_id: int | None = None) -> None:
    """Clear buckets (test helper / admin)."""
    with _lock:
        if telegram_id is None:
            _buckets.clear()
        else:
            for k in [k for k in _buckets if k[0] == int(telegram_id)]:
                _buckets.pop(k, None)
