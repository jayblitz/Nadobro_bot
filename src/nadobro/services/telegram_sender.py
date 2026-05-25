"""Telegram delivery service with priority lanes + token buckets.

Why this module exists
======================

The Telegram Bot API enforces two limits that bite at scale:

* **~30 messages/second per bot** globally;
* **~1 message/second per chat** for plain ``send_message``.

Today every notification path calls ``bot.send_message`` directly. With
1000 users a single market move can fire hundreds of alerts plus
strategy notifications in the same second. The result is HTTP 429 from
Telegram, dropped messages, and inconsistent user experience.

This service is the **single egress point** for outbound Telegram
traffic. Every other module that wants to send something must call
:func:`send_text` or :func:`send_priority` instead of touching ``bot``
directly. Behind the scenes:

* a **global token bucket** caps app-wide throughput at
  :data:`TELEGRAM_GLOBAL_RPS` (default 25, headroom below 30) with burst
  :data:`TELEGRAM_GLOBAL_BURST`;
* a **per-chat token bucket** caps per-chat throughput at
  :data:`TELEGRAM_CHAT_RPS` (default 1) with burst
  :data:`TELEGRAM_CHAT_BURST`;
* **priority lanes** (USER_REPLY > ORDER > ALERT > INFO) so a strategy
  status broadcast cannot starve a user-typed command reply during a
  burst;
* **bounded per-lane queues** that drop the oldest INFO/ALERT items when
  saturated rather than back up unbounded;
* **Telegram-aware retries**: 429 responses re-queue with the
  ``retry_after`` Telegram provided.

Idempotency
-----------

Callers that must not duplicate a message (e.g. order-confirm) can pass
``dedupe_key=`` to skip an enqueue when an equivalent item is already
pending. This is a process-local guard intended for "near-simultaneous"
duplicates (same lane, same key, within ~30s).

Observability
-------------

The sender records SLI samples ``telegram.send`` per (lane, outcome)
and counters ``telegram.dropped`` per lane when items are evicted.
"""
from __future__ import annotations

import asyncio
import enum
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from src.nadobro.services import sli
from src.nadobro.services.bounded_cache import LRUCache

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tunables.
# ---------------------------------------------------------------------------
TELEGRAM_GLOBAL_RPS = float(os.environ.get("TELEGRAM_GLOBAL_RPS", "25"))
TELEGRAM_GLOBAL_BURST = float(os.environ.get("TELEGRAM_GLOBAL_BURST", "30"))
TELEGRAM_CHAT_RPS = float(os.environ.get("TELEGRAM_CHAT_RPS", "1"))
TELEGRAM_CHAT_BURST = float(os.environ.get("TELEGRAM_CHAT_BURST", "2"))
TELEGRAM_LANE_CAPACITY: dict[str, int] = {
    "user_reply": int(os.environ.get("TELEGRAM_LANE_CAPACITY_USER_REPLY", "1000")),
    "order": int(os.environ.get("TELEGRAM_LANE_CAPACITY_ORDER", "1000")),
    "alert": int(os.environ.get("TELEGRAM_LANE_CAPACITY_ALERT", "2000")),
    "info": int(os.environ.get("TELEGRAM_LANE_CAPACITY_INFO", "2000")),
}
TELEGRAM_PER_CHAT_BUCKETS_MAX = int(os.environ.get("TELEGRAM_PER_CHAT_BUCKETS_MAX", "8192"))
TELEGRAM_DEDUPE_TTL_SECONDS = float(os.environ.get("TELEGRAM_DEDUPE_TTL_SECONDS", "30"))
TELEGRAM_MAX_RETRY_AFTER = float(os.environ.get("TELEGRAM_MAX_RETRY_AFTER", "20"))


class Lane(str, enum.Enum):
    """Priority lanes (lower ordinal = higher priority)."""

    USER_REPLY = "user_reply"
    ORDER = "order"
    ALERT = "alert"
    INFO = "info"

    @property
    def order(self) -> int:
        return {
            Lane.USER_REPLY: 0,
            Lane.ORDER: 1,
            Lane.ALERT: 2,
            Lane.INFO: 3,
        }[self]


# ---------------------------------------------------------------------------
# Token bucket primitive.
# ---------------------------------------------------------------------------
@dataclass
class _TokenBucket:
    rps: float
    burst: float
    tokens: float = 0.0
    last_refill: float = field(default_factory=time.monotonic)

    def __post_init__(self) -> None:
        if self.tokens == 0.0:
            self.tokens = self.burst

    def _refill(self, now: float) -> None:
        elapsed = max(0.0, now - self.last_refill)
        self.tokens = min(self.burst, self.tokens + elapsed * self.rps)
        self.last_refill = now

    def try_consume(self, n: float = 1.0) -> tuple[bool, float]:
        """Returns ``(consumed, wait_seconds_if_not)``."""
        now = time.monotonic()
        self._refill(now)
        if self.tokens >= n:
            self.tokens -= n
            return True, 0.0
        needed = n - self.tokens
        return False, needed / self.rps if self.rps > 0 else 1.0


# ---------------------------------------------------------------------------
# Pending message envelope.
# ---------------------------------------------------------------------------
@dataclass
class _Envelope:
    chat_id: int
    text: str
    lane: Lane
    enqueued_at: float
    kwargs: dict[str, Any]
    dedupe_key: Optional[str] = None
    future: Optional["asyncio.Future[bool]"] = None


# ---------------------------------------------------------------------------
# The sender.
# ---------------------------------------------------------------------------
class TelegramSender:
    """Singleton-style sender. Use :func:`get_sender` to access."""

    def __init__(self) -> None:
        self._lanes: dict[Lane, deque[_Envelope]] = {lane: deque() for lane in Lane}
        self._lane_caps: dict[Lane, int] = {
            lane: TELEGRAM_LANE_CAPACITY.get(lane.value, 1000) for lane in Lane
        }
        self._global = _TokenBucket(rps=TELEGRAM_GLOBAL_RPS, burst=TELEGRAM_GLOBAL_BURST)
        self._chat_buckets: LRUCache[int, _TokenBucket] = LRUCache(
            max_size=TELEGRAM_PER_CHAT_BUCKETS_MAX
        )
        self._dedupe: LRUCache[str, float] = LRUCache(
            max_size=10000, ttl_seconds=TELEGRAM_DEDUPE_TTL_SECONDS
        )
        self._cv = asyncio.Condition()
        self._bot_callable: Optional[Callable[..., Any]] = None
        self._worker_task: Optional[asyncio.Task[None]] = None
        self._running = False
        self._stats = {
            "sent": 0,
            "dropped": 0,
            "retried": 0,
            "failed": 0,
        }

    def bind(self, bot_callable: Callable[..., Any]) -> None:
        """Bind the PTB ``bot.send_message`` (or compatible) callable.

        Should be invoked once during bot startup.
        """
        self._bot_callable = bot_callable

    def is_bound(self) -> bool:
        return self._bot_callable is not None

    async def start(self) -> None:
        if self._running:
            return
        if not self._bot_callable:
            raise RuntimeError("TelegramSender.bind() must be called before start()")
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop(), name="telegram-sender")

    async def stop(self) -> None:
        self._running = False
        async with self._cv:
            self._cv.notify_all()
        if self._worker_task is not None:
            try:
                await asyncio.wait_for(self._worker_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._worker_task.cancel()

    # -- Public send API ---------------------------------------------------

    async def send_text(
        self,
        chat_id: int,
        text: str,
        *,
        lane: Lane = Lane.USER_REPLY,
        dedupe_key: Optional[str] = None,
        await_result: bool = False,
        **kwargs: Any,
    ) -> bool:
        """Enqueue ``text`` for ``chat_id``.

        Returns ``True`` if the message was accepted into a lane (delivery
        will be attempted with retries). Returns ``False`` if the lane is
        full and the message was dropped, or if dedupe rejected it. If
        ``await_result=True`` blocks until the message is actually sent
        and returns the final boolean (``True`` only on confirmed send).
        """
        if not text:
            return False
        if dedupe_key and self._dedupe.get(dedupe_key) is not None:
            return False
        env = _Envelope(
            chat_id=int(chat_id),
            text=text,
            lane=lane,
            enqueued_at=time.monotonic(),
            kwargs=dict(kwargs),
            dedupe_key=dedupe_key,
        )
        if await_result:
            env.future = asyncio.get_running_loop().create_future()
        async with self._cv:
            cap = self._lane_caps[lane]
            queue = self._lanes[lane]
            if len(queue) >= cap:
                # Drop oldest INFO/ALERT; refuse USER_REPLY/ORDER.
                if lane in (Lane.INFO, Lane.ALERT):
                    dropped = queue.popleft()
                    self._stats["dropped"] += 1
                    sli.increment("telegram.dropped", lane=lane.value)
                    if dropped.future and not dropped.future.done():
                        dropped.future.set_result(False)
                else:
                    self._stats["dropped"] += 1
                    sli.increment("telegram.dropped", lane=lane.value)
                    if env.future is not None:
                        env.future.set_result(False)
                    return False
            queue.append(env)
            if dedupe_key:
                self._dedupe.set(dedupe_key, time.monotonic())
            self._cv.notify()
        if env.future is None:
            return True
        return await env.future

    # -- Worker loop -------------------------------------------------------

    async def _worker_loop(self) -> None:
        logger.info(
            "TelegramSender started global=%.1f rps burst=%.1f per_chat=%.1f rps burst=%.1f",
            TELEGRAM_GLOBAL_RPS,
            TELEGRAM_GLOBAL_BURST,
            TELEGRAM_CHAT_RPS,
            TELEGRAM_CHAT_BURST,
        )
        while self._running:
            env = await self._next_eligible()
            if env is None:
                continue
            await self._dispatch(env)

    async def _next_eligible(self) -> Optional[_Envelope]:
        """Wait until something is sendable, then return it."""
        async with self._cv:
            while self._running:
                env, wait_for = self._pick_eligible_locked()
                if env is not None:
                    return env
                if wait_for is None:
                    # Nothing queued.
                    await self._cv.wait()
                    continue
                # Throttled — sleep outside the lock so producers can enqueue.
                wait = max(0.001, min(wait_for, 1.0))
                try:
                    await asyncio.wait_for(self._cv.wait(), timeout=wait)
                except asyncio.TimeoutError:
                    continue
        return None

    def _pick_eligible_locked(self) -> tuple[Optional[_Envelope], Optional[float]]:
        """Find the highest-priority message whose chat bucket has tokens.

        If no message is eligible, return the shortest required wait time
        across the candidate messages so the caller can sleep efficiently.
        """
        min_wait: Optional[float] = None
        any_queued = False
        for lane in (Lane.USER_REPLY, Lane.ORDER, Lane.ALERT, Lane.INFO):
            queue = self._lanes[lane]
            if not queue:
                continue
            any_queued = True
            env = queue[0]
            # Check global bucket.
            ok_g, wait_g = self._global.try_consume(0.0)
            # Pre-check chat bucket without consuming.
            chat_bucket = self._chat_buckets.get(env.chat_id)
            if chat_bucket is None:
                chat_bucket = _TokenBucket(rps=TELEGRAM_CHAT_RPS, burst=TELEGRAM_CHAT_BURST)
                self._chat_buckets.set(env.chat_id, chat_bucket)
            # Actual consume only if both buckets have headroom.
            chat_ok, chat_wait = chat_bucket.try_consume(0.0)
            ok_g, wait_g = self._global.try_consume(1.0) if chat_ok else (False, wait_g)
            if ok_g and chat_ok:
                # We "spent" a global token; spend the chat token too.
                chat_bucket.try_consume(1.0)
                queue.popleft()
                return env, None
            # Roll back the global token if chat starved us.
            if ok_g and not chat_ok:
                self._global.tokens = min(self._global.burst, self._global.tokens + 1.0)
            candidate_wait = max(wait_g, chat_wait)
            min_wait = candidate_wait if min_wait is None else min(min_wait, candidate_wait)
        if not any_queued:
            return None, None
        return None, min_wait

    async def _dispatch(self, env: _Envelope) -> None:
        started = time.perf_counter()
        delivered = False
        outcome = "ok"
        retried = False
        try:
            assert self._bot_callable is not None
            await self._bot_callable(chat_id=env.chat_id, text=env.text, **env.kwargs)
            self._stats["sent"] += 1
            delivered = True
        except Exception as exc:
            outcome = "error"
            retry_after = _extract_retry_after(exc)
            if retry_after is not None and retry_after <= TELEGRAM_MAX_RETRY_AFTER:
                self._stats["retried"] += 1
                retried = True
                logger.warning(
                    "telegram 429 chat=%s lane=%s retry_after=%.2fs",
                    env.chat_id, env.lane.value, retry_after,
                )

                async def _requeue() -> None:
                    await asyncio.sleep(retry_after)
                    async with self._cv:
                        self._lanes[env.lane].appendleft(env)
                        self._cv.notify()

                asyncio.create_task(_requeue())
            else:
                self._stats["failed"] += 1
                logger.warning(
                    "telegram send failed chat=%s lane=%s err=%s",
                    env.chat_id, env.lane.value, exc,
                )
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            sli.record_latency("telegram.send", elapsed_ms, lane=env.lane.value, outcome=outcome)
            # Only finalize the caller's future on terminal outcomes
            # (success or non-retryable failure). Retried envelopes will
            # be re-dispatched and may still succeed.
            if not retried and env.future is not None and not env.future.done():
                env.future.set_result(delivered)

    # -- Diagnostics -------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        lanes = {lane.value: len(queue) for lane, queue in self._lanes.items()}
        return {
            "lanes": lanes,
            **self._stats,
            "global_tokens": round(self._global.tokens, 2),
            "chat_buckets": self._chat_buckets.stats(),
        }


def _extract_retry_after(exc: BaseException) -> Optional[float]:
    """Best-effort extraction of Telegram's ``retry_after`` from PTB errors."""
    retry_after = getattr(exc, "retry_after", None)
    if retry_after is None:
        retry_after = getattr(exc, "parameters", {}) and getattr(
            getattr(exc, "parameters", None), "get", lambda *_: None
        )("retry_after")
    if retry_after is None:
        return None
    try:
        return float(retry_after)
    except (TypeError, ValueError):
        return None


_SENDER: Optional[TelegramSender] = None


def get_sender() -> TelegramSender:
    """Return the process-wide TelegramSender singleton."""
    global _SENDER
    if _SENDER is None:
        _SENDER = TelegramSender()
    return _SENDER


async def send_text(chat_id: int, text: str, **kwargs: Any) -> bool:
    """Convenience: USER_REPLY lane."""
    return await get_sender().send_text(chat_id, text, lane=Lane.USER_REPLY, **kwargs)


async def send_priority(chat_id: int, text: str, lane: Lane, **kwargs: Any) -> bool:
    return await get_sender().send_text(chat_id, text, lane=lane, **kwargs)


__all__ = (
    "Lane",
    "TelegramSender",
    "get_sender",
    "send_text",
    "send_priority",
)
