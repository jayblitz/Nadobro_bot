"""Serialize LOWIQPTS Telegram traffic across relay replicas via Postgres advisory locks."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable

import asyncpg

logger = logging.getLogger("relay.lowiq_turn")

# Stable int32 pair — one global LOWIQPTS outbound serialization slot for all relay processes.
_LOWIQ_ADV_LOCK_K1 = 0x4C4F5751
_LOWIQ_ADV_LOCK_K2 = 0x49505453

_DEFAULT_WAIT_SECONDS = float(os.environ.get("RELAY_LOWIQ_CHANNEL_WAIT_SECONDS", "180") or "180")


async def run_exclusive_lowiq_channel(
    pool: asyncpg.Pool,
    *,
    timeout_seconds: float | None = None,
    coro: Callable[[], Awaitable[None]],
) -> bool:
    """Run ``coro`` while holding a Postgres session advisory lock.

    Between attempts the pool connection is released so waiters do not pin the pool.

    Returns True after ``coro`` completes. Returns False if the lock could not be
    acquired before ``timeout_seconds``. Propagates any exception raised by ``coro``
    after releasing the lock.
    """
    wait_budget = max(1.0, float(timeout_seconds if timeout_seconds is not None else _DEFAULT_WAIT_SECONDS))
    deadline = time.monotonic() + wait_budget
    k1, k2 = _LOWIQ_ADV_LOCK_K1, _LOWIQ_ADV_LOCK_K2

    while True:
        async with pool.acquire() as conn:
            locked = await conn.fetchval(
                "SELECT pg_try_advisory_lock($1::int, $2::int)",
                k1,
                k2,
            )
            if locked:
                try:
                    await coro()
                finally:
                    unlocked = await conn.fetchval(
                        "SELECT pg_advisory_unlock($1::int, $2::int)",
                        k1,
                        k2,
                    )
                    if not unlocked:
                        logger.warning(
                            "LOWIQPTS advisory unlock returned false (unexpected lock state)"
                        )
                return True

        if time.monotonic() >= deadline:
            logger.info(
                "LOWIQPTS channel busy (advisory lock wait %.1fs timed out)",
                wait_budget,
            )
            return False
        await asyncio.sleep(0.05)
