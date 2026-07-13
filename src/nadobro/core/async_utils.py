"""Purpose-split thread pools for blocking work.

Phase 2: a single 16-worker pool was shared by DB, Nado SDK, and misc
handlers — one slow strategy cycle could starve Telegram reply DB reads.

Pools (env-tunable, defaults sized for ~250 users/shard):

* ``run_blocking_db`` — Postgres via psycopg2 (default 30 workers)
* ``run_blocking_sdk`` — Nado SDK / signing (default 12 workers)
* ``run_blocking`` — everything else (default 8 workers)
"""
from __future__ import annotations

import asyncio
import contextvars

from src.nadobro.utils.env import env_float, env_int
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, ParamSpec, TypeVar

_DB_WORKERS = env_int("NADO_DB_POOL_WORKERS", 30)
_SDK_WORKERS = env_int("NADO_SDK_POOL_WORKERS", 12)
_MISC_WORKERS = env_int("NADO_MISC_POOL_WORKERS", 8)

_db_pool = ThreadPoolExecutor(max_workers=max(1, _DB_WORKERS), thread_name_prefix="nadobro-db")
_sdk_pool = ThreadPoolExecutor(max_workers=max(1, _SDK_WORKERS), thread_name_prefix="nadobro-sdk")
_misc_pool = ThreadPoolExecutor(max_workers=max(1, _MISC_WORKERS), thread_name_prefix="nadobro-misc")
# Legacy alias — misc pool.
_blocking_pool = _misc_pool

P = ParamSpec("P")
R = TypeVar("R")


async def _run_in(pool: ThreadPoolExecutor, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    return await loop.run_in_executor(pool, lambda: ctx.run(func, *args, **kwargs))


async def run_blocking(func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
    return await _run_in(_misc_pool, func, *args, **kwargs)


async def run_blocking_db(func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
    return await _run_in(_db_pool, func, *args, **kwargs)


async def run_blocking_sdk(func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
    return await _run_in(_sdk_pool, func, *args, **kwargs)


# Default wall-clock ceiling for an SDK call that sits on a user-facing render
# path. The Nado SDK session timeout is read=12s × up-to-3 sessions (~30s), which
# is far too long to make a Telegram tap wait. ``run_blocking_sdk_capped`` bounds
# the *await* so the handler returns a placeholder; the underlying thread keeps
# running (it warms the cache for the next render) — we just stop blocking on it.
_SDK_CALL_CEILING_SECONDS = env_float("NADO_SDK_CALL_CEILING_SECONDS", 1.5)


async def run_blocking_sdk_capped(
    func: Callable[P, R],
    *args: P.args,
    timeout_seconds: float | None = None,
    default: R = None,  # type: ignore[assignment]
    **kwargs: P.kwargs,
) -> R:
    """Run a blocking SDK call on the SDK pool with a hard wall-clock cap.

    On timeout, returns ``default`` immediately instead of blocking the click
    path for the full SDK timeout chain. The orphaned thread runs to completion
    (Python can't cancel it) and typically warms the relevant cache.
    """
    ceiling = _SDK_CALL_CEILING_SECONDS if timeout_seconds is None else timeout_seconds
    try:
        return await asyncio.wait_for(run_blocking_sdk(func, *args, **kwargs), timeout=ceiling)
    except asyncio.TimeoutError:
        return default


def pool_stats() -> dict[str, int]:
    return {
        "db_workers": _DB_WORKERS,
        "sdk_workers": _SDK_WORKERS,
        "misc_workers": _MISC_WORKERS,
    }
