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
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, ParamSpec, TypeVar

_DB_WORKERS = int(os.environ.get("NADO_DB_POOL_WORKERS", "30"))
_SDK_WORKERS = int(os.environ.get("NADO_SDK_POOL_WORKERS", "12"))
_MISC_WORKERS = int(os.environ.get("NADO_MISC_POOL_WORKERS", "8"))

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


def pool_stats() -> dict[str, int]:
    return {
        "db_workers": _DB_WORKERS,
        "sdk_workers": _SDK_WORKERS,
        "misc_workers": _MISC_WORKERS,
    }
