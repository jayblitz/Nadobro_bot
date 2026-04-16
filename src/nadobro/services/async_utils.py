import asyncio
import contextvars
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, ParamSpec, TypeVar

_blocking_pool = ThreadPoolExecutor(max_workers=16, thread_name_prefix="nadobro-blocking")

P = ParamSpec("P")
R = TypeVar("R")


async def run_blocking(func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
    loop = asyncio.get_running_loop()
    # Keep ContextVar values (for example active language) when hopping to
    # threadpool workers; asyncio.run_in_executor does not preserve context.
    ctx = contextvars.copy_context()
    return await loop.run_in_executor(_blocking_pool, lambda: ctx.run(func, *args, **kwargs))

