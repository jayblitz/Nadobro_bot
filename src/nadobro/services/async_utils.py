import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

_blocking_pool = ThreadPoolExecutor(max_workers=16, thread_name_prefix="nadobro-blocking")


async def run_blocking(func, *args, **kwargs) -> Any:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_blocking_pool, lambda: func(*args, **kwargs))

