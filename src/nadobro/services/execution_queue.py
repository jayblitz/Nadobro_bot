import asyncio
import logging
import time
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

_strategy_queue: asyncio.Queue = asyncio.Queue(maxsize=500)
_alert_queue: asyncio.Queue = asyncio.Queue(maxsize=200)
_workers: list[asyncio.Task] = []
_dedupe_seen: dict[str, float] = {}
_DEDUP_TTL_SECONDS = 20.0

_strategy_handler: Callable[[dict[str, Any]], Awaitable[None]] | None = None
_alert_handler: Callable[[dict[str, Any]], Awaitable[None]] | None = None


def register_handlers(
    strategy_handler: Callable[[dict[str, Any]], Awaitable[None]],
    alert_handler: Callable[[dict[str, Any]], Awaitable[None]],
) -> None:
    global _strategy_handler, _alert_handler
    _strategy_handler = strategy_handler
    _alert_handler = alert_handler


def _dedupe_ok(dedupe_key: str) -> bool:
    now = time.time()
    stale = [k for k, ts in _dedupe_seen.items() if now - ts > _DEDUP_TTL_SECONDS]
    for k in stale:
        _dedupe_seen.pop(k, None)
    if dedupe_key in _dedupe_seen:
        return False
    _dedupe_seen[dedupe_key] = now
    return True


async def enqueue_strategy(payload: dict[str, Any], dedupe_key: str) -> bool:
    if not _dedupe_ok(f"strategy:{dedupe_key}"):
        return False
    try:
        _strategy_queue.put_nowait(payload)
        return True
    except asyncio.QueueFull:
        logger.warning("strategy queue full; dropping job key=%s", dedupe_key)
        return False


async def enqueue_alert(payload: dict[str, Any], dedupe_key: str) -> bool:
    if not _dedupe_ok(f"alert:{dedupe_key}"):
        return False
    try:
        _alert_queue.put_nowait(payload)
        return True
    except asyncio.QueueFull:
        logger.warning("alert queue full; dropping job key=%s", dedupe_key)
        return False


async def _worker_loop(name: str, queue: asyncio.Queue, handler_getter: Callable[[], Callable | None]):
    while True:
        payload = await queue.get()
        try:
            handler = handler_getter()
            if not handler:
                continue
            await handler(payload)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("%s worker job failed: %s", name, e, exc_info=True)
        finally:
            queue.task_done()


def start_workers(strategy_workers: int = 2, alert_workers: int = 1):
    if _workers:
        return
    for idx in range(max(1, strategy_workers)):
        _workers.append(
            asyncio.create_task(
                _worker_loop(f"strategy-{idx}", _strategy_queue, lambda: _strategy_handler)
            )
        )
    for idx in range(max(1, alert_workers)):
        _workers.append(
            asyncio.create_task(
                _worker_loop(f"alert-{idx}", _alert_queue, lambda: _alert_handler)
            )
        )
    logger.info(
        "Execution queues started (strategy_workers=%s alert_workers=%s)",
        strategy_workers, alert_workers,
    )


def stop_workers():
    for task in list(_workers):
        task.cancel()
    _workers.clear()

