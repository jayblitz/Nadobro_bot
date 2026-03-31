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
_stats: dict[str, int] = {
    "strategy_enqueued": 0,
    "strategy_deduped": 0,
    "strategy_dropped": 0,
    "alert_enqueued": 0,
    "alert_deduped": 0,
    "alert_dropped": 0,
}

_strategy_handler: Callable[[dict[str, Any]], Awaitable[None]] | None = None
_alert_handler: Callable[[dict[str, Any]], Awaitable[None]] | None = None


def register_handlers(
    strategy_handler: Callable[[dict[str, Any]], Awaitable[None]],
    alert_handler: Callable[[dict[str, Any]], Awaitable[None]],
) -> None:
    global _strategy_handler, _alert_handler
    _strategy_handler = strategy_handler
    _alert_handler = alert_handler


_last_cleanup_ts: float = 0.0
_CLEANUP_INTERVAL = 60.0

def _dedupe_ok(dedupe_key: str) -> bool:
    global _last_cleanup_ts
    now = time.time()
    if now - _last_cleanup_ts > _CLEANUP_INTERVAL:
        stale = [k for k, ts in _dedupe_seen.items() if now - ts > _DEDUP_TTL_SECONDS]
        for k in stale:
            _dedupe_seen.pop(k, None)
        _last_cleanup_ts = now
    if dedupe_key in _dedupe_seen:
        return False
    _dedupe_seen[dedupe_key] = now
    return True


async def enqueue_strategy(payload: dict[str, Any], dedupe_key: str) -> bool:
    if not _dedupe_ok(f"strategy:{dedupe_key}"):
        _stats["strategy_deduped"] += 1
        return False
    try:
        _strategy_queue.put_nowait(payload)
        _stats["strategy_enqueued"] += 1
        return True
    except asyncio.QueueFull:
        _stats["strategy_dropped"] += 1
        logger.warning("strategy queue full; dropping job key=%s", dedupe_key)
        return False


async def enqueue_alert(payload: dict[str, Any], dedupe_key: str) -> bool:
    if not _dedupe_ok(f"alert:{dedupe_key}"):
        _stats["alert_deduped"] += 1
        return False
    try:
        _alert_queue.put_nowait(payload)
        _stats["alert_enqueued"] += 1
        return True
    except asyncio.QueueFull:
        _stats["alert_dropped"] += 1
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


async def stop_workers():
    if not _workers:
        return
    tasks = list(_workers)
    _workers.clear()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Execution queues stopped")


def get_queue_diagnostics() -> dict[str, Any]:
    return {
        "strategy_qsize": int(_strategy_queue.qsize()),
        "strategy_qmax": int(_strategy_queue.maxsize),
        "alert_qsize": int(_alert_queue.qsize()),
        "alert_qmax": int(_alert_queue.maxsize),
        "workers_running": len([t for t in _workers if not t.done()]),
        "dedupe_cache_size": len(_dedupe_seen),
        "stats": dict(_stats),
    }

