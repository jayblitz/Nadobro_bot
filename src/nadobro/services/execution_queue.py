import asyncio
import logging
import os
import time
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

_strategy_queue: asyncio.Queue = asyncio.Queue(maxsize=500)
_alert_queue: asyncio.Queue = asyncio.Queue(maxsize=200)
_workers: list[asyncio.Task] = []
_strategy_worker_target: int = 1
_alert_worker_target: int = 1
_dedupe_seen: dict[str, float] = {}
_DEDUP_TTL_SECONDS = 20.0
_stats: dict[str, int] = {
    "strategy_enqueued": 0,
    "strategy_deduped": 0,
    "strategy_backpressured": 0,
    "strategy_dropped": 0,
    "vol_strategy_enqueued": 0,
    "vol_strategy_deduped": 0,
    "vol_strategy_backpressured": 0,
    "vol_strategy_dropped": 0,
    "alert_enqueued": 0,
    "alert_deduped": 0,
    "alert_dropped": 0,
}

_strategy_handler: Callable[[dict[str, Any]], Awaitable[None]] | None = None
_alert_handler: Callable[[dict[str, Any]], Awaitable[None]] | None = None


def _queue_put_timeout_seconds(kind: str) -> float:
    env_key = "NADO_STRATEGY_QUEUE_PUT_TIMEOUT_SECONDS" if kind == "strategy" else "NADO_ALERT_QUEUE_PUT_TIMEOUT_SECONDS"
    raw = (os.environ.get(env_key) or "2.0").strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 2.0


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
    strategy = str((payload or {}).get("strategy") or "").lower().strip()
    is_vol = strategy == "vol"
    if not any((not t.done()) and (t.get_name() or "").startswith("strategy-") for t in _workers):
        logger.warning("No active strategy queue workers detected; restarting workers")
        start_workers(_strategy_worker_target, _alert_worker_target)
    dedupe_marker = f"strategy:{dedupe_key}"
    if not _dedupe_ok(dedupe_marker):
        _stats["strategy_deduped"] += 1
        if is_vol:
            _stats["vol_strategy_deduped"] += 1
        return False
    try:
        if _strategy_queue.full():
            _stats["strategy_backpressured"] += 1
            if is_vol:
                _stats["vol_strategy_backpressured"] += 1
            logger.warning(
                "strategy queue full; waiting for capacity key=%s qsize=%s/%s",
                dedupe_key,
                _strategy_queue.qsize(),
                _strategy_queue.maxsize,
            )
        await asyncio.wait_for(_strategy_queue.put(payload), timeout=_queue_put_timeout_seconds("strategy"))
        _stats["strategy_enqueued"] += 1
        if is_vol:
            _stats["vol_strategy_enqueued"] += 1
        return True
    except asyncio.TimeoutError:
        _dedupe_seen.pop(dedupe_marker, None)
        _stats["strategy_dropped"] += 1
        if is_vol:
            _stats["vol_strategy_dropped"] += 1
        logger.error(
            "strategy queue remained full after timeout; dropping job key=%s qsize=%s/%s",
            dedupe_key,
            _strategy_queue.qsize(),
            _strategy_queue.maxsize,
        )
        return False


async def enqueue_alert(payload: dict[str, Any], dedupe_key: str) -> bool:
    dedupe_marker = f"alert:{dedupe_key}"
    if not _dedupe_ok(dedupe_marker):
        _stats["alert_deduped"] += 1
        return False
    try:
        if _alert_queue.full():
            logger.warning(
                "alert queue full; waiting for capacity key=%s qsize=%s/%s",
                dedupe_key,
                _alert_queue.qsize(),
                _alert_queue.maxsize,
            )
        await asyncio.wait_for(_alert_queue.put(payload), timeout=_queue_put_timeout_seconds("alert"))
        _stats["alert_enqueued"] += 1
        return True
    except asyncio.TimeoutError:
        _dedupe_seen.pop(dedupe_marker, None)
        _stats["alert_dropped"] += 1
        logger.error(
            "alert queue remained full after timeout; dropping job key=%s qsize=%s/%s",
            dedupe_key,
            _alert_queue.qsize(),
            _alert_queue.maxsize,
        )
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
    global _strategy_worker_target, _alert_worker_target
    _strategy_worker_target = max(1, int(strategy_workers))
    _alert_worker_target = max(1, int(alert_workers))

    # Prune done/cancelled tasks so startup is idempotent across reconnects.
    if _workers:
        _workers[:] = [t for t in _workers if not t.done()]

    strategy_running = len(
        [t for t in _workers if (t.get_name() or "").startswith("strategy-")]
    )
    alert_running = len(
        [t for t in _workers if (t.get_name() or "").startswith("alert-")]
    )

    for idx in range(strategy_running, _strategy_worker_target):
        _workers.append(
            asyncio.create_task(
                _worker_loop(f"strategy-{idx}", _strategy_queue, lambda: _strategy_handler),
                name=f"strategy-{idx}",
            )
        )
    for idx in range(alert_running, _alert_worker_target):
        _workers.append(
            asyncio.create_task(
                _worker_loop(f"alert-{idx}", _alert_queue, lambda: _alert_handler),
                name=f"alert-{idx}",
            )
        )
    logger.info(
        "Execution queues started (strategy_workers=%s alert_workers=%s)",
        _strategy_worker_target, _alert_worker_target,
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
    running_tasks = [t for t in _workers if not t.done()]
    strategy_running = len([t for t in running_tasks if (t.get_name() or "").startswith("strategy-")])
    alert_running = len([t for t in running_tasks if (t.get_name() or "").startswith("alert-")])
    return {
        "strategy_qsize": int(_strategy_queue.qsize()),
        "strategy_qmax": int(_strategy_queue.maxsize),
        "alert_qsize": int(_alert_queue.qsize()),
        "alert_qmax": int(_alert_queue.maxsize),
        "workers_running": len(running_tasks),
        "strategy_workers_running": strategy_running,
        "alert_workers_running": alert_running,
        "strategy_workers_target": int(_strategy_worker_target),
        "alert_workers_target": int(_alert_worker_target),
        "dedupe_cache_size": len(_dedupe_seen),
        "stats": dict(_stats),
    }

