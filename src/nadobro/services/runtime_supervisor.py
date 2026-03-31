import asyncio
import logging
import os
from concurrent.futures import ProcessPoolExecutor
from typing import Any

logger = logging.getLogger(__name__)

_MODE = (os.environ.get("NADO_RUNTIME_MODE") or "single").strip().lower()
_STARTED = False
_POOLS: dict[str, ProcessPoolExecutor] = {}

_WORKER_GROUP_MAP = {
    "grid": "mm_grid",
    "rgrid": "mm_grid",
    "dn": "dn",
    "vol": "vol",
    "bro": "bro",
}


def runtime_mode() -> str:
    return _MODE


def is_multiprocess_enabled() -> bool:
    return _MODE in ("multiprocess", "multi_process", "process")


def strategy_worker_group(strategy: str | None) -> str:
    return _WORKER_GROUP_MAP.get(str(strategy or "").lower(), "general")


def _pool_size_for_group(group: str) -> int:
    specific = os.environ.get(f"NADO_{group.upper()}_WORKERS")
    default_size = os.environ.get("NADO_PROCESS_WORKERS", "1")
    try:
        return max(1, int((specific or default_size).strip()))
    except Exception:
        return 1


def start_runtime_supervisor() -> None:
    global _STARTED
    if _STARTED:
        return
    if not is_multiprocess_enabled():
        logger.info("Runtime supervisor disabled (mode=%s).", _MODE)
        _STARTED = True
        return

    groups = ("mm_grid", "dn", "vol", "bro", "general")
    for group in groups:
        workers = _pool_size_for_group(group)
        _POOLS[group] = ProcessPoolExecutor(
            max_workers=workers,
            mp_context=None,
        )
    _STARTED = True
    logger.info(
        "Runtime supervisor started in multiprocess mode with pools: %s",
        ", ".join(f"{k}={v._max_workers}" for k, v in _POOLS.items()),
    )


def stop_runtime_supervisor() -> None:
    global _STARTED
    for pool in _POOLS.values():
        pool.shutdown(wait=False, cancel_futures=True)
    _POOLS.clear()
    _STARTED = False


def get_runtime_supervisor_diagnostics() -> dict[str, Any]:
    pools = {}
    for name, pool in _POOLS.items():
        max_workers = getattr(pool, "_max_workers", 0)
        processes = getattr(pool, "_processes", {}) or {}
        alive = 0
        for proc in processes.values():
            try:
                if proc.is_alive():
                    alive += 1
            except Exception:
                continue
        pools[name] = {
            "max_workers": int(max_workers or 0),
            "alive_workers": int(alive),
        }
    return {
        "mode": runtime_mode(),
        "multiprocess_enabled": is_multiprocess_enabled(),
        "started": bool(_STARTED),
        "pools": pools,
    }


def _run_cycle_job(payload: dict[str, Any]) -> dict[str, Any]:
    # Imported lazily inside workers so this module stays lightweight.
    from src.nadobro.services.bot_runtime import run_cycle_job_sync

    return run_cycle_job_sync(payload)


async def submit_cycle_job(payload: dict[str, Any]) -> dict[str, Any]:
    if not _STARTED:
        start_runtime_supervisor()
    if not is_multiprocess_enabled():
        return {"delegated": False}

    group = str(payload.get("worker_group") or strategy_worker_group(payload.get("strategy")))
    pool = _POOLS.get(group) or _POOLS.get("general")
    if pool is None:
        raise RuntimeError("runtime supervisor pool is not initialized")
    loop = asyncio.get_running_loop()
    future = pool.submit(_run_cycle_job, payload)
    return await asyncio.wrap_future(future, loop=loop)
