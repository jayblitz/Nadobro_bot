"""Central strategy tick coordinator — one loop instead of N per-user tasks."""
from __future__ import annotations

import asyncio
import logging

from src.nadobro.utils.env import env_float
import time
from dataclasses import dataclass
from typing import Callable, Optional

logger = logging.getLogger(__name__)

_TICK_SECONDS = env_float("NADO_STRATEGY_SCHEDULER_TICK_SECONDS", 2.0)
_load_state: Optional[Callable[[int, str], dict]] = None


@dataclass
class _Session:
    user_id: int
    network: str
    last_enqueued: float = 0.0


class StrategyScheduler:
    def __init__(self) -> None:
        self._sessions: dict[tuple[int, str], _Session] = {}
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def register(self, user_id: int, network: str) -> None:
        key = (int(user_id), str(network))
        if key not in self._sessions:
            self._sessions[key] = _Session(user_id=int(user_id), network=str(network))

    def unregister(self, user_id: int, network: str) -> None:
        self._sessions.pop((int(user_id), str(network)), None)

    async def start(self, load_state: Optional[Callable[[int, str], dict]] = None) -> None:
        global _load_state
        if load_state is not None:
            _load_state = load_state
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="strategy-scheduler")
        logger.info("Strategy scheduler started tick=%.1fs sessions=%d", _TICK_SECONDS, len(self._sessions))

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _loop(self) -> None:
        from src.nadobro.trading.execution_queue import enqueue_strategy
        from src.nadobro.core.cadence import effective_interval_seconds

        while self._running:
            now = time.time()
            for key, session in list(self._sessions.items()):
                try:
                    state = (_load_state(session.user_id, session.network) if _load_state else {}) or {}
                    if not state.get("running"):
                        self.unregister(session.user_id, session.network)
                        continue
                    strategy = str(state.get("strategy") or "").lower().strip()
                    raw_interval = max(1, int(state.get("interval_seconds") or 60))
                    interval = effective_interval_seconds(strategy, raw_interval)
                    last_run = float(state.get("last_run_ts") or 0)
                    if last_run > 0 and (now - last_run) < interval:
                        continue
                    if session.last_enqueued > 0 and (now - session.last_enqueued) < interval:
                        continue
                    bucket = int(now / max(1.0, _TICK_SECONDS))
                    enqueued = await enqueue_strategy(
                        {"telegram_id": session.user_id, "network": session.network, "strategy": strategy},
                        dedupe_key=f"{session.user_id}:{session.network}:{bucket}",
                    )
                    if enqueued:
                        session.last_enqueued = now
                except Exception as exc:
                    logger.debug("strategy scheduler tick failed key=%s err=%s", key, exc)
            await asyncio.sleep(_TICK_SECONDS)

    def stats(self) -> dict:
        return {"sessions": len(self._sessions), "running": self._running}


_scheduler: Optional[StrategyScheduler] = None


def get_scheduler() -> StrategyScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = StrategyScheduler()
    return _scheduler
