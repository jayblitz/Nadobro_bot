"""Executor Orchestrator — single supervisor that owns executor lifecycles.

Supports spawn / stop / list (filtered by ``controller_id``), an event bus
(queue + inspectable log), batched cancel via ``asyncio.gather``, and consults
the Risk Engine before each spawn. Enforces a process-level kill switch.

Implemented in Phase 1.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import AsyncIterator, Dict, List, Optional

from src.nadobro.engine.executor_base import Executor, ExecutorFailed
from src.nadobro.engine.risk import ExecutorRequest, RiskEngine
from src.nadobro.engine.types import CloseType, RiskState


@dataclass
class ExecutorEvent:
    kind: str  # spawned | spawn_rejected | stopped | tick | failed | kill_switch
    executor_id: Optional[str] = None
    controller_id: Optional[str] = None
    close_type: Optional[CloseType] = None
    reason: Optional[str] = None
    ts: float = field(default_factory=time.time)


class ExecutorOrchestrator:
    def __init__(
        self,
        risk_engine: Optional[RiskEngine] = None,
        risk_state_provider: Optional[object] = None,
    ) -> None:
        self.risk = risk_engine
        # callable(controller_id) -> RiskState; defaults to an empty snapshot
        self.risk_state_provider = risk_state_provider
        self._executors: Dict[str, Executor] = {}
        self._queue: "asyncio.Queue[ExecutorEvent]" = asyncio.Queue()
        self.event_log: List[ExecutorEvent] = []
        self._killed = False
        self._kill_reason: Optional[str] = None

    # -- kill switch ------------------------------------------------------
    def kill_switch_on(self, reason: str) -> None:
        self._killed = True
        self._kill_reason = reason
        if self.risk is not None:
            self.risk.kill_switch_on(reason)
        self._emit(ExecutorEvent(kind="kill_switch", reason=reason))

    def kill_switch_off(self) -> None:
        self._killed = False
        self._kill_reason = None
        if self.risk is not None:
            self.risk.kill_switch_off()

    @property
    def is_killed(self) -> bool:
        return self._killed or (self.risk is not None and self.risk.is_killed())

    # -- registry ---------------------------------------------------------
    def list(
        self, controller_id: Optional[str] = None, active_only: bool = False
    ) -> List[Executor]:
        vals = list(self._executors.values())
        if controller_id is not None:
            vals = [e for e in vals if e.controller_id == controller_id]
        if active_only:
            vals = [e for e in vals if not e.is_terminated]
        return vals

    def get(self, executor_id: str) -> Optional[Executor]:
        return self._executors.get(executor_id)

    def _state_for(self, controller_id: str) -> RiskState:
        if callable(self.risk_state_provider):
            state = self.risk_state_provider(controller_id)
        else:
            state = RiskState()
        state.executor_count = len(self.list(controller_id, active_only=True))
        return state

    # -- lifecycle --------------------------------------------------------
    async def spawn(
        self, executor: Executor, request: Optional[ExecutorRequest] = None
    ) -> bool:
        if self.is_killed:
            self._emit(
                ExecutorEvent(
                    kind="spawn_rejected",
                    executor_id=executor.id,
                    controller_id=executor.controller_id,
                    reason="kill_switch",
                )
            )
            return False
        if self.risk is not None and request is not None:
            state = self._state_for(executor.controller_id)
            ok, reason = self.risk.pre_executor_check(
                executor.controller_id, request, state
            )
            if not ok:
                self._emit(
                    ExecutorEvent(
                        kind="spawn_rejected",
                        executor_id=executor.id,
                        controller_id=executor.controller_id,
                        reason=reason,
                    )
                )
                return False
        self._executors[executor.id] = executor
        try:
            await executor.on_create()
        except ExecutorFailed as exc:
            self._emit(
                ExecutorEvent(
                    kind="failed",
                    executor_id=executor.id,
                    controller_id=executor.controller_id,
                    close_type=CloseType.FAILED,
                    reason=str(exc),
                )
            )
            return False
        self._emit(
            ExecutorEvent(
                kind="spawned",
                executor_id=executor.id,
                controller_id=executor.controller_id,
            )
        )
        return True

    async def tick(self, executor_id: str) -> None:
        ex = self._executors.get(executor_id)
        if ex is None or ex.is_terminated:
            return
        try:
            await ex.on_tick()
        except ExecutorFailed as exc:
            self._emit(
                ExecutorEvent(
                    kind="failed",
                    executor_id=ex.id,
                    controller_id=ex.controller_id,
                    close_type=CloseType.FAILED,
                    reason=str(exc),
                )
            )
            return
        self._emit(
            ExecutorEvent(kind="tick", executor_id=ex.id, controller_id=ex.controller_id)
        )

    async def stop(
        self, executor_id: str, close_type: CloseType = CloseType.EARLY_STOP
    ) -> bool:
        ex = self._executors.get(executor_id)
        if ex is None:
            return False
        await ex.on_stop(close_type)
        self._emit(
            ExecutorEvent(
                kind="stopped",
                executor_id=ex.id,
                controller_id=ex.controller_id,
                close_type=ex.close_type,
            )
        )
        return True

    async def stop_controller(
        self, controller_id: str, close_type: CloseType = CloseType.EARLY_STOP
    ) -> int:
        """Batched cancel: stop every active executor for a controller
        concurrently."""
        targets = self.list(controller_id, active_only=True)
        await asyncio.gather(*(self.stop(e.id, close_type) for e in targets))
        return len(targets)

    # -- events -----------------------------------------------------------
    def _emit(self, event: ExecutorEvent) -> None:
        self.event_log.append(event)
        self._queue.put_nowait(event)

    async def events(self) -> AsyncIterator[ExecutorEvent]:
        while True:
            yield await self._queue.get()

    def drain_events(self) -> List[ExecutorEvent]:
        """Non-blocking snapshot-and-clear of the queued events (test helper)."""
        out: List[ExecutorEvent] = []
        while not self._queue.empty():
            out.append(self._queue.get_nowait())
        return out
