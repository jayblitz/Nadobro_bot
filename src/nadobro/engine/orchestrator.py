"""Executor Orchestrator — single supervisor that owns executor lifecycles.

Supports spawn / stop / list (filtered by ``controller_id``), an event bus
(queue + inspectable log), batched cancel via ``asyncio.gather``, and consults
the Risk Engine before each spawn. Enforces a process-level kill switch.

Implemented in Phase 1.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, AsyncIterator, Deque, Dict, List, Optional

from src.nadobro.engine.executor_base import Executor, ExecutorFailed
from src.nadobro.engine.risk import ExecutorRequest, RiskEngine
from src.nadobro.engine.types import CloseType, RiskState

if TYPE_CHECKING:
    from src.nadobro.engine.controllers.controller_base import Controller

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default


# BUG-ORC-1 fix: cap the orchestrator's in-memory event_log and event queue.
# Previously both were unbounded — long-running deployments leaked memory.
DEFAULT_EVENT_LOG_LIMIT = _env_int("NADO_ORCH_EVENT_LOG_LIMIT", 10000)
DEFAULT_EVENT_QUEUE_LIMIT = _env_int("NADO_ORCH_EVENT_QUEUE_LIMIT", 5000)


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
        *,
        event_log_limit: int = DEFAULT_EVENT_LOG_LIMIT,
        event_queue_limit: int = DEFAULT_EVENT_QUEUE_LIMIT,
    ) -> None:
        self.risk = risk_engine
        # callable(controller_id) -> RiskState; defaults to an empty snapshot
        self.risk_state_provider = risk_state_provider
        self._executors: Dict[str, Executor] = {}
        self._controllers: Dict[str, "Controller"] = {}
        # Bounded queue: when full, oldest events are dropped (with a log).
        self._queue: "asyncio.Queue[ExecutorEvent]" = asyncio.Queue(
            maxsize=max(1, event_queue_limit)
        )
        self._event_queue_limit = max(1, event_queue_limit)
        # event_log is a bounded ring buffer.
        self._event_log_limit = max(1, event_log_limit)
        self._event_log_deque: Deque[ExecutorEvent] = deque(maxlen=self._event_log_limit)
        self._queue_overflows = 0
        self._killed = False
        self._kill_reason: Optional[str] = None

    @property
    def event_log(self) -> List[ExecutorEvent]:
        """Snapshot of the bounded event log. Mutating the returned list does
        not affect the orchestrator's internal ring buffer."""
        return list(self._event_log_deque)

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
        # BUG-RISK-1 fix: roll over daily counters on a UTC date boundary so
        # yesterday's daily-pnl floor / cost-cap doesn't keep the gate armed
        # into a new trading day.
        today = time.strftime("%Y-%m-%d", time.gmtime())
        state = state.rolled_over(today)
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

    # -- controller management -------------------------------------------
    async def spawn_controller(self, controller: "Controller") -> bool:
        if self.is_killed:
            self._emit(ExecutorEvent(kind="controller_rejected", controller_id=controller.id, reason="kill_switch"))
            return False
        # BUG-CC-2 fix: register first so the controller can spawn child
        # executors during on_start (their controller_id resolves via the
        # registry), but if on_start raises, tear down any child executors
        # the controller managed to spawn before bailing — otherwise the
        # failed controller leaves live orders on the venue with no owner.
        self._controllers[controller.id] = controller
        try:
            await controller.on_start()
        except Exception as exc:  # noqa: BLE001
            controller._set_failed()
            # Stop any child executors the controller created mid-start.
            child_ids = [
                ex.id for ex in self.list(controller.id, active_only=True)
            ]
            if child_ids:
                try:
                    await asyncio.gather(
                        *(self.stop(eid, CloseType.FAILED) for eid in child_ids),
                        return_exceptions=True,
                    )
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "controller %s on_start rollback: child stop failed",
                        controller.id, exc_info=True,
                    )
            self._emit(ExecutorEvent(kind="controller_failed", controller_id=controller.id, reason=str(exc)))
            return False
        controller._set_active()
        self._emit(ExecutorEvent(kind="controller_spawned", controller_id=controller.id))
        return True

    async def tick_controller(self, controller_id: str) -> None:
        controller = self._controllers.get(controller_id)
        if controller is None or not controller.is_active:
            return
        if self.risk is not None:
            ok, reason = self.risk.pre_tick_check(controller_id, self._state_for(controller_id))
            if not ok:
                self._emit(ExecutorEvent(kind="controller_skipped", controller_id=controller_id, reason=reason))
                return
        try:
            await controller.on_tick()
        except Exception as exc:  # noqa: BLE001
            controller._set_failed()
            self._emit(ExecutorEvent(kind="controller_failed", controller_id=controller_id, reason=str(exc)))
            return
        self._emit(ExecutorEvent(kind="controller_tick", controller_id=controller_id))

    def list_controllers(self, user_id: Optional[int] = None) -> List["Controller"]:
        vals = list(self._controllers.values())
        if user_id is not None:
            vals = [c for c in vals if c.user_id == user_id]
        return vals

    def get_controller_status(self, controller_id: str) -> Optional[Dict[str, object]]:
        controller = self._controllers.get(controller_id)
        if controller is None:
            return None
        return {
            "id": controller.id,
            "name": controller.name,
            "user_id": controller.user_id,
            "state": controller.state.value,
            "open_executors": len(self.list(controller_id, active_only=True)),
        }

    async def stop_controller(
        self,
        controller_id: str,
        close_type: CloseType = CloseType.EARLY_STOP,
        reason: str = "stopped",
    ) -> int:
        """Stop a controller (if registered) and batch-cancel all of its
        active executors concurrently. Returns the executors stopped."""
        controller = self._controllers.get(controller_id)
        if controller is not None:
            try:
                await controller.on_stop(reason)
            except Exception:  # noqa: BLE001
                pass
            controller._set_stopped()
        targets = self.list(controller_id, active_only=True)
        await asyncio.gather(*(self.stop(e.id, close_type) for e in targets))
        self._emit(ExecutorEvent(kind="controller_stopped", controller_id=controller_id, reason=reason))
        return len(targets)

    # -- events -----------------------------------------------------------
    def _emit(self, event: ExecutorEvent) -> None:
        # Ring buffer (deque maxlen) drops oldest automatically.
        self._event_log_deque.append(event)
        # Queue is bounded; on overflow, drop the oldest queued event so the
        # newest one can fit. This preserves a bounded *recent* window for
        # consumers without blocking emitters.
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self._queue.put_nowait(event)
            except asyncio.QueueFull:
                # Queue still full (raced with another producer). Drop the
                # new event and continue; the event_log retains a copy.
                pass
            self._queue_overflows += 1
            if self._queue_overflows % 100 == 1:
                logger.warning(
                    "orchestrator event queue overflow (count=%d, cap=%d) — "
                    "downstream consumer is too slow",
                    self._queue_overflows, self._event_queue_limit,
                )

    async def events(self) -> AsyncIterator[ExecutorEvent]:
        while True:
            yield await self._queue.get()

    def drain_events(self) -> List[ExecutorEvent]:
        """Non-blocking snapshot-and-clear of the queued events (test helper)."""
        out: List[ExecutorEvent] = []
        while not self._queue.empty():
            out.append(self._queue.get_nowait())
        return out

    @property
    def queue_overflow_count(self) -> int:
        return self._queue_overflows
