"""Controller base — long-running strategy with on_start / on_tick / on_stop.

A controller owns a stable ``id`` (used to filter the orchestrator's executor
pool), splits its parameters into **configs** (strategy knobs) and **limits**
(``RiskLimits`` consumed by the Risk Engine), and spawns Executors via the
orchestrator. Lifecycle state (CREATED → ACTIVE → STOPPED/FAILED) is driven by
the orchestrator's spawn/stop/tick methods.

Implemented in Phase 4.
"""
from __future__ import annotations

import abc
import time
import uuid
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from src.nadobro.engine.adapter.base import NadoAdapterBase
from src.nadobro.engine.executor_base import Executor
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.types import RiskLimits

if TYPE_CHECKING:  # avoid a runtime import cycle (orchestrator imports nothing here)
    from src.nadobro.engine.orchestrator import ExecutorOrchestrator


class ControllerState(Enum):
    CREATED = "CREATED"
    ACTIVE = "ACTIVE"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


class Controller(abc.ABC):
    def __init__(
        self,
        *,
        user_id: int,
        name: str,
        orchestrator: "ExecutorOrchestrator",
        adapter: NadoAdapterBase,
        inventory: Optional[InventoryRepository] = None,
        configs: Optional[Dict[str, object]] = None,
        limits: Optional[RiskLimits] = None,
        controller_id: Optional[str] = None,
    ) -> None:
        self.id = controller_id or f"{name}-{uuid.uuid4().hex[:8]}"
        self.user_id = user_id
        self.name = name
        self.orchestrator = orchestrator
        self.adapter = adapter
        self.inventory = inventory
        self.configs: Dict[str, object] = configs or {}
        self.limits = limits or RiskLimits()
        self.state = ControllerState.CREATED
        self.started_at: Optional[float] = None
        self.stopped_at: Optional[float] = None

    # -- state transitions (called by the orchestrator) -------------------
    @property
    def is_active(self) -> bool:
        return self.state is ControllerState.ACTIVE

    def _set_active(self) -> None:
        self.state = ControllerState.ACTIVE
        self.started_at = time.time()

    def _set_stopped(self) -> None:
        self.state = ControllerState.STOPPED
        self.stopped_at = time.time()

    def _set_failed(self) -> None:
        self.state = ControllerState.FAILED
        self.stopped_at = time.time()

    # -- helpers ----------------------------------------------------------
    async def spawn_executor(
        self, executor: Executor, request: Optional[ExecutorRequest] = None
    ) -> bool:
        return await self.orchestrator.spawn(executor, request)

    def my_executors(self, active_only: bool = True) -> List[Executor]:
        return self.orchestrator.list(self.id, active_only=active_only)

    def cfg(self, key: str, default: Any = None) -> Any:
        return self.configs.get(key, default)

    # -- lifecycle hooks --------------------------------------------------
    @abc.abstractmethod
    async def on_start(self) -> None:
        ...

    @abc.abstractmethod
    async def on_tick(self) -> None:
        ...

    async def on_stop(self, reason: str = "stopped") -> None:
        """Default: rely on the orchestrator to batch-cancel child executors."""
        return None
