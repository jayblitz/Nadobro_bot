"""Executor base class — abstract lifecycle (CREATED -> ACTIVE -> TERMINATED),
standardized metrics, ``controller_id`` linkage, and a retry policy (up to 3
attempts with exponential backoff on :class:`AdapterError`; on exhaustion the
executor transitions to TERMINATED with ``close_type=FAILED``).

Implemented in Phase 1.
"""
from __future__ import annotations

import abc
import asyncio
import time
import uuid
from decimal import Decimal
from typing import Awaitable, Callable, Dict, Optional, TypeVar

from src.nadobro.engine.adapter.base import AdapterError, Fill, NadoAdapterBase
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.types import CloseType, ExecutorState, TradeType

T = TypeVar("T")


class ExecutorFailed(Exception):
    """Raised when an executor exhausts its retry budget on an adapter call."""


class Executor(abc.ABC):
    MAX_ATTEMPTS = 3
    BASE_BACKOFF = 0.01  # seconds; exponential per attempt

    # Regime gate / inventory cap: set by the owning controller to stop NEW
    # opening orders while exits, close legs, and stops keep running.
    # Executors that don't open new exposure simply ignore it.
    suppress_new_entries: bool = False

    def __init__(
        self,
        *,
        user_id: int,
        controller_id: str,
        trading_pair: str,
        adapter: NadoAdapterBase,
        inventory: Optional[InventoryRepository] = None,
        keep_position: bool = True,
        executor_id: Optional[str] = None,
    ) -> None:
        self.id = executor_id or uuid.uuid4().hex
        self.user_id = user_id
        self.controller_id = controller_id
        self.connector_name = "nado"
        self.trading_pair = trading_pair
        self.adapter = adapter
        self.inventory = inventory
        self.keep_position = keep_position
        self.state = ExecutorState.CREATED
        self.close_type: Optional[CloseType] = None
        self.created_at = time.time()
        self.terminated_at: Optional[float] = None
        self.retries = 0
        self._net_pnl_quote = Decimal(0)
        self._fees_paid_quote = Decimal(0)
        self._volume_quote = Decimal(0)
        # Optional bridge to the legacy ``trades_<network>`` reporting tables.
        # Injected by the orchestrator at spawn time (see orchestrator.spawn);
        # ``None`` in unit tests / read-only modes, where recording is a no-op.
        self.trade_recorder: Optional[object] = None

    # -- lifecycle --------------------------------------------------------
    @abc.abstractmethod
    async def on_create(self) -> None:
        ...

    @abc.abstractmethod
    async def on_tick(self) -> None:
        ...

    async def on_stop(self, close_type: CloseType = CloseType.EARLY_STOP) -> None:
        self._terminate(close_type)

    def _activate(self) -> None:
        if self.state is ExecutorState.CREATED:
            self.state = ExecutorState.ACTIVE

    def _terminate(self, close_type: CloseType) -> None:
        if self.state is ExecutorState.TERMINATED:
            return
        self.state = ExecutorState.TERMINATED
        self.close_type = close_type
        self.terminated_at = time.time()

    # -- status -----------------------------------------------------------
    @property
    def is_active(self) -> bool:
        return self.state is ExecutorState.ACTIVE

    @property
    def is_terminated(self) -> bool:
        return self.state is ExecutorState.TERMINATED

    @property
    def duration_seconds(self) -> float:
        end = self.terminated_at if self.terminated_at is not None else time.time()
        return end - self.created_at

    def metrics(self) -> Dict[str, object]:
        return {
            "net_pnl_quote": self._net_pnl_quote,
            "fees_paid_quote": self._fees_paid_quote,
            "volume_quote": self._volume_quote,
            "duration_seconds": self.duration_seconds,
        }

    # -- helpers ----------------------------------------------------------
    async def _guard(self, op: Callable[[], Awaitable[T]], *, label: str = "") -> T:
        """Run an async adapter op with retry + exponential backoff. On
        exhaustion, terminate the executor FAILED and raise ExecutorFailed."""
        last_exc: Optional[Exception] = None
        for attempt in range(self.MAX_ATTEMPTS):
            try:
                return await op()
            except AdapterError as exc:
                last_exc = exc
                self.retries += 1
                if attempt < self.MAX_ATTEMPTS - 1:
                    await asyncio.sleep(self.BASE_BACKOFF * (2 ** attempt))
        self._terminate(CloseType.FAILED)
        raise ExecutorFailed(
            f"{label or 'adapter op'} failed after {self.MAX_ATTEMPTS} attempts: {last_exc}"
        )

    def _record_fill(self, fill: Fill) -> None:
        self._fees_paid_quote += fill.fee_quote
        self._volume_quote += fill.amount_quote
        if self.inventory is not None:
            self.inventory.apply_fill(
                self.user_id,
                self.trading_pair,
                self.controller_id,
                fill.side,
                fill.amount_base,
                fill.amount_quote,
                fill.fee_quote,
                fill.timestamp,
            )
        # Bridge the fill into the legacy reporting tables so /status,
        # /mm_status, /mm_fills, portfolio cards, and the per-session rollup
        # reflect engine strategies. Best-effort: the recorder swallows its own
        # errors, but guard here too so a missing/edge recorder can never break
        # a fill.
        recorder = self.trade_recorder
        if recorder is not None:
            try:
                recorder.record(
                    self.controller_id,
                    self.trading_pair,
                    fill.side,
                    fill.amount_base,
                    fill.price,
                    fill.fee_quote,
                    fill.order_id,
                    fill.timestamp,
                )
            except Exception:  # noqa: BLE001  # policy: degrade-ok(trade-recording is best-effort; the recorder logs its own failures — a fill must never be lost to a reporting-bridge error)
                pass

    def _record_realized(self, amount_quote: Decimal) -> None:
        self._net_pnl_quote += amount_quote
