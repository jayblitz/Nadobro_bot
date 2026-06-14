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
        # Set by the orchestrator when on_start raises, so the runtime can
        # surface why a start failed (e.g. a leg rejected by the risk gate).
        self._start_error: Optional[str] = None

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

    # -- regime gate (grid family + MM) ------------------------------------
    # Pause semantics: PAUSE blocks NEW opening quotes only. Existing
    # positions, close legs, barriers, and the inventory cap keep running —
    # pause is "stop digging", never "flatten". The gate transition is
    # surfaced via ``consume_gate_event`` so the runtime can notify the user
    # exactly once per flip.
    async def evaluate_quote_gate(self, trading_pair: str, *, pause_on_trend: bool = True) -> str:
        """Refresh ``self.gate_verdict`` from the regime-gate routine.

        Requires ``regime_gate_enabled`` in configs and a ``candle_provider``;
        without either, the gate stays inactive (verdict QUOTE) — a missing
        candle feed must degrade to ungated behavior, not silence.

        ``pause_on_trend=False`` (dgrid): the controller's own regime
        selector TRADES trends (ReverseGrid), so a trend verdict is treated
        as QUOTE and only breakout/expansion (no acceptance anywhere) pauses.

        Transition discipline is asymmetric: a PAUSE commits IMMEDIATELY
        (protection first), but resuming to QUOTE requires
        ``gate_resume_confirm_ticks`` consecutive QUOTE verdicts — a regime
        flickering at the threshold must not churn cancels or spam the user
        with pause/resume notifications.
        """
        if not getattr(self, "gate_verdict", None):
            self.gate_verdict: str = "QUOTE"
            self.gate_reason: str = ""
            self.gate_atr_pct: float = 0.0
            self._gate_event: Optional[Dict[str, str]] = None
            self._gate_resume_streak: int = 0
        if not bool(self.cfg("regime_gate_enabled", False)):
            return self.gate_verdict
        provider = self.cfg("candle_provider")
        if provider is None:
            return self.gate_verdict
        try:
            import inspect as _inspect

            from src.nadobro.engine.routines import regime_gate

            raw = provider(trading_pair)  # type: ignore[operator]
            if _inspect.isawaitable(raw):
                raw = await raw
            result = await regime_gate.run(trading_pair, list(raw or []))
        except Exception:  # policy: degrade-ok(gate eval is best-effort; stay on last verdict)
            return self.gate_verdict
        new_verdict = str(result.get("verdict") or "QUOTE")
        new_reason = str(result.get("reason") or "")
        self.gate_atr_pct = float(str(result.get("atr_pct") or 0.0))
        if not pause_on_trend and new_reason in ("trending_up", "trending_down"):
            new_verdict, new_reason = "QUOTE", ""

        if new_verdict == "PAUSE":
            self._gate_resume_streak = 0
            if self.gate_verdict != "PAUSE":
                self._gate_event = {"state": "PAUSE", "reason": new_reason}
            self.gate_verdict, self.gate_reason = "PAUSE", new_reason
            return self.gate_verdict

        # new_verdict == QUOTE
        if self.gate_verdict == "PAUSE":
            confirm = int(self.cfg("gate_resume_confirm_ticks", 2) or 2)
            self._gate_resume_streak += 1
            if self._gate_resume_streak < max(1, confirm):
                return self.gate_verdict  # stay paused until the range confirms
            self._gate_event = {"state": "QUOTE", "reason": new_reason}
        self._gate_resume_streak = 0
        self.gate_verdict, self.gate_reason = "QUOTE", new_reason
        return self.gate_verdict

    @property
    def gate_paused(self) -> bool:
        return getattr(self, "gate_verdict", "QUOTE") == "PAUSE"

    def consume_gate_event(self) -> Optional[Dict[str, str]]:
        """Pop the pending QUOTE<->PAUSE transition (None if no flip)."""
        event = getattr(self, "_gate_event", None)
        self._gate_event = None
        return event

    # -- inventory cap (backstop behind the gate) ---------------------------
    # Suppress the side that WORSENS net exposure once it exceeds
    # ``max_net_exposure_pct`` of allocated margin; re-allow below
    # ``resume_frac`` of the cap (hysteresis, no flapping). Reduce-only
    # quoting always continues — this caps how lopsided the book can get
    # before the session stop would have to act.
    def exposure_allowed_sides(self, trading_pair: str, mid: object) -> Dict[str, bool]:
        from decimal import Decimal

        allowed = {"buy": True, "sell": True}
        if self.inventory is None:
            return allowed
        cap_pct = self.cfg("max_net_exposure_pct")
        margin = self.cfg("margin_quote")
        try:
            cap_frac = Decimal(str(cap_pct)) / Decimal(100)
            margin_quote = Decimal(str(margin))
            mid_d = Decimal(str(mid))
        except Exception:  # policy: degrade-ok(cap unset/malformed; cap inactive)
            return allowed
        if cap_frac <= 0 or margin_quote <= 0 or mid_d <= 0:
            return allowed
        net_quote = self.inventory.get(self.user_id, trading_pair, self.id).net_amount_base * mid_d
        cap_quote = margin_quote * cap_frac
        resume_quote = cap_quote * Decimal(str(self.cfg("exposure_resume_frac", "0.7")))
        capped = bool(getattr(self, "_exposure_capped", False))
        if abs(net_quote) >= cap_quote:
            capped = True
        elif abs(net_quote) <= resume_quote:
            capped = False
        self._exposure_capped = capped
        self.exposure_net_quote = net_quote
        if capped:
            if net_quote > 0:
                allowed["buy"] = False   # long over cap: only reduce
            else:
                allowed["sell"] = False  # short over cap: only reduce
        return allowed

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
