"""Risk Engine — pre-tick gates (daily_pnl floor, drawdown cap, daily cost
cap) and per-executor gates (max_open_executors, max_single_order_quote,
max_position_size_quote), plus a single kill switch.

The kill switch is persisted through a pluggable :class:`KillSwitchStore`. In
Phase 1 the default store is in-memory; the Phase 2 DB-backed store will make
the switch survive process restarts (constructing a new RiskEngine over the
same store keeps the engaged state — see ``test_risk``).

Current risk telemetry (daily PnL, drawdown, exposure, executor count) is
passed in as a :class:`RiskState` snapshot rather than read from the DB, so
the gates are unit-testable without persistence (Phase 2 wires the provider).

Implemented in Phase 1.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Tuple

from src.nadobro.engine.types import RiskLimits, RiskState, _dec

CheckResult = Tuple[bool, Optional[str]]


class KillSwitchStore:
    """Interface for kill-switch persistence."""

    def is_engaged(self) -> bool:  # pragma: no cover - interface
        raise NotImplementedError

    def engage(self, reason: str) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def disengage(self) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def reason(self) -> Optional[str]:  # pragma: no cover - interface
        raise NotImplementedError


class InMemoryKillSwitchStore(KillSwitchStore):
    def __init__(self) -> None:
        self._engaged = False
        self._reason: Optional[str] = None
        self._lock = threading.RLock()

    def is_engaged(self) -> bool:
        with self._lock:
            return self._engaged

    def engage(self, reason: str) -> None:
        with self._lock:
            self._engaged = True
            self._reason = reason

    def disengage(self) -> None:
        with self._lock:
            self._engaged = False
            self._reason = None

    def reason(self) -> Optional[str]:
        with self._lock:
            return self._reason


@dataclass
class ExecutorRequest:
    """The slice of an executor's config the Risk Engine gates on spawn."""

    order_amount_quote: Decimal
    position_size_quote: Decimal = Decimal(0)

    def __post_init__(self) -> None:
        self.order_amount_quote = _dec(self.order_amount_quote)
        self.position_size_quote = _dec(self.position_size_quote)


class RiskEngine:
    def __init__(
        self,
        limits: Optional[RiskLimits] = None,
        kill_switch: Optional[KillSwitchStore] = None,
    ) -> None:
        self.limits = limits or RiskLimits()
        self.kill_switch: KillSwitchStore = kill_switch or InMemoryKillSwitchStore()

    # -- kill switch ------------------------------------------------------
    def kill_switch_on(self, reason: str) -> None:
        self.kill_switch.engage(reason)

    def kill_switch_off(self) -> None:
        self.kill_switch.disengage()

    def is_killed(self) -> bool:
        return self.kill_switch.is_engaged()

    def _kill_block(self) -> CheckResult:
        if self.kill_switch.is_engaged():
            return False, f"kill_switch:{self.kill_switch.reason()}"
        return True, None

    # -- gates ------------------------------------------------------------
    def pre_tick_check(self, controller_id: str, state: RiskState) -> CheckResult:
        ok, reason = self._kill_block()
        if not ok:
            return ok, reason
        lim = self.limits
        if (
            lim.daily_pnl_floor_quote is not None
            and state.daily_pnl_quote <= lim.daily_pnl_floor_quote
        ):
            return False, "daily_pnl_floor"
        if lim.max_drawdown_pct is not None and state.drawdown_pct >= lim.max_drawdown_pct:
            return False, "max_drawdown"
        if (
            lim.daily_cost_cap_usd is not None
            and state.daily_cost_usd >= lim.daily_cost_cap_usd
        ):
            return False, "daily_cost_cap"
        return True, None

    def pre_executor_check(
        self, controller_id: str, request: ExecutorRequest, state: RiskState
    ) -> CheckResult:
        ok, reason = self._kill_block()
        if not ok:
            return ok, reason
        lim = self.limits
        if (
            lim.max_open_executors is not None
            and state.executor_count >= lim.max_open_executors
        ):
            return False, "max_open_executors"
        if (
            lim.max_single_order_quote is not None
            and request.order_amount_quote > lim.max_single_order_quote
        ):
            return False, "max_single_order_quote"
        if (
            lim.max_position_size_quote is not None
            and request.position_size_quote > lim.max_position_size_quote
        ):
            return False, "max_position_size_quote"
        return True, None
