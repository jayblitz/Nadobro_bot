"""Durable strategy state machine definitions.

Strategies keep their detailed local state, but this module provides a common
phase vocabulary for user-facing status, recovery, and workflow triggers.
"""

from __future__ import annotations

from dataclasses import dataclass


PHASE_IDLE = "idle"
PHASE_STARTING = "starting"
PHASE_SCANNING = "scanning"
PHASE_PLACING = "placing"
PHASE_WAITING_FILL = "waiting_fill"
PHASE_RECONCILING = "reconciling"
PHASE_CLOSING = "closing"
PHASE_PAUSED = "paused"
PHASE_FAILED = "failed"
PHASE_STOPPED = "stopped"

TERMINAL_PHASES = {PHASE_FAILED, PHASE_STOPPED}

LEGAL_TRANSITIONS: dict[str, set[str]] = {
    PHASE_IDLE: {PHASE_STARTING, PHASE_STOPPED},
    PHASE_STARTING: {PHASE_SCANNING, PHASE_FAILED, PHASE_STOPPED},
    PHASE_SCANNING: {PHASE_PLACING, PHASE_WAITING_FILL, PHASE_RECONCILING, PHASE_PAUSED, PHASE_CLOSING, PHASE_FAILED, PHASE_STOPPED},
    PHASE_PLACING: {PHASE_WAITING_FILL, PHASE_RECONCILING, PHASE_FAILED, PHASE_CLOSING},
    PHASE_WAITING_FILL: {PHASE_RECONCILING, PHASE_CLOSING, PHASE_FAILED, PHASE_STOPPED},
    PHASE_RECONCILING: {PHASE_SCANNING, PHASE_CLOSING, PHASE_FAILED, PHASE_STOPPED},
    PHASE_CLOSING: {PHASE_RECONCILING, PHASE_STOPPED, PHASE_FAILED},
    PHASE_PAUSED: {PHASE_SCANNING, PHASE_CLOSING, PHASE_STOPPED, PHASE_FAILED},
    PHASE_FAILED: {PHASE_RECONCILING, PHASE_STOPPED},
    PHASE_STOPPED: {PHASE_STARTING},
}


@dataclass(frozen=True)
class StrategyPhase:
    phase: str
    detail: str = ""
    recoverable: bool = True
    allowed_actions: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "phase": self.phase,
            "detail": self.detail,
            "recoverable": self.recoverable,
            "allowed_actions": list(self.allowed_actions),
        }


def can_transition(current: str, target: str) -> bool:
    current = current or PHASE_IDLE
    target = target or PHASE_IDLE
    return target in LEGAL_TRANSITIONS.get(current, set())


def infer_phase(state: dict) -> StrategyPhase:
    if not state.get("running"):
        reason = state.get("last_error") or state.get("stopped_reason") or "Strategy is not running."
        return StrategyPhase(PHASE_STOPPED, str(reason), recoverable=True, allowed_actions=("start",))
    if state.get("mm_paused"):
        return StrategyPhase(PHASE_PAUSED, str(state.get("mm_pause_reason") or "Paused."), allowed_actions=("resume", "stop", "flatten"))
    if state.get("last_error"):
        return StrategyPhase(PHASE_FAILED, str(state.get("last_error")), recoverable=True, allowed_actions=("recover", "stop", "flatten"))

    vol_phase = str(state.get("vol_phase") or "")
    if vol_phase in {"pending_entry_fill", "pending_close_fill"}:
        return StrategyPhase(PHASE_WAITING_FILL, vol_phase, allowed_actions=("stop", "recover"))
    if vol_phase in {"filled_wait_close"}:
        return StrategyPhase(PHASE_CLOSING, vol_phase, allowed_actions=("stop", "flatten"))

    action = str(state.get("last_action") or "").lower()
    if action in {"open_long", "open_short", "placing", "quote", "placed"}:
        return StrategyPhase(PHASE_PLACING, action, allowed_actions=("stop", "flatten"))
    if action in {"close", "emergency_flatten", "closing"}:
        return StrategyPhase(PHASE_CLOSING, action, allowed_actions=("stop", "flatten", "recover"))
    if action in {"reconcile", "fill_sync", "waiting_limit_close_fill"}:
        return StrategyPhase(PHASE_RECONCILING, action, allowed_actions=("stop", "recover"))
    return StrategyPhase(PHASE_SCANNING, str(state.get("last_action_detail") or "Scanning."), allowed_actions=("pause", "stop", "flatten"))


def apply_phase(state: dict, target: str, detail: str = "") -> dict:
    current = str(state.get("strategy_phase") or PHASE_IDLE)
    if can_transition(current, target) or current == target:
        state["strategy_phase"] = target
        state["strategy_phase_detail"] = detail
    else:
        state["strategy_phase"] = PHASE_FAILED
        state["strategy_phase_detail"] = f"Illegal transition {current} -> {target}"
    return state
