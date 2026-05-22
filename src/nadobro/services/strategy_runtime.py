"""Strategy-runtime shim (Engine v2 Phase 4 cutover).

Replaces the legacy per-cycle strategy dispatch (the removed
``strategies/mm_bot``, ``volume_bot``, ``delta_neutral`` ``run_cycle``
functions). Strategy execution now flows through Engine v2 controllers
spawned via ``engine.orchestrator``; the legacy synchronous run-cycle path is
retired. This shim keeps ``bot_runtime``'s dispatch call-site stable while the
engine-driven runtime wiring is rolled out.

NOTE: with no strategies running pre-launch, the legacy dispatch is a no-op
that reports the strategy as engine-managed. Wiring the live start/stop/tick
flow to ``orchestrator.spawn_controller`` / ``tick_controller`` is the
follow-up integration tracked on the Engine v2 epic.
"""
from __future__ import annotations

from typing import Optional

LEGACY_STRATEGY_KEYS = ("grid", "rgrid", "dgrid", "mid", "dn", "vol")


def dispatch_cycle(
    strategy: str,
    telegram_id: int,
    network: str,
    state: dict,
    *,
    client: object = None,
    mid: float = 0.0,
    product_id: int = 0,
    product: str = "",
    open_orders: Optional[list] = None,
) -> dict:
    """Legacy dispatch entry point. Engine-managed strategies no-op here; the
    orchestrator/controllers own execution."""
    if strategy in LEGACY_STRATEGY_KEYS:
        return {"success": True, "action": "skipped", "reason": "engine_v2_controller_managed"}
    return {"success": False, "error": f"Unknown strategy '{strategy}'"}
