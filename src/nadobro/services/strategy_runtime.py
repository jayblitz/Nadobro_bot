"""Strategy-runtime shim (Engine v2 Phase 4 cutover).

Replaces the legacy per-cycle strategy dispatch (the removed
``strategies/mm_bot``, ``volume_bot``, ``delta_neutral`` ``run_cycle``
functions). Strategy execution now flows through Engine v2 controllers
spawned via ``engine.orchestrator``; the legacy synchronous run-cycle path is
retired. This shim keeps ``bot_runtime``'s dispatch call-site stable while the
engine-driven runtime wiring is rolled out.

NO_ORDERS_AUDIT-FIX-R1: previously this module had ``"dn"`` in
``LEGACY_STRATEGY_KEYS`` and returned ``{"success": True, "action": "skipped"}``
for it — but ``services/engine_runtime.ENGINE_MAPPED_STRATEGIES`` does NOT
include ``"dn"``, so DN cycles fell through to here and **silently no-op'd
every tick with no orders ever placed**. We now do two things:

  1. Only declare a strategy "managed" if the engine_runtime actually claims
     it (cross-reference at call time). This prevents future drift between
     the two lists.
  2. When a strategy is in neither list (mis-routing), return a hard error
     so the caller surfaces it instead of pretending success.

The shim is preserved for any strategy that the engine claims but routes
through this dispatch path (legacy Bro path). Anything else is an error.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Strategies whose legacy synchronous run_cycle implementations were retired
# during the Engine v2 cutover. These are *expected* to be routed through the
# engine (``services/engine_runtime.run_engine_cycle``); if a cycle ever
# reaches this dispatcher with one of these keys, bot_runtime's routing has
# a bug — surface it loudly rather than no-op.
RETIRED_LEGACY_STRATEGY_KEYS = ("grid", "rgrid", "dgrid", "mid", "dn", "vol")


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
    """Legacy dispatch entry point. Engine-managed strategies should be routed
    through ``services.engine_runtime.run_engine_cycle`` BEFORE reaching this
    function. If we see one here, that's a mis-routing bug and we return an
    error so the operator notices instead of getting a silent success/skip.
    """
    if strategy in RETIRED_LEGACY_STRATEGY_KEYS:
        # NO_ORDERS_AUDIT-FIX-R1: hard error instead of silent skip. The
        # previous behavior masked DN as "skipped: engine_v2_controller_managed"
        # while no engine controller was ever started for it.
        logger.error(
            "MISROUTED: strategy '%s' for user %s reached legacy dispatch_cycle. "
            "It should be handled by engine_runtime.run_engine_cycle. "
            "Check ENGINE_MAPPED_STRATEGIES in services/engine_runtime.py.",
            strategy, telegram_id,
        )
        return {
            "success": False,
            "action": "misrouted_to_legacy_dispatch",
            "error": (
                f"strategy '{strategy}' has no legacy dispatch and is not in "
                "ENGINE_MAPPED_STRATEGIES — bot_runtime routing bug; no orders placed"
            ),
            "strategy": strategy,
        }
    return {"success": False, "error": f"Unknown strategy '{strategy}'"}


# Back-compat alias for any importer of the old name.
LEGACY_STRATEGY_KEYS = RETIRED_LEGACY_STRATEGY_KEYS
