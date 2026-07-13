"""Service-side facade for the Desk plan model.

The pure model (dataclasses, validation, trigger resolution) lives in
:mod:`src.nadobro.engine.desk_plan` so the engine's DeskController can use it
without importing services. This module re-exports it for the service layer
(parser, store, handlers) and adds the helpers that touch config/env.
"""
from __future__ import annotations


from src.nadobro.utils.env import env_int
from src.nadobro.engine.desk_plan import (  # noqa: F401 - re-exports
    ACTIVE_STATUSES,
    ALGOS,
    DEFAULT_TWAP_INTERVAL_SECONDS,
    EXEC_MODES,
    MARKETS,
    MAX_PLAN_DURATION_MINUTES,
    MAX_TRIGGER_WAIT_MINUTES,
    MAX_TWAP_SLICES,
    MIN_TWAP_DURATION_MINUTES,
    MIN_TWAP_INTERVAL_SECONDS,
    SIDES,
    ST_AWAITING_TRIGGER,
    ST_CANCELLED,
    ST_COMPLETED,
    ST_DRAFT,
    ST_FAILED,
    ST_RUNNING,
    TERMINAL_STATUSES,
    TRIGGER_KINDS,
    EntryTrigger,
    ExecutionPlan,
    ExitPlan,
    describe_trigger,
    resolve_trigger,
    trigger_satisfied,
    validate_plan,
)


def daily_plan_cap() -> int:
    """Max NEW plans per user per UTC day (default 5; env-overridable)."""
    try:
        return max(1, env_int("NADO_DESK_DAILY_PLAN_CAP", 5))
    except (TypeError, ValueError):
        return 5


def resolve_catalogs(network: str, client=None) -> tuple[set[str], set[str]]:
    """(perp_symbols, spot_symbols) from the live catalog, uppercased.

    Spot universe is the same live list the Volume Bot trades against, so new
    stock tokens (QQQX, SPYX, ...) appear automatically. USDT0 is quote-only.
    """
    from src.nadobro.config import get_perp_products, list_volume_spot_product_names

    perps = {str(s).upper().strip() for s in (get_perp_products(network=network, client=client) or []) if s}
    spots = {
        str(s).upper().strip()
        for s in (list_volume_spot_product_names(network=network, client=client) or [])
        if s
    }
    spots.discard("USDT0")
    return perps, spots
