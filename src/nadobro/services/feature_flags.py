"""Environment-backed feature flags for additive Nadobro features."""

from __future__ import annotations

import os


def env_flag(name: str, default: bool = False) -> bool:
    """Return a boolean env flag using the project's existing truthy style."""
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def time_limit_enabled() -> bool:
    return env_flag("NADO_FEATURE_TIME_LIMIT", True)


def studio_enabled() -> bool:
    # Default-flipped (CEO directive 2026-05): Strategy Studio is now the canonical
    # free-text trading experience that replaces legacy Bro Mode. Operators can still
    # disable explicitly with NADO_FEATURE_STUDIO=false to roll back temporarily.
    return env_flag("NADO_FEATURE_STUDIO", True)


def legacy_bro_autoloop_enabled() -> bool:
    return env_flag("NADO_LEGACY_BRO_AUTOLOOP", False)


def studio_condition_interval_seconds() -> int:
    raw = (os.environ.get("STUDIO_CONDITION_INTERVAL_SECONDS") or "60").strip()
    try:
        return max(5, int(float(raw)))
    except ValueError:
        return 60


def portfolio_sync_enabled() -> bool:
    return env_flag("NADO_PORTFOLIO_SYNC", False)


def portfolio_ws_enabled() -> bool:
    return env_flag("NADO_PORTFOLIO_WS", False)


def portfolio_sync_interval_seconds() -> int:
    raw = (os.environ.get("NADO_PORTFOLIO_SYNC_SECONDS") or "30").strip()
    try:
        return max(5, int(float(raw)))
    except ValueError:
        return 30


def dgrid_intelligence_enabled() -> bool:
    """Master switch for the D-Grid intelligence upgrade.

    When True, mm_bot.run_cycle activates the regime classifier
    (_regime.py), the adaptive layer-sizing engine (_layer_sizing.py), and
    the active position manager (_position_manager.py) for any session whose
    ``strategy == "dgrid"``. Individual sessions can override by setting
    ``state["dgrid_intelligence_enabled"]`` (True/False).
    """
    return env_flag("NADO_DGRID_INTELLIGENCE", False)
