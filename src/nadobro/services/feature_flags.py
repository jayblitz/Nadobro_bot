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


def legacy_bro_autoloop_enabled() -> bool:
    return env_flag("NADO_LEGACY_BRO_AUTOLOOP", False)


def portfolio_sync_enabled() -> bool:
    # Default ON per the workflow plan so the combined Positions screen
    # tracks fills in near real time without requiring an environment
    # override. Operators can disable by setting the env flag to 0.
    return env_flag("NADO_PORTFOLIO_SYNC", True)


def portfolio_ws_enabled() -> bool:
    return env_flag("NADO_PORTFOLIO_WS", False)


def vault_deposit_watch_enabled() -> bool:
    return env_flag("NADO_VAULT_DEPOSIT_WATCH", True)


def vault_deposit_watch_interval_seconds() -> int:
    raw = (os.environ.get("NADO_VAULT_DEPOSIT_WATCH_SECONDS") or "60").strip()
    try:
        return max(30, int(float(raw)))
    except ValueError:
        return 60


def portfolio_sync_interval_seconds() -> int:
    # Default 5s for near-real-time Positions tracking. Floor is also 5s
    # so we don't hammer Nado faster than the snapshot cache TTL.
    raw = (os.environ.get("NADO_PORTFOLIO_SYNC_SECONDS") or "5").strip()
    try:
        return max(5, int(float(raw)))
    except ValueError:
        return 5


def dgrid_intelligence_enabled() -> bool:
    """Master switch for the D-Grid intelligence upgrade.

    When True, mm_bot.run_cycle activates the regime classifier
    (_regime.py), the adaptive layer-sizing engine (_layer_sizing.py), and
    the active position manager (_position_manager.py) for any session whose
    ``strategy == "dgrid"``. Individual sessions can override by setting
    ``state["dgrid_intelligence_enabled"]`` (True/False).
    """
    return env_flag("NADO_DGRID_INTELLIGENCE", False)
