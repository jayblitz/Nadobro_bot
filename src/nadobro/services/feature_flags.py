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
    return env_flag("NADO_FEATURE_STUDIO", False)


def legacy_bro_autoloop_enabled() -> bool:
    return env_flag("NADO_LEGACY_BRO_AUTOLOOP", False)


def studio_condition_interval_seconds() -> int:
    raw = (os.environ.get("STUDIO_CONDITION_INTERVAL_SECONDS") or "60").strip()
    try:
        return max(5, int(float(raw)))
    except ValueError:
        return 60
