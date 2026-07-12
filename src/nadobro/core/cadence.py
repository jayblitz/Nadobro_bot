"""Per-strategy cycle cadence — how often a strategy is allowed to re-run.

Single source of truth for the "raise the clock" behaviour (Option 1). Test
scope: ``rgrid`` and ``mid`` ONLY. Their effective interval is capped to
``NADO_FAST_CADENCE_SECONDS`` (default 8) and floored at
``NADO_FAST_CADENCE_FLOOR_SECONDS`` (default 3, to respect venue rate limits).
An explicitly-faster configured interval is still honoured (we take the min).
Every other strategy keeps its configured interval unchanged.

Leaf module: stdlib only, no intra-package imports, so both the central
scheduler and the strategy runtime can import it without an import cycle.
"""

from __future__ import annotations

import os

# Strategies whose cadence Option 1 accelerates. Grid/dgrid/dn stay on the
# default cadence; vol joined 2026-07-11 — its v3 requote/cross deadlines are
# timer-driven and a 20s tick made every reaction lag two buckets.
FAST_CADENCE_STRATEGIES = frozenset({"rgrid", "mid", "vol"})


def _env_float(name: str, default: float) -> float:
    try:
        raw = os.environ.get(name)
        if raw is None or str(raw).strip() == "":
            return float(default)
        return float(str(raw).strip())
    except (TypeError, ValueError):
        return float(default)


def effective_interval_seconds(strategy: str, raw_interval_seconds: object) -> float:
    """Return the cadence (seconds between cycles) to actually enforce.

    For ``rgrid``/``mid`` this is ``min(configured, fast)`` floored at the
    rate-limit floor; for every other strategy it is the configured interval
    unchanged.
    """
    try:
        raw = float(raw_interval_seconds)
    except (TypeError, ValueError):
        raw = 60.0
    strat = str(strategy or "").lower().strip()
    if strat in FAST_CADENCE_STRATEGIES:
        fast = _env_float("NADO_FAST_CADENCE_SECONDS", 8.0)
        floor = _env_float("NADO_FAST_CADENCE_FLOOR_SECONDS", 3.0)
        return max(floor, min(raw, fast))
    return raw
