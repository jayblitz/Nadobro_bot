"""Per-strategy cycle cadence — how often a strategy is allowed to re-run.

Single source of truth for the "raise the clock" behaviour. ``rgrid``, ``mid``
and ``vol`` are capped to ``NADO_FAST_CADENCE_SECONDS`` (default 8); an
explicitly-faster configured interval is still honoured (we take the min).
Every other strategy keeps its configured interval — but EVERY strategy is
floored at ``NADO_FAST_CADENCE_FLOOR_SECONDS`` (default 3, venue rate limits),
so fast configured intervals (Turbo Volume writes 5s grid/dgrid cycles) are a
supported, bounded path.

Leaf module: stdlib only, no intra-package imports, so both the central
scheduler and the strategy runtime can import it without an import cycle.
"""

from __future__ import annotations

from src.nadobro.utils.env import env_float as _env_float

# Strategies whose cadence Option 1 accelerates. Grid/dgrid/dn stay on the
# default cadence; vol joined 2026-07-11 — its v3 requote/cross deadlines are
# timer-driven and a 20s tick made every reaction lag two buckets.
FAST_CADENCE_STRATEGIES = frozenset({"rgrid", "mid", "vol"})


def effective_interval_seconds(strategy: str, raw_interval_seconds: object) -> float:
    """Return the cadence (seconds between cycles) to actually enforce.

    For ``rgrid``/``mid``/``vol`` this is ``min(configured, fast)``; every other
    strategy keeps its configured interval. EVERY strategy is floored at the
    rate-limit floor — a hand-typed 1s interval on grid/dgrid previously ran
    unfloored, and the Turbo Volume preset (5s grid/dgrid cycles) makes fast
    configured intervals a supported path rather than an accident.
    """
    try:
        raw = float(raw_interval_seconds)
    except (TypeError, ValueError):
        raw = 60.0
    strat = str(strategy or "").lower().strip()
    floor = _env_float("NADO_FAST_CADENCE_FLOOR_SECONDS", 3.0)
    if strat in FAST_CADENCE_STRATEGIES:
        fast = _env_float("NADO_FAST_CADENCE_SECONDS", 8.0)
        return max(floor, min(raw, fast))
    return max(floor, raw)
