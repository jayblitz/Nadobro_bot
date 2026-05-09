"""Tread Fi Percent-of-Volume (POV) participation engine.

Phase 2 of the Tread Fi parity rollout. Maps a target notional and a participation
preset (Aggressive / Normal / Passive) to a duration / cycle cadence / per-cycle
notional, using the pair's rolling 24h volume from the Nado archive
``/market_snapshots`` endpoint.

Multipliers are per-Tread documented per-minute participation rates against the
pair's 24h volume. Higher multipliers complete a target faster; lower
multipliers stretch it across more cycles.
"""

from __future__ import annotations

from typing import Mapping


# Per-minute participation rates documented by Tread Fi:
# Aggressive 10%, Normal 5%, Passive 1%.
PARTICIPATION_MULTIPLIERS: Mapping[str, float] = {
    "aggressive": 0.10,
    "normal": 0.05,
    "passive": 0.01,
}

DEFAULT_PRESET = "normal"
PRESET_NAMES: tuple[str, ...] = ("aggressive", "normal", "passive")

# Sentinel cap for cadence math when volume is unknown / zero.
_MIN_VOLUME_PER_MINUTE_FLOOR = 1e-9


def normalize_preset(preset: str | None) -> str:
    text = (preset or "").strip().lower()
    if text in PARTICIPATION_MULTIPLIERS:
        return text
    return DEFAULT_PRESET


def participation_rate(preset: str | None) -> float:
    """Per-minute participation fraction for the given preset."""
    return float(PARTICIPATION_MULTIPLIERS.get(normalize_preset(preset), PARTICIPATION_MULTIPLIERS[DEFAULT_PRESET]))


def compute_pov_duration(
    notional_usd: float,
    preset: str,
    pair_24h_volume_usd: float,
) -> dict:
    """Compute duration / cadence / per-cycle notional for a POV preset.

    ``notional_usd`` — total USD notional the user wants to push through the
    book over the lifetime of the strategy.
    ``preset`` — "aggressive" / "normal" / "passive".
    ``pair_24h_volume_usd`` — rolling 24h USD volume on the pair, sourced from
    Nado archive ``/market_snapshots``.

    Returns a dict with ``duration_minutes``, ``interval_seconds``,
    ``cycle_notional_usd``, ``multiplier``, and the resolved preset.
    """
    multiplier = participation_rate(preset)
    notional = max(0.0, float(notional_usd or 0.0))
    pair_volume = max(0.0, float(pair_24h_volume_usd or 0.0))

    # 1440 minutes in 24h. We never divide by zero — clamp the per-minute volume
    # floor so missing/empty archive responses fall back to a long but finite
    # duration instead of inf or NaN.
    vol_per_minute = max(pair_volume / 1440.0, _MIN_VOLUME_PER_MINUTE_FLOOR)
    duration_minutes = notional / max(multiplier * vol_per_minute, _MIN_VOLUME_PER_MINUTE_FLOOR)
    interval_seconds = max(1, int(60 / max(multiplier, 0.001)))
    interval_minutes = max(1e-9, interval_seconds / 60.0)
    cycles = max(1.0, duration_minutes / interval_minutes)
    cycle_notional_usd = notional / cycles

    return {
        "preset": normalize_preset(preset),
        "multiplier": multiplier,
        "duration_minutes": duration_minutes,
        "interval_seconds": interval_seconds,
        "cycle_notional_usd": cycle_notional_usd,
        "pair_24h_volume_usd": pair_volume,
        "vol_per_minute_usd": vol_per_minute,
    }


def bound_user_duration_minutes(
    requested_minutes: float,
    notional_usd: float,
    pair_24h_volume_usd: float,
) -> tuple[float, float, float]:
    """Clamp a user-typed duration to ``[Aggressive_minutes, 10 × Passive_minutes]``.

    Returns ``(clamped_minutes, lower_bound, upper_bound)``.

    Per the plan: Aggressive sets the floor (fastest legal completion); Passive
    sets the ceiling, scaled 10× to allow for very long, careful schedules.
    """
    aggressive = compute_pov_duration(notional_usd, "aggressive", pair_24h_volume_usd)
    passive = compute_pov_duration(notional_usd, "passive", pair_24h_volume_usd)
    lower = float(aggressive["duration_minutes"])
    upper = 10.0 * float(passive["duration_minutes"])
    if upper < lower:
        # Degenerate volume produces upper < lower; widen both to a safe band.
        upper = max(lower * 10.0, 1.0)
    requested = max(0.0, float(requested_minutes or 0.0))
    if requested <= 0:
        clamped = (lower + upper) / 2.0
    else:
        clamped = max(lower, min(upper, requested))
    return clamped, lower, upper
