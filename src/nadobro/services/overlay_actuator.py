"""Financial-overlay actuator — map a :class:`Signal` onto the live MM
controller config, bounded so it can never exceed the user's own settings.

Runs in the background for the four MM strategies (grid / rgrid / dgrid / mid);
the normal user configures nothing. It only ever turns knobs the controllers
already consume (directional_bias, per-side spread, order size, the net-exposure
cap), so no controller code changes and every adjustment is inside the same
rails the user's config already lives behind. The session SL/TP rail and a
separate overlay-drawdown kill-switch are the backstops.

Everything here is pure and deterministic; the runtime does the I/O (candle
fetch, persistence) and the flatten.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Dict, Optional

from src.nadobro.services.feature_flags import env_flag
from src.nadobro.services.signal_engine import Signal

OVERLAY_STRATEGIES = ("grid", "rgrid", "dgrid", "mid")

# Overlay-specific max drawdown (% of session margin). Independent of, and
# additional to, the user's session SL — EITHER trips flatten + stand-down.
OVERLAY_DRAWDOWN_CAP_PCT = 10.0

# Bounds — the overlay can shade size and spread, never blow past the user's
# posture. Size can add up to +25% on a strong trend or cut to 50%; the venue
# leverage/exposure caps + session rails remain the hard limits downstream.
_SIZE_LO, _SIZE_HI = 0.5, 1.25
_SPREAD_LO, _SPREAD_HI = 0.75, 3.0
# Per-side spread can never quote through this fee-clearing floor.
_SPREAD_FLOOR_FRACTION = Decimal("0.00015")


def overlay_enabled() -> bool:
    """Background overlay is ON by default; operators can disable via env."""
    return env_flag("NADO_SIGNAL_OVERLAY", True)


def overlay_applies(strategy: str) -> bool:
    return overlay_enabled() and str(strategy or "").lower() in OVERLAY_STRATEGIES


def overlay_drawdown_breached(
    session_pnl_pct_net: float, cap_pct: float = OVERLAY_DRAWDOWN_CAP_PCT
) -> bool:
    """True when the session's net drawdown has breached the overlay cap."""
    try:
        return cap_pct > 0 and float(session_pnl_pct_net) <= -abs(float(cap_pct))
    except (TypeError, ValueError):
        return False


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def compute_overrides(strategy: str, signal: Signal) -> Dict[str, object]:
    """Bounded, controller-agnostic overrides derived from the signal.

    ``size_factor``  0.5..1.25 multiplier on per-order / ladder notional.
    ``spread_factor`` 0.75..3.0 multiplier on the quoted per-side spread.
    ``directional_bias``  only for Mid (it consumes a continuous bias).
    ``suppress_new_entries``  True -> choke NEW exposure (reduce-only posture).
    """
    strat = str(strategy or "").lower()
    # Size: add on the favoured side only on a confident trend; trim otherwise.
    # scale in [-1,1]; positive -> larger, negative -> smaller.
    size_factor = _clamp(1.0 + 0.25 * float(signal.scale) * float(signal.confidence),
                         _SIZE_LO, _SIZE_HI)
    spread_factor = _clamp(float(signal.spread_mult), _SPREAD_LO, _SPREAD_HI)
    suppress = (not signal.entry_ok) or signal.regime == "chop"
    # When suppressing, do not also add size.
    if suppress:
        size_factor = min(size_factor, 1.0)
    overrides: Dict[str, object] = {
        "size_factor": round(size_factor, 4),
        "spread_factor": round(spread_factor, 4),
        "suppress_new_entries": bool(suppress),
        "regime": signal.regime,
        "bias": float(signal.bias),
        "confidence": float(signal.confidence),
    }
    if strat == "mid":
        overrides["directional_bias"] = _clamp(float(signal.bias), -1.0, 1.0)
    return overrides


def _mul_dec(configs: Dict[str, object], key: str, factor: float) -> bool:
    val = configs.get(key)
    if val is None:
        return False
    try:
        configs[key] = Decimal(str(val)) * Decimal(str(factor))
        return True
    except Exception:  # noqa: BLE001 - leave the config untouched on a bad value
        return False


def apply_overrides_to_configs(
    strategy: str, configs: Dict[str, object], overrides: Dict[str, object]
) -> Dict[str, object]:
    """Mutate the mapped ``configs`` in place with the bounded overrides, using
    ONLY keys the controllers already consume. Returns a compact record of what
    changed (for persistence). Fee-floors the spread; chokes new exposure via
    the existing net-exposure cap when suppressing."""
    changed: Dict[str, object] = {}

    size_factor = float(overrides.get("size_factor", 1.0) or 1.0)
    if abs(size_factor - 1.0) > 1e-9:
        # Grid family sizes the ladder via total_amount_quote; Mid + fill-anchored
        # via order_amount_quote. Scale whichever the mapped config carries.
        for key in ("order_amount_quote", "total_amount_quote"):
            if _mul_dec(configs, key, size_factor):
                changed[key] = str(configs[key])

    spread_factor = float(overrides.get("spread_factor", 1.0) or 1.0)
    if abs(spread_factor - 1.0) > 1e-9:
        for key in ("spread_bid_pct", "spread_ask_pct", "min_spread_between_orders"):
            if key in configs and _mul_dec(configs, key, spread_factor):
                # Never quote through the fee floor.
                try:
                    if Decimal(str(configs[key])) < _SPREAD_FLOOR_FRACTION:
                        configs[key] = _SPREAD_FLOOR_FRACTION
                except Exception:  # noqa: BLE001
                    pass
                changed[key] = str(configs[key])

    if "directional_bias" in overrides:
        configs["directional_bias"] = float(overrides["directional_bias"])
        changed["directional_bias"] = configs["directional_bias"]

    if overrides.get("suppress_new_entries"):
        # Choke NEW exposure via the existing net-exposure cap (the inventory
        # gate keeps the reducing side quoting), and arm the regime gate so
        # trends/breakouts pause new opens. Both are honored by the controllers.
        configs["max_net_exposure_pct"] = 0.0
        configs["regime_gate_enabled"] = True
        changed["suppress_new_entries"] = True

    return changed
