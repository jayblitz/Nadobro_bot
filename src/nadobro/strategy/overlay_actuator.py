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
from typing import Dict, Mapping, Optional, Tuple

from src.nadobro.core.feature_flags import env_flag
from src.nadobro.llm.signal_engine import Signal

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
    # Regime-adjusted barriers (% of margin), derived from the user's base SL/TP
    # in the engine (trend widens, chop tightens). Applied to the controller's
    # per-level barrier AND surfaced for the session rail; the 10% overlay
    # drawdown cap is the hard backstop over both.
    if signal.sl_pct is not None:
        overrides["sl_pct"] = float(signal.sl_pct)
    if signal.tp_pct is not None:
        overrides["tp_pct"] = float(signal.tp_pct)
    if strat == "mid":
        overrides["directional_bias"] = _clamp(float(signal.bias), -1.0, 1.0)
    return overrides


def rail_barriers(
    base_sl_pct: float, base_tp_pct: float, signal: Signal
) -> Tuple[Optional[float], Optional[float]]:
    """Session-rail barriers derived from the signal, bounded by the user's own
    config. Both barriers respect the user's setting as the binding contract:

    * SL is TIGHTEN-ONLY (chop ×0.8): the overlay may pull the stop closer but
      never widen it past the configured stop.
    * TP is WIDEN-ONLY: the overlay may let a winner run PAST the user's target
      in a trend (×1.6), but must never take profit BEFORE it — the chop-regime
      ×0.8 tightening used to lower the user's TP silently and fire the session
      rail early (OVERLAY-TP-NO-FLOOR). The user's TP is the floor.

    A barrier the user disarmed (``<= 0``) stays disarmed — the 10% overlay
    drawdown cap is the backstop either way."""
    sl: Optional[float] = None
    tp: Optional[float] = None
    if base_sl_pct > 0 and signal.sl_pct is not None:
        sl = min(float(signal.sl_pct), float(base_sl_pct))
    if base_tp_pct > 0 and signal.tp_pct is not None:
        # Floor at the user's configured TP: the overlay only widens it.
        tp = max(float(signal.tp_pct), float(base_tp_pct))
    return sl, tp


def stabilize_overrides(
    prev: Optional[Mapping[str, object]], overrides: Dict[str, object]
) -> Dict[str, object]:
    """Dead-band the continuous override factors against the previously APPLIED
    values. Every applied change flips the live-config signature, and each flip
    recenters the grid ladder / resets Mid quotes — so a 4th-decimal wobble in
    bias/ATR must not churn live orders every candle refresh. Regime flips,
    suppression flips and barrier changes always pass through (risk controls);
    size/spread/bias move only in ≥ 0.05 / 0.10 / 0.10 steps."""
    if not isinstance(prev, Mapping):
        return overrides

    def _n(value: object, default: float = 0.0) -> float:
        try:
            return float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return default

    has_bias_prev = "directional_bias" in prev
    has_bias_new = "directional_bias" in overrides
    material = (
        str(prev.get("regime")) != str(overrides.get("regime"))
        or bool(prev.get("suppress_new_entries")) != bool(overrides.get("suppress_new_entries"))
        or prev.get("sl_pct") != overrides.get("sl_pct")
        or prev.get("tp_pct") != overrides.get("tp_pct")
        or has_bias_prev != has_bias_new
        or abs(_n(prev.get("size_factor"), 1.0) - _n(overrides.get("size_factor"), 1.0)) >= 0.05
        or abs(_n(prev.get("spread_factor"), 1.0) - _n(overrides.get("spread_factor"), 1.0)) >= 0.10
        or (has_bias_new
            and abs(_n(prev.get("directional_bias")) - _n(overrides.get("directional_bias"))) >= 0.10)
    )
    if material:
        return overrides
    out = dict(overrides)
    for key in ("size_factor", "spread_factor", "directional_bias"):
        if key in prev and key in out:
            out[key] = prev[key]
    return out


# The subset of override keys whose APPLIED values must stay sticky across
# cycles (persisted in state as ``overlay_applied`` and compared by
# ``stabilize_overrides``).
APPLIED_OVERRIDE_KEYS = (
    "size_factor", "spread_factor", "directional_bias",
    "suppress_new_entries", "regime", "sl_pct", "tp_pct",
)


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

    # Regime-adjusted barriers → the controller's per-level triple barrier (grid
    # ladder). Mid + fill-anchored are rail-only, so this key is simply absent
    # there and the session rail carries the overlay sl/tp instead (via state).
    sl_pct = overrides.get("sl_pct")
    tp_pct = overrides.get("tp_pct")
    if (sl_pct is not None or tp_pct is not None) and "triple_barrier_config" in configs:
        try:
            from src.nadobro.engine.types import TripleBarrierConfig

            sl_frac = (Decimal(str(sl_pct)) / Decimal(100)) if sl_pct else None
            tp_frac = (Decimal(str(tp_pct)) / Decimal(100)) if tp_pct else None
            if sl_frac or tp_frac:
                configs["triple_barrier_config"] = TripleBarrierConfig(
                    take_profit=tp_frac or None, stop_loss=sl_frac or None
                )
                changed["barriers"] = {"sl_pct": sl_pct, "tp_pct": tp_pct}
        except Exception:  # noqa: BLE001 - barrier override is best-effort
            pass

    if overrides.get("suppress_new_entries"):
        # Choke NEW exposure via the existing net-exposure cap (the inventory
        # gate keeps the reducing side quoting), and arm the regime gate so
        # trends/breakouts pause new opens. Both are honored by the controllers.
        configs["max_net_exposure_pct"] = 0.0
        configs["regime_gate_enabled"] = True
        changed["suppress_new_entries"] = True

    return changed
