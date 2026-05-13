"""
Dynamic Grid — adaptive per-level quote sizing.

Replaces the constant ``min_order_notional_usd / mid`` size used in
``mm_bot.run_cycle`` (lines 1631-1633) with a 5-factor product:

    size_usd = base_notional_usd
             * side_inventory_brake          # 0.25..1.0 by |inv| / soft_cap
             * regime_size_mult              # regime x side table
             * level_taper                   # geometric 1, 0.7, 0.49, 0.34...
             * fill_performance_mult         # 0.7..1.3 by rolling per-side PnL
             * volatility_scaling            # target / realized vol clipped

Hard-clipped to [min_order_notional_usd, max_per_level_usd].

Pure functions; safe to call inside run_cycle with no side effects beyond
the optional ``state["mm_layer_sizing_telemetry"]`` row we leave behind for
the dashboard.
"""
from __future__ import annotations

import logging
from typing import Any

from ._regime import (
    REGIME_RANGE_TIGHT,
    REGIME_RANGE_WIDE,
    REGIME_TREND_UP,
    REGIME_TREND_DOWN,
    REGIME_CHOP_HIGH_VOL,
)

logger = logging.getLogger(__name__)


# Multiplier table: regime -> (buy_mult, sell_mult).
# Tuned conservatively. Override via state["layer_regime_size_table"] for tests.
DEFAULT_REGIME_SIZE_TABLE: dict[str, dict[str, float]] = {
    REGIME_RANGE_TIGHT: {"buy": 1.50, "sell": 1.50},
    REGIME_RANGE_WIDE: {"buy": 1.00, "sell": 1.00},
    REGIME_TREND_UP:   {"buy": 1.00, "sell": 0.50},
    REGIME_TREND_DOWN: {"buy": 0.50, "sell": 1.00},
    REGIME_CHOP_HIGH_VOL: {"buy": 0.40, "sell": 0.40},
}

# Per-strategy regime size tables. The R-Grid table leans INTO trends because
# R-Grid's "reversed" mode is intentionally trend-following — once the strategy
# is in that mode the trend side is the side we want to fill. Both sides are
# slightly punished in CHOP_HIGH_VOL since R-Grid's reversed mode loses badly
# when there's no follow-through.
STRATEGY_REGIME_TABLES: dict[str, dict[str, dict[str, float]]] = {
    "dgrid": DEFAULT_REGIME_SIZE_TABLE,
    "grid": DEFAULT_REGIME_SIZE_TABLE,
    "rgrid": {
        REGIME_RANGE_TIGHT: {"buy": 1.20, "sell": 1.20},
        REGIME_RANGE_WIDE:  {"buy": 1.00, "sell": 1.00},
        # Lean into trend sides more than dgrid's table.
        REGIME_TREND_UP:    {"buy": 1.30, "sell": 0.40},
        REGIME_TREND_DOWN:  {"buy": 0.40, "sell": 1.30},
        REGIME_CHOP_HIGH_VOL: {"buy": 0.30, "sell": 0.30},
    },
    "mid": {
        # Mid Mode quotes tight ±spread × level with no anchor. The user's
        # directional bias is layered separately via ``bias_tilt`` (so this
        # table is symmetric on the regime axis). Lower trend penalty than
        # dgrid because Mid Mode runs higher turnover.
        REGIME_RANGE_TIGHT: {"buy": 1.30, "sell": 1.30},
        REGIME_RANGE_WIDE:  {"buy": 1.00, "sell": 1.00},
        REGIME_TREND_UP:    {"buy": 1.00, "sell": 0.65},
        REGIME_TREND_DOWN:  {"buy": 0.65, "sell": 1.00},
        REGIME_CHOP_HIGH_VOL: {"buy": 0.50, "sell": 0.50},
    },
}


# Mid Mode user-bias alpha tilt magnitude. Matches mm_bot.MID_BIAS_ALPHA_TILT
# so the per-level size tilt is consistent with the legacy slot-allocation
# tilt the Tread Mid Mode docs describe.
MID_BIAS_ALPHA_TILT = 0.20


def _resolve_regime_table(
    config: dict | None, state: dict | None
) -> dict[str, dict[str, float]]:
    """Pick the right regime table: explicit config > strategy default > module default."""
    cfg = config or {}
    explicit = cfg.get("layer_regime_size_table")
    if explicit:
        return explicit
    strategy = str((state or {}).get("strategy") or cfg.get("strategy") or "dgrid").lower()
    return STRATEGY_REGIME_TABLES.get(strategy) or DEFAULT_REGIME_SIZE_TABLE

DEFAULT_LAYER_TAPER_RATIO = 0.7        # geometric across levels
DEFAULT_LAYER_TAPER_FLOOR = 0.30       # never below this
DEFAULT_TARGET_VOL_BP = 30.0           # daily realized vol the engine sizes toward
DEFAULT_FILL_PERF_LOOKBACK = 50        # fills per side to look back over
DEFAULT_FILL_PERF_MIN_MULT = 0.7
DEFAULT_FILL_PERF_MAX_MULT = 1.3
DEFAULT_INVENTORY_BRAKE_FLOOR = 0.25   # smallest brake before side suppression
DEFAULT_INVENTORY_BRAKE_KNEE = 0.30    # |inv|/soft cap above which the brake activates


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(v, hi))


def _inventory_brake(
    *,
    side: str,
    net_units: float,
    inv_usd: float,
    inv_soft_usd: float,
    knee: float,
    floor: float,
) -> float:
    """Smooth ramp from 1.0 down to ``floor`` as same-side inventory grows.

    The "same side" is whichever side would *add* to the existing position.
    Long inventory (net_units>0) brakes BUYS. Short inventory brakes SELLS.
    The opposite side gets a 1.0 multiplier (we want it to fill: flatten).
    """
    if inv_soft_usd <= 0 or inv_usd <= 0:
        return 1.0
    ratio = abs(inv_usd) / inv_soft_usd
    if net_units > 0:
        adding_side = "buy"
    elif net_units < 0:
        adding_side = "sell"
    else:
        return 1.0
    if side != adding_side:
        return 1.0  # flattening side runs at full size
    if ratio <= knee:
        return 1.0
    # Linear ramp from 1.0 at knee -> floor at 1.0 (soft cap)
    span = max(1e-6, 1.0 - knee)
    progress = (ratio - knee) / span
    progress = _clamp(progress, 0.0, 1.0)
    return _clamp(1.0 - progress * (1.0 - floor), floor, 1.0)


def _regime_mult(side: str, regime: str, table: dict[str, dict[str, float]]) -> float:
    row = table.get(regime) or table.get(REGIME_RANGE_WIDE) or {}
    return float(row.get(side, 1.0))


def _level_taper(level_index: int, ratio: float, floor: float) -> float:
    """Geometric taper. Level 1 = 1.0, level k = max(floor, ratio^(k-1))."""
    if level_index <= 1:
        return 1.0
    return max(floor, ratio ** (level_index - 1))


def _per_side_pnl(side_fills: list[dict]) -> float:
    """Sum the ``realized_pnl`` field of recent fills if present. Returns 0 if absent.

    mm_bot's ``_append_grid_exposure_fill`` does not currently record realized
    PnL on every fill (that data lives on the position/match record). We treat
    absence as neutral. If your trade_service is wired to write a ``pnl`` key
    into the fill, this picks it up automatically.
    """
    total = 0.0
    for f in side_fills or []:
        try:
            total += float(f.get("realized_pnl") or f.get("pnl") or 0.0)
        except Exception:
            continue
    return total


def _fill_performance_mult(
    state: dict,
    side: str,
    lookback: int,
    min_mult: float,
    max_mult: float,
) -> float:
    """Reward/punish the side whose recent fills have been winning/losing.

    Uses a saturating tanh-like mapping so a single big-PnL fill can't blow
    the multiplier out of bounds. If there isn't enough fill history yet,
    returns neutral 1.0.
    """
    fills = state.get(f"grid_{side}_fills") or []
    recent = list(fills)[-lookback:]
    if len(recent) < 5:
        return 1.0
    pnl = _per_side_pnl(recent)
    # Normalize by aggregate notional so the multiplier is on a comparable scale.
    notional = 0.0
    for f in recent:
        try:
            notional += float(f.get("size") or 0.0) * float(f.get("price") or 0.0)
        except Exception:
            continue
    if notional <= 0:
        return 1.0
    pnl_bp = (pnl / notional) * 1e4
    # Map pnl_bp through a sigmoid centered at 0 bp, span ~[-15, +15] bp -> [min, max]
    k = 0.1  # smoothness; 0.1 => +/-15 bp ~ saturation
    import math
    raw = math.tanh(pnl_bp * k)
    mid = (min_mult + max_mult) / 2.0
    half = (max_mult - min_mult) / 2.0
    return _clamp(mid + raw * half, min_mult, max_mult)


def _volatility_scaling(realized_vol_bp: float, target_vol_bp: float) -> float:
    """When realized > target, scale down proportionally. Never scale up above 1.0."""
    if target_vol_bp <= 0 or realized_vol_bp <= 0:
        return 1.0
    return _clamp(target_vol_bp / realized_vol_bp, 0.1, 1.0)


def size_quote_level(
    *,
    side: str,
    level: int,
    base_notional_usd: float,
    mid_price: float,
    inv_usd: float,
    net_units: float,
    inv_soft_usd: float,
    regime: str,
    realized_vol_bp: float,
    min_order_notional_usd: float,
    max_per_level_usd: float,
    state: dict | None = None,
    config: dict | None = None,
    bias_value: float = 0.0,
) -> dict[str, Any]:
    """Return per-level sizing decision.

    Output:
        size_base : float — base-unit size to send to execute_limit_order
        size_usd  : float — notional in USD (size_base * mid_price)
        components: dict  — each multiplier (for telemetry / debugging)
    """
    side = "buy" if str(side).lower() in ("buy", "long", "bid") else "sell"
    level = max(1, int(level))
    cfg = config or {}
    s = state if isinstance(state, dict) else {}

    knee = float(cfg.get("layer_inventory_brake_knee") or DEFAULT_INVENTORY_BRAKE_KNEE)
    floor = float(cfg.get("layer_inventory_brake_floor") or DEFAULT_INVENTORY_BRAKE_FLOOR)
    taper = float(cfg.get("layer_taper_ratio") or DEFAULT_LAYER_TAPER_RATIO)
    taper_floor = float(cfg.get("layer_taper_floor") or DEFAULT_LAYER_TAPER_FLOOR)
    target_vol_bp = float(cfg.get("layer_target_vol_bp") or DEFAULT_TARGET_VOL_BP)
    fp_lookback = int(cfg.get("layer_fill_perf_lookback") or DEFAULT_FILL_PERF_LOOKBACK)
    fp_min = float(cfg.get("layer_fill_perf_min_mult") or DEFAULT_FILL_PERF_MIN_MULT)
    fp_max = float(cfg.get("layer_fill_perf_max_mult") or DEFAULT_FILL_PERF_MAX_MULT)
    table = _resolve_regime_table(cfg, s)

    m_inv = _inventory_brake(
        side=side,
        net_units=net_units,
        inv_usd=inv_usd,
        inv_soft_usd=inv_soft_usd,
        knee=knee,
        floor=floor,
    )
    m_regime = _regime_mult(side, regime, table)
    m_taper = _level_taper(level, taper, taper_floor)
    m_fillperf = _fill_performance_mult(s, side, fp_lookback, fp_min, fp_max)
    m_vol = _volatility_scaling(realized_vol_bp, target_vol_bp)

    # Mid Mode (and any caller that passes a non-zero bias_value) applies a
    # multiplicative directional tilt that layers on top of the symmetric
    # regime table. +1.0 bias = +20% buy, -20% sell. -1.0 = inverse. Clamped.
    m_bias = 1.0
    if bias_value:
        bv = _clamp(float(bias_value), -1.0, 1.0)
        if side == "buy":
            m_bias = max(0.0, 1.0 + MID_BIAS_ALPHA_TILT * bv)
        else:
            m_bias = max(0.0, 1.0 - MID_BIAS_ALPHA_TILT * bv)

    size_usd = (
        float(base_notional_usd)
        * m_inv * m_regime * m_taper * m_fillperf * m_vol * m_bias
    )
    size_usd = _clamp(size_usd, float(min_order_notional_usd), float(max_per_level_usd))
    size_base = size_usd / max(float(mid_price), 1e-12)

    components = {
        "inventory_brake": round(m_inv, 4),
        "regime_mult": round(m_regime, 4),
        "level_taper": round(m_taper, 4),
        "fill_perf_mult": round(m_fillperf, 4),
        "vol_scaling": round(m_vol, 4),
        "bias_tilt": round(m_bias, 4),
        "raw_size_usd_before_clamp": round(
            float(base_notional_usd) * m_inv * m_regime * m_taper * m_fillperf * m_vol * m_bias, 6
        ),
    }

    if isinstance(state, dict):
        rows = state.setdefault("mm_layer_sizing_telemetry", [])
        if not isinstance(rows, list):
            rows = []
            state["mm_layer_sizing_telemetry"] = rows
        rows.append({
            "side": side, "level": level,
            "size_usd": round(size_usd, 4), "regime": regime,
            **components,
        })
        # Keep only last 40 rows so this can't grow unbounded.
        if len(rows) > 40:
            del rows[: len(rows) - 40]

    return {
        "size_base": size_base,
        "size_usd": size_usd,
        "components": components,
    }
