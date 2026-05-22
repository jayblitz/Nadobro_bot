"""Market-making quote-capacity / margin math.

Extracted from the (now removed) legacy ``strategies/mm_bot.py`` during the
Engine v2 Phase 4 cutover so the pre-trade card / dashboard previews keep
working without importing legacy strategy code. Pure helpers — no venue or
strategy-runtime dependencies.
"""
from __future__ import annotations

DEFAULT_MIN_ORDER_NOTIONAL_USD = 100.0
# Cushion vs venue maintenance / account-health buffers when translating
# per-quote notional into a collateral budget.
MM_COLLATERAL_SAFETY_FACTOR = 1.25
# +20% margin requirement when |bias| = 1.0.
MID_FULL_BIAS_MARGIN_UPLIFT = 0.20


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(v, hi))


def _resolve_directional_bias_value(directional_bias) -> float:
    """Convert a directional_bias config value into a float in [-1.0, +1.0].

    Mid Mode (Tread parity) accepts a continuous float; legacy GRID/RGRID/DGRID
    accept the strings ``neutral``, ``long_bias``, ``short_bias`` and map them to
    discrete ±1 / 0 values for the purpose of margin uplift accounting.
    """
    if directional_bias is None:
        return 0.0
    if isinstance(directional_bias, (int, float)) and not isinstance(directional_bias, bool):
        try:
            return _clamp(float(directional_bias), -1.0, 1.0)
        except (TypeError, ValueError):
            return 0.0
    text = str(directional_bias).lower().strip()
    if text in ("long_bias", "long", "+long"):
        return 1.0
    if text in ("short_bias", "short", "-short"):
        return -1.0
    if text in ("neutral", "", "none", "off"):
        return 0.0
    try:
        return _clamp(float(text), -1.0, 1.0)
    except (TypeError, ValueError):
        return 0.0


def _mm_margin_per_quote_estimate_usd(
    min_order_notional_usd: float,
    leverage: float,
    safety_factor: float,
) -> float:
    lev = max(1.0, float(leverage or 1.0))
    mn = max(1.0, float(min_order_notional_usd))
    sf = max(1.0, float(safety_factor))
    return (mn / lev) * sf


def estimate_mm_quote_capacity(
    collateral_usd: float,
    min_order_notional_usd: float,
    leverage: float,
    max_open_orders: int = 6,
    *,
    safety_factor: float | None = None,
) -> dict[str, float | int]:
    """Estimate resting-quote concurrency from a collateral budget (previews /
    preflight). Each resting quote still carries ~min_order_notional_usd
    notional; leverage only reduces estimated margin per quote for budgeting."""
    sf = MM_COLLATERAL_SAFETY_FACTOR if safety_factor is None else float(safety_factor)
    margin_per = _mm_margin_per_quote_estimate_usd(min_order_notional_usd, leverage, sf)
    if margin_per <= 0:
        return {
            "margin_per_quote_est_usd": 0.0,
            "max_resting_quotes": 0,
            "min_collateral_1_quote_usd": 0.0,
            "min_collateral_2_quote_usd": 0.0,
        }
    raw_slots = int(max(0.0, float(collateral_usd)) / margin_per)
    max_slots = max(0, min(int(max_open_orders), raw_slots))
    return {
        "margin_per_quote_est_usd": round(margin_per, 4),
        "max_resting_quotes": max_slots,
        "min_collateral_1_quote_usd": round(margin_per, 4),
        "min_collateral_2_quote_usd": round(2.0 * margin_per, 4),
    }
