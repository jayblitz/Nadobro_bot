"""
Phase 4 — per-quote expectancy filter.

Phase 3's regime gate stops quoting when the *market state* is wrong.
This module stops quoting when the *spread itself* is too tight to cover
round-trip costs — even in a permissive regime. It answers one question
before every quote-placement cycle:

    Does one grid step (spread_bp) clear fees + slippage + funding +
    a minimum required edge?

If not, it either auto-widens the spread to the economically-viable
level (default) or vetoes the cycle when even the max spread can't
cover costs (e.g. extreme adverse funding).

Cost model (all in bp, per round-turn):

    open_cost   = maker_fee_bp                       (post-only grid quote)
    close_cost  = close_taker_fraction * (taker_fee_bp + close_slippage_bp)
                + (1 - close_taker_fraction) * maker_fee_bp
    funding     = max(0, expected adverse funding over the hold)
    total_cost  = open_cost + close_cost + funding

    gross_edge  = spread_bp           (conservative: one grid step captured)
    expected_edge = gross_edge - total_cost
    required_spread = total_cost + min_required_edge_bp

Pure functions; no network calls. Funding is read from state when a
recent rate is cached (mm_bot's rgrid path already fetches it); absent
that, funding cost is treated as 0.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from typing import Any

from ._regime import REGIME_CHOP_HIGH_VOL, REGIME_RANGE_TIGHT

logger = logging.getLogger(__name__)


# --- Fee / cost model defaults (bp) ----------------------------------------
# Tread routing carries a 1 bp builder fee per order; the exchange maker fee
# is ~0, so we model the open (always a post-only maker quote) at 1 bp.
DEFAULT_MAKER_FEE_BP = 1.0
# Taker fee for a market close (PM forced exit, stale flatten, etc.).
DEFAULT_TAKER_FEE_BP = 4.0
# Fraction of closes that go out as taker (PM market close) vs maker (an
# opposing grid rung fills). 0.40 = 40% of round-turns close via taker.
DEFAULT_CLOSE_TAKER_FRACTION = 0.40
# Expected slippage on the taker close leg.
DEFAULT_CLOSE_SLIPPAGE_BP = 1.0
# Expected hold in hours — used only to scale funding into a per-round-turn
# cost. Grid round-turns are short; 1h is a conservative upper bound.
DEFAULT_EXPECTED_HOLD_HOURS = 1.0


# --- Per-strategy minimum required edge (bp) -------------------------------
# The edge we demand *after* all costs. D-Grid/Grid sit at 4 bp. R-Grid
# rides momentum (bigger expected captures, lower bar). Mid runs the
# tightest spreads by design so it accepts the thinnest post-cost edge.
DEFAULT_MIN_REQUIRED_EDGE_BP = 4.0
STRATEGY_MIN_EDGE_BP: dict[str, float] = {
    "dgrid": 4.0,
    "grid": 4.0,
    "rgrid": 2.0,
    "mid": 1.5,
}

# Regime multipliers on the min-required-edge. Chop demands a fatter
# post-cost cushion; tight ranges are handled by the Phase 3 gate already
# but we keep a mild bump here for the case where the gate didn't veto.
REGIME_EDGE_MULT: dict[str, float] = {
    REGIME_CHOP_HIGH_VOL: 2.0,
    REGIME_RANGE_TIGHT: 1.5,
}


@dataclass
class QuoteEconomics:
    spread_bp: float = 0.0
    gross_edge_bp: float = 0.0
    open_cost_bp: float = 0.0
    close_cost_bp: float = 0.0
    funding_cost_bp: float = 0.0
    total_cost_bp: float = 0.0
    expected_edge_bp: float = 0.0
    min_required_edge_bp: float = 0.0
    required_spread_bp: float = 0.0
    viable: bool = True
    recommended_spread_bp: float = 0.0
    widened: bool = False
    reason: str = "viable"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _cfg_float(state: dict | None, key: str, default: float) -> float:
    if not state:
        return default
    v = state.get(key)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _resolve_min_edge_bp(strategy: str, regime: str, state: dict | None) -> float:
    base = STRATEGY_MIN_EDGE_BP.get(str(strategy).lower(), DEFAULT_MIN_REQUIRED_EDGE_BP)
    base = _cfg_float(state, "quote_econ_min_edge_bp", base)
    mult = REGIME_EDGE_MULT.get(str(regime or ""), 1.0)
    return base * mult


def evaluate_quote_economics(
    *,
    strategy: str,
    spread_bp: float,
    regime: str = "",
    funding_bp_per_hour: float = 0.0,
    max_spread_bp: float = 0.0,
    state: dict[str, Any] | None = None,
) -> QuoteEconomics:
    """Return the economics decision for the current spread.

    ``spread_bp`` is the per-level grid offset in bp (the value mm_bot
    passes to _compute_grid_prices). ``funding_bp_per_hour`` is the
    *current* funding rate already expressed as bp per hour; pass 0 when
    no rate is available (the default — funding cost is then ignored,
    which is safe: the fee+slippage legs are the dominant cost anyway).

    ``max_spread_bp`` caps the auto-widen. When 0, auto-widen is
    unbounded (the caller's own clamp still applies downstream).
    """
    econ = QuoteEconomics()
    spread_bp = abs(float(spread_bp or 0.0))
    econ.spread_bp = spread_bp

    maker_fee = _cfg_float(state, "quote_econ_maker_fee_bp", DEFAULT_MAKER_FEE_BP)
    taker_fee = _cfg_float(state, "quote_econ_taker_fee_bp", DEFAULT_TAKER_FEE_BP)
    close_taker_frac = _cfg_float(
        state, "quote_econ_close_taker_fraction", DEFAULT_CLOSE_TAKER_FRACTION
    )
    close_taker_frac = min(1.0, max(0.0, close_taker_frac))
    close_slip = _cfg_float(state, "quote_econ_close_slippage_bp", DEFAULT_CLOSE_SLIPPAGE_BP)
    hold_hours = _cfg_float(
        state, "quote_econ_expected_hold_hours", DEFAULT_EXPECTED_HOLD_HOURS
    )
    auto_widen = True
    if state is not None:
        aw = state.get("quote_econ_auto_widen")
        if aw is not None and aw != "":
            auto_widen = bool(aw)

    # --- Cost legs --------------------------------------------------------
    open_cost = maker_fee
    close_cost = (
        close_taker_frac * (taker_fee + close_slip)
        + (1.0 - close_taker_frac) * maker_fee
    )
    # Funding only counts when it's *against* us. We don't know the side
    # mix ahead of time, so treat the magnitude as a worst-case adverse
    # drag scaled by the expected hold.
    funding_cost = abs(float(funding_bp_per_hour or 0.0)) * max(0.0, hold_hours)

    total_cost = open_cost + close_cost + funding_cost
    gross_edge = spread_bp  # conservative: one grid step captured per round-turn
    min_edge = _resolve_min_edge_bp(strategy, regime, state)
    required_spread = total_cost + min_edge

    econ.open_cost_bp = round(open_cost, 4)
    econ.close_cost_bp = round(close_cost, 4)
    econ.funding_cost_bp = round(funding_cost, 4)
    econ.total_cost_bp = round(total_cost, 4)
    econ.gross_edge_bp = round(gross_edge, 4)
    econ.min_required_edge_bp = round(min_edge, 4)
    econ.required_spread_bp = round(required_spread, 4)
    econ.expected_edge_bp = round(gross_edge - total_cost, 4)

    # --- Decision ---------------------------------------------------------
    if spread_bp >= required_spread:
        econ.viable = True
        econ.recommended_spread_bp = spread_bp
        econ.reason = (
            f"spread {spread_bp:.2f}bp >= required {required_spread:.2f}bp "
            f"(cost {total_cost:.2f} + edge {min_edge:.2f})"
        )
        return econ

    # Spread too tight. Auto-widen if allowed and the widened spread fits
    # under max_spread_bp; otherwise veto.
    if auto_widen:
        capped = required_spread
        if max_spread_bp and max_spread_bp > 0:
            capped = min(required_spread, float(max_spread_bp))
        if capped >= required_spread:
            econ.viable = True
            econ.widened = True
            econ.recommended_spread_bp = capped
            econ.reason = (
                f"spread {spread_bp:.2f}bp < required {required_spread:.2f}bp "
                f"-> auto-widened to {capped:.2f}bp"
            )
            return econ
        # max_spread can't cover the cost: widen as far as we can, but the
        # cycle is still not economically viable -> veto.
        econ.viable = False
        econ.widened = True
        econ.recommended_spread_bp = capped
        econ.reason = (
            f"required {required_spread:.2f}bp exceeds max_spread {max_spread_bp:.2f}bp "
            f"(cost {total_cost:.2f} + edge {min_edge:.2f}) -> veto"
        )
        return econ

    econ.viable = False
    econ.recommended_spread_bp = spread_bp
    econ.reason = (
        f"spread {spread_bp:.2f}bp < required {required_spread:.2f}bp and "
        f"auto-widen disabled -> veto"
    )
    return econ
