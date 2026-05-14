"""
Phase 3 — regime-aware quote gating.

The audit found the bot quoted aggressively in regimes where the math is
negative (tight ranges below fee cost, chop with no follow-through, and
the *adverse* side of a confirmed trend). Layer sizing (_layer_sizing.py)
already shrinks size on regime, but a small quote is still a fee. The
gate here decides *whether to quote at all* per side.

The gate runs after _compute_grid_prices in run_cycle, so existing logic
(soft reset, anchor, drift) is unaffected — we simply drop the orders
the gate vetoes before they go to the placement loop.

Pure function w.r.t. inputs; safe to unit-test with synthetic regime
info.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from typing import Any

from ._regime import (
    REGIME_RANGE_TIGHT,
    REGIME_RANGE_WIDE,
    REGIME_TREND_UP,
    REGIME_TREND_DOWN,
    REGIME_CHOP_HIGH_VOL,
)

logger = logging.getLogger(__name__)


@dataclass
class QuoteGateDecision:
    skip_buy: bool = False
    skip_sell: bool = False
    level_cap: int | None = None       # max level index allowed; None = no cap
    spread_widen_mult: float = 1.0     # multiply dynamic_spread_bp before quoting
    reason: str = "permit"
    regime: str = ""
    confidence: float = 0.0
    active: bool = False               # True if the gate took any action

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# Per-strategy quote-gate defaults.
#
# - skip_tight_min_confidence: confidence threshold above which a
#   REGIME_RANGE_TIGHT regime vetoes new quotes on both sides. The whole
#   point of "tight range" classification is that realized vol < fee
#   cost, so quoting is structurally negative-expectancy.
# - skip_trend_adverse_min_confidence: threshold for skipping the
#   adverse side in a confirmed trend (buys in trend_down / sells in
#   trend_up). Higher bar than tight-range because trends are noisier.
# - chop_min_confidence: threshold for capping levels in CHOP_HIGH_VOL.
# - chop_level_cap: maximum quote level allowed in chop.
# - chop_spread_widen_mult: multiplier applied to dynamic_spread_bp in
#   chop so the remaining quotes sit further from mid.
# - chop_skip_both_min_confidence: at very-high chop confidence we also
#   skip both sides entirely. Defaults to 1.01 (effectively disabled) for
#   most strategies; R-Grid sets it lower because reversed-mode R-Grid
#   loses badly in chop.
DEFAULT_QUOTE_GATE: dict[str, float] = {
    "skip_tight_min_confidence": 0.65,
    "skip_trend_adverse_min_confidence": 0.70,
    "chop_min_confidence": 0.60,
    "chop_level_cap": 1.0,
    "chop_spread_widen_mult": 1.5,
    "chop_skip_both_min_confidence": 1.01,
}


STRATEGY_QUOTE_GATE_DEFAULTS: dict[str, dict[str, float]] = {
    "dgrid": {
        "skip_tight_min_confidence": 0.65,
        "skip_trend_adverse_min_confidence": 0.70,
        "chop_min_confidence": 0.60,
        "chop_level_cap": 1.0,
        "chop_spread_widen_mult": 1.5,
        "chop_skip_both_min_confidence": 0.85,
    },
    "grid": {
        "skip_tight_min_confidence": 0.65,
        "skip_trend_adverse_min_confidence": 0.70,
        "chop_min_confidence": 0.60,
        "chop_level_cap": 1.0,
        "chop_spread_widen_mult": 1.5,
        "chop_skip_both_min_confidence": 0.85,
    },
    "rgrid": {
        # R-Grid's reversed mode is explicitly trend-following: when the
        # mode switches to reversed we want to KEEP the trend side. Use a
        # higher confidence bar for the adverse-side skip so the gate
        # doesn't fight the mode-switcher.
        "skip_tight_min_confidence": 0.65,
        "skip_trend_adverse_min_confidence": 0.85,
        "chop_min_confidence": 0.55,
        "chop_level_cap": 1.0,
        "chop_spread_widen_mult": 1.5,
        # R-Grid loses badly in chop because the reversed-mode flips have
        # no follow-through; veto both sides at moderate chop confidence.
        "chop_skip_both_min_confidence": 0.70,
    },
    "mid": {
        # Mid Mode runs the tightest spreads, so the gate is *more* eager
        # to step out of regimes where the spread can't earn back fees.
        "skip_tight_min_confidence": 0.55,
        "skip_trend_adverse_min_confidence": 0.65,
        "chop_min_confidence": 0.55,
        "chop_level_cap": 1.0,
        "chop_spread_widen_mult": 2.0,
        "chop_skip_both_min_confidence": 0.80,
    },
}


def _resolve_cfg(strategy: str, state: dict | None) -> dict[str, float]:
    state = state or {}
    strat_defaults = STRATEGY_QUOTE_GATE_DEFAULTS.get(
        str(strategy).lower(), DEFAULT_QUOTE_GATE
    )
    out = dict(DEFAULT_QUOTE_GATE)
    out.update(strat_defaults)

    def _pick(state_key: str, defaults_key: str) -> None:
        sv = state.get(state_key)
        if sv is not None and sv != "":
            try:
                out[defaults_key] = float(sv)
            except (TypeError, ValueError):
                pass

    _pick("quote_gate_skip_tight_min_confidence", "skip_tight_min_confidence")
    _pick("quote_gate_skip_trend_adverse_min_confidence",
          "skip_trend_adverse_min_confidence")
    _pick("quote_gate_chop_min_confidence", "chop_min_confidence")
    _pick("quote_gate_chop_level_cap", "chop_level_cap")
    _pick("quote_gate_chop_spread_widen_mult", "chop_spread_widen_mult")
    _pick("quote_gate_chop_skip_both_min_confidence",
          "chop_skip_both_min_confidence")
    return out


def evaluate_quote_gate(
    *,
    strategy: str,
    regime_info: dict[str, Any] | None,
    state: dict[str, Any] | None = None,
) -> QuoteGateDecision:
    """Return the gate decision for this cycle.

    A no-action result (``active=False``) means the placement loop should
    proceed normally. ``skip_buy=True`` and ``skip_sell=True`` together
    mean the placement loop should be skipped entirely for this cycle;
    PM still manages existing inventory.
    """
    decision = QuoteGateDecision()
    if not regime_info:
        return decision
    regime = str(regime_info.get("regime") or "")
    confidence = float(regime_info.get("confidence") or 0.0)
    decision.regime = regime
    decision.confidence = confidence
    cfg = _resolve_cfg(strategy, state)

    # --- 1. Tight-range veto: don't quote, period. -----------------------
    if regime == REGIME_RANGE_TIGHT and confidence >= cfg["skip_tight_min_confidence"]:
        decision.skip_buy = True
        decision.skip_sell = True
        decision.active = True
        decision.reason = (
            f"regime_range_tight conf={confidence:.2f} "
            f">= {cfg['skip_tight_min_confidence']:.2f}"
        )
        return decision

    # --- 2. CHOP_HIGH_VOL gate: cap levels + widen spread, optionally veto.
    if regime == REGIME_CHOP_HIGH_VOL and confidence >= cfg["chop_min_confidence"]:
        if confidence >= cfg["chop_skip_both_min_confidence"]:
            decision.skip_buy = True
            decision.skip_sell = True
            decision.active = True
            decision.reason = (
                f"regime_chop_high_vol conf={confidence:.2f} "
                f">= chop_skip_both={cfg['chop_skip_both_min_confidence']:.2f}"
            )
            return decision
        decision.level_cap = int(max(1, cfg["chop_level_cap"]))
        decision.spread_widen_mult = max(1.0, float(cfg["chop_spread_widen_mult"]))
        decision.active = True
        decision.reason = (
            f"regime_chop_high_vol conf={confidence:.2f} "
            f">= chop_min={cfg['chop_min_confidence']:.2f} "
            f"-> level_cap={decision.level_cap} widen={decision.spread_widen_mult:.2f}x"
        )
        return decision

    # --- 3. Trend regimes: skip the adverse side. ------------------------
    trend_skip_conf = cfg["skip_trend_adverse_min_confidence"]
    if regime == REGIME_TREND_DOWN and confidence >= trend_skip_conf:
        # R-Grid's reversed mode is intentionally trend-following — don't
        # fight the mode switcher when it has decided we're on the trend.
        if _rgrid_in_reversed_mode(strategy, state):
            decision.reason = (
                f"regime_trend_down conf={confidence:.2f} but rgrid reversed mode "
                "— gate permits both sides"
            )
            return decision
        decision.skip_buy = True
        decision.active = True
        decision.reason = (
            f"regime_trend_down conf={confidence:.2f} >= {trend_skip_conf:.2f}"
        )
        return decision
    if regime == REGIME_TREND_UP and confidence >= trend_skip_conf:
        if _rgrid_in_reversed_mode(strategy, state):
            decision.reason = (
                f"regime_trend_up conf={confidence:.2f} but rgrid reversed mode "
                "— gate permits both sides"
            )
            return decision
        decision.skip_sell = True
        decision.active = True
        decision.reason = (
            f"regime_trend_up conf={confidence:.2f} >= {trend_skip_conf:.2f}"
        )
        return decision

    return decision


def _rgrid_in_reversed_mode(strategy: str, state: dict | None) -> bool:
    if str(strategy or "").lower() != "rgrid":
        return False
    if not state:
        return False
    return str(state.get("rgrid_active_mode") or "classic").lower() == "reversed"


def apply_gate_to_orders(
    decision: QuoteGateDecision, grid_orders: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Drop orders forbidden by the gate's skip / level_cap fields."""
    if not decision.active:
        return list(grid_orders)
    filtered: list[dict[str, Any]] = []
    for o in grid_orders:
        is_long = bool(o.get("is_long"))
        level = int(o.get("level") or 1)
        if is_long and decision.skip_buy:
            continue
        if (not is_long) and decision.skip_sell:
            continue
        if decision.level_cap is not None and level > decision.level_cap:
            continue
        filtered.append(o)
    return filtered
