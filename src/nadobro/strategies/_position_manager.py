"""
Dynamic Grid — active position manager.

Runs at the *top* of every D-Grid cycle, before the quote-placement loop in
``mm_bot.run_cycle``. Reads current positions and decides whether to:

  1. PARTIAL_TP       — close a fraction of a side that's in profit > X bp.
  2. ADVERSE_CUT      — close 50% of a losing side when regime flips against it.
  3. TRAIL_CLOSE      — close a side after a profit high-water-mark fades.
  4. STALE_FLATTEN    — flatten a side that's been held too long in expanding vol.
  5. COOLDOWN         — after a session SL/TP, gate re-engagement with reduced
                        size for a configurable window.

All closes are issued as reduce-only market IOC via
``services.trade_service.execute_market_order(reduce_only=True)`` — the
existing primitive volume_bot uses for forced exits, so we inherit its rate
limit, intent dedup, and idempotency handling.

The PM only *closes*. It never opens. New quotes are still placed by the
existing grid engine, which now receives a ``pm_size_dampener`` factor when
in cooldown.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

from ._regime import (
    REGIME_TREND_UP,
    REGIME_TREND_DOWN,
    REGIME_CHOP_HIGH_VOL,
)

logger = logging.getLogger(__name__)


# --- Defaults --------------------------------------------------------------
DEFAULT_PARTIAL_TP_BP = 8.0
DEFAULT_PARTIAL_TP_FRACTION = 0.33
DEFAULT_CUT_CONFIDENCE = 0.65
DEFAULT_CUT_FRACTION = 0.50
DEFAULT_TRAIL_ARM_USD = 0.50
# Trail arm as a % of position margin. The legacy arm at $0.50 fires on tiny
# unrealized P&L when position size is small (your $108-notional WTI fills
# armed at +0.5% unrealized and exited at +0.25% — net negative after fees).
# When pm_trail_arm_usd is not explicitly set, manage_positions scales the
# arm to ``trail_arm_pct / 100 * margin_per_side_usd`` instead.
DEFAULT_TRAIL_ARM_PCT = 0.60
DEFAULT_TRAIL_GIVE_BACK_FRACTION = 0.5  # close when current PnL < 0.5 * HWM
DEFAULT_STALE_HOLD_SECONDS = 30 * 60     # 30 min
DEFAULT_STALE_VOL_EXPANSION_RATIO = 2.0  # current vol > 2x rolling normal
DEFAULT_COOLDOWN_SECONDS = 300
DEFAULT_COOLDOWN_SIZE_DAMPENER = 0.5
DEFAULT_COOLDOWN_REGAIN_SECONDS = 30 * 60

# Phase 2 — universal exit rails.
#
# Breakeven stop: once unrealized PnL exceeds ``breakeven_arm_bp`` of cost
# basis, mark the side as "BE armed". On subsequent cycles, if the PnL
# rolls back through zero we close the whole side at breakeven — locks in
# the trip from red to green into something that can't go back to red.
# Different from trail: trail waits for a HWM and give-back fraction; BE
# only needs one "we were green, now we're not" round-trip.
DEFAULT_BREAKEVEN_ARM_BP = 10.0
DEFAULT_BREAKEVEN_EXIT_OFFSET_BP = 2.0  # exit at entry + 2 bp so we cover fees
# Time-in-loss stop: if a side has been continuously underwater for more
# than ``max_underwater_seconds`` we force-close it. Independent of stale
# flatten which requires age + vol expansion — this is just "we've been
# wrong for too long."
DEFAULT_MAX_UNDERWATER_SECONDS = 25 * 60
# Momentum-reversal close: if the regime flips against a *winning* side
# with high confidence, take everything (don't trail). Inverse of
# adverse_cut which trims 50% of a losing side on regime against.
DEFAULT_MOM_REV_CONFIDENCE = 0.75
# Mid Mode inventory cap: Mid intentionally disables grid anchor + soft
# reset, so inventory can accumulate unbounded. The cap fires a
# reduce-only close on whichever side breached |inv_usd| > cap.
# Per-strategy default lives in STRATEGY_DEFAULTS; module default is
# "unbounded" so the rule is a no-op outside Mid.
DEFAULT_INVENTORY_CAP_USD = 0.0


# Per-strategy default overrides. R-Grid intentionally rides momentum, so it
# wants: (a) higher partial_tp_bp — don't clip profitable trend captures early,
# (b) higher cut_confidence — don't cut against the trend the strategy is built
# to ride, (c) MORE aggressive trail give-back fraction — lock in more of HWM
# once the trend rolls over.
STRATEGY_DEFAULTS: dict[str, dict[str, float]] = {
    "dgrid": {
        # partial_tp_bp raised from 8 → 18 so we don't skim 33% of every
        # winner at half a fee. trail_give_back tightened from 0.50 → 0.30
        # so we keep more of HWM before triggering an exit. trail_arm now
        # scales with margin via trail_arm_pct (legacy USD floor preserved).
        "partial_tp_bp": 18.0,
        "partial_tp_fraction": 0.33,
        "cut_confidence": 0.65,
        "cut_fraction": 0.50,
        "trail_arm_usd": 0.50,
        "trail_arm_pct": 0.60,
        "trail_give_back_fraction": 0.30,
        "stale_hold_seconds": 30 * 60,
        "cooldown_seconds": 300,
        "cooldown_size_dampener": 0.50,
        # Phase 2 exit rails.
        "breakeven_arm_bp": 10.0,
        "breakeven_exit_offset_bp": 2.0,
        "max_underwater_seconds": 25 * 60,
        "mom_rev_confidence": 0.75,
        "inventory_cap_usd": 0.0,
    },
    "rgrid": {
        # R-Grid rides momentum — keep winners longer, cut against the trend
        # only when we're really sure, and trail tighter once HWM is set.
        # partial_tp_bp raised 15 → 25 in lockstep with D-Grid's 8 → 18 so the
        # "R-Grid lets winners run further than D-Grid" hierarchy is preserved.
        "partial_tp_bp": 25.0,
        "partial_tp_fraction": 0.25,
        "cut_confidence": 0.75,
        "cut_fraction": 0.50,
        "trail_arm_usd": 0.75,
        "trail_arm_pct": 0.80,
        "trail_give_back_fraction": 0.70,  # close once PnL drops below 70% of HWM
        "stale_hold_seconds": 45 * 60,     # R-Grid expects longer holds
        "cooldown_seconds": 240,
        "cooldown_size_dampener": 0.50,
        # Phase 2 exit rails. R-Grid sits on momentum trades longer, so we
        # arm breakeven a bit further out and let the position sit underwater
        # longer than D-Grid before forcing a close. Momentum-reversal close
        # threshold matches the higher cut_confidence — R-Grid only cuts on
        # high-conviction reversals.
        "breakeven_arm_bp": 18.0,
        "breakeven_exit_offset_bp": 3.0,
        "max_underwater_seconds": 40 * 60,
        "mom_rev_confidence": 0.80,
        "inventory_cap_usd": 0.0,
    },
    "grid": {
        # Classic symmetric grid — same conservative profile as D-Grid.
        "partial_tp_bp": 18.0,
        "partial_tp_fraction": 0.33,
        "cut_confidence": 0.65,
        "cut_fraction": 0.50,
        "trail_arm_usd": 0.50,
        "trail_arm_pct": 0.60,
        "trail_give_back_fraction": 0.30,
        "stale_hold_seconds": 30 * 60,
        "cooldown_seconds": 300,
        "cooldown_size_dampener": 0.50,
        # Phase 2 exit rails. Same defaults as D-Grid.
        "breakeven_arm_bp": 10.0,
        "breakeven_exit_offset_bp": 2.0,
        "max_underwater_seconds": 25 * 60,
        "mom_rev_confidence": 0.75,
        "inventory_cap_usd": 0.0,
    },
    "mid": {
        # Mid Mode (Tread parity) quotes tighter spreads, so partial-TP bp
        # threshold is lower — wins are smaller per round-turn. The user's
        # continuous directional_bias signals conviction, so we don't cut
        # against them too eagerly (cut_confidence sits between dgrid and
        # rgrid). Stale hold is short because Mid Mode is built for high
        # turnover.
        "partial_tp_bp": 5.0,
        "partial_tp_fraction": 0.33,
        "cut_confidence": 0.70,
        "cut_fraction": 0.50,
        "trail_arm_usd": 0.30,
        "trail_arm_pct": 0.40,
        "trail_give_back_fraction": 0.50,
        "stale_hold_seconds": 20 * 60,
        "cooldown_seconds": 240,
        "cooldown_size_dampener": 0.50,
        # Phase 2 exit rails. Mid Mode disables anchor + soft-reset by
        # design, so inventory can accumulate unbounded without intervention.
        # An inventory cap (in USD; 0 = disabled) provides the hard rail.
        # Tight breakeven/time-in-loss thresholds reflect Mid's high
        # turnover — winners and losers don't sit long under normal flow.
        "breakeven_arm_bp": 6.0,
        "breakeven_exit_offset_bp": 1.5,
        "max_underwater_seconds": 15 * 60,
        "mom_rev_confidence": 0.70,
        "inventory_cap_usd": 5000.0,
    },
}


# Action types
ACTION_PARTIAL_TP = "partial_tp"
ACTION_ADVERSE_CUT = "adverse_cut"
ACTION_TRAIL_CLOSE = "trail_close"
ACTION_STALE_FLATTEN = "stale_flatten"
ACTION_BREAKEVEN_STOP = "breakeven_stop"
ACTION_TIME_IN_LOSS_STOP = "time_in_loss_stop"
ACTION_MOM_REVERSAL_CLOSE = "momentum_reversal_close"
ACTION_INVENTORY_CAP = "inventory_cap_flatten"


def _pm_config(state: dict) -> dict[str, float]:
    """Resolve PM config from state, honoring per-strategy defaults.

    Precedence (highest first):
      1. Explicit ``state["pm_*"]`` value set by the user.
      2. Per-strategy default from STRATEGY_DEFAULTS based on
         ``state["strategy"]`` (or "dgrid" as the universal default).
      3. Module-level DEFAULT_* constant.
    """
    strategy = str(state.get("strategy") or "dgrid").lower()
    strat_defaults = STRATEGY_DEFAULTS.get(strategy) or STRATEGY_DEFAULTS["dgrid"]

    def _pick(state_key: str, defaults_key: str, fallback: float) -> float:
        sv = state.get(state_key)
        if sv is not None and sv != "":
            try:
                return float(sv)
            except (TypeError, ValueError):
                pass
        return float(strat_defaults.get(defaults_key, fallback))

    # Track whether the user explicitly set pm_trail_arm_usd so manage_positions
    # knows whether to override the pct-of-margin calculation.
    trail_arm_usd_explicit = state.get("pm_trail_arm_usd")
    trail_arm_usd_explicit = (
        trail_arm_usd_explicit is not None and trail_arm_usd_explicit != ""
    )
    return {
        "partial_tp_bp": _pick("pm_partial_tp_bp", "partial_tp_bp", DEFAULT_PARTIAL_TP_BP),
        "partial_tp_fraction": _pick("pm_partial_tp_fraction", "partial_tp_fraction", DEFAULT_PARTIAL_TP_FRACTION),
        "cut_confidence": _pick("pm_cut_confidence_threshold", "cut_confidence", DEFAULT_CUT_CONFIDENCE),
        "cut_fraction": _pick("pm_cut_fraction", "cut_fraction", DEFAULT_CUT_FRACTION),
        "trail_arm_usd": _pick("pm_trail_arm_usd", "trail_arm_usd", DEFAULT_TRAIL_ARM_USD),
        "trail_arm_usd_explicit": bool(trail_arm_usd_explicit),
        "trail_arm_pct": _pick("pm_trail_arm_pct", "trail_arm_pct", DEFAULT_TRAIL_ARM_PCT),
        "trail_give_back_fraction": _pick(
            "pm_trail_give_back_fraction", "trail_give_back_fraction",
            DEFAULT_TRAIL_GIVE_BACK_FRACTION,
        ),
        "stale_hold_seconds": _pick("pm_stale_hold_seconds", "stale_hold_seconds", DEFAULT_STALE_HOLD_SECONDS),
        "stale_vol_expansion_ratio": _pick(
            "pm_stale_vol_expansion_ratio", "stale_vol_expansion_ratio",
            DEFAULT_STALE_VOL_EXPANSION_RATIO,
        ),
        "cooldown_seconds": _pick("pm_cooldown_seconds", "cooldown_seconds", DEFAULT_COOLDOWN_SECONDS),
        "cooldown_size_dampener": _pick(
            "pm_cooldown_size_dampener", "cooldown_size_dampener",
            DEFAULT_COOLDOWN_SIZE_DAMPENER,
        ),
        "cooldown_regain_seconds": _pick(
            "pm_cooldown_regain_seconds", "cooldown_regain_seconds",
            DEFAULT_COOLDOWN_REGAIN_SECONDS,
        ),
        # Phase 2 exit rails.
        "breakeven_arm_bp": _pick(
            "pm_breakeven_arm_bp", "breakeven_arm_bp", DEFAULT_BREAKEVEN_ARM_BP
        ),
        "breakeven_exit_offset_bp": _pick(
            "pm_breakeven_exit_offset_bp", "breakeven_exit_offset_bp",
            DEFAULT_BREAKEVEN_EXIT_OFFSET_BP,
        ),
        "max_underwater_seconds": _pick(
            "pm_max_underwater_seconds", "max_underwater_seconds",
            DEFAULT_MAX_UNDERWATER_SECONDS,
        ),
        "mom_rev_confidence": _pick(
            "pm_mom_rev_confidence", "mom_rev_confidence",
            DEFAULT_MOM_REV_CONFIDENCE,
        ),
        "inventory_cap_usd": _pick(
            "pm_inventory_cap_usd", "inventory_cap_usd",
            DEFAULT_INVENTORY_CAP_USD,
        ),
        "strategy": strategy,
    }


def _split_inventory(positions: list[dict], product_id: int) -> tuple[float, float, float, float]:
    """Return (long_units, short_units, long_unrealized_usd, short_unrealized_usd)."""
    long_u = 0.0
    short_u = 0.0
    long_pnl = 0.0
    short_pnl = 0.0
    for p in positions or []:
        try:
            if int(p.get("product_id", -1)) != int(product_id):
                continue
        except (TypeError, ValueError):
            continue
        amt = abs(float(p.get("amount") or 0.0))
        side = str(p.get("side") or "").upper()
        if amt <= 0 or side not in {"LONG", "SHORT"}:
            continue
        # Prefer unrealized_pnl, fall back to net_pnl - realized_pnl.
        try:
            u = float(p.get("unrealized_pnl") or 0.0)
            if u == 0:
                net = p.get("net_pnl")
                real = p.get("realized_pnl")
                if net is not None and real is not None:
                    u = float(net) - float(real)
        except (TypeError, ValueError):
            u = 0.0
        if side == "LONG":
            long_u += amt
            long_pnl += u
        else:
            short_u += amt
            short_pnl += u
    return long_u, short_u, long_pnl, short_pnl


def _pnl_bp_on_inventory(pnl_usd: float, inv_units: float, vwap: float) -> float:
    """Return PnL in bp on the inventory's cost basis."""
    cost_basis = abs(inv_units) * float(vwap or 0.0)
    if cost_basis <= 0:
        return 0.0
    return (pnl_usd / cost_basis) * 1e4


def _update_hwm(state: dict, long_pnl: float, short_pnl: float) -> tuple[float, float]:
    hwm = state.setdefault("pm_hwm", {"long": 0.0, "short": 0.0})
    if not isinstance(hwm, dict):
        hwm = {"long": 0.0, "short": 0.0}
        state["pm_hwm"] = hwm
    hwm["long"] = max(float(hwm.get("long") or 0.0), float(long_pnl))
    hwm["short"] = max(float(hwm.get("short") or 0.0), float(short_pnl))
    return float(hwm["long"]), float(hwm["short"])


def _execute_close(
    *,
    telegram_id: int,
    product: str,
    side: str,
    size_base: float,
    state: dict,
    source: str,
) -> dict[str, Any]:
    """Reduce-only market close. Returns the trade_service result dict.

    Imported lazily to keep this module unit-testable without the heavy
    trade_service dependency chain.
    """
    if size_base <= 0:
        return {"success": False, "error": "size <= 0", "skipped": True}
    try:
        from src.nadobro.services.trade_service import execute_market_order
    except Exception as exc:
        logger.warning("PM cannot import trade_service: %s", exc)
        return {"success": False, "error": f"import: {exc}"}
    # side here refers to the inventory we're closing; the order side flips:
    # long inventory closes with a SELL, short inventory closes with a BUY.
    is_long = side == "short"  # closing a SHORT = buy back; closing a LONG = sell
    leverage = max(1.0, float(state.get("leverage") or 1.0))
    result = execute_market_order(
        telegram_id,
        product,
        size_base,
        is_long=is_long,
        leverage=leverage,
        slippage_pct=float(state.get("pm_market_slippage_pct") or 1.0),
        enforce_rate_limit=False,
        source=source,
        strategy_session_id=state.get("strategy_session_id"),
        reduce_only=True,
    )
    return result or {"success": False, "error": "no result"}


def _should_cut_against_regime(regime: str, long_u: float, short_u: float,
                                long_pnl: float, short_pnl: float, confidence: float,
                                cfg: dict) -> Optional[dict]:
    if confidence < cfg["cut_confidence"]:
        return None
    if regime == REGIME_TREND_DOWN and long_u > 0 and long_pnl < 0:
        return {
            "type": ACTION_ADVERSE_CUT,
            "side": "long",
            "size_base": long_u * cfg["cut_fraction"],
            "reason": f"regime={regime} conf={confidence:.2f} long_pnl={long_pnl:.2f}",
        }
    if regime == REGIME_TREND_UP and short_u > 0 and short_pnl < 0:
        return {
            "type": ACTION_ADVERSE_CUT,
            "side": "short",
            "size_base": short_u * cfg["cut_fraction"],
            "reason": f"regime={regime} conf={confidence:.2f} short_pnl={short_pnl:.2f}",
        }
    return None


def _evaluate_partial_tp(side: str, inv_u: float, pnl_usd: float, vwap: float,
                         cfg: dict) -> Optional[dict]:
    if inv_u <= 0 or pnl_usd <= 0 or vwap <= 0:
        return None
    pnl_bp = _pnl_bp_on_inventory(pnl_usd, inv_u, vwap)
    if pnl_bp < cfg["partial_tp_bp"]:
        return None
    return {
        "type": ACTION_PARTIAL_TP,
        "side": side,
        "size_base": inv_u * cfg["partial_tp_fraction"],
        "reason": f"pnl_bp={pnl_bp:.1f} vwap={vwap:.4f}",
    }


def _resolve_trail_arm_usd(cfg: dict, inv_u: float, vwap: float, leverage: float) -> float:
    """Effective trail-arm threshold in USD.

    If the user explicitly set ``pm_trail_arm_usd`` we honour it verbatim (legacy
    behaviour). Otherwise we scale the arm by margin so a $1k notional doesn't
    arm at the same $0.50 as a $100k notional: arm = trail_arm_pct% * margin.
    """
    if cfg.get("trail_arm_usd_explicit"):
        return float(cfg.get("trail_arm_usd") or 0.0)
    pct = float(cfg.get("trail_arm_pct") or 0.0)
    margin_usd = abs(inv_u) * float(vwap or 0.0) / max(1.0, float(leverage or 1.0))
    pct_arm = pct / 100.0 * margin_usd
    # Fall back to the legacy USD floor when pct-of-margin is too small to
    # cover even one fee tick (e.g. very small positions on cheap markets).
    return max(pct_arm, float(cfg.get("trail_arm_usd") or 0.0))


def _evaluate_trail(side: str, inv_u: float, pnl_usd: float, hwm: float,
                    cfg: dict, *, vwap: float = 0.0, leverage: float = 1.0) -> Optional[dict]:
    if inv_u <= 0:
        return None
    arm_usd = _resolve_trail_arm_usd(cfg, inv_u, vwap, leverage)
    if hwm <= arm_usd:
        return None
    threshold = cfg["trail_give_back_fraction"] * hwm
    if pnl_usd >= threshold:
        return None
    return {
        "type": ACTION_TRAIL_CLOSE,
        "side": side,
        "size_base": inv_u,
        "reason": f"hwm={hwm:.2f} pnl={pnl_usd:.2f} threshold={threshold:.2f} arm={arm_usd:.2f}",
    }


def _evaluate_stale(state: dict, regime_info: dict, side: str, inv_u: float,
                    cfg: dict) -> Optional[dict]:
    if inv_u <= 0:
        return None
    last_fill_ts = float(state.get("grid_last_fill_ts") or 0.0)
    if last_fill_ts <= 0:
        return None
    age = time.time() - last_fill_ts
    if age < cfg["stale_hold_seconds"]:
        return None
    expansion_bp = float(regime_info.get("range_expansion_bp") or 0.0)
    realized_bp = float(regime_info.get("realized_vol_bp") or 0.0)
    # "Normal" baseline: half the configured min_spread or 5 bp, whichever is larger.
    normal_bp = max(5.0, 0.5 * float(state.get("dgrid_min_spread_bp") or 5.0))
    if max(expansion_bp, realized_bp) < cfg["stale_vol_expansion_ratio"] * normal_bp:
        return None
    return {
        "type": ACTION_STALE_FLATTEN,
        "side": side,
        "size_base": inv_u,
        "reason": f"age={age:.0f}s expansion_bp={expansion_bp:.1f} normal_bp={normal_bp:.1f}",
    }


# ---------------------------------------------------------------------------
# Phase 2 — universal exit rails
# ---------------------------------------------------------------------------


def _update_breakeven_arm(state: dict, side: str, pnl_usd: float, inv_u: float,
                          vwap: float, cfg: dict) -> bool:
    """Track per-side "have we ever been above the BE arm threshold this trade?".

    Once armed, the flag stays set until the side flattens. Returns True when
    the BE stop should fire (we were green, now we're back to / below entry).
    """
    armed_map = state.setdefault("pm_be_armed", {"long": False, "short": False})
    if not isinstance(armed_map, dict):
        armed_map = {"long": False, "short": False}
        state["pm_be_armed"] = armed_map
    if inv_u <= 0:
        # Side flat: reset arm so the next trade starts clean.
        armed_map[side] = False
        return False
    arm_bp = float(cfg.get("breakeven_arm_bp") or 0.0)
    if arm_bp <= 0:
        return False
    pnl_bp = _pnl_bp_on_inventory(pnl_usd, inv_u, vwap)
    if not armed_map.get(side):
        if pnl_bp >= arm_bp:
            armed_map[side] = True
        return False
    # Already armed. Trigger when PnL drops to (or below) the exit floor —
    # entry + exit_offset_bp so we cover round-trip fees, not literally zero.
    exit_offset_bp = float(cfg.get("breakeven_exit_offset_bp") or 0.0)
    return pnl_bp <= exit_offset_bp


def _evaluate_breakeven_stop(state: dict, side: str, inv_u: float,
                             pnl_usd: float, vwap: float, cfg: dict) -> Optional[dict]:
    # Always call the arm-tracker so a flatten on this side resets the latch
    # for the next trade (it returns False when inv_u <= 0).
    if not _update_breakeven_arm(state, side, pnl_usd, inv_u, vwap, cfg):
        return None
    pnl_bp = _pnl_bp_on_inventory(pnl_usd, inv_u, vwap)
    return {
        "type": ACTION_BREAKEVEN_STOP,
        "side": side,
        "size_base": inv_u,
        "reason": (
            f"pnl_bp={pnl_bp:.1f} arm_bp={float(cfg['breakeven_arm_bp']):.1f} "
            f"exit_offset_bp={float(cfg['breakeven_exit_offset_bp']):.1f}"
        ),
    }


def _update_time_in_loss_ts(state: dict, side: str, pnl_usd: float, inv_u: float,
                            now: float) -> float:
    """Track "underwater since" timestamp per side.

    Returns the seconds the side has been continuously underwater (0 if not
    currently in loss or just flipped).
    """
    ts_map = state.setdefault("pm_loss_since_ts", {"long": 0.0, "short": 0.0})
    if not isinstance(ts_map, dict):
        ts_map = {"long": 0.0, "short": 0.0}
        state["pm_loss_since_ts"] = ts_map
    if inv_u <= 0:
        ts_map[side] = 0.0
        return 0.0
    if pnl_usd >= 0:
        ts_map[side] = 0.0
        return 0.0
    if float(ts_map.get(side) or 0.0) <= 0:
        ts_map[side] = float(now)
        return 0.0
    return max(0.0, float(now) - float(ts_map.get(side) or 0.0))


def _evaluate_time_in_loss(state: dict, side: str, inv_u: float, pnl_usd: float,
                           cfg: dict, now: float) -> Optional[dict]:
    if inv_u <= 0:
        return None
    max_underwater = float(cfg.get("max_underwater_seconds") or 0.0)
    if max_underwater <= 0:
        return None
    age = _update_time_in_loss_ts(state, side, pnl_usd, inv_u, now)
    if age < max_underwater:
        return None
    return {
        "type": ACTION_TIME_IN_LOSS_STOP,
        "side": side,
        "size_base": inv_u,
        "reason": f"underwater_seconds={age:.0f} max={max_underwater:.0f} pnl={pnl_usd:.2f}",
    }


def _evaluate_momentum_reversal(regime: str, confidence: float,
                                long_u: float, short_u: float,
                                long_pnl: float, short_pnl: float,
                                cfg: dict) -> Optional[dict]:
    """Take everything on a *winning* side when the regime flips against it.

    Different from adverse_cut (which trims 50% of a *losing* side under
    similar conditions). This locks in profit before the reversal eats it.
    """
    threshold = float(cfg.get("mom_rev_confidence") or 0.0)
    if threshold <= 0 or confidence < threshold:
        return None
    if regime == REGIME_TREND_DOWN and long_u > 0 and long_pnl > 0:
        return {
            "type": ACTION_MOM_REVERSAL_CLOSE,
            "side": "long",
            "size_base": long_u,
            "reason": f"regime={regime} conf={confidence:.2f} long_pnl={long_pnl:.2f}",
        }
    if regime == REGIME_TREND_UP and short_u > 0 and short_pnl > 0:
        return {
            "type": ACTION_MOM_REVERSAL_CLOSE,
            "side": "short",
            "size_base": short_u,
            "reason": f"regime={regime} conf={confidence:.2f} short_pnl={short_pnl:.2f}",
        }
    return None


def _evaluate_inventory_cap(side: str, inv_u: float, vwap: float,
                            mid: float, cfg: dict) -> Optional[dict]:
    """Hard cap: when |inv_usd| > cap, flatten the side breaching it.

    Per-strategy default lives in STRATEGY_DEFAULTS (Mid carries a non-zero
    cap; the other strategies default to 0 = disabled). Notional is
    measured at mid so a sudden price move on a thin product can't sit
    underweight forever.
    """
    cap = float(cfg.get("inventory_cap_usd") or 0.0)
    if cap <= 0 or inv_u <= 0:
        return None
    ref_price = float(mid or vwap or 0.0)
    if ref_price <= 0:
        return None
    inv_usd = inv_u * ref_price
    if inv_usd <= cap:
        return None
    overflow = inv_usd - cap
    # Close enough size to bring inventory back under the cap. Round up
    # slightly so we don't end up just over after the close.
    size_base = min(inv_u, overflow / ref_price * 1.05)
    return {
        "type": ACTION_INVENTORY_CAP,
        "side": side,
        "size_base": size_base,
        "reason": f"inv_usd={inv_usd:.2f} cap={cap:.2f} overflow={overflow:.2f}",
    }


def manage_positions(
    *,
    telegram_id: int,
    product: str,
    product_id: int,
    state: dict,
    positions: list[dict],
    regime_info: dict[str, Any],
    enabled: bool = True,
    mid: float = 0.0,
) -> dict[str, Any]:
    """Run all PM rules. Issues reduce-only market closes for any actions found.

    Returns a summary dict suitable for inclusion in run_cycle's result row.
    Idempotent enough for safe replay: tracks the last-action timestamp per
    type so we don't repeatedly fire the same partial close in adjacent cycles.

    ``mid`` is only used by the Phase 2 inventory_cap rule. It defaults to 0
    so existing callers without mid still work — the cap is also gated by a
    non-zero ``inventory_cap_usd`` (only Mid Mode carries a non-zero default).
    """
    out: dict[str, Any] = {
        "enabled": bool(enabled),
        "actions": [],
        "cooldown_active": False,
        "size_dampener": 1.0,
    }
    if not enabled:
        return out

    cfg = _pm_config(state)
    now = time.time()

    # --- Cooldown gating ---------------------------------------------------
    cooldown_until = float(state.get("pm_cooldown_until") or 0.0)
    if cooldown_until > now:
        # Fully suppressed for first 1/3 of cooldown, gradually rising over the rest.
        elapsed = now - (cooldown_until - cfg["cooldown_seconds"])
        first_third = cfg["cooldown_seconds"] / 3.0
        if elapsed < first_third:
            dampener = cfg["cooldown_size_dampener"] * 0.5
        else:
            dampener = cfg["cooldown_size_dampener"]
        out["cooldown_active"] = True
        out["size_dampener"] = dampener
        out["cooldown_remaining_s"] = int(cooldown_until - now)
        # We still let position-management actions fire during cooldown;
        # cooldown only affects new-quote sizing.

    long_u, short_u, long_pnl, short_pnl = _split_inventory(positions, product_id)
    hwm_long, hwm_short = _update_hwm(state, long_pnl, short_pnl)

    # Pull VWAPs already tracked by mm_bot.
    vwap_buy = float(state.get("grid_buy_exposure_price") or 0.0)
    vwap_sell = float(state.get("grid_sell_exposure_price") or 0.0)

    confidence = float(regime_info.get("confidence") or 0.0)
    regime = str(regime_info.get("regime") or "")

    actions: list[dict] = []

    # Rule 1: partial TP per side
    a = _evaluate_partial_tp("long", long_u, long_pnl, vwap_buy, cfg)
    if a:
        actions.append(a)
    a = _evaluate_partial_tp("short", short_u, short_pnl, vwap_sell, cfg)
    if a:
        actions.append(a)

    # Rule 2: adverse cut on regime flip
    a = _should_cut_against_regime(regime, long_u, short_u, long_pnl, short_pnl, confidence, cfg)
    if a:
        actions.append(a)

    # Rule 3: trailing close. Pass per-side VWAP + leverage so the arm
    # threshold scales with margin (see _resolve_trail_arm_usd).
    leverage = max(1.0, float(state.get("leverage") or 1.0))
    a = _evaluate_trail("long", long_u, long_pnl, hwm_long, cfg, vwap=vwap_buy, leverage=leverage)
    if a:
        actions.append(a)
    a = _evaluate_trail("short", short_u, short_pnl, hwm_short, cfg, vwap=vwap_sell, leverage=leverage)
    if a:
        actions.append(a)

    # Rule 4: stale flatten
    a = _evaluate_stale(state, regime_info, "long", long_u, cfg)
    if a:
        actions.append(a)
    a = _evaluate_stale(state, regime_info, "short", short_u, cfg)
    if a:
        actions.append(a)

    # Rule 5: breakeven stop (Phase 2). Per-side latch — once unrealized
    # PnL crosses the arm threshold we lock in "never let it go red." Fires
    # the full-close when PnL rolls back through entry + exit_offset.
    a = _evaluate_breakeven_stop(state, "long", long_u, long_pnl, vwap_buy, cfg)
    if a:
        actions.append(a)
    a = _evaluate_breakeven_stop(state, "short", short_u, short_pnl, vwap_sell, cfg)
    if a:
        actions.append(a)

    # Rule 6: time-in-loss stop (Phase 2). Closes any side that has been
    # continuously underwater longer than max_underwater_seconds.
    a = _evaluate_time_in_loss(state, "long", long_u, long_pnl, cfg, now)
    if a:
        actions.append(a)
    a = _evaluate_time_in_loss(state, "short", short_u, short_pnl, cfg, now)
    if a:
        actions.append(a)

    # Rule 7: momentum-reversal close (Phase 2). Take everything on a
    # *winning* side when the regime flips against it with high confidence.
    a = _evaluate_momentum_reversal(
        regime, confidence, long_u, short_u, long_pnl, short_pnl, cfg
    )
    if a:
        actions.append(a)

    # Rule 8: inventory cap (Phase 2). Mid Mode hard rail since it disables
    # anchor + soft reset. No-op for strategies with cap_usd = 0.
    a = _evaluate_inventory_cap("long", long_u, vwap_buy, mid, cfg)
    if a:
        actions.append(a)
    a = _evaluate_inventory_cap("short", short_u, vwap_sell, mid, cfg)
    if a:
        actions.append(a)

    # De-dup. Two passes:
    #   1) Drop duplicates of the same (type, side).
    #   2) A full-close action on a side suppresses any partial close action
    #      on that same side — otherwise we'd ask the exchange to close 100%
    #      of the side and then ask it to partial close 33% of (now-empty)
    #      inventory, which spends an order slot for no reason.
    seen: set[tuple[str, str]] = set()
    deduped: list[dict] = []
    for a in actions:
        k = (a["type"], a["side"])
        if k in seen:
            continue
        seen.add(k)
        deduped.append(a)
    full_close_types = {
        ACTION_TRAIL_CLOSE,
        ACTION_STALE_FLATTEN,
        ACTION_BREAKEVEN_STOP,
        ACTION_TIME_IN_LOSS_STOP,
        ACTION_MOM_REVERSAL_CLOSE,
    }
    full_closed_sides = {a["side"] for a in deduped if a["type"] in full_close_types}
    pruned: list[dict] = []
    for a in deduped:
        if a["type"] not in full_close_types and a["side"] in full_closed_sides:
            continue
        pruned.append(a)

    # --- Execute closes ----------------------------------------------------
    for a in pruned:
        size_base = max(0.0, float(a.get("size_base") or 0.0))
        if size_base <= 0:
            continue
        source = f"dgrid_pm_{a['type']}"
        try:
            res = _execute_close(
                telegram_id=telegram_id,
                product=product,
                side=a["side"],
                size_base=size_base,
                state=state,
                source=source,
            )
        except Exception as exc:
            logger.exception("PM close failed: %s", exc)
            res = {"success": False, "error": str(exc)}
        a["result"] = {"success": bool(res.get("success")), "error": res.get("error")}
        logger.info(
            "PM %s %s size=%.6f reason=%s success=%s",
            a["type"], a["side"], size_base, a["reason"], a["result"]["success"],
        )

    # Reset HWM after a successful trail close (so we don't keep trying to
    # close again before new HWM is built).
    for a in pruned:
        if a.get("type") == ACTION_TRAIL_CLOSE and a.get("result", {}).get("success"):
            hwm = state.get("pm_hwm") or {}
            hwm[a["side"]] = 0.0
            state["pm_hwm"] = hwm

    out["actions"] = pruned
    out["long_units"] = long_u
    out["short_units"] = short_u
    out["long_unrealized_usd"] = round(long_pnl, 6)
    out["short_unrealized_usd"] = round(short_pnl, 6)
    state["pm_last_run_ts"] = now
    state["pm_last_actions"] = [
        {k: v for k, v in a.items() if k != "result"} for a in pruned
    ]
    state["pm_last_action_count"] = len(pruned)
    return out


def trigger_cooldown(state: dict, reason: str) -> None:
    """Called by mm_bot on session SL/TP. Starts the PM cooldown."""
    cfg = _pm_config(state)
    state["pm_cooldown_until"] = time.time() + cfg["cooldown_seconds"]
    state["pm_cooldown_reason"] = str(reason or "")
    logger.info("PM cooldown started: %s seconds. Reason: %s", cfg["cooldown_seconds"], reason)


def cooldown_size_dampener(state: dict) -> float:
    """Read the current size dampener, called by run_cycle each loop."""
    cfg = _pm_config(state)
    cd_until = float(state.get("pm_cooldown_until") or 0.0)
    now = time.time()
    if cd_until <= now:
        return 1.0
    remaining = cd_until - now
    # Smoothly recover dampener back to 1.0 over the regain_seconds window.
    regain = max(1.0, cfg["cooldown_regain_seconds"])
    fraction_left = min(1.0, remaining / regain)
    floor = cfg["cooldown_size_dampener"]
    return floor + (1.0 - floor) * (1.0 - fraction_left)
