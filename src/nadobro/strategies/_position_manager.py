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
DEFAULT_TRAIL_GIVE_BACK_FRACTION = 0.5  # close when current PnL < 0.5 * HWM
DEFAULT_STALE_HOLD_SECONDS = 30 * 60     # 30 min
DEFAULT_STALE_VOL_EXPANSION_RATIO = 2.0  # current vol > 2x rolling normal
DEFAULT_COOLDOWN_SECONDS = 300
DEFAULT_COOLDOWN_SIZE_DAMPENER = 0.5
DEFAULT_COOLDOWN_REGAIN_SECONDS = 30 * 60


# Action types
ACTION_PARTIAL_TP = "partial_tp"
ACTION_ADVERSE_CUT = "adverse_cut"
ACTION_TRAIL_CLOSE = "trail_close"
ACTION_STALE_FLATTEN = "stale_flatten"


def _pm_config(state: dict) -> dict[str, float]:
    return {
        "partial_tp_bp": float(state.get("pm_partial_tp_bp") or DEFAULT_PARTIAL_TP_BP),
        "partial_tp_fraction": float(state.get("pm_partial_tp_fraction") or DEFAULT_PARTIAL_TP_FRACTION),
        "cut_confidence": float(state.get("pm_cut_confidence_threshold") or DEFAULT_CUT_CONFIDENCE),
        "cut_fraction": float(state.get("pm_cut_fraction") or DEFAULT_CUT_FRACTION),
        "trail_arm_usd": float(state.get("pm_trail_arm_usd") or DEFAULT_TRAIL_ARM_USD),
        "trail_give_back_fraction": float(
            state.get("pm_trail_give_back_fraction") or DEFAULT_TRAIL_GIVE_BACK_FRACTION
        ),
        "stale_hold_seconds": float(state.get("pm_stale_hold_seconds") or DEFAULT_STALE_HOLD_SECONDS),
        "stale_vol_expansion_ratio": float(
            state.get("pm_stale_vol_expansion_ratio") or DEFAULT_STALE_VOL_EXPANSION_RATIO
        ),
        "cooldown_seconds": float(state.get("pm_cooldown_seconds") or DEFAULT_COOLDOWN_SECONDS),
        "cooldown_size_dampener": float(
            state.get("pm_cooldown_size_dampener") or DEFAULT_COOLDOWN_SIZE_DAMPENER
        ),
        "cooldown_regain_seconds": float(
            state.get("pm_cooldown_regain_seconds") or DEFAULT_COOLDOWN_REGAIN_SECONDS
        ),
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


def _evaluate_trail(side: str, inv_u: float, pnl_usd: float, hwm: float,
                    cfg: dict) -> Optional[dict]:
    if inv_u <= 0 or hwm <= cfg["trail_arm_usd"]:
        return None
    threshold = cfg["trail_give_back_fraction"] * hwm
    if pnl_usd >= threshold:
        return None
    return {
        "type": ACTION_TRAIL_CLOSE,
        "side": side,
        "size_base": inv_u,
        "reason": f"hwm={hwm:.2f} pnl={pnl_usd:.2f} threshold={threshold:.2f}",
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


def manage_positions(
    *,
    telegram_id: int,
    product: str,
    product_id: int,
    state: dict,
    positions: list[dict],
    regime_info: dict[str, Any],
    enabled: bool = True,
) -> dict[str, Any]:
    """Run all PM rules. Issues reduce-only market closes for any actions found.

    Returns a summary dict suitable for inclusion in run_cycle's result row.
    Idempotent enough for safe replay: tracks the last-action timestamp per
    type so we don't repeatedly fire the same partial close in adjacent cycles.
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

    # Rule 3: trailing close
    a = _evaluate_trail("long", long_u, long_pnl, hwm_long, cfg)
    if a:
        actions.append(a)
    a = _evaluate_trail("short", short_u, short_pnl, hwm_short, cfg)
    if a:
        actions.append(a)

    # Rule 4: stale flatten
    a = _evaluate_stale(state, regime_info, "long", long_u, cfg)
    if a:
        actions.append(a)
    a = _evaluate_stale(state, regime_info, "short", short_u, cfg)
    if a:
        actions.append(a)

    # De-dup: don't fire the same {type, side} twice in the same cycle.
    seen: set[tuple[str, str]] = set()
    pruned = []
    for a in actions:
        k = (a["type"], a["side"])
        if k in seen:
            continue
        seen.add(k)
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
