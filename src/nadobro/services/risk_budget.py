"""
Phase 5 — per-product daily loss budget + cross-strategy exposure registry.

Two independent safety rails, both persisted in ``bot_state`` so they
survive restarts and are visible across workers:

1. Daily loss budget — per ``(telegram_id, network, product, UTC-date)``.
   Tracks the day's PnL and trips two thresholds:
     * soft stop  → strategy stops opening / quoting new size, but the
       position manager still runs (existing inventory is managed down).
     * hard stop  → strategy stops entirely for a cooldown window
       (default 4h) and the PM cooldown is triggered so re-engagement
       after the window is gradual.

   PnL signal differs by strategy, by design:
     * mm_bot feeds a mark-to-market *snapshot* every cycle
       (``record_pnl_snapshot``). The budget tracks the running snapshot
       plus the day's low-water-mark. This catches the "holding a big
       loser" failure mode — the exact thing the audit flagged — without
       the telescoping bug that summing cycle-deltas would introduce.
     * volume_bot / alpha_agent feed *realized* PnL on each close
       (``record_realized_pnl``); those strategies fully realize each
       round-turn so the accumulation is exact.
   ``get_daily_pnl`` returns ``realized_accumulated + current_mtm`` — the
   honest combined number.

2. Cross-strategy exposure registry — per
   ``(telegram_id, network, product, strategy)``. Each strategy writes
   its net units + USD inventory every cycle; ``get_product_exposure``
   reads them all back so a dashboard (or a future coordinator) can see
   when two strategies are fighting on the same product.

Pure persistence helpers — no strategy logic here.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from src.nadobro.db import execute
from src.nadobro.models.database import get_bot_state, set_bot_state

logger = logging.getLogger(__name__)

DEFAULT_HARD_STOP_COOLDOWN_SECONDS = 4 * 3600  # 4 hours
# Per-product exposure rows older than this are treated as stale (the
# strategy stopped running) and excluded from the aggregate.
EXPOSURE_STALE_SECONDS = 600

_KNOWN_STRATEGIES = ("grid", "rgrid", "dgrid", "mid", "dn", "vol", "bro")


def _utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _budget_key(telegram_id: int, network: str, product: str, date: str | None = None) -> str:
    date = date or _utc_date()
    return f"risk_budget:{int(telegram_id)}:{network}:{str(product).upper()}:{date}"


def _exposure_key(telegram_id: int, network: str, product: str, strategy: str) -> str:
    return (
        f"strategy_exposure:{int(telegram_id)}:{network}:"
        f"{str(product).upper()}:{str(strategy).lower()}"
    )


# --- Daily loss budget ------------------------------------------------------


def _load_budget_row(telegram_id: int, network: str, product: str) -> dict:
    row = get_bot_state(_budget_key(telegram_id, network, product))
    if not row or not isinstance(row, dict):
        return {
            "realized_pnl_usd": 0.0,
            "current_mtm_usd": 0.0,
            "mtm_low_usd": 0.0,
            "hard_stop_until": 0.0,
            "updated_at": 0.0,
        }
    return row


def _save_budget_row(telegram_id: int, network: str, product: str, row: dict) -> None:
    row["updated_at"] = time.time()
    set_bot_state(_budget_key(telegram_id, network, product), row)


def record_pnl_snapshot(
    telegram_id: int, network: str, product: str, mtm_pnl_usd: float
) -> float:
    """Record the current mark-to-market PnL of open positions.

    Used by mm_bot, which can't cleanly attribute realized PnL per close
    but can report the live open-position PnL every cycle. Returns the
    combined daily PnL after the update.
    """
    row = _load_budget_row(telegram_id, network, product)
    mtm = float(mtm_pnl_usd or 0.0)
    row["current_mtm_usd"] = round(mtm, 6)
    row["mtm_low_usd"] = round(min(float(row.get("mtm_low_usd") or 0.0), mtm), 6)
    _save_budget_row(telegram_id, network, product, row)
    return round(float(row.get("realized_pnl_usd") or 0.0) + mtm, 6)


def record_realized_pnl(
    telegram_id: int, network: str, product: str, realized_pnl_usd: float
) -> float:
    """Accumulate a realized PnL chunk into today's per-product budget.

    Used by volume_bot / alpha_agent which realize each round-turn fully.
    Returns the combined daily PnL after the update.
    """
    row = _load_budget_row(telegram_id, network, product)
    total = float(row.get("realized_pnl_usd") or 0.0) + float(realized_pnl_usd or 0.0)
    row["realized_pnl_usd"] = round(total, 6)
    _save_budget_row(telegram_id, network, product, row)
    return round(total + float(row.get("current_mtm_usd") or 0.0), 6)


def get_daily_pnl(telegram_id: int, network: str, product: str) -> float:
    """Combined daily PnL = accumulated realized + current mark-to-market."""
    row = _load_budget_row(telegram_id, network, product)
    return round(
        float(row.get("realized_pnl_usd") or 0.0)
        + float(row.get("current_mtm_usd") or 0.0),
        6,
    )


def resolve_budget_thresholds(
    state: dict, notional_usd: float
) -> tuple[float, float, int]:
    """Resolve (soft_stop_usd, hard_stop_usd, cooldown_seconds) from state.

    Defaults scale with the configured per-cycle notional so a large-size
    user isn't halted by a small-size default and vice versa:
        soft = max(25, notional)
        hard = max(50, notional * 2)
    Set ``risk_budget_soft_stop_usd`` / ``risk_budget_hard_stop_usd`` to
    0 to disable that rail.
    """
    notional = max(0.0, float(notional_usd or 0.0))
    soft_raw = state.get("risk_budget_soft_stop_usd")
    hard_raw = state.get("risk_budget_hard_stop_usd")
    if soft_raw is None or soft_raw == "":
        soft = max(25.0, notional)
    else:
        try:
            soft = float(soft_raw)
        except (TypeError, ValueError):
            soft = max(25.0, notional)
    if hard_raw is None or hard_raw == "":
        hard = max(50.0, notional * 2.0)
    else:
        try:
            hard = float(hard_raw)
        except (TypeError, ValueError):
            hard = max(50.0, notional * 2.0)
    try:
        cooldown = int(
            state.get("risk_budget_hard_stop_cooldown_seconds")
            or DEFAULT_HARD_STOP_COOLDOWN_SECONDS
        )
    except (TypeError, ValueError):
        cooldown = DEFAULT_HARD_STOP_COOLDOWN_SECONDS
    return soft, hard, cooldown


def check_budget(
    telegram_id: int,
    network: str,
    product: str,
    *,
    soft_stop_usd: float,
    hard_stop_usd: float,
    cooldown_seconds: int = DEFAULT_HARD_STOP_COOLDOWN_SECONDS,
) -> dict:
    """Evaluate the daily budget. Returns a status dict.

    status ∈ {"ok", "soft_stopped", "hard_stopped"}.

    A hard stop sets / extends the cooldown timestamp persistently so the
    block survives restarts. A soft stop is re-derived from the live PnL
    every call (no persistent flag) so it clears automatically if the
    position recovers.
    """
    row = _load_budget_row(telegram_id, network, product)
    daily_pnl = round(
        float(row.get("realized_pnl_usd") or 0.0)
        + float(row.get("current_mtm_usd") or 0.0),
        6,
    )
    now = time.time()

    # An already-tripped hard stop stays in effect for the cooldown window.
    hard_until = float(row.get("hard_stop_until") or 0.0)
    if hard_until > now:
        return {
            "status": "hard_stopped",
            "daily_pnl_usd": daily_pnl,
            "hard_stop_until": hard_until,
            "cooldown_remaining_s": int(hard_until - now),
            "reason": "hard-stop cooldown active",
        }

    if hard_stop_usd > 0 and daily_pnl <= -abs(hard_stop_usd):
        row["hard_stop_until"] = now + max(60, int(cooldown_seconds))
        _save_budget_row(telegram_id, network, product, row)
        logger.warning(
            "Risk budget HARD STOP: uid=%s %s/%s daily_pnl=$%.2f <= -$%.2f "
            "— halting for %ds",
            telegram_id, network, product, daily_pnl, abs(hard_stop_usd),
            int(cooldown_seconds),
        )
        return {
            "status": "hard_stopped",
            "daily_pnl_usd": daily_pnl,
            "hard_stop_until": row["hard_stop_until"],
            "cooldown_remaining_s": int(cooldown_seconds),
            "reason": (
                f"daily loss ${daily_pnl:.2f} <= hard stop "
                f"-${abs(hard_stop_usd):.2f}"
            ),
        }

    if soft_stop_usd > 0 and daily_pnl <= -abs(soft_stop_usd):
        return {
            "status": "soft_stopped",
            "daily_pnl_usd": daily_pnl,
            "reason": (
                f"daily loss ${daily_pnl:.2f} <= soft stop "
                f"-${abs(soft_stop_usd):.2f}"
            ),
        }

    return {"status": "ok", "daily_pnl_usd": daily_pnl}


def clear_hard_stop(telegram_id: int, network: str, product: str) -> None:
    """Manually clear an active hard-stop cooldown (admin / user override)."""
    row = _load_budget_row(telegram_id, network, product)
    row["hard_stop_until"] = 0.0
    _save_budget_row(telegram_id, network, product, row)


# --- Cross-strategy exposure registry ---------------------------------------


def record_strategy_exposure(
    telegram_id: int,
    network: str,
    product: str,
    strategy: str,
    net_units: float,
    inv_usd: float,
) -> None:
    """Persist one strategy's current net exposure for a product."""
    set_bot_state(
        _exposure_key(telegram_id, network, product, strategy),
        {
            "strategy": str(strategy).lower(),
            "net_units": round(float(net_units or 0.0), 8),
            "inv_usd": round(float(inv_usd or 0.0), 4),
            "updated_at": time.time(),
        },
    )


def get_product_exposure(
    telegram_id: int,
    network: str,
    product: str,
    *,
    stale_seconds: int = EXPOSURE_STALE_SECONDS,
) -> dict:
    """Aggregate live exposure across every strategy on a product.

    Returns:
        {
          "net_units": float,        # signed sum across strategies
          "gross_inv_usd": float,    # sum of |inv_usd|
          "by_strategy": {strat: {net_units, inv_usd}},
          "conflicting": bool,       # True if some strat is long & another short
        }
    Rows older than ``stale_seconds`` are skipped (the strategy stopped).
    """
    by_strategy: dict[str, dict] = {}
    net_units = 0.0
    gross_inv = 0.0
    longs = 0
    shorts = 0
    now = time.time()
    for strat in _KNOWN_STRATEGIES:
        row = get_bot_state(_exposure_key(telegram_id, network, product, strat))
        if not row or not isinstance(row, dict):
            continue
        if stale_seconds > 0 and (now - float(row.get("updated_at") or 0.0)) > stale_seconds:
            continue
        nu = float(row.get("net_units") or 0.0)
        iv = float(row.get("inv_usd") or 0.0)
        if abs(nu) < 1e-12 and abs(iv) < 1e-9:
            continue
        by_strategy[strat] = {"net_units": nu, "inv_usd": iv}
        net_units += nu
        gross_inv += abs(iv)
        if nu > 0:
            longs += 1
        elif nu < 0:
            shorts += 1
    return {
        "net_units": round(net_units, 8),
        "gross_inv_usd": round(gross_inv, 4),
        "by_strategy": by_strategy,
        "conflicting": longs > 0 and shorts > 0,
    }


def clear_strategy_exposure(
    telegram_id: int, network: str, product: str, strategy: str
) -> None:
    """Drop a strategy's exposure row (e.g. when the strategy is stopped)."""
    try:
        execute(
            "DELETE FROM bot_state WHERE key = %s",
            (_exposure_key(telegram_id, network, product, strategy),),
        )
    except Exception:
        logger.exception(
            "Failed to clear exposure for uid=%s %s/%s/%s",
            telegram_id, network, product, strategy,
        )
