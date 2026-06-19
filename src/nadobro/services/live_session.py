"""Authoritative live snapshot of a strategy session, sourced from Nado.

This is the single place that defines "what Nado shows for this session". It is
consumed by three surfaces that previously disagreed (or read zero):

* the safety rails in ``bot_runtime._run_cycle`` — which fire SL/TP off the real
  session PnL (realized + **unrealized**), as a percentage of the configured
  margin;
* the ``/mm_status`` and ``/mm_fills`` dashboards;
* (indirectly) the finalize rollup, which now uses the same product+window
  attribution.

PnL convention
--------------
``session_pnl = realized_pnl + unrealized_pnl - funding_paid``

Everything is sourced from the session's OWN tagged fills (``strategy_session_id``)
in ``trades_<network>``, marked to the live mid — never the account-aggregate
position on the product. This is the fix for the false-SL bug (session #40): a
pre-existing / overlapping / untagged position on the same product can no longer
contaminate a run's PnL.

* ``session_pnl_gross = signed_cash + net_base * mark`` — exactly the session's
  realized + open-leg unrealized, with no VWAP matching needed. ``net_base`` is
  the session's signed open base (long +, short −) and ``signed_cash`` its signed
  cash flow (short +quote, long −quote), both from the session's tagged fills.
* ``realized_pnl`` — venue-authoritative per-match PnL when synced, else the
  recorder rows' buy/sell cash-flow (see ``_session_realized_pnl``). **GROSS of
  fees** — PnL is PnL. Fees are a standalone metric (``fees``), tracked per run
  and surfaced separately, NOT folded into PnL.
* ``unrealized_pnl = session_pnl_gross - realized_pnl`` — the open leg marked to
  the live mid. uPnL is the dominant term in-flight (the $32-loss bug was an open
  position), so a realized-only basis would never have tripped the stop.
* ``funding_paid`` — paid-positive; reduces PnL. Best-effort from the session
  row during a live run (funding is rolled up authoritatively at finalize).
"""

from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _resolve_margin(state: Optional[dict], session: Optional[dict]) -> float:
    """Allocated margin the SL/TP percentage is measured against. The user sets
    SL/TP as a % of this (SL 1% of $100 margin => stop at -$1)."""
    state = state or {}
    for key in ("notional_usd", "cycle_notional_usd", "fixed_margin_usd", "session_margin_usd"):
        v = _f(state.get(key), 0.0)
        if v > 0:
            return v
    if session:
        for key in ("notional_usd", "margin_usd", "target_quote"):
            v = _f(session.get(key), 0.0)
            if v > 0:
                return v
    return 0.0


def get_live_session_snapshot(
    telegram_id: int,
    network: str,
    session: dict,
    *,
    state: Optional[dict] = None,
    client=None,
    mark: Optional[float] = None,
) -> dict:
    """Live figures for ``session`` (a ``strategy_sessions`` row), scoped to
    THIS run only. Read-only, best-effort, never raises. Blocking — call via
    ``run_blocking``.

    PnL integrity: realized/volume/fees AND the open position come exclusively
    from this session's tagged fills in ``trades_<network>`` (NOT the account's
    aggregate position on the product). The session's net base is marked to the
    live ``mark`` so ``session_pnl`` reflects only what this run did — a
    pre-existing or other-source position on the same product can never
    contaminate it (the false-SL bug). The account/portfolio view shows the
    Nado aggregate separately.
    """
    from src.nadobro.models.database import (
        count_open_orders_for_product,
        get_session_live_metrics,
    )

    session = session or {}
    product_id = session.get("product_id")
    session_id = int(session.get("id") or 0)
    # Scope to BOTH this session AND this user — stats are unique per user, per
    # run. A session id is globally unique, but pinning user_id too means a
    # mis-tagged/venue-synced row can never leak another user's PnL here.
    metrics = (
        get_session_live_metrics(session_id, network, user_id=int(telegram_id))
        if session_id else {}
    )

    # Live mark for valuing the session's open base. Prefer the caller-supplied
    # mark (the rail already has the cycle mid); else read it once.
    mark_f = _f(mark, 0.0)
    if mark_f <= 0 and client is not None and product_id is not None:
        try:
            mp = client.get_market_price(int(product_id)) or {}
            mark_f = _f(mp.get("mid"), 0.0)
        except Exception:  # noqa: BLE001 - display/guard path must never raise
            logger.debug("live mark read failed pid=%s", product_id, exc_info=True)

    realized = _f(metrics.get("realized_pnl"))
    fees = _f(metrics.get("fees"))
    volume = _f(metrics.get("volume"))
    fills = int(metrics.get("fills") or 0)
    net_base = _f(metrics.get("net_base"))
    signed_cash = _f(metrics.get("signed_cash"))
    funding_paid = _f((session or {}).get("total_funding_paid"))

    # Safety: if we hold open base but couldn't read a live mark (client down /
    # market read failed), DO NOT value the open leg as ``signed_cash`` alone —
    # that is the raw cash spent and reads as a huge phantom loss, which would
    # trip a FALSE SL (the exact bug class we're fixing). Fall back to the mark
    # that zeroes the open-leg uPnL (== realized-only basis): we simply don't
    # know the unrealized yet, so report 0 rather than fabricate a loss.
    if mark_f <= 0 and abs(net_base) > 1e-12:
        mark_f = (realized - signed_cash) / net_base

    # session realized + unrealized (gross), marked to the live mark, from this
    # run's own fills. signed_cash + net_base*mark == realized + open-leg uPnL.
    session_pnl_gross = signed_cash + net_base * mark_f
    unrealized = session_pnl_gross - realized
    session_pnl = session_pnl_gross - funding_paid
    margin = _resolve_margin(state, session)
    session_pnl_pct = (session_pnl / margin * 100.0) if margin > 0 else 0.0

    # Position view = the SESSION's own net base (not the account aggregate).
    has_position = abs(net_base) > 1e-12
    position_side = "long" if net_base > 0 else "short" if net_base < 0 else ""
    # Effective breakeven from the marked uPnL: uPnL = net_base*(mark - entry).
    entry_price = (mark_f - unrealized / net_base) if net_base else 0.0

    # --- open orders for the product (session owns the product during a run) ---
    open_orders = 0
    if product_id is not None:
        open_orders = count_open_orders_for_product(int(telegram_id), network, int(product_id))
        if open_orders == 0 and client is not None:
            try:
                open_orders = len(client.get_open_orders(int(product_id)) or [])
            except Exception:  # noqa: BLE001
                logger.debug("live open-orders read failed pid=%s", product_id, exc_info=True)

    return {
        "product_id": product_id,
        "fills": fills,
        "volume": volume,
        "fees": fees,
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "funding_paid": funding_paid,
        "open_orders": open_orders,
        "session_pnl": session_pnl,
        "margin": margin,
        "session_pnl_pct": session_pnl_pct,
        "mark": mark_f,
        "has_position": has_position,
        "position_size": abs(net_base),
        "position_side": position_side,
        "entry_price": entry_price,
        "liq_price": 0.0,
        "leverage": _f((state or {}).get("leverage"), 0.0),
        "margin_used": 0.0,
        "net_base": net_base,
    }
