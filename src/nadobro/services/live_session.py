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

* ``realized_pnl`` — venue-authoritative per-match PnL when synced, else the
  recorder rows' buy/sell/fee decomposition (see ``_session_realized_pnl``).
  Fees are already netted in the recorder fallback and surfaced separately, so
  they are NOT subtracted again here.
* ``unrealized_pnl`` — the open position's ``est_pnl`` (exactly the uPnL Nado
  shows). The $32-loss bug was an *open* position, so a realized-only basis
  would never have tripped the stop — uPnL is the dominant term in-flight.
* ``funding_paid`` — paid-positive; reduces PnL. Best-effort from the session
  row during a live run (funding is rolled up authoritatively at finalize).

Attribution is by ``product_id`` + session time window rather than the fragile
``strategy_session_id`` tag, so the numbers are correct even when the engine
never wrote an ``order_intents`` digest for a fill.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

# A positions row older than this (vs its ``synced_at``) is treated as stale and
# the snapshot tries a direct venue read instead. ~2 portfolio-sync ticks.
_POSITION_STALE_SECONDS = 90.0


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


def _aggregate_db_position(rows: list) -> dict:
    """Net an open product's cross + isolated rows into a single position view."""
    net_signed = 0.0
    unrealized = 0.0
    margin_used = 0.0
    dominant = None
    dominant_abs = -1.0
    newest_sync = 0.0
    for r in rows:
        size = abs(_f(r.get("size")))
        side = str(r.get("side") or "").lower()
        signed = size if side == "long" else -size
        net_signed += signed
        unrealized += _f(r.get("est_pnl"))
        margin_used += _f(r.get("margin_used"))
        newest_sync = max(newest_sync, _f(r.get("synced_ts")))
        if size > dominant_abs:
            dominant_abs = size
            dominant = r
    side = "long" if net_signed > 0 else "short" if net_signed < 0 else ""
    return {
        "has_position": abs(net_signed) > 0,
        "position_size": abs(net_signed),
        "position_side": side,
        "entry_price": _f(dominant.get("avg_entry_price")) if dominant else 0.0,
        "liq_price": _f(dominant.get("est_liq_price")) if dominant else 0.0,
        "leverage": _f(dominant.get("leverage")) if dominant else 0.0,
        "margin_used": margin_used,
        "unrealized_pnl": unrealized,
        "synced_ts": newest_sync,
    }


def _live_position_from_client(client, product_id: int) -> Optional[dict]:
    """Fresh position read straight from Nado for one product (best-effort)."""
    if client is None:
        return None
    try:
        positions = client.get_all_positions() or []
    except Exception:  # noqa: BLE001 - display/guard path must never raise
        logger.debug("live position read failed pid=%s", product_id, exc_info=True)
        return None
    net_signed = 0.0
    unrealized = 0.0
    dominant = None
    dominant_abs = -1.0
    matched = False
    for p in positions:
        if int(p.get("product_id") or 0) != int(product_id):
            continue
        matched = True
        size = abs(_f(p.get("amount")))
        signed = _f(p.get("signed_amount"), size if str(p.get("side")).upper() == "LONG" else -size)
        net_signed += signed
        unrealized += _f(p.get("unrealized_pnl"))
        if size > dominant_abs:
            dominant_abs = size
            dominant = p
    if not matched:
        # Nado authoritatively reports no position on this product → flat.
        return {
            "has_position": False, "position_size": 0.0, "position_side": "",
            "entry_price": 0.0, "liq_price": 0.0, "leverage": 0.0,
            "margin_used": 0.0, "unrealized_pnl": 0.0, "synced_ts": time.time(),
        }
    side = "long" if net_signed > 0 else "short" if net_signed < 0 else ""
    return {
        "has_position": abs(net_signed) > 0,
        "position_size": abs(net_signed),
        "position_side": side,
        "entry_price": _f(dominant.get("price")) if dominant else 0.0,
        "liq_price": _f(dominant.get("liquidation_price")) if dominant else 0.0,
        "leverage": 0.0,
        "margin_used": 0.0,
        "unrealized_pnl": unrealized,
        "synced_ts": time.time(),
    }


def get_live_session_snapshot(
    telegram_id: int,
    network: str,
    session: dict,
    *,
    state: Optional[dict] = None,
    client=None,
) -> dict:
    """Return the live figures Nado shows for ``session`` (an active
    ``strategy_sessions`` row). Read-only and best-effort: never raises, and
    degrades to whatever sources are reachable. Blocking (DB + optional venue
    call) — call via ``run_blocking`` from a coroutine.
    """
    from src.nadobro.models.database import (
        count_open_orders_for_product,
        get_open_position_rows_for_product,
        get_session_live_metrics,
    )

    session = session or {}
    product_id = session.get("product_id")
    started_at = session.get("started_at")
    stopped_at = session.get("stopped_at")
    window = (product_id, started_at, stopped_at)

    session_id = int(session.get("id") or 0)
    metrics = get_session_live_metrics(session_id, network, window) if session_id else {}

    # --- open position / unrealized PnL (DB first, fresh venue read if stale) --
    position = {
        "has_position": False, "position_size": 0.0, "position_side": "",
        "entry_price": 0.0, "liq_price": 0.0, "leverage": 0.0,
        "margin_used": 0.0, "unrealized_pnl": 0.0, "synced_ts": 0.0,
    }
    if product_id is not None:
        rows = get_open_position_rows_for_product(int(telegram_id), network, int(product_id))
        if rows:
            position = _aggregate_db_position(rows)
        stale = (time.time() - _f(position.get("synced_ts"), 0.0)) > _POSITION_STALE_SECONDS
        if (not rows or stale) and client is not None:
            live = _live_position_from_client(client, int(product_id))
            if live is not None:
                position = live

    # --- open orders (DB first, fresh venue read as fallback) ------------------
    open_orders = 0
    if product_id is not None:
        open_orders = count_open_orders_for_product(int(telegram_id), network, int(product_id))
        if open_orders == 0 and client is not None:
            try:
                open_orders = len(client.get_open_orders(int(product_id)) or [])
            except Exception:  # noqa: BLE001
                logger.debug("live open-orders read failed pid=%s", product_id, exc_info=True)

    realized = _f(metrics.get("realized_pnl"))
    fees = _f(metrics.get("fees"))
    volume = _f(metrics.get("volume"))
    fills = int(metrics.get("fills") or 0)
    unrealized = _f(position.get("unrealized_pnl"))
    funding_paid = _f((session or {}).get("total_funding_paid"))

    session_pnl = realized + unrealized - funding_paid
    margin = _resolve_margin(state, session)
    session_pnl_pct = (session_pnl / margin * 100.0) if margin > 0 else 0.0

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
        **position,
    }
