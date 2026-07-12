"""NLP vault metrics — pool TVL/APR and user PnL accounting."""

from __future__ import annotations

import logging
import os
import time
from typing import Optional

from src.nadobro.utils.env import env_int
from src.nadobro.services.nado_archive import query_nlp_lp_events, query_nlp_snapshots

logger = logging.getLogger(__name__)

_POOL_METRICS_TTL_SECONDS = max(30, env_int("NADO_VAULT_POOL_METRICS_TTL", 60))
_pool_metrics_cache: dict[str, tuple[float, dict]] = {}


def _x18(value) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(int(value)) / 1e18
    except (TypeError, ValueError):
        try:
            v = float(value)
            return v / 1e18 if abs(v) >= 1e9 else v
        except (TypeError, ValueError):
            return 0.0


def annualize_apr_from_snapshots(snapshots: list[dict]) -> tuple[float | None, str]:
    """Estimate pool APR from cumulative PnL delta over the snapshot window."""
    if len(snapshots) < 2:
        return None, "unavailable"
    ordered = sorted(snapshots, key=lambda s: int(s.get("timestamp") or 0))
    oldest, newest = ordered[0], ordered[-1]
    tvl0 = _x18(oldest.get("tvl"))
    tvl1 = _x18(newest.get("tvl"))
    pnl0 = _x18(oldest.get("cumulative_pnl"))
    pnl1 = _x18(newest.get("cumulative_pnl"))
    ts0 = int(oldest.get("timestamp") or 0)
    ts1 = int(newest.get("timestamp") or 0)
    days = (ts1 - ts0) / 86400.0 if ts1 > ts0 else 0.0
    if days <= 0:
        return None, "unavailable"
    avg_tvl = (tvl0 + tvl1) / 2.0
    if avg_tvl <= 0:
        return None, "unavailable"
    pnl_delta = pnl1 - pnl0
    apr = (pnl_delta / avg_tvl) * (365.0 / days) * 100.0
    return apr, "snapshots"


def get_pool_metrics(network: str, client=None) -> dict:
    """Return pool TVL and APR with short TTL cache."""
    now = time.time()
    cached = _pool_metrics_cache.get(network)
    if cached and (now - cached[0]) < _POOL_METRICS_TTL_SECONDS:
        return dict(cached[1])

    tvl_usdt0 = 0.0
    apr_pct: float | None = None
    apr_source = "unavailable"

    snapshots = query_nlp_snapshots(network, count=30, granularity=86400)
    if snapshots:
        latest = max(snapshots, key=lambda s: int(s.get("timestamp") or 0))
        tvl_usdt0 = _x18(latest.get("tvl"))
        apr_pct, apr_source = annualize_apr_from_snapshots(snapshots)

    if client is not None and tvl_usdt0 <= 0:
        try:
            stats = client.get_nlp_pool_stats() or {}
            tvl_usdt0 = float(stats.get("tvl_usdt0") or 0.0)
        except Exception as e:
            logger.debug("pool stats fallback failed network=%s err=%s", network, e)

    result = {
        "tvl_usdt0": tvl_usdt0,
        "apr_pct": apr_pct,
        "apr_source": apr_source,
    }
    _pool_metrics_cache[network] = (now, result)
    return dict(result)


def _balance_amount(row: dict) -> float:
    spot = row.get("spot") or row.get("pre_balance", {}).get("spot") or {}
    bal = spot.get("balance") or {}
    return _x18(bal.get("amount"))


def _oracle_from_event(event: dict) -> float:
    product = event.get("product") or {}
    spot = product.get("spot") or {}
    px = _x18(spot.get("oracle_price_x18"))
    return px if px > 0 else 1.0


def build_lp_ledger_from_archive(
    events_payload: dict,
    nlp_product_id: int,
) -> list[dict]:
    """Normalize mint/burn archive payload into chronological ledger rows."""
    events = events_payload.get("events") or []
    txs = events_payload.get("txs") or []
    tx_by_idx = {str(t.get("submission_idx")): t for t in txs if t.get("submission_idx") is not None}

    # Group NLP balance deltas by submission_idx for mint/burn sizing.
    nlp_delta: dict[str, float] = {}
    oracle_by_idx: dict[str, float] = {}
    for ev in events:
        idx = str(ev.get("submission_idx") or "")
        if not idx:
            continue
        if int(ev.get("product_id") or -1) == int(nlp_product_id):
            pre = _balance_amount(ev.get("pre_balance") or {})
            post = _balance_amount(ev.get("post_balance") or {})
            nlp_delta[idx] = post - pre
            oracle_by_idx[idx] = _oracle_from_event(ev)

    rows: list[dict] = []
    seen: set[str] = set()
    for tx in sorted(txs, key=lambda t: int(t.get("timestamp") or 0)):
        idx = str(tx.get("submission_idx") or "")
        if not idx or idx in seen:
            continue
        seen.add(idx)
        body = tx.get("tx") or {}
        ts = int(tx.get("timestamp") or 0)
        if "mint_nlp" in body:
            mint = body["mint_nlp"]
            quote = _x18(mint.get("quote_amount"))
            nlp_amt = max(0.0, nlp_delta.get(idx, 0.0))
            rows.append({
                "event_type": "mint",
                "timestamp": ts,
                "quote_usdt0": quote,
                "nlp_amount": nlp_amt,
                "submission_idx": idx,
            })
        elif "burn_nlp" in body:
            burn = body["burn_nlp"]
            nlp_amt = _x18(burn.get("nlp_amount"))
            if nlp_amt <= 0:
                nlp_amt = abs(min(0.0, nlp_delta.get(idx, 0.0)))
            oracle = oracle_by_idx.get(idx, 1.0)
            rows.append({
                "event_type": "burn",
                "timestamp": ts,
                "nlp_amount": nlp_amt,
                "quote_usdt0": nlp_amt * oracle,
                "submission_idx": idx,
            })
    rows.sort(key=lambda r: (r.get("timestamp") or 0, r.get("submission_idx") or ""))
    return rows


def compute_pnl_from_ledger(
    ledger: list[dict],
    current_lp_value_usdt0: float,
) -> dict:
    """Average-cost LP accounting → all-time earned + unrealized PnL."""
    total_deposited = 0.0
    total_withdrawn = 0.0
    cost_basis = 0.0
    nlp_units = 0.0

    for row in ledger:
        if row.get("event_type") == "mint":
            quote = float(row.get("quote_usdt0") or 0.0)
            nlp = float(row.get("nlp_amount") or 0.0)
            total_deposited += quote
            cost_basis += quote
            nlp_units += nlp
        elif row.get("event_type") == "burn":
            nlp = float(row.get("nlp_amount") or 0.0)
            quote = float(row.get("quote_usdt0") or 0.0)
            total_withdrawn += quote
            if nlp_units > 0 and nlp > 0:
                fraction = min(1.0, nlp / nlp_units)
                cost_basis = max(0.0, cost_basis * (1.0 - fraction))
                nlp_units = max(0.0, nlp_units - nlp)

    all_time = (current_lp_value_usdt0 + total_withdrawn) - total_deposited
    unrealized = current_lp_value_usdt0 - cost_basis
    return {
        "total_deposited_usdt0": total_deposited,
        "total_withdrawn_usdt0": total_withdrawn,
        "cost_basis_usdt0": cost_basis,
        "all_time_earned_usdt0": all_time,
        "unrealized_pnl_usdt0": unrealized,
    }


def get_user_pnl_metrics(
    network: str,
    subaccount_hex: str,
    nlp_product_id: int,
    current_lp_value_usdt0: float,
    *,
    cached_events: Optional[list[dict]] = None,
) -> dict:
    """Build user PnL from DB-cached events plus optional archive backfill."""
    ledger: list[dict] = []

    if cached_events:
        for row in cached_events:
            ledger.append(dict(row))

    if not ledger:
        payload = query_nlp_lp_events(network, subaccount_hex, limit=500)
        ledger = build_lp_ledger_from_archive(payload, nlp_product_id)

    return compute_pnl_from_ledger(ledger, current_lp_value_usdt0)


def deposit_room_usdt0(
    lp_value_usdt0: float,
    max_mintable_usdt0: float,
    cap_usdt0: float,
) -> float:
    cap_room = max(0.0, cap_usdt0 - lp_value_usdt0)
    return max(0.0, min(cap_room, max_mintable_usdt0))
