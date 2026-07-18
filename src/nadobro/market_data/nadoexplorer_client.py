"""NadoExplorer public API client — the copy-trading DISCOVERY plane.

Read-only JSON endpoints on https://nadoexplorer.com (API version 2026-06-22,
no auth, 120 requests/minute/IP with ``x-ratelimit-remaining`` response
headers). Used to rank and preview traders for copy trading:

  GET /api/traders/leaderboard   ranked rows (pnlUsd, roi percentage points,
                                 winRate 0-1,
                                 profitFactor, equityUsd, maxDrawdownPct,
                                 closedTrades, badges, ...)      cache 60s
  GET /api/traders/{id}/daily    per-trader performance summary  cache 60s
  GET /api/traders/{id}/live     live positions/orders sections  cache 5s
  GET /api/traders/search        wallet/text search              cache 20s

The MIRRORING plane stays on the venue's read-only client
(trading/copy_service._load_leader_position_map) — the venue exposes entry
price, leverage, and the leader's TP/SL orders, none of which this API
publishes (live positions carry only side/amount/markPriceUsd/valueUsd/pnlUsd).

Budget discipline: every response's ``x-ratelimit-remaining`` is recorded;
below ``_RL_SOFT_FLOOR`` remaining, cached data is served stale and refreshes
are skipped rather than risking a 429 for the whole IP (which the mirroring
plane does not share, but other bot features might).
"""

from __future__ import annotations

import logging
import math
import threading
import time
from typing import Any, Optional

from src.nadobro.core.http_session import SESSION
from src.nadobro.utils.env import env_float, env_int, env_str

logger = logging.getLogger(__name__)

BASE_URL = env_str("NADOEXPLORER_BASE_URL", "https://nadoexplorer.com").rstrip("/")
_TIMEOUT_SECONDS = env_float("NADOEXPLORER_TIMEOUT_SECONDS", 8.0)
# Stop issuing optional refreshes when the shared per-IP budget runs low.
_RL_SOFT_FLOOR = env_int("NADOEXPLORER_RL_SOFT_FLOOR", 20)
_RL_WINDOW_SECONDS = 60.0

# TTLs mirror the API's own cache windows — refreshing faster buys nothing.
_LEADERBOARD_TTL = 60.0
_DAILY_TTL = 60.0
_LIVE_TTL = 5.0
_SEARCH_TTL = 20.0
_CACHE_MAX_ENTRIES = 256

_lock = threading.RLock()
_cache: dict[str, tuple[float, Any]] = {}
_ratelimit_remaining: int = 120
_ratelimit_observed_at: float = 0.0
_last_error_at: float = 0.0


def _prune_cache() -> None:
    if len(_cache) <= _CACHE_MAX_ENTRIES:
        return
    for key in sorted(_cache, key=lambda k: _cache[k][0])[: len(_cache) - _CACHE_MAX_ENTRIES]:
        _cache.pop(key, None)


def ratelimit_remaining() -> int:
    with _lock:
        return _ratelimit_remaining


def _get(path: str, params: dict | None = None, *, ttl: float) -> Any:
    """Cached GET. Returns parsed JSON, or the stale cached value (or None)
    when the budget is exhausted or the request fails — discovery is a UI
    feature and must degrade, never raise into a handler."""
    global _ratelimit_remaining, _ratelimit_observed_at, _last_error_at
    key = f"{path}?{sorted((params or {}).items())}"
    now = time.time()
    with _lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
        if _ratelimit_remaining < _RL_SOFT_FLOOR:
            # Protect the remaining shared-IP budget even on a cache miss.
            # After the documented one-minute rate window, reserve exactly one
            # probe so the client can observe a reset instead of staying
            # permanently pinned to the last low header value.
            if now - _ratelimit_observed_at < _RL_WINDOW_SECONDS:
                return hit[1] if hit else None
            _ratelimit_observed_at = now

    try:
        resp = SESSION.get(
            f"{BASE_URL}{path}", params=params or {}, timeout=_TIMEOUT_SECONDS
        )
        remaining = resp.headers.get("x-ratelimit-remaining")
        if remaining is not None:
            try:
                with _lock:
                    _ratelimit_remaining = int(remaining)
                    _ratelimit_observed_at = now
            except ValueError:
                pass
        if resp.status_code != 200:
            logger.warning("nadoexplorer %s -> HTTP %s", path, resp.status_code)
            with _lock:
                _last_error_at = now
                return _cache.get(key, (0, None))[1]
        data = resp.json()
        with _lock:
            _cache[key] = (now, data)
            _prune_cache()
        return data
    except Exception as e:  # noqa: BLE001 - degrade to stale/None, never raise into UI
        logger.warning("nadoexplorer %s failed: %s", path, e)
        with _lock:
            _last_error_at = now
            return _cache.get(key, (0, None))[1]


# ── public surface ────────────────────────────────────────────────────

VALID_PERIODS = ("all", "1", "7", "30", "90")
VALID_SORTS = ("pnl", "roi", "points")


def _optional_finite_float(value: object) -> float | None:
    """Parse an optional metric without manufacturing a perfect zero."""
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _roi_decimal(value: object) -> float:
    """Normalize Explorer ROI percentage points to the internal decimal form."""
    parsed = _optional_finite_float(value)
    return 0.0 if parsed is None else parsed / 100.0


def get_leaderboard(
    *,
    period: str = "30",
    sort: str = "pnl",
    limit: int = 10,
    offset: int = 0,
    min_equity: float | None = None,
    min_win_rate: float | None = None,
    min_active_days: int | None = None,
) -> list[dict]:
    """Ranked traders. Pinned to entity=wallet — the API returns the same
    trader as both a wallet row and a subaccount row, and copy_traders keys
    leaders by main wallet address."""
    if period not in VALID_PERIODS:
        period = "30"
    if sort not in VALID_SORTS:
        sort = "pnl"
    result = get_leaderboard_result(
        period=period,
        sort=sort,
        limit=limit,
        offset=offset,
        min_equity=min_equity,
        min_win_rate=min_win_rate,
        min_active_days=min_active_days,
    )
    return list((result or {}).get("rows") or [])


def get_leaderboard_result(
    *,
    period: str = "30",
    sort: str = "pnl",
    limit: int = 10,
    offset: int = 0,
    min_equity: float | None = None,
    min_win_rate: float | None = None,
    min_active_days: int | None = None,
) -> Optional[dict]:
    """Leaderboard rows plus the API's pagination signal.

    ``None`` means the request was unavailable (including a rate-limit-safe
    cache miss); an empty ``rows`` list is a successful query with no matching
    traders. Callers that page the UI need that distinction so they do not
    report an empty final page as an outage.
    """
    if period not in VALID_PERIODS:
        period = "30"
    if sort not in VALID_SORTS:
        sort = "pnl"
    params: dict[str, Any] = {
        "limit": max(1, min(50, int(limit))),
        "offset": max(0, int(offset)),
        "period": period,
        "sort": sort,
        "entity": "wallet",
    }
    if min_equity is not None:
        params["minEquity"] = float(min_equity)
    if min_win_rate is not None:
        params["minWinRate"] = float(min_win_rate)
    if min_active_days is not None:
        params["minActiveDays"] = max(0, int(min_active_days))
    data = _get("/api/traders/leaderboard", params, ttl=_LEADERBOARD_TTL)
    if data is None:
        return None
    if not isinstance(data, dict):
        logger.warning("nadoexplorer leaderboard returned a non-object payload")
        return None
    rows = data.get("rows") or []
    out = []
    for row in rows:
        wallet = row.get("walletAddress")
        if not wallet:
            continue
        try:
            active_days = int(row["activeDays"]) if row.get("activeDays") is not None else None
        except (TypeError, ValueError):
            active_days = None
        try:
            period_days = int(row["periodDays"]) if row.get("periodDays") is not None else None
        except (TypeError, ValueError):
            period_days = None
        out.append(
            {
                "wallet_address": str(wallet),
                "pnl_usd": float(row.get("pnlUsd") or 0.0),
                "roi": _roi_decimal(row.get("roi")),
                "win_rate": _optional_finite_float(row.get("winRate")),
                "profit_factor": float(row.get("profitFactor") or 0.0),
                "equity_usd": float(row.get("equityUsd") or 0.0),
                "volume_usd": float(row.get("volumeUsd") or 0.0),
                "closed_trades": int(row.get("closedTrades") or 0),
                "max_drawdown_pct": _optional_finite_float(
                    row.get("maxDrawdownPct")
                ),
                "nado_points": float(row.get("nadoPoints") or 0.0),
                "badges": list(row.get("badges") or []),
                "period_days": period_days,
                # Activity and recency are discovery-only fields. Keep missing
                # values as None rather than manufacturing a zero — quality
                # ranking must not mistake an unknown field for inactivity.
                "active_days": active_days,
                "last_activity_at": row.get("lastActivityAt"),
            }
        )
    return {
        "rows": out,
        "has_more": bool(data.get("hasMore")),
    }


def get_trader_daily_summary(wallet: str, *, range_: str = "30d") -> Optional[dict]:
    """Per-trader performance summary (pnlUsd, realizedPnlUsd, volumeUsd,
    feesUsd, fundingUsd, closedTrades, bestDayPnlUsd, worstDayPnlUsd, ...)."""
    if range_ not in ("7d", "30d", "90d", "180d", "1y"):
        range_ = "30d"
    data = _get(f"/api/traders/{wallet}/daily", {"range": range_}, ttl=_DAILY_TTL)
    if not data:
        return None
    return data.get("summary") or None


def get_trader_live_positions(wallet: str) -> list[dict]:
    """Live open positions from the explorer (display only — NOT the
    mirroring source; no entry price/leverage/TP/SL here). Fields per row:
    productId, symbol, side ('L'/'S'), amount, markPriceUsd, valueUsd,
    pnlUsd, accountLabel."""
    data = _get(f"/api/traders/{wallet}/live", {"section": "positions"}, ttl=_LIVE_TTL)
    tables = (data or {}).get("liveTables") or {}
    return list(tables.get("positions") or [])


def search_traders(query: str, *, limit: int = 8) -> list[dict]:
    q = (query or "").strip()[:128]
    if not q:
        return []
    data = _get("/api/traders/search", {"q": q, "limit": max(1, min(12, int(limit)))}, ttl=_SEARCH_TTL)
    return list((data or {}).get("rows") or [])


def snapshot() -> dict:
    """Diagnostics for /health-style surfaces."""
    with _lock:
        return {
            "ratelimit_remaining": _ratelimit_remaining,
            "cache_entries": len(_cache),
            "last_error_at": _last_error_at,
        }
