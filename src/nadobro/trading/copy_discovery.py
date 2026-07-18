"""Copy-trading leader discovery — NadoExplorer leaderboard + trader cards.

This is the DISCOVERY plane only: ranking, previews, and follow-from-
leaderboard. The mirroring plane (position polling, sizing, TP/SL, closes)
stays on the venue read-only client in copy_service — the venue exposes entry
price, leverage, and the leader's TP/SL orders, which the explorer API does
not publish.

Following a trader here writes the copy_traders row exactly like the manual
wallet-paste path (add_trader) and additionally stamps the leaderboard stat
columns (total_pnl_usd, total_volume_usd, nado_points, win_rate,
last_updated_at) that the schema has carried unused since the leaderboard was
first scaffolded.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import math
from typing import Optional

from src.nadobro.market_data import nadoexplorer_client as explorer
from src.nadobro.models.database import update_copy_trader_stats
from src.nadobro.trading.copy_service import add_trader

logger = logging.getLogger(__name__)

LEADERBOARD_PAGE_SIZE = 5
# Discovery quality floor: hide dust accounts whose "top PnL" is unreplicable.
MIN_LEADER_EQUITY_USD = 1000.0
MIN_LEADER_ACTIVE_DAYS = 10
MIN_LEADER_CLOSED_TRADES = 20
MAX_LEADER_IDLE_HOURS = 7 * 24
QUALITY_CANDIDATE_PAGE_SIZE = 50
# Bound one interactive discovery refresh to 1,000 candidates / 20 HTTP
# pages. If the provider has more, fail closed rather than burn the shared
# 120-request/minute budget or present a truncated prefix as global.
MAX_QUALITY_CANDIDATE_PAGES = 20


@dataclass(frozen=True)
class LeaderboardPage:
    """A discovery page with explicit availability and pagination state."""

    rows: list[dict]
    has_more: bool
    available: bool


def _as_finite(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _last_activity_age_hours(value: object, now: datetime) -> float | None:
    """Return a non-negative age for ISO timestamps or Unix second/millis.

    NadoExplorer currently supplies ISO timestamps, but accepting both common
    numeric forms makes a provider format change fail closed in ranking rather
    than promote a stale trader as "recent".
    """
    try:
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if timestamp > 10_000_000_000:  # Unix milliseconds.
                timestamp /= 1000.0
            when = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        elif isinstance(value, str) and value.strip():
            normalized = value.strip().replace("Z", "+00:00")
            when = datetime.fromisoformat(normalized)
            if when.tzinfo is None:
                when = when.replace(tzinfo=timezone.utc)
            else:
                when = when.astimezone(timezone.utc)
        else:
            return None
    except (OverflowError, OSError, TypeError, ValueError):
        return None
    return max(0.0, (now - when).total_seconds() / 3600.0)


def _rank_quality_rows(rows: list[dict], *, now: datetime | None = None) -> list[dict]:
    """Rank active, repeatable leaders by risk-adjusted ROI rather than PnL.

    Absolute PnL favours the largest accounts, while raw ROI favours stale or
    tiny samples. This score gives most weight to participation and recency,
    then to ROI adjusted for sample size and drawdown. PnL is a tie-breaker
    only, never the discovery driver.
    """
    now = now or datetime.now(timezone.utc)
    ranked: list[dict] = []
    for source in rows:
        row = dict(source)
        roi = _as_finite(row.get("roi"))
        active_days_raw = row.get("active_days")
        closed_trades_raw = row.get("closed_trades")
        drawdown = _as_finite(row.get("max_drawdown_pct"))
        win_rate = _as_finite(row.get("win_rate"))
        try:
            active_days = int(active_days_raw) if active_days_raw is not None else -1
            closed_trades = int(closed_trades_raw) if closed_trades_raw is not None else -1
            period_days = max(1, int(row.get("period_days") or 30))
        except (TypeError, ValueError):
            continue
        idle_hours = _last_activity_age_hours(row.get("last_activity_at"), now)
        if (
            roi is None
            or roi <= 0
            or active_days < MIN_LEADER_ACTIVE_DAYS
            or closed_trades < MIN_LEADER_CLOSED_TRADES
            or idle_hours is None
            or idle_hours > MAX_LEADER_IDLE_HOURS
        ):
            continue
        # Missing drawdown/win rate must not be interpreted as a perfect score.
        if drawdown is None or win_rate is None:
            continue
        drawdown = max(0.0, drawdown)
        win_rate = max(0.0, win_rate)
        activity_score = min(1.0, active_days / period_days)
        recency_score = 2 ** (-idle_hours / 48.0)
        risk_adjusted_roi = (
            (roi / (roi + 0.25))
            * (closed_trades / (closed_trades + 50.0))
            * (1.0 / (1.0 + drawdown))
        )
        win_rate_score = min(1.0, win_rate / 0.60)
        row["quality_score"] = (
            0.40 * activity_score
            + 0.25 * recency_score
            + 0.30 * risk_adjusted_roi
            + 0.05 * win_rate_score
        )
        row["idle_hours"] = idle_hours
        ranked.append(row)

    def _sort_key(row: dict) -> tuple[float, float, int, int, float]:
        return (
            float(row.get("quality_score") or 0.0),
            float(row.get("roi") or 0.0),
            int(row.get("active_days") or 0),
            int(row.get("closed_trades") or 0),
            float(row.get("pnl_usd") or 0.0),
        )

    return sorted(ranked, key=_sort_key, reverse=True)


def _load_quality_candidates(period: str) -> list[dict] | None:
    """Load the complete server-filtered ROI pool before local ranking.

    Each API page is independently cached by the explorer client. If any
    uncached page cannot be fetched safely, return ``None`` instead of ranking
    an incomplete prefix as though it were the global leaderboard.
    """
    candidates: list[dict] = []
    seen_wallets: set[str] = set()
    seen_pages: set[tuple[str, ...]] = set()
    offset = 0
    for _page_number in range(MAX_QUALITY_CANDIDATE_PAGES):
        result = explorer.get_leaderboard_result(
            period=period,
            sort="roi",
            limit=QUALITY_CANDIDATE_PAGE_SIZE,
            offset=offset,
            min_equity=MIN_LEADER_EQUITY_USD,
            min_active_days=MIN_LEADER_ACTIVE_DAYS,
        )
        if result is None:
            logger.warning(
                "copy discovery: quality leaderboard incomplete at offset %s",
                offset,
            )
            return None

        page_rows = list(result.get("rows") or [])
        has_more = bool(result.get("has_more"))
        page_identity = tuple(
            str(row.get("wallet_address") or "") for row in page_rows
        )
        if page_identity in seen_pages or (has_more and not page_rows):
            logger.warning(
                "copy discovery: quality leaderboard made no progress at offset %s",
                offset,
            )
            return None
        seen_pages.add(page_identity)

        previous_count = len(candidates)
        for row in page_rows:
            wallet = str(row.get("wallet_address") or "")
            if wallet and wallet not in seen_wallets:
                candidates.append(row)
                seen_wallets.add(wallet)
        if has_more and len(candidates) == previous_count:
            logger.warning(
                "copy discovery: quality leaderboard repeated wallets at offset %s",
                offset,
            )
            return None
        if not has_more:
            return candidates
        offset += QUALITY_CANDIDATE_PAGE_SIZE
    logger.warning(
        "copy discovery: quality leaderboard exceeded safe page cap (%s pages)",
        MAX_QUALITY_CANDIDATE_PAGES,
    )
    return None


def leaderboard_page(
    page: int = 0, *, sort: str = "quality", period: str = "30"
) -> LeaderboardPage:
    """One page of ranked wallet leaders with outage-safe pagination.

    ``quality`` is the recommended default: it loads every server-filtered ROI
    candidate, rejects stale/tiny samples, then ranks the complete pool before
    slicing. This prevents raw API page order from promoting inactive high-PnL
    accounts or silently treating the first API page as the global result.
    """
    page = max(0, int(page))
    sort = sort if sort in {"quality", "pnl", "roi"} else "quality"
    if sort == "quality":
        candidates = _load_quality_candidates(period)
        if candidates is None:
            return LeaderboardPage(rows=[], has_more=False, available=False)
        ranked = _rank_quality_rows(candidates)
        start = page * LEADERBOARD_PAGE_SIZE
        rows = ranked[start : start + LEADERBOARD_PAGE_SIZE]
        has_more = len(ranked) > start + LEADERBOARD_PAGE_SIZE
    else:
        result = explorer.get_leaderboard_result(
            period=period,
            sort=sort,
            limit=LEADERBOARD_PAGE_SIZE,
            offset=page * LEADERBOARD_PAGE_SIZE,
            min_equity=MIN_LEADER_EQUITY_USD,
            min_active_days=MIN_LEADER_ACTIVE_DAYS,
        )
        if result is None:
            return LeaderboardPage(rows=[], has_more=False, available=False)
        rows = list(result.get("rows") or [])
        has_more = bool(result.get("has_more"))
    for rank, row in enumerate(rows):
        row["rank"] = page * LEADERBOARD_PAGE_SIZE + rank + 1
    return LeaderboardPage(rows=rows, has_more=has_more, available=True)


def trader_card(wallet: str) -> dict:
    """Everything the preview screen shows for a leaderboard trader.

    Explorer-sourced; degrades field-by-field (each key may be None/[] when
    the corresponding endpoint fails) so the card renders what it has.
    """
    summary = explorer.get_trader_daily_summary(wallet, range_="30d")
    positions = explorer.get_trader_live_positions(wallet)
    open_notional = sum(float(p.get("valueUsd") or 0.0) for p in positions)
    return {
        "wallet_address": wallet,
        "summary_30d": summary,
        "open_positions": positions,
        "open_position_count": len(positions),
        "open_notional_usd": open_notional,
    }


def follow_from_leaderboard(
    telegram_id: int, wallet: str, row: Optional[dict] = None
) -> tuple[bool, str, int | None]:
    """Create (or reuse) the private copy_traders row for a leaderboard pick
    and stamp its stat columns from the leaderboard row when provided."""
    label = f"Top trader {wallet[:6]}…{wallet[-4:]}"
    ok, msg, trader_id = add_trader(
        wallet, label=label, is_curated=False, owner_user_id=telegram_id
    )
    if ok and trader_id and row:
        try:
            update_copy_trader_stats(
                trader_id,
                total_pnl_usd=float(row.get("pnl_usd") or 0.0),
                total_volume_usd=float(row.get("volume_usd") or 0.0),
                nado_points=float(row.get("nado_points") or 0.0),
                win_rate=float(row.get("win_rate") or 0.0),
                leader_roi=row.get("roi"),
                leader_active_days=row.get("active_days"),
                leader_period_days=row.get("period_days"),
                leader_last_activity_at=row.get("last_activity_at"),
                leader_closed_trades=row.get("closed_trades"),
                leader_max_drawdown_pct=row.get("max_drawdown_pct"),
            )
        except Exception:  # noqa: BLE001 - stats are cosmetic; the follow must succeed
            logger.warning("copy discovery: stat stamp failed for trader %s", trader_id)
    return ok, msg, trader_id
