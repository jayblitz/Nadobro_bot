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

import logging
from typing import Optional

from src.nadobro.market_data import nadoexplorer_client as explorer
from src.nadobro.models.database import update_copy_trader_stats
from src.nadobro.trading.copy_service import add_trader

logger = logging.getLogger(__name__)

LEADERBOARD_PAGE_SIZE = 5
# Discovery quality floor: hide dust accounts whose "top PnL" is unreplicable.
MIN_LEADER_EQUITY_USD = 1000.0


def leaderboard_page(page: int = 0, *, sort: str = "pnl", period: str = "30") -> list[dict]:
    """One page of ranked wallets (entity=wallet, deduped by the client).

    Returns [] when the explorer is unreachable — callers render a
    "leaderboard unavailable, paste a wallet instead" fallback.
    """
    page = max(0, int(page))
    rows = explorer.get_leaderboard(
        period=period,
        sort=sort,
        limit=LEADERBOARD_PAGE_SIZE,
        offset=page * LEADERBOARD_PAGE_SIZE,
        min_equity=MIN_LEADER_EQUITY_USD,
    )
    for rank, row in enumerate(rows):
        row["rank"] = page * LEADERBOARD_PAGE_SIZE + rank + 1
    return rows


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
            )
        except Exception:  # noqa: BLE001 - stats are cosmetic; the follow must succeed
            logger.warning("copy discovery: stat stamp failed for trader %s", trader_id)
    return ok, msg, trader_id
