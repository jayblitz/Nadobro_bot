from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from src.nadobro.utils.visual import b, divider, esc, money, pnl_dot, signed_money


PAGE_SIZE = 5


def render_history_view(
    snapshot: dict[str, Any],
    page: int = 0,
    page_size: int = PAGE_SIZE,
) -> tuple[str, InlineKeyboardMarkup]:
    """Render the History tab — round-trips for non-strategy trades only.

    Per the workflow plan:
    - Strategy fills are excluded (they live in the Performance tab).
    - Manual fills are paired open/close FIFO into round-trips via
      :func:`trade_service.compute_round_trips`.
    - Each round-trip gets its own ``Share PnL`` button so the user can
      mint a per-trade card. The card is requested via
      ``portfolio:share_pnl:rt:{trip_key}``.
    """
    from src.nadobro.services.trade_service import compute_round_trips

    network = str(snapshot.get("network") or "mainnet")
    user_id = int(snapshot.get("user_id") or 0)
    try:
        round_trips = compute_round_trips(user_id, network, limit=200) if user_id else []
    except Exception:
        round_trips = []
    total_pages = max(1, (len(round_trips) + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    visible = round_trips[page * page_size:(page + 1) * page_size]

    lines = [
        f"📜 <b>Trade History</b> · {esc(network.upper())} · page {page + 1}/{total_pages}",
        "Manual trades only. Strategy sessions live under Performance",
        divider(),
    ]
    rows: list[list[InlineKeyboardButton]] = []
    for idx, trip in enumerate(visible, start=page * page_size + 1):
        pair = str(trip.get("pair") or trip.get("product_name") or f"ID:{trip.get('product_id')}")
        side = "📈 long" if str(trip.get("side") or "").lower() == "long" else "📉 short"
        size = _dec(trip.get("size"))
        open_px = _dec(trip.get("avg_open_price"))
        close_px = _dec(trip.get("avg_close_price"))
        pnl = _dec(trip.get("realized_pnl"))
        fees = _dec(trip.get("fees"))
        funding = _dec(trip.get("funding_paid"))
        volume = _dec(trip.get("volume_usd"))
        hold = _hold_duration(trip.get("open_ts"), trip.get("close_ts"))
        margin = "iso" if bool(trip.get("isolated")) else "cross"
        lines.extend([
            f"{idx}. {b(pair)}  {side} · {margin}",
            f"    {abs(size)} @ {money(open_px)} → {money(close_px)} · held {hold}",
            f"    Realized {pnl_dot(pnl)} {signed_money(pnl)} · Fees -{money(abs(fees))} · "
            f"Funding {signed_money(-funding)} · Vol {money(volume)}",  # funding_paid > 0 is a cost
            "",
        ])
        rows.append(
            [InlineKeyboardButton(
                f"📤 Share PnL · #{idx}",
                callback_data=f"portfolio:share_pnl:rt:{trip.get('trip_key')}",
            )]
        )
    if not visible:
        lines.append("No manual trades yet")

    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ Back", callback_data=f"portfolio:history:{page - 1}"))
    if page + 1 < total_pages:
        nav.append(InlineKeyboardButton("Next ➡", callback_data=f"portfolio:history:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("📈 Performance", callback_data="portfolio:performance")])
    rows.append([InlineKeyboardButton("⬅ Portfolio", callback_data="portfolio:view")])
    return "\n".join(lines)[:3500], InlineKeyboardMarkup(rows)


def _dec(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _hold_duration(open_ts: Any, close_ts: Any) -> str:
    if not (isinstance(open_ts, datetime) and isinstance(close_ts, datetime)):
        return "—"
    seconds = int((close_ts - open_ts).total_seconds())
    if seconds < 60:
        return f"{max(0, seconds)}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"
    return f"{seconds // 86400}d{(seconds % 86400) // 3600:02d}h"
