from __future__ import annotations

from decimal import Decimal
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from src.nadobro.utils.visual import divider, money, pct, signed


def render_positions_view(snapshot: dict[str, Any], page: int = 0, page_size: int = 6) -> tuple[str, InlineKeyboardMarkup]:
    network = str(snapshot.get("network") or "mainnet").upper()
    positions = sorted(
        list(snapshot.get("positions") or []),
        key=lambda p: (str(p.get("symbol") or p.get("product_name") or ""), int(p.get("product_id") or 0), bool(p.get("isolated"))),
    )
    total_pages = max(1, (len(positions) + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    visible = positions[page * page_size:(page + 1) * page_size]

    lines = [f"🚀 Open Positions ({len(positions)}) · {network}", divider()]
    for idx, pos in enumerate(visible, start=page * page_size + 1):
        symbol = str(pos.get("symbol") or pos.get("product_name") or f"ID:{pos.get('product_id')}")
        margin = "🔒 ISO" if bool(pos.get("isolated")) else "⚖️ CROSS"
        direction = "📈 LONG" if bool(pos.get("is_long", True)) else "📉 SHORT"
        lines.extend([
            f"{idx} ╱ {symbol} {margin} {direction}",
            f"🎯 Entry {money(_dec(pos.get('avg_entry_price')))}    📡 Mark —",
            f"📦 Size {abs(_dec(pos.get('amount')))}       💎 Value {money(_dec(pos.get('notional_value')))}",
            f"{'🟢' if _dec(pos.get('est_pnl')) >= 0 else '🔴'} uPnL {signed(_dec(pos.get('est_pnl')))} ({pct(_dec(pos.get('upnl_pct')) if pos.get('upnl_pct') is not None else Decimal('0'))})",
            f"💧 Liq {money(_dec(pos.get('est_liq_price'))) if pos.get('est_liq_price') is not None else '—'}        ⚖️ Lev {_dec(pos.get('leverage'))}x",
            "",
        ])
    if not visible:
        lines.append("No open positions.")
    lines.append(divider())

    rows = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ Prev", callback_data=f"portfolio:positions:{page - 1}"))
    if page + 1 < total_pages:
        nav.append(InlineKeyboardButton("Next ➡", callback_data=f"portfolio:positions:{page + 1}"))
    if nav:
        rows.append(nav)
    if positions:
        rows.append([InlineKeyboardButton("❌ Close All", callback_data="portfolio:close_all_confirm")])
    rows.append([InlineKeyboardButton("⬅ Portfolio", callback_data="portfolio:view")])
    return "\n".join(lines)[:3500], InlineKeyboardMarkup(rows)


def _dec(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
