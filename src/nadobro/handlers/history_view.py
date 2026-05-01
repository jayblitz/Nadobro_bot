from __future__ import annotations

from decimal import Decimal
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from src.nadobro.utils.visual import divider, money, signed


def render_history_view(snapshot: dict[str, Any], page: int = 0, page_size: int = 10) -> tuple[str, InlineKeyboardMarkup]:
    network = str(snapshot.get("network") or "mainnet").upper()
    matches = sorted(list(snapshot.get("matches") or []), key=_submission_idx_sort_key, reverse=True)
    total_pages = max(1, (len(matches) + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    visible = matches[page * page_size:(page + 1) * page_size]

    lines = [f"📜 Trade History · {network}    Page {page + 1}/{total_pages}", divider()]
    for idx, match in enumerate(visible, start=page * page_size + 1):
        order = match.get("order") or {}
        base = _dec_x18(match.get("base_filled"))
        quote = _dec_x18(match.get("quote_filled"))
        fee = abs(_dec_x18(match.get("fee")))
        pnl = _dec_x18(match.get("realized_pnl") or 0)
        symbol = str(match.get("product_name") or f"ID:{match.get('product_id') or order.get('product_id') or '?'}")
        side = "📈" if base >= 0 else "📉"
        lines.extend([
            f"{idx} ╱ {symbol} {side} FILL",
            f"📦 Size {abs(base)}    💰 Quote {money(abs(quote))}",
            f"{'🟢' if pnl >= 0 else '🔴'} PnL {signed(pnl)}",
            f"🏷 Fee -{money(fee)}",
            f"🕐 {str(match.get('timestamp') or '—')} UTC",
            "",
        ])
    if not visible:
        lines.append("No synced trade history yet.")
    lines.append(divider())
    rows = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ Prev", callback_data=f"portfolio:history:{page - 1}"))
    if page + 1 < total_pages:
        nav.append(InlineKeyboardButton("Next ➡", callback_data=f"portfolio:history:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅ Portfolio", callback_data="portfolio:view")])
    return "\n".join(lines)[:3500], InlineKeyboardMarkup(rows)


def _dec_x18(value: Any) -> Decimal:
    from src.nadobro.utils.x18 import from_x18

    if value is None or value == "":
        return Decimal("0")
    return from_x18(value)


def _submission_idx_sort_key(match: dict[str, Any]) -> Decimal:
    try:
        return Decimal(str(match.get("submission_idx") or 0))
    except Exception:
        return Decimal("0")
