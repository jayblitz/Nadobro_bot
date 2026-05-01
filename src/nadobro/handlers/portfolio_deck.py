from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from src.nadobro.services.feature_flags import portfolio_sync_enabled, portfolio_sync_interval_seconds
from src.nadobro.services.nado_sync import sync_user
from src.nadobro.services.user_service import get_user
from src.nadobro.utils.visual import divider, money, pct, signed, stale_banner, time_ago


def portfolio_deck_kb(has_positions: bool = False, has_orders: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("🚀 Positions", callback_data="portfolio:positions"),
            InlineKeyboardButton("📋 Orders", callback_data="portfolio:orders"),
        ],
        [
            InlineKeyboardButton("📈 Performance", callback_data="portfolio:performance"),
            InlineKeyboardButton("📜 History", callback_data="portfolio:history"),
        ],
        [InlineKeyboardButton("🔄 Refresh", callback_data="portfolio:refresh")],
    ]
    if has_positions:
        rows.append([InlineKeyboardButton("❌ Close All", callback_data="portfolio:close_all_confirm")])
    rows.append([InlineKeyboardButton("🏠 Home", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


async def snapshot_for_user(user_id: int, *, force: bool = False) -> dict[str, Any]:
    user = get_user(user_id)
    network = user.network_mode.value if user else "mainnet"
    return await sync_user(user_id, network=network, reason="refresh" if force else "cold_render", force=force)


def render_portfolio_deck(snapshot: dict[str, Any]) -> tuple[str, InlineKeyboardMarkup]:
    network = str(snapshot.get("network") or "mainnet").upper()
    positions = list(snapshot.get("positions") or [])
    orders = list(snapshot.get("open_orders") or [])
    stats = snapshot.get("stats") or {}
    last_sync = _as_dt(snapshot.get("last_sync"))
    threshold = (portfolio_sync_interval_seconds() * 2) if portfolio_sync_enabled() else 300
    stale = stale_banner(last_sync, threshold) if last_sync else "⚠ Stale · last sync never"

    total_upnl = sum((_dec(p.get("est_pnl")) for p in positions if p.get("est_pnl") is not None), Decimal("0"))
    pos_value = sum((_dec(p.get("notional_value")) for p in positions), Decimal("0"))
    best = max(positions, key=lambda p: _dec(p.get("est_pnl")), default=None)
    worst = min(positions, key=lambda p: _dec(p.get("est_pnl")), default=None)

    lines = []
    if stale or snapshot.get("stale"):
        lines.append(stale or "⚠ Stale · last sync unknown")
    lines.extend([
        f"📋 Portfolio · {network}",
        divider(),
        f"📡 Synced {time_ago(last_sync) if last_sync else 'never'}",
        f"🚀 Positions {len(positions)}    📋 Orders {len(orders)}",
        f"💎 Value {money(pos_value)}    🟢 uPnL {signed(total_upnl)}",
        "",
        f"📈 Best  {_pos_symbol(best)} {signed(_dec((best or {}).get('est_pnl')))}",
        f"📉 Worst {_pos_symbol(worst)} {signed(_dec((worst or {}).get('est_pnl')))}",
        "",
        "⚡ Trading Stats (24h / 7d / 30d / All)",
        _volume_line(stats),
        f"🏷 Fees -{money(abs(_dec(stats.get('total_fees'))))} (all)",
        _funding_line(stats),
        f"🏆 Realized {signed(_dec(stats.get('total_pnl')))}    Win {_win_rate(stats)} · {int(stats.get('total_trades') or 0)} trades",
        divider(),
        "🔝 Top Positions",
    ])
    for idx, pos in enumerate(sorted(positions, key=lambda p: abs(_dec(p.get("est_pnl"))), reverse=True)[:5], start=1):
        direction = "📈" if bool(pos.get("is_long", True)) else "📉"
        lines.append(
            f"{idx} ╱ {_pos_symbol(pos)} {_margin(pos)} {direction} {money(_dec(pos.get('est_pnl')))} ({pct(_dec(pos.get('upnl_pct')) if pos.get('upnl_pct') is not None else Decimal('0'))})"
        )
    if not positions:
        lines.append("No open positions.")
    lines.append("")
    lines.append("📋 Open Orders")
    for idx, order in enumerate(orders[:3], start=1):
        side = "📈" if str(order.get("side") or "").upper() in {"LONG", "BUY"} else "📉"
        lines.append(f"{idx} ╱ {_order_symbol(order)} {side} LIMIT {money(_dec(order.get('price')))}")
    if not orders:
        lines.append("No open orders.")
    return "\n".join(lines)[:3500], portfolio_deck_kb(bool(positions), bool(orders))


def render_loading() -> str:
    return "⏳ Loading portfolio…"


def render_close_all_confirm() -> tuple[str, InlineKeyboardMarkup]:
    return (
        "❌ Close all open positions?\n\nThis will submit reduce-only market closes, then refresh Portfolio from Nado.",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("Yes — close all", callback_data="portfolio:close_all_yes")],
            [InlineKeyboardButton("Cancel", callback_data="portfolio:view")],
        ]),
    )


def _volume_line(stats: dict[str, Any]) -> str:
    windows = stats.get("volume_windows") or {}
    return (
        f"💰 Vol {money(_dec(windows.get('24h')))} / {money(_dec(windows.get('7d')))} / "
        f"{money(_dec(windows.get('30d')))} / {money(_dec(windows.get('all') or stats.get('total_volume')))}"
    )


def _funding_line(stats: dict[str, Any]) -> str:
    funding = _dec(stats.get("total_funding"))
    label = "paid" if funding > 0 else "received"
    return f"🔵 Funding {signed(funding)} ({label})"


def _win_rate(stats: dict[str, Any]) -> str:
    return f"{_dec(stats.get('win_rate')):.1f}%"


def _pos_symbol(pos: dict[str, Any] | None) -> str:
    if not pos:
        return "—"
    return str(pos.get("symbol") or pos.get("product_name") or f"ID:{pos.get('product_id')}")


def _order_symbol(order: dict[str, Any]) -> str:
    return str(order.get("product_name") or order.get("product") or f"ID:{order.get('product_id')}")


def _margin(pos: dict[str, Any]) -> str:
    return "🔒" if bool(pos.get("isolated")) else "⚖️"


def _dec(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _as_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
