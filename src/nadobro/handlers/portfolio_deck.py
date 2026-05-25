from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from src.nadobro.handlers.orders_view import order_kind_label
from src.nadobro.services.feature_flags import portfolio_sync_enabled, portfolio_sync_interval_seconds
from src.nadobro.services.nado_sync import sync_user
from src.nadobro.services.user_service import get_user
from src.nadobro.utils.visual import divider, money, pct, signed, stale_banner, time_ago


_VALID_WINDOWS = ("24h", "7d", "30d", "all")
_DEFAULT_WINDOW = "24h"


def _normalize_window(value: Any) -> str:
    token = str(value or "").strip().lower()
    if token in _VALID_WINDOWS:
        return token
    return _DEFAULT_WINDOW


def _window_label(window: str) -> str:
    return {"24h": "24h", "7d": "7d", "30d": "30d", "all": "All"}[window]


def portfolio_deck_kb(
    has_positions: bool = False,
    has_orders: bool = False,
    *,
    window: str = _DEFAULT_WINDOW,
) -> InlineKeyboardMarkup:
    window = _normalize_window(window)
    # Single shared 24h/7d/30d/All toggle — flips both Volume and PnL windows.
    toggle_row = [
        InlineKeyboardButton(
            ("• " + _window_label(w) + " •") if w == window else _window_label(w),
            callback_data=f"portfolio:view:{w}",
        )
        for w in _VALID_WINDOWS
    ]
    rows = [
        toggle_row,
        [
            InlineKeyboardButton("🚀 Positions", callback_data="portfolio:positions"),
            InlineKeyboardButton("📈 Performance & History", callback_data="portfolio:performance"),
        ],
        [InlineKeyboardButton("📊 Refresh portfolio", callback_data=f"portfolio:refresh:{window}")],
    ]
    if has_positions:
        rows.append([InlineKeyboardButton("❌ Close All", callback_data="portfolio:close_all_confirm")])
    rows.append([InlineKeyboardButton("🏠 Home", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)


async def snapshot_for_user(
    user_id: int,
    *,
    force: bool = False,
    max_age_ms: int | None = 2000,
) -> dict[str, Any]:
    """Return a portfolio snapshot, refreshing from Nado when stale.

    Every Portfolio render path passes ``max_age_ms=2000`` so Positions and
    Overview never show data older than ~2s when the user is actively
    navigating. Background polling keeps the cache warm between taps.
    """
    from src.nadobro.services.nado_sync import mark_user_active

    mark_user_active(int(user_id))
    user = get_user(user_id)
    network = user.network_mode.value if user else "mainnet"
    return await sync_user(
        user_id,
        network=network,
        reason="refresh" if force else "cold_render",
        force=force,
        max_age_ms=max_age_ms,
    )


def render_portfolio_deck(
    snapshot: dict[str, Any],
    *,
    window: str = _DEFAULT_WINDOW,
) -> tuple[str, InlineKeyboardMarkup]:
    window = _normalize_window(window)
    network = str(snapshot.get("network") or "mainnet").upper()
    positions = list(snapshot.get("positions") or [])
    orders = list(snapshot.get("open_orders") or [])
    stats = snapshot.get("stats") or {}
    equity = snapshot.get("equity") or {}
    last_sync = _as_dt(snapshot.get("last_sync"))
    threshold = (portfolio_sync_interval_seconds() * 2) if portfolio_sync_enabled() else 300
    stale = stale_banner(last_sync, threshold) if last_sync else "⚠ Stale · last sync never"

    total_upnl = sum((_dec(p.get("est_pnl")) for p in positions if p.get("est_pnl") is not None), Decimal("0"))
    total_balance = _dec(equity.get("total")) if equity else Decimal("0")
    spot_eq = _dec(equity.get("spot")) if equity else Decimal("0")
    cross_eq = _dec(equity.get("cross")) if equity else Decimal("0")
    iso_eq = _dec(equity.get("isolated")) if equity else Decimal("0")
    if not total_balance and (spot_eq or cross_eq or iso_eq):
        total_balance = spot_eq + cross_eq + iso_eq

    vol_window = _window_value(stats, "volume_windows", window)
    pnl_window = _window_value(stats, "pnl_windows", window)
    fees_window = _window_value(stats, "fees_windows", window)
    funding_window = _window_value(stats, "funding_windows", window)

    lines = []
    if stale or snapshot.get("stale"):
        lines.append(stale or "⚠ Stale · last sync unknown")
    lines.extend([
        f"📋 Portfolio · {network} · {_window_label(window)}",
        divider(),
        f"📡 Synced {time_ago(last_sync) if last_sync else 'never'}",
        f"💎 Total Balance {money(total_balance)}    🟢 uPnL {signed(total_upnl)}",
        f"   Spot {money(spot_eq)} · Cross {money(cross_eq)} · Isolated {money(iso_eq)}",
        f"🚀 Positions {len(positions)}    📋 Orders {len(orders)}",
        "",
        f"⚡ Volume ({_window_label(window)})",
        f"💰 Vol {money(vol_window)}",
        "",
        f"🏆 PnL ({_window_label(window)})",
        f"   Realized {signed(pnl_window)}",
        f"   Fees -{money(abs(fees_window))}",
        f"   Funding {signed(funding_window)} ({'paid' if funding_window > 0 else 'received'})",
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
        lines.append(
            f"{idx} ╱ {_order_symbol(order)} {side} {order_kind_label(order)} {money(_dec(order.get('price') or order.get('limit_price')))}"
        )
    if not orders:
        lines.append("No open orders.")
    return "\n".join(lines)[:3500], portfolio_deck_kb(
        bool(positions), bool(orders), window=window
    )


def _window_value(stats: dict[str, Any], key: str, window: str) -> Decimal:
    bucket = stats.get(key) or {}
    if isinstance(bucket, dict) and window in bucket:
        return _dec(bucket.get(window))
    # Backward compat: older snapshots only carried a single total.
    legacy_map = {
        "volume_windows": "total_volume",
        "pnl_windows": "total_pnl",
        "fees_windows": "total_fees",
        "funding_windows": "total_funding",
    }
    fallback_key = legacy_map.get(key)
    if fallback_key:
        return _dec(stats.get(fallback_key))
    return Decimal("0")


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


# --- Engine v2 portfolio (per-controller PnL) -----------------------------
async def fetch_engine_portfolio_state(user_id: int):
    """Engine v2 portfolio state, sourced through engine.portfolio (no
    handler-level PnL aggregation)."""
    from src.nadobro.services.portfolio_history_worker import build_db_portfolio

    return await build_db_portfolio().state(user_id)


def render_per_controller_pnl(state: Any) -> str:
    """Render the per-controller PnL section from engine.portfolio.state().

    Returns '' when there are no engine-managed controllers, so the existing
    deck is unchanged until the engine is driving trades.
    """
    per = getattr(state, "per_controller", {}) or {}
    if not per:
        return ""
    lines = ["", "*Strategy PnL (per controller)*"]
    for cid, pnl in per.items():
        lines.append(
            f"`{cid}` — net ${pnl.net:.2f} "
            f"(realized ${pnl.realized:.2f} · unrealized ${pnl.unrealized:.2f} · "
            f"fees ${pnl.fees:.2f}) · {pnl.open_executors} open"
        )
    return "\n".join(lines)
