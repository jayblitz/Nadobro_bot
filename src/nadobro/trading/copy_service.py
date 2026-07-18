"""
Nado-native Copy Trading Service.

Monitors copied trader addresses on Nado DEX and mirrors their positions
with user-defined parameters (margin, leverage, TP/SL, cumulative limits).
"""
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Optional

from src.nadobro.config import get_product_name, get_product_max_leverage
from src.nadobro.models.database import (
    upsert_copy_trader,
    get_copy_trader,
    get_copy_trader_by_wallet,
    get_active_copy_traders,
    get_curated_copy_traders,
    deactivate_copy_trader,
    clear_saved_copy_trader_selections,
    get_copy_mirror,
    stop_copy_mirror,
    request_copy_mirror_stop,
    pause_copy_mirror,
    resume_copy_mirror,
    count_user_active_mirrors,
    get_mirrors_for_trader,
    create_copy_mirror_v2,
    get_user_active_mirrors_v2,
    get_all_active_mirrors_v2,
    update_mirror_cumulative_pnl,
    auto_stop_mirror,
    insert_copy_position,
    get_open_copy_positions,
    get_open_copy_position_for_product,
    close_copy_position,
    reduce_copy_position,
    update_mirror_accounting,
    set_mirror_unrealized,
    set_mirror_session,
    insert_strategy_session,
    update_strategy_session,
    rollup_session_from_trades,
    rollup_engine_session_pnl_funding,
    save_copy_snapshot,
    get_latest_copy_snapshot,
    get_copy_trades_by_mirror,
)
from src.nadobro.users.user_service import get_user, get_user_nado_client
from src.nadobro.trading.trade_service import execute_market_order, execute_limit_order
from src.nadobro.venue.nado_client import NadoClient
from src.nadobro.venue.nado_archive import query_order_by_digest
from src.nadobro.core.async_utils import run_blocking
from src.nadobro.utils.env import env_bool, env_float

logger = logging.getLogger(__name__)

MAX_MIRRORS_PER_USER = 5
# Product floor: copy trading requires at least $100 margin per trade so the
# proportional mirror of a serious leader position clears venue minimums.
MIN_MARGIN_PER_TRADE = 100.0
MAX_MARGIN_PER_TRADE = 5000.0
POLL_INTERVAL_SECONDS = 30
# Partial-close mirroring: when the leader trims a position below the baseline
# we copied by more than this fraction, the same fraction of the copy is
# closed reduce-only. Kill switch: NADO_COPY_PARTIAL_CLOSES=false.
PARTIAL_CLOSES_ENABLED = env_bool("NADO_COPY_PARTIAL_CLOSES", True)
PARTIAL_CLOSE_MIN_DELTA_PCT = env_float("NADO_COPY_PARTIAL_MIN_DELTA_PCT", 5.0)
# Below this remaining fraction a partial becomes a full close (avoids dust).
PARTIAL_CLOSE_DUST_FRACTION = 0.05
_bot_app = None
_poll_task: Optional[asyncio.Task] = None


def set_copy_bot_app(app):
    global _bot_app
    _bot_app = app


async def _notify_user(telegram_id: int, text: str, reply_markup=None):
    if not _bot_app:
        return
    try:
        await _bot_app.bot.send_message(chat_id=telegram_id, text=text, reply_markup=reply_markup)
    except Exception as e:
        logger.warning("Copy notify failed for %s: %s", telegram_id, e)


# ─── Public API ────────────────────────────────────────────────

def add_trader(
    wallet_address: str,
    label: str = "",
    is_curated: bool = False,
    owner_user_id: int | None = None,
) -> tuple[bool, str, int | None]:
    """Add a copy-trade target.

    Curated entries (admin-only) get owner_user_id=None and are visible to
    everyone. Personal entries require an owner_user_id and remain private
    to that telegram user.
    """
    if not wallet_address or len(wallet_address) < 10:
        return False, "Invalid wallet address.", None
    wallet = wallet_address.strip()
    if not wallet.startswith("0x"):
        return False, "Wallet address must start with 0x.", None
    if not is_curated and owner_user_id is None:
        return False, "Owner required for personal trader.", None
    trader_id = upsert_copy_trader(
        wallet,
        label=label,
        is_curated=is_curated,
        owner_user_id=None if is_curated else owner_user_id,
    )
    if not trader_id:
        return False, "Failed to save trader.", None
    return True, f"Trader added: {label or wallet[:10]}...", trader_id


def remove_trader(trader_id: int, requester_user_id: int | None = None, is_admin: bool = False) -> tuple[bool, str]:
    """Remove a copy trader.

    A non-admin requester may only remove a trader they personally own
    (owner_user_id == requester). Curated entries are removable only by
    admins. This prevents one user from deactivating another user's
    private copy target.
    """
    trader = get_copy_trader(trader_id)
    if not trader:
        return False, "Trader not found."
    owner_id = trader.get("owner_user_id")
    if not is_admin:
        if owner_id is None:
            return False, "Curated traders are admin-managed."
        if requester_user_id is None or int(owner_id) != int(requester_user_id):
            return False, "Trader not found."
    mirrors = get_mirrors_for_trader(trader_id)
    pending = 0
    stopped = 0
    for m in mirrors:
        _closed, _pnl, _volume, errors = _flatten_mirror_positions(
            int(m["id"]),
            int(m["user_id"]),
            str(m.get("network", "mainnet")),
            reason="trader_removed",
        )
        if errors:
            # Never hide a trader/mirror while a copied position still needs a
            # close. ``stop_requested`` switches the poller to flatten-only.
            request_copy_mirror_stop(int(m["id"]))
            pending += 1
            continue
        stop_copy_mirror(m["id"])
        _finalize_mirror_session(m, "trader_removed")
        stopped += 1
    label = trader.get("label") or trader["wallet_address"][:10]
    if pending:
        return (
            False,
            f"Trader {label} remains active while {pending} mirror(s) retry failed position closes. "
            "Remove it again after they are flat.",
        )
    deactivate_copy_trader(trader_id)
    return True, f"Trader {label} removed. {stopped} mirror(s) stopped."


def clear_saved_copy_traders(telegram_id: int) -> tuple[int, int]:
    """Clear only dormant personal selections, never active copy exposure.

    This is intentionally separate from ``remove_trader``: removing a trader
    stops and flattens mirrors, while the user-facing clear button is a safe
    housekeeping action for abandoned selections.
    """
    return clear_saved_copy_trader_selections(int(telegram_id))


def start_copy(
    telegram_id: int,
    trader_id: int,
    network: str = "mainnet",
    margin_per_trade: float = 50.0,
    max_leverage: float = 10.0,
    cumulative_stop_loss_pct: float = 0.0,
    cumulative_take_profit_pct: float = 0.0,
    total_allocated_usd: float = 500.0,
) -> tuple[bool, str]:
    user = get_user(telegram_id)
    if not user:
        return False, "User not registered."
    if not user.linked_signer_address:
        return False, "Please link a wallet first."

    trader = get_copy_trader(trader_id)
    if not trader or not trader.get("active"):
        return False, "Trader not found or inactive."

    owner_id = trader.get("owner_user_id")
    if owner_id is not None and int(owner_id) != int(telegram_id):
        return False, "Trader not found or inactive."

    existing = get_user_active_mirrors_v2(telegram_id, network)
    if any(
        int(m.get("trader_id") or 0) == int(trader_id) and m.get("stop_requested")
        for m in existing
    ):
        return False, "Your earlier copy is still closing positions. Wait for it to finish before restarting."

    if margin_per_trade < MIN_MARGIN_PER_TRADE or margin_per_trade > MAX_MARGIN_PER_TRADE:
        return False, f"Margin per trade must be between ${MIN_MARGIN_PER_TRADE} and ${MAX_MARGIN_PER_TRADE}."

    count = count_user_active_mirrors(telegram_id)
    if count >= MAX_MIRRORS_PER_USER:
        return False, f"Maximum {MAX_MIRRORS_PER_USER} simultaneous copy traders allowed."

    mirror_id = create_copy_mirror_v2(
        user_id=telegram_id,
        trader_id=trader_id,
        network=network,
        margin_per_trade=margin_per_trade,
        max_leverage=max_leverage,
        cumulative_stop_loss_pct=cumulative_stop_loss_pct,
        cumulative_take_profit_pct=cumulative_take_profit_pct,
        total_allocated_usd=total_allocated_usd,
    )
    if not mirror_id:
        return False, "Trader is no longer available, or an earlier copy is still closing."

    label = trader.get("label") or trader["wallet_address"][:10]
    # COPY-SESSION: every mirror run is a strategy_sessions row (strategy=
    # 'copy') so Performance history, PnL cards, and the venue-authoritative
    # rollup (volume/fees/funding from trades tagged strategy_session_id)
    # work exactly like any other session. Best-effort: copying still starts
    # if the session insert fails — accounting then degrades to mirror rows.
    try:
        session_id = insert_strategy_session({
            "user_id": telegram_id,
            "strategy": "copy",
            "product_name": label,
            "network": network,
            "config_snapshot": json.dumps({
                "trader_id": trader_id,
                "wallet": trader["wallet_address"],
                "margin_per_trade": margin_per_trade,
                "max_leverage": max_leverage,
                "cumulative_stop_loss_pct": cumulative_stop_loss_pct,
                "cumulative_take_profit_pct": cumulative_take_profit_pct,
                "total_allocated_usd": total_allocated_usd,
            }),
        })
        if session_id:
            set_mirror_session(mirror_id, int(session_id))
    except Exception:  # noqa: BLE001 - session row is reporting infra, not the mirror
        logger.warning("copy session row create failed for mirror %s", mirror_id, exc_info=True)

    return True, (
        f"🔗 Now copying {label}\n"
        f"💰 Margin/Trade: ${margin_per_trade:.0f}\n"
        f"📊 Max Leverage: {max_leverage}x\n"
        f"🛑 Stop Loss: {cumulative_stop_loss_pct}% of ${total_allocated_usd:.0f}\n"
        f"🎯 Take Profit: {cumulative_take_profit_pct}% of ${total_allocated_usd:.0f}"
    )


def _finalize_mirror_session(mirror: dict, stop_reason: str) -> None:
    """Finalize the mirror's strategy_sessions row: venue-authoritative rollup
    (volume/fees from x18-enriched trades tagged with the session id, funding
    window-summed like engine sessions), then mark it stopped. Best-effort —
    a rollup failure never blocks the stop itself."""
    session_id = mirror.get("strategy_session_id")
    if not session_id:
        return
    network = str(mirror.get("network", "mainnet"))
    try:
        rollup_session_from_trades(int(session_id), network)
        rollup_engine_session_pnl_funding(int(session_id), network)
        update_strategy_session(int(session_id), {
            "status": "stopped",
            "stopped_at": datetime.utcnow().isoformat(),
            "stop_reason": stop_reason,
        })
    except Exception:  # noqa: BLE001 - reporting must not block the stop
        logger.warning("copy session finalize failed for session %s", session_id, exc_info=True)


def stop_copy(telegram_id: int, mirror_id: int) -> tuple[bool, str]:
    mirror = get_copy_mirror(mirror_id)
    if not mirror:
        return False, "Mirror not found."
    if mirror["user_id"] != telegram_id:
        return False, "Not your mirror."
    if not mirror.get("active"):
        return False, "Mirror already stopped."
    closed_count, _, _, errors = _flatten_mirror_positions(mirror_id, telegram_id, str(mirror.get("network", "mainnet")), reason="user_stop")
    if errors:
        # Do not hide a failed close by marking this mirror inactive. The poller
        # keeps a stop-requested mirror alive solely to retry its flatten; it
        # will not mirror new leader opens while that intent is set.
        request_copy_mirror_stop(mirror_id)
        return False, (
            "Copy stop is pending because some copied positions could not be closed. "
            "The mirror remains monitored and will retry the close automatically. "
            + "; ".join(errors[:2])
        )
    stop_copy_mirror(mirror_id)
    _finalize_mirror_session(mirror, "user_stop")
    suffix = f" Flattened {closed_count} copied position(s)." if closed_count > 0 else ""
    return True, f"Copy trading stopped for this trader.{suffix}"


def pause_copy(telegram_id: int, mirror_id: int) -> tuple[bool, str]:
    mirror = get_copy_mirror(mirror_id)
    if not mirror:
        return False, "Mirror not found."
    if mirror["user_id"] != telegram_id:
        return False, "Not your mirror."
    if not mirror.get("active"):
        return False, "Mirror is stopped."
    if mirror.get("stop_requested"):
        return False, "Copy trading is stopping while existing positions are closed."
    if mirror.get("paused"):
        return False, "Mirror is already paused."
    pause_copy_mirror(mirror_id)
    return True, "Copy trading paused. New trades will not be mirrored until resumed."


def resume_copy(telegram_id: int, mirror_id: int) -> tuple[bool, str]:
    mirror = get_copy_mirror(mirror_id)
    if not mirror:
        return False, "Mirror not found."
    if mirror["user_id"] != telegram_id:
        return False, "Not your mirror."
    if not mirror.get("active"):
        return False, "Mirror is stopped."
    if mirror.get("stop_requested"):
        return False, "Copy trading is stopping while existing positions are closed."
    if not mirror.get("paused"):
        return False, "Mirror is not paused."
    resume_copy_mirror(mirror_id)
    return True, "Copy trading resumed."


def stop_all_copies(telegram_id: int) -> tuple[bool, str]:
    mirrors = get_user_active_mirrors_v2(telegram_id)
    if not mirrors:
        return False, "No active copy mirrors."
    total_closed = 0
    stopped = 0
    pending = 0
    for m in mirrors:
        closed_count, _, _, errors = _flatten_mirror_positions(int(m["id"]), telegram_id, str(m.get("network", "mainnet")), reason="user_stop_all")
        total_closed += closed_count
        if errors:
            request_copy_mirror_stop(int(m["id"]))
            pending += 1
            continue
        stop_copy_mirror(m["id"])
        _finalize_mirror_session(m, "user_stop_all")
        stopped += 1
    suffix = f" Flattened {total_closed} copied position(s)." if total_closed > 0 else ""
    if pending:
        suffix += f" {pending} mirror(s) remain monitored while failed closes retry."
        # The stop request is retry-safe, but it is not complete. Propagate a
        # non-success result so /stop_all cannot render an all-clear while any
        # copied exposure is still waiting to close.
        return False, f"Stopped {stopped} copy mirror(s).{suffix}"
    return True, f"Stopped {stopped} copy mirror(s).{suffix}"


def get_user_copies(telegram_id: int, network: str = None) -> list[dict]:
    mirrors = get_user_active_mirrors_v2(telegram_id, network=network)
    result = []
    for m in mirrors:
        open_positions = get_open_copy_positions(m["id"])
        wallet = m.get("wallet_address", "")
        wallet_snip = f"{wallet[:6]}...{wallet[-4:]}" if len(wallet) >= 10 else wallet
        result.append({
            "mirror_id": m["id"],
            "trader_label": m.get("label") or wallet_snip,
            "wallet": wallet,
            "budget_usd": m.get("total_allocated_usd", 500.0),
            "margin_per_trade": m.get("margin_per_trade", 50.0),
            "max_leverage": m.get("max_leverage", 10.0),
            "cumulative_stop_loss_pct": m.get("cumulative_stop_loss_pct", 50.0),
            "cumulative_take_profit_pct": m.get("cumulative_take_profit_pct", 100.0),
            "total_allocated_usd": m.get("total_allocated_usd", 500.0),
            "cumulative_pnl": float(m.get("cumulative_pnl", 0)),
            "cumulative_fees_usd": float(m.get("cumulative_fees_usd") or 0.0),
            "cumulative_volume_usd": float(m.get("cumulative_volume_usd") or 0.0),
            "unrealized_pnl": float(m.get("last_unrealized_pnl_usd") or 0.0),
            # Rail-consistent net: realized (gross, derived) + last unrealized
            # snapshot - fees. This is exactly the number the SL/TP rail judges.
            "net_pnl": (
                float(m.get("cumulative_pnl", 0))
                + float(m.get("last_unrealized_pnl_usd") or 0.0)
                - float(m.get("cumulative_fees_usd") or 0.0)
            ),
            "strategy_session_id": m.get("strategy_session_id"),
            "open_positions": len(open_positions),
            "paused": bool(m.get("paused")),
            "stop_requested": bool(m.get("stop_requested")),
            "network": m.get("network", "mainnet"),
            "created_at": m.get("created_at"),
        })
    return result


def get_user_mirrors(telegram_id: int) -> list[dict]:
    return get_user_copies(telegram_id)


def _flatten_mirror_positions(mirror_id: int, user_id: int, network: str, reason: str = "user_stop") -> tuple[int, float, float, list[str]]:
    mirror = get_copy_mirror(mirror_id) or {}
    session_id = mirror.get("strategy_session_id")
    open_positions = get_open_copy_positions(mirror_id) or []
    closed = 0
    total_pnl = 0.0
    total_notional = 0.0
    errors: list[str] = []
    for cp in open_positions:
        product_name = cp.get("product_name", "")
        product_key = product_name.replace("-PERP", "")
        size = float(cp.get("size", 0) or 0.0)
        if size <= 0:
            continue
        try:
            result = execute_market_order(
                telegram_id=user_id,
                product=product_key,
                size=size,
                is_long=cp["side"].upper() != "LONG",
                leverage=float(cp.get("leverage", 1.0) or 1.0),
                slippage_pct=1.5,
                enforce_rate_limit=False,
                reduce_only=True,
                source="copy",
                network=network,
                strategy_session_id=session_id,
            )
            if not result.get("success"):
                errors.append(f"{product_name}: {result.get('error', 'close failed')}")
                continue
            pnl, _fee, close_price = _settle_copy_close(mirror_id, cp, result, network, size)
            close_copy_position(cp["id"], pnl=pnl, reason=reason)
            closed += 1
            total_pnl += pnl
            total_notional += size * float(close_price or cp.get("entry_price") or 0.0)
        except Exception as e:
            errors.append(f"{product_name}: {e}")
    return closed, total_pnl, total_notional, errors


def get_available_traders(user_id: int | None = None) -> list[dict]:
    """Traders visible to `user_id`: curated entries + their own privates.

    Callers MUST pass the requesting user's telegram_id. The old signature
    (no user_id) returned the full table and leaked custom wallets between
    users; we now treat the public path as curated-only and require an
    explicit caller for the personal hub.
    """
    traders = get_active_copy_traders(user_id=user_id)
    return [
        {
            "id": t["id"],
            "wallet": t["wallet_address"],
            "label": t.get("label") or t["wallet_address"][:10],
            "is_curated": t.get("is_curated", False),
            "owner_user_id": t.get("owner_user_id"),
            # Snapshot of public leader performance at selection time. It is
            # deliberately separate from get_trader_stats(), which is the
            # viewer's own copy result and must stay private.
            "leader_pnl_usd": t.get("total_pnl_usd"),
            "leader_volume_usd": t.get("total_volume_usd"),
            "leader_win_rate": t.get("win_rate"),
            "leader_roi": t.get("leader_roi"),
            "leader_active_days": t.get("leader_active_days"),
            "leader_period_days": t.get("leader_period_days"),
            "leader_last_activity_at": t.get("leader_last_activity_at"),
            "leader_closed_trades": t.get("leader_closed_trades"),
            "leader_max_drawdown_pct": t.get("leader_max_drawdown_pct"),
        }
        for t in traders
    ]


def get_trader_stats(trader_id: int, user_id: int | None = None) -> dict:
    """Aggregate display metrics for a copy trader across active mirrors.

    ``user_id`` scopes the aggregation to THAT user's mirrors — the hub and
    preview must show each viewer their own results with a trader, never a
    pool of every follower's PnL (curated traders are shared; results are
    not). ``None`` keeps the global aggregate for admin/ops callers.
    """
    mirrors = get_mirrors_for_trader(trader_id) or []
    if user_id is not None:
        mirrors = [m for m in mirrors if int(m.get("user_id") or 0) == int(user_id)]
    stats = {
        "pnl_usd": 0.0,
        "volume_usd": 0.0,
        "win_rate": 0.0,
        "total_trades": 0,
        "filled": 0,
        "failed": 0,
    }
    if not mirrors:
        return stats

    # v2 accounting: the legacy copy_trades table (Hyperliquid-era) is never
    # written by this path, so aggregate from what the mirrors actually track —
    # derived realized PnL net of fees, execution volume, and closed copy
    # positions (win = positive derived PnL on the closed row).
    from src.nadobro.db import query_all as _query_all

    for mirror in mirrors:
        mirror_id = int(mirror.get("id") or 0)
        if mirror_id <= 0:
            continue
        stats["pnl_usd"] += (
            float(mirror.get("cumulative_pnl") or 0.0)
            - float(mirror.get("cumulative_fees_usd") or 0.0)
        )
        stats["volume_usd"] += float(mirror.get("cumulative_volume_usd") or 0.0)
        try:
            rows = _query_all(
                """SELECT pnl FROM copy_positions
                   WHERE mirror_id = %s AND status = 'closed'""",
                (mirror_id,),
            ) or []
        except Exception:  # policy: degrade-ok(stats query; dashboard tolerates undercount)
            rows = []
        for r in rows:
            stats["total_trades"] += 1
            stats["filled"] += 1
            if float(r.get("pnl") or 0.0) > 0:
                stats["wins"] = stats.get("wins", 0) + 1

    if stats["total_trades"] > 0:
        stats["win_rate"] = (stats.get("wins", 0) / stats["total_trades"]) * 100.0
    return stats


def get_trader_preview(trader_id: int, network: str = "mainnet", requester_user_id: int | None = None) -> dict:
    trader = get_copy_trader(trader_id)
    if not trader:
        return {"found": False}
    owner_id = trader.get("owner_user_id")
    if owner_id is not None and (requester_user_id is None or int(owner_id) != int(requester_user_id)):
        return {"found": False}
    wallet = str(trader.get("wallet_address") or "")
    # NO_ORDERS_AUDIT-FIX-R6b: cached.
    from src.nadobro.venue.nado_client import get_or_create_readonly_client
    client = get_or_create_readonly_client(wallet, network)
    positions = client.get_all_positions() or []
    balance = client.get_balance() or {}
    balances = balance.get("balances", {}) or {}
    usdt_balance = float(balances.get(0, balances.get("0", 0.0)) or 0.0)
    total_notional = 0.0
    for pos in positions:
        try:
            size = abs(float(pos.get("amount", 0) or 0.0))
            entry = float(
                pos.get("entry_price") or pos.get("avg_entry_price")
                or pos.get("price") or 0.0
            )
            total_notional += size * entry
        except Exception:  # policy: degrade-ok(malformed position row; preview tolerates undercount)
            continue
    return {
        "found": True,
        "wallet": wallet,
        "label": trader.get("label") or wallet[:10],
        "equity_usd": usdt_balance,
        "open_positions": len([p for p in positions if abs(float(p.get("amount", 0) or 0.0)) > 0]),
        "gross_notional_usd": total_notional,
        "network": network,
    }


# ─── Position Monitoring Loop ─────────────────────────────────

async def start_copy_polling():
    """Start the background polling loop for copy trading."""
    global _poll_task
    if _poll_task and not _poll_task.done():
        return
    _poll_task = asyncio.create_task(_poll_loop(), name="nadobro-copy-poll")
    logger.info("Copy trading polling loop started (interval=%ds)", POLL_INTERVAL_SECONDS)


async def stop_copy_polling():
    global _poll_task
    task = _poll_task
    _poll_task = None
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        logger.info("Copy trading polling loop stopped")


def get_copy_polling_diagnostics() -> dict:
    task = _poll_task
    return {
        "enabled": bool(task),
        "running": bool(task and not task.done()),
        "interval_seconds": int(POLL_INTERVAL_SECONDS),
        "task_name": str(task.get_name()) if task else "",
    }


async def boot_stand_down_mirrors() -> int:
    """Redeploy rule: boot NEVER auto-resumes trading — pause every active,
    unpaused mirror so nothing mirrors until its owner taps Resume.

    Paused mirrors are excluded from polling entirely, which also means any
    still-open copied positions are unmonitored (no leader-close mirroring,
    no rail) until resumed — the notification says so explicitly and carries
    the same Resume/Stop controls as the copy dashboard. Returns the number
    of mirrors paused.
    """
    mirrors = await run_blocking(get_all_active_mirrors_v2)
    # A failed user stop is intentionally included in the poller even if it
    # used to be paused. It must never be paused again on redeploy: its
    # flatten-only retry path is the remaining safety mechanism.
    mirrors = [m for m in mirrors if not m.get("stop_requested")]
    if not mirrors:
        return 0

    by_user: dict[int, list[dict]] = {}
    for m in mirrors:
        by_user.setdefault(int(m["user_id"]), []).append(m)

    paused = 0
    for user_id, user_mirrors in by_user.items():
        for m in user_mirrors:
            try:
                await run_blocking(pause_copy_mirror, int(m["id"]))
                paused += 1
            except Exception:  # noqa: BLE001 - one bad row must not block the boot
                logger.warning("boot stand-down: pause failed for mirror %s", m.get("id"), exc_info=True)
        try:
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup

            lines = [
                "⏸ Copy trading paused (bot redeploy)",
                "Redeploys never auto-resume trading. Your copy mirror(s) are "
                "paused until you resume them:",
            ]
            rows = []
            for m in user_mirrors:
                wallet = str(m.get("wallet_address") or "")
                label = m.get("label") or (f"{wallet[:6]}...{wallet[-4:]}" if len(wallet) >= 10 else wallet)
                open_rows = await run_blocking(get_open_copy_positions, int(m["id"])) or []
                lines.append(
                    f"• {label} ({str(m.get('network', 'mainnet')).upper()})"
                    + (f" — ⚠️ {len(open_rows)} open position(s) NOT monitored while paused" if open_rows else "")
                )
                rows.append([
                    InlineKeyboardButton(f"▶ Resume {label}"[:60], callback_data=f"copy:resume:{m['id']}"),
                    InlineKeyboardButton("🛑 Stop", callback_data=f"copy:stop:{m['id']}"),
                ])
            await _notify_user(user_id, "\n".join(lines), reply_markup=InlineKeyboardMarkup(rows))
        except Exception:  # noqa: BLE001 - notify is best-effort; the stand-down already happened
            logger.warning("boot stand-down: notify failed for user %s", user_id, exc_info=True)
    return paused


async def _poll_loop():
    """Main polling loop: check all active mirrors and sync positions."""
    while True:
        try:
            await _poll_all_mirrors()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Copy polling error: %s", e, exc_info=True)
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def _poll_all_mirrors():
    """Poll all active mirrors and process position changes."""
    mirrors = await run_blocking(get_all_active_mirrors_v2)
    if not mirrors:
        return

    # Group mirrors by trader+network for efficient polling
    trader_groups: dict[str, list[dict]] = {}
    for m in mirrors:
        key = f"{m['trader_id']}:{m.get('network', 'mainnet')}"
        trader_groups.setdefault(key, []).append(m)

    for group_key, group_mirrors in trader_groups.items():
        trader_id = group_mirrors[0]["trader_id"]
        network = group_mirrors[0].get("network", "mainnet")
        wallet = group_mirrors[0].get("wallet_address", "")

        # A requested stop does not need the leader's current portfolio. Run
        # its flatten-only retry first so a leader-read outage cannot strand
        # the follower's open exposure.
        stopping_mirrors = [m for m in group_mirrors if m.get("stop_requested")]
        for mirror in stopping_mirrors:
            try:
                await _sync_mirror_positions(mirror, {})
            except Exception as e:
                logger.error(
                    "Copy stop retry failed for mirror %s user %s: %s",
                    mirror["id"], mirror["user_id"], e, exc_info=True,
                )

        copying_mirrors = [m for m in group_mirrors if not m.get("stop_requested")]
        if not copying_mirrors:
            continue

        try:
            leader_pos_map = await run_blocking(_load_leader_position_map, trader_id, wallet, network)

            # Process each mirror
            for mirror in copying_mirrors:
                try:
                    await _sync_mirror_positions(mirror, leader_pos_map)
                except Exception as e:
                    logger.error(
                        "Copy sync failed for mirror %s user %s: %s",
                        mirror["id"], mirror["user_id"], e, exc_info=True,
                    )

        except Exception as e:
            logger.error("Failed to poll trader %s on %s: %s", wallet[:10], network, e)


def _load_leader_position_map(trader_id: int, wallet: str, network: str) -> dict:
    # NO_ORDERS_AUDIT-FIX-R6b: cached.
    from src.nadobro.venue.nado_client import get_or_create_readonly_client
    leader_client = get_or_create_readonly_client(wallet, network)
    leader_positions = leader_client.get_all_positions() or []
    leader_pos_map = {}
    for pos in leader_positions:
        pid = int(pos.get("product_id", -1))
        amount = float(pos.get("amount", 0) or 0)
        if abs(amount) <= 0:
            continue
        side = pos.get("side", "").upper()
        if not side:
            side = "LONG" if amount > 0 else "SHORT"
        leader_pos_map[pid] = {
            "product_id": pid,
            "side": side,
            "size": abs(amount),
            # The venue client publishes the average entry under "price"
            # ("entry_price" is a newer alias) — read both. Every open is
            # gated on entry > 0, so missing this key silently killed all
            # copy opens.
            "entry_price": float(pos.get("entry_price") or pos.get("price") or 0),
            "unrealized_pnl": float(pos.get("unrealized_pnl", 0) or 0),
            # COPY-LEVERAGE fix: capture the leader's leverage so the follower can
            # mirror the leader's risk profile instead of always using its own
            # max. 0.0 when the venue doesn't report it (we then fall back).
            "leverage": float(pos.get("leverage", 0) or 0),
        }
        try:
            # Venue open orders currently expose no order type (rows carry
            # digest/amount/price/side only) and trigger orders don't appear
            # in the book at all, so leader TP/SL is undetectable today —
            # this scan engages only if the venue starts publishing a type.
            # Followers stay protected by close/partial mirroring + the
            # cumulative rail regardless.
            orders = leader_client.get_open_orders(pid) or []
            tp_price = None
            sl_price = None
            for o in orders:
                otype = (o.get("order_type") or o.get("type") or "").lower()
                if "take_profit" in otype or "tp" in otype:
                    tp_price = float(o.get("price", 0) or 0)
                elif "stop_loss" in otype or "sl" in otype:
                    sl_price = float(o.get("price", 0) or 0)
            leader_pos_map[pid]["tp_price"] = tp_price
            leader_pos_map[pid]["sl_price"] = sl_price
        except Exception as e:
            logger.debug("Failed to fetch orders for product %s: %s", pid, e)
    try:
        save_copy_snapshot(trader_id, network, json.dumps(list(leader_pos_map.values())))
    except Exception as e:
        logger.warning(
            "copy snapshot not persisted for trader %s (%s) — dashboard may show stale leader state: %s",
            trader_id, network, e,
        )
    return leader_pos_map


def _compute_copy_sizing(
    *,
    leader_size: float,
    leader_entry: float,
    leader_leverage: float,
    leader_max_notional: float,
    margin_per_trade: float,
    max_leverage: float,
    product_max_leverage: float,
) -> tuple[float, float]:
    """Return ``(copy_size_base, leverage)`` for a mirrored position.

    COPY-SIZE fix: the old formula was ``margin_per_trade * leverage / entry`` —
    a FIXED notional that ignored the leader's actual size, so a leader's tiny
    probe and their max-conviction position were copied identically. We now scale
    the committed margin by this position's conviction = its notional as a
    fraction of the leader's LARGEST open position (their biggest bet = full
    ``margin_per_trade``; a 10%-size probe = 10% of it), capped at
    ``margin_per_trade``.

    COPY-LEVERAGE fix: leverage mirrors the leader's, capped by the user's max
    and the product max; falls back to ``min(max, product_max)`` when the venue
    doesn't report the leader's leverage.
    """
    if leader_entry <= 0 or leader_size <= 0:
        return 0.0, 0.0
    lev = min(float(max_leverage), float(product_max_leverage))
    if leader_leverage and leader_leverage > 0:
        lev = min(float(leader_leverage), float(max_leverage), float(product_max_leverage))
    lev = max(1.0, lev)
    leader_notional = leader_size * leader_entry
    if leader_max_notional and leader_max_notional > 0:
        weight = min(1.0, leader_notional / leader_max_notional)
    else:
        weight = 1.0
    copy_margin = max(0.0, float(margin_per_trade) * weight)
    copy_size = (copy_margin * lev) / leader_entry
    return copy_size, lev


def _entry_deviation_too_far(mid: float, leader_entry: float, max_dev_pct: float) -> bool:
    """COPY-NO-SLIPPAGE gate: True when the follower would enter too far from the
    leader's entry. With a 30s poll, price can move a lot between the leader's
    fill and ours; opening anyway means systematically buying higher / selling
    lower than the trader we copy. We skip (and retry next poll) when the current
    mid deviates from ``leader_entry`` by more than ``max_dev_pct`` percent.
    Unknown/zero inputs => allow (can't assess, don't block)."""
    try:
        mid = float(mid)
        leader_entry = float(leader_entry)
        max_dev_pct = float(max_dev_pct)
    except (TypeError, ValueError):
        return False
    if mid <= 0 or leader_entry <= 0 or max_dev_pct <= 0:
        return False
    return abs(mid - leader_entry) / leader_entry * 100.0 > max_dev_pct


def _leader_max_notional(leader_pos_map: dict) -> float:
    """Largest single-position notional in the leader's book (the conviction
    anchor for proportional copy sizing)."""
    best = 0.0
    for p in (leader_pos_map or {}).values():
        try:
            n = float(p.get("size", 0) or 0) * float(p.get("entry_price", 0) or 0)
        except (TypeError, ValueError):
            n = 0.0
        if n > best:
            best = n
    return best


async def _sync_mirror_positions(mirror: dict, leader_pos_map: dict):
    """Sync a single mirror's positions with the leader's current positions."""
    mirror_id = mirror["id"]
    user_id = mirror["user_id"]
    network = mirror.get("network", "mainnet")
    margin_per_trade = float(mirror.get("margin_per_trade", 50.0))
    max_leverage = float(mirror.get("max_leverage", 10.0))
    cumulative_stop_loss_pct = float(mirror.get("cumulative_stop_loss_pct", 50.0))
    cumulative_take_profit_pct = float(mirror.get("cumulative_take_profit_pct", 100.0))
    total_allocated = float(mirror.get("total_allocated_usd", 500.0))
    cumulative_pnl = float(mirror.get("cumulative_pnl", 0.0))

    if mirror.get("stop_requested"):
        closed_count, total_pnl, _, errors = await run_blocking(
            _flatten_mirror_positions,
            mirror_id,
            user_id,
            str(network),
            "user_stop",
        )
        if errors:
            logger.error(
                "copy stop retry incomplete for mirror %s (%s)",
                mirror_id,
                "; ".join(errors[:3]),
            )
            return
        await run_blocking(stop_copy_mirror, mirror_id)
        await run_blocking(_finalize_mirror_session, mirror, "user_stop")
        await _notify_user(
            user_id,
            "✅ Copy trading stopped after closing "
            f"{closed_count} copied position(s) for ${total_pnl:+,.2f}.",
        )
        return

    session_id = mirror.get("strategy_session_id")

    # Load tracked rows and the follower's REAL on-venue positions FIRST — the
    # bracket-fill sweep and the rail's unrealized PnL both need them (the old
    # code evaluated the rail before any live data was loaded).
    open_copy_positions = await run_blocking(get_open_copy_positions, mirror_id)
    copy_pos_by_product = {}
    for cp in open_copy_positions:
        copy_pos_by_product[cp["product_id"]] = cp

    # COPY-VENUE-RECONCILE: best-effort — if the client/read is unavailable we
    # degrade to DB-only behavior (no sweep, realized-only rail) rather than
    # block copying.
    max_entry_deviation_pct = float(mirror.get("max_entry_deviation_pct", 1.5) or 1.5)
    venue_pos_by_product: dict = {}
    venue_read_ok = False
    try:
        # A mirror is explicitly network-scoped. A user changing the app's
        # current network must not disable another mirror; use the mirror's
        # persisted network for its read/reconcile client as well as orders.
        follower_client = await run_blocking(get_user_nado_client, user_id, network=network)
    except Exception as e:  # noqa: BLE001 - reconcile is best-effort; degrade to DB-only
        logger.debug("copy follower client unavailable for reconcile: %s", e)
        follower_client = None
    if follower_client is not None:
        try:
            for vp in (await run_blocking(follower_client.get_all_positions) or []):
                try:
                    if abs(float(vp.get("amount", 0) or 0)) > 0:
                        venue_pos_by_product[int(vp.get("product_id", -1))] = vp
                except (TypeError, ValueError):
                    continue
            venue_read_ok = True
        except Exception as e:  # noqa: BLE001 - reconcile is best-effort
            logger.debug("copy venue-position reconcile read failed: %s", e)

    # COPY-BRACKET-SWEEP: book venue-side closes nobody reported — a TP/SL
    # bracket that FILLED on the venue, a manual close, or a liquidation.
    # Without this, a filled stop-loss left a phantom open row, zero booked
    # PnL, and a rail that never saw the loss.
    if venue_read_ok:
        swept_pnl, swept_fees = await _sweep_external_closes(
            mirror, copy_pos_by_product, venue_pos_by_product, follower_client
        )
        cumulative_pnl += swept_pnl

    # Unrealized PnL of the surviving open copy rows — venue uPnL scaled to
    # the copy's share of the venue position, mid-price fallback.
    upnl = await _compute_open_upnl(
        copy_pos_by_product, venue_pos_by_product, follower_client
    )
    try:
        await run_blocking(set_mirror_unrealized, mirror_id, upnl)
    except Exception:  # noqa: BLE001 - display cache only
        logger.debug("copy set_mirror_unrealized failed", exc_info=True)

    # COPY-RAIL: judged like every session rail in this product — live PnL
    # INCLUDING unrealized, NET of fees, as % of the allocated amount.
    cumulative_fees = float(mirror.get("cumulative_fees_usd") or 0.0)
    net_pnl = cumulative_pnl + upnl - cumulative_fees
    if total_allocated > 0:
        pnl_pct = (net_pnl / total_allocated) * 100
        reason = _rail_decision(
            net_pnl, total_allocated, cumulative_stop_loss_pct, cumulative_take_profit_pct
        )
        if reason is not None:
            if reason == "auto_stop_loss":
                title = "🛑 Copy Trading Auto-Stopped"
                detail = f"Cumulative loss hit {abs(pnl_pct):.1f}% (limit: {cumulative_stop_loss_pct}%)"
            else:
                title = "🎯 Copy Trading Auto-Stopped — Target Hit!"
                detail = f"Cumulative profit hit {pnl_pct:.1f}% (target: {cumulative_take_profit_pct}%)"
            closed_count, total_pnl, _, errors = await run_blocking(
                _flatten_mirror_positions, mirror_id, user_id, str(network), reason,
            )
            if errors:
                # A rail fired but the flatten is incomplete: keep the mirror
                # ACTIVE so the next poll retries the flatten (stopping now
                # would orphan live positions with no monitor).
                logger.error(
                    "copy rail %s flatten incomplete for mirror %s (%s) — retrying next poll",
                    reason, mirror_id, "; ".join(errors[:3]),
                )
            else:
                await run_blocking(auto_stop_mirror, mirror_id, detail)
                await run_blocking(_finalize_mirror_session, mirror, reason)
            await _notify_user(
                user_id,
                (
                    f"{title}\n{detail}\n"
                    f"Net P&L: ${net_pnl:+,.2f} (realized ${cumulative_pnl:+,.2f}, "
                    f"unrealized ${upnl:+,.2f}, fees ${cumulative_fees:,.2f}) "
                    f"/ ${total_allocated:,.0f} allocated\n"
                    + (f"Closed {closed_count} copied position(s) for ${total_pnl:+,.2f}.\n" if closed_count > 0 else "")
                    + (f"⚠️ Some closes failed and will be retried: {'; '.join(errors[:2])}" if errors else "")
                )
            )
            return

    # 1. Close positions that leader has closed
    for pid, cp in list(copy_pos_by_product.items()):
        if pid not in leader_pos_map:
            # Leader closed this position — close ours too
            product_name = cp.get("product_name") or get_product_name(pid, network=network)
            product_key = product_name.replace("-PERP", "")
            try:
                # Close by opening opposite side
                is_close_long = cp["side"].upper() != "LONG"
                result = await run_blocking(
                    execute_market_order,
                    telegram_id=user_id,
                    product=product_key,
                    size=float(cp.get("size", 0)),
                    is_long=is_close_long,
                    leverage=float(cp.get("leverage", 1.0)),
                    slippage_pct=1.5,
                    enforce_rate_limit=False,
                    reduce_only=True,
                    source="copy",
                    network=network,
                    strategy_session_id=session_id,
                )
                if not result.get("success"):
                    logger.warning("Failed to close copy position %s: %s", cp["id"], result.get("error", "close failed"))
                    continue
                pnl, fee, _price = await run_blocking(
                    _settle_copy_close, mirror_id, cp, result, network, float(cp.get("size", 0))
                )
                await run_blocking(close_copy_position, cp["id"], pnl=pnl, reason="leader_closed")
                await _notify_user(
                    user_id,
                    f"📋 Copy Position Closed\n"
                    f"{product_name}: Leader closed → Your position closed\n"
                    f"P&L: ${pnl:+,.2f}" + (f" (fees ${fee:,.2f})" if fee else "")
                )
            except Exception as e:
                logger.error("Failed to close copy position %s: %s", cp["id"], e)

    # 2. Open new positions that leader has opened
    for pid, leader_pos in leader_pos_map.items():
        if pid in copy_pos_by_product:
            # Already tracking this position — update TP/SL if changed
            existing = copy_pos_by_product[pid]
            if existing["side"].upper() != leader_pos["side"].upper():
                product_name = existing.get("product_name") or get_product_name(pid, network=network)
                product_key = product_name.replace("-PERP", "")
                try:
                    close_res = await run_blocking(
                        execute_market_order,
                        telegram_id=user_id,
                        product=product_key,
                        size=float(existing.get("size", 0)),
                        is_long=existing["side"].upper() != "LONG",
                        leverage=float(existing.get("leverage", 1.0)),
                        slippage_pct=1.5,
                        enforce_rate_limit=False,
                        reduce_only=True,
                        source="copy",
                        network=network,
                        strategy_session_id=session_id,
                    )
                    if not close_res.get("success"):
                        logger.error("Failed to flip copy position %s: %s", existing["id"], close_res.get("error", "close failed"))
                        continue
                    pnl, _fee, _price = await run_blocking(
                        _settle_copy_close, mirror_id, existing, close_res, network,
                        float(existing.get("size", 0)),
                    )
                    await run_blocking(close_copy_position, existing["id"], pnl=pnl, reason="leader_flipped_side")
                except Exception as e:
                    logger.error("Failed to flip copy position %s: %s", existing["id"], e)
                    continue
                copy_pos_by_product.pop(pid, None)
            else:
                resized = await _mirror_partial_close_if_needed(
                    existing, leader_pos, user_id, mirror_id, network,
                    session_id=session_id,
                )
                if resized == "closed":
                    copy_pos_by_product.pop(pid, None)
                    continue
                await run_blocking(
                    _update_tp_sl_if_changed, existing, leader_pos, user_id, network,
                    force=(resized == "reduced"), session_id=session_id,
                )
                continue

        # New position from leader — open a copy
        product_name = get_product_name(pid, network=network)
        product_key = product_name.replace("-PERP", "")
        is_long = leader_pos["side"] == "LONG"
        leader_entry = leader_pos.get("entry_price", 0)

        if leader_entry <= 0:
            continue

        # COPY-VENUE-RECONCILE: the follower already holds this product on-venue
        # but we have no DB record of it — opening again would stack a duplicate /
        # orphaned position outside cumulative accounting. Skip; a manual/orphan
        # position must be reconciled before we copy it.
        if pid in venue_pos_by_product:
            logger.warning(
                "copy skip open: follower already holds product %s on-venue with no "
                "tracked copy row (mirror %s) — reconcile before copying",
                pid, mirror_id,
            )
            continue

        # COPY-NO-SLIPPAGE: skip a late entry that's drifted too far from the
        # leader's fill (we'd buy higher / sell lower than the trader we copy).
        if follower_client is not None:
            try:
                _mp = await run_blocking(follower_client.get_market_price, pid)
                _mid = float((_mp or {}).get("mid") or 0.0)
            except Exception:  # noqa: BLE001 - price read best-effort; allow if unknown
                _mid = 0.0
            if _entry_deviation_too_far(_mid, float(leader_entry or 0), max_entry_deviation_pct):
                logger.info(
                    "copy skip open: %s mid %.6f deviates >%.2f%% from leader entry %.6f "
                    "(mirror %s) — waiting for a closer price",
                    product_key, _mid, max_entry_deviation_pct, float(leader_entry), mirror_id,
                )
                continue

        # COPY-SIZE + COPY-LEVERAGE fix: mirror the leader's conviction (size as a
        # fraction of their largest position) and leverage, capped by the user's
        # per-trade budget and max leverage — not a fixed max-leverage notional.
        product_max_lev = get_product_max_leverage(product_key, network=network)
        copy_size, leverage = _compute_copy_sizing(
            leader_size=float(leader_pos.get("size", 0) or 0),
            leader_entry=float(leader_entry or 0),
            leader_leverage=float(leader_pos.get("leverage", 0) or 0),
            leader_max_notional=_leader_max_notional(leader_pos_map),
            margin_per_trade=margin_per_trade,
            max_leverage=max_leverage,
            product_max_leverage=product_max_lev,
        )

        if copy_size <= 0:
            continue

        try:
            result = await run_blocking(
                execute_market_order,
                telegram_id=user_id,
                product=product_key,
                size=copy_size,
                is_long=is_long,
                leverage=leverage,
                slippage_pct=1.5,
                enforce_rate_limit=False,
                source="copy",
                network=network,
                strategy_session_id=session_id,
            )

            if result.get("success"):
                fill_price = float(result.get("price", leader_entry) or leader_entry)
                # Open-side accounting: fee (when the archive resolved inline)
                # and entry notional as volume.
                await run_blocking(
                    update_mirror_accounting, mirror_id,
                    fees_delta=float(result.get("fee") or 0.0),
                    volume_delta=fill_price * copy_size,
                )
                # Record the copy position
                copy_position_id = await run_blocking(
                    insert_copy_position,
                    {
                        "mirror_id": mirror_id,
                        "user_id": user_id,
                        "product_id": pid,
                        "product_name": product_name,
                        "side": "long" if is_long else "short",
                        "entry_price": fill_price,
                        "size": copy_size,
                        "leverage": leverage,
                        "tp_price": leader_pos.get("tp_price"),
                        "sl_price": leader_pos.get("sl_price"),
                        "leader_entry_price": leader_entry,
                        "leader_size": leader_pos["size"],
                    },
                )

                # Place TP/SL orders if leader has them
                bracket_digests = await run_blocking(
                    _place_tp_sl_orders,
                    user_id,
                    product_key,
                    pid,
                    session_id=session_id,
                    size=copy_size,
                    is_long=is_long,
                    leverage=leverage,
                    tp_price=leader_pos.get("tp_price"),
                    sl_price=leader_pos.get("sl_price"),
                    network=network,
                )
                if copy_position_id and bracket_digests:
                    from src.nadobro.db import execute as db_execute

                    await run_blocking(
                        db_execute,
                        """
                        UPDATE copy_positions
                        SET tp_order_digest = %s,
                            sl_order_digest = %s
                        WHERE id = %s
                        """,
                        (
                            bracket_digests.get("tp_order_digest"),
                            bracket_digests.get("sl_order_digest"),
                            copy_position_id,
                        ),
                    )

                side_emoji = "🟢 LONG" if is_long else "🔴 SHORT"
                wallet = mirror.get("wallet_address", "")
                wallet_snip = f"{wallet[:6]}...{wallet[-4:]}" if len(wallet) >= 10 else wallet
                await _notify_user(
                    user_id,
                    f"📋 Copy Trade Opened\n"
                    f"Trader: {mirror.get('label') or wallet_snip}\n"
                    f"{side_emoji} {product_name}\n"
                    f"Size: {copy_size:.6f} @ ${fill_price:,.2f}\n"
                    f"Leverage: {leverage}x"
                    + (f"\nTP: ${leader_pos['tp_price']:,.2f}" if leader_pos.get("tp_price") else "")
                    + (f"\nSL: ${leader_pos['sl_price']:,.2f}" if leader_pos.get("sl_price") else "")
                )
            else:
                error = result.get("error", "Unknown error")
                logger.warning("Copy trade failed for user %s: %s", user_id, error)

        except Exception as e:
            logger.error("Copy trade exception for user %s product %s: %s", user_id, pid, e)


def _place_tp_sl_orders(user_id: int, product_key: str, product_id: int,
                        size: float, is_long: bool, leverage: float,
                        tp_price: Optional[float], sl_price: Optional[float],
                        network: str, session_id=None):
    """Place TP and SL orders for a copy position."""
    digests = {}
    if tp_price and tp_price > 0:
        try:
            result = execute_limit_order(
                telegram_id=user_id,
                product=product_key,
                size=size,
                is_long=not is_long,  # TP closes the position
                leverage=leverage,
                price=tp_price,
                enforce_rate_limit=False,
                reduce_only=True,
                order_type_override="TAKE_PROFIT",
                source="copy",
                network=network,
                strategy_session_id=session_id,
            )
            if result.get("success") and result.get("digest"):
                digests["tp_order_digest"] = result.get("digest")
        except Exception as e:
            logger.warning("Failed to place TP for copy user %s: %s", user_id, e)

    if sl_price and sl_price > 0:
        try:
            result = execute_limit_order(
                telegram_id=user_id,
                product=product_key,
                size=size,
                is_long=not is_long,  # SL closes the position
                leverage=leverage,
                price=sl_price,
                enforce_rate_limit=False,
                reduce_only=True,
                order_type_override="STOP_LOSS",
                source="copy",
                network=network,
                strategy_session_id=session_id,
            )
            if result.get("success") and result.get("digest"):
                digests["sl_order_digest"] = result.get("digest")
        except Exception as e:
            logger.warning("Failed to place SL for copy user %s: %s", user_id, e)
    return digests


def _rail_decision(
    net_pnl: float, total_allocated: float, stop_loss_pct: float, take_profit_pct: float
) -> Optional[str]:
    """The cumulative rail, as a pure decision: "auto_stop_loss",
    "auto_take_profit", or None. ``net_pnl`` is realized (derived gross)
    + unrealized - fees — the same net-of-fees, uPnL-inclusive basis every
    session rail in this product uses."""
    if total_allocated <= 0:
        return None
    pnl_pct = (net_pnl / total_allocated) * 100.0
    if stop_loss_pct > 0 and net_pnl < 0 and abs(pnl_pct) >= stop_loss_pct:
        return "auto_stop_loss"
    if take_profit_pct > 0 and net_pnl > 0 and pnl_pct >= take_profit_pct:
        return "auto_take_profit"
    return None


def _classify_external_change(cp: dict, venue_pos: Optional[dict]) -> tuple[str, float]:
    """How does the venue's reality compare to a tracked open copy row?

    Returns ("closed", 0.0) when the venue no longer holds the product on the
    copy's side; ("reduced", venue_size) when the venue holds the same side
    but LESS than the copy size (externally reduced — bracket partial, manual
    trim); ("ok", venue_size) otherwise. A 2% tolerance absorbs venue rounding.
    """
    copy_size = float(cp.get("size") or 0.0)
    if venue_pos is None:
        return "closed", 0.0
    try:
        venue_size = abs(float(venue_pos.get("signed_amount") or venue_pos.get("amount") or 0.0))
        venue_side = str(venue_pos.get("side") or "").upper()
    except (TypeError, ValueError):
        return "ok", copy_size
    if venue_size <= 0 or (venue_side and venue_side != str(cp.get("side", "")).upper()):
        return "closed", 0.0
    if venue_size < copy_size * 0.98:
        return "reduced", venue_size
    return "ok", venue_size


def _resolve_bracket_fill(cp: dict, network: str) -> tuple[float, float, str]:
    """(close_price, fee, reason) for an externally-vanished copy position.

    Checks the position's own TP/SL bracket digests in the archive — a filled
    bracket gives the exact fill price and fee; otherwise the close happened
    outside our orders (manual/liquidation) and the caller falls back to mid.
    """
    for digest, reason in (
        (str(cp.get("tp_order_digest") or "").strip(), "bracket_take_profit"),
        (str(cp.get("sl_order_digest") or "").strip(), "bracket_stop_loss"),
    ):
        if not digest:
            continue
        try:
            parsed = query_order_by_digest(network, digest, 1.0, 0.25)
        except Exception:  # noqa: BLE001 - archive lookup best-effort
            parsed = None
        if parsed and parsed.get("is_filled"):
            price = float(parsed.get("fill_price") or 0.0)
            fee = float(parsed.get("fee") or 0.0) + float(parsed.get("builder_fee") or 0.0)
            if price > 0:
                return price, fee, reason
    return 0.0, 0.0, "external_close"


async def _sweep_external_closes(
    mirror: dict,
    copy_pos_by_product: dict,
    venue_pos_by_product: dict,
    follower_client,
) -> tuple[float, float]:
    """Book copy closes that happened ON the venue without us placing them:
    filled TP/SL brackets, manual closes, liquidations. Returns
    (pnl_booked, fees_booked) so the caller's rail sees them this poll."""
    mirror_id = mirror["id"]
    user_id = mirror["user_id"]
    network = str(mirror.get("network", "mainnet"))
    pnl_total = 0.0
    fees_total = 0.0
    for pid, cp in list(copy_pos_by_product.items()):
        kind, venue_size = _classify_external_change(cp, venue_pos_by_product.get(pid))
        if kind == "ok":
            continue
        copy_size = float(cp.get("size") or 0.0)
        closed_qty = copy_size if kind == "closed" else max(0.0, copy_size - venue_size)
        if closed_qty <= 0:
            continue
        close_price, fee, reason = await run_blocking(_resolve_bracket_fill, cp, network)
        if close_price <= 0 and follower_client is not None:
            try:
                mp = await run_blocking(follower_client.get_market_price, pid)
                close_price = float((mp or {}).get("mid") or 0.0)
            except Exception:  # noqa: BLE001 - price best-effort
                close_price = 0.0
        pnl = _close_pnl_gross(cp.get("entry_price"), close_price, closed_qty, cp.get("side", ""))
        await run_blocking(
            update_mirror_accounting, mirror_id,
            pnl_delta=pnl, fees_delta=fee, volume_delta=close_price * closed_qty,
        )
        pnl_total += pnl
        fees_total += fee
        product_name = cp.get("product_name") or get_product_name(pid, network=network)
        if kind == "closed":
            await run_blocking(close_copy_position, cp["id"], pnl=pnl, reason=reason)
            copy_pos_by_product.pop(pid, None)
            # Cancel the surviving sibling bracket so it can't fire on a
            # position that no longer exists (reduce-only protects funds, but
            # a stale resting order confuses the venue reconcile).
            if follower_client is not None:
                for digest in {
                    str(cp.get("tp_order_digest") or "").strip(),
                    str(cp.get("sl_order_digest") or "").strip(),
                }:
                    if digest:
                        try:
                            await run_blocking(follower_client.cancel_order, pid, digest)
                        except Exception:  # policy: degrade-ok(sibling bracket usually already gone with the position; reduce-only means a stale survivor cannot open exposure)
                            pass
            label = {
                "bracket_take_profit": "Take-profit filled on venue",
                "bracket_stop_loss": "Stop-loss filled on venue",
            }.get(reason, "Position closed on venue")
            await _notify_user(
                user_id,
                f"📋 Copy Position Closed\n{product_name}: {label}\nP&L: ${pnl:+,.2f}",
            )
        else:
            await run_blocking(
                reduce_copy_position, cp["id"], venue_size,
                float(cp.get("leader_size") or 0.0), pnl,
            )
            cp["size"] = venue_size
            await _notify_user(
                user_id,
                f"📋 Copy Position Reduced on venue\n"
                f"{product_name}: now {venue_size:g}\nRealized P&L: ${pnl:+,.2f}",
            )
    return pnl_total, fees_total


async def _compute_open_upnl(
    copy_pos_by_product: dict, venue_pos_by_product: dict, follower_client
) -> float:
    """Unrealized PnL of the open copy rows.

    Prefers the venue's own uPnL scaled to the copy's share of the venue
    position (exact when only the copy holds the product); falls back to
    (mid - entry) x size pairing when the venue omits uPnL.
    """
    total = 0.0
    for pid, cp in copy_pos_by_product.items():
        copy_size = float(cp.get("size") or 0.0)
        if copy_size <= 0:
            continue
        vp = venue_pos_by_product.get(pid)
        if vp is not None and vp.get("unrealized_pnl") is not None:
            try:
                venue_size = abs(float(vp.get("signed_amount") or vp.get("amount") or 0.0))
                if venue_size > 0:
                    share = min(1.0, copy_size / venue_size)
                    total += float(vp["unrealized_pnl"]) * share
                    continue
            except (TypeError, ValueError):
                pass
        if follower_client is not None:
            try:
                mp = await run_blocking(follower_client.get_market_price, pid)
                mid = float((mp or {}).get("mid") or 0.0)
            except Exception:  # noqa: BLE001 - price best-effort
                mid = 0.0
            if mid > 0:
                total += _close_pnl_gross(cp.get("entry_price"), mid, copy_size, cp.get("side", ""))
    return total


def _close_pnl_gross(entry_price: float, close_price: float, size: float, side: str) -> float:
    """Derived realized PnL for a close, GROSS of fees.

    The venue reports NO per-fill realized PnL (realized_pnl_x18 is always 0),
    so copy accounting pairs the close fill price against the recorded entry —
    the same derivation discipline as quant/portfolio_calculator. Fees are
    tracked separately (cumulative_fees_usd) so rails judge NET.
    """
    entry = float(entry_price or 0.0)
    close = float(close_price or 0.0)
    qty = float(size or 0.0)
    if entry <= 0 or close <= 0 or qty <= 0:
        return 0.0
    direction = 1.0 if str(side).upper() == "LONG" else -1.0
    return (close - entry) * qty * direction


def _resolve_close_fill(result: Optional[dict], network: str) -> tuple[float, float]:
    """(close_price, fee) for a close order — result payload first (price is
    fill→post-fill→mid; fee present when the archive resolved inline), then
    one archive lookup by digest. A zero price means 'unknown' and the caller
    must fall back rather than book a bogus 0-PnL pairing."""
    result = result or {}
    close_price = float(result.get("price") or 0.0)
    fee = float(result.get("fee") or 0.0)
    if close_price > 0 and fee > 0:
        return close_price, fee
    digest = str(result.get("digest") or "").strip()
    if digest:
        try:
            parsed = query_order_by_digest(network, digest, 2.0, 0.25)
        except Exception:  # noqa: BLE001 - archive lookup is best-effort
            parsed = None
        if parsed:
            if close_price <= 0:
                close_price = float(parsed.get("fill_price") or 0.0)
            if fee <= 0:
                fee = float(parsed.get("fee") or 0.0) + float(parsed.get("builder_fee") or 0.0)
    return close_price, fee


def _settle_copy_close(
    mirror_id: int, cp: dict, result: Optional[dict], network: str, closed_size: float
) -> tuple[float, float, float]:
    """Book one copy close into the mirror accounting.

    Returns (pnl_gross, fee, close_price). Books derived gross PnL + fee +
    close-side volume additively onto copy_mirrors. When the close price is
    unresolvable (archive miss AND no payload price) books fee/volume=0 and
    pnl=0 with a loud log — never a silently wrong pairing.
    """
    close_price, fee = _resolve_close_fill(result, network)
    if close_price <= 0:
        logger.warning(
            "copy close for position %s has no resolvable fill price — "
            "booked 0 PnL; session rollup will correct totals at stop",
            cp.get("id"),
        )
        return 0.0, 0.0, 0.0
    pnl = _close_pnl_gross(cp.get("entry_price"), close_price, closed_size, cp.get("side", ""))
    update_mirror_accounting(
        mirror_id,
        pnl_delta=pnl,
        fees_delta=fee,
        volume_delta=close_price * float(closed_size or 0.0),
    )
    return pnl, fee, close_price


def _partial_close_fraction(baseline_leader_size: float, new_leader_size: float) -> float:
    """Fraction of the copy to close when the leader trims below our baseline.

    Baseline is the leader size we last mirrored (at open, or after the last
    partial). A leader ADD does not move the baseline — so add-then-trim back
    to the baseline correctly mirrors nothing, and only a trim BELOW the
    baseline reduces the copy. Returns 0.0 when the delta is under the
    PARTIAL_CLOSE_MIN_DELTA_PCT noise threshold, 1.0 for a dust remainder.
    """
    baseline = float(baseline_leader_size or 0.0)
    new = float(new_leader_size or 0.0)
    if baseline <= 0 or new >= baseline:
        return 0.0
    fraction = (baseline - new) / baseline
    if fraction * 100.0 < PARTIAL_CLOSE_MIN_DELTA_PCT:
        return 0.0
    if (1.0 - fraction) <= PARTIAL_CLOSE_DUST_FRACTION:
        return 1.0
    return fraction


async def _mirror_partial_close_if_needed(
    existing_cp: dict, leader_pos: dict, user_id: int, mirror_id: int, network: str,
    session_id=None,
) -> str:
    """Mirror a leader's partial close onto the copy position.

    Returns "reduced" (copy shrunk; caller re-places TP/SL for the new size),
    "closed" (dust remainder -> fully closed), or "" (no action).
    """
    if not PARTIAL_CLOSES_ENABLED:
        return ""
    baseline = float(existing_cp.get("leader_size") or 0.0)
    new_leader = float(leader_pos.get("size") or 0.0)
    fraction = _partial_close_fraction(baseline, new_leader)
    if fraction <= 0.0:
        return ""

    copy_size = float(existing_cp.get("size") or 0.0)
    if copy_size <= 0:
        return ""
    product_name = existing_cp.get("product_name") or get_product_name(
        existing_cp["product_id"], network=network
    )
    product_key = product_name.replace("-PERP", "")
    is_close_long = existing_cp["side"].upper() != "LONG"
    close_size = copy_size if fraction >= 1.0 else copy_size * fraction

    try:
        result = await run_blocking(
            execute_market_order,
            telegram_id=user_id,
            product=product_key,
            size=close_size,
            is_long=is_close_long,
            leverage=float(existing_cp.get("leverage", 1.0)),
            slippage_pct=1.5,
            enforce_rate_limit=False,
            reduce_only=True,
            source="copy",
            network=network,
            strategy_session_id=session_id,
        )
        if not result.get("success"):
            logger.warning(
                "copy partial close failed for position %s: %s",
                existing_cp["id"], result.get("error", "close failed"),
            )
            return ""
        pnl, _fee, _price = await run_blocking(
            _settle_copy_close, mirror_id, existing_cp, result, network, close_size
        )

        if fraction >= 1.0:
            await run_blocking(
                close_copy_position, existing_cp["id"], pnl=pnl, reason="leader_partial_dust"
            )
            await _notify_user(
                user_id,
                f"📋 Copy Position Closed\n"
                f"{product_name}: Leader trimmed to dust → Your position closed\n"
                f"P&L: ${pnl:+,.2f}",
            )
            return "closed"

        remaining = copy_size - close_size
        await run_blocking(
            reduce_copy_position, existing_cp["id"], remaining, new_leader, pnl
        )
        # Keep the in-memory row consistent for the TP/SL resize that follows.
        existing_cp["size"] = remaining
        existing_cp["leader_size"] = new_leader
        await _notify_user(
            user_id,
            f"📋 Copy Position Reduced\n"
            f"{product_name}: Leader closed {fraction * 100.0:.0f}% → "
            f"Your position reduced to {remaining:g}\n"
            f"Realized P&L: ${pnl:+,.2f}",
        )
        return "reduced"
    except Exception as e:  # noqa: BLE001 - one mirror's failure must not stall the poll
        logger.error("copy partial close error for position %s: %s", existing_cp["id"], e)
        return ""


def _update_tp_sl_if_changed(
    existing_cp: dict, leader_pos: dict, user_id: int, network: str, force: bool = False,
    session_id=None,
):
    """Check if leader's TP/SL changed and update the copy position's orders.

    ``force`` re-places the bracket even at unchanged prices — used after a
    partial close so the resting reduce-only orders match the shrunken size
    (they were placed for the pre-reduction size).
    """
    new_tp = leader_pos.get("tp_price")
    new_sl = leader_pos.get("sl_price")
    old_tp = existing_cp.get("tp_price")
    old_sl = existing_cp.get("sl_price")

    # Only update if there's a meaningful change
    tp_changed = (new_tp or 0) != (old_tp or 0)
    sl_changed = (new_sl or 0) != (old_sl or 0)

    if not tp_changed and not sl_changed and not force:
        return
    if force and not (new_tp or new_sl or old_tp or old_sl):
        return  # nothing bracketed — nothing to resize

    # Update the copy_positions record. On a pure force-resize (partial close
    # with unchanged bracket prices) there are no price columns to update —
    # skip the UPDATE entirely (an empty SET is a SQL syntax error) and go
    # straight to cancel + re-place for the new size.
    from src.nadobro.db import execute as db_execute
    updates = []
    params = []
    if tp_changed:
        updates.append("tp_price = %s")
        params.append(new_tp)
    if sl_changed:
        updates.append("sl_price = %s")
        params.append(new_sl)
    if updates:
        params.append(existing_cp["id"])
        db_execute(f"UPDATE copy_positions SET {', '.join(updates)} WHERE id = %s", params)

    # Cancel only the TP/SL orders this copy position created.
    product_name = existing_cp.get("product_name", "")
    product_key = product_name.replace("-PERP", "")
    pid = existing_cp["product_id"]
    size = float(existing_cp.get("size", 0))
    is_long = existing_cp["side"].upper() == "LONG"
    leverage = float(existing_cp.get("leverage", 1.0))

    # Cancel existing TP/SL orders for this product
    try:
        client = get_user_nado_client(user_id, network)
        if client:
            for digest in {
                str(existing_cp.get("tp_order_digest") or "").strip(),
                str(existing_cp.get("sl_order_digest") or "").strip(),
            }:
                if digest:
                    try:
                        client.cancel_order(pid, digest)
                    except Exception as e:
                        logger.warning(
                            "old TP/SL cancel failed for product %s digest %s — "
                            "stale protective order may still be resting: %s",
                            pid, digest, e,
                        )
    except Exception as e:
        logger.debug("Failed to cancel old TP/SL: %s", e)

    # Place updated TP/SL
    bracket_digests = _place_tp_sl_orders(
        user_id, product_key, pid, session_id=session_id, size=size,
        is_long=is_long, leverage=leverage, tp_price=new_tp, sl_price=new_sl,
        network=network,
    )
    db_execute(
        """
        UPDATE copy_positions
        SET tp_order_digest = %s,
            sl_order_digest = %s
        WHERE id = %s
        """,
        (
            bracket_digests.get("tp_order_digest"),
            bracket_digests.get("sl_order_digest"),
            existing_cp["id"],
        ),
    )
