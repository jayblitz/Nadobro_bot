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
    get_copy_mirror,
    stop_copy_mirror,
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
    save_copy_snapshot,
    get_latest_copy_snapshot,
    get_copy_trades_by_mirror,
)
from src.nadobro.services.user_service import get_user, get_user_nado_client
from src.nadobro.services.trade_service import execute_market_order
from src.nadobro.services.nado_client import NadoClient
from src.nadobro.services.async_utils import run_blocking

logger = logging.getLogger(__name__)

MAX_MIRRORS_PER_USER = 5
MIN_MARGIN_PER_TRADE = 5.0
MAX_MARGIN_PER_TRADE = 5000.0
POLL_INTERVAL_SECONDS = 30
_bot_app = None
_poll_task: Optional[asyncio.Task] = None


def set_copy_bot_app(app):
    global _bot_app
    _bot_app = app


async def _notify_user(telegram_id: int, text: str):
    if not _bot_app:
        return
    try:
        await _bot_app.bot.send_message(chat_id=telegram_id, text=text)
    except Exception as e:
        logger.warning("Copy notify failed for %s: %s", telegram_id, e)


# ─── Public API ────────────────────────────────────────────────

def add_trader(wallet_address: str, label: str = "", is_curated: bool = False) -> tuple[bool, str, int | None]:
    if not wallet_address or len(wallet_address) < 10:
        return False, "Invalid wallet address.", None
    wallet = wallet_address.strip()
    if not wallet.startswith("0x"):
        return False, "Wallet address must start with 0x.", None
    trader_id = upsert_copy_trader(wallet, label=label, is_curated=is_curated)
    if not trader_id:
        return False, "Failed to save trader.", None
    return True, f"Trader added: {label or wallet[:10]}...", trader_id


def remove_trader(trader_id: int) -> tuple[bool, str]:
    trader = get_copy_trader(trader_id)
    if not trader:
        return False, "Trader not found."
    mirrors = get_mirrors_for_trader(trader_id)
    for m in mirrors:
        stop_copy_mirror(m["id"])
    deactivate_copy_trader(trader_id)
    label = trader.get("label") or trader["wallet_address"][:10]
    return True, f"Trader {label} removed. {len(mirrors)} mirror(s) stopped."


def start_copy(
    telegram_id: int,
    trader_id: int,
    network: str = "mainnet",
    margin_per_trade: float = 50.0,
    max_leverage: float = 10.0,
    cumulative_stop_loss_pct: float = 50.0,
    cumulative_take_profit_pct: float = 100.0,
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
        return False, "Failed to create copy mirror."

    label = trader.get("label") or trader["wallet_address"][:10]
    return True, (
        f"🔗 Now copying {label}\n"
        f"💰 Margin/Trade: ${margin_per_trade:.0f}\n"
        f"📊 Max Leverage: {max_leverage}x\n"
        f"🛑 Stop Loss: {cumulative_stop_loss_pct}% of ${total_allocated_usd:.0f}\n"
        f"🎯 Take Profit: {cumulative_take_profit_pct}% of ${total_allocated_usd:.0f}"
    )


def stop_copy(telegram_id: int, mirror_id: int) -> tuple[bool, str]:
    mirror = get_copy_mirror(mirror_id)
    if not mirror:
        return False, "Mirror not found."
    if mirror["user_id"] != telegram_id:
        return False, "Not your mirror."
    if not mirror.get("active"):
        return False, "Mirror already stopped."
    stop_copy_mirror(mirror_id)
    return True, "Copy trading stopped for this trader."


def pause_copy(telegram_id: int, mirror_id: int) -> tuple[bool, str]:
    mirror = get_copy_mirror(mirror_id)
    if not mirror:
        return False, "Mirror not found."
    if mirror["user_id"] != telegram_id:
        return False, "Not your mirror."
    if not mirror.get("active"):
        return False, "Mirror is stopped."
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
    if not mirror.get("paused"):
        return False, "Mirror is not paused."
    resume_copy_mirror(mirror_id)
    return True, "Copy trading resumed."


def stop_all_copies(telegram_id: int) -> tuple[bool, str]:
    mirrors = get_user_active_mirrors_v2(telegram_id)
    if not mirrors:
        return False, "No active copy mirrors."
    for m in mirrors:
        stop_copy_mirror(m["id"])
    return True, f"Stopped {len(mirrors)} copy mirror(s)."


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
            "margin_per_trade": m.get("margin_per_trade", 50.0),
            "max_leverage": m.get("max_leverage", 10.0),
            "cumulative_stop_loss_pct": m.get("cumulative_stop_loss_pct", 50.0),
            "cumulative_take_profit_pct": m.get("cumulative_take_profit_pct", 100.0),
            "total_allocated_usd": m.get("total_allocated_usd", 500.0),
            "cumulative_pnl": float(m.get("cumulative_pnl", 0)),
            "open_positions": len(open_positions),
            "paused": bool(m.get("paused")),
            "network": m.get("network", "mainnet"),
            "created_at": m.get("created_at"),
        })
    return result


def get_user_mirrors(telegram_id: int) -> list[dict]:
    return get_user_copies(telegram_id)


def get_available_traders() -> list[dict]:
    traders = get_active_copy_traders()
    return [
        {
            "id": t["id"],
            "wallet": t["wallet_address"],
            "label": t.get("label") or t["wallet_address"][:10],
            "is_curated": t.get("is_curated", False),
        }
        for t in traders
    ]


def get_trader_stats(trader_id: int) -> dict:
    """Aggregate display metrics for a copy trader across active mirrors.

    This keeps callback handlers resilient even when richer analytics sources
    are unavailable, and preserves the expected response shape.
    """
    mirrors = get_mirrors_for_trader(trader_id) or []
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

    for mirror in mirrors:
        mirror_id = int(mirror.get("id") or 0)
        if mirror_id <= 0:
            continue

        # Mirror-level cumulative PnL is the most reliable value currently tracked.
        stats["pnl_usd"] += float(mirror.get("cumulative_pnl") or 0.0)

        trades = get_copy_trades_by_mirror(mirror_id, limit=500) or []
        for t in trades:
            status = str(t.get("status") or "").lower()
            stats["total_trades"] += 1
            if status == "failed":
                stats["failed"] += 1
                continue
            if status != "filled":
                continue

            stats["filled"] += 1
            try:
                nado_sz = float(t.get("nado_size") or 0.0)
                nado_px = float(t.get("nado_price") or 0.0)
                hl_sz = float(t.get("hl_size") or 0.0)
                hl_px = float(t.get("hl_price") or 0.0)
                notional = abs(nado_sz * nado_px) if nado_sz and nado_px else abs(hl_sz * hl_px)
                stats["volume_usd"] += float(notional or 0.0)
            except Exception:
                pass

    # We currently don't persist per-trade realized PnL in copy_trades, so
    # represent win-rate as execution success ratio for now.
    if stats["total_trades"] > 0:
        stats["win_rate"] = (stats["filled"] / stats["total_trades"]) * 100.0
    return stats


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

        try:
            leader_pos_map = await run_blocking(_load_leader_position_map, trader_id, wallet, network)

            # Process each mirror
            for mirror in group_mirrors:
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
    leader_client = NadoClient.from_address(wallet, network)
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
            "entry_price": float(pos.get("entry_price", 0) or 0),
            "unrealized_pnl": float(pos.get("unrealized_pnl", 0) or 0),
        }
        try:
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
    except Exception:
        pass
    return leader_pos_map


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

    # Check cumulative P&L limits
    if total_allocated > 0:
        pnl_pct = (cumulative_pnl / total_allocated) * 100
        if cumulative_pnl < 0 and abs(pnl_pct) >= cumulative_stop_loss_pct:
            await run_blocking(auto_stop_mirror, mirror_id, f"Cumulative stop loss hit: {pnl_pct:.1f}%")
            await _notify_user(
                user_id,
                f"🛑 Copy Trading Auto-Stopped\n"
                f"Cumulative loss hit {abs(pnl_pct):.1f}% (limit: {cumulative_stop_loss_pct}%)\n"
                f"P&L: ${cumulative_pnl:,.2f} / ${total_allocated:,.0f} allocated"
            )
            return
        if cumulative_pnl > 0 and pnl_pct >= cumulative_take_profit_pct:
            await run_blocking(auto_stop_mirror, mirror_id, f"Cumulative take profit hit: {pnl_pct:.1f}%")
            await _notify_user(
                user_id,
                f"🎯 Copy Trading Auto-Stopped — Target Hit!\n"
                f"Cumulative profit hit {pnl_pct:.1f}% (target: {cumulative_take_profit_pct}%)\n"
                f"P&L: +${cumulative_pnl:,.2f} / ${total_allocated:,.0f} allocated"
            )
            return

    # Get current copy positions for this mirror
    open_copy_positions = await run_blocking(get_open_copy_positions, mirror_id)
    copy_pos_by_product = {}
    for cp in open_copy_positions:
        copy_pos_by_product[cp["product_id"]] = cp

    # 1. Close positions that leader has closed
    for pid, cp in list(copy_pos_by_product.items()):
        if pid not in leader_pos_map:
            # Leader closed this position — close ours too
            product_name = cp.get("product_name", get_product_name(pid))
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
                )
                pnl = float(result.get("pnl", 0) or 0)
                await run_blocking(close_copy_position, cp["id"], pnl=pnl, reason="leader_closed")
                await run_blocking(update_mirror_cumulative_pnl, mirror_id, pnl)
                await _notify_user(
                    user_id,
                    f"📋 Copy Position Closed\n"
                    f"{product_name}: Leader closed → Your position closed\n"
                    f"P&L: ${pnl:+,.2f}"
                )
            except Exception as e:
                logger.error("Failed to close copy position %s: %s", cp["id"], e)

    # 2. Open new positions that leader has opened
    for pid, leader_pos in leader_pos_map.items():
        if pid in copy_pos_by_product:
            # Already tracking this position — update TP/SL if changed
            existing = copy_pos_by_product[pid]
            await run_blocking(_update_tp_sl_if_changed, existing, leader_pos, user_id, network)
            continue

        # New position from leader — open a copy
        product_name = get_product_name(pid)
        product_key = product_name.replace("-PERP", "")
        is_long = leader_pos["side"] == "LONG"
        leader_entry = leader_pos.get("entry_price", 0)

        if leader_entry <= 0:
            continue

        # Calculate size based on user's margin_per_trade
        product_max_lev = get_product_max_leverage(product_key, network=network)
        leverage = min(max_leverage, product_max_lev)
        copy_size = (margin_per_trade * leverage) / leader_entry

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
            )

            if result.get("success"):
                fill_price = float(result.get("price", leader_entry) or leader_entry)
                # Record the copy position
                await run_blocking(
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
                await run_blocking(
                    _place_tp_sl_orders,
                    user_id,
                    product_key,
                    pid,
                    copy_size,
                    is_long,
                    leverage,
                    leader_pos.get("tp_price"),
                    leader_pos.get("sl_price"),
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
                        tp_price: Optional[float], sl_price: Optional[float]):
    """Place TP and SL orders for a copy position."""
    from src.nadobro.services.trade_service import execute_limit_order

    if tp_price and tp_price > 0:
        try:
            execute_limit_order(
                telegram_id=user_id,
                product=product_key,
                size=size,
                is_long=not is_long,  # TP closes the position
                leverage=leverage,
                price=tp_price,
                order_type="take_profit",
                enforce_rate_limit=False,
            )
        except Exception as e:
            logger.warning("Failed to place TP for copy user %s: %s", user_id, e)

    if sl_price and sl_price > 0:
        try:
            execute_limit_order(
                telegram_id=user_id,
                product=product_key,
                size=size,
                is_long=not is_long,  # SL closes the position
                leverage=leverage,
                price=sl_price,
                order_type="stop_loss",
                enforce_rate_limit=False,
            )
        except Exception as e:
            logger.warning("Failed to place SL for copy user %s: %s", user_id, e)


def _update_tp_sl_if_changed(existing_cp: dict, leader_pos: dict, user_id: int, network: str):
    """Check if leader's TP/SL changed and update the copy position's orders."""
    new_tp = leader_pos.get("tp_price")
    new_sl = leader_pos.get("sl_price")
    old_tp = existing_cp.get("tp_price")
    old_sl = existing_cp.get("sl_price")

    # Only update if there's a meaningful change
    tp_changed = (new_tp or 0) != (old_tp or 0) and new_tp
    sl_changed = (new_sl or 0) != (old_sl or 0) and new_sl

    if not tp_changed and not sl_changed:
        return

    # Update the copy_positions record
    from src.nadobro.db import execute as db_execute
    updates = []
    params = []
    if tp_changed:
        updates.append("tp_price = %s")
        params.append(new_tp)
    if sl_changed:
        updates.append("sl_price = %s")
        params.append(new_sl)
    params.append(existing_cp["id"])
    db_execute(f"UPDATE copy_positions SET {', '.join(updates)} WHERE id = %s", params)

    # Cancel old orders and place new ones
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
            orders = client.get_open_orders(pid) or []
            for o in orders:
                otype = (o.get("order_type") or o.get("type") or "").lower()
                if "take_profit" in otype or "stop_loss" in otype or "tp" in otype or "sl" in otype:
                    try:
                        digest = o.get("digest") or o.get("order_digest") or o.get("id")
                        if digest:
                            client.cancel_order(pid, str(digest))
                    except Exception:
                        pass
    except Exception as e:
        logger.debug("Failed to cancel old TP/SL: %s", e)

    # Place updated TP/SL
    _place_tp_sl_orders(user_id, product_key, pid, size, is_long, leverage,
                        new_tp if tp_changed else old_tp,
                        new_sl if sl_changed else old_sl)
