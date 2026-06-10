"""Portfolio callback handlers (portfolio:* callback data).

Extracted from callbacks.py (decomposition slice, 2026-06). May import
shared utils from callbacks at module level — callbacks only imports this
module lazily inside its _handle_portfolio shim, so there is no cycle.
"""
from __future__ import annotations

import logging

from src.nadobro.handlers.keyboards import portfolio_analytics_kb
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.perf import timed_metric
from src.nadobro.services.trade_service import close_all_positions
from src.nadobro.services.user_service import get_user_nado_client, get_user
from telegram.error import BadRequest

from src.nadobro.handlers.callbacks import _edit_loc  # noqa: E402

logger = logging.getLogger(__name__)


async def _handle_portfolio(query, data, telegram_id):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"
    try:
        user = get_user(telegram_id)
    except Exception:
        user = None
    mode_label = None
    if user:
        mode_label = user.network_mode.value.upper()

    if action == "close_all_confirm":
        from src.nadobro.handlers.portfolio_deck import render_close_all_confirm

        text, kb = render_close_all_confirm()
        await _edit_loc(query, text, reply_markup=kb)
        return

    if action == "close_all_yes":
        await _edit_loc(query, "⏳ Closing all positions…")
        network = user.network_mode.value if user else "mainnet"
        result = await run_blocking(close_all_positions, telegram_id, network=network)
        if isinstance(result, dict) and not result.get("success", False):
            await _edit_loc(query, f"⚠ Close all failed: {str(result.get('error') or result.get('message') or 'unknown error')[:240]}")
            return
        from src.nadobro.handlers.portfolio_deck import render_portfolio_deck, snapshot_for_user

        snapshot = await snapshot_for_user(telegram_id, force=True)
        text, kb = render_portfolio_deck(snapshot)
        await _edit_loc(query, text, reply_markup=kb)
        return

    if action == "cancel_all_confirm":
        from src.nadobro.handlers.orders_view import render_cancel_all_confirm

        text, kb = render_cancel_all_confirm()
        await _edit_loc(query, text, reply_markup=kb)
        return

    if action == "cancel_all_yes":
        await _edit_loc(query, "⏳ Cancelling open orders…")
        from src.nadobro.handlers.portfolio_deck import snapshot_for_user
        from src.nadobro.handlers.positions_view import render_positions_view

        snapshot = await snapshot_for_user(telegram_id, force=True)
        client = await run_blocking(get_user_nado_client, telegram_id, snapshot.get("network"))
        if not client:
            await _edit_loc(query, "⚠ Cannot cancel orders: Nado client unavailable.")
            return
        plain_by_product: dict[int, list[str]] = {}
        trigger_by_product: dict[int, list[str]] = {}
        skipped = 0
        failures: list[str] = []
        for order in snapshot.get("open_orders") or []:
            try:
                pid = int(order.get("product_id"))
            except Exception:
                skipped += 1
                continue
            digest = str(order.get("digest") or order.get("order_digest") or "")
            if not digest:
                skipped += 1
                continue
            target = trigger_by_product if bool(order.get("is_trigger")) else plain_by_product
            target.setdefault(pid, []).append(digest)
        for pid, digests in plain_by_product.items():
            result = await client.cancel_orders(product_id=pid, digests=digests)
            if not result.get("success"):
                failures.append(str(result.get("error") or f"product {pid}"))
        for pid, digests in trigger_by_product.items():
            result = await client.cancel_trigger_orders(product_id=pid, digests=digests)
            if not result.get("success"):
                failures.append(str(result.get("error") or f"trigger product {pid}"))
        if failures:
            await _edit_loc(query, f"⚠ Some orders were not cancelled: {'; '.join(failures)[:240]}")
            return
        if skipped:
            logger.warning("portfolio_cancel_all_skipped_orders user=%s skipped=%s", telegram_id, skipped)
        snapshot = await snapshot_for_user(telegram_id, force=True)
        from src.nadobro.handlers.positions_view import render_positions_view

        text, kb = render_positions_view(snapshot)
        await _edit_loc(query, text, reply_markup=kb)
        return

    if action == "cancel_order":
        await _edit_loc(query, "⏳ Cancelling order…")
        from src.nadobro.handlers.orders_view import sorted_orders
        from src.nadobro.handlers.portfolio_deck import snapshot_for_user
        from src.nadobro.handlers.positions_view import render_positions_view

        try:
            order_index = int(parts[2]) if len(parts) > 2 else -1
        except (TypeError, ValueError):
            order_index = -1
        snapshot = await snapshot_for_user(telegram_id, force=True)
        orders = sorted_orders(snapshot)
        if order_index < 0 or order_index >= len(orders):
            text, kb = render_positions_view(snapshot)
            await _edit_loc(query, "⚠ Order list changed. Please try again.\n\n" + text, reply_markup=kb)
            return
        order = orders[order_index]
        try:
            product_id = int(order.get("product_id"))
        except Exception:
            await _edit_loc(query, "⚠ Cannot cancel this order: missing product id.")
            return
        digest = str(order.get("digest") or order.get("order_digest") or "")
        if not digest:
            await _edit_loc(query, "⚠ Cannot cancel this order: missing order digest.")
            return
        client = await run_blocking(get_user_nado_client, telegram_id, snapshot.get("network"))
        if not client:
            await _edit_loc(query, "⚠ Cannot cancel order: Nado client unavailable.")
            return
        if bool(order.get("is_trigger")):
            result = await client.cancel_trigger_orders(product_id=product_id, digests=[digest])
        else:
            result = await client.cancel_orders(product_id=product_id, digests=[digest])
        if not result.get("success"):
            await _edit_loc(query, f"⚠ Cancel failed: {str(result.get('error') or 'unknown error')[:240]}")
            return
        snapshot = await snapshot_for_user(telegram_id, force=True)
        text, kb = render_positions_view(snapshot)
        await _edit_loc(query, text, reply_markup=kb)
        return

    if action == "positions":
        # Combined Positions + Orders screen. Callbacks:
        #   portfolio:positions           -> both at page 0
        #   portfolio:positions:{n}       -> back-compat: both at page n
        #   portfolio:positions:pos:{n}   -> position section page n
        #   portfolio:positions:ord:{n}   -> order section page n
        from src.nadobro.handlers.portfolio_deck import snapshot_for_user
        from src.nadobro.handlers.positions_view import render_positions_view

        pos_page = None
        ord_page = None
        page = 0
        if len(parts) > 2:
            tail = parts[2:]
            if tail and tail[0].isdigit():
                page = int(tail[0])
            elif tail[0] == "pos" and len(tail) > 1 and tail[1].isdigit():
                pos_page = int(tail[1])
            elif tail[0] == "ord" and len(tail) > 1 and tail[1].isdigit():
                ord_page = int(tail[1])
        snapshot = await snapshot_for_user(telegram_id)
        text, kb = render_positions_view(
            snapshot, page=page, pos_page=pos_page, ord_page=ord_page
        )
        await _edit_loc(query, text, reply_markup=kb)
        return

    if action == "orders":
        # Legacy alias: forward to the combined Positions screen so callers
        # following older callback_data still land somewhere sensible.
        from src.nadobro.handlers.portfolio_deck import snapshot_for_user
        from src.nadobro.handlers.positions_view import render_positions_view

        page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        snapshot = await snapshot_for_user(telegram_id)
        text, kb = render_positions_view(snapshot, ord_page=page)
        await _edit_loc(query, text, reply_markup=kb)
        return

    if action == "history":
        from src.nadobro.handlers.history_view import render_history_view
        from src.nadobro.handlers.portfolio_deck import snapshot_for_user

        try:
            page = max(0, int(parts[2])) if len(parts) > 2 else 0
        except (TypeError, ValueError):
            page = 0
        if not user:
            await _edit_loc(
                query,
                "⚠ History unavailable — execution mode not set. Use /start to choose Testnet or Mainnet.",
            )
            return
        try:
            snapshot = await snapshot_for_user(telegram_id)
        except Exception as e:
            logger.warning("portfolio_history_snapshot_failed user=%s err=%s", telegram_id, e)
            snapshot = {
                "user_id": int(telegram_id),
                "network": user.network_mode.value,
                "matches": [],
            }
        if str(snapshot.get("network") or "").lower() != user.network_mode.value.lower():
            snapshot["network"] = user.network_mode.value
        text, kb = render_history_view(snapshot, page=page)
        await _edit_loc(query, text, reply_markup=kb)
        return

    if action in ("analytics", "performance"):
        from src.nadobro.handlers.performance_view import render_performance_view

        if not user:
            await _edit_loc(
                query,
                "⚠ Performance unavailable — execution mode not set. Use /start to choose Testnet or Mainnet.",
            )
            return
        network = user.network_mode.value
        page = 0
        if len(parts) > 2 and parts[2].isdigit():
            page = int(parts[2])
        try:
            text, kb = await run_blocking(render_performance_view, telegram_id, network, page)
        except Exception as e:
            logger.warning("portfolio_performance_failed user=%s err=%s", telegram_id, e)
            text, kb = "📊 Performance\n\nNo performance data available.", portfolio_analytics_kb()
        await _edit_loc(query, text, reply_markup=kb)
        return

    if action == "share_pnl" and not user:
        await query.answer("Execution mode not set.", show_alert=True)
        return

    if action == "share_pnl":
        # Build a PnL share-card from a specific session (or the latest one
        # if no session id is provided). Per the workflow plan, each card
        # in the Performance tab triggers ``portfolio:share_pnl:{sid}`` so
        # the image reflects THAT session, not cumulative stats. Trade
        # round-trips from History use ``portfolio:share_pnl:rt:{trade_id}``.
        import io as _io

        from src.nadobro.services.pnl_card import generate_pnl_card
        from src.nadobro.services.pnl_card_builder import (
            build_pnl_card_data,
            build_round_trip_card_data,
        )

        network = user.network_mode.value if user else "mainnet"
        session_id: int | None = None
        round_trip_key: str | None = None
        if len(parts) > 2:
            if parts[2] == "rt" and len(parts) > 3:
                round_trip_key = parts[3]
            else:
                try:
                    session_id = int(parts[2])
                except (TypeError, ValueError):
                    session_id = None
        try:
            if round_trip_key:
                data = await run_blocking(
                    build_round_trip_card_data, telegram_id, network, round_trip_key
                )
            else:
                data = await run_blocking(
                    build_pnl_card_data, telegram_id, network, session_id
                )
            png_bytes = await run_blocking(generate_pnl_card, data)
        except Exception as e:
            logger.warning("portfolio_share_pnl_failed user=%s err=%s", telegram_id, e)
            await query.answer("Could not generate PnL card.", show_alert=True)
            return
        try:
            await query.message.reply_photo(
                photo=_io.BytesIO(png_bytes),
                caption=(
                    "📊 *Your PnL Card*\n"
                    "Share your performance on Nado."
                ),
                parse_mode="Markdown",
            )
            await query.answer()
        except Exception as e:
            logger.warning("portfolio_share_pnl_send_failed user=%s err=%s", telegram_id, e)
            await query.answer("Could not send PnL card image.", show_alert=True)
        return

    # Default: portfolio overview (single shared 24h / 7d / 30d / All toggle).
    from src.nadobro.handlers.portfolio_deck import (
        render_loading,
        render_portfolio_deck,
        snapshot_for_user,
    )

    force_refresh = action == "refresh"
    # Both ``portfolio:view:{win}`` and ``portfolio:refresh:{win}`` encode
    # the active stats window in callback_data. The default ``view`` /
    # ``refresh`` actions keep ``24h``.
    window = parts[2] if action in ("view", "refresh") and len(parts) > 2 else "24h"
    await _edit_loc(query, render_loading())
    with timed_metric("cb.portfolio.view"):
        snapshot = await snapshot_for_user(telegram_id, force=force_refresh)
        msg, reply_markup = render_portfolio_deck(snapshot, window=window)
    try:
        await _edit_loc(query,
            msg,
            reply_markup=reply_markup,
        )
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        raise
