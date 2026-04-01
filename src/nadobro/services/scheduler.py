import logging
import asyncio
import os
from datetime import datetime, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from src.nadobro.services.alert_service import get_triggered_alerts
from src.nadobro.services.stop_loss_service import process_stop_losses
from src.nadobro.services.nado_client import NadoClient
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.perf import timed_metric
from src.nadobro.services.execution_queue import enqueue_alert
from src.nadobro.services.lowiq_relay_client import relay_poll_interval_seconds

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone="UTC")
_bot_app = None
_check_client = None
_ALERT_SCAN_SECONDS = int(os.environ.get("ALERT_SCAN_SECONDS", "5"))
# Archive indexer can lag behind fills; allow longer polling than inline trade resolution.
_FILL_SYNC_DIGEST_WAIT = float(os.environ.get("NADO_FILL_SYNC_DIGEST_WAIT_SECONDS", "12"))
_FILL_SYNC_DIGEST_POLL = float(os.environ.get("NADO_FILL_SYNC_DIGEST_POLL_SECONDS", "0.45"))
_MARKET_SNAPSHOT_TTL_SECONDS = float(os.environ.get("NADO_MARKET_SNAPSHOT_TTL_SECONDS", "3.0"))
_last_market_snapshot: dict = {"ts": 0.0, "prices": {}}


def _format_alert_metric_value(condition: str, value: float) -> str:
    condition = str(condition or "")
    if condition.startswith("funding_"):
        return f"{float(value):,.4f}%"
    return f"${float(value):,.2f}"


def _alert_condition_label(condition: str) -> str:
    labels = {
        "above": "Price Above",
        "below": "Price Below",
        "funding_above": "Funding Above",
        "funding_below": "Funding Below",
        "pnl_above": "PnL Above",
        "pnl_below": "PnL Below",
    }
    return labels.get(str(condition or ""), str(condition or ""))


async def _build_alert_context() -> tuple[dict, dict]:
    """Build optional context maps used by funding/pnl alerts."""
    from src.nadobro.models.database import get_all_active_alerts, AlertCondition
    from src.nadobro.config import get_product_id
    from src.nadobro.services.user_service import get_user_readonly_client

    funding_rates: dict = {}
    positions_by_user: dict = {}

    active_alerts = await run_blocking(get_all_active_alerts)
    if not active_alerts:
        return funding_rates, positions_by_user

    needs_funding = False
    needs_pnl = False
    funding_products: set[str] = set()
    pnl_user_ids: set[int] = set()

    for alert in active_alerts:
        cond = alert.get("condition")
        if cond in (AlertCondition.FUNDING_ABOVE.value, AlertCondition.FUNDING_BELOW.value):
            needs_funding = True
            product = str((alert.get("product_name") or "")).replace("-PERP", "")
            if product:
                funding_products.add(product)
        elif cond in (AlertCondition.PNL_ABOVE.value, AlertCondition.PNL_BELOW.value):
            needs_pnl = True
            uid = alert.get("user_id")
            if uid is not None:
                try:
                    pnl_user_ids.add(int(uid))
                except Exception:
                    continue

    if needs_funding and _check_client:
        for product in funding_products:
            try:
                pid = get_product_id(
                    product,
                    network=getattr(_check_client, "network", "mainnet"),
                    client=_check_client,
                )
                if pid is None:
                    continue
                fr = await run_blocking(_check_client.get_funding_rate, pid)
                if isinstance(fr, dict):
                    funding_rates[product] = float(fr.get("funding_rate", 0) or 0)
            except Exception:
                continue

    if needs_pnl:
        for user_id in pnl_user_ids:
            try:
                client = await run_blocking(get_user_readonly_client, user_id)
                if not client:
                    continue
                positions = await run_blocking(client.get_all_positions)
                positions_by_user[user_id] = positions or []
            except Exception:
                continue

    return funding_rates, positions_by_user


def set_bot_app(app):
    global _bot_app
    _bot_app = app


def set_check_client(client: NadoClient):
    global _check_client
    _check_client = client


async def check_alerts():
    global _bot_app, _check_client
    if not _bot_app or not _check_client:
        return

    try:
        await enqueue_alert({"ts": asyncio.get_running_loop().time()}, dedupe_key=f"scan:{int(asyncio.get_running_loop().time() / 5)}")
    except Exception as e:
        logger.error(f"Alert enqueue failed: {e}")


async def handle_alert_job(payload: dict):
    global _bot_app, _check_client
    if not _bot_app or not _check_client:
        return
    try:
        with timed_metric("scheduler.check_alerts.total"):
            prices = await _get_market_snapshot()
        if not prices:
            return
        funding_rates, positions_by_user = await _build_alert_context()
        triggered = await run_blocking(get_triggered_alerts, prices, funding_rates, positions_by_user)
        for alert in triggered:
            try:
                from src.nadobro.i18n import language_context, get_user_language, localize_text, get_active_language
                with language_context(get_user_language(alert["user_id"])):
                    lang = get_active_language()
                    condition_label = _alert_condition_label(alert.get("condition"))
                    target_fmt = _format_alert_metric_value(alert.get("condition"), alert.get("target", 0))
                    current_fmt = _format_alert_metric_value(
                        alert.get("condition"),
                        alert.get("current_value", alert.get("current_price", 0)),
                    )
                    msg = (
                        f"{localize_text('Alert Triggered!', lang)}\n"
                        f"{alert['product']} {localize_text('is', lang)} {condition_label}: {target_fmt}\n"
                        f"{localize_text('Current value:', lang)} {current_fmt}"
                    )
                await _bot_app.bot.send_message(chat_id=alert["user_id"], text=msg)
                logger.info(f"Alert sent to user {alert['user_id']}: {alert['product']} {alert['condition']}")
            except Exception as e:
                logger.error(f"Failed to send alert to {alert['user_id']}: {e}")
        sl_notifications = await run_blocking(process_stop_losses, prices)
        for note in sl_notifications:
            try:
                from src.nadobro.i18n import language_context, get_user_language, localize_text, get_active_language
                with language_context(get_user_language(note["user_id"])):
                    lang = get_active_language()
                    await _bot_app.bot.send_message(chat_id=note["user_id"], text=localize_text(note["text"], lang))
            except Exception as e:
                logger.error("Failed to send stop-loss notification to %s: %s", note.get("user_id"), e)
    except Exception as e:
        logger.error(f"Alert check failed: {e}")


async def tick_price_tracker():
    global _check_client
    if not _check_client:
        return
    try:
        from src.nadobro.services.price_tracker import record_prices_snapshot
        prices = await _get_market_snapshot()
        if prices:
            await run_blocking(record_prices_snapshot, prices)
    except Exception as e:
        logger.error("Price tracker tick failed: %s", e)


async def _get_market_snapshot(force_refresh: bool = False) -> dict:
    global _last_market_snapshot
    now = asyncio.get_running_loop().time()
    if (not force_refresh) and _last_market_snapshot.get("prices") and (
        now - float(_last_market_snapshot.get("ts") or 0.0) < _MARKET_SNAPSHOT_TTL_SECONDS
    ):
        return _last_market_snapshot.get("prices") or {}
    prices = await run_blocking(_check_client.get_all_market_prices)
    _last_market_snapshot = {"ts": now, "prices": prices or {}}
    return _last_market_snapshot["prices"]


async def tick_howl():
    global _bot_app
    if not _bot_app:
        return
    try:
        from src.nadobro.services.howl_service import run_howl_analysis, format_howl_message, get_pending_howl
        from src.nadobro.services.bot_runtime import _load_state
        from src.nadobro.db import query_all
        import json

        rows = await run_blocking(
            query_all,
            "SELECT key, value FROM bot_state WHERE key LIKE %s",
            ("strategy_bot:%",),
        )
        for row in rows:
            try:
                key = row.get("key", "")
                state = json.loads(row.get("value") or "{}")
                if not state.get("running") or state.get("strategy") != "bro":
                    continue
                if not state.get("bro_state", {}).get("started_at"):
                    continue

                settings = state.get("bro_state", {})
                if not bool(state.get("howl_enabled", True)):
                    continue

                user_network = key.replace("strategy_bot:", "")
                user_id_str, network = user_network.split(":", 1)
                telegram_id = int(user_id_str)

                existing = await run_blocking(get_pending_howl, telegram_id, network)
                if existing:
                    continue

                howl_data = await run_blocking(run_howl_analysis, telegram_id, network, state)
                if howl_data and howl_data.get("suggestions"):
                    msg = format_howl_message(howl_data)
                    from src.nadobro.handlers.keyboards import howl_approval_kb
                    from src.nadobro.i18n import language_context, get_user_language, localize_text, localize_markup, get_active_language
                    suggestions_count = len(howl_data.get("suggestions", []))
                    try:
                        with language_context(get_user_language(telegram_id)):
                            lang = get_active_language()
                            await _bot_app.bot.send_message(
                                chat_id=telegram_id,
                                text=localize_text(msg, lang),
                                reply_markup=localize_markup(howl_approval_kb(suggestions_count), lang),
                            )
                    except Exception as e:
                        logger.error("Failed to send HOWL to user %s: %s", telegram_id, e)
            except Exception as e:
                logger.debug("HOWL skip for row: %s", e)
    except Exception as e:
        logger.error("HOWL ticker failed: %s", e)


async def poll_lowiqpts_relay():
    global _bot_app
    if not _bot_app:
        return
    try:
        from src.nadobro.services.points_service import poll_lowiqpts_relay_events
        await poll_lowiqpts_relay_events(_bot_app)
    except Exception as e:
        logger.error("LOWIQPTS relay poll failed: %s", e)


def _is_partial_fill(requested_size: float, fill_size: float, epsilon: float = 1e-12) -> bool:
    if requested_size <= 0:
        return False
    if fill_size <= 0:
        return False
    return fill_size + epsilon < requested_size


async def _notify_limit_order_filled_once(trade_row: dict, network: str):
    global _bot_app
    if not _bot_app or not trade_row:
        return
    try:
        from src.nadobro.models.database import get_bot_state, set_bot_state

        trade_id = int(trade_row.get("id"))
        dedupe_key = f"limit_fill_notified:{network}:{trade_id}"
        if get_bot_state(dedupe_key):
            return

        user_id = int(trade_row.get("user_id"))
        side = str(trade_row.get("side") or "").upper()
        product = str(trade_row.get("product_name") or "")
        size = float(trade_row.get("size") or 0)
        fill_price = float(trade_row.get("fill_price") or trade_row.get("price") or 0)

        msg = (
            "✅ Limit order filled\n\n"
            "📋 Type: LIMIT\n"
            f"📌 Side: {side or '?'}\n"
            f"🪙 Product: {product or '?'}\n"
            f"📏 Size: {size:.4f}\n"
            f"💲 Fill price: ${fill_price:,.6f}\n"
            "📶 Status: fully filled\n"
            f"🌐 Network: {network}"
        )
        await _bot_app.bot.send_message(chat_id=user_id, text=msg)
        set_bot_state(dedupe_key, {"notified_at": datetime.now(timezone.utc).isoformat()})
    except Exception as e:
        logger.warning("Failed to send limit-fill notification: %s", e)


def _infer_cancel_source(trade_row: dict) -> str:
    source = str((trade_row or {}).get("source") or "").strip().lower()
    # Best-effort: manual/chat-driven trades are usually user-originated.
    if source in {"manual", "chat", "intent", "ui"}:
        return "user_initiated"
    return "system_detected"


async def _notify_limit_order_cancelled_once(
    trade_row: dict,
    network: str,
    cancel_source: str = "system_detected",
):
    global _bot_app
    if not _bot_app or not trade_row:
        return
    try:
        from src.nadobro.models.database import get_bot_state, set_bot_state

        trade_id = int(trade_row.get("id"))
        dedupe_key = f"limit_cancel_notified:{network}:{trade_id}"
        if get_bot_state(dedupe_key):
            return

        user_id = int(trade_row.get("user_id"))
        side = str(trade_row.get("side") or "").upper()
        product = str(trade_row.get("product_name") or "")
        size = float(trade_row.get("size") or 0)
        limit_price = float(trade_row.get("price") or 0)

        is_user = str(cancel_source or "").lower() == "user_initiated"
        headline = "🟠 Limit order cancelled by user" if is_user else "⚙️ Limit order cancelled (system-detected)"
        source_label = "user-initiated" if is_user else "system-detected"
        msg = (
            f"{headline}\n\n"
            "📋 Type: LIMIT\n"
            f"📌 Side: {side or '?'}\n"
            f"🪙 Product: {product or '?'}\n"
            f"📏 Size: {size:.4f}\n"
            f"💲 Limit price: ${limit_price:,.6f}\n"
            "📶 Status: cancelled\n"
            f"🧭 Cancel source: {source_label}\n"
            f"🌐 Network: {network}"
        )
        await _bot_app.bot.send_message(chat_id=user_id, text=msg)
        set_bot_state(dedupe_key, {"notified_at": datetime.now(timezone.utc).isoformat()})
    except Exception as e:
        logger.warning("Failed to send limit-cancel notification: %s", e)


async def sync_pending_fills():
    """Background job: resolve pending fills via Nado archive API."""
    try:
        from src.nadobro.models.database import (
            get_pending_fill_syncs, update_trade, resolve_fill_sync,
            expire_fill_sync, increment_fill_sync_attempts,
            increment_session_metrics,
            get_trade_by_id,
        )
        from src.nadobro.services.nado_archive import query_order_by_digest

        pending = await run_blocking(get_pending_fill_syncs, 50)
        if not pending:
            return

        resolved_count = 0
        for entry in pending:
            try:
                sync_id = entry["id"]
                trade_id = entry["trade_id"]
                network = entry["network"]
                digest = entry["order_digest"]
                attempts = int(entry.get("attempts", 0))
                trade_row = await run_blocking(get_trade_by_id, trade_id, network)

                # Expire old entries
                created = entry.get("created_at")
                if created and attempts >= 10:
                    from datetime import datetime as dt

                    if isinstance(created, str):
                        # Keep explicit timezone offsets (including trailing Z -> UTC).
                        created_dt = dt.fromisoformat(created.replace("Z", "+00:00"))
                    else:
                        created_dt = created

                    # Normalize both sides to aware UTC before comparing.
                    if created_dt.tzinfo is None:
                        created_dt = created_dt.replace(tzinfo=timezone.utc)
                    age_seconds = (dt.now(timezone.utc) - created_dt.astimezone(timezone.utc)).total_seconds()
                    if age_seconds > 7200:  # 2 hours
                        await run_blocking(expire_fill_sync, sync_id)
                        continue

                await run_blocking(increment_fill_sync_attempts, sync_id)

                fill_data = await run_blocking(
                    query_order_by_digest,
                    network,
                    digest,
                    _FILL_SYNC_DIGEST_WAIT,
                    _FILL_SYNC_DIGEST_POLL,
                )
                if not fill_data or not fill_data.get("is_filled"):
                    # Check if order was cancelled only after enough retries to avoid
                    # misclassifying late archive indexing as cancellation.
                    if attempts >= 8:
                        try:
                            from src.nadobro.services.user_service import get_user_nado_client
                            user_id = entry["user_id"]
                            product_id = entry["product_id"]
                            client = get_user_nado_client(int(user_id), network=network)
                            if client:
                                open_orders = client.get_open_orders(product_id, refresh=True) or []
                                digest_still_open = any(
                                    str(o.get("digest")) == digest for o in open_orders
                                )
                                if not digest_still_open:
                                    await run_blocking(
                                        update_trade, trade_id,
                                        {"status": "cancelled"}, network,
                                    )
                                    refreshed_trade = await run_blocking(get_trade_by_id, trade_id, network)
                                    trade_for_notify = refreshed_trade or trade_row
                                    await _notify_limit_order_cancelled_once(
                                        trade_for_notify,
                                        network,
                                        cancel_source=_infer_cancel_source(trade_for_notify or {}),
                                    )
                                    await run_blocking(resolve_fill_sync, sync_id)
                                    resolved_count += 1
                                    continue
                        except Exception:
                            pass
                    continue

                requested_size = abs(float((trade_row or {}).get("size") or 0))
                filled_size = abs(float(fill_data.get("fill_size") or 0))
                is_partial = _is_partial_fill(requested_size, filled_size)

                # Fill resolved — update trade. Keep queue pending for partial fills.
                update_data = {
                    "status": "partially_filled" if is_partial else "filled",
                    "fill_price": fill_data["fill_price"],
                    "price": fill_data["fill_price"],
                    "fill_size": fill_data.get("fill_size"),
                    "fill_fee": fill_data.get("fee", 0),
                    "fees": fill_data.get("fee", 0),
                    "realized_pnl": fill_data.get("realized_pnl", 0),
                    "is_taker": fill_data.get("is_taker"),
                }
                if fill_data.get("first_fill_ts"):
                    from datetime import datetime as dt
                    try:
                        update_data["filled_at"] = dt.utcfromtimestamp(
                            int(fill_data["first_fill_ts"])
                        ).isoformat()
                    except Exception:
                        pass

                await run_blocking(update_trade, trade_id, update_data, network)
                if is_partial:
                    digest_still_open = True
                    try:
                        from src.nadobro.services.user_service import get_user_nado_client
                        user_id = entry["user_id"]
                        product_id = entry["product_id"]
                        client = get_user_nado_client(int(user_id), network=network)
                        if client:
                            open_orders = client.get_open_orders(product_id, refresh=True) or []
                            digest_still_open = any(str(o.get("digest")) == digest for o in open_orders)
                    except Exception:
                        digest_still_open = True
                    if not digest_still_open:
                        await run_blocking(resolve_fill_sync, sync_id)
                        resolved_count += 1
                    logger.info(
                        "Fill sync partial trade #%s: filled=%.6f/%.6f price=%.6f",
                        trade_id,
                        filled_size,
                        requested_size,
                        fill_data["fill_price"],
                    )
                else:
                    await run_blocking(resolve_fill_sync, sync_id)
                    resolved_count += 1
                    refreshed_trade = await run_blocking(get_trade_by_id, trade_id, network)
                    await _notify_limit_order_filled_once(refreshed_trade or trade_row, network)

                if not is_partial:
                    logger.info(
                        "Fill sync resolved trade #%s: price=%.6f fee=%.6f pnl=%.6f",
                        trade_id, fill_data["fill_price"],
                        fill_data.get("fee", 0), fill_data.get("realized_pnl", 0),
                    )
            except Exception as e:
                logger.warning("Fill sync error for entry %s: %s", entry.get("id"), e)
                continue

        if resolved_count > 0:
            logger.info("Fill sync: resolved %d/%d pending fills", resolved_count, len(pending))
    except Exception as e:
        logger.error("Fill sync job failed: %s", e)


async def tick_edge_scanner():
    """Scan for trading edges, promotions, and multipliers on X."""
    try:
        from src.nadobro.services.edge_scanner import async_scan_edges
        await async_scan_edges()
    except Exception as e:
        logger.error("Edge scanner tick failed: %s", e)


async def initial_ai_setup():
    """Run initial AI setup: KB indexing + first edge scan."""
    try:
        from src.nadobro.services.edge_scanner import async_initial_scan
        await async_initial_scan()
    except Exception as e:
        logger.error("Initial AI setup failed: %s", e)


_EDGE_SCAN_SECONDS = int(os.environ.get("EDGE_SCAN_INTERVAL_SECONDS", "1800"))


def start_scheduler():
    relay_poll_seconds = relay_poll_interval_seconds()
    scheduler.add_job(check_alerts, "interval", seconds=_ALERT_SCAN_SECONDS, id="check_alerts", replace_existing=True)
    scheduler.add_job(tick_price_tracker, "interval", seconds=60, id="price_tracker", replace_existing=True)
    scheduler.add_job(tick_howl, "cron", hour=2, minute=0, id="howl_nightly", replace_existing=True)
    scheduler.add_job(poll_lowiqpts_relay, "interval", seconds=relay_poll_seconds, id="lowiqpts_relay_poll", replace_existing=True)
    scheduler.add_job(sync_pending_fills, "interval", seconds=30, id="fill_sync", replace_existing=True)
    scheduler.add_job(tick_edge_scanner, "interval", seconds=_EDGE_SCAN_SECONDS, id="edge_scanner", replace_existing=True)
    scheduler.add_job(initial_ai_setup, "date", id="initial_ai_setup", replace_existing=True)
    scheduler.start()
    logger.info(
        "Scheduler started - alerts %ss, price tracker 60s, HOWL nightly 02:00 UTC, LOWIQ relay %ss, fill sync 30s, edge scanner %ss",
        _ALERT_SCAN_SECONDS,
        relay_poll_seconds,
        _EDGE_SCAN_SECONDS,
    )


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


def get_scheduler_diagnostics() -> dict:
    try:
        jobs = scheduler.get_jobs() if scheduler.running else []
    except Exception:
        jobs = []
    return {
        "running": bool(scheduler.running),
        "job_count": len(jobs),
        "alert_scan_seconds": int(_ALERT_SCAN_SECONDS),
    }
