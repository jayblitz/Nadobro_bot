import logging
import asyncio
import os
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
_MARKET_SNAPSHOT_TTL_SECONDS = float(os.environ.get("NADO_MARKET_SNAPSHOT_TTL_SECONDS", "3.0"))
_last_market_snapshot: dict = {"ts": 0.0, "prices": {}}


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
        triggered = await run_blocking(get_triggered_alerts, prices)
        for alert in triggered:
            try:
                from src.nadobro.i18n import language_context, get_user_language, localize_text, get_active_language
                with language_context(get_user_language(alert["user_id"])):
                    lang = get_active_language()
                    msg = (
                        f"{localize_text('Alert Triggered!', lang)}\n"
                        f"{alert['product']} {localize_text('is', lang)} {alert['condition']} ${alert['target']:,.2f}\n"
                        f"{localize_text('Current price:', lang)} ${alert['current_price']:,.2f}"
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


async def sync_pending_fills():
    """Background job: resolve pending fills via Nado archive API."""
    try:
        from src.nadobro.models.database import (
            get_pending_fill_syncs, update_trade, resolve_fill_sync,
            expire_fill_sync, increment_fill_sync_attempts,
            increment_session_metrics,
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

                # Expire old entries
                created = entry.get("created_at")
                if created and attempts >= 10:
                    from datetime import datetime as dt
                    if isinstance(created, str):
                        age_seconds = (dt.utcnow() - dt.fromisoformat(created.replace("Z", "+00:00").replace("+00:00", ""))).total_seconds()
                    else:
                        age_seconds = (dt.utcnow() - created).total_seconds()
                    if age_seconds > 7200:  # 2 hours
                        await run_blocking(expire_fill_sync, sync_id)
                        continue

                await run_blocking(increment_fill_sync_attempts, sync_id)

                fill_data = await run_blocking(
                    query_order_by_digest, network, digest, 0.5, 0.3,
                )
                if not fill_data or not fill_data.get("is_filled"):
                    # Check if order was cancelled (no fill, high attempts)
                    if attempts >= 5:
                        try:
                            from src.nadobro.services.user_service import get_user_nado_client
                            user_id = entry["user_id"]
                            product_id = entry["product_id"]
                            client = get_user_nado_client(int(user_id), network=network)
                            if client:
                                open_orders = client.get_open_orders(product_id) or []
                                digest_still_open = any(
                                    str(o.get("digest")) == digest for o in open_orders
                                )
                                if not digest_still_open:
                                    await run_blocking(
                                        update_trade, trade_id,
                                        {"status": "cancelled"}, network,
                                    )
                                    await run_blocking(resolve_fill_sync, sync_id)
                                    resolved_count += 1
                                    continue
                        except Exception:
                            pass
                    continue

                # Fill resolved — update trade
                update_data = {
                    "status": "filled",
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
                await run_blocking(resolve_fill_sync, sync_id)
                resolved_count += 1

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


def start_scheduler():
    relay_poll_seconds = relay_poll_interval_seconds()
    scheduler.add_job(check_alerts, "interval", seconds=_ALERT_SCAN_SECONDS, id="check_alerts", replace_existing=True)
    scheduler.add_job(tick_price_tracker, "interval", seconds=60, id="price_tracker", replace_existing=True)
    scheduler.add_job(tick_howl, "cron", hour=2, minute=0, id="howl_nightly", replace_existing=True)
    scheduler.add_job(poll_lowiqpts_relay, "interval", seconds=relay_poll_seconds, id="lowiqpts_relay_poll", replace_existing=True)
    scheduler.add_job(sync_pending_fills, "interval", seconds=30, id="fill_sync", replace_existing=True)
    scheduler.start()
    logger.info(
        "Scheduler started - alerts %ss, price tracker 60s, HOWL nightly 02:00 UTC, LOWIQ relay %ss, fill sync 30s",
        _ALERT_SCAN_SECONDS,
        relay_poll_seconds,
    )


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
