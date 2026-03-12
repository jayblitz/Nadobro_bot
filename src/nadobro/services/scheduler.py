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

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone="UTC")
_bot_app = None
_check_client = None
_ALERT_SCAN_SECONDS = int(os.environ.get("ALERT_SCAN_SECONDS", "5"))


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
            prices = await run_blocking(_check_client.get_all_market_prices)
        if not prices:
            return
        triggered = await run_blocking(get_triggered_alerts, prices)
        for alert in triggered:
            try:
                msg = (
                    f"Alert Triggered!\n"
                    f"{alert['product']} is {alert['condition']} ${alert['target']:,.2f}\n"
                    f"Current price: ${alert['current_price']:,.2f}"
                )
                await _bot_app.bot.send_message(chat_id=alert["user_id"], text=msg)
                logger.info(f"Alert sent to user {alert['user_id']}: {alert['product']} {alert['condition']}")
            except Exception as e:
                logger.error(f"Failed to send alert to {alert['user_id']}: {e}")
        sl_notifications = await run_blocking(process_stop_losses, prices)
        for note in sl_notifications:
            try:
                await _bot_app.bot.send_message(chat_id=note["user_id"], text=note["text"])
            except Exception as e:
                logger.error("Failed to send stop-loss notification to %s: %s", note.get("user_id"), e)
    except Exception as e:
        logger.error(f"Alert check failed: {e}")


async def tick_price_tracker():
    global _check_client
    if not _check_client:
        return
    try:
        from src.nadobro.services.price_tracker import record_prices_from_client
        await run_blocking(record_prices_from_client, _check_client)
    except Exception as e:
        logger.error("Price tracker tick failed: %s", e)


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
                    suggestions_count = len(howl_data.get("suggestions", []))
                    try:
                        await _bot_app.bot.send_message(
                            chat_id=telegram_id,
                            text=msg,
                            reply_markup=howl_approval_kb(suggestions_count),
                        )
                    except Exception as e:
                        logger.error("Failed to send HOWL to user %s: %s", telegram_id, e)
            except Exception as e:
                logger.debug("HOWL skip for row: %s", e)
    except Exception as e:
        logger.error("HOWL ticker failed: %s", e)


def start_scheduler():
    scheduler.add_job(check_alerts, "interval", seconds=_ALERT_SCAN_SECONDS, id="check_alerts", replace_existing=True)
    scheduler.add_job(tick_price_tracker, "interval", seconds=60, id="price_tracker", replace_existing=True)
    scheduler.add_job(tick_howl, "cron", hour=2, minute=0, id="howl_nightly", replace_existing=True)
    scheduler.start()
    logger.info("Scheduler started - alerts %ss, price tracker 60s, HOWL nightly 02:00 UTC", _ALERT_SCAN_SECONDS)


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
