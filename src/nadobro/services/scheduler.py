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

scheduler = AsyncIOScheduler()
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


def start_scheduler():
    scheduler.add_job(check_alerts, "interval", seconds=_ALERT_SCAN_SECONDS, id="check_alerts", replace_existing=True)
    scheduler.start()
    logger.info("Scheduler started - checking alerts every %ss", _ALERT_SCAN_SECONDS)


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
