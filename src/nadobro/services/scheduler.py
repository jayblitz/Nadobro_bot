import logging
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from src.nadobro.services.alert_service import get_triggered_alerts
from src.nadobro.services.nado_client import NadoClient

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()
_bot_app = None
_check_client = None


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
        prices = _check_client.get_all_market_prices()
        if not prices:
            return

        triggered = get_triggered_alerts(prices)
        for alert in triggered:
            try:
                msg = (
                    f"Alert Triggered!\n"
                    f"{alert['product']} is {alert['condition']} ${alert['target']:,.2f}\n"
                    f"Current price: ${alert['current_price']:,.2f}"
                )
                await _bot_app.bot.send_message(
                    chat_id=alert["user_id"],
                    text=msg,
                )
                logger.info(f"Alert sent to user {alert['user_id']}: {alert['product']} {alert['condition']}")
            except Exception as e:
                logger.error(f"Failed to send alert to {alert['user_id']}: {e}")
    except Exception as e:
        logger.error(f"Alert check failed: {e}")


def start_scheduler():
    scheduler.add_job(check_alerts, "interval", seconds=30, id="check_alerts", replace_existing=True)
    scheduler.start()
    logger.info("Scheduler started - checking alerts every 30s")


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
