import os
import sys
import logging
import asyncio
import signal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("nadobro")

from src.nadobro.config import TELEGRAM_TOKEN, ENCRYPTION_KEY
from src.nadobro.models.database import init_db
from src.nadobro.services.crypto import validate_encryption_key

if not ENCRYPTION_KEY:
    logger.error(
        "ENCRYPTION_KEY is required for wallet encryption. "
        "Please set ENCRYPTION_KEY in your Replit Secrets tab. "
        "Without a persistent key, encrypted wallets cannot be recovered after restart."
    )
    sys.exit(1)

try:
    validate_encryption_key()
    logger.info("Encryption key validated successfully")
except RuntimeError as e:
    logger.error(str(e))
    sys.exit(1)


def check_config():
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    xai_key = os.environ.get("XAI_API_KEY")
    if not xai_key:
        logger.warning("XAI_API_KEY not set - AI features will use fallback keyword matching")

    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        logger.error("Please set these in your Replit Secrets tab.")
        sys.exit(1)

    logger.info("Configuration check passed")


def setup_bot():
    from telegram.ext import Application, CommandHandler, MessageHandler, filters

    from src.nadobro.handlers.commands import (
        cmd_start, cmd_help, cmd_long, cmd_short,
        cmd_limit_long, cmd_limit_short, cmd_tp, cmd_sl,
        cmd_close, cmd_close_all,
        cmd_positions, cmd_balance, cmd_price, cmd_funding,
        cmd_history, cmd_analytics,
        cmd_wallet, cmd_mode, cmd_recover,
        cmd_alert, cmd_my_alerts, cmd_del_alert,
        cmd_admin_stats, cmd_admin_pause, cmd_admin_logs,
    )
    from src.nadobro.handlers.messages import handle_message

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("long", cmd_long))
    app.add_handler(CommandHandler("short", cmd_short))
    app.add_handler(CommandHandler("limit_long", cmd_limit_long))
    app.add_handler(CommandHandler("limit_short", cmd_limit_short))
    app.add_handler(CommandHandler("tp", cmd_tp))
    app.add_handler(CommandHandler("sl", cmd_sl))
    app.add_handler(CommandHandler("close", cmd_close))
    app.add_handler(CommandHandler("close_all", cmd_close_all))
    app.add_handler(CommandHandler("positions", cmd_positions))
    app.add_handler(CommandHandler("balance", cmd_balance))
    app.add_handler(CommandHandler("price", cmd_price))
    app.add_handler(CommandHandler("funding", cmd_funding))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("analytics", cmd_analytics))
    app.add_handler(CommandHandler("wallet", cmd_wallet))
    app.add_handler(CommandHandler("mode", cmd_mode))
    app.add_handler(CommandHandler("recover", cmd_recover))
    app.add_handler(CommandHandler("alert", cmd_alert))
    app.add_handler(CommandHandler("my_alerts", cmd_my_alerts))
    app.add_handler(CommandHandler("del_alert", cmd_del_alert))
    app.add_handler(CommandHandler("admin_stats", cmd_admin_stats))
    app.add_handler(CommandHandler("admin_pause", cmd_admin_pause))
    app.add_handler(CommandHandler("admin_logs", cmd_admin_logs))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Bot handlers registered")
    return app


async def run_bot():
    check_config()

    logger.info("Initializing database...")
    init_db()
    logger.info("Database initialized")

    logger.info("Setting up Telegram bot...")
    app = setup_bot()

    from src.nadobro.services.scheduler import set_bot_app, set_check_client, start_scheduler
    set_bot_app(app)

    try:
        from src.nadobro.services.nado_client import NadoClient
        alert_client = NadoClient("0x0000000000000000000000000000000000000000000000000000000000000001", "testnet")
        alert_client.initialize()
        set_check_client(alert_client)
        logger.info("Alert price-check client initialized")
    except Exception as e:
        logger.warning(f"Alert price-check client failed to initialize (alerts will be limited): {e}")

    from src.nadobro.config import ADMIN_USER_IDS
    if not ADMIN_USER_IDS:
        logger.warning("ADMIN_USER_IDS not set - admin commands will be inaccessible")

    start_scheduler()

    logger.info("Starting bot with polling...")
    await app.initialize()
    await app.start()
    await app.updater.start_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "callback_query"],
    )

    logger.info("Nadobro bot is live! Waiting for messages...")

    stop_event = asyncio.Event()

    def handle_signal(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        await stop_event.wait()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        logger.info("Shutting down...")
        from src.nadobro.services.scheduler import stop_scheduler
        stop_scheduler()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        logger.info("Bot stopped cleanly")


if __name__ == "__main__":
    asyncio.run(run_bot())
