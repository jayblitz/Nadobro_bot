import os
import sys
import logging
import asyncio
import signal
import threading

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
        "Please set ENCRYPTION_KEY in your Replit Secrets tab."
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
        sys.exit(1)

    logger.info("Configuration check passed")


def setup_bot():
    from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters
    from telegram import WebAppInfo, MenuButtonWebApp

    from src.nadobro.handlers.commands import cmd_start, cmd_help
    from src.nadobro.handlers.messages import handle_message
    from src.nadobro.handlers.callbacks import handle_callback

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Bot handlers registered (Mini App mode)")
    return app


def run_flask():
    from src.nadobro.api import app as flask_app
    flask_app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)


async def run_bot():
    check_config()

    logger.info("Initializing database...")
    init_db()
    logger.info("Database initialized")

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask web server started on port 5000")

    logger.info("Setting up Telegram bot...")
    bot_app = setup_bot()

    from src.nadobro.services.scheduler import set_bot_app, set_check_client, start_scheduler
    set_bot_app(bot_app)

    try:
        from src.nadobro.services.nado_client import NadoClient
        alert_client = NadoClient("0x0000000000000000000000000000000000000000000000000000000000000001", "testnet")
        alert_client.initialize()
        set_check_client(alert_client)
        logger.info("Alert price-check client initialized")
    except Exception as e:
        logger.warning(f"Alert price-check client failed to initialize: {e}")

    start_scheduler()

    logger.info("Starting bot with polling...")
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "callback_query"],
    )

    logger.info("Nadobro is live! Mini App + Bot running.")

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
        await bot_app.updater.stop()
        await bot_app.stop()
        await bot_app.shutdown()
        logger.info("Bot stopped cleanly")


if __name__ == "__main__":
    asyncio.run(run_bot())
