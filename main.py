import os
import sys
import logging
import asyncio
import signal
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("nadobro")

# Load local .env during development before reading config vars.
load_dotenv()

from src.nadobro.config import TELEGRAM_TOKEN, ENCRYPTION_KEY, DATABASE_URL
from src.nadobro.services.crypto import validate_encryption_key
from src.nadobro.services.debug_logger import debug_log

if not ENCRYPTION_KEY:
    # region agent log
    debug_log(
        "baseline",
        "H7",
        "main.py:24",
        "missing_encryption_key",
        {"has_telegram_token": bool(TELEGRAM_TOKEN), "has_database_url": bool(DATABASE_URL)},
    )
    # endregion
    logger.error(
        "ENCRYPTION_KEY is required for wallet encryption. "
        "Please set ENCRYPTION_KEY in environment variables or a local .env file."
    )
    sys.exit(1)

try:
    validate_encryption_key()
    logger.info("Encryption key validated successfully")
except RuntimeError as e:
    # region agent log
    debug_log(
        "baseline",
        "H7",
        "main.py:40",
        "invalid_encryption_key",
        {"error": str(e)},
    )
    # endregion
    logger.error(str(e))
    sys.exit(1)


def check_config():
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    if not DATABASE_URL:
        missing.append("DATABASE_URL")
    xai_key = os.environ.get("XAI_API_KEY")
    openai_key = os.environ.get("OPENAI_API_KEY")
    if not xai_key and not openai_key:
        logger.warning(
            "No AI key set (XAI_API_KEY/OPENAI_API_KEY) - support AI features will be unavailable"
        )
    elif not xai_key:
        logger.info("XAI_API_KEY not set - OpenAI-only support mode enabled")
    elif not openai_key:
        logger.info("OPENAI_API_KEY not set - xAI-only support mode enabled")

    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    logger.info("Configuration check passed")


def setup_bot():
    from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters

    from src.nadobro.handlers.commands import cmd_start, cmd_help, cmd_status, cmd_stop_all, cmd_import_key
    from src.nadobro.handlers.messages import handle_message
    from src.nadobro.handlers.callbacks import handle_callback

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("stop_all", cmd_stop_all))
    app.add_handler(CommandHandler("import_key", cmd_import_key))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    # region agent log
    debug_log(
        "baseline",
        "H6",
        "main.py:75",
        "handlers_registered",
        {"has_callback_handler": True, "has_message_handler": True},
    )
    # endregion

    logger.info("Bot handlers registered (pure bot mode)")
    return app


async def run_bot():
    # region agent log
    debug_log(
        "baseline",
        "H6",
        "main.py:87",
        "run_bot_started",
        {"pid": os.getpid()},
    )
    # endregion
    check_config()
    from src.nadobro.models.database import init_db

    logger.info("Initializing database...")
    init_db()
    logger.info("Database initialized")

    logger.info("Setting up Telegram bot...")
    bot_app = setup_bot()

    from src.nadobro.services.scheduler import set_bot_app, set_check_client, start_scheduler
    from src.nadobro.services.bot_runtime import (
        set_bot_app as set_runtime_app,
        restore_running_bots,
        stop_runtime,
    )
    set_bot_app(bot_app)
    set_runtime_app(bot_app)

    try:
        from src.nadobro.services.nado_client import NadoClient
        alert_client = NadoClient("0x0000000000000000000000000000000000000000000000000000000000000001", "testnet")
        alert_client.initialize()
        set_check_client(alert_client)
        logger.info("Alert price-check client initialized")
    except Exception as e:
        logger.warning(f"Alert price-check client failed to initialize: {e}")

    start_scheduler()
    restore_running_bots()

    logger.info("Starting bot with polling...")
    # region agent log
    debug_log(
        "baseline",
        "H6",
        "main.py:121",
        "starting_polling",
        {"allowed_updates": ["message", "callback_query"]},
    )
    # endregion
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "callback_query"],
    )

    logger.info("Nadobro is live! Pure bot mode running.")

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
        stop_runtime()
        await bot_app.updater.stop()
        await bot_app.stop()
        await bot_app.shutdown()
        logger.info("Bot stopped cleanly")


if __name__ == "__main__":
    asyncio.run(run_bot())
