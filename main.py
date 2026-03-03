import os
import sys
import logging
import asyncio
import signal
from urllib.parse import urlparse
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

if not ENCRYPTION_KEY:
    logger.error(
        "ENCRYPTION_KEY is required for wallet encryption. "
        "Please set ENCRYPTION_KEY in environment variables or a local .env file."
    )
    sys.exit(1)

try:
    validate_encryption_key()
    logger.info("Encryption key validated successfully")
except RuntimeError as e:
    logger.error(str(e))
    sys.exit(1)


def check_config():
    transport_mode = os.environ.get("TELEGRAM_TRANSPORT", "polling").strip().lower()
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    if not DATABASE_URL and not os.environ.get("SUPABASE_DATABASE_URL"):
        missing.append("DATABASE_URL or SUPABASE_DATABASE_URL")
    if transport_mode not in ("polling", "webhook"):
        missing.append("TELEGRAM_TRANSPORT must be polling or webhook")
    if transport_mode == "webhook" and not os.environ.get("TELEGRAM_WEBHOOK_URL"):
        missing.append("TELEGRAM_WEBHOOK_URL (required when TELEGRAM_TRANSPORT=webhook)")
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

    logger.info("Configuration check passed (transport=%s)", transport_mode)


def setup_bot():
    from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters

    from src.nadobro.handlers.commands import cmd_start, cmd_help, cmd_status, cmd_stop_all, cmd_revoke
    from src.nadobro.handlers.messages import handle_message
    from src.nadobro.handlers.callbacks import handle_callback

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("stop_all", cmd_stop_all))
    app.add_handler(CommandHandler("revoke", cmd_revoke))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Bot handlers registered (pure bot mode)")
    return app


async def run_bot():
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
        handle_strategy_job,
    )
    from src.nadobro.services.scheduler import handle_alert_job
    from src.nadobro.services.execution_queue import register_handlers, start_workers, stop_workers
    set_bot_app(bot_app)
    set_runtime_app(bot_app)
    register_handlers(handle_strategy_job, handle_alert_job)
    start_workers(
        strategy_workers=int(os.environ.get("NADO_STRATEGY_WORKERS", "2")),
        alert_workers=int(os.environ.get("NADO_ALERT_WORKERS", "1")),
    )

    try:
        from src.nadobro.services.nado_client import NadoClient
        alert_client = NadoClient("0x0000000000000000000000000000000000000000000000000000000000000001", "testnet")
        alert_client.initialize()
        set_check_client(alert_client)
        logger.info("Alert price-check client initialized")
    except Exception as e:
        logger.warning(f"Alert price-check client failed to initialize: {e}")

    start_scheduler()
    # In production we want strategy loops to resume after restarts/deploys by default.
    auto_restore = os.environ.get("NADO_AUTO_RESTORE_STRATEGIES", "true").strip().lower() in ("1", "true", "yes", "on")
    restore_running_bots(enabled=auto_restore)

    transport_mode = os.environ.get("TELEGRAM_TRANSPORT", "polling").strip().lower()
    webhook_url = (os.environ.get("TELEGRAM_WEBHOOK_URL") or "").strip()
    webhook_path = (os.environ.get("TELEGRAM_WEBHOOK_PATH") or "/telegram/webhook").strip()
    if not webhook_path.startswith("/"):
        webhook_path = "/" + webhook_path
    webhook_secret = (os.environ.get("TELEGRAM_WEBHOOK_SECRET") or "").strip()
    if transport_mode == "webhook" and webhook_url:
        parsed = urlparse(webhook_url)
        if parsed.path.rstrip("/") != webhook_path.rstrip("/"):
            base = f"{parsed.scheme}://{parsed.netloc}"
            webhook_url = base + webhook_path

    webhook_listen = os.environ.get("TELEGRAM_WEBHOOK_LISTEN", "0.0.0.0").strip()
    webhook_port = int(os.environ.get("PORT", os.environ.get("TELEGRAM_WEBHOOK_PORT", "8080")))

    logger.info("Starting bot (transport=%s)...", transport_mode)
    await bot_app.initialize()
    await bot_app.start()

    from telegram import BotCommand
    await bot_app.bot.set_my_commands([
        BotCommand("start", "Open dashboard"),
        BotCommand("help", "Show help"),
        BotCommand("status", "Bot & strategy status"),
        BotCommand("revoke", "Revoke linked signer"),
        BotCommand("stop_all", "Stop strategy runtime"),
    ])
    logger.info("Bot commands registered in Menu")

    if transport_mode == "webhook":
        logger.info(
            "Starting webhook server listen=%s port=%s path=%s",
            webhook_listen,
            webhook_port,
            webhook_path,
        )
        await bot_app.updater.start_webhook(
            listen=webhook_listen,
            port=webhook_port,
            url_path=webhook_path.lstrip("/"),
            webhook_url=webhook_url,
            drop_pending_updates=True,
            allowed_updates=["message", "callback_query"],
            secret_token=webhook_secret or None,
        )
        logger.info("Nadobro is live in webhook mode.")
    else:
        await bot_app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=["message", "callback_query"],
        )
        logger.info("Nadobro is live in polling mode.")

        # Polling mode only: lightweight TCP health responder on PORT.
        port_str = os.environ.get("PORT")
        if port_str:
            try:
                port = int(port_str)

                async def _health_handler(reader, writer):
                    try:
                        await reader.read(4096)
                        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
                        await writer.drain()
                    finally:
                        writer.close()
                        await writer.wait_closed()

                health_server = await asyncio.start_server(_health_handler, "0.0.0.0", port)

                async def _serve_health():
                    async with health_server:
                        await asyncio.Future()  # run until cancelled

                asyncio.create_task(_serve_health())
                logger.info("Health check listening on port %s", port)
            except Exception as e:
                logger.warning("Health server failed (non-fatal): %s", e)

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
        stop_workers()
        await bot_app.updater.stop()
        await bot_app.stop()
        await bot_app.shutdown()
        logger.info("Bot stopped cleanly")


if __name__ == "__main__":
    asyncio.run(run_bot())
