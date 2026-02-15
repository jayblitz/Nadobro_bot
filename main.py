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

    from telegram import BotCommand
    await bot_app.bot.set_my_commands([
        BotCommand("start", "Open dashboard"),
        BotCommand("help", "Show help"),
        BotCommand("status", "Bot & strategy status"),
        BotCommand("import_key", "Import trading key"),
        BotCommand("stop_all", "Stop bot & cancel orders"),
    ])
    logger.info("Bot commands registered in Menu")

    await bot_app.updater.start_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "callback_query"],
    )

    logger.info("Nadobro is live! Pure bot mode running.")

    from aiohttp import web as aio_web
    webhook_app = aio_web.Application()

    webhook_secret = os.environ.get("WEBHOOK_SECRET", "")

    async def handle_webhook(request):
        if webhook_secret:
            provided = request.headers.get("X-Webhook-Secret", "")
            if provided != webhook_secret:
                return aio_web.json_response({"error": "unauthorized"}, status=401)

        try:
            body = await request.json()
        except Exception:
            return aio_web.json_response({"error": "invalid JSON"}, status=400)

        action = body.get("action", "").lower().strip()
        raw_price = body.get("price")
        telegram_id = body.get("telegram_id")

        if raw_price is not None:
            try:
                price = float(raw_price)
                if price < 0:
                    return aio_web.json_response({"error": "price must be non-negative"}, status=400)
            except (ValueError, TypeError):
                return aio_web.json_response({"error": "invalid price value"}, status=400)
        else:
            price = 0.0

        if not action or action not in ("long", "short", "neutral"):
            return aio_web.json_response({"error": "action must be long/short/neutral"}, status=400)
        if not telegram_id:
            return aio_web.json_response({"error": "telegram_id required"}, status=400)

        try:
            telegram_id = int(telegram_id)
        except (ValueError, TypeError):
            return aio_web.json_response({"error": "invalid telegram_id"}, status=400)

        from src.nadobro.services.whale_strategy import get_whale_strategy

        ws = get_whale_strategy(telegram_id)
        if not ws:
            return aio_web.json_response({"error": "no wallet configured for this user"}, status=404)

        status = ws.get_status()
        if not status.get("active"):
            return aio_web.json_response({"error": "whale strategy not active"}, status=400)

        if price <= 0:
            try:
                mp = ws.client.get_market_price(ws._get_perp_product_id())
                price = float(mp.get("mid", 0) or 0)
            except Exception:
                pass
        if price <= 0:
            return aio_web.json_response({"error": "could not determine price"}, status=400)

        result = ws.process_signal(action, price)

        try:
            from src.nadobro.handlers.formatters import escape_md
            await bot_app.bot.send_message(
                chat_id=telegram_id,
                text=f"🐋 {escape_md(result)}",
                parse_mode="MarkdownV2",
            )
        except Exception as notify_err:
            logger.warning("Webhook notification failed for %s: %s", telegram_id, notify_err)

        return aio_web.json_response({"ok": True, "action": action, "price": price})

    async def handle_health(request):
        return aio_web.json_response({"status": "ok", "service": "nadobro"})

    webhook_app.router.add_post("/webhook", handle_webhook)
    webhook_app.router.add_get("/health", handle_health)

    runner = aio_web.AppRunner(webhook_app)
    await runner.setup()
    webhook_site = aio_web.TCPSite(runner, "0.0.0.0", 8099)
    await webhook_site.start()
    logger.info("Webhook server started on port 8099")

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
        await runner.cleanup()
        await bot_app.updater.stop()
        await bot_app.stop()
        await bot_app.shutdown()
        logger.info("Bot stopped cleanly")


if __name__ == "__main__":
    asyncio.run(run_bot())
