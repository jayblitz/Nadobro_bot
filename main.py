import os
import re
import sys
import logging
import asyncio
import signal
import json
import time
from urllib.parse import urlparse
from dotenv import load_dotenv


class _TokenRedactFilter(logging.Filter):
    _BOT_TOKEN_RE = re.compile(r"/bot\d+:[A-Za-z0-9_-]+/")
    _ADDR_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")

    def _redact(self, val):
        # Keep original arg types whenever possible so %-style log formatting
        # (e.g. `%d`) keeps working for numeric arguments.
        if isinstance(val, str):
            s = val
            if self._BOT_TOKEN_RE.search(s):
                s = self._BOT_TOKEN_RE.sub("/bot<REDACTED>/", s)
            if self._ADDR_RE.search(s):
                s = self._ADDR_RE.sub("0x<REDACTED_ADDR>", s)
            return s

        s = str(val)
        redacted = s
        if self._BOT_TOKEN_RE.search(redacted):
            redacted = self._BOT_TOKEN_RE.sub("/bot<REDACTED>/", redacted)
        if self._ADDR_RE.search(redacted):
            redacted = self._ADDR_RE.sub("0x<REDACTED_ADDR>", redacted)
        return redacted if redacted != s else val

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = self._BOT_TOKEN_RE.sub("/bot<REDACTED>/", record.msg)
            record.msg = self._ADDR_RE.sub("0x<REDACTED_ADDR>", record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {k: self._redact(v) for k, v in record.args.items()}
            elif isinstance(record.args, tuple):
                record.args = tuple(self._redact(a) for a in record.args)
        return True


_redact_filter = _TokenRedactFilter()
_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.addFilter(_redact_filter)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[_stream_handler],
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
    transport_mode, webhook_url, webhook_path = _resolve_transport_settings()
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    if not DATABASE_URL and not os.environ.get("SUPABASE_DATABASE_URL"):
        missing.append("DATABASE_URL or SUPABASE_DATABASE_URL")
    if transport_mode not in ("polling", "webhook"):
        missing.append("TELEGRAM_TRANSPORT must be polling or webhook")
    if transport_mode == "webhook" and not webhook_url:
        missing.append("TELEGRAM_WEBHOOK_URL (required when TELEGRAM_TRANSPORT=webhook)")
    data_env = (os.environ.get("DATA_ENV") or "").strip()
    if data_env and data_env not in ("nadoMainnet", "nadoTestnet"):
        missing.append("DATA_ENV must be nadoMainnet or nadoTestnet when set")
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

    if data_env == "nadoMainnet":
        require_linked = os.environ.get("NADO_REQUIRE_MAINNET_LINKED_SIGNER", "true").strip().lower() in ("1", "true", "yes", "on")
        if require_linked:
            logger.info("Mainnet guardrail enabled: linked signer required for active wallet readiness checks.")
    if (os.environ.get("NADO_TOOLING_ENABLE", "true").strip().lower() in ("1", "true", "yes", "on")):
        logger.info("Nado tooling adapter enabled (SDK writes remain primary).")

    logger.info("Configuration check passed (transport=%s)", transport_mode)


def _resolve_transport_settings():
    transport_mode = (os.environ.get("TELEGRAM_TRANSPORT") or "").strip().lower()
    webhook_path = (os.environ.get("TELEGRAM_WEBHOOK_PATH") or "/telegram/webhook").strip()
    if not webhook_path.startswith("/"):
        webhook_path = "/" + webhook_path
    webhook_url = (os.environ.get("TELEGRAM_WEBHOOK_URL") or "").strip()

    if not transport_mode:
        # Auto-prefer webhook on Fly deployments to reduce update polling latency.
        if os.environ.get("FLY_APP_NAME"):
            transport_mode = "webhook"
        elif webhook_url:
            transport_mode = "webhook"
        else:
            transport_mode = "polling"
    if transport_mode not in ("polling", "webhook"):
        transport_mode = "polling"

    if transport_mode == "webhook" and not webhook_url:
        fly_app = os.environ.get("FLY_APP_NAME", "").strip()
        if fly_app:
            webhook_url = f"https://{fly_app}.fly.dev{webhook_path}"

    return transport_mode, webhook_url, webhook_path


async def _start_bootstrap_health_server(port: int):
    """Serve a minimal temporary health endpoint during webhook startup."""
    async def _bootstrap_health_handler(reader, writer):
        try:
            await reader.read(4096)
            body = json.dumps({"status": "booting", "ts": time.time()}).encode("utf-8")
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: application/json\r\n"
                + f"Content-Length: {len(body)}\r\n\r\n".encode("utf-8")
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    return await asyncio.start_server(_bootstrap_health_handler, "0.0.0.0", port)


def _runtime_health_payload() -> dict:
    payload = {"status": "ok", "ts": time.time()}
    try:
        from src.nadobro.services.execution_queue import get_queue_diagnostics
        from src.nadobro.services.runtime_supervisor import get_runtime_supervisor_diagnostics
        from src.nadobro.services.copy_service import get_copy_polling_diagnostics
        from src.nadobro.services.scheduler import get_scheduler_diagnostics
        from src.nadobro.services.perf import summary_lines

        queue_diag = get_queue_diagnostics()
        scheduler_diag = get_scheduler_diagnostics()
        payload.update(
            {
                "queue": queue_diag,
                "scheduler": scheduler_diag,
                "runtime_supervisor": get_runtime_supervisor_diagnostics(),
                "copy_polling": get_copy_polling_diagnostics(),
                "perf_top": summary_lines(top_n=5),
            }
        )
        if int(queue_diag.get("strategy_qsize") or 0) >= int(queue_diag.get("strategy_qmax") or 0):
            payload["status"] = "degraded"
        if not scheduler_diag.get("running"):
            payload["status"] = "degraded"
    except Exception as e:
        payload["status"] = "degraded"
        payload["health_error"] = str(e)
    return payload


def setup_bot():
    from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, TypeHandler, filters
    from telegram import Update

    from src.nadobro.handlers.commands import cmd_start, cmd_help, cmd_status, cmd_ops, cmd_stop_all, cmd_revoke
    from src.nadobro.handlers.messages import handle_message
    from src.nadobro.handlers.callbacks import handle_callback

    async def _language_middleware(update: Update, context):
        user = update.effective_user
        if user:
            from src.nadobro.i18n import _ACTIVE_LANG, get_user_language, normalize_lang
            lang = normalize_lang(get_user_language(user.id))
            _ACTIVE_LANG.set(lang)

    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .concurrent_updates(True)
        .build()
    )

    app.add_handler(TypeHandler(Update, _language_middleware), group=-1)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("ops", cmd_ops))
    app.add_handler(CommandHandler("stop_all", cmd_stop_all))
    app.add_handler(CommandHandler("revoke", cmd_revoke))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Bot handlers registered (pure bot mode, language middleware active)")
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
    from src.nadobro.services.runtime_supervisor import (
        start_runtime_supervisor,
        stop_runtime_supervisor,
    )
    from src.nadobro.services.scheduler import handle_alert_job
    from src.nadobro.services.execution_queue import register_handlers, start_workers, stop_workers
    from src.nadobro.services.copy_service import set_copy_bot_app
    from src.nadobro.services.copy_service import start_copy_polling, stop_copy_polling
    set_bot_app(bot_app)
    set_runtime_app(bot_app)
    set_copy_bot_app(bot_app)
    register_handlers(handle_strategy_job, handle_alert_job)
    _sw = int(os.environ.get("NADO_STRATEGY_WORKERS", "2"))
    start_workers(
        strategy_workers=max(1, _sw),
        alert_workers=int(os.environ.get("NADO_ALERT_WORKERS", "1")),
    )
    start_runtime_supervisor()
    from src.nadobro.services.runtime_supervisor import runtime_mode

    logger.info(
        "Strategy queue: NADO_STRATEGY_WORKERS=%s NADO_RUNTIME_MODE=%s NADO_STRATEGY_CYCLE_TIMEOUT_SECONDS=%s",
        max(1, _sw),
        runtime_mode(),
        (os.environ.get("NADO_STRATEGY_CYCLE_TIMEOUT_SECONDS") or "180").strip(),
    )

    try:
        from src.nadobro.services.nado_client import NadoClient
        alert_network = (os.environ.get("NADO_ALERT_CHECK_NETWORK") or "testnet").strip().lower()
        alert_pk = (os.environ.get("NADO_ALERT_CHECK_PRIVATE_KEY") or "").strip()
        alert_address = (
            os.environ.get("NADO_ALERT_CHECK_ADDRESS")
            or "0x0000000000000000000000000000000000000000"
        ).strip()
        if alert_pk:
            alert_client = NadoClient(alert_pk, alert_network)
            alert_client.initialize()
        else:
            # Read-only client is sufficient for alert price checks.
            alert_client = NadoClient.from_address(alert_address, alert_network)
        set_check_client(alert_client)
        logger.info("Alert price-check client initialized (network=%s)", alert_network)
    except Exception as e:
        logger.warning(f"Alert price-check client failed to initialize: {e}")

    start_scheduler()
    auto_restore = os.environ.get("NADO_AUTO_RESTORE_STRATEGIES", "true").strip().lower() in ("1", "true", "yes", "on")
    restore_running_bots(enabled=auto_restore)

    copy_enabled = os.environ.get("NADO_COPY_TRADING", "true").strip().lower() in ("1", "true", "yes", "on")
    if copy_enabled:
        await start_copy_polling()
        logger.info("Copy trading polling started")

    transport_mode, webhook_url, webhook_path = _resolve_transport_settings()
    webhook_secret = (os.environ.get("TELEGRAM_WEBHOOK_SECRET") or "").strip()
    if transport_mode == "webhook" and not webhook_secret:
        if os.environ.get("ENVIRONMENT", "").lower() == "production":
            raise RuntimeError(
                "TELEGRAM_WEBHOOK_SECRET must be set in production. "
                "Webhook mode without a secret allows spoofed updates."
            )
        logger.warning(
            "TELEGRAM_WEBHOOK_SECRET is not set. Webhook mode without a secret "
            "allows spoofed updates. Set TELEGRAM_WEBHOOK_SECRET for production."
        )
    if transport_mode == "webhook" and webhook_url:
        parsed = urlparse(webhook_url)
        if parsed.path.rstrip("/") != webhook_path.rstrip("/"):
            base = f"{parsed.scheme}://{parsed.netloc}"
            webhook_url = base + webhook_path

    webhook_listen = os.environ.get("TELEGRAM_WEBHOOK_LISTEN", "0.0.0.0").strip()
    # Prefer TELEGRAM_WEBHOOK_PORT when set so a reverse proxy can own PORT (e.g. nginx on 8080, PTB on 8082).
    _wh = os.environ.get("TELEGRAM_WEBHOOK_PORT", "").strip()
    webhook_port = int(_wh) if _wh else int(os.environ.get("PORT", "8080"))
    bootstrap_health_server = None
    async def _start_polling_mode():
        # Ensure Telegram stops sending updates to webhook before polling starts.
        try:
            await bot_app.bot.delete_webhook(drop_pending_updates=True)
        except Exception as e:
            logger.warning("Could not delete existing webhook before polling fallback: %s", e)
        await bot_app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=["message", "callback_query"],
        )
        logger.info("Nadobro is live in polling mode.")

    if transport_mode == "webhook":
        try:
            bootstrap_health_server = await _start_bootstrap_health_server(webhook_port)
            logger.info("Bootstrap health check listening on port %s", webhook_port)
        except Exception as e:
            logger.warning("Bootstrap health server failed to start (non-fatal): %s", e)

    logger.info("Starting bot (transport=%s)...", transport_mode)
    await bot_app.initialize()
    await bot_app.start()

    from telegram import BotCommand
    await bot_app.bot.set_my_commands([
        BotCommand("start", "Open home dashboard"),
        BotCommand("help", "Show guide and examples"),
        BotCommand("status", "View bot and strategy status"),
        BotCommand("ops", "View runtime diagnostics"),
        BotCommand("revoke", "Show signer revoke steps"),
        BotCommand("stop_all", "Stop all running strategies"),
    ])
    logger.info("Bot commands registered in Menu")

    try:
        from src.nadobro.config import MINIAPP_URL
        if MINIAPP_URL:
            from telegram import MenuButtonWebApp, WebAppInfo

            await bot_app.bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(text="Mini App", web_app=WebAppInfo(url=MINIAPP_URL)),
            )
            logger.info("Mini App menu button registered (MINIAPP_URL set)")
    except Exception as e:
        logger.warning("Mini App menu button not set: %s", e)

    if transport_mode == "webhook":
        if bootstrap_health_server:
            try:
                bootstrap_health_server.close()
                await bootstrap_health_server.wait_closed()
            except Exception:
                pass
        logger.info(
            "Starting webhook server listen=%s port=%s path=%s",
            webhook_listen,
            webhook_port,
            webhook_path,
        )
        try:
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
        except OSError as e:
            if getattr(e, "errno", None) == 98:
                logger.error(
                    "Webhook bind failed on %s:%s (address in use). Falling back to polling mode.",
                    webhook_listen,
                    webhook_port,
                )
                await _start_polling_mode()
            else:
                raise
    else:
        await _start_polling_mode()

        # Polling mode only: lightweight TCP health responder on PORT.
        port_str = os.environ.get("PORT")
        if port_str:
            try:
                port = int(port_str)

                async def _health_handler(reader, writer):
                    try:
                        await reader.read(4096)
                        payload = _runtime_health_payload()
                        body = json.dumps(payload).encode("utf-8")
                        status_line = b"HTTP/1.1 200 OK\r\n" if payload.get("status") == "ok" else b"HTTP/1.1 503 Service Unavailable\r\n"
                        writer.write(
                            status_line
                            + b"Content-Type: application/json\r\n"
                            + f"Content-Length: {len(body)}\r\n\r\n".encode("utf-8")
                            + body
                        )
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

    def handle_signal(sig, _frame):
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
        await stop_copy_polling()
        stop_runtime()
        stop_runtime_supervisor()
        await stop_workers()
        await bot_app.updater.stop()
        await bot_app.stop()
        await bot_app.shutdown()
        logger.info("Bot stopped cleanly")


if __name__ == "__main__":
    asyncio.run(run_bot())
