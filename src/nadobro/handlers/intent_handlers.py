import re
import logging

from telegram.constants import ParseMode
from telegram.ext import CallbackContext

from src.nadobro.handlers.formatters import escape_md, fmt_trade_preview, fmt_trade_result
from src.nadobro.handlers.intent_parser import parse_trade_intent
from src.nadobro.services.admin_service import is_trading_paused
from src.nadobro.services.onboarding_service import get_resume_step
from src.nadobro.services.settings_service import get_user_settings
from src.nadobro.services.trade_service import execute_market_order, execute_limit_order
from src.nadobro.services.user_service import ensure_active_wallet_ready, get_user_nado_client
from src.nadobro.config import get_product_id

PENDING_TEXT_TRADE_KEY = "pending_text_trade"
logger = logging.getLogger(__name__)


def _auto_execute_requested(text: str) -> bool:
    return bool(re.search(r"\b(now|execute|confirm|place)\b", text.lower()))


def _settings_for_user(telegram_id: int) -> dict:
    _, settings = get_user_settings(telegram_id)
    return settings


def _enrich_trade_payload(telegram_id: int, payload: dict, settings: dict) -> dict:
    result = dict(payload)
    result["leverage"] = int(payload.get("leverage") or settings.get("default_leverage", 3))
    result["slippage_pct"] = float(settings.get("slippage", 1))

    price = 0.0
    try:
        if result.get("order_type") == "limit":
            price = float(result.get("limit_price") or 0)
        else:
            client = get_user_nado_client(telegram_id)
            if client:
                pid = get_product_id(result.get("product", "BTC"))
                if pid is not None:
                    mp = client.get_market_price(pid)
                    price = float(mp.get("mid", 0) or 0)
    except Exception:
        price = 0.0

    size = float(result.get("size") or 0)
    leverage = int(result.get("leverage") or 1)
    est_margin = (size * price) / leverage if leverage > 0 and price else None
    result["price"] = price
    result["est_margin"] = est_margin
    return result


def _preview_text(payload: dict) -> str:
    direction = payload.get("direction", "long")
    order_type = payload.get("order_type", "market")
    product = payload.get("product", "BTC")
    size = float(payload.get("size") or 0)
    leverage = int(payload.get("leverage") or 1)
    price = float(payload.get("price") or 0)
    est_margin = payload.get("est_margin")
    action = "limit_long" if (order_type == "limit" and direction == "long") else "limit_short" if order_type == "limit" else direction
    preview = fmt_trade_preview(action, product, size, price, leverage, est_margin)
    if payload.get("tp"):
        preview += f"\n\n📈 *Take Profit:* {escape_md(str(payload['tp']))}"
    if payload.get("sl"):
        preview += f"\n📉 *Stop Loss:* {escape_md(str(payload['sl']))}"
    return preview


def _execute_trade_payload(telegram_id: int, payload: dict) -> dict:
    direction = payload.get("direction", "long")
    order_type = payload.get("order_type", "market")
    product = payload.get("product", "BTC")
    size = float(payload.get("size") or 0)
    leverage = int(payload.get("leverage") or 1)
    slippage_pct = float(payload.get("slippage_pct") or 1)

    if order_type == "limit":
        return execute_limit_order(
            telegram_id,
            product,
            size,
            float(payload.get("limit_price") or 0),
            is_long=(direction == "long"),
            leverage=leverage,
        )
    return execute_market_order(
        telegram_id,
        product,
        size,
        is_long=(direction == "long"),
        leverage=leverage,
        slippage_pct=slippage_pct,
    )


async def handle_pending_text_trade_confirmation(update, context: CallbackContext, telegram_id: int, text: str) -> bool:
    pending = context.user_data.get(PENDING_TEXT_TRADE_KEY)
    if not pending:
        return False

    normalized = text.strip().lower()
    if normalized in ("cancel", "no", "n", "abort"):
        context.user_data.pop(PENDING_TEXT_TRADE_KEY, None)
        await update.message.reply_text(
            "❌ Trade cancelled\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return True

    if normalized not in ("confirm", "yes", "y", "execute", "place"):
        await update.message.reply_text(
            "Type `confirm` to execute this trade or `cancel` to discard it\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return True

    context.user_data.pop(PENDING_TEXT_TRADE_KEY, None)
    if is_trading_paused():
        await update.message.reply_text("⏸ Trading is temporarily paused by admin\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return True
    wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
    if not wallet_ready:
        await update.message.reply_text(f"⚠️ {escape_md(wallet_msg)}", parse_mode=ParseMode.MARKDOWN_V2)
        return True

    result = _execute_trade_payload(telegram_id, pending)
    await update.message.reply_text(fmt_trade_result(result), parse_mode=ParseMode.MARKDOWN_V2)
    return True


async def handle_trade_intent_message(update, context: CallbackContext, telegram_id: int, text: str) -> bool:
    intent = parse_trade_intent(text)
    if not intent:
        return False
    logger.info(
        "trade_intent_detected telegram_id=%s direction=%s product=%s order_type=%s missing=%s",
        telegram_id,
        intent.get("direction"),
        intent.get("product"),
        intent.get("order_type"),
        intent.get("missing"),
    )

    step = get_resume_step(telegram_id)
    if step != "complete":
        await update.message.reply_text(
            f"⚠️ Setup incomplete\\. Resume onboarding at *{escape_md(step.upper())}*\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return True

    wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
    if not wallet_ready:
        await update.message.reply_text(
            f"⚠️ {escape_md(wallet_msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return True

    if intent.get("missing"):
        missing = ", ".join(intent["missing"])
        await update.message.reply_text(
            "I can place this trade from text, but I need a bit more info\\.\n\n"
            f"Missing: *{escape_md(missing)}*\n\n"
            "Example: `buy 0\\.01 BTC 5x market` or `sell 0\\.2 ETH limit 3200`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return True

    settings = _settings_for_user(telegram_id)
    payload = _enrich_trade_payload(telegram_id, intent, settings)
    preview = _preview_text(payload)

    if _auto_execute_requested(text):
        if is_trading_paused():
            await update.message.reply_text("⏸ Trading is temporarily paused by admin\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return True
        result = _execute_trade_payload(telegram_id, payload)
        await update.message.reply_text(fmt_trade_result(result), parse_mode=ParseMode.MARKDOWN_V2)
        return True

    context.user_data[PENDING_TEXT_TRADE_KEY] = payload
    await update.message.reply_text(
        f"{preview}\n\nType `confirm` to execute or `cancel` to discard\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return True
