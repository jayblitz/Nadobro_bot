import logging
from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.services.user_service import (
    get_or_create_user, get_user_nado_client, get_user_wallet_info, get_user,
)
from src.nadobro.services.trade_service import (
    execute_market_order, close_position, close_all_positions,
    get_trade_history, get_trade_analytics,
)
from src.nadobro.services.alert_service import create_alert, get_user_alerts
from src.nadobro.services.admin_service import is_trading_paused
from src.nadobro.services.ai_parser import parse_user_message
from src.nadobro.services.knowledge_service import answer_nado_question
from src.nadobro.config import get_product_id, PRODUCTS
from src.nadobro.handlers.formatters import (
    escape_md, fmt_positions, fmt_balance, fmt_prices, fmt_funding,
    fmt_trade_preview, fmt_trade_result, fmt_history, fmt_analytics,
    fmt_alerts, fmt_price,
)
from src.nadobro.handlers.keyboards import (
    main_menu_kb, trade_confirm_kb, back_kb,
    trade_leverage_kb,
)

logger = logging.getLogger(__name__)


async def handle_message(update: Update, context: CallbackContext):
    if not update.message or not update.message.text:
        return

    telegram_id = update.effective_user.id
    username = update.effective_user.username
    text = update.message.text.strip()

    get_or_create_user(telegram_id, username)

    if await _handle_pending_question(update, context, text):
        return

    if await _handle_pending_trade(update, context, telegram_id, text):
        return

    if await _handle_pending_alert(update, context, telegram_id, text):
        return

    parsed = parse_user_message(text)
    intent = parsed.get("intent", "chat")

    try:
        if intent == "trade":
            await _handle_trade_intent(update, context, telegram_id, parsed)
        elif intent == "query":
            await _handle_query_intent(update, context, telegram_id, parsed)
        elif intent == "command":
            await _handle_command_intent(update, context, telegram_id, parsed)
        elif intent == "nado_question":
            await _handle_nado_question(update, context, text)
        else:
            ai_msg = parsed.get("message", "I'm not sure what you mean.")
            await update.message.reply_text(
                f"💬 {escape_md(ai_msg)}\n\n"
                f"Use the buttons below to navigate:",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=main_menu_kb(),
            )
    except Exception as e:
        logger.error(f"Message handler error: {e}", exc_info=True)
        await update.message.reply_text(
            "⚠️ Something went wrong\\. Please try again\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )


async def _handle_pending_question(update, context, text):
    if not context.user_data.get("pending_question"):
        return False

    context.user_data.pop("pending_question", None)

    await _handle_nado_question(update, context, text)
    return True


async def _handle_pending_trade(update, context, telegram_id, text):
    pending = context.user_data.get("pending_trade")
    if not pending:
        return False

    step = pending.get("step", "")

    if step == "custom_size":
        try:
            parts = text.split()
            size = float(parts[0])
            leverage = 1
            if len(parts) >= 2:
                lev_str = parts[1].replace("x", "").replace("X", "")
                leverage = int(float(lev_str))
        except (ValueError, IndexError):
            await update.message.reply_text(
                "⚠️ Invalid size\\. Enter a number \\(e\\.g\\. `0\\.01`\\)\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return True

        action = pending["action"]
        product = pending["product"]

        price = 0
        try:
            client = get_user_nado_client(telegram_id)
            if client:
                pid = get_product_id(product)
                if pid is not None:
                    mp = client.get_market_price(pid)
                    price = mp.get("mid", 0)
        except Exception:
            pass

        est_margin = (size * price) / leverage if leverage > 0 and price else None

        context.user_data["pending_trade"] = {
            "action": action,
            "product": product,
            "size": size,
            "leverage": leverage,
            "price": price,
            "est_margin": est_margin,
        }

        preview = fmt_trade_preview(action, product, size, price, leverage, est_margin)
        await update.message.reply_text(
            preview,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_confirm_kb(),
        )
        return True

    if step == "limit_input":
        try:
            parts = text.split()
            size = float(parts[0])
            price = float(parts[1])
        except (ValueError, IndexError):
            await update.message.reply_text(
                "⚠️ Enter size and price \\(e\\.g\\. `0\\.01 95000`\\)\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return True

        action = pending["action"]
        product = pending["product"]

        context.user_data["pending_trade"] = {
            "action": action,
            "product": product,
            "size": size,
            "leverage": 1,
            "price": price,
        }

        preview = fmt_trade_preview(action, product, size, price, 1)
        await update.message.reply_text(
            preview,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_confirm_kb(),
        )
        return True

    return False


async def _handle_pending_alert(update, context, telegram_id, text):
    pending = context.user_data.get("pending_alert")
    if not pending:
        return False

    product = pending["product"]
    context.user_data.pop("pending_alert", None)

    try:
        parts = text.lower().split()
        condition = parts[0]
        target = float(parts[1])
    except (ValueError, IndexError):
        await update.message.reply_text(
            "⚠️ Invalid format\\. Use: `above 100000` or `below 90000`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return True

    if condition not in ("above", "below", "funding_above", "funding_below"):
        await update.message.reply_text(
            "⚠️ Invalid condition\\. Use: above, below",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return True

    result = create_alert(telegram_id, product, condition, target)
    if result["success"]:
        await update.message.reply_text(
            f"✅ Alert set\\!\n"
            f"{escape_md(result['product'])} {escape_md(condition)} "
            f"{escape_md(f'${target:,.2f}')}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
    else:
        await update.message.reply_text(
            f"❌ {escape_md(result['error'])}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
    return True


async def _handle_trade_intent(update, context, telegram_id, parsed):
    action = parsed.get("action", "long")
    product = parsed.get("product")
    size = parsed.get("size")
    leverage = parsed.get("leverage", 1) or 1

    if action == "close":
        if product:
            result = close_position(telegram_id, product)
            if result["success"]:
                msg = f"✅ Closed {escape_md(str(result['cancelled']))} order\\(s\\) on {escape_md(result['product'])}\\."
            else:
                msg = f"❌ {escape_md(result['error'])}"
        else:
            result = close_all_positions(telegram_id)
            if result["success"]:
                products = ", ".join(result.get("products", []))
                msg = f"✅ Closed {escape_md(str(result['cancelled']))} order\\(s\\) on {escape_md(products)}\\."
            else:
                msg = f"❌ {escape_md(result['error'])}"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())
        return

    if action == "close_all":
        result = close_all_positions(telegram_id)
        if result["success"]:
            products = ", ".join(result.get("products", []))
            msg = f"✅ Closed {escape_md(str(result['cancelled']))} order\\(s\\) on {escape_md(products)}\\."
        else:
            msg = f"❌ {escape_md(result['error'])}"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())
        return

    if not product or not size:
        ai_msg = parsed.get("message", "Please specify product and size.")
        await update.message.reply_text(
            f"💬 {escape_md(ai_msg)}\n\nUse the buttons for easier trading:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
        return

    if is_trading_paused():
        await update.message.reply_text(
            "⏸ Trading is temporarily paused by admin\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
        return

    price = 0
    try:
        client = get_user_nado_client(telegram_id)
        if client:
            pid = get_product_id(product)
            if pid is not None:
                mp = client.get_market_price(pid)
                price = mp.get("mid", 0)
    except Exception:
        pass

    est_margin = (size * price) / leverage if leverage > 0 and price else None

    context.user_data["pending_trade"] = {
        "action": action,
        "product": product,
        "size": size,
        "leverage": leverage,
        "price": price,
        "est_margin": est_margin,
    }

    preview = fmt_trade_preview(action, product, size, price, leverage, est_margin)
    await update.message.reply_text(
        preview,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=trade_confirm_kb(),
    )


async def _handle_query_intent(update, context, telegram_id, parsed):
    action = parsed.get("action", "")
    product = parsed.get("product")

    client = get_user_nado_client(telegram_id)
    if not client:
        await update.message.reply_text(
            "⚠️ Wallet not initialized\\. Use /start first\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if action == "positions":
        positions = client.get_all_positions()
        prices = None
        try:
            prices = client.get_all_market_prices()
        except Exception:
            pass
        msg = fmt_positions(positions, prices)
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())

    elif action == "balance":
        balance = client.get_balance()
        info = get_user_wallet_info(telegram_id)
        addr = info.get("active_address") if info else None
        msg = fmt_balance(balance, addr)
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())

    elif action == "price":
        if product:
            pid = get_product_id(product)
            if pid is not None:
                mp = client.get_market_price(pid)
                bid_str = "$" + fmt_price(mp["bid"], product)
                ask_str = "$" + fmt_price(mp["ask"], product)
                mid_str = "$" + fmt_price(mp["mid"], product)
                msg = (
                    f"💹 *{escape_md(product)}\\-PERP*\n\n"
                    f"Bid: {escape_md(bid_str)}\n"
                    f"Ask: {escape_md(ask_str)}\n"
                    f"Mid: {escape_md(mid_str)}"
                )
            else:
                msg = f"❌ Unknown product: {escape_md(product)}"
        else:
            prices = client.get_all_market_prices()
            msg = fmt_prices(prices)
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())

    elif action == "funding":
        funding = {}
        for name, info in PRODUCTS.items():
            if info["type"] == "perp":
                if product and name.upper() != product.upper():
                    continue
                fr = client.get_funding_rate(info["id"])
                if fr:
                    funding[name] = fr.get("funding_rate", 0)
        msg = fmt_funding(funding)
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())

    elif action == "history":
        trades = get_trade_history(telegram_id)
        msg = fmt_history(trades)
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())

    elif action == "analytics":
        stats = get_trade_analytics(telegram_id)
        msg = fmt_analytics(stats)
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())

    else:
        ai_msg = parsed.get("message", "")
        await update.message.reply_text(
            f"💬 {escape_md(ai_msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )


async def _handle_nado_question(update, context, question):
    thinking_msg = await update.message.reply_text(
        "🧠 _Thinking\\.\\.\\._",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    try:
        answer = answer_nado_question(question)
        await thinking_msg.edit_text(
            f"🧠 *Ask Nado*\n\n{escape_md(answer)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
    except Exception as e:
        logger.error(f"Nado Q&A error: {e}", exc_info=True)
        await thinking_msg.edit_text(
            "⚠️ Something went wrong answering your question\\. Please try again\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )


async def _handle_command_intent(update, context, telegram_id, parsed):
    action = parsed.get("action", "")
    product = parsed.get("product")

    if action == "alerts":
        condition = parsed.get("alert_condition")
        value = parsed.get("alert_value")

        if product and condition and value:
            result = create_alert(telegram_id, product, condition, value)
            if result["success"]:
                msg = (
                    f"✅ Alert set\\!\n"
                    f"{escape_md(result['product'])} {escape_md(condition)} "
                    f"{escape_md(f'${value:,.2f}')}"
                )
            else:
                msg = f"❌ {escape_md(result['error'])}"
        else:
            alerts = get_user_alerts(telegram_id)
            msg = fmt_alerts(alerts)

        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())

    else:
        ai_msg = parsed.get("message", "I'm not sure what you mean.")
        await update.message.reply_text(
            f"💬 {escape_md(ai_msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
