import logging
import re
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext
from telegram.constants import ParseMode, ChatAction
from src.nadobro.services.user_service import (
    get_or_create_user, get_user_nado_client, get_user_wallet_info, get_user,
    ensure_active_wallet_ready,
)
from src.nadobro.services.trade_service import execute_market_order, execute_limit_order
from src.nadobro.services.trade_service import close_position, close_all_positions, get_trade_analytics
from src.nadobro.services.alert_service import create_alert
from src.nadobro.services.knowledge_service import answer_nado_question, stream_nado_answer
from src.nadobro.services.settings_service import get_user_settings, update_user_settings
from src.nadobro.services.onboarding_service import get_resume_step, evaluate_readiness
from src.nadobro.services.debug_logger import debug_log
from src.nadobro.services.crypto import (
    is_probable_mnemonic,
    normalize_private_key,
    derive_address_from_private_key,
    private_key_fingerprint,
)
from src.nadobro.services.admin_service import is_trading_paused
from src.nadobro.config import get_product_id
from src.nadobro.handlers.formatters import (
    escape_md, fmt_positions, fmt_trade_preview, fmt_strategy_update,
    fmt_trade_result, fmt_wallet_info, fmt_settings, fmt_portfolio,
)
from src.nadobro.handlers.keyboards import (
    persistent_menu_kb, trade_confirm_kb, REPLY_BUTTON_MAP,
    trade_direction_kb, trade_order_type_kb, trade_product_reply_kb,
    trade_leverage_reply_kb, trade_size_reply_kb, trade_tpsl_kb,
    trade_tpsl_edit_kb, trade_confirm_reply_kb, SIZE_PRESETS,
    mode_kb, strategy_hub_kb, wallet_kb, positions_kb, markets_kb,
    alerts_kb, settings_kb, close_product_kb, confirm_close_all_kb, portfolio_kb,
)
from src.nadobro.handlers.trade_card import (
    open_trade_card_from_message,
    handle_trade_card_text_input,
    is_trade_card_mode_enabled,
)
from src.nadobro.handlers.home_card import open_home_card_view_from_message
from src.nadobro.handlers.intent_handlers import (
    handle_pending_text_trade_confirmation,
    handle_trade_intent_message,
)
from src.nadobro.handlers.intent_parser import parse_interaction_intent

logger = logging.getLogger(__name__)

TRADE_FLOW_STEPS = ["direction", "order_type", "product", "leverage", "size", "limit_price", "tpsl", "confirm"]
PENDING_TEXT_CLOSE_ALL_KEY = "pending_text_close_all"


_STATE_REQUIRED_ACTIONS = {
    "trade_flow:direction:long": "direction",
    "trade_flow:direction:short": "direction",
    "trade_flow:order_type:market": "order_type",
    "trade_flow:order_type:limit": "order_type",
    "trade_flow:tpsl:set": "tpsl",
    "trade_flow:tpsl:skip": "tpsl",
    "trade_flow:tpsl:set_tp": "tpsl_edit",
    "trade_flow:tpsl:set_sl": "tpsl_edit",
    "trade_flow:tpsl:done": "tpsl_edit",
    "trade_flow:confirm": "confirm",
}


def _is_contextual_button(callback_data: str, context) -> bool:
    if not callback_data.startswith("trade_flow:"):
        return True

    if callback_data in ("trade_flow:home", "trade_flow:back", "trade_flow:cancel"):
        return True

    flow = context.user_data.get("trade_flow")
    if not flow:
        return callback_data in ("trade_flow:home", "trade_flow:back", "trade_flow:cancel")

    state = flow.get("state", "")

    if callback_data in _STATE_REQUIRED_ACTIONS:
        return state == _STATE_REQUIRED_ACTIONS[callback_data]

    if callback_data.startswith("trade_flow:product:"):
        return state == "product"
    if callback_data.startswith("trade_flow:leverage:"):
        return state == "leverage"
    if callback_data.startswith("trade_flow:size:"):
        return state == "size"

    return True


def _get_trade_flow(context):
    return context.user_data.get("trade_flow")


def _set_trade_flow(context, flow):
    context.user_data["trade_flow"] = flow


def _clear_trade_flow(context):
    context.user_data.pop("trade_flow", None)
    context.user_data.pop("trade_flow_custom_size", None)
    context.user_data.pop("trade_flow_tp_input", None)
    context.user_data.pop("trade_flow_sl_input", None)
    context.user_data.pop("trade_flow_limit_price_input", None)


def _looks_like_private_key_candidate(raw_text: str) -> bool:
    text = (raw_text or "").strip()
    if text.startswith("0x") or text.startswith("0X"):
        text = text[2:]
    if len(text) != 64:
        return False
    return bool(re.fullmatch(r"[0-9a-fA-F]{64}", text))


async def handle_message(update: Update, context: CallbackContext):
    if not update.message or not update.message.text:
        return

    telegram_id = update.effective_user.id
    username = update.effective_user.username
    text = update.message.text.strip()

    get_or_create_user(telegram_id, username)
    debug_log(
        "baseline",
        "H5",
        "messages.py:51",
        "message_received",
        {
            "telegram_id": telegram_id,
            "text_len": len(text),
            "has_pending_trade": bool(context.user_data.get("pending_trade")),
            "has_pending_alert": bool(context.user_data.get("pending_alert")),
            "has_pending_question": bool(context.user_data.get("pending_question")),
        },
    )

    if text in REPLY_BUTTON_MAP:
        callback_data = REPLY_BUTTON_MAP[text]
        if _is_contextual_button(callback_data, context):
            try:
                await _dispatch_reply_button(update, context, telegram_id, callback_data, text)
            except Exception as e:
                logger.error(f"Button dispatch error for '{text}': {e}", exc_info=True)
                try:
                    await update.message.reply_text(
                        "⚠️ Something went wrong\\. Please try again\\.",
                        parse_mode=ParseMode.MARKDOWN_V2,
                        reply_markup=persistent_menu_kb(),
                    )
                except Exception:
                    pass
            return

    if await handle_trade_card_text_input(update, context, telegram_id, text):
        return

    if await handle_pending_text_trade_confirmation(update, context, telegram_id, text):
        return

    if await _handle_pending_text_close_all_confirmation(update, context, telegram_id, text):
        return

    if await _handle_trade_flow_free_text(update, context, telegram_id, text):
        return

    if await _handle_pending_key_import(update, context, telegram_id, text):
        return

    if await _handle_pending_trade(update, context, telegram_id, text):
        return

    if await _handle_pending_alert(update, context, telegram_id, text):
        return

    if await _handle_pending_strategy_input(update, context, telegram_id, text):
        return

    if await handle_trade_intent_message(update, context, telegram_id, text):
        return

    if await _handle_interaction_intent_message(update, context, telegram_id, text):
        return

    await _handle_nado_question(update, context, text)


async def _dispatch_reply_button(update, context, telegram_id, callback_data, text):
    if callback_data == "nav:main":
        if is_trade_card_mode_enabled():
            await open_home_card_view_from_message(update, context, telegram_id, "nav:main")
            return
        await update.message.reply_text(
            "Use /start to open the dashboard\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return

    if is_trade_card_mode_enabled() and callback_data in (
        "pos:view",
        "portfolio:view",
        "wallet:view",
        "mkt:menu",
        "nav:strategy_hub",
        "alert:menu",
        "settings:view",
        "nav:mode",
    ):
        target = "home:mode" if callback_data == "nav:mode" else callback_data
        await open_home_card_view_from_message(update, context, telegram_id, target)
        return

    if callback_data == "nav:trade":
        if is_trade_card_mode_enabled():
            await open_trade_card_from_message(update, context, telegram_id)
            return
        await update.message.reply_text(
            "📊 *Trade*\n\nSelect direction:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_direction_kb(),
        )
        _clear_trade_flow(context)
        _set_trade_flow(context, {"state": "direction"})
        return

    if callback_data.startswith("trade_flow:"):
        if is_trade_card_mode_enabled():
            if callback_data in ("trade_flow:home", "trade_flow:cancel"):
                _clear_trade_flow(context)
                await update.message.reply_text(
                    "↩️ Returned to home\\.",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=persistent_menu_kb(),
                )
                return
            await open_trade_card_from_message(update, context, telegram_id)
            return
        await _handle_trade_flow_button(update, context, telegram_id, callback_data)
        return

    if callback_data == "nav:mode":
        user = get_user(telegram_id)
        current_network = user.network_mode.value if user else "testnet"
        network_label = "🧪 TESTNET" if current_network == "testnet" else "🌐 MAINNET"
        await update.message.reply_text(
            f"🔄 *Network Mode*\n\n"
            f"Current: *{escape_md(network_label)}*\n\n"
            f"Switch network below:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=mode_kb(current_network),
        )
        return

    if callback_data == "nav:strategy_hub":
        await update.message.reply_text(
            "🧭 *Strategy Hub*\n\n"
            "Pick a strategy, review setup, then start with pre\\-trade analytics\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_hub_kb(),
        )
        return

    if callback_data == "wallet:view":
        await update.message.chat.send_action(ChatAction.TYPING)
        info = get_user_wallet_info(telegram_id)
        msg = fmt_wallet_info(info)
        await update.message.reply_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=wallet_kb(),
        )
        return

    if callback_data == "pos:view":
        await update.message.chat.send_action(ChatAction.TYPING)
        client = get_user_nado_client(telegram_id)
        if not client:
            await update.message.reply_text(
                "⚠️ Wallet not initialized\\. Use /start first\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return
        positions = client.get_all_positions()
        prices = None
        try:
            prices = client.get_all_market_prices()
        except Exception:
            pass
        msg = fmt_positions(positions, prices)
        await update.message.reply_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=positions_kb(positions or []),
        )
        return

    if callback_data == "portfolio:view":
        await update.message.chat.send_action(ChatAction.TYPING)
        client = get_user_nado_client(telegram_id)
        if not client:
            await update.message.reply_text(
                "⚠️ Wallet not initialized\\. Use /start first\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return
        positions = client.get_all_positions() or []
        prices = None
        try:
            prices = client.get_all_market_prices()
        except Exception:
            pass
        stats = get_trade_analytics(telegram_id)
        msg = fmt_portfolio(stats, positions, prices)
        await update.message.reply_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=portfolio_kb(has_positions=bool(positions)),
        )
        return

    if callback_data == "mkt:menu":
        await update.message.reply_text(
            "💹 *Markets*\n\nPick a market view:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=markets_kb(),
        )
        return

    if callback_data == "alert:menu":
        await update.message.reply_text(
            "🔔 *Alerts*\n\nManage your price alerts\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=alerts_kb(),
        )
        return

    if callback_data == "settings:view":
        user_settings = _get_user_settings(telegram_id, context)
        msg = fmt_settings(user_settings)
        lev = user_settings.get("default_leverage", 1)
        slip = user_settings.get("slippage", 1)
        await update.message.reply_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(lev, slip),
        )
        return

    await update.message.reply_text(
        f"Use the keyboard buttons or type a question for AI chat\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )


async def _handle_trade_flow_button(update, context, telegram_id, callback_data):
    parts = callback_data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    value = parts[2] if len(parts) > 2 else ""

    flow = _get_trade_flow(context)

    if action == "home" or action == "cancel":
        _clear_trade_flow(context)
        await update.message.reply_text(
            "↩️ Returned to home\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return

    if action == "back":
        if not flow:
            _clear_trade_flow(context)
            await update.message.reply_text(
                "↩️ Returned to home\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=persistent_menu_kb(),
            )
            return
        state = flow.get("state", "direction")
        await _go_back(update, context, flow, state)
        return

    if action == "direction":
        step = get_resume_step(telegram_id)
        if step != "complete":
            await update.message.reply_text(
                f"⚠️ Setup incomplete\\. Resume onboarding at *{escape_md(step.upper())}*\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=persistent_menu_kb(),
            )
            _clear_trade_flow(context)
            return
        wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
        if not wallet_ready:
            await update.message.reply_text(
                f"⚠️ {escape_md(wallet_msg)}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=persistent_menu_kb(),
            )
            _clear_trade_flow(context)
            return

        direction = value
        direction_label = "🟢 LONG" if direction == "long" else "🔴 SHORT"
        _set_trade_flow(context, {"state": "order_type", "direction": direction})
        await update.message.reply_text(
            f"{direction_label} → Select order type:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_order_type_kb(),
        )
        return

    if action == "order_type":
        if not flow:
            return
        flow["order_type"] = value
        flow["state"] = "product"
        _set_trade_flow(context, flow)
        order_label = "📈 MARKET" if value == "market" else "📉 LIMIT"
        await update.message.reply_text(
            f"{order_label} → Select product:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_product_reply_kb(),
        )
        return

    if action == "product":
        if not flow:
            return
        flow["product"] = value
        flow["state"] = "leverage"
        _set_trade_flow(context, flow)
        await update.message.reply_text(
            f"🪙 {escape_md(value)}\\-PERP → Select leverage:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_leverage_reply_kb(),
        )
        return

    if action == "leverage":
        if not flow:
            return
        lev_str = value.replace("x", "").replace("X", "")
        try:
            leverage = int(lev_str)
        except ValueError:
            leverage = 1
        flow["leverage"] = leverage
        flow["state"] = "size"
        _set_trade_flow(context, flow)
        product = flow.get("product", "BTC")
        await update.message.reply_text(
            f"⚡ {escape_md(str(leverage))}x → Select size for {escape_md(product)}:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_size_reply_kb(product),
        )
        return

    if action == "size":
        if not flow:
            return
        if value == "custom":
            context.user_data["trade_flow_custom_size"] = True
            await update.message.reply_text(
                "✏️ Enter custom size \\(e\\.g\\. `0\\.01`\\):",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return

        try:
            size = float(value)
        except ValueError:
            return
        flow["size"] = size
        if flow.get("order_type") == "limit":
            flow["state"] = "limit_price"
            _set_trade_flow(context, flow)
            context.user_data["trade_flow_limit_price_input"] = True
            await update.message.reply_text(
                f"📏 Size: {escape_md(str(size))} → Enter limit price:",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
        else:
            flow["state"] = "tpsl"
            _set_trade_flow(context, flow)
            await update.message.reply_text(
                f"📏 Size: {escape_md(str(size))} → Set TP/SL or skip:",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=trade_tpsl_kb(),
            )
        return

    if action == "tpsl":
        if not flow:
            return
        if value == "skip":
            await _move_to_confirm(update, context, telegram_id, flow)
            return
        if value == "set":
            flow["state"] = "tpsl_edit"
            _set_trade_flow(context, flow)
            tp_val = flow.get("tp")
            sl_val = flow.get("sl")
            tp_str = f"TP: {escape_md(str(tp_val))}" if tp_val else "TP: not set"
            sl_str = f"SL: {escape_md(str(sl_val))}" if sl_val else "SL: not set"
            await update.message.reply_text(
                f"📐 *TP/SL Settings*\n{tp_str} \\| {sl_str}\n\nTap Set TP or Set SL to enter values:",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=trade_tpsl_edit_kb(),
            )
            return
        if value == "set_tp":
            context.user_data["trade_flow_tp_input"] = True
            await update.message.reply_text(
                "Enter take profit price:",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return
        if value == "set_sl":
            context.user_data["trade_flow_sl_input"] = True
            await update.message.reply_text(
                "Enter stop loss price:",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return
        if value == "done":
            await _move_to_confirm(update, context, telegram_id, flow)
            return
        return

    if action == "confirm":
        if not flow:
            return
        await _execute_trade_flow(update, context, telegram_id, flow)
        return


async def _go_back(update, context, flow, state):
    if state in ("direction", "order_type"):
        _clear_trade_flow(context)
        await update.message.reply_text(
            "↩️ Returned to home\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return

    back_map = {
        "product": ("order_type", trade_order_type_kb, "Select order type:"),
        "leverage": ("product", trade_product_reply_kb, "Select product:"),
        "size": ("leverage", trade_leverage_reply_kb, "Select leverage:"),
        "limit_price": ("size", None, None),
        "tpsl": ("size", None, None),
        "tpsl_edit": ("tpsl", trade_tpsl_kb, "Set TP/SL or skip:"),
        "confirm": ("tpsl", trade_tpsl_kb, "Set TP/SL or skip:"),
    }

    if state in back_map:
        prev_state, kb_fn, prompt = back_map[state]
        flow["state"] = prev_state
        context.user_data.pop("trade_flow_custom_size", None)
        context.user_data.pop("trade_flow_tp_input", None)
        context.user_data.pop("trade_flow_sl_input", None)
        context.user_data.pop("trade_flow_limit_price_input", None)
        _set_trade_flow(context, flow)

        if prev_state == "size":
            product = flow.get("product", "BTC")
            await update.message.reply_text(
                f"Select size for {escape_md(product)}:",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=trade_size_reply_kb(product),
            )
        elif kb_fn and prompt:
            await update.message.reply_text(
                prompt,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=kb_fn(),
            )
        else:
            if prev_state == "size":
                product = flow.get("product", "BTC")
                await update.message.reply_text(
                    f"Select size for {escape_md(product)}:",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=trade_size_reply_kb(product),
                )
    else:
        _clear_trade_flow(context)
        await update.message.reply_text(
            "↩️ Returned to home\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )


async def _move_to_confirm(update, context, telegram_id, flow):
    product = flow.get("product", "BTC")
    size = flow.get("size", 0)
    leverage = flow.get("leverage", 1)
    direction = flow.get("direction", "long")
    order_type = flow.get("order_type", "market")

    if order_type == "limit":
        action = "limit_long" if direction == "long" else "limit_short"
    else:
        action = direction

    price = 0
    try:
        if order_type == "limit":
            price = flow.get("limit_price", 0)
        else:
            client = get_user_nado_client(telegram_id)
            if client:
                pid = get_product_id(product)
                if pid is not None:
                    mp = client.get_market_price(pid)
                    price = mp.get("mid", 0)
    except Exception:
        pass

    est_margin = (size * price) / leverage if leverage > 0 and price else None

    flow["state"] = "confirm"
    flow["price"] = price
    flow["est_margin"] = est_margin
    _set_trade_flow(context, flow)

    preview = fmt_trade_preview(action, product, size, price, leverage, est_margin)
    tp_val = flow.get("tp")
    sl_val = flow.get("sl")
    if tp_val or sl_val:
        preview += "\n"
        if tp_val:
            preview += f"\n📈 *Take Profit:* {escape_md(str(tp_val))}"
        if sl_val:
            preview += f"\n📉 *Stop Loss:* {escape_md(str(sl_val))}"

    await update.message.reply_text(
        preview,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=trade_confirm_reply_kb(),
    )


async def _execute_trade_flow(update, context, telegram_id, flow):
    direction = flow.get("direction", "long")
    order_type = flow.get("order_type", "market")
    product = flow.get("product", "BTC")
    size = flow.get("size", 0)
    leverage = flow.get("leverage", 1)
    slippage_pct = _get_user_settings(telegram_id, context).get("slippage", 1)

    _clear_trade_flow(context)

    if is_trading_paused():
        await update.message.reply_text(
            "⏸ Trading is temporarily paused by admin\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return

    wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
    if not wallet_ready:
        await update.message.reply_text(
            f"⚠️ {escape_md(wallet_msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return

    if order_type == "limit":
        price = flow.get("limit_price", flow.get("price", 0))
        is_long = direction == "long"
        result = execute_limit_order(telegram_id, product, size, price, is_long=is_long, leverage=leverage)
    else:
        is_long = direction == "long"
        result = execute_market_order(
            telegram_id,
            product,
            size,
            is_long=is_long,
            leverage=leverage,
            slippage_pct=slippage_pct,
        )

    msg = fmt_trade_result(result)
    await update.message.reply_text(
        msg,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=persistent_menu_kb(),
    )


async def _handle_trade_flow_free_text(update, context, telegram_id, text):
    if context.user_data.get("trade_flow_custom_size"):
        context.user_data.pop("trade_flow_custom_size", None)
        flow = _get_trade_flow(context)
        if not flow:
            return False
        try:
            size = float(text.strip())
        except ValueError:
            await update.message.reply_text(
                "⚠️ Invalid size\\. Enter a number \\(e\\.g\\. `0\\.01`\\)\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            context.user_data["trade_flow_custom_size"] = True
            return True
        flow["size"] = size
        if flow.get("order_type") == "limit":
            flow["state"] = "limit_price"
            _set_trade_flow(context, flow)
            context.user_data["trade_flow_limit_price_input"] = True
            await update.message.reply_text(
                f"📏 Size: {escape_md(str(size))} → Enter limit price:",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
        else:
            flow["state"] = "tpsl"
            _set_trade_flow(context, flow)
            await update.message.reply_text(
                f"📏 Size: {escape_md(str(size))} → Set TP/SL or skip:",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=trade_tpsl_kb(),
            )
        return True

    if context.user_data.get("trade_flow_limit_price_input"):
        context.user_data.pop("trade_flow_limit_price_input", None)
        flow = _get_trade_flow(context)
        if not flow:
            return False
        try:
            price = float(text.strip())
        except ValueError:
            await update.message.reply_text(
                "⚠️ Invalid price\\. Enter a number \\(e\\.g\\. `95000`\\)\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            context.user_data["trade_flow_limit_price_input"] = True
            return True
        flow["limit_price"] = price
        flow["state"] = "tpsl"
        _set_trade_flow(context, flow)
        await update.message.reply_text(
            f"💲 Limit price: {escape_md(str(price))} → Set TP/SL or skip:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_tpsl_kb(),
        )
        return True

    if context.user_data.get("trade_flow_tp_input"):
        context.user_data.pop("trade_flow_tp_input", None)
        flow = _get_trade_flow(context)
        if not flow:
            return False
        try:
            tp = float(text.strip())
        except ValueError:
            await update.message.reply_text(
                "⚠️ Invalid price\\. Enter a number\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            context.user_data["trade_flow_tp_input"] = True
            return True
        flow["tp"] = tp
        _set_trade_flow(context, flow)
        tp_str = f"TP: {escape_md(str(tp))}"
        sl_val = flow.get("sl")
        sl_str = f"SL: {escape_md(str(sl_val))}" if sl_val else "SL: not set"
        await update.message.reply_text(
            f"✅ TP set\\!\n{tp_str} \\| {sl_str}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_tpsl_edit_kb(),
        )
        return True

    if context.user_data.get("trade_flow_sl_input"):
        context.user_data.pop("trade_flow_sl_input", None)
        flow = _get_trade_flow(context)
        if not flow:
            return False
        try:
            sl = float(text.strip())
        except ValueError:
            await update.message.reply_text(
                "⚠️ Invalid price\\. Enter a number\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            context.user_data["trade_flow_sl_input"] = True
            return True
        flow["sl"] = sl
        _set_trade_flow(context, flow)
        tp_val = flow.get("tp")
        tp_str = f"TP: {escape_md(str(tp_val))}" if tp_val else "TP: not set"
        sl_str = f"SL: {escape_md(str(sl))}"
        await update.message.reply_text(
            f"✅ SL set\\!\n{tp_str} \\| {sl_str}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_tpsl_edit_kb(),
        )
        return True

    return False


async def _handle_pending_trade(update, context, telegram_id, text):
    pending = context.user_data.get("pending_trade")
    if not pending:
        return False

    step = pending.get("step", "")
    debug_log(
        "baseline",
        "H2",
        "messages.py:138",
        "pending_trade_message_step",
        {
            "telegram_id": telegram_id,
            "step": step,
            "action": pending.get("action"),
            "product": pending.get("product"),
        },
    )

    if step == "custom_size":
        try:
            parts = text.split()
            size = float(parts[0])
            leverage = _get_user_settings(telegram_id, context).get("default_leverage", 3)
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
            "slippage_pct": _get_user_settings(telegram_id, context).get("slippage", 1),
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
            "leverage": _get_user_settings(telegram_id, context).get("default_leverage", 3),
            "price": price,
            "slippage_pct": _get_user_settings(telegram_id, context).get("slippage", 1),
        }

        preview = fmt_trade_preview(
            action,
            product,
            size,
            price,
            _get_user_settings(telegram_id, context).get("default_leverage", 3),
        )
        await update.message.reply_text(
            preview,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_confirm_kb(),
        )
        return True

    return False


async def _handle_pending_key_import(update, context, telegram_id, text):
    pending_confirm = context.user_data.get("pending_key_confirm")
    if pending_confirm:
        await _delete_user_message(update)
        await update.message.reply_text(
            "⚠️ You already have a key import awaiting confirmation\\. "
            "Use the *Confirm Import* or *Cancel* button\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Confirm Import", callback_data="keyimp:confirm")],
                [InlineKeyboardButton("❌ Cancel", callback_data="keyimp:cancel")],
            ]),
        )
        return True

    pending = context.user_data.get("pending_key_import")
    if not pending:
        return False

    started_at = float(pending.get("started_at") or 0)
    if started_at and time.time() - started_at > 300:
        await _delete_user_message(update)
        context.user_data.pop("pending_key_import", None)
        await update.message.reply_text(
            "⌛ Key import session expired\\. Run /import\\_key again\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return True

    lowered = text.strip().lower()
    if lowered in ("cancel", "stop", "abort", "exit", "skip", "nevermind", "never mind"):
        context.user_data.pop("pending_key_import", None)
        await update.message.reply_text(
            "✅ Key import cancelled\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return True

    if is_probable_mnemonic(text):
        await _delete_user_message(update)
        await update.message.reply_text(
            "🛑 This looks like a seed phrase\\. Nadobro accepts *private key only* for dedicated trading wallets\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        context.user_data.pop("pending_key_import", None)
        return True

    if not _looks_like_private_key_candidate(text):
        # User sent normal chat text while import was pending; cancel import so chat can continue.
        context.user_data.pop("pending_key_import", None)
        return False

    network = pending.get("network", "testnet")
    try:
        normalized = normalize_private_key(text.strip())
        address = derive_address_from_private_key(normalized)
        fingerprint = private_key_fingerprint(normalized)
    except Exception as e:
        await _delete_user_message(update)
        await update.message.reply_text(
            f"❌ {escape_md(str(e))}\n\nTry again with a dedicated private key for this mode\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return True

    context.user_data["pending_key_confirm"] = {
        "network": network,
        "private_key": normalized,
        "address": address,
        "fingerprint": fingerprint,
        "started_at": time.time(),
    }
    await _delete_user_message(update)
    context.user_data.pop("pending_key_import", None)
    await update.message.reply_text(
        f"🔐 *Confirm Key Import* \\({escape_md(network.upper())}\\)\n\n"
        f"Address: `{escape_md(address)}`\n"
        f"Fingerprint: `fp\\-{escape_md(fingerprint)}`\n\n"
        "⚠️ Ensure this is your *dedicated trading key* \\(not your main wallet\\) before confirming\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm Import", callback_data="keyimp:confirm")],
            [InlineKeyboardButton("❌ Cancel", callback_data="keyimp:cancel")],
        ]),
    )
    return True


async def _delete_user_message(update: Update):
    try:
        if update.message:
            await update.message.delete()
    except Exception:
        pass


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

    if condition not in ("above", "below"):
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
            reply_markup=persistent_menu_kb(),
        )
    else:
        await update.message.reply_text(
            f"❌ {escape_md(result['error'])}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
    return True


async def _handle_pending_strategy_input(update, context, telegram_id, text):
    pending = context.user_data.get("pending_strategy_input")
    if not pending:
        return False

    strategy = pending.get("strategy")
    field = pending.get("field")
    if strategy not in ("mm", "grid", "dn") or field not in ("notional_usd", "spread_bp", "interval_seconds", "tp_pct", "sl_pct"):
        context.user_data.pop("pending_strategy_input", None)
        return False

    try:
        value = float(text.strip())
    except (TypeError, ValueError):
        await update.message.reply_text(
            "⚠️ Invalid value\\. Please enter a number\\. Example: `1\\.2`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return True

    limits = {
        "notional_usd": (1, 1000000),
        "spread_bp": (0.1, 200),
        "interval_seconds": (10, 3600),
        "tp_pct": (0.05, 100),
        "sl_pct": (0.05, 100),
    }
    lo, hi = limits[field]
    if value < lo or value > hi:
        await update.message.reply_text(
            f"⚠️ Value out of range\\. Allowed: {escape_md(str(lo))} to {escape_md(str(hi))}",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return True

    def _mutate(s):
        strategies = s.setdefault("strategies", {})
        cfg = strategies.setdefault(strategy, {})
        if field == "interval_seconds":
            cfg[field] = int(value)
        else:
            cfg[field] = value

    network, settings = update_user_settings(telegram_id, _mutate)
    conf = settings.get("strategies", {}).get(strategy, {})
    context.user_data.pop("pending_strategy_input", None)
    await update.message.reply_text(
        fmt_strategy_update(strategy, network, conf),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⚙️ Continue Editing", callback_data=f"strategy:config:{strategy}")],
            [InlineKeyboardButton("🧭 Strategy Hub", callback_data="nav:strategy_hub")],
        ]),
    )
    return True


async def _handle_nado_question(update, context, question):
    import random
    chat_id = update.effective_chat.id
    draft_id = random.randint(1, 2**31 - 1)

    try:
        await update.message.chat.send_action(ChatAction.TYPING)
    except Exception:
        pass

    full_text = ""
    last_draft_len = 0
    draft_ok = True

    try:
        async for chunk in stream_nado_answer(question):
            full_text += chunk
            if draft_ok and len(full_text) - last_draft_len >= 40:
                try:
                    await context.bot.send_message_draft(
                        chat_id=chat_id,
                        draft_id=draft_id,
                        text=f"🧠 Ask Nado\n\n{full_text}",
                    )
                    last_draft_len = len(full_text)
                except Exception:
                    draft_ok = False

        if full_text.strip():
            await update.message.reply_text(
                f"🧠 *Ask Nado*\n\n{escape_md(full_text)}",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
        else:
            await update.message.reply_text(
                "⚠️ I couldn't generate an answer\\. Please try again\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
    except Exception as e:
        logger.error(f"Nado Q&A error: {e}", exc_info=True)
        if full_text.strip():
            try:
                await update.message.reply_text(
                    f"🧠 *Ask Nado*\n\n{escape_md(full_text)}",
                    parse_mode=ParseMode.MARKDOWN_V2,
                )
                return
            except Exception:
                pass
        await update.message.reply_text(
            "⚠️ Something went wrong answering your question\\. Please try again\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )


async def _handle_pending_text_close_all_confirmation(update, context, telegram_id, text):
    if not context.user_data.get(PENDING_TEXT_CLOSE_ALL_KEY):
        return False

    normalized = (text or "").strip().lower()
    if normalized in ("cancel", "no", "n", "abort"):
        context.user_data.pop(PENDING_TEXT_CLOSE_ALL_KEY, None)
        await update.message.reply_text(
            "❌ Close-all request cancelled\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return True

    if normalized not in ("confirm", "yes", "y", "execute", "close all"):
        await update.message.reply_text(
            "Type `confirm` to close all positions or `cancel` to discard\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=confirm_close_all_kb(),
        )
        return True

    context.user_data.pop(PENDING_TEXT_CLOSE_ALL_KEY, None)
    result = close_all_positions(telegram_id)
    if result.get("success"):
        products = ", ".join(result.get("products", []))
        msg = f"✅ Closed total size {escape_md(str(result.get('cancelled', 0)))} across {escape_md(products)}\\."
    else:
        msg = f"❌ Close failed: {escape_md(result.get('error', 'unknown error'))}"
    await update.message.reply_text(
        msg,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=persistent_menu_kb(),
    )
    return True


async def _handle_interaction_intent_message(update, context, telegram_id, text):
    intent = parse_interaction_intent(text)
    if not intent:
        return False

    action = intent.get("action")
    if action == "open_view":
        callback_data = intent.get("target")
        if not callback_data:
            return False
        await _dispatch_reply_button(update, context, telegram_id, callback_data, text)
        return True

    if action == "close_menu":
        await update.message.reply_text(
            "*Close Position*\n\nSelect the product to close:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=close_product_kb(),
        )
        return True

    if action == "close_all":
        context.user_data[PENDING_TEXT_CLOSE_ALL_KEY] = True
        await update.message.reply_text(
            "⚠️ *Close All Positions*\n\nAre you sure you want to close ALL open orders?\n\n"
            "Type `confirm` to execute or `cancel` to discard\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=confirm_close_all_kb(),
        )
        return True

    if action == "close_product":
        product = intent.get("product")
        if not product:
            return False
        result = close_position(telegram_id, product)
        if result.get("success"):
            msg = (
                f"✅ Closed {escape_md(str(result.get('cancelled', 0)))} "
                f"{escape_md(result.get('product', product))} position size\\."
            )
        else:
            msg = f"❌ Close failed: {escape_md(result.get('error', 'unknown error'))}"
        await update.message.reply_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return True

    return False


def _get_user_settings(telegram_id: int, context: CallbackContext) -> dict:
    network, settings = get_user_settings(telegram_id)
    context.user_data[f"settings:{network}"] = settings
    context.user_data["settings"] = settings
    return settings
