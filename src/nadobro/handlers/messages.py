import logging
import re
import time
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext
from telegram.constants import ParseMode, ChatAction
from src.nadobro.services.user_service import (
    get_or_create_user, get_user_readonly_client, get_user_wallet_info, get_user,
    ensure_active_wallet_ready, save_linked_signer, get_user_nado_client,
)
from src.nadobro.services.trade_service import execute_market_order, execute_limit_order
from src.nadobro.services.trade_service import close_position, close_all_positions, get_trade_analytics
from src.nadobro.services.alert_service import create_alert
from src.nadobro.services.knowledge_service import answer_nado_question, stream_nado_answer
from src.nadobro.services.settings_service import get_user_settings, update_user_settings
from src.nadobro.services.bot_runtime import start_user_bot
from src.nadobro.services.onboarding_service import get_resume_step, evaluate_readiness
from src.nadobro.services.crypto import encrypt_with_passphrase
from src.nadobro.services.admin_service import is_trading_paused
from src.nadobro.config import get_product_id, get_product_max_leverage
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
    TRADE_CARD_SESSION_KEY,
)
from src.nadobro.handlers.home_card import open_home_card_view_from_message
from src.nadobro.handlers.intent_handlers import (
    handle_pending_text_trade_confirmation,
    handle_trade_intent_message,
)
from src.nadobro.handlers.intent_parser import parse_interaction_intent
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.perf import timed_metric, log_slow

logger = logging.getLogger(__name__)

TRADE_FLOW_STEPS = ["direction", "order_type", "product", "leverage", "size", "limit_price", "tpsl", "confirm"]
PENDING_TEXT_CLOSE_ALL_KEY = "pending_text_close_all"
PENDING_PASSPHRASE_ACTION = "pending_passphrase_action"
SESSION_PASSPHRASE_KEY = "session_passphrase"


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


def clear_session_passphrase(context: CallbackContext, telegram_id: int | None = None):
    context.user_data.pop(SESSION_PASSPHRASE_KEY, None)
    if telegram_id is not None:
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "mainnet"
        from src.nadobro.services.bot_runtime import clear_manual_passphrase, clear_runtime_passphrase
        clear_manual_passphrase(telegram_id, network)
        clear_runtime_passphrase(telegram_id, network)


def terminate_active_processes(context: CallbackContext, telegram_id: int | None = None):
    # Clear all pending conversational/process state when user exits to home/back.
    _clear_trade_flow(context)
    context.user_data.pop(PENDING_PASSPHRASE_ACTION, None)
    context.user_data.pop(PENDING_TEXT_CLOSE_ALL_KEY, None)
    context.user_data.pop("pending_trade", None)
    context.user_data.pop("pending_alert", None)
    context.user_data.pop("pending_strategy_input", None)
    context.user_data.pop("pending_question", None)
    context.user_data.pop("wallet_flow", None)
    context.user_data.pop("wallet_linked_signer_pk", None)
    context.user_data.pop("wallet_main_address", None)
    context.user_data.pop("wallet_linked_signer_address", None)
    context.user_data.pop(TRADE_CARD_SESSION_KEY, None)
    clear_session_passphrase(context, telegram_id=telegram_id)


def _cache_session_passphrase(context: CallbackContext, telegram_id: int, passphrase: str):
    if not passphrase:
        return
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    from src.nadobro.services.bot_runtime import set_manual_passphrase
    set_manual_passphrase(telegram_id, network, passphrase)
    context.user_data[SESSION_PASSPHRASE_KEY] = {
        "value": passphrase,
        "network": network,
        "set_at": time.time(),
    }


def _get_session_passphrase(context: CallbackContext, telegram_id: int) -> str | None:
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    payload = context.user_data.get(SESSION_PASSPHRASE_KEY)
    if isinstance(payload, dict):
        if payload.get("network") != network:
            context.user_data.pop(SESSION_PASSPHRASE_KEY, None)
        else:
            value = payload.get("value")
            if isinstance(value, str) and value:
                return value

    # Fallback to runtime/manual session cache so users do not need to
    # re-enter passphrase repeatedly during an active session.
    from src.nadobro.services.bot_runtime import get_runtime_passphrase

    return get_runtime_passphrase(telegram_id, network)


async def _execute_authorized_action(message, context, telegram_id: int, action_data: dict, passphrase: str) -> tuple[bool, str]:
    action_type = action_data.get("type")

    if action_type == "execute_trade":
        payload = action_data.get("payload", {})
        from src.nadobro.handlers.intent_handlers import _execute_trade_payload
        result = await run_blocking(_execute_trade_payload, telegram_id, payload, passphrase=passphrase)
        await message.reply_text(
            fmt_trade_result(result),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return bool(result.get("success")), str(result.get("error", ""))

    if action_type == "execute_trade_flow":
        flow = action_data.get("flow", {})
        direction = flow.get("direction", "long")
        order_type = flow.get("order_type", "market")
        product = flow.get("product", "BTC")
        size = flow.get("size", 0)
        leverage = flow.get("leverage", 1)
        slippage_pct = flow.get("slippage_pct", 1)

        if order_type == "limit":
            price = flow.get("limit_price", flow.get("price", 0))
            is_long = direction == "long"
            result = await run_blocking(
                execute_limit_order, telegram_id, product, size, price,
                is_long=is_long, leverage=leverage, passphrase=passphrase,
                tp_price=flow.get("tp"), sl_price=flow.get("sl"),
            )
        else:
            is_long = direction == "long"
            result = await run_blocking(
                execute_market_order, telegram_id, product, size,
                is_long=is_long, leverage=leverage, slippage_pct=slippage_pct, passphrase=passphrase,
                tp_price=flow.get("tp"), sl_price=flow.get("sl"),
            )
        await message.reply_text(
            fmt_trade_result(result),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return bool(result.get("success")), str(result.get("error", ""))

    if action_type == "exec_trade_callback":
        pending = action_data.get("pending", {})
        action = pending.get("action", "long")
        product = pending.get("product", "BTC")
        size = pending.get("size", 0)
        leverage = pending.get("leverage", 1)
        slippage_pct = pending.get("slippage_pct", 1)

        if action in ("limit_long", "limit_short"):
            price = pending.get("price", 0)
            is_long = action == "limit_long"
            result = await run_blocking(
                execute_limit_order, telegram_id, product, size, price,
                is_long=is_long, leverage=leverage, passphrase=passphrase
            )
        else:
            is_long = action == "long"
            result = await run_blocking(
                execute_market_order, telegram_id, product, size,
                is_long=is_long, leverage=leverage, slippage_pct=slippage_pct, passphrase=passphrase
            )
        await message.reply_text(
            fmt_trade_result(result),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return bool(result.get("success")), str(result.get("error", ""))

    if action_type == "close_position":
        product = action_data.get("product")
        result = await run_blocking(close_position, telegram_id, product, passphrase=passphrase)
        if result.get("success"):
            msg = f"✅ Closed {escape_md(str(result.get('cancelled', 0)))} {escape_md(result.get('product', product))} position size\\."
        else:
            msg = f"❌ Close failed: {escape_md(result.get('error', 'unknown error'))}"
        await message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=persistent_menu_kb())
        return bool(result.get("success")), str(result.get("error", ""))

    if action_type == "close_all":
        result = await run_blocking(close_all_positions, telegram_id, passphrase=passphrase)
        if result.get("success"):
            products = ", ".join(result.get("products", []))
            msg = f"✅ Closed total size {escape_md(str(result.get('cancelled', 0)))} across {escape_md(products)}\\."
        else:
            msg = f"❌ Close failed: {escape_md(result.get('error', 'unknown error'))}"
        await message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=persistent_menu_kb())
        return bool(result.get("success")), str(result.get("error", ""))

    if action_type == "trade_card":
        flow = action_data.get("flow", {})
        order_type = flow.get("order_type", "market")
        product = flow.get("product", "BTC")
        size = flow.get("size", 0)
        direction = flow.get("direction", "long")
        leverage = flow.get("leverage", 1)
        slippage_pct = flow.get("slippage_pct", 1)

        if order_type == "limit":
            price = float(flow.get("limit_price", flow.get("price", 0)) or 0)
            result = await run_blocking(
                execute_limit_order, telegram_id, product, size, price,
                is_long=(direction == "long"), leverage=leverage, passphrase=passphrase,
                tp_price=flow.get("tp"), sl_price=flow.get("sl"),
            )
        else:
            result = await run_blocking(
                execute_market_order, telegram_id, product, size,
                is_long=(direction == "long"), leverage=leverage, slippage_pct=slippage_pct, passphrase=passphrase,
                tp_price=flow.get("tp"), sl_price=flow.get("sl"),
            )
        result_msg = fmt_trade_result(result)
        await message.reply_text(
            f"{result_msg}\n\nUse the menu for your next action\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return bool(result.get("success")), str(result.get("error", ""))

    if action_type == "start_strategy":
        strategy = action_data.get("strategy")
        product = action_data.get("product")
        leverage = float(action_data.get("leverage", 3))
        slippage_pct = float(action_data.get("slippage_pct", 1))
        ok, msg = await run_blocking(
            start_user_bot,
            telegram_id,
            strategy=strategy,
            product=product,
            leverage=leverage,
            slippage_pct=slippage_pct,
            passphrase=passphrase,
        )
        if ok:
            reply = f"🚀 {escape_md(msg)}\n\nUse /status to monitor live loop health\\."
        else:
            reply = f"❌ {escape_md(msg)}"
        await message.reply_text(
            reply,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return bool(ok), "" if ok else str(msg)

    await message.reply_text(
        "⚠️ Unknown action\\. Please try again\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=persistent_menu_kb(),
    )
    return False, "unknown action"


async def _prompt_passphrase(update_or_query, context, action_data: dict):
    context.user_data[PENDING_PASSPHRASE_ACTION] = action_data
    msg_text = "🔐 Enter your passphrase to authorize this command:"
    if hasattr(update_or_query, 'message') and update_or_query.message:
        await update_or_query.message.reply_text(msg_text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="nav:main")],
        ]))
    elif hasattr(update_or_query, 'edit_message_text'):
        await update_or_query.edit_message_text(
            msg_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="nav:main")],
            ]),
        )


async def authorize_or_prompt_passphrase(update_or_query, context, telegram_id: int, action_data: dict):
    cached = _get_session_passphrase(context, telegram_id)
    if cached:
        valid = await run_blocking(lambda: bool(get_user_nado_client(telegram_id, passphrase=cached)))
        if valid:
            target_message = getattr(update_or_query, "message", None)
            if target_message:
                ok, _ = await _execute_authorized_action(target_message, context, telegram_id, action_data, cached)
                if ok:
                    _cache_session_passphrase(context, telegram_id, cached)
                return
        clear_session_passphrase(context, telegram_id=telegram_id)
    await _prompt_passphrase(update_or_query, context, action_data)


async def _handle_passphrase_input(update, context, telegram_id, text):
    action_data = context.user_data.get(PENDING_PASSPHRASE_ACTION)
    if not action_data:
        return False

    passphrase = text.strip()
    context.user_data.pop(PENDING_PASSPHRASE_ACTION, None)

    try:
        await update.message.delete()
    except Exception:
        pass

    ok, _ = await _execute_authorized_action(update.message, context, telegram_id, action_data, passphrase)
    if ok:
        _cache_session_passphrase(context, telegram_id, passphrase)

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


async def handle_message(update: Update, context: CallbackContext):
    started = time.perf_counter()
    try:
        if not update.message or not update.message.text:
            return

        telegram_id = update.effective_user.id
        username = update.effective_user.username
        text = update.message.text.strip()

        get_or_create_user(telegram_id, username)

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

        if await _handle_passphrase_input(update, context, telegram_id, text):
            return

        if await handle_trade_card_text_input(update, context, telegram_id, text):
            return

        if await handle_pending_text_trade_confirmation(update, context, telegram_id, text):
            return

        if await _handle_pending_text_close_all_confirmation(update, context, telegram_id, text):
            return

        if await _handle_wallet_flow(update, context, telegram_id, text):
            return

        if await _handle_trade_flow_free_text(update, context, telegram_id, text):
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
    finally:
        log_slow("message.total", threshold_ms=1000.0, started_at=started)


async def _dispatch_reply_button(update, context, telegram_id, callback_data, text):
    if callback_data == "nav:main":
        terminate_active_processes(context, telegram_id=telegram_id)
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
            "🤖 *Trade Console*\n\nSelect direction:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_direction_kb(),
        )
        _clear_trade_flow(context)
        _set_trade_flow(context, {"state": "direction"})
        return

    if callback_data.startswith("trade_flow:"):
        if is_trade_card_mode_enabled():
            if callback_data in ("trade_flow:home", "trade_flow:cancel"):
                terminate_active_processes(context, telegram_id=telegram_id)
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
            f"🌐 *Execution Mode Control*\n\n"
            f"Current Mode: *{escape_md(network_label)}*\n\n"
            f"Switch mode below:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=mode_kb(current_network),
        )
        return

    if callback_data == "nav:strategy_hub":
        await update.message.reply_text(
            "🤖 *Nadobro Strategy Lab*\n\n"
            "Pick a strategy to open its cockpit dashboard, tune risk, and launch with pre\\-trade analytics\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_hub_kb(),
        )
        return

    if callback_data == "wallet:view":
        await update.message.chat.send_action(ChatAction.TYPING)
        with timed_metric("msg.wallet.view"):
            info = await run_blocking(get_user_wallet_info, telegram_id)
        msg = fmt_wallet_info(info)
        await update.message.reply_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=wallet_kb(),
        )
        return

    if callback_data == "pos:view":
        await update.message.chat.send_action(ChatAction.TYPING)
        client = get_user_readonly_client(telegram_id)
        if not client:
            await update.message.reply_text(
                "⚠️ Wallet not initialized\\. Use /start first\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return
        with timed_metric("msg.positions.view"):
            positions = await run_blocking(client.get_all_positions)
        prices = None
        try:
            prices = await run_blocking(client.get_all_market_prices)
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
        client = get_user_readonly_client(telegram_id)
        if not client:
            await update.message.reply_text(
                "⚠️ Wallet not initialized\\. Use /start first\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return
        with timed_metric("msg.portfolio.view"):
            positions = (await run_blocking(client.get_all_positions)) or []
        prices = None
        try:
            prices = await run_blocking(client.get_all_market_prices)
        except Exception:
            pass
        stats = await run_blocking(get_trade_analytics, telegram_id)
        msg = fmt_portfolio(stats, positions, prices)
        await update.message.reply_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=portfolio_kb(has_positions=bool(positions)),
        )
        return

    if callback_data == "mkt:menu":
        await update.message.reply_text(
            "📡 *Market Radar*\n\nPick a market view:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=markets_kb(),
        )
        return

    if callback_data == "alert:menu":
        await update.message.reply_text(
            "🔔 *Alert Engine*\n\nManage your trigger alerts\\.",
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
        terminate_active_processes(context, telegram_id=telegram_id)
        await update.message.reply_text(
            "↩️ Returned to home\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return

    if action == "back":
        if not flow:
            terminate_active_processes(context, telegram_id=telegram_id)
            await update.message.reply_text(
                "↩️ Returned to home\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=persistent_menu_kb(),
            )
            return
        state = flow.get("state", "direction")
        await _go_back(update, context, flow, state, telegram_id)
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
            reply_markup=trade_leverage_reply_kb(value),
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
        product = flow.get("product", "BTC")
        max_leverage = get_product_max_leverage(product)
        if leverage > max_leverage:
            await update.message.reply_text(
                f"⚠️ Max leverage for {escape_md(product)} is {escape_md(str(max_leverage))}x\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=trade_leverage_reply_kb(product),
            )
            return
        flow["leverage"] = leverage
        flow["state"] = "size"
        _set_trade_flow(context, flow)
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


async def _go_back(update, context, flow, state, telegram_id):
    if state in ("direction", "order_type"):
        terminate_active_processes(context, telegram_id=telegram_id)
        await update.message.reply_text(
            "↩️ Returned to home\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=persistent_menu_kb(),
        )
        return

    back_map = {
        "product": ("order_type", trade_order_type_kb, "Select order type:"),
        "leverage": ("product", trade_product_reply_kb, "Select product:"),
        "size": ("leverage", None, "Select leverage:"),
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
            elif prev_state == "leverage":
                product = flow.get("product", "BTC")
                await update.message.reply_text(
                    prompt,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=trade_leverage_reply_kb(product),
                )
    else:
        terminate_active_processes(context, telegram_id=telegram_id)
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
            client = get_user_readonly_client(telegram_id)
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

    await authorize_or_prompt_passphrase(update, context, telegram_id, {
        "type": "execute_trade_flow",
        "flow": {
            "direction": direction,
            "order_type": order_type,
            "product": product,
            "size": size,
            "leverage": leverage,
            "slippage_pct": slippage_pct,
            "limit_price": flow.get("limit_price", flow.get("price", 0)),
            "tp": flow.get("tp"),
            "sl": flow.get("sl"),
        },
    })


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

    if step == "custom_size":
        try:
            parts = text.split()
            size = float(parts[0])
            leverage = _get_user_settings(telegram_id, context).get("default_leverage", 3)
            explicit_leverage = False
            if len(parts) >= 2:
                lev_str = parts[1].replace("x", "").replace("X", "")
                leverage = int(float(lev_str))
                explicit_leverage = True
        except (ValueError, IndexError):
            await update.message.reply_text(
                "⚠️ Invalid size\\. Enter a number \\(e\\.g\\. `0\\.01`\\)\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return True

        action = pending["action"]
        product = pending["product"]
        max_leverage = get_product_max_leverage(product)
        if leverage > max_leverage:
            if explicit_leverage:
                await update.message.reply_text(
                    f"⚠️ Max leverage for {escape_md(product)} is {escape_md(str(max_leverage))}x\\.",
                    parse_mode=ParseMode.MARKDOWN_V2,
                )
                return True
            leverage = max_leverage

        price = 0
        try:
            client = get_user_readonly_client(telegram_id)
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
        leverage = _get_user_settings(telegram_id, context).get("default_leverage", 3)
        max_leverage = get_product_max_leverage(product)
        if leverage > max_leverage:
            leverage = max_leverage

        context.user_data["pending_trade"] = {
            "action": action,
            "product": product,
            "size": size,
            "leverage": leverage,
            "price": price,
            "slippage_pct": _get_user_settings(telegram_id, context).get("slippage", 1),
        }

        preview = fmt_trade_preview(
            action,
            product,
            size,
            price,
            leverage,
        )
        await update.message.reply_text(
            preview,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_confirm_kb(),
        )
        return True

    return False


def _is_valid_main_address(text: str) -> bool:
    t = (text or "").strip()
    if not t.startswith("0x"):
        return False
    t = t[2:]
    return len(t) == 40 and bool(re.fullmatch(r"[0-9a-fA-F]{40}", t))


def _is_strong_passphrase(text: str) -> tuple[bool, str]:
    t = (text or "").strip()
    if len(t) < 12:
        return False, "Use at least 12 characters."
    if not re.search(r"[A-Z]", t):
        return False, "Include at least one uppercase letter."
    if not re.search(r"[a-z]", t):
        return False, "Include at least one lowercase letter."
    if not re.search(r"\d", t):
        return False, "Include at least one number."
    if not re.search(r"[!@#$%^&*()_+\-=\[\]{};':\"\\|,.<>/?]", t):
        return False, "Include at least one symbol."
    return True, ""


async def _handle_wallet_flow(update, context, telegram_id, text):
    flow = context.user_data.get("wallet_flow")
    if not flow:
        return False

    if flow == "awaiting_main_address":
        if not _is_valid_main_address(text):
            await update.message.reply_text(
                "❌ That doesn't look right. Send your main wallet address — starts with 0x followed by 40 hex characters.",
                parse_mode=ParseMode.MARKDOWN,
            )
            return True
        main_addr = text.strip()
        if main_addr.startswith("0x"):
            main_addr = main_addr[:2] + main_addr[2:].lower()
        context.user_data["wallet_main_address"] = main_addr
        context.user_data["wallet_flow"] = "awaiting_passphrase"
        await update.message.reply_text(
            "🔐 Almost done! Choose a *strong passphrase* to encrypt your 1CT key.\n\n"
            "Requirements: min 12 characters, mix of uppercase, lowercase, number, and symbol.\n\n"
            "You'll need this passphrase each time you start a trading session.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return True

    if flow == "awaiting_passphrase":
        ok, msg = _is_strong_passphrase(text)
        if not ok:
            await update.message.reply_text(f"❌ {msg}", parse_mode=ParseMode.MARKDOWN)
            return True
        pk_hex = context.user_data.get("wallet_linked_signer_pk")
        main_addr = context.user_data.get("wallet_main_address")
        linked_addr = context.user_data.get("wallet_linked_signer_address")
        if not pk_hex or not main_addr or not linked_addr:
            context.user_data.pop("wallet_flow", None)
            await update.message.reply_text("⚠️ Session expired. Tap the Wallet button to start again.")
            return True
        passphrase = text.strip()
        pk_bytes = pk_hex.encode("utf-8")
        ciphertext, salt = encrypt_with_passphrase(pk_bytes, passphrase)
        save_linked_signer(telegram_id, main_addr, linked_addr, ciphertext, salt)
        for key in ("wallet_flow", "wallet_linked_signer_pk", "wallet_main_address", "wallet_linked_signer_address"):
            context.user_data.pop(key, None)
        try:
            await update.message.delete()
        except Exception:
            pass
        await update.message.reply_text(
            "✅ Wallet linked! Your 1CT key is encrypted and stored.\n\n"
            "You can now trade directly from this bot. Revoke anytime with /revoke.",
            reply_markup=persistent_menu_kb(),
        )
        return True

    return False


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
    supported = ("mm", "grid", "dn", "vol")
    supported_fields = (
        "notional_usd", "spread_bp", "interval_seconds", "tp_pct", "sl_pct",
        "levels", "min_range_pct", "max_range_pct", "threshold_bp", "close_offset_bp",
    )
    if strategy not in supported or field not in supported_fields:
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
        "levels": (1, 20),
        "min_range_pct": (0.1, 20),
        "max_range_pct": (0.1, 40),
        "threshold_bp": (1, 500),
        "close_offset_bp": (1, 1000),
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
        if field in {"interval_seconds", "levels"}:
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
            [InlineKeyboardButton("🧠 Strategy Lab", callback_data="nav:strategy_hub")],
        ]),
    )
    return True


async def _handle_nado_question(update, context, question):
    import random
    chat_id = update.effective_chat.id
    draft_id = random.randint(1, 2**31 - 1)
    telegram_id = update.effective_user.id
    user = update.effective_user
    user_name = user.first_name or user.username or "trader"

    try:
        await update.message.chat.send_action(ChatAction.TYPING)
    except Exception:
        pass

    typing_task = None
    stop_typing = asyncio.Event()

    async def _typing_heartbeat():
        # Telegram typing indicator expires quickly; refresh while LLM is running.
        while not stop_typing.is_set():
            try:
                await update.message.chat.send_action(ChatAction.TYPING)
            except Exception:
                pass
            try:
                await asyncio.wait_for(stop_typing.wait(), timeout=4.0)
            except asyncio.TimeoutError:
                continue

    full_text = ""
    last_draft_len = 0
    last_draft_ts = 0.0
    draft_ok = True

    try:
        typing_task = asyncio.create_task(_typing_heartbeat())
        try:
            # Show immediate visual feedback while retrieval/LLM warm-up happens.
            await context.bot.send_message_draft(
                chat_id=chat_id,
                draft_id=draft_id,
                text="🧠 Ask NadoBro\n\nThinking...",
            )
            last_draft_ts = time.time()
        except Exception:
            draft_ok = False

        async for chunk in stream_nado_answer(question, telegram_id=telegram_id, user_name=user_name):
            full_text += chunk
            now_ts = time.time()
            if draft_ok and (len(full_text) - last_draft_len >= 120) and (now_ts - last_draft_ts >= 1.2):
                try:
                    await context.bot.send_message_draft(
                        chat_id=chat_id,
                        draft_id=draft_id,
                        text=f"🧠 Ask NadoBro\n\n{full_text}",
                    )
                    last_draft_len = len(full_text)
                    last_draft_ts = now_ts
                except Exception:
                    draft_ok = False

        if full_text.strip():
            await update.message.reply_text(
                f"🧠 *Ask NadoBro*\n\n{escape_md(full_text)}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 Home", callback_data="nav:main")],
                ]),
            )
        else:
            await update.message.reply_text(
                "⚠️ I couldn't generate an answer\\. Please try again\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 Home", callback_data="nav:main")],
                ]),
            )
    except Exception as e:
        logger.error(f"Nado Q&A error: {e}", exc_info=True)
        if full_text.strip():
            try:
                await update.message.reply_text(
                    f"🧠 *Ask NadoBro*\n\n{escape_md(full_text)}",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🏠 Home", callback_data="nav:main")],
                    ]),
                )
                return
            except Exception:
                pass
        await update.message.reply_text(
            "⚠️ Something went wrong answering your question\\. Please try again\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Home", callback_data="nav:main")],
            ]),
        )
    finally:
        stop_typing.set()
        if typing_task:
            try:
                await typing_task
            except Exception:
                pass


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
    await authorize_or_prompt_passphrase(update, context, telegram_id, {"type": "close_all"})
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
        await authorize_or_prompt_passphrase(update, context, telegram_id, {"type": "close_position", "product": product})
        return True

    return False


def _get_user_settings(telegram_id: int, context: CallbackContext) -> dict:
    from src.nadobro.handlers import shared_get_user_settings
    return shared_get_user_settings(telegram_id, context)
