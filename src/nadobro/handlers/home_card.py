import logging

from telegram.constants import ParseMode
from telegram.ext import CallbackContext

from src.nadobro.handlers.formatters import escape_md, fmt_positions, fmt_settings, fmt_wallet_info, fmt_portfolio
from src.nadobro.handlers.keyboards import (
    home_card_kb,
    mode_kb,
    strategy_hub_kb,
    wallet_kb,
    positions_kb,
    markets_kb,
    alerts_kb,
    settings_kb,
    portfolio_kb,
    persistent_menu_kb,
)
from src.nadobro.services.trade_service import get_trade_analytics
from src.nadobro.services.settings_service import get_user_settings
from src.nadobro.services.user_service import get_user, get_user_nado_client, get_user_wallet_info

logger = logging.getLogger(__name__)

HOME_CARD_KEY = "home_card_message"
KEYBOARD_REMOVED_KEY = "dual_mode_keyboard_removed"


def build_home_card_text(telegram_id: int) -> str:
    user = get_user(telegram_id)
    if not user:
        return "⚠️ User not found\\. Use /start first\\."

    network = user.network_mode.value
    network_label = "🧪 TESTNET" if network == "testnet" else "🌐 MAINNET"
    balance_str = "N/A"
    try:
        client = get_user_nado_client(telegram_id)
        if client:
            balance = client.get_balance()
            if balance and balance.get("exists"):
                raw = (balance.get("balances", {}) or {}).get(0, 0)
                if not raw:
                    raw = (balance.get("balances", {}) or {}).get("0", 0)
                balance_str = f"${float(raw or 0):,.2f}"
    except Exception:
        pass

    return (
        "📊 *Nadobro Home*\n\n"
        f"Mode: *{escape_md(network_label)}*\n"
        f"Balance: *{escape_md(balance_str)}*\n\n"
        "All button workflows run in this message card\\. "
        "Use chat messages for AI Q&A and typed trades\\."
    )


def _remember_home_card(context: CallbackContext, chat_id: int, message_id: int) -> None:
    context.user_data[HOME_CARD_KEY] = {
        "chat_id": chat_id,
        "message_id": message_id,
    }


async def _edit_or_send_card(update, context: CallbackContext, text: str, reply_markup):
    chat_id = update.effective_chat.id

    if not context.user_data.get(KEYBOARD_REMOVED_KEY):
        try:
            # Ensure message-mode has an always-available Home shortcut.
            shortcut_msg = await context.bot.send_message(
                chat_id=chat_id,
                text="Home shortcut enabled.",
                reply_markup=persistent_menu_kb(),
            )
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=shortcut_msg.message_id)
            except Exception:
                pass
            context.user_data[KEYBOARD_REMOVED_KEY] = True
        except Exception:
            # Non-blocking: card rendering should continue even if keyboard remove fails.
            pass

    home = context.user_data.get(HOME_CARD_KEY) or {}
    message_id = home.get("message_id") if home.get("chat_id") == chat_id else None

    if message_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=reply_markup,
            )
            return
        except Exception:
            logger.info("home_card_edit_failed_new_message chat_id=%s", chat_id)

    message = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=reply_markup,
    )
    _remember_home_card(context, chat_id, message.message_id)


def _view_mode_text(telegram_id: int):
    user = get_user(telegram_id)
    current_network = user.network_mode.value if user else "testnet"
    network_label = "🧪 TESTNET" if current_network == "testnet" else "🌐 MAINNET"
    text = (
        f"🔄 *Network Mode*\n\n"
        f"Current: *{escape_md(network_label)}*\n\n"
        f"Switch network below:"
    )
    return text, mode_kb(current_network)


def _view_strategy_text():
    return (
        "🧭 *Strategy Hub*\n\n"
        "Pick a strategy, review setup, then start with pre\\-trade analytics\\.",
        strategy_hub_kb(),
    )


def _view_wallet_text(telegram_id: int):
    info = get_user_wallet_info(telegram_id)
    return fmt_wallet_info(info), wallet_kb()


def _view_positions_text(telegram_id: int):
    client = get_user_nado_client(telegram_id)
    if not client:
        return "⚠️ Wallet not initialized\\. Use /start first\\.", home_card_kb()
    positions = client.get_all_positions()
    prices = None
    try:
        prices = client.get_all_market_prices()
    except Exception:
        pass
    return fmt_positions(positions, prices), positions_kb(positions or [])


def _view_portfolio_text(telegram_id: int):
    client = get_user_nado_client(telegram_id)
    if not client:
        return "⚠️ Wallet not initialized\\. Use /start first\\.", home_card_kb()
    positions = client.get_all_positions() or []
    prices = None
    try:
        prices = client.get_all_market_prices()
    except Exception:
        pass
    stats = get_trade_analytics(telegram_id)
    msg = fmt_portfolio(stats, positions, prices)
    return msg, portfolio_kb(has_positions=bool(positions))


def _view_markets_text():
    return "💹 *Markets*\n\nPick a market view:", markets_kb()


def _view_alerts_text():
    return "🔔 *Alerts*\n\nManage your price alerts\\.", alerts_kb()


def _view_settings_text(telegram_id: int):
    _, settings = get_user_settings(telegram_id)
    msg = fmt_settings(settings)
    lev = settings.get("default_leverage", 1)
    slip = settings.get("slippage", 1)
    return msg, settings_kb(lev, slip)


def resolve_home_view(callback_data: str, telegram_id: int):
    if callback_data == "home:mode":
        return _view_mode_text(telegram_id)
    if callback_data == "nav:strategy_hub":
        return _view_strategy_text()
    if callback_data == "wallet:view":
        return _view_wallet_text(telegram_id)
    if callback_data == "portfolio:view":
        return _view_portfolio_text(telegram_id)
    if callback_data == "pos:view":
        return _view_positions_text(telegram_id)
    if callback_data == "mkt:menu":
        return _view_markets_text()
    if callback_data == "alert:menu":
        return _view_alerts_text()
    if callback_data == "settings:view":
        return _view_settings_text(telegram_id)
    return build_home_card_text(telegram_id), home_card_kb()


async def open_home_card_view_from_message(update, context: CallbackContext, telegram_id: int, callback_data: str):
    text, kb = resolve_home_view(callback_data, telegram_id)
    await _edit_or_send_card(update, context, text, kb)


async def open_home_card_from_command(update, context: CallbackContext, telegram_id: int):
    await _edit_or_send_card(update, context, build_home_card_text(telegram_id), home_card_kb())


async def open_help_card_from_command(update, context: CallbackContext):
    await _edit_or_send_card(update, context, "❓ *Help*\n\nUse the buttons below or type your request in chat\\.", home_card_kb())


async def open_status_card_from_command(update, context: CallbackContext, text: str):
    await _edit_or_send_card(update, context, text, home_card_kb())

