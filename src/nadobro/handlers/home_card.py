import logging

from telegram.constants import ParseMode
from telegram.ext import CallbackContext

from src.nadobro.handlers.formatters import (
    escape_md,
    fmt_positions,
    fmt_settings,
    fmt_wallet_info,
    fmt_portfolio,
    fmt_help,
    fmt_points_dashboard,
    _compute_total_equity,
)
from src.nadobro.handlers.keyboards import (
    home_card_kb,
    mode_kb,
    strategy_hub_kb,
    wallet_kb,
    positions_kb,
    alerts_kb,
    settings_kb,
    portfolio_kb,
    persistent_menu_kb,
    points_scope_kb,
)
from src.nadobro.services.trade_service import get_trade_analytics
from src.nadobro.services.equity_snapshots import record_snapshot, get_1d_7d_changes
from src.nadobro.services.settings_service import get_user_settings
from src.nadobro.services.user_service import get_user, get_user_readonly_client, get_user_wallet_info
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.perf import timed_metric
from src.nadobro.services.points_service import get_points_dashboard
from src.nadobro.i18n import (
    language_context,
    get_user_language,
    localize_payload,
    localize_text,
)

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
        client = get_user_readonly_client(telegram_id)
        if client:
            balance = client.get_balance()
            if balance and balance.get("exists"):
                raw = (balance.get("balances", {}) or {}).get(0, 0)
                if not raw:
                    raw = (balance.get("balances", {}) or {}).get("0", 0)
                balance_str = f"${float(raw or 0):,.2f}"
    except Exception:
        pass

    return localize_text(
        "🤖 *Nadobro Command Center*\n\n"
        f"Mode: *{escape_md(network_label)}*\n"
        f"Balance: *{escape_md(balance_str)}*\n\n"
        "Use this control panel for trading, portfolio, strategy lab, and risk settings\\.\n"
        "Use chat messages for AI Q\\&A and typed trade commands\\."
    )


async def build_home_card_text_async(telegram_id: int) -> str:
    with timed_metric("card.home.build"):
        return await run_blocking(build_home_card_text, telegram_id)


def _remember_home_card(context: CallbackContext, chat_id: int, message_id: int) -> None:
    context.user_data[HOME_CARD_KEY] = {
        "chat_id": chat_id,
        "message_id": message_id,
    }


async def _edit_or_send_card(
    update,
    context: CallbackContext,
    text: str,
    reply_markup,
    force_new: bool = False,
):
    chat_id = update.effective_chat.id

    if not context.user_data.get(KEYBOARD_REMOVED_KEY):
        try:
            # Ensure message-mode has an always-available Home shortcut.
            loc_text, loc_kb = localize_payload("Home shortcut enabled.", persistent_menu_kb())
            shortcut_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=loc_text,
                reply_markup=loc_kb,
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

    if message_id and not force_new:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=localize_text(text),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=localize_payload(reply_markup=reply_markup)[1],
            )
            return
        except Exception as e:
            logger.info("home_card_edit_failed_new_message chat_id=%s err=%s", chat_id, e)
            # Fallback if MarkdownV2 entities cannot be parsed.
            try:
                cleaned_text = localize_text(text).replace("\\", "")
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=cleaned_text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=localize_payload(reply_markup=reply_markup)[1],
                )
                return
            except Exception:
                # Final fallback: plain text, no formatting.
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=localize_text(text).replace("\\", ""),
                    reply_markup=localize_payload(reply_markup=reply_markup)[1],
                )
                return

    loc_text, loc_kb = localize_payload(text, reply_markup)
    try:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=loc_text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=loc_kb,
        )
    except Exception as e:
        logger.warning("home_card_send_markdown_failed chat_id=%s err=%s", chat_id, e)
        cleaned_text = loc_text.replace("\\", "")
        try:
            message = await context.bot.send_message(
                chat_id=chat_id,
                text=cleaned_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=loc_kb,
            )
        except Exception:
            # Final fallback: plain text, no formatting.
            message = await context.bot.send_message(
                chat_id=chat_id,
                text=cleaned_text,
                reply_markup=loc_kb,
            )
    _remember_home_card(context, chat_id, message.message_id)


def _view_mode_text(telegram_id: int):
    user = get_user(telegram_id)
    current_network = user.network_mode.value if user else "testnet"
    network_label = "🧪 TESTNET" if current_network == "testnet" else "🌐 MAINNET"
    text = (
        f"🌐 *Execution Mode Control*\n\n"
        f"Current Mode: *{escape_md(network_label)}*\n\n"
        f"Switch mode below:"
    )
    return text, mode_kb(current_network)


def _view_strategy_text():
    return (
        "🤖 *Nadobro Strategy Lab*\n\n"
        "Pick a strategy to open its cockpit dashboard, edit parameters, and launch with pre\\-trade analytics\\.",
        strategy_hub_kb(),
    )


def _view_wallet_text(telegram_id: int):
    info = get_user_wallet_info(telegram_id)
    return fmt_wallet_info(info), wallet_kb()


def _view_positions_text(telegram_id: int):
    client = get_user_readonly_client(telegram_id)
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
    client = get_user_readonly_client(telegram_id)
    if not client:
        return "⚠️ Wallet not initialized\\. Use /start first\\.", home_card_kb()
    positions = client.get_all_positions() or []
    prices = None
    balance = None
    try:
        prices = client.get_all_market_prices()
    except Exception:
        pass
    try:
        balance = client.get_balance()
    except Exception:
        pass
    stats = get_trade_analytics(telegram_id)
    total_equity, _, _, _ = _compute_total_equity(balance, positions, prices)
    record_snapshot(telegram_id, total_equity)
    p1d, p7d = get_1d_7d_changes(telegram_id)
    msg = fmt_portfolio(stats, positions, prices, balance=balance, equity_1d_pct=p1d, equity_7d_pct=p7d)
    return msg, portfolio_kb(has_positions=bool(positions))


def _view_alerts_text():
    return "🔔 *Alert Engine*\n\nManage your trigger alerts\\.", alerts_kb()


def _view_settings_text(telegram_id: int):
    _, settings = get_user_settings(telegram_id)
    msg = fmt_settings(settings)
    lev = settings.get("default_leverage", 1)
    slip = settings.get("slippage", 1)
    return msg, settings_kb(lev, slip)


def _view_points_text(telegram_id: int, scope: str = "week"):
    points = get_points_dashboard(telegram_id, scope)
    return fmt_points_dashboard(points), points_scope_kb("week")


async def resolve_home_view(callback_data: str, telegram_id: int):
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
    if callback_data == "alert:menu":
        return _view_alerts_text()
    if callback_data == "settings:view":
        return _view_settings_text(telegram_id)
    if callback_data == "nav:help":
        return fmt_help(), home_card_kb()
    if callback_data.startswith("points:view:"):
        scope = callback_data.split(":")[2] if len(callback_data.split(":")) > 2 else "week"
        if scope in ("current", "all", "epoch"):
            scope = "week"
        return _view_points_text(telegram_id, scope)
    if callback_data.startswith("points:refresh:"):
        scope = callback_data.split(":")[2] if len(callback_data.split(":")) > 2 else "week"
        if scope in ("current", "all", "epoch"):
            scope = "week"
        return _view_points_text(telegram_id, scope)
    return await build_home_card_text_async(telegram_id), home_card_kb()


async def open_home_card_view_from_message(
    update,
    context: CallbackContext,
    telegram_id: int,
    callback_data: str,
    force_new: bool = False,
):
    with language_context(get_user_language(telegram_id)):
        text, kb = await resolve_home_view(callback_data, telegram_id)
        await _edit_or_send_card(update, context, text, kb, force_new=force_new)


async def open_home_card_from_command(update, context: CallbackContext, telegram_id: int):
    with language_context(get_user_language(telegram_id)):
        text = await build_home_card_text_async(telegram_id)
        await _edit_or_send_card(update, context, text, home_card_kb(), force_new=True)


async def open_help_card_from_command(update, context: CallbackContext):
    telegram_id = update.effective_user.id
    with language_context(get_user_language(telegram_id)):
        await _edit_or_send_card(update, context, fmt_help(), home_card_kb(), force_new=True)


async def open_status_card_from_command(update, context: CallbackContext, text: str):
    telegram_id = update.effective_user.id
    with language_context(get_user_language(telegram_id)):
        await _edit_or_send_card(update, context, text, home_card_kb(), force_new=True)

