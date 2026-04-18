import logging
import time

from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import CallbackContext

from src.nadobro.i18n import localize_text, localize_markup, get_active_language
from src.nadobro.handlers.render_utils import plain_text_fallback
from src.nadobro.handlers.formatters import (
    escape_md,
    fmt_alert_menu_intro,
    fmt_mode_view,
    md2_rule,
    fmt_positions,
    fmt_settings,
    fmt_wallet_info,
    fmt_portfolio,
    fmt_help,
    fmt_points_dashboard,
)
from src.nadobro.handlers.keyboards import (
    home_card_kb,
    mode_kb,
    strategy_hub_kb,
    wallet_kb,
    positions_kb,
    points_scope_kb,
    alerts_kb,
    settings_kb,
    portfolio_kb,
    persistent_menu_kb,
)
from src.nadobro.services.trade_service import get_trade_analytics, get_open_limit_orders
from src.nadobro.services.settings_service import get_user_settings
from src.nadobro.services.user_service import get_user, get_user_readonly_client, get_user_wallet_info
from src.nadobro.services.points_service import get_points_dashboard
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.perf import timed_metric

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

    return (
        "🤖 *Nadobro Command Center*\n"
        f"{md2_rule()}\n\n"
        f"*Execution mode*\n{escape_md(network_label)}\n\n"
        f"*Wallet balance \\(USDT\\)*\n{escape_md(balance_str)}\n\n"
        f"{md2_rule()}\n\n"
        "*Shortcuts*\n"
        "Tap the buttons below for Trade, Portfolio, Strategies, Points, Alerts, and Settings\\.\n\n"
        "_Chat here for AI Q\\&A and plain\\-language trades\\._"
    )


async def build_home_card_text_async(telegram_id: int) -> str:
    with timed_metric("card.home.build"):
        return await run_blocking(build_home_card_text, telegram_id)


def build_positions_view(telegram_id: int):
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    client = get_user_readonly_client(telegram_id, network=network)
    if not client:
        return localize_text("⚠️ Wallet not initialized\\. Use /start first\\.", get_active_language()), home_card_kb()
    try:
        positions = client.get_all_positions() or []
    except Exception as e:
        logger.warning("positions_view_failed user=%s err=%s", telegram_id, e)
        return localize_text(
            "⚠️ Positions refresh is temporarily unavailable\\. Try again shortly\\.",
            get_active_language(),
        ), home_card_kb()
    prices = None
    try:
        prices = client.get_all_market_prices()
    except Exception as e:
        logger.debug("positions_prices_failed user=%s err=%s", telegram_id, e)
    return fmt_positions(positions, prices), positions_kb(positions or [])


def build_portfolio_view(telegram_id: int, force_refresh_orders: bool = False):
    started = time.perf_counter()
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    client = get_user_readonly_client(telegram_id, network=network)
    if not client:
        return localize_text("⚠️ Wallet not initialized\\. Use /start first\\.", get_active_language()), home_card_kb()
    try:
        t_positions = time.perf_counter()
        positions = client.get_all_positions() or []
        t_positions = time.perf_counter() - t_positions
    except Exception as e:
        logger.warning("portfolio_positions_failed user=%s err=%s", telegram_id, e)
        return localize_text(
            "⚠️ Portfolio refresh is temporarily unavailable\\. Try again shortly\\.",
            get_active_language(),
        ), home_card_kb()
    prices = None
    try:
        t_prices = time.perf_counter()
        prices = client.get_all_market_prices()
        t_prices = time.perf_counter() - t_prices
    except Exception as e:
        logger.debug("portfolio_prices_failed user=%s err=%s", telegram_id, e)
        t_prices = 0.0
    try:
        t_stats = time.perf_counter()
        stats = get_trade_analytics(telegram_id)
        t_stats = time.perf_counter() - t_stats
    except Exception as e:
        logger.warning("portfolio_stats_failed user=%s err=%s", telegram_id, e)
        stats = {}
        t_stats = 0.0
    try:
        t_orders = time.perf_counter()
        open_orders = get_open_limit_orders(telegram_id, refresh=force_refresh_orders)
        t_orders = time.perf_counter() - t_orders
    except Exception as e:
        logger.warning("portfolio_open_orders_failed user=%s err=%s", telegram_id, e)
        open_orders = []
        t_orders = 0.0
    msg = fmt_portfolio(stats, positions, prices, open_orders=open_orders)
    elapsed = time.perf_counter() - started
    if elapsed >= 1.2:
        logger.info(
            "portfolio_view_slow user=%s mode=%s refresh=%s total=%.3fs positions=%.3fs prices=%.3fs stats=%.3fs orders=%.3fs",
            telegram_id,
            network,
            force_refresh_orders,
            elapsed,
            t_positions if "t_positions" in locals() else 0.0,
            t_prices,
            t_stats,
            t_orders,
        )
    return msg, portfolio_kb(has_positions=bool(positions))


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
    *,
    prefer_reply_to_message: bool = False,
):
    """Show or refresh the home / module card.

    When *prefer_reply_to_message* is True (reply-keyboard flows), we always send a
    new message replying to the user's message. Editing only the last remembered
    home-card message updates a bubble that may be far above the viewport, so
    users think the bot ignored the tap until they tap again or scroll up.
    """
    chat_id = update.effective_chat.id
    lang = get_active_language()
    text = localize_text(text, lang)
    reply_markup = localize_markup(reply_markup, lang)

    if not context.user_data.get(KEYBOARD_REMOVED_KEY):
        try:
            shortcut_text = localize_text("Home shortcut enabled.", lang)
            shortcut_kb = localize_markup(persistent_menu_kb(), lang)
            shortcut_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=shortcut_text,
                reply_markup=shortcut_kb,
            )
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=shortcut_msg.message_id)
            except Exception:
                pass
            context.user_data[KEYBOARD_REMOVED_KEY] = True
        except Exception:
            pass

    user_message = getattr(update, "message", None)
    if prefer_reply_to_message and user_message:
        try:
            msg = await user_message.reply_text(
                text,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=reply_markup,
            )
        except BadRequest as e:
            if "Can't parse entities" not in str(e):
                raise
            msg = await user_message.reply_text(
                plain_text_fallback(text),
                reply_markup=reply_markup,
            )
        _remember_home_card(context, chat_id, msg.message_id)
        return

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
        except BadRequest as e:
            if "Message is not modified" in str(e):
                return
            if "Can't parse entities" not in str(e):
                logger.info("home_card_edit_failed_new_message chat_id=%s", chat_id)
            else:
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=plain_text_fallback(text),
                        reply_markup=reply_markup,
                    )
                    return
                except BadRequest as e2:
                    if "Message is not modified" in str(e2):
                        return
                    logger.info("home_card_edit_fallback_failed_new_message chat_id=%s", chat_id)
                except Exception:
                    logger.info("home_card_edit_fallback_failed_new_message chat_id=%s", chat_id)
        except Exception:
            logger.info("home_card_edit_failed_new_message chat_id=%s", chat_id)

    try:
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=reply_markup,
        )
    except BadRequest as e:
        if "Can't parse entities" not in str(e):
            raise
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=plain_text_fallback(text),
            reply_markup=reply_markup,
        )
    _remember_home_card(context, chat_id, message.message_id)


def _view_mode_text(telegram_id: int):
    user = get_user(telegram_id)
    current_network = user.network_mode.value if user else "testnet"
    return fmt_mode_view(current_network), mode_kb(current_network)


def _view_strategy_text():
    lang = get_active_language()
    header = localize_text("🤖 *Nadobro Strategy Lab*", lang)
    body = localize_text("Pick a strategy to open its cockpit dashboard, edit parameters, and launch with pre\\-trade analytics\\.", lang)
    return f"{header}\n\n{body}", strategy_hub_kb()


def _view_wallet_text(telegram_id: int):
    info = get_user_wallet_info(telegram_id)
    return fmt_wallet_info(info), wallet_kb()


def _view_positions_text(telegram_id: int):
    return build_positions_view(telegram_id)


def _view_portfolio_text(telegram_id: int):
    return build_portfolio_view(telegram_id)


def _view_points_text(telegram_id: int):
    payload = get_points_dashboard(telegram_id, scope="week")
    return fmt_points_dashboard(payload), points_scope_kb("week")


def _view_alerts_text():
    return fmt_alert_menu_intro(), alerts_kb()


def _view_settings_text(telegram_id: int):
    _, settings = get_user_settings(telegram_id)
    msg = fmt_settings(settings)
    lev = settings.get("default_leverage", 1)
    slip = settings.get("slippage", 1)
    return msg, settings_kb(lev, slip)


async def resolve_home_view(callback_data: str, telegram_id: int):
    if callback_data in ("market:view", "nav:market_radar", "market:radar", "home:market_radar"):
        callback_data = "points:view"
    if callback_data == "home:mode":
        return _view_mode_text(telegram_id)
    if callback_data == "nav:strategy_hub":
        return _view_strategy_text()
    if callback_data == "wallet:view":
        return await run_blocking(_view_wallet_text, telegram_id)
    if callback_data == "portfolio:view":
        return await run_blocking(_view_portfolio_text, telegram_id)
    if callback_data == "pos:view":
        return await run_blocking(_view_positions_text, telegram_id)
    if callback_data == "points:view":
        return await run_blocking(_view_points_text, telegram_id)
    if callback_data == "alert:menu":
        return _view_alerts_text()
    if callback_data == "settings:view":
        return await run_blocking(_view_settings_text, telegram_id)
    return await build_home_card_text_async(telegram_id), home_card_kb()


async def open_home_card_view_from_message(update, context: CallbackContext, telegram_id: int, callback_data: str):
    text, kb = await resolve_home_view(callback_data, telegram_id)
    await _edit_or_send_card(update, context, text, kb, prefer_reply_to_message=True)


async def open_home_card_from_command(update, context: CallbackContext, telegram_id: int):
    text = await build_home_card_text_async(telegram_id)
    await _edit_or_send_card(update, context, text, home_card_kb())


async def open_help_card_from_command(update, context: CallbackContext):
    await _edit_or_send_card(update, context, fmt_help(), home_card_kb())


async def open_status_card_from_command(update, context: CallbackContext, text: str):
    await _edit_or_send_card(update, context, text, home_card_kb())

