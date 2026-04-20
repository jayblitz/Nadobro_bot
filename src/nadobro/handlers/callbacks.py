import logging
import time
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import CallbackContext
from telegram.constants import ParseMode, ChatAction
from src.nadobro.i18n import language_context, get_user_language, localize_text, localize_markup, get_active_language, _ACTIVE_LANG
from src.nadobro.handlers.formatters import (
    escape_md, fmt_alert_menu_intro, fmt_alert_product_prompt, fmt_alert_target_prompt,
    fmt_close_all_confirm, fmt_dashboard_home, fmt_mode_view, fmt_positions,
    fmt_trade_preview, fmt_trade_result,
    fmt_wallet_balance_card, fmt_wallet_balance_error, fmt_wallet_connect_card,
    fmt_wallet_info, fmt_alerts, fmt_portfolio, fmt_wallet_revoke_steps_card,
    fmt_settings, fmt_help, fmt_price, fmt_status_overview, fmt_points_dashboard,
    fmt_trade_history, fmt_analytics,
)
from src.nadobro.handlers.keyboards import (
    persistent_menu_kb, trade_product_kb, trade_size_kb, trade_leverage_kb,
    trade_confirm_kb, positions_kb, wallet_kb, wallet_kb_not_linked, wallet_revoke_confirm_kb, alerts_kb,
    alert_product_kb, alert_condition_kb, alert_delete_kb, settings_kb, settings_leverage_kb,
    settings_slippage_kb, settings_language_kb, close_product_kb, confirm_close_all_kb, back_kb,
    risk_profile_kb, strategy_hub_kb, strategy_action_kb, strategy_product_picker_kb,
    onboarding_language_kb,
    points_scope_kb,
    mode_kb,     home_card_kb, status_kb, portfolio_kb, portfolio_history_kb, portfolio_analytics_kb,
    onboarding_accept_tos_kb,
    copy_hub_kb, copy_trader_preview_kb, copy_budget_kb, copy_risk_kb,
    copy_leverage_kb, copy_confirm_kb, copy_dashboard_kb, copy_admin_menu_kb,
)
from src.nadobro.handlers.trade_card import handle_trade_card_callback, open_trade_card_from_callback
from src.nadobro.handlers.render_utils import plain_text_fallback
from src.nadobro.handlers.wallet_view import build_wallet_view_payload
from src.nadobro.handlers.home_card import (
    build_home_card_text_async,
    build_portfolio_view,
    build_positions_view,
)
from src.nadobro.handlers.state_reset import clear_pending_user_state
from src.nadobro.services.user_service import (
    get_or_create_user, get_user_nado_client, get_user_readonly_client, get_user_wallet_info,
    switch_network, get_user, remove_user_private_key, ensure_active_wallet_ready, update_user_language,
)
from src.nadobro.services.trade_service import (
    execute_market_order, execute_limit_order, close_position,
    close_all_positions, get_trade_history, get_trade_analytics,
)
from src.nadobro.services.alert_service import create_alert, get_user_alerts, delete_alert
from src.nadobro.services.admin_service import is_trading_paused, is_admin
from src.nadobro.services.bot_runtime import stop_user_bot, get_user_bot_status
from src.nadobro.services.settings_service import get_user_settings, update_user_settings
from src.nadobro.services.points_service import (
    get_points_dashboard,
    relay_option_reply_to_lowiqpts,
    relay_user_reply_to_lowiqpts,
    request_points_refresh,
)
from src.nadobro.services.onboarding_service import (
    get_resume_step,
    evaluate_readiness,
    set_new_onboarding_language,
    set_new_onboarding_tos_accepted,
    is_new_onboarding_complete,
    get_new_onboarding_state,
)
from src.nadobro.config import get_product_name, get_product_id, get_product_max_leverage, PRODUCTS, get_perp_products
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.perf import timed_metric, log_slow

logger = logging.getLogger(__name__)


async def _edit_loc(query, text, parse_mode=None, reply_markup=None, **fmt):
    lang = get_active_language()
    localized = localize_text(text, lang)
    if fmt:
        try:
            localized = localized.format(**fmt)
        except (KeyError, ValueError) as e:
            import logging as _logging
            _logging.getLogger("nadobro").warning(
                "_edit_loc template error key=%r lang=%r err=%r", text[:60], lang, e
            )
            localized = text.format(**fmt)
    kwargs = {}
    if parse_mode is not None:
        kwargs["parse_mode"] = parse_mode
    if reply_markup is not None:
        kwargs["reply_markup"] = localize_markup(reply_markup, lang)
    try:
        return await query.edit_message_text(localized, **kwargs)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        if "Can't parse entities" in str(e) and kwargs.get("parse_mode") == ParseMode.MARKDOWN_V2:
            fallback_kwargs = dict(kwargs)
            fallback_kwargs.pop("parse_mode", None)
            try:
                return await query.edit_message_text(plain_text_fallback(localized), **fallback_kwargs)
            except BadRequest as e2:
                if "Message is not modified" in str(e2):
                    return
                raise
        raise


async def handle_callback(update: Update, context: CallbackContext):
    started = time.perf_counter()
    query = update.callback_query
    data = query.data
    if data in ("market:view", "nav:market_radar", "market:radar", "home:market_radar"):
        data = "points:view"
    telegram_id = query.from_user.id

    with language_context(get_user_language(telegram_id)):
      return await _handle_callback_inner(update, context, query, data, telegram_id, started)


async def _handle_callback_inner(update, context, query, data, telegram_id, started):
    try:
        try:
            await query.answer()
        except BadRequest as e:
            # Callback queries expire quickly; ignore stale answers and continue.
            if "Query is too old" not in str(e) and "query id is invalid" not in str(e):
                raise
        await query.message.chat.send_action(ChatAction.TYPING)

        if data.startswith("onb:"):
            await _handle_onb_new(query, data, telegram_id, context)
        elif data.startswith("nav:"):
            await _handle_nav(query, data, telegram_id, context)
        elif data.startswith("card:trade:"):
            await handle_trade_card_callback(update, context, telegram_id, data)
        elif data.startswith("onboarding:"):
            await _handle_onboarding(query, data, telegram_id, context)
        elif data.startswith("trade:"):
            await _handle_trade(query, data, telegram_id, context)
        elif data.startswith("product:"):
            await _handle_product(query, data, telegram_id, context)
        elif data.startswith("size:"):
            await _handle_size(query, data, telegram_id, context)
        elif data.startswith("leverage:"):
            await _handle_leverage(query, data, telegram_id, context)
        elif data.startswith("exec_trade:"):
            await _handle_exec_trade(query, data, telegram_id, context)
        elif data == "cancel_trade":
            context.user_data.pop("pending_trade", None)
            await _show_dashboard(query, telegram_id)
        elif data.startswith("pos:"):
            await _handle_positions(query, data, telegram_id, context)
        elif data.startswith("portfolio:"):
            await _handle_portfolio(query, data, telegram_id)
        elif data.startswith("status:"):
            await _handle_status_callback(query, data, telegram_id)
        elif data.startswith("wallet:"):
            await _handle_wallet(query, data, telegram_id, context)
        elif data.startswith("points:"):
            await _handle_points(query, data, telegram_id, context)
        elif data.startswith("alert:"):
            await _handle_alert(query, data, telegram_id, context)
        elif data.startswith("settings:"):
            await _handle_settings(query, data, telegram_id, context)
        elif data.startswith("strategy:"):
            await _handle_strategy(query, data, context, telegram_id)
        elif data.startswith("copy:"):
            await _handle_copy(query, data, context, telegram_id)
        elif data.startswith("bro:"):
            await _handle_bro(query, data, telegram_id, context)
        elif data.startswith("howl:"):
            await _handle_howl(query, data, telegram_id, context)
        elif data == "home:mode":
            user = get_user(telegram_id)
            current_network = user.network_mode.value if user else "testnet"
            network_label = "🧪 TESTNET" if current_network == "testnet" else "🌐 MAINNET"
            await _edit_loc(query,
                "🌐 *Execution Mode Control*\n\nCurrent Mode: *{label}*\n\nSwitch mode below:",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=mode_kb(current_network),
                label=escape_md(network_label),
            )
        elif data.startswith("mode:"):
            await _handle_mode(query, data, telegram_id, context)
        else:
            await _edit_loc(query,
                "Unknown action\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(),
            )
    except BadRequest as e:
        # Harmless: refresh/navigation edited the message to identical text+keyboard.
        if "Message is not modified" in str(e):
            return
        logger.error(f"Callback BadRequest for '{data}': {e}", exc_info=True)
        try:
            await _edit_loc(query,
                "⚠️ An error occurred\\. Please try again\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(),
            )
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Callback error for '{data}': {e}", exc_info=True)
        try:
            await _edit_loc(query,
                "⚠️ An error occurred\\. Please try again\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(),
            )
        except Exception:
            pass
    finally:
        log_slow("callback.total", threshold_ms=800.0, started_at=started)


# New onboarding (language → ToS) message text
_ONB_WELCOME_LANG_MSG = """Welcome to Nadobro 👋

Trade perps on Nado DEX from Telegram with guided execution, portfolio tools, automation, and AI support.

Pick your language:"""

_ONB_WELCOME_CARD = """🔥 You're in!

By tapping *"Let's Get It"* you accept the Terms of Use & Privacy Policy.

🔐 How it works:
We generate a secure 1CT signing key for your account. Your main wallet keys are never touched. Revoke anytime.

Ready?"""

async def _handle_onb_new(query, data, telegram_id, context):
    if data == "onb:accept_tos":
        set_new_onboarding_tos_accepted(telegram_id)
        await _edit_loc(
            query,
            fmt_dashboard_home(),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=home_card_kb(),
        )
        return
    if data.startswith("onb:lang:"):
        parts = data.split(":")
        if len(parts) < 3:
            return
        lang = parts[2]
        set_new_onboarding_language(telegram_id, lang)
        update_user_language(telegram_id, lang)
        _ACTIVE_LANG.set(lang)
        await _edit_loc(query, _ONB_WELCOME_CARD,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=onboarding_accept_tos_kb(),
        )


async def _show_dashboard(query, telegram_id):
    await _edit_loc(query, await build_home_card_text_async(telegram_id),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=home_card_kb(),
    )


async def _handle_mode(query, data, telegram_id, context=None):
    parts = data.split(":")
    target_network = parts[1] if len(parts) > 1 else ""
    if target_network not in ("testnet", "mainnet"):
        return

    user = get_user(telegram_id)
    current_network = user.network_mode.value if user else "testnet"

    if target_network == current_network:
        network_label = "🧪 TESTNET" if current_network == "testnet" else "🌐 MAINNET"
        try:
            await _edit_loc(query,
                "🌐 *Execution Mode Control*\n\nAlready on *{label}*\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=mode_kb(current_network),
                label=escape_md(network_label),
            )
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                raise
        return

    success, result_msg = switch_network(telegram_id, target_network)
    if success:
        network_label = "🧪 TESTNET" if target_network == "testnet" else "🌐 MAINNET"
        await _edit_loc(query,
            "✅ *Switched to {label}*\n\n{msg}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=mode_kb(target_network),
            label=escape_md(network_label),
            msg=escape_md(result_msg),
        )
    else:
        await _edit_loc(query,
            "❌ {msg}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=mode_kb(current_network),
            msg=escape_md(result_msg),
        )


async def _handle_nav(query, data, telegram_id, context=None):
    target = data.split(":", 1)[1] if ":" in data else "main"

    clear_pending_user_state(context)

    if target in ("main", "refresh"):
        await _show_dashboard(query, telegram_id)
    elif target == "help":
        try:
            await _edit_loc(query, fmt_help(),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(),
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                return
            raise
    elif target == "quick_start":
        await _handle_onboarding(query, "onboarding:resume", telegram_id, context)
    elif target == "strategy_hub":
        await _edit_loc(query,
            "🤖 *Nadobro Strategy Lab*\n\n"
            "Pick a strategy to open its cockpit dashboard, edit parameters, and launch with pre\\-trade analytics\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_hub_kb(),
        )
    elif target == "trade" and context is not None:
        if not is_new_onboarding_complete(telegram_id):
            await _edit_loc(query,
                "⚠️ Complete setup first (language + accept terms).",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶ Complete setup", callback_data="onboarding:resume")],
                    [InlineKeyboardButton("Exit", callback_data="nav:main")],
                ]),
            )
            return
        if await open_trade_card_from_callback(query, context, telegram_id):
            return
    elif target == "ask_nado" and context is not None:
        context.user_data["pending_question"] = True
        await _edit_loc(query,
            "🧠 *Ask NadoBro AI Console*\n\n"
            "Ask me anything about Nado \\(docs, dev docs, API, website, X updates, troubleshooting\\)\\!\n\n"
            "Examples:\n"
            "  • `What is unified margin?`\n"
            "  • `How do liquidations work?`\n"
            "  • `What order types are available?`\n"
            "  • `How do I deposit funds?`\n"
            "  • `What changed recently on Nado X?`\n"
            "  • `How do I sign gateway API requests?`\n"
            "  • `What is the NLP vault?`\n"
            "  • `What are the trading fees?`\n\n"
            "Type your question below:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )
    elif target.startswith("strategy:"):
        await _handle_strategy(query, target, context, telegram_id)
    elif target.startswith("copy:"):
        await _handle_copy(query, target, context, telegram_id)
    elif target.startswith("bro:"):
        await _handle_bro(query, target, telegram_id, context)
    elif target.startswith("settings:"):
        await _handle_settings(query, target, telegram_id, context)
    elif target.startswith("alert:"):
        await _handle_alert(query, target, telegram_id, context)
    elif target.startswith("wallet:"):
        await _handle_wallet(query, target, telegram_id, context)
    elif target.startswith("portfolio:"):
        await _handle_portfolio(query, target, telegram_id)


async def _handle_trade(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    if not is_new_onboarding_complete(telegram_id):
        await _edit_loc(query,
            "⚠️ Complete setup first (language + accept terms).",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("▶ Complete setup", callback_data="onboarding:resume")],
                [InlineKeyboardButton("Exit", callback_data="nav:main")],
            ]),
        )
        return
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
    if action in ("long", "short", "limit_long", "limit_short") and not wallet_ready:
        await _edit_loc(query,
            "⚠️ {msg}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
            msg=escape_md(wallet_msg),
        )
        return

    if action in ("long", "short"):
        action_label = "🟢 BUY / LONG" if action == "long" else "🔴 SELL / SHORT"
        await _edit_loc(query,
            "*{label}*\n\n{select_product}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_product_kb(action, network=network),
            label=escape_md(action_label),
            select_product=localize_text("Select a product:", get_active_language()),
        )
    elif action in ("limit_long", "limit_short"):
        context.user_data["pending_trade"] = {"action": action, "step": "product_select"}
        action_label = "LIMIT LONG" if action == "limit_long" else "LIMIT SHORT"
        await _edit_loc(query, 
            "*{label}*\n\n{select_product}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_product_kb(action, network=network),
            label=escape_md(action_label),
            select_product=localize_text("Select a product:", get_active_language()),
        )
    elif action == "close":
        await _edit_loc(query, 
            "*Close Position*\n\nSelect the product to close:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=close_product_kb(network=network),
        )
    elif action == "close_all":
        await _edit_loc(query, 
            "⚠️ *Close All Positions*\n\nAre you sure you want to close ALL open orders?",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=confirm_close_all_kb(),
        )



async def _handle_product(query, data, telegram_id, context):
    parts = data.split(":")
    if len(parts) < 3:
        return

    action = parts[1]
    product = parts[2]

    if action in ("limit_long", "limit_short"):
        context.user_data["pending_trade"] = {
            "action": action,
            "product": product,
            "step": "limit_input",
        }
        _lang = get_active_language()
        await _edit_loc(query, 
            "*{label} {product}*\n\n{enter_size_price}\n{example}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
            label=escape_md(action.replace('_', ' ').upper()),
            product=escape_md(product),
            enter_size_price=localize_text("Enter size and price:", _lang),
            example=localize_text("Example: `0\\.01 95000`", _lang),
        )
        return

    action_label = "LONG" if action == "long" else "SHORT"
    await _edit_loc(query, 
        "*{label} {product}\\-PERP*\n\n{select_size}",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=trade_size_kb(product, action),
        label=escape_md(action_label),
        product=escape_md(product),
        select_size=localize_text("Select trade size:", get_active_language()),
    )


async def _handle_size(query, data, telegram_id, context):
    parts = data.split(":")
    if len(parts) < 4:
        return

    action = parts[1]
    product = parts[2]
    size_str = parts[3]

    if size_str == "custom":
        context.user_data["pending_trade"] = {
            "action": action,
            "product": product,
            "step": "custom_size",
        }
        _lang = get_active_language()
        await _edit_loc(query, 
            "*{label} {product}\\-PERP*\n\n{prompt}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
            label=escape_md(action.upper()),
            product=escape_md(product),
            prompt=localize_text("Type the trade size \\(e\\.g\\. `0\\.01`\\):", _lang),
        )
        return

    size = float(size_str)
    action_label = "LONG" if action == "long" else "SHORT"
    await _edit_loc(query, 
        "*{label} {size} {product}\\-PERP*\n\n{select_leverage}",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=trade_leverage_kb(product, action, size),
        label=escape_md(action_label),
        size=escape_md(str(size)),
        product=escape_md(product),
        select_leverage=localize_text("Select leverage:", get_active_language()),
    )


async def _handle_leverage(query, data, telegram_id, context):
    parts = data.split(":")
    if len(parts) < 5:
        return

    action = parts[1]
    product = parts[2]
    size = float(parts[3])
    leverage = int(parts[4])
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    max_leverage = get_product_max_leverage(product, network=network)
    if leverage > max_leverage:
        await _edit_loc(query, 
            "⚠️ Max leverage for *{product}* is *{max_lev}x*\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_leverage_kb(product, action, size),
            product=escape_md(product),
            max_lev=escape_md(str(max_leverage)),
        )
        return

    price = 0
    try:
        client = get_user_readonly_client(telegram_id)
        if client:
            user = get_user(telegram_id)
            network = user.network_mode.value if user else "mainnet"
            pid = get_product_id(product, network=network, client=client)
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
    await _edit_loc(query, 
        preview,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=trade_confirm_kb(),
    )


async def _handle_exec_trade(query, data, telegram_id, context):
    pending = context.user_data.get("pending_trade")
    if not pending:
        await _edit_loc(query, 
            "⚠️ No pending trade found\\. Please start again\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )
        return
    if not is_new_onboarding_complete(telegram_id):
        await _edit_loc(query,
            "⚠️ Complete setup first (language + accept terms).",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("▶ Complete setup", callback_data="onboarding:resume")],
                [InlineKeyboardButton("Exit", callback_data="nav:main")],
            ]),
        )
        return

    action = pending.get("action", "long")
    product = pending.get("product", "BTC")
    size = pending.get("size", 0)
    leverage = pending.get("leverage", 1)
    slippage_pct = pending.get("slippage_pct", _get_user_settings(telegram_id, context).get("slippage", 1))

    context.user_data.pop("pending_trade", None)

    if is_trading_paused():
        await _edit_loc(query, 
            "⏸ Trading is temporarily paused by admin\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )
        return
    wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
    if not wallet_ready:
        await _edit_loc(query, 
            f"⚠️ {escape_md(wallet_msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )
        return

    from src.nadobro.handlers.messages import execute_action_directly
    await execute_action_directly(query, context, telegram_id, {
        "type": "exec_trade_callback",
        "pending": {
            "action": action,
            "product": product,
            "size": size,
            "leverage": leverage,
            "slippage_pct": slippage_pct,
            "price": pending.get("price", 0),
        },
    })


async def _handle_positions(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"

    if action == "view":
        with timed_metric("cb.positions.view"):
            msg, reply_markup = await run_blocking(build_positions_view, telegram_id)
        await _edit_loc(query, 
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=reply_markup,
        )

    elif action == "close" and len(parts) >= 3:
        product = parts[2]
        from src.nadobro.handlers.messages import execute_action_directly
        await execute_action_directly(query, context, telegram_id, {"type": "close_position", "product": product})

    elif action == "close_all":
        await _edit_loc(query, 
            fmt_close_all_confirm(),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=confirm_close_all_kb(),
        )

    elif action == "confirm_close_all":
        from src.nadobro.handlers.messages import execute_action_directly
        await execute_action_directly(query, context, telegram_id, {"type": "close_all"})


async def _handle_portfolio(query, data, telegram_id):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"
    user = get_user(telegram_id)
    mode_label = None
    if user:
        mode_label = user.network_mode.value.upper()

    if action == "history":
        try:
            page = max(0, int(parts[2])) if len(parts) > 2 else 0
        except (TypeError, ValueError):
            page = 0
        PAGE_SIZE = 10
        trades = await run_blocking(get_trade_history, telegram_id, limit=500)
        has_more = len(trades) > (page + 1) * PAGE_SIZE
        msg = fmt_trade_history(trades, page=page, page_size=PAGE_SIZE, mode_label=mode_label)
        try:
            await _edit_loc(query,
                msg,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=portfolio_history_kb(page=page, has_more=has_more),
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                return
            raise
        return

    if action == "analytics":
        try:
            stats = await run_blocking(get_trade_analytics, telegram_id)
        except Exception as e:
            logger.warning("portfolio_analytics_failed user=%s err=%s", telegram_id, e)
            stats = {}
        msg = fmt_analytics(stats, mode_label=mode_label)
        try:
            await _edit_loc(query,
                msg,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=portfolio_analytics_kb(),
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                return
            raise
        return

    # Default: portfolio overview
    force_refresh_orders = action == "refresh"
    with timed_metric("cb.portfolio.view"):
        msg, reply_markup = await run_blocking(build_portfolio_view, telegram_id, force_refresh_orders)
    try:
        await _edit_loc(query,
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=reply_markup,
        )
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        raise


async def _handle_status_callback(query, data: str, telegram_id: int):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "refresh"
    if action == "stop":
        ok, msg = stop_user_bot(telegram_id, cancel_orders=True)
        status = await run_blocking(get_user_bot_status, telegram_id)
        onboarding = await run_blocking(evaluate_readiness, telegram_id)
        text = fmt_status_overview(status, onboarding)
        prefix = "🛑" if ok else "⚠️"
        text += f"\n\n{prefix} {escape_md(msg)}"
        with language_context(get_user_language(telegram_id)):
            await _edit_loc(
                query,
                text,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=status_kb(
                    is_running=bool(status.get("running")),
                    strategy_label=str(status.get("strategy") or "").upper() or None,
                ),
            )
        return
    if action != "refresh":
        return
    with language_context(get_user_language(telegram_id)):
        lang = get_active_language()
        status = await run_blocking(get_user_bot_status, telegram_id)
        onboarding = await run_blocking(evaluate_readiness, telegram_id)
        text = fmt_status_overview(status, onboarding)
        localized = localize_text(text, lang)
        try:
            await query.edit_message_text(
                localized,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=localize_markup(
                    status_kb(
                        is_running=bool(status.get("running")),
                        strategy_label=str(status.get("strategy") or "").upper() or None,
                    ),
                    lang,
                ),
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                return
            if "Can't parse entities" in str(e):
                try:
                    await query.edit_message_text(
                        plain_text_fallback(localized),
                        reply_markup=localize_markup(
                            status_kb(
                                is_running=bool(status.get("running")),
                                strategy_label=str(status.get("strategy") or "").upper() or None,
                            ),
                            lang,
                        ),
                    )
                except BadRequest as e2:
                    if "Message is not modified" in str(e2):
                        return
                    raise
                return
            raise


async def _handle_wallet(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"

    if action == "view":
        msg, kb = build_wallet_view_payload(telegram_id, context=context, verify_signer=True)
        await _edit_loc(query, 
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=kb,
        )
    elif action == "balance":
        client = get_user_readonly_client(telegram_id)
        if not client:
            await _edit_loc(query, 
                fmt_wallet_balance_error(),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=wallet_kb(),
            )
            return
        try:
            bal = client.get_balance()
            usdt = (bal.get("balances") or {}).get(0, 0) or (bal.get("balances") or {}).get("0", 0)
            msg = fmt_wallet_balance_card(float(usdt or 0))
        except Exception:
            msg = fmt_wallet_balance_error()
        await _edit_loc(query, msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=wallet_kb())
    elif action == "revoke_steps":
        await _edit_loc(query, fmt_wallet_revoke_steps_card(), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=wallet_revoke_confirm_kb())
    elif action == "revoke_confirm":
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "testnet"
        ok, msg = remove_user_private_key(telegram_id, network)
        success_msg = "✅ Key reset! Your stored signer has been cleared. Tap 👛 Wallet to link a new 1CT key."
        fail_msg = "❌ {msg}"
        if ok:
            await _edit_loc(query, success_msg, parse_mode=ParseMode.MARKDOWN, reply_markup=wallet_kb_not_linked())
        else:
            await _edit_loc(query, fail_msg, parse_mode=ParseMode.MARKDOWN, reply_markup=wallet_kb(), msg=escape_md(msg))
    elif action == "remove_active":
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "testnet"
        ok, msg = remove_user_private_key(telegram_id, network)
        prefix = "✅" if ok else "❌"
        await _edit_loc(query, 
            "{prefix} {msg}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=wallet_kb(),
            prefix=prefix,
            msg=escape_md(msg),
        )
    elif action == "network" and len(parts) >= 3:
        net = parts[2]
        if net not in ("testnet", "mainnet"):
            return

        success, result_msg = switch_network(telegram_id, net)

        if success:
            info = get_user_wallet_info(telegram_id)
            msg = fmt_wallet_info(info)
            await _edit_loc(query, 
                "{switch_msg}\n\n{wallet_info}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=wallet_kb(),
                switch_msg=escape_md(result_msg),
                wallet_info=msg,
            )
        else:
            await _edit_loc(query, 
                "❌ {msg}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=wallet_kb(),
                msg=escape_md(result_msg),
            )


async def _handle_points(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"
    scope = parts[2] if len(parts) > 2 else "week"

    if action == "view":
        payload = await run_blocking(get_points_dashboard, telegram_id, "week")
        await _edit_loc(
            query,
            fmt_points_dashboard(payload),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=points_scope_kb("week"),
        )
        return

    if action == "scope":
        payload = await run_blocking(get_points_dashboard, telegram_id, scope)
        await _edit_loc(
            query,
            fmt_points_dashboard(payload),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=points_scope_kb(scope),
        )
        return

    if action == "cancel":
        relay_result = await relay_user_reply_to_lowiqpts(context, query.message.chat.id, "/cancel")
        if relay_result.get("cancelled"):
            await _edit_loc(
                query,
                "✅ Points request closed\\. Tap *🔄 Refresh* to start again\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=points_scope_kb("week"),
            )
            return
        await _edit_loc(
            query,
            escape_md(relay_result.get("error", "No active LOWIQPTS request to close.")),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=points_scope_kb("week"),
        )
        return

    if action == "replyopt":
        option_index = parts[2] if len(parts) > 2 else "-1"
        relay_result = await relay_option_reply_to_lowiqpts(context, query.message.chat.id, option_index)
        if relay_result.get("ok"):
            choice = str(relay_result.get("choice", "")).strip()
            if choice:
                await context.bot.send_message(
                    chat_id=query.message.chat.id,
                    text=f"↪️ Sent to LOWIQPTS: {choice}",
                )
            return
        await context.bot.send_message(
            chat_id=query.message.chat.id,
            text=str(relay_result.get("error", "Could not send selection."))[:180],
        )
        return

    if action == "refresh":
        result = await request_points_refresh(context, telegram_id, query.message.chat.id)
        if result.get("ok"):
            await _edit_loc(
                query,
                "⏳ Refresh requested\\. I will post your points update when LOWIQPTS replies\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=points_scope_kb("week"),
            )
            return
        await _edit_loc(
            query,
            escape_md(result.get("error", "Could not refresh points right now.")),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=points_scope_kb("week"),
        )
        return


async def _handle_alert(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"

    if action == "menu":
        await _edit_loc(query, 
            fmt_alert_menu_intro(),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=alerts_kb(),
        )

    elif action == "set":
        await _edit_loc(query, 
            fmt_alert_product_prompt(),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=alert_product_kb(network=network),
        )

    elif action == "product" and len(parts) >= 3:
        product = parts[2]
        context.user_data["pending_alert"] = {"product": product}
        await _edit_loc(query,
            fmt_alert_condition_prompt(product),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=alert_condition_kb(product),
        )

    elif action == "cond" and len(parts) >= 4:
        product = parts[2]
        condition = parts[3]
        context.user_data["pending_alert"] = {"product": product, "condition": condition}
        _lang = get_active_language()
        condition_labels = {
            "above": "Price Above",
            "below": "Price Below",
            "funding_above": "Funding Rate Above",
            "funding_below": "Funding Rate Below",
            "pnl_above": "PnL Above",
            "pnl_below": "PnL Below",
        }
        label = condition_labels.get(condition, condition)
        if condition.startswith("funding"):
            example = localize_text("Example: `0.01` (funding rate in %)", _lang)
        elif condition.startswith("pnl"):
            example = localize_text("Example: `50` (PnL in USD)", _lang)
        else:
            example = localize_text("Example: `100000` (price in USD)", _lang)
        await _edit_loc(query,
            fmt_alert_target_prompt(product, label, example),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )

    elif action == "view":
        alerts = get_user_alerts(telegram_id)
        msg = fmt_alerts(alerts)
        kb = alert_delete_kb(alerts) if alerts else back_kb()
        await _edit_loc(query,
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=kb,
        )

    elif action == "del" and len(parts) >= 3:
        alert_id = int(parts[2])
        result = delete_alert(telegram_id, alert_id)

        if result["success"]:
            msg = f"✅ {escape_md(result['message'])}"
        else:
            msg = f"❌ {escape_md(result['error'])}"

        alerts = get_user_alerts(telegram_id)
        alerts_msg = fmt_alerts(alerts)
        final_msg = f"{msg}\n\n{alerts_msg}"

        kb = alert_delete_kb(alerts) if alerts else back_kb()
        await _edit_loc(query, 
            final_msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=kb,
        )


async def _handle_settings(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"

    user_settings = _get_user_settings(telegram_id, context)

    if action == "view":
        msg = fmt_settings(user_settings)
        lev = user_settings.get("default_leverage", 1)
        slip = user_settings.get("slippage", 1)
        await _edit_loc(query, 
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(lev, slip),
        )

    elif action == "leverage_menu":
        await _edit_loc(query, 
            "⚡ *Default Leverage*\n\nChoose the leverage Nadobro should prefill for manual trades\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_leverage_kb(),
        )
    elif action == "risk_menu":
        await _edit_loc(query, 
            "🛡 *Risk Profile*\n\n"
            "This presets leverage and slippage so trades are faster and more consistent\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=risk_profile_kb(),
        )

    elif action == "leverage" and len(parts) >= 3:
        lev = int(parts[2])
        _, user_settings = update_user_settings(
            telegram_id, lambda s: s.update({"default_leverage": lev})
        )
        msg = fmt_settings(user_settings)
        slip = user_settings.get("slippage", 1)
        await _edit_loc(query, 
            "✅ *Default leverage updated*\n\n{settings}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(lev, slip),
            lev=escape_md(str(lev)),
            settings=msg,
        )

    elif action == "slippage_menu":
        await _edit_loc(query, 
            "📊 *Slippage*\n\nChoose the default slippage tolerance used for manual trades\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_slippage_kb(),
        )
    elif action == "language_menu":
        user = get_user(telegram_id)
        lang = (getattr(user, "language", None) or "en").lower()
        await _edit_loc(query, 
            "🌐 *Select Language*\n\nChoose your preferred language for onboarding and UI copy\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_language_kb(lang),
        )
    elif action == "language" and len(parts) >= 3:
        lang = (parts[2] or "").lower()
        supported = {"en", "zh", "fr", "ar", "ru", "ko"}
        if lang not in supported:
            return
        update_user_language(telegram_id, lang)
        _ACTIVE_LANG.set(lang)
        user = get_user(telegram_id)
        current = (getattr(user, "language", None) or lang).lower()
        await _edit_loc(query,
            "✅ *Language updated* to *{lang}*\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            lang=escape_md(lang.upper()),
            reply_markup=settings_language_kb(current),
        )

    elif action == "slippage" and len(parts) >= 3:
        slip = float(parts[2])
        _, user_settings = update_user_settings(
            telegram_id, lambda s: s.update({"slippage": slip})
        )
        msg = fmt_settings(user_settings)
        lev = user_settings.get("default_leverage", 1)
        await _edit_loc(query, 
            "✅ *Slippage updated*\n\n{settings}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(lev, slip),
            slip=escape_md(str(slip)),
            settings=msg,
        )
    elif action == "risk" and len(parts) >= 3:
        profile = parts[2]
        presets = {
            "conservative": {"default_leverage": 2, "slippage": 0.5},
            "balanced": {"default_leverage": 5, "slippage": 1.0},
            "aggressive": {"default_leverage": 10, "slippage": 2.0},
        }
        chosen = presets.get(profile)
        if not chosen:
            return
        _, saved = update_user_settings(
            telegram_id,
            lambda s: s.update(
                {
                    "default_leverage": chosen["default_leverage"],
                    "slippage": chosen["slippage"],
                    "risk_profile": profile,
                }
            ),
        )
        context.user_data["settings"] = saved
        msg = fmt_settings(chosen)
        await _edit_loc(query, 
            "✅ *Risk profile updated* — *{profile}*\n\n{settings}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(chosen["default_leverage"], chosen["slippage"]),
            profile=escape_md(profile.upper()),
            settings=msg,
        )


async def _handle_strategy(query, data, context, telegram_id):
    supported = ("grid", "rgrid", "dn", "vol", "bro")
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    strategy_id = parts[2] if len(parts) > 2 else ""

    if action == "preview":
        if strategy_id not in supported:
            return
        if strategy_id == "bro":
            from src.nadobro.handlers.keyboards import bro_action_kb
            with timed_metric("cb.strategy.preview.bro"):
                preview_text = await run_blocking(_build_bro_preview_text, telegram_id)
            bot_status = get_user_bot_status(telegram_id) or {}
            is_running = bool(bot_status.get("running") and bot_status.get("strategy") == "bro")
            await _edit_loc(query, 
                preview_text,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=bro_action_kb(is_running=is_running),
            )
            return
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "mainnet"
        available_pairs = ("BTC", "ETH") if strategy_id == "dn" else tuple(get_perp_products(network=network) or ("BTC", "ETH", "SOL"))
        selected_product = str(context.user_data.get(f"strategy_pair:{strategy_id}", available_pairs[0]) or available_pairs[0]).upper()
        if selected_product not in available_pairs:
            selected_product = available_pairs[0]
            context.user_data[f"strategy_pair:{strategy_id}"] = selected_product
        with timed_metric("cb.strategy.preview"):
            preview_text = await run_blocking(_build_strategy_preview_text, telegram_id, strategy_id, selected_product)
        bot_status = get_user_bot_status(telegram_id) or {}
        is_running = bool(
            bot_status.get("running")
            and str(bot_status.get("strategy") or "").lower() == strategy_id
        )
        await _edit_loc(query, 
            preview_text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_action_kb(
                strategy_id,
                selected_product,
                list(available_pairs),
                is_running=is_running,
            ),
        )
    elif action == "custom" and len(parts) >= 4:
        strategy_id = parts[2]
        if strategy_id not in supported:
            return
        if strategy_id == "dn":
            return
        try:
            page = int(parts[3])
        except (TypeError, ValueError):
            page = 0
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "mainnet"
        available_pairs = tuple(get_perp_products(network=network) or ("BTC", "ETH", "SOL"))
        selected_product = str(context.user_data.get(f"strategy_pair:{strategy_id}", available_pairs[0]) or available_pairs[0]).upper()
        if selected_product not in available_pairs:
            selected_product = available_pairs[0]
        await _edit_loc(
            query,
            f"🎯 *Select Asset for {escape_md(strategy_id.upper())}*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_product_picker_kb(
                strategy_id=strategy_id,
                selected_product=selected_product,
                available_products=list(available_pairs),
                page=page,
            ),
        )
    elif action == "pair" and len(parts) >= 4:
        strategy_id = parts[2]
        selected_product = parts[3].upper()
        if strategy_id not in supported:
            return
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "mainnet"
        allowed_pairs = ("BTC", "ETH") if strategy_id == "dn" else tuple(get_perp_products(network=network) or ("BTC", "ETH", "SOL"))
        if selected_product not in allowed_pairs:
            return
        context.user_data[f"strategy_pair:{strategy_id}"] = selected_product
        with timed_metric("cb.strategy.preview"):
            preview_text = await run_blocking(_build_strategy_preview_text, telegram_id, strategy_id, selected_product)
        bot_status = get_user_bot_status(telegram_id) or {}
        is_running = bool(
            bot_status.get("running")
            and str(bot_status.get("strategy") or "").lower() == strategy_id
        )
        await _edit_loc(query, 
            preview_text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_action_kb(
                strategy_id,
                selected_product,
                list(allowed_pairs),
                is_running=is_running,
            ),
        )
    elif action == "config":
        if strategy_id not in supported:
            return
        network, settings = get_user_settings(telegram_id)
        conf = settings.get("strategies", {}).get(strategy_id, {})
        context.user_data.pop(f"strategy_config_section:{strategy_id}", None)
        await _edit_loc(query, 
            _strategy_config_menu_text(strategy_id, conf, network),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_strategy_config_menu_kb(strategy_id),
        )
    elif action == "config_section" and len(parts) >= 4:
        section = parts[3]
        if strategy_id not in supported:
            return
        valid_sections = {name for name, _label in _strategy_config_sections(strategy_id)}
        if section not in valid_sections:
            return
        context.user_data[f"strategy_config_section:{strategy_id}"] = section
        network, settings = get_user_settings(telegram_id)
        conf = settings.get("strategies", {}).get(strategy_id, {})
        await _edit_loc(
            query,
            _strategy_config_section_text(strategy_id, conf, network, section),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_strategy_config_section_kb(strategy_id, section),
        )
    elif action == "set" and len(parts) >= 5:
        strategy_id = parts[2]
        field = parts[3]
        raw_value = parts[4]
        if strategy_id not in supported:
            return
        if strategy_id == "vol" and field not in {"tp_pct", "sl_pct"}:
            return
        allowed_numeric_fields = {
            "notional_usd", "spread_bp", "interval_seconds", "tp_pct", "sl_pct",
            "levels", "min_range_pct", "max_range_pct", "threshold_bp", "close_offset_bp",
            "cycle_notional_usd", "session_notional_cap_usd", "inventory_soft_limit_usd",
            "quote_ttl_seconds", "min_spread_bp", "max_spread_bp", "vol_sensitivity",
            "grid_reset_threshold_pct", "grid_reset_timeout_seconds",
            "rgrid_spread_bp", "rgrid_stop_loss_pct", "rgrid_take_profit_pct",
            "rgrid_reset_threshold_pct", "rgrid_reset_timeout_seconds", "rgrid_discretion",
            "auto_close_on_maintenance", "is_long_bias",
        }
        if field not in allowed_numeric_fields:
            return
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return
        limits = {
            "notional_usd": (1, 1000000),
            "spread_bp": (0.1, 200),
            "interval_seconds": (10, 3600),
            "tp_pct": (0.05, 100),
            "sl_pct": (0.05, 100),
            "levels": (1, 20),
            "min_range_pct": (0.1, 20),
            "max_range_pct": (0.1, 40),
            "threshold_bp": (0, 500),
            "close_offset_bp": (1, 1000),
            "cycle_notional_usd": (1, 1000000),
            "session_notional_cap_usd": (0, 10000000),
            "inventory_soft_limit_usd": (1, 1000000),
            "quote_ttl_seconds": (5, 86400),
            "min_spread_bp": (0.1, 200),
            "max_spread_bp": (0.1, 500),
            "vol_sensitivity": (0.0, 1.0),
            "grid_reset_threshold_pct": (0.05, 20),
            "grid_reset_timeout_seconds": (15, 86400),
            "rgrid_spread_bp": (0.1, 200),
            "rgrid_stop_loss_pct": (0.05, 100),
            "rgrid_take_profit_pct": (0.05, 200),
            "rgrid_reset_threshold_pct": (0.05, 20),
            "rgrid_reset_timeout_seconds": (15, 86400),
            "rgrid_discretion": (0.01, 0.5),
            "auto_close_on_maintenance": (0, 1),
            "is_long_bias": (0, 1),
        }
        lo, hi = limits[field]
        if value < lo or value > hi:
            return
        int_fields = {
            "interval_seconds", "levels", "max_open_orders",
            "auto_close_on_maintenance", "is_long_bias", "rgrid_reset_timeout_seconds",
        }

        def _mutate(s):
            strategies = s.setdefault("strategies", {})
            cfg = strategies.setdefault(strategy_id, {})
            if field in int_fields:
                cfg[field] = int(value)
            else:
                cfg[field] = value
            if field == "notional_usd":
                from src.nadobro.services.settings_service import sync_cycle_notional_with_margin

                sync_cycle_notional_with_margin(strategies, strategy_id)

        network, settings = update_user_settings(telegram_id, _mutate)
        conf = settings.get("strategies", {}).get(strategy_id, {})
        section = context.user_data.get(f"strategy_config_section:{strategy_id}") or _strategy_section_for_field(strategy_id, field)
        context.user_data[f"strategy_config_section:{strategy_id}"] = section
        await _edit_loc(query, 
            _strategy_config_section_text(strategy_id, conf, network, section),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_strategy_config_section_kb(strategy_id, section),
        )
    elif action == "set_text" and len(parts) >= 5:
        strategy_id = parts[2]
        field = parts[3]
        raw_value = parts[4]
        if strategy_id not in supported:
            return
        allowed_text = {
            "reference_mode": {"mid", "ema_fast", "ema_slow"},
            "directional_bias": {"neutral", "long_bias", "short_bias"},
            "vol_direction": {"long", "short"},
        }
        allowed_vals = allowed_text.get(field, set())
        if raw_value not in allowed_vals:
            return

        def _mutate(s):
            strategies = s.setdefault("strategies", {})
            cfg = strategies.setdefault(strategy_id, {})
            cfg[field] = raw_value

        network, settings = update_user_settings(telegram_id, _mutate)
        conf = settings.get("strategies", {}).get(strategy_id, {})
        section = context.user_data.get(f"strategy_config_section:{strategy_id}") or _strategy_section_for_field(strategy_id, field)
        context.user_data[f"strategy_config_section:{strategy_id}"] = section
        await _edit_loc(query, 
            _strategy_config_section_text(strategy_id, conf, network, section),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_strategy_config_section_kb(strategy_id, section),
        )
    elif action == "input" and len(parts) >= 4:
        strategy_id = parts[2]
        field = parts[3]
        if strategy_id not in supported:
            return
        allowed_inputs = (
            "notional_usd", "spread_bp", "interval_seconds", "tp_pct", "sl_pct",
            "levels", "min_range_pct", "max_range_pct", "threshold_bp", "close_offset_bp",
            "cycle_notional_usd", "session_notional_cap_usd", "inventory_soft_limit_usd",
            "quote_ttl_seconds", "min_spread_bp", "max_spread_bp", "vol_sensitivity",
            "rgrid_spread_bp", "rgrid_stop_loss_pct", "rgrid_take_profit_pct",
            "rgrid_reset_threshold_pct", "rgrid_reset_timeout_seconds", "rgrid_discretion",
        )
        if strategy_id == "vol" and field not in {"tp_pct", "sl_pct"}:
            return
        if field not in allowed_inputs:
            return
        section = context.user_data.get(f"strategy_config_section:{strategy_id}") or _strategy_section_for_field(strategy_id, field)
        context.user_data[f"strategy_config_section:{strategy_id}"] = section
        context.user_data["pending_strategy_input"] = {
            "strategy": strategy_id,
            "field": field,
            "section": section,
        }
        help_text = {
            "notional_usd": "Enter margin in USD \\(example: `150`\\)",
            "spread_bp": "Enter spread in bps \\(example: `6`\\)",
            "interval_seconds": "Enter loop interval seconds \\(example: `45`\\)",
            "tp_pct": "Enter take profit % \\(example: `1\\.2`\\)",
            "sl_pct": "Enter stop loss % \\(example: `0\\.7`\\)",
            "levels": "Enter grid levels \\(example: `4`\\)",
            "min_range_pct": "Enter min range % \\(example: `1\\.0`\\)",
            "max_range_pct": "Enter max range % \\(example: `2\\.0`\\)",
            "threshold_bp": "Enter threshold in bps \\(example: `0` to disable, or `12`\\)",
            "close_offset_bp": "Enter close offset in bps \\(example: `25`\\)",
            "cycle_notional_usd": "Enter per\\-cycle budget in USD \\(usually same as margin\\)",
            "session_notional_cap_usd": "Enter optional session cap in USD \\(example: `5000`, or `0` to disable\\)",
            "inventory_soft_limit_usd": "Enter inventory soft limit in USD \\(example: `45`\\)",
            "quote_ttl_seconds": "Enter quote TTL seconds \\(example: `90`\\)",
            "min_spread_bp": "Enter minimum spread in bps \\(example: `2`\\)",
            "max_spread_bp": "Enter maximum spread in bps \\(example: `20`\\)",
            "vol_sensitivity": "Enter volatility sensitivity \\(example: `0\\.02`\\)",
            "grid_reset_threshold_pct": "Enter GRID reset threshold % \\(example: `0\\.8`\\)",
            "grid_reset_timeout_seconds": "Enter GRID reset timeout seconds \\(example: `120`\\)",
            "rgrid_spread_bp": "Enter RGRID spread in bps \\(example: `10`\\)",
            "rgrid_stop_loss_pct": "Enter RGRID PnL stop loss % of margin \\(example: `0\\.8`\\)",
            "rgrid_take_profit_pct": "Enter RGRID PnL take profit % of margin \\(example: `1\\.2`\\)",
            "rgrid_reset_threshold_pct": "Enter RGRID reset threshold % \\(example: `1\\.0`\\)",
            "rgrid_reset_timeout_seconds": "Enter RGRID reset timeout seconds \\(example: `120`\\)",
            "rgrid_discretion": "Enter RGRID discretion \\(example: `0\\.06`\\)",
        }
        await _edit_loc(query, 
            f"✏️ *Custom {escape_md(field)}*\n\n"
            f"{help_text.get(field, 'Enter value')}\n\n"
            "Your next message will be used as this value\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(f"strategy:config_section:{strategy_id}:{section}"),
        )
    elif action == "activate":
        context.user_data["active_setup"] = strategy_id
        await _edit_loc(query, 
            f"✅ Active setup is now *{escape_md(strategy_id.upper())}*\\.\n\n"
            "Next: open Buy/Long or Sell/Short and execute with preview\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    elif action == "start" and len(parts) >= 4:
        strategy_id = parts[2]
        product = str(parts[3] or "").upper()
        start_direction = "long"
        if strategy_id == "vol" and len(parts) >= 5:
            start_direction = "short" if str(parts[4]).lower() == "short" else "long"
        if strategy_id not in supported:
            return
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "mainnet"
        available_pairs = ("BTC", "ETH") if strategy_id == "dn" else tuple(get_perp_products(network=network) or ("BTC", "ETH", "SOL"))
        allowed_pairs = set(available_pairs)
        if product not in allowed_pairs:
            await _edit_loc(
                query,
                f"⚠️ {escape_md(product)} is not currently available on {escape_md(network)}\\.\nPlease pick another asset\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=strategy_action_kb(strategy_id, available_pairs[0], list(available_pairs)),
            )
            return
        if not is_new_onboarding_complete(telegram_id):
            await _edit_loc(query, 
                "⚠️ Complete setup first (language + accept terms).",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶ Complete setup", callback_data="onboarding:resume")],
                    [InlineKeyboardButton("Exit", callback_data="nav:main")],
                ]),
            )
            return
        wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
        if not wallet_ready:
            await _edit_loc(query, 
                f"⚠️ {escape_md(wallet_msg)}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(),
            )
            return
        settings = _get_user_settings(telegram_id, context)
        from src.nadobro.handlers.messages import execute_action_directly
        strategy_leverage = 1 if strategy_id == "vol" else settings.get("default_leverage", 3)
        if strategy_id == "dn":
            strategy_leverage = max(1, min(float(strategy_leverage), 5))
        await execute_action_directly(query, context, telegram_id, {
            "type": "start_strategy",
            "strategy": strategy_id,
            "product": product,
            "leverage": strategy_leverage,
            "slippage_pct": settings.get("slippage", 1),
            "direction": start_direction,
        })
    elif action == "status":
        st = get_user_bot_status(telegram_id)
        readiness = evaluate_readiness(telegram_id)
        text = fmt_status_overview(st, readiness)
        await _edit_loc(query, 
            text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=status_kb(
                is_running=bool(st.get("running")),
                strategy_label=str(st.get("strategy") or "").upper() or None,
            ),
        )
    elif action == "stop":
        ok, msg = stop_user_bot(telegram_id, cancel_orders=True)
        st = get_user_bot_status(telegram_id)
        readiness = evaluate_readiness(telegram_id)
        text = fmt_status_overview(st, readiness)
        prefix = "🛑" if ok else "⚠️"
        text += f"\n\n{prefix} {escape_md(msg)}"
        await _edit_loc(query, 
            text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=status_kb(
                is_running=bool(st.get("running")),
                strategy_label=str(st.get("strategy") or "").upper() or None,
            ),
        )


async def _handle_bro(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "config":
        network, settings = get_user_settings(telegram_id)
        conf = settings.get("strategies", {}).get("bro", {})
        context.user_data.pop("bro_config_section", None)
        b_budget = float(conf.get("budget_usd", 500))
        b_risk = conf.get("risk_level", "balanced").upper()
        b_conf_val = float(conf.get("min_confidence", 0.65))
        b_lev = int(conf.get("leverage_cap", 5))
        b_tp = float(conf.get("tp_pct", 2.0))
        b_sl = float(conf.get("sl_pct", 1.5))
        b_maxp = int(conf.get("max_positions", 3))
        b_maxl = float(conf.get("max_loss_pct", 15))
        b_profile = conf.get("bro_profile", "normal").upper()
        profile_emoji = {"CHILL": "😎", "NORMAL": "🤙", "DEGEN": "🔥"}.get(b_profile, "🤙")
        text = (
            "⚙️ *Alpha Agent · Advanced*\n\n"
            f"Preset: {profile_emoji} *{escape_md(b_profile)}*\n"
            f"Budget: *{escape_md(f'${b_budget:,.0f}')}* \\| Risk style: *{escape_md(b_risk)}*\n"
            f"Confidence: *{escape_md(f'{b_conf_val:.0%}')}* \\| Max leverage: *{escape_md(f'{b_lev}x')}*\n"
            f"TP/SL: *{escape_md(f'{b_tp:.1f}%/{b_sl:.1f}%')}* \\| Max positions: *{escape_md(str(b_maxp))}*\n"
            f"Max loss: *{escape_md(f'{b_maxl:.0f}%')}*\n\n"
            "Choose one section below to keep setup simple\\."
        )
        from src.nadobro.handlers.keyboards import bro_config_menu_kb
        await _edit_loc(query, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=bro_config_menu_kb())

    elif action == "config_section" and len(parts) >= 3:
        from src.nadobro.handlers.keyboards import bro_config_section_kb
        section = parts[2]
        if section not in {"preset", "risk", "exits", "risk_style"}:
            return
        context.user_data["bro_config_section"] = section
        _network, settings = get_user_settings(telegram_id)
        conf = settings.get("strategies", {}).get("bro", {})
        if section == "preset":
            text = (
                "⚙️ *Alpha Agent · Preset*\n\n"
                "Pick a personality preset to apply a ready-made risk profile\\."
            )
        elif section == "risk_style":
            text = (
                "⚙️ *Alpha Agent · Risk Style*\n\n"
                f"Current style: *{escape_md(str(conf.get('risk_level', 'balanced')).upper())}*\n\n"
                "Choose how aggressive the AI should trade\\."
            )
        elif section == "risk":
            budget_str = f"${float(conf.get('budget_usd', 500)):,.0f}"
            confidence_str = f"{float(conf.get('min_confidence', 0.65)):.0%}"
            max_leverage_str = f"{int(conf.get('leverage_cap', 5))}x"
            max_positions_str = str(int(conf.get("max_positions", 3)))
            text = (
                "⚙️ *Alpha Agent · Risk*\n\n"
                f"Budget: *{escape_md(budget_str)}* \\| "
                f"Confidence: *{escape_md(confidence_str)}*\n"
                f"Max leverage: *{escape_md(max_leverage_str)}* \\| "
                f"Max positions: *{escape_md(max_positions_str)}*\n\n"
                "Tune the core risk controls here\\."
            )
        else:
            tp_sl_str = f"{float(conf.get('tp_pct', 2.0)):.1f}% / {float(conf.get('sl_pct', 1.5)):.1f}%"
            text = (
                "⚙️ *Alpha Agent · Exits*\n\n"
                f"Current TP/SL: *{escape_md(tp_sl_str)}*\n\n"
                "Set how Alpha Agent locks profit and cuts risk\\."
            )
        await _edit_loc(query, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=bro_config_section_kb(section))

    elif action == "risk" and len(parts) >= 3:
        profile = parts[2]
        presets = {
            "conservative": {"risk_level": "conservative", "leverage_cap": 3, "max_positions": 2, "min_confidence": 0.75, "tp_pct": 1.5, "sl_pct": 1.0},
            "balanced": {"risk_level": "balanced", "leverage_cap": 5, "max_positions": 3, "min_confidence": 0.65, "tp_pct": 2.0, "sl_pct": 1.5},
            "aggressive": {"risk_level": "aggressive", "leverage_cap": 10, "max_positions": 4, "min_confidence": 0.55, "tp_pct": 3.0, "sl_pct": 2.0},
        }
        chosen = presets.get(profile)
        if not chosen:
            return
        def _mutate(s):
            strategies = s.setdefault("strategies", {})
            bro = strategies.setdefault("bro", {})
            bro.update(chosen)
        update_user_settings(telegram_id, _mutate)
        from src.nadobro.handlers.keyboards import bro_config_section_kb
        context.user_data["bro_config_section"] = "risk"
        await _edit_loc(query, 
            f"✅ Bro Mode risk set to *{escape_md(profile.upper())}*\n\n"
            f"Leverage cap: {chosen['leverage_cap']}x \\| Confidence: {chosen['min_confidence']:.0%}\n"
            f"TP/SL: {chosen['tp_pct']:.1f}%/{chosen['sl_pct']:.1f}%\n"
            f"Max positions: {chosen['max_positions']}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=bro_config_section_kb("risk"),
        )

    elif action == "set" and len(parts) >= 3:
        field = parts[2]
        allowed = {"budget_usd", "min_confidence", "leverage_cap", "max_positions", "tp_sl", "risk_level"}
        if field not in allowed:
            return
        if field == "tp_sl":
            context.user_data["pending_bro_input"] = {"field": "tp_sl", "section": "exits"}
            context.user_data["bro_config_section"] = "exits"
            await _edit_loc(query, 
                "✏️ *Set TP/SL*\n\nEnter as `TP,SL` \\(example: `2.0,1.5`\\)",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb("bro:config_section:exits"),
            )
        else:
            section = "risk"
            if field == "risk_level":
                section = "risk_style"
            context.user_data["pending_bro_input"] = {"field": field, "section": section}
            context.user_data["bro_config_section"] = section
            hints = {
                "budget_usd": "Enter budget in USD \\(example: `500`\\)",
                "min_confidence": "Enter min confidence 0\\-1 \\(example: `0.65`\\)",
                "leverage_cap": "Enter max leverage \\(example: `5`\\)",
                "max_positions": "Enter max simultaneous positions \\(example: `3`\\)",
                "risk_level": "Enter: `conservative`, `balanced`, or `aggressive`",
            }
            await _edit_loc(query, 
                f"✏️ *Set {escape_md(field)}*\n\n{hints.get(field, 'Enter value')}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(f"bro:config_section:{section}"),
            )

    elif action == "set_text" and len(parts) >= 4:
        field = parts[2]
        raw_value = parts[3]
        if field != "risk_level" or raw_value not in {"conservative", "balanced", "aggressive"}:
            return
        context.user_data["bro_config_section"] = "risk_style"
        def _mutate(s):
            s.setdefault("strategies", {}).setdefault("bro", {})["risk_level"] = raw_value
        update_user_settings(telegram_id, _mutate)
        from src.nadobro.handlers.keyboards import bro_config_section_kb
        await _edit_loc(
            query,
            f"✅ Risk style set to *{escape_md(raw_value.upper())}*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=bro_config_section_kb("risk_style"),
        )

    elif action == "status":
        from src.nadobro.services.bot_runtime import get_user_bot_status
        from src.nadobro.services.budget_guard import get_budget_status
        from src.nadobro.services.settings_service import get_strategy_settings
        bot_status = get_user_bot_status(telegram_id)
        _, bro_conf = get_strategy_settings(telegram_id, "bro")
        bro_settings = {"budget_usd": bro_conf.get("budget_usd", 500), "risk_level": bro_conf.get("risk_level", "balanced"), "max_loss_pct": bro_conf.get("max_loss_pct", 15)}

        is_running = bot_status.get("running") and bot_status.get("strategy") == "bro"
        status_text = "🟢 ACTIVE" if is_running else "⚪ INACTIVE"
        runs = bot_status.get("runs", 0)
        last_error = bot_status.get("last_error", "")

        budget_status = await run_blocking(get_budget_status, telegram_id, bro_settings)
        exposure = budget_status.get("current_exposure", 0)
        copy_exp = budget_status.get("copy_exposure", 0)
        remaining = budget_status.get("remaining_budget", 0)
        positions = budget_status.get("position_count", 0)
        util = budget_status.get("utilization_pct", 0)

        b_profile = bro_conf.get("bro_profile", "normal").upper()
        profile_emoji = {"CHILL": "😎", "NORMAL": "🤙", "DEGEN": "🔥"}.get(b_profile, "🤙")

        text = (
            f"📊 *Bro Mode Status*\n\n"
            f"Status: {escape_md(status_text)} \\| Profile: {profile_emoji} {escape_md(b_profile)}\n"
            f"Cycles: *{escape_md(str(runs))}*\n"
            f"Exposure: *{escape_md(f'${exposure:,.0f}')}* \\| Copy: *{escape_md(f'${copy_exp:,.0f}')}*\n"
            f"Remaining: *{escape_md(f'${remaining:,.0f}')}* \\| Utilization: *{escape_md(f'{util:.0f}%')}*\n"
            f"Positions: *{escape_md(str(positions))}*\n"
        )
        if last_error:
            text += f"\nLast error: _{escape_md(str(last_error)[:150])}_"
        from src.nadobro.handlers.keyboards import bro_action_kb
        await _edit_loc(query, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=bro_action_kb())

    elif action == "profile" and len(parts) >= 3:
        profile = parts[2]
        if profile not in ("chill", "normal", "degen"):
            return
        from src.nadobro.services.budget_guard import get_bro_profile, BRO_PROFILES
        profile_data = get_bro_profile(profile)
        emoji_map = {"chill": "😎", "normal": "🤙", "degen": "🔥"}

        def _mutate(s):
            strategies = s.setdefault("strategies", {})
            bro = strategies.setdefault("bro", {})
            bro["bro_profile"] = profile
            bro["risk_level"] = profile_data["risk_level"]
            bro["leverage_cap"] = profile_data["leverage_cap"]
            bro["max_positions"] = profile_data["max_positions"]
            bro["min_confidence"] = profile_data["min_confidence"]
            bro["tp_pct"] = profile_data["tp_pct"]
            bro["sl_pct"] = profile_data["sl_pct"]
            bro["max_loss_pct"] = profile_data["max_loss_pct"]

        update_user_settings(telegram_id, _mutate)
        from src.nadobro.handlers.keyboards import bro_config_section_kb
        context.user_data["bro_config_section"] = "preset"
        await _edit_loc(query,
            f"{emoji_map.get(profile, '🤙')} *Bro Profile: {escape_md(profile.upper())}*\n\n"
            f"_{escape_md(profile_data['description'])}_\n\n"
            f"Leverage: {profile_data['leverage_cap']}x \\| Confidence: {profile_data['min_confidence']:.0%}\n"
            f"TP/SL: {profile_data['tp_pct']:.1f}%/{profile_data['sl_pct']:.1f}%\n"
            f"Max positions: {profile_data['max_positions']} \\| Max loss: {profile_data['max_loss_pct']}%",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=bro_config_section_kb("preset"),
        )

    elif action == "explain":
        from src.nadobro.services.budget_guard import get_budget_status
        from src.nadobro.services.settings_service import get_strategy_settings
        from src.nadobro.services.bro_llm import explain_position
        _, bro_conf = get_strategy_settings(telegram_id, "bro")
        bro_settings = {"budget_usd": bro_conf.get("budget_usd", 500), "risk_level": bro_conf.get("risk_level", "balanced"), "max_loss_pct": bro_conf.get("max_loss_pct", 15)}
        budget_status = await run_blocking(get_budget_status, telegram_id, bro_settings)
        positions = budget_status.get("positions", [])
        if not positions:
            from src.nadobro.handlers.keyboards import bro_action_kb
            await _edit_loc(query,
                "🧠 *Why?*\n\nNo open positions to explain\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=bro_action_kb(),
            )
            return

        bot_status = get_user_bot_status(telegram_id)
        bro_state = bot_status.get("bro_state", {}) if bot_status else {}
        trades_log = bro_state.get("trades_log", [])

        explanations = []
        for pos in positions:
            product = pos.get("product", "?")
            side = pos.get("side", "?")
            entry = pos.get("entry_price", 0)
            pnl = pos.get("unrealized_pnl", 0)
            notional = pos.get("notional_usd", 0)

            matching_trade = None
            for t in reversed(trades_log):
                if t.get("product", "").upper() == product.upper() and t.get("side") == side:
                    matching_trade = t
                    break

            reasoning = matching_trade.get("reasoning", "No entry data") if matching_trade else "Opened before current session"
            signals = matching_trade.get("signals", []) if matching_trade else []

            from src.nadobro.services.user_service import get_user_readonly_client as _get_ro
            ro = _get_ro(telegram_id)
            current_price = entry
            if ro:
                try:
                    user = get_user(telegram_id)
                    network = user.network_mode.value if user else "mainnet"
                    pid = get_product_id(product, network=network, client=ro)
                    if pid is not None:
                        mp = ro.get_market_price(pid)
                        current_price = float(mp.get("mid", entry))
                except Exception:
                    pass

            explanation = await run_blocking(
                explain_position,
                product, side, entry, current_price, pnl, reasoning, signals,
            )
            if explanation:
                explanations.append(f"*{escape_md(product)} {escape_md(side.upper())}* \\(${escape_md(f'{notional:.0f}')} PnL=${escape_md(f'{pnl:+.2f}')}\\)\n{escape_md(explanation)}")
            else:
                explanations.append(f"*{escape_md(product)} {escape_md(side.upper())}* \\(${escape_md(f'{notional:.0f}')} PnL=${escape_md(f'{pnl:+.2f}')}\\)\n_{escape_md(reasoning[:150])}_")

        text = "🧠 *Why These Positions?*\n\n" + "\n\n".join(explanations)
        from src.nadobro.handlers.keyboards import bro_action_kb
        await _edit_loc(query, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=bro_action_kb())

    elif action == "gameplan":
        from src.nadobro.services.budget_guard import get_budget_status
        from src.nadobro.services.settings_service import get_strategy_settings
        from src.nadobro.services.bro_llm import generate_game_plan
        _, bro_conf = get_strategy_settings(telegram_id, "bro")
        bro_settings = {"budget_usd": bro_conf.get("budget_usd", 500), "risk_level": bro_conf.get("risk_level", "balanced"), "max_loss_pct": bro_conf.get("max_loss_pct", 15)}
        budget_status = await run_blocking(get_budget_status, telegram_id, bro_settings)
        positions = budget_status.get("positions", [])
        remaining = budget_status.get("remaining_budget", 0)
        budget = bro_conf.get("budget_usd", 500)
        bro_profile = bro_conf.get("bro_profile", "normal")

        bot_status = get_user_bot_status(telegram_id)
        bro_state = bot_status.get("bro_state", {}) if bot_status else {}
        decisions_log = bro_state.get("decisions_log", [])

        plan = await run_blocking(
            generate_game_plan,
            bro_conf.get("products", get_perp_products()[:6] or ["BTC", "ETH", "SOL"]),
            budget, remaining, positions, bro_profile, decisions_log,
        )

        if plan:
            text = f"📋 *Bro's 24h Game Plan*\n\n{escape_md(plan)}"
        else:
            text = "📋 *Game Plan*\n\nCouldn't generate a plan right now\\. Try again later\\."
        from src.nadobro.handlers.keyboards import bro_action_kb
        await _edit_loc(query, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=bro_action_kb())

    elif action == "howl":
        from src.nadobro.services.howl_service import get_pending_howl, format_howl_message
        from src.nadobro.handlers.keyboards import howl_approval_kb
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "mainnet"
        pending = get_pending_howl(telegram_id, network)
        if pending:
            text = format_howl_message(pending)
            suggestions = pending.get("suggestions", [])
            pending_count = sum(1 for s in suggestions if s.get("status", "pending") == "pending")
            await _edit_loc(query, 
                escape_md(text),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=howl_approval_kb(len(suggestions)) if pending_count > 0 else back_kb("strategy:preview:bro"),
            )
        else:
            from src.nadobro.handlers.keyboards import bro_action_kb
            await _edit_loc(query, 
                "🐺 *HOWL*\n\nNo pending optimization suggestions\\.\nHOWL runs nightly and will notify you when it has suggestions\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=bro_action_kb(),
            )


async def _handle_howl(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"

    if action == "approve" and len(parts) >= 3:
        index = int(parts[2])
        from src.nadobro.services.howl_service import approve_howl_suggestion, get_pending_howl, format_howl_message
        ok, msg = approve_howl_suggestion(telegram_id, network, index)
        pending = get_pending_howl(telegram_id, network)
        if pending:
            text = format_howl_message(pending)
            suggestions = pending.get("suggestions", [])
            pending_count = sum(1 for s in suggestions if s.get("status", "pending") == "pending")
            from src.nadobro.handlers.keyboards import howl_approval_kb
            await _edit_loc(query, 
                escape_md(f"{'✅' if ok else '⚠️'} {msg}\n\n{text}"),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=howl_approval_kb(len(suggestions)) if pending_count > 0 else back_kb("strategy:preview:bro"),
            )
        else:
            prefix = "✅" if ok else "⚠️"
            await _edit_loc(query, "{prefix} {msg}", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb("strategy:preview:bro"), prefix=prefix, msg=escape_md(msg))

    elif action == "reject" and len(parts) >= 3:
        index = int(parts[2])
        from src.nadobro.services.howl_service import reject_howl_suggestion, get_pending_howl, format_howl_message
        ok, msg = reject_howl_suggestion(telegram_id, network, index)
        pending = get_pending_howl(telegram_id, network)
        if pending:
            text = format_howl_message(pending)
            suggestions = pending.get("suggestions", [])
            pending_count = sum(1 for s in suggestions if s.get("status", "pending") == "pending")
            from src.nadobro.handlers.keyboards import howl_approval_kb
            await _edit_loc(query, 
                escape_md(f"{'❌' if ok else '⚠️'} {msg}\n\n{text}"),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=howl_approval_kb(len(suggestions)) if pending_count > 0 else back_kb("strategy:preview:bro"),
            )
        else:
            prefix = "❌" if ok else "⚠️"
            await _edit_loc(query, "{prefix} {msg}", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb("strategy:preview:bro"), prefix=prefix, msg=escape_md(msg))

    elif action == "approve_all":
        from src.nadobro.services.howl_service import approve_howl_suggestion, get_pending_howl
        pending = get_pending_howl(telegram_id, network)
        if pending:
            results = []
            for i, s in enumerate(pending.get("suggestions", [])):
                if s.get("status", "pending") == "pending":
                    ok, msg = approve_howl_suggestion(telegram_id, network, i)
                    results.append(f"{'✅' if ok else '⚠️'} {msg}")
            text = "\n".join(results) if results else "No pending suggestions"
            await _edit_loc(query, escape_md(text), parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb("strategy:preview:bro"))
        else:
            await _edit_loc(query, "No pending HOWL suggestions\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb("strategy:preview:bro"))

    elif action == "dismiss":
        from src.nadobro.services.howl_service import dismiss_all_howl
        dismiss_all_howl(telegram_id, network)
        await _edit_loc(query, "🐺 HOWL suggestions dismissed\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb("strategy:preview:bro"))


def _get_user_settings(telegram_id: int, context: CallbackContext) -> dict:
    from src.nadobro.handlers import shared_get_user_settings
    return shared_get_user_settings(telegram_id, context)


def _fmt_strategy_config_text(strategy: str, conf: dict, network: str) -> str:
    if strategy == "vol":
        tp_pct = float(conf.get("tp_pct", 1.0))
        sl_pct = float(conf.get("sl_pct", 1.0))
        direction = "SHORT" if str(conf.get("vol_direction", "long")).lower() == "short" else "LONG"
        return (
            "⚙️ *VOL*\n\n"
            f"Mode: *{escape_md(network.upper())}*\n"
            f"Fixed margin: *{escape_md('$100.00')}* · Fixed leverage: *{escape_md('1x')}*\n"
            f"Direction: *{escape_md(direction)}*\n"
            "Entry: *Limit @ mid* · Exit: *Market close after 60s from fill*\n"
            f"Session TP/SL: *{escape_md(f'{tp_pct:.2f}%/{sl_pct:.2f}%')}* of fixed margin\n\n"
            "Use controls below to change direction or TP/SL only\\."
        )

    notional = float(conf.get("notional_usd", 100.0))
    spread_bp = float(conf.get("spread_bp", 5.0))
    if strategy == "rgrid":
        spread_bp = float(conf.get("rgrid_spread_bp", conf.get("grid_spread_bp", spread_bp)))
    interval_seconds = int(conf.get("interval_seconds", 60))
    tp_pct = float(conf.get("tp_pct", 1.0))
    sl_pct = float(conf.get("sl_pct", 0.5))
    base = (
        f"⚙️ *{escape_md(strategy.upper())}*\n\n"
        f"Mode: *{escape_md(network.upper())}*\n"
        f"Margin: *{escape_md(f'${notional:,.2f}')}* · Spread: *{escape_md(f'{spread_bp:.1f} bp')}*\n"
        f"Interval: *{escape_md(f'{interval_seconds}s')}*"
    )
    if strategy != "rgrid":
        base += f" · TP/SL: *{escape_md(f'{tp_pct:.2f}%/{sl_pct:.2f}%')}*"
    base += "\n\n"
    extra = ""
    if strategy == "rgrid":
        grid_sl = float(conf.get("rgrid_stop_loss_pct", conf.get("grid_stop_loss_pct", sl_pct)))
        grid_tp = float(conf.get("rgrid_take_profit_pct", conf.get("grid_take_profit_pct", tp_pct)))
        grid_discretion = float(conf.get("rgrid_discretion", conf.get("grid_discretion", 0.06)))
        reset_threshold = float(conf.get("rgrid_reset_threshold_pct", conf.get("grid_reset_threshold_pct", 1.0)))
        reset_timeout = int(conf.get("rgrid_reset_timeout_seconds", conf.get("grid_reset_timeout_seconds", 120)))
        spread_hint = "Reverse breakout width"
        extra = (
            f"Reverse Levels: *{escape_md(str(int(conf.get('levels', 4))))}* \\| "
            f"Spread mode: *{escape_md(spread_hint)}*\n"
            f"PnL SL/TP: *{escape_md(f'{grid_sl:.2f}% / {grid_tp:.2f}%')}* \\| "
            f"Reset: *{escape_md(f'{reset_threshold:.2f}% / {reset_timeout}s')}*\n"
            f"Discretion: *{escape_md(f'{grid_discretion:.2f}')}*\n\n"
        )
    elif strategy == "grid":
        threshold = f"{float(conf.get('threshold_bp', 12.0)):.1f} bp"
        close_offset = f"{float(conf.get('close_offset_bp', 24.0)):.1f} bp"
        ref_mode = str(conf.get("reference_mode", "ema_fast")).upper()
        bias = str(conf.get("directional_bias", "neutral")).upper()
        cycle_notional = float(conf.get("cycle_notional_usd", notional))
        session_cap = float(conf.get("session_notional_cap_usd", 0) or 0)
        inv_soft = float(conf.get("inventory_soft_limit_usd", notional * 0.6))
        quote_ttl = int(conf.get("quote_ttl_seconds", 90))
        min_spread = float(conf.get("min_spread_bp", 2.0))
        max_spread = float(conf.get("max_spread_bp", 20.0))
        vol_sensitivity = float(conf.get("vol_sensitivity", 0.02))
        cap_str = f"${session_cap:,.0f}" if session_cap > 0 else "OFF"
        spread_band = f"{min_spread:.1f} - {max_spread:.1f} bp"
        extra = (
            f"Move to quote: *{escape_md(threshold)}* · Close offset: *{escape_md(close_offset)}*\n"
            f"Ref: *{escape_md(ref_mode)}* · Bias: *{escape_md(bias)}*\n"
            f"Per\\-cycle budget: *{escape_md(f'${cycle_notional:,.0f}')}* · Cap: *{escape_md(cap_str)}*\n"
            f"Inventory limit: *{escape_md(f'${inv_soft:,.0f}')}* · Quote TTL: *{escape_md(f'{quote_ttl}s')}*\n"
            f"Spread band: *{escape_md(spread_band)}* · Vol: *{escape_md(f'{vol_sensitivity:.3f}')}*\n\n"
        )
    elif strategy == "dn":
        auto_close = "ON" if float(conf.get("auto_close_on_maintenance", 1) or 0) >= 0.5 else "OFF"
        extra = (
            "Hedge model: *Spot long + matching perp short* \\(BTC/ETH only\\)\n"
            "Leverage cap: *1x to 5x* \\(used on perp leg\\)\n"
            f"Auto-close on maintenance: *{escape_md(auto_close)}*\n\n"
        )
    return base + extra + "Use presets or set custom values below\\."


def _strategy_config_default_section(strategy: str) -> str:
    return "direction" if strategy == "vol" else "setup"


def _strategy_config_sections(strategy: str) -> list[tuple[str, str]]:
    if strategy == "vol":
        return [("direction", "🎯 Direction"), ("risk", "🛡 TP / SL")]
    if strategy == "grid":
        return [("setup", "⚙️ Core"), ("execution", "🧠 Execution"), ("risk", "🛡 Risk")]
    if strategy == "rgrid":
        return [("setup", "⚙️ Core"), ("risk", "🛡 Risk"), ("reset", "🔄 Reset")]
    if strategy == "dn":
        return [("setup", "⚙️ Core"), ("safety", "🛡 Safety")]
    return [("setup", "⚙️ Core")]


def _strategy_section_for_field(strategy: str, field: str) -> str:
    if strategy == "vol":
        return "direction" if field == "vol_direction" else "risk"
    if strategy == "grid":
        if field in {"threshold_bp", "close_offset_bp", "reference_mode", "directional_bias"}:
            return "execution"
        if field in {"cycle_notional_usd", "inventory_soft_limit_usd", "quote_ttl_seconds", "session_notional_cap_usd", "min_spread_bp", "max_spread_bp", "vol_sensitivity"}:
            return "risk"
        return "setup"
    if strategy == "rgrid":
        if field in {"rgrid_reset_threshold_pct", "rgrid_reset_timeout_seconds", "rgrid_discretion"}:
            return "reset"
        if field in {"rgrid_stop_loss_pct", "rgrid_take_profit_pct"}:
            return "risk"
        return "setup"
    if strategy == "dn":
        return "safety" if field == "auto_close_on_maintenance" else "setup"
    return "setup"


def _strategy_config_menu_text(strategy: str, conf: dict, network: str) -> str:
    titles = {
        "grid": "GRID",
        "rgrid": "Reverse GRID",
        "dn": "Mirror Delta Neutral",
        "vol": "Volume Bot",
    }
    return (
        f"⚙️ *{escape_md(titles.get(strategy, strategy.upper()))} Advanced*\n\n"
        f"Mode: *{escape_md(network.upper())}*\n\n"
        "Choose one section below to edit\\. This keeps the setup clean and focused\\."
    )


def _strategy_config_menu_kb(strategy: str):
    rows = []
    section_buttons = [
        InlineKeyboardButton(label, callback_data=f"strategy:config_section:{strategy}:{section}")
        for section, label in _strategy_config_sections(strategy)
    ]
    for i in range(0, len(section_buttons), 2):
        rows.append(section_buttons[i:i + 2])
    rows.append([InlineKeyboardButton("◀ Back", callback_data=f"strategy:preview:{strategy}")])
    return InlineKeyboardMarkup(rows)


def _strategy_config_section_text(strategy: str, conf: dict, network: str, section: str) -> str:
    if strategy == "vol":
        direction = "SHORT" if str(conf.get("vol_direction", "long")).lower() == "short" else "LONG"
        tp_pct = float(conf.get("tp_pct", 1.0))
        sl_pct = float(conf.get("sl_pct", 1.0))
        if section == "direction":
            return (
                "⚙️ *Volume Bot · Direction*\n\n"
                f"Mode: *{escape_md(network.upper())}*\n"
                f"Current direction: *{escape_md(direction)}*\n\n"
                "Pick the side you want the volume loop to favor\\."
            )
        return (
            "⚙️ *Volume Bot · TP / SL*\n\n"
            f"Current TP/SL: *{escape_md(f'{tp_pct:.2f}% / {sl_pct:.2f}%')}*\n\n"
            "Choose quick presets or set custom values\\."
        )

    notional = float(conf.get("notional_usd", 100.0))
    spread_bp = float(conf.get("spread_bp", 5.0))
    interval_seconds = int(conf.get("interval_seconds", 60))
    tp_pct = float(conf.get("tp_pct", 1.0))
    sl_pct = float(conf.get("sl_pct", 0.5))

    if strategy == "grid":
        if section == "execution":
            threshold_str = f"{float(conf.get('threshold_bp', 12.0)):.1f} bp"
            close_offset_str = f"{float(conf.get('close_offset_bp', 24.0)):.1f} bp"
            ref_mode = str(conf.get("reference_mode", "ema_fast")).upper()
            bias = str(conf.get("directional_bias", "neutral")).upper()
            return (
                "⚙️ *GRID · Execution*\n\n"
                f"Threshold: *{escape_md(threshold_str)}* \\| "
                f"Close offset: *{escape_md(close_offset_str)}*\n"
                f"Reference: *{escape_md(ref_mode)}* \\| "
                f"Bias: *{escape_md(bias)}*\n\n"
                "Tune how quotes react to the market\\."
            )
        if section == "risk":
            cycle_budget = f"${float(conf.get('cycle_notional_usd', notional)):,.0f}"
            inventory_limit = f"${float(conf.get('inventory_soft_limit_usd', notional * 0.6)):,.0f}"
            ttl_str = f"{int(conf.get('quote_ttl_seconds', 90))}s"
            session_cap_value = float(conf.get("session_notional_cap_usd", 0) or 0)
            session_cap = f"${session_cap_value:,.0f}" if session_cap_value > 0 else "OFF"
            return (
                "⚙️ *GRID · Risk*\n\n"
                f"TP/SL: *{escape_md(f'{tp_pct:.2f}% / {sl_pct:.2f}%')}*\n"
                f"Cycle budget: *{escape_md(cycle_budget)}* \\| "
                f"Inventory limit: *{escape_md(inventory_limit)}*\n"
                f"TTL: *{escape_md(ttl_str)}* \\| "
                f"Session cap: *{escape_md(session_cap)}*\n\n"
                "Control downside and pacing here\\."
            )
        return (
            "⚙️ *GRID · Core*\n\n"
            f"Margin: *{escape_md(f'${notional:,.0f}')}* \\| Spread: *{escape_md(f'{spread_bp:.1f} bp')}* \\| Interval: *{escape_md(f'{interval_seconds}s')}*\n\n"
            "Set the main loop size and cadence\\."
        )

    if strategy == "rgrid":
        if section == "risk":
            pnl_sl = f"{float(conf.get('rgrid_stop_loss_pct', sl_pct)):.2f}%"
            pnl_tp = f"{float(conf.get('rgrid_take_profit_pct', tp_pct)):.2f}%"
            return (
                "⚙️ *Reverse GRID · Risk*\n\n"
                f"PnL stop: *{escape_md(pnl_sl)}* \\| "
                f"PnL take profit: *{escape_md(pnl_tp)}*\n\n"
                "Set when the strategy should cut or lock gains\\."
            )
        if section == "reset":
            reset_threshold = f"{float(conf.get('rgrid_reset_threshold_pct', 1.0)):.2f}%"
            reset_timeout = f"{int(conf.get('rgrid_reset_timeout_seconds', 120))}s"
            discretion = f"{float(conf.get('rgrid_discretion', 0.06)):.2f}"
            return (
                "⚙️ *Reverse GRID · Reset*\n\n"
                f"Reset threshold: *{escape_md(reset_threshold)}* \\| "
                f"Timeout: *{escape_md(reset_timeout)}*\n"
                f"Discretion: *{escape_md(discretion)}*\n\n"
                "Use these only if you want tighter re-anchoring\\."
            )
        levels = str(int(conf.get("levels", 4)))
        rgrid_spread = f"{float(conf.get('rgrid_spread_bp', spread_bp)):.1f} bp"
        return (
            "⚙️ *Reverse GRID · Core*\n\n"
            f"Margin: *{escape_md(f'${notional:,.0f}')}* \\| Interval: *{escape_md(f'{interval_seconds}s')}*\n"
            f"Levels: *{escape_md(levels)}* \\| Spread: *{escape_md(rgrid_spread)}*\n\n"
            "Set the basic breakout loop here\\."
        )

    if strategy == "dn":
        if section == "safety":
            auto_close = "ON" if float(conf.get("auto_close_on_maintenance", 1) or 0) >= 0.5 else "OFF"
            return (
                "⚙️ *Mirror Delta Neutral · Safety*\n\n"
                f"Auto-close on maintenance: *{escape_md(auto_close)}*\n\n"
                "This decides whether DN exits automatically during maintenance windows\\."
            )
        return (
            "⚙️ *Mirror Delta Neutral · Core*\n\n"
            f"Margin: *{escape_md(f'${notional:,.0f}')}* \\| Spread: *{escape_md(f'{spread_bp:.1f} bp')}* \\| Interval: *{escape_md(f'{interval_seconds}s')}*\n"
            f"TP/SL: *{escape_md(f'{tp_pct:.2f}% / {sl_pct:.2f}%')}*\n\n"
            "Keep this simple and hedge-focused\\."
        )

    return _fmt_strategy_config_text(strategy, conf, network)


def _strategy_config_section_kb(strategy: str, section: str):
    if strategy == "vol":
        if section == "direction":
            rows = [[
                InlineKeyboardButton("LONG", callback_data="strategy:set_text:vol:vol_direction:long"),
                InlineKeyboardButton("SHORT", callback_data="strategy:set_text:vol:vol_direction:short"),
            ]]
        else:
            rows = [
                [
                    InlineKeyboardButton("TP 0.5%", callback_data="strategy:set:vol:tp_pct:0.5"),
                    InlineKeyboardButton("TP 1.0%", callback_data="strategy:set:vol:tp_pct:1.0"),
                    InlineKeyboardButton("TP 2.0%", callback_data="strategy:set:vol:tp_pct:2.0"),
                ],
                [
                    InlineKeyboardButton("SL 0.5%", callback_data="strategy:set:vol:sl_pct:0.5"),
                    InlineKeyboardButton("SL 1.0%", callback_data="strategy:set:vol:sl_pct:1.0"),
                    InlineKeyboardButton("SL 2.0%", callback_data="strategy:set:vol:sl_pct:2.0"),
                ],
                [
                    InlineKeyboardButton("Custom TP", callback_data="strategy:input:vol:tp_pct"),
                    InlineKeyboardButton("Custom SL", callback_data="strategy:input:vol:sl_pct"),
                ],
            ]
        rows.append([InlineKeyboardButton("◀ Back", callback_data="strategy:config:vol")])
        return InlineKeyboardMarkup(rows)

    rows: list[list[InlineKeyboardButton]] = []
    if strategy == "grid":
        if section == "setup":
            rows = [
                [
                    InlineKeyboardButton("Margin $50", callback_data="strategy:set:grid:notional_usd:50"),
                    InlineKeyboardButton("Margin $100", callback_data="strategy:set:grid:notional_usd:100"),
                    InlineKeyboardButton("Margin $250", callback_data="strategy:set:grid:notional_usd:250"),
                ],
                [
                    InlineKeyboardButton("Spread 2bp", callback_data="strategy:set:grid:spread_bp:2"),
                    InlineKeyboardButton("Spread 5bp", callback_data="strategy:set:grid:spread_bp:5"),
                    InlineKeyboardButton("Spread 10bp", callback_data="strategy:set:grid:spread_bp:10"),
                ],
                [
                    InlineKeyboardButton("30s", callback_data="strategy:set:grid:interval_seconds:30"),
                    InlineKeyboardButton("60s", callback_data="strategy:set:grid:interval_seconds:60"),
                    InlineKeyboardButton("120s", callback_data="strategy:set:grid:interval_seconds:120"),
                ],
                [
                    InlineKeyboardButton("Custom Margin", callback_data="strategy:input:grid:notional_usd"),
                    InlineKeyboardButton("Custom Interval", callback_data="strategy:input:grid:interval_seconds"),
                ],
            ]
        elif section == "execution":
            rows = [
                [
                    InlineKeyboardButton("Threshold 8bp", callback_data="strategy:set:grid:threshold_bp:8"),
                    InlineKeyboardButton("12bp", callback_data="strategy:set:grid:threshold_bp:12"),
                    InlineKeyboardButton("20bp", callback_data="strategy:set:grid:threshold_bp:20"),
                ],
                [
                    InlineKeyboardButton("Close 20bp", callback_data="strategy:set:grid:close_offset_bp:20"),
                    InlineKeyboardButton("30bp", callback_data="strategy:set:grid:close_offset_bp:30"),
                ],
                [
                    InlineKeyboardButton("Ref MID", callback_data="strategy:set_text:grid:reference_mode:mid"),
                    InlineKeyboardButton("EMA Fast", callback_data="strategy:set_text:grid:reference_mode:ema_fast"),
                    InlineKeyboardButton("EMA Slow", callback_data="strategy:set_text:grid:reference_mode:ema_slow"),
                ],
                [
                    InlineKeyboardButton("Bias Neutral", callback_data="strategy:set_text:grid:directional_bias:neutral"),
                    InlineKeyboardButton("Bias Long", callback_data="strategy:set_text:grid:directional_bias:long_bias"),
                    InlineKeyboardButton("Bias Short", callback_data="strategy:set_text:grid:directional_bias:short_bias"),
                ],
                [
                    InlineKeyboardButton("Custom Threshold", callback_data="strategy:input:grid:threshold_bp"),
                    InlineKeyboardButton("Custom Close", callback_data="strategy:input:grid:close_offset_bp"),
                ],
            ]
        else:
            rows = [
                [
                    InlineKeyboardButton("TP 0.5%", callback_data="strategy:set:grid:tp_pct:0.5"),
                    InlineKeyboardButton("TP 1.0%", callback_data="strategy:set:grid:tp_pct:1.0"),
                    InlineKeyboardButton("TP 2.0%", callback_data="strategy:set:grid:tp_pct:2.0"),
                ],
                [
                    InlineKeyboardButton("SL 0.25%", callback_data="strategy:set:grid:sl_pct:0.25"),
                    InlineKeyboardButton("SL 0.5%", callback_data="strategy:set:grid:sl_pct:0.5"),
                    InlineKeyboardButton("SL 1.0%", callback_data="strategy:set:grid:sl_pct:1.0"),
                ],
                [
                    InlineKeyboardButton("Cycle $50", callback_data="strategy:set:grid:cycle_notional_usd:50"),
                    InlineKeyboardButton("Cycle $100", callback_data="strategy:set:grid:cycle_notional_usd:100"),
                    InlineKeyboardButton("Cycle $250", callback_data="strategy:set:grid:cycle_notional_usd:250"),
                ],
                [
                    InlineKeyboardButton("Inv $30", callback_data="strategy:set:grid:inventory_soft_limit_usd:30"),
                    InlineKeyboardButton("Inv $60", callback_data="strategy:set:grid:inventory_soft_limit_usd:60"),
                    InlineKeyboardButton("TTL 90s", callback_data="strategy:set:grid:quote_ttl_seconds:90"),
                ],
                [
                    InlineKeyboardButton("Reset 0.8%", callback_data="strategy:set:grid:grid_reset_threshold_pct:0.8"),
                    InlineKeyboardButton("1.5%", callback_data="strategy:set:grid:grid_reset_threshold_pct:1.5"),
                ],
                [
                    InlineKeyboardButton("Reset 120s", callback_data="strategy:set:grid:grid_reset_timeout_seconds:120"),
                    InlineKeyboardButton("300s", callback_data="strategy:set:grid:grid_reset_timeout_seconds:300"),
                ],
                [
                    InlineKeyboardButton("Custom TP", callback_data="strategy:input:grid:tp_pct"),
                    InlineKeyboardButton("Custom SL", callback_data="strategy:input:grid:sl_pct"),
                ],
                [
                    InlineKeyboardButton("Session Cap", callback_data="strategy:input:grid:session_notional_cap_usd"),
                    InlineKeyboardButton("Custom Reset", callback_data="strategy:input:grid:grid_reset_threshold_pct"),
                ],
            ]
    elif strategy == "rgrid":
        if section == "setup":
            rows = [
                [
                    InlineKeyboardButton("Margin $50", callback_data="strategy:set:rgrid:notional_usd:50"),
                    InlineKeyboardButton("Margin $100", callback_data="strategy:set:rgrid:notional_usd:100"),
                    InlineKeyboardButton("Margin $250", callback_data="strategy:set:rgrid:notional_usd:250"),
                ],
                [
                    InlineKeyboardButton("Levels 3", callback_data="strategy:set:rgrid:levels:3"),
                    InlineKeyboardButton("Levels 5", callback_data="strategy:set:rgrid:levels:5"),
                    InlineKeyboardButton("Levels 7", callback_data="strategy:set:rgrid:levels:7"),
                ],
                [
                    InlineKeyboardButton("Spread 5bp", callback_data="strategy:set:rgrid:rgrid_spread_bp:5"),
                    InlineKeyboardButton("10bp", callback_data="strategy:set:rgrid:rgrid_spread_bp:10"),
                    InlineKeyboardButton("20bp", callback_data="strategy:set:rgrid:rgrid_spread_bp:20"),
                ],
                [
                    InlineKeyboardButton("60s", callback_data="strategy:set:rgrid:interval_seconds:60"),
                    InlineKeyboardButton("120s", callback_data="strategy:set:rgrid:interval_seconds:120"),
                ],
                [
                    InlineKeyboardButton("Custom Levels", callback_data="strategy:input:rgrid:levels"),
                    InlineKeyboardButton("Custom Spread", callback_data="strategy:input:rgrid:rgrid_spread_bp"),
                ],
                [
                    InlineKeyboardButton("Custom Margin", callback_data="strategy:input:rgrid:notional_usd"),
                    InlineKeyboardButton("Custom Interval", callback_data="strategy:input:rgrid:interval_seconds"),
                ],
            ]
        elif section == "risk":
            rows = [
                [
                    InlineKeyboardButton("PnL SL 0.5%", callback_data="strategy:set:rgrid:rgrid_stop_loss_pct:0.5"),
                    InlineKeyboardButton("1.0%", callback_data="strategy:set:rgrid:rgrid_stop_loss_pct:1.0"),
                ],
                [
                    InlineKeyboardButton("PnL TP 1.5%", callback_data="strategy:set:rgrid:rgrid_take_profit_pct:1.5"),
                    InlineKeyboardButton("2.0%", callback_data="strategy:set:rgrid:rgrid_take_profit_pct:2.0"),
                ],
                [
                    InlineKeyboardButton("Custom PnL SL", callback_data="strategy:input:rgrid:rgrid_stop_loss_pct"),
                    InlineKeyboardButton("Custom PnL TP", callback_data="strategy:input:rgrid:rgrid_take_profit_pct"),
                ],
            ]
        else:
            rows = [
                [
                    InlineKeyboardButton("Reset 0.8%", callback_data="strategy:set:rgrid:rgrid_reset_threshold_pct:0.8"),
                    InlineKeyboardButton("1.5%", callback_data="strategy:set:rgrid:rgrid_reset_threshold_pct:1.5"),
                ],
                [
                    InlineKeyboardButton("Timeout 120s", callback_data="strategy:set:rgrid:rgrid_reset_timeout_seconds:120"),
                    InlineKeyboardButton("300s", callback_data="strategy:set:rgrid:rgrid_reset_timeout_seconds:300"),
                ],
                [
                    InlineKeyboardButton("Disc 0.06", callback_data="strategy:set:rgrid:rgrid_discretion:0.06"),
                    InlineKeyboardButton("Disc 0.10", callback_data="strategy:set:rgrid:rgrid_discretion:0.10"),
                ],
                [
                    InlineKeyboardButton("Custom Reset", callback_data="strategy:input:rgrid:rgrid_reset_threshold_pct"),
                    InlineKeyboardButton("Custom Disc", callback_data="strategy:input:rgrid:rgrid_discretion"),
                ],
            ]
    elif strategy == "dn":
        if section == "setup":
            rows = [
                [
                    InlineKeyboardButton("Margin $50", callback_data="strategy:set:dn:notional_usd:50"),
                    InlineKeyboardButton("Margin $100", callback_data="strategy:set:dn:notional_usd:100"),
                    InlineKeyboardButton("Margin $250", callback_data="strategy:set:dn:notional_usd:250"),
                ],
                [
                    InlineKeyboardButton("30s", callback_data="strategy:set:dn:interval_seconds:30"),
                    InlineKeyboardButton("60s", callback_data="strategy:set:dn:interval_seconds:60"),
                    InlineKeyboardButton("120s", callback_data="strategy:set:dn:interval_seconds:120"),
                ],
                [
                    InlineKeyboardButton("TP 0.5%", callback_data="strategy:set:dn:tp_pct:0.5"),
                    InlineKeyboardButton("TP 1.0%", callback_data="strategy:set:dn:tp_pct:1.0"),
                    InlineKeyboardButton("TP 2.0%", callback_data="strategy:set:dn:tp_pct:2.0"),
                ],
                [
                    InlineKeyboardButton("SL 0.25%", callback_data="strategy:set:dn:sl_pct:0.25"),
                    InlineKeyboardButton("SL 0.5%", callback_data="strategy:set:dn:sl_pct:0.5"),
                    InlineKeyboardButton("SL 1.0%", callback_data="strategy:set:dn:sl_pct:1.0"),
                ],
                [
                    InlineKeyboardButton("Custom Margin", callback_data="strategy:input:dn:notional_usd"),
                    InlineKeyboardButton("Custom Interval", callback_data="strategy:input:dn:interval_seconds"),
                ],
            ]
        else:
            rows = [[
                InlineKeyboardButton("Auto-Close ON", callback_data="strategy:set:dn:auto_close_on_maintenance:1"),
                InlineKeyboardButton("Auto-Close OFF", callback_data="strategy:set:dn:auto_close_on_maintenance:0"),
            ]]

    rows.append([InlineKeyboardButton("◀ Back", callback_data=f"strategy:config:{strategy}")])
    return InlineKeyboardMarkup(rows)


def _build_bro_preview_text(telegram_id: int) -> str:
    network, settings = get_user_settings(telegram_id)
    conf = settings.get("strategies", {}).get("bro", {})
    budget = float(conf.get("budget_usd", 500))
    risk_level = conf.get("risk_level", "balanced")
    max_positions = int(conf.get("max_positions", 3))
    leverage_cap = int(conf.get("leverage_cap", 5))
    tp_pct = float(conf.get("tp_pct", 2.0))
    sl_pct = float(conf.get("sl_pct", 1.5))
    min_confidence = float(conf.get("min_confidence", 0.65))
    products = conf.get("products", get_perp_products()[:6] or ["BTC", "ETH", "SOL"])
    max_loss = float(conf.get("max_loss_pct", 15))
    cycle_seconds = int(conf.get("cycle_seconds", 300))

    available_margin = 0.0
    client = get_user_readonly_client(telegram_id)
    if client:
        try:
            bal = client.get_balance()
            if bal and bal.get("exists"):
                available_margin = float((bal.get("balances", {}) or {}).get(0, 0) or 0)
                if available_margin == 0:
                    available_margin = float((bal.get("balances", {}) or {}).get("0", 0) or 0)
        except Exception:
            pass

    from src.nadobro.services.bot_runtime import get_user_bot_status
    bot_status = get_user_bot_status(telegram_id)
    is_running = bool(bot_status.get("running") and bot_status.get("strategy") == "bro")
    if is_running:
        status_emoji = "⏸️" if bool(bot_status.get("is_paused")) else "🟢"
        status_label = "PAUSED" if bool(bot_status.get("is_paused")) else "LIVE"
    else:
        status_emoji = "🟠"
        status_label = "READY"

    wallet_ready, _wallet_msg = ensure_active_wallet_ready(telegram_id)
    wallet_info = get_user_wallet_info(telegram_id, verify_signer=False) or {}
    wallet_addr = str(wallet_info.get("active_address") or "")
    wallet_short = "N/A"
    if wallet_addr:
        wallet_short = f"{wallet_addr[:6]}...{wallet_addr[-4:]}" if len(wallet_addr) >= 10 else wallet_addr
    account_status = "✅ Connected" if wallet_ready else "⚠️ Setup Needed"

    risk_emoji = {"conservative": "🛡️", "balanced": "⚖️", "aggressive": "🔥"}.get(risk_level, "⚖️")
    products_str = ", ".join(products)
    session_volume = float(bot_status.get("session_volume_usd") or 0.0)
    session_pnl = float((bot_status.get("bro_state") or {}).get("total_pnl") or 0.0)
    trade_count = int((bot_status.get("bro_state") or {}).get("trade_count") or 0)
    active_positions = len((bot_status.get("bro_state") or {}).get("active_positions") or [])
    warning = ""
    if not wallet_ready:
        warning = "⚠️ Open Wallet to link your 1CT signer and fund this mode\\."
    elif available_margin < budget * 0.2:
        warning = f"⚠️ Keep at least {escape_md(f'${budget * 0.2:,.2f}')} available for BRO allocations\\."

    return (
        "🧠 *Alpha Agent Dashboard*\n"
        f"Status: {status_emoji} *{escape_md(status_label)}*\n\n"
        "🔑 *Account*\n"
        f"• Status: *{escape_md(account_status)}*\n"
        f"• Wallet: `{escape_md(wallet_short)}`\n"
        f"• Balance: *{escape_md(f'${available_margin:,.2f}')}*\n\n"
        "⚙️ *Configuration*\n"
        f"• Budget: *{escape_md(f'${budget:,.0f}')}*\n"
        f"• Risk: {escape_md(risk_emoji)} *{escape_md(risk_level.upper())}*\n"
        f"• Assets: *{escape_md(products_str)}*\n"
        f"• Max Positions: *{escape_md(str(max_positions))}*\n"
        f"• Max Leverage: *{escape_md(f'{leverage_cap}x')}*\n"
        f"• TP/SL: *{escape_md(f'{tp_pct:.1f}% / {sl_pct:.1f}%')}*\n"
        f"• Min Confidence: *{escape_md(f'{min_confidence:.0%}')}*\n"
        f"• Cycle: *{escape_md(f'{cycle_seconds}s')}*\n"
        f"• Max Loss: *{escape_md(f'{max_loss:.0f}%')}*\n\n"
        "📊 *Statistics*\n"
        f"• Total Volume: *{escape_md(f'${session_volume:,.2f}')}*\n"
        f"• Trades: *{escape_md(str(trade_count))}*\n"
        f"• Open Positions: *{escape_md(str(active_positions))}*\n"
        f"• PnL: *{escape_md(f'{session_pnl:+,.2f} USD')}*\n\n"
        "ℹ️ *How it works*\n"
        "Scans supported markets, scores setups with AI and sentiment, then opens only high-confidence trades under risk guardrails\\."
        + (f"\n\n{warning}" if warning else "")
    )


def _build_strategy_preview_text(telegram_id: int, strategy_id: str, product: str) -> str:
    names = {
        "grid": "GRID",
        "rgrid": "Reverse GRID",
        "dn": "Mirror Delta Neutral",
        "vol": "Volume Bot",
    }
    network, settings = get_user_settings(telegram_id)
    conf = settings.get("strategies", {}).get(strategy_id, {})
    margin_usd = float(conf.get("notional_usd", 100.0))
    cycle_notional_cfg = float(conf.get("cycle_notional_usd", margin_usd))
    spread_bp = float(conf.get("spread_bp", 5.0))
    if strategy_id == "rgrid":
        spread_bp = float(conf.get("rgrid_spread_bp", conf.get("grid_spread_bp", spread_bp)))
    interval_seconds = int(conf.get("interval_seconds", 60))
    tp_pct = float(conf.get("tp_pct", 1.0))
    sl_pct = float(conf.get("sl_pct", 0.5))
    leverage = 1.0 if strategy_id == "vol" else float(settings.get("default_leverage", 3))
    if strategy_id == "dn":
        leverage = max(1.0, min(leverage, 5.0))

    def _fmt_usd(value: float) -> str:
        return f"${value:,.2f}"

    available_margin = 0.0
    mid = 0.0
    funding_rate = 0.0
    client = get_user_readonly_client(telegram_id)
    if client:
        try:
            bal = client.get_balance()
            if bal and bal.get("exists"):
                available_margin = float((bal.get("balances", {}) or {}).get(0, 0) or 0.0)
                if available_margin == 0:
                    available_margin = float((bal.get("balances", {}) or {}).get("0", 0) or 0.0)
        except Exception:
            pass
        try:
            user = get_user(telegram_id)
            network = user.network_mode.value if user else "mainnet"
            pid = get_product_id(product, network=network, client=client)
            if pid is not None:
                mp = client.get_market_price(pid)
                mid = float(mp.get("mid", 0) or 0.0)
                fr = client.get_funding_rate(pid) or {}
                funding_rate = float(fr.get("funding_rate", 0) or 0.0)
        except Exception:
            pass

    bot_status = get_user_bot_status(telegram_id) or {}
    active_same_strategy = (
        str(bot_status.get("strategy") or "").lower() == strategy_id
        and str(bot_status.get("product") or "").upper() == str(product or "").upper()
    )
    if active_same_strategy and bool(bot_status.get("running")):
        status_emoji = "⏸️" if bool(bot_status.get("is_paused")) else "🟢"
        status_label = "PAUSED" if bool(bot_status.get("is_paused")) else "LIVE"
    elif active_same_strategy:
        status_emoji = "⚪"
        status_label = "STOPPED"
    else:
        status_emoji = "🟠"
        status_label = "READY"

    wallet_ready, _wallet_msg = ensure_active_wallet_ready(telegram_id)
    wallet_info = get_user_wallet_info(telegram_id, verify_signer=False) or {}
    wallet_addr = str(wallet_info.get("active_address") or "")
    wallet_short = "N/A"
    if wallet_addr:
        wallet_short = f"{wallet_addr[:6]}...{wallet_addr[-4:]}" if len(wallet_addr) >= 10 else wallet_addr
    account_status = "✅ Connected" if wallet_ready else "⚠️ Setup Needed"

    cycle_notional = (
        max(cycle_notional_cfg, margin_usd * max(1.0, leverage))
        if strategy_id in ("grid", "rgrid")
        else cycle_notional_cfg
    )
    required_margin = margin_usd if strategy_id in ("grid", "rgrid") else (cycle_notional / leverage if leverage > 0 else cycle_notional)
    inventory_soft_limit = float(conf.get("inventory_soft_limit_usd", margin_usd * 0.6))
    recommended_buffer = max(5.0, required_margin * 0.20)
    recommended_available = required_margin + (inventory_soft_limit / max(leverage, 1.0)) + recommended_buffer
    mid_str = f"${fmt_price(mid, product)}" if mid > 0 else "N/A"

    trades_count = int(bot_status.get("session_trade_count") or 0)
    session_volume = float(bot_status.get("session_volume_usd") or 0.0)
    session_fees = float(bot_status.get("session_fees_usd") or 0.0)
    session_pnl = float(bot_status.get("session_analytics_pnl_usd") or 0.0)

    if strategy_id == "vol":
        fixed_margin = float(conf.get("fixed_margin_usd") or 100.0)
        target_volume = float(conf.get("target_volume_usd") or 10000.0)
        volume_done = float(bot_status.get("volume_done_usd") or 0.0)
        volume_remaining = float(bot_status.get("volume_remaining_usd") or max(0.0, target_volume - volume_done))
        session_fees = float(bot_status.get("session_fees_usd") or session_fees)
        session_pnl = float(bot_status.get("session_realized_pnl_usd") or session_pnl)
        direction = "SHORT" if str(conf.get("vol_direction", "long")).lower() == "short" else "LONG"
        phase = str(bot_status.get("vol_phase") or "idle").upper()
        warning = ""
        if not wallet_ready:
            warning = "⚠️ Open Wallet to link your 1CT signer and fund this mode\\."
        elif available_margin < fixed_margin:
            warning = f"⚠️ Add margin before starting \\(need {escape_md(_fmt_usd(fixed_margin))}\\)\\."
        return (
            "🔁 *Volume Bot Dashboard*\n"
            f"Status: {status_emoji} *{status_label}*\n\n"
            "🔑 *Account*\n"
            f"• Status: *{escape_md(account_status)}*\n"
            f"• Wallet: `{escape_md(wallet_short)}`\n"
            f"• Balance: *{escape_md(_fmt_usd(available_margin))}*\n\n"
            "⚙️ *Configuration*\n"
            f"• Market: *{escape_md(product)}\\-PERP*\n"
            f"• Size: *{escape_md(_fmt_usd(fixed_margin))}*\n"
            f"• Direction: *{escape_md(direction)}*\n"
            f"• Timing: *{escape_md(f'{interval_seconds}s')}*\n"
            f"• TP/SL: *{escape_md(f'{tp_pct:.1f}% / {sl_pct:.1f}%')}*\n\n"
            "📊 *Statistics*\n"
            f"• Target Volume: *{escape_md(_fmt_usd(target_volume))}*\n"
            f"• Done: *{escape_md(_fmt_usd(volume_done))}*\n"
            f"• Remaining: *{escape_md(_fmt_usd(volume_remaining))}*\n"
            f"• Fees Paid: *{escape_md(_fmt_usd(session_fees))}*\n"
            f"• PnL: *{escape_md(f'{session_pnl:+,.2f} USD')}*\n"
            f"• Phase: *{escape_md(phase)}*\n\n"
            "ℹ️ *How it works*\n"
            "Places maker-only entry and exit orders to build volume while enforcing session TP/SL\\."
            + (f"\n\n{warning}" if warning else "")
        )

    if strategy_id == "grid":
        levels = int(conf.get("levels", 2) or 2)
        min_spread = float(conf.get("min_spread_bp", 2.0))
        max_spread = float(conf.get("max_spread_bp", 20.0))
        reset_threshold = float(conf.get("grid_reset_threshold_pct", 0.8))
        reset_timeout = int(conf.get("grid_reset_timeout_seconds", 120))
        session_volume = float(bot_status.get("session_notional_done_usd") or session_volume)
        warning = ""
        if not wallet_ready:
            warning = "⚠️ Open Wallet to link your 1CT signer and fund this mode\\."
        elif available_margin < required_margin:
            warning = (
                f"⚠️ Recommended available margin {escape_md(_fmt_usd(recommended_available))} "
                f"\\(trade {escape_md(_fmt_usd(required_margin))} + buffer {escape_md(_fmt_usd(recommended_available - required_margin))}\\)\\."
            )
        return (
            "📊 *GRID Dashboard*\n"
            f"Status: {status_emoji} *{status_label}*\n\n"
            "🔑 *Account*\n"
            f"• Status: *{escape_md(account_status)}*\n"
            f"• Wallet: `{escape_md(wallet_short)}`\n"
            f"• Balance: *{escape_md(_fmt_usd(available_margin))}*\n\n"
            "⚙️ *Configuration*\n"
            f"• Market: *{escape_md(product)}\\-PERP*\n"
            f"• Margin: *{escape_md(_fmt_usd(margin_usd))}*\n"
            f"• Levels: *{escape_md(str(levels))}*\n"
            f"• Spread: *{escape_md(f'{min_spread:.0f}bp - {max_spread:.0f}bp')}*\n"
            f"• Timing: *{escape_md(f'{interval_seconds}s')}*\n"
            f"• Leverage: *{escape_md(f'{leverage:.0f}x')}*\n"
            f"• Reset: *{escape_md(f'{reset_threshold:.2f}% / {reset_timeout}s')}*\n"
            f"• TP/SL: *{escape_md(f'{tp_pct:.1f}% / {sl_pct:.1f}%')}*\n\n"
            "📊 *Statistics*\n"
            f"• Total Volume: *{escape_md(_fmt_usd(session_volume))}*\n"
            f"• Total Trades: *{escape_md(str(trades_count))}*\n"
            f"• Fees Paid: *{escape_md(_fmt_usd(session_fees))}*\n"
            f"• PnL: *{escape_md(f'{session_pnl:+,.2f} USD')}*\n"
            f"• Effective Notional: *{escape_md(_fmt_usd(cycle_notional))}*\n\n"
            "ℹ️ *How it works*\n"
            "Places maker-only bids and asks around the market to harvest spread\\."
            + (f"\n\n{warning}" if warning else "")
        )

    if strategy_id == "rgrid":
        levels = int(conf.get("levels", 4) or 4)
        grid_tp = float(conf.get("rgrid_take_profit_pct", conf.get("grid_take_profit_pct", tp_pct)))
        max_loss_pct = float(conf.get("rgrid_stop_loss_pct", conf.get("grid_stop_loss_pct", sl_pct)))
        discretion = float(conf.get("rgrid_discretion", conf.get("grid_discretion", 0.06)))
        reset_threshold = float(conf.get("rgrid_reset_threshold_pct", conf.get("grid_reset_threshold_pct", 1.0)))
        reset_timeout = int(conf.get("rgrid_reset_timeout_seconds", conf.get("grid_reset_timeout_seconds", 120)))
        session_volume = float(bot_status.get("session_notional_done_usd") or session_volume)
        session_pnl = float(bot_status.get("rgrid_last_cycle_pnl_usd") or session_pnl)
        warning = ""
        if not wallet_ready:
            warning = "⚠️ Open Wallet to link your 1CT signer and fund this mode\\."
        elif available_margin < required_margin:
            warning = (
                f"⚠️ Recommended available margin {escape_md(_fmt_usd(recommended_available))} "
                f"\\(trade {escape_md(_fmt_usd(required_margin))} + buffer {escape_md(_fmt_usd(recommended_available - required_margin))}\\)\\."
            )
        return (
            "🧮 *Reverse GRID Dashboard*\n"
            f"Status: {status_emoji} *{status_label}*\n\n"
            "🔑 *Account*\n"
            f"• Status: *{escape_md(account_status)}*\n"
            f"• Wallet: `{escape_md(wallet_short)}`\n"
            f"• Balance: *{escape_md(_fmt_usd(available_margin))}*\n\n"
            "⚙️ *Configuration*\n"
            f"• Market: *{escape_md(product)}\\-PERP*\n"
            f"• Margin: *{escape_md(_fmt_usd(margin_usd))}*\n"
            f"• Levels: *{escape_md(str(levels))}*\n"
            f"• Spread: *{escape_md(f'{spread_bp:.0f}bp')}*\n"
            f"• Timing: *{escape_md(f'{interval_seconds}s')}*\n"
            f"• Leverage: *{escape_md(f'{leverage:.0f}x')}*\n"
            f"• Reset: *{escape_md(f'{reset_threshold:.2f}% / {reset_timeout}s')}*\n"
            f"• Discretion: *{escape_md(f'{discretion:.2f}')}*\n"
            f"• PnL SL/TP: *{escape_md(f'{max_loss_pct:.2f}% / {grid_tp:.2f}%')}*\n\n"
            "📊 *Statistics*\n"
            f"• Total Volume: *{escape_md(_fmt_usd(session_volume))}*\n"
            f"• Total Trades: *{escape_md(str(trades_count))}*\n"
            f"• Fees Paid: *{escape_md(_fmt_usd(session_fees))}*\n"
            f"• PnL: *{escape_md(f'{session_pnl:+,.2f} USD')}*\n"
            f"• Effective Notional: *{escape_md(_fmt_usd(cycle_notional))}*\n\n"
            "ℹ️ *How it works*\n"
            "Anchors to exposure and places buy above / sell below to capture continuation\\."
            + (f"\n\n{warning}" if warning else "")
        )

    cycles_per_day = 86400 / max(interval_seconds, 10)
    est_daily_volume = cycle_notional * 2.0 * cycles_per_day

    # Conservative fee estimate using builder fee (2 bps) + maker fee proxy (1 bp).
    from src.nadobro.config import EST_FEE_RATE, EST_FILL_EFFICIENCY
    est_fees = est_daily_volume * EST_FEE_RATE
    est_spread_pnl = est_daily_volume * (spread_bp / 10000.0) * EST_FILL_EFFICIENCY
    est_funding = abs(funding_rate) * margin_usd * 3 if strategy_id == "dn" else 0.0
    est_net = est_spread_pnl + est_funding - est_fees
    status_dot = "🟢" if est_net >= 0 else "🟠"
    funding_bias = "FAVORABLE" if funding_rate > 0.000001 else "UNFAVORABLE"
    auto_close = "ON" if float(conf.get("auto_close_on_maintenance", 1) or 0) >= 0.5 else "OFF"
    funding_mode = str(conf.get("funding_entry_mode", "enter_anyway")).strip().lower()
    mode_label = "ENTER ANYWAY" if funding_mode == "enter_anyway" else "WAIT FAVORABLE"
    warning = ""
    if not wallet_ready:
        warning = "⚠️ Open Wallet to link your 1CT signer and fund this mode\\."
    elif available_margin < required_margin:
        warning = f"⚠️ Add margin before starting \\(need {escape_md(_fmt_usd(required_margin))}\\)\\."
    return (
        "🪞 *Mirror Delta Neutral Dashboard*\n"
        f"Strategy Status: {status_emoji} *{status_label}*\n\n"
        "📊 *Your Stats*\n"
        f"• Volume Traded: *{escape_md(_fmt_usd(session_volume))}*\n"
        f"• Positions Created: *{escape_md(str(trades_count))}*\n"
        f"• Fees Paid: *{escape_md(_fmt_usd(session_fees))}*\n"
        f"• Funding: *{escape_md(f'{funding_rate:.6f}')}*\n\n"
        "🔑 *Exchange Account*\n"
        f"• Status: *{escape_md(account_status)}*\n"
        f"• Wallet: `{escape_md(wallet_short)}`\n"
        f"• Balance: *{escape_md(_fmt_usd(available_margin))}*\n\n"
        "⚙️ *Current Settings*\n"
        f"• Market: *{escape_md(product)}*\n"
        f"• Size: *{escape_md(_fmt_usd(margin_usd))}*\n"
        f"• Funding Leverage: *{escape_md(f'{leverage:.0f}x')}*\n"
        f"• Funding Entry: *{escape_md(mode_label)}*\n"
        f"• Timing: *{escape_md(f'{interval_seconds}s')}*\n"
        f"• Auto-close on maintenance: *{escape_md(auto_close)}*\n\n"
        "ℹ️ *How it works*\n"
        "Opens LONG on Nado spot and SHORT on the same perp to farm funding while staying delta neutral\\.\n"
        f"Funding now: *{escape_md(funding_bias)}* \\| Est\\. Daily Fees: *{escape_md(_fmt_usd(est_fees))}*"
        + (f"\n\n{warning}" if warning else "")
    )


async def _handle_onboarding(query, data, telegram_id, context):
    """Resume new onboarding (language → ToS). Only handles onboarding:resume."""
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    if action != "resume":
        return
    if is_new_onboarding_complete(telegram_id):
        await _show_dashboard(query, telegram_id)
        return
    state = get_new_onboarding_state(telegram_id)
    if not state.get("language"):
        await _edit_loc(query, 
            _ONB_WELCOME_LANG_MSG,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=onboarding_language_kb(),
        )
    else:
        await _edit_loc(query, 
            _ONB_WELCOME_CARD,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=onboarding_accept_tos_kb(),
        )


async def _delete_message_later(query, chat_id: int, message_id: int, delay_seconds: int = 30):
    await asyncio.sleep(delay_seconds)
    try:
        await query.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def _handle_copy(query, data, context, telegram_id):
    from src.nadobro.services.copy_service import (
        get_available_traders, get_user_copies, start_copy, stop_copy,
        pause_copy, resume_copy, get_trader_stats, get_trader_preview,
    )
    from src.nadobro.services.admin_service import (
        add_copy_trader, remove_copy_trader, list_copy_traders,
    )

    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "hub":
        traders = await run_blocking(get_available_traders)
        admin_flag = is_admin(telegram_id)
        if traders:
            lang = get_active_language()
            l_pnl = localize_text("PnL", lang)
            l_vol = localize_text("Vol", lang)
            l_wr = localize_text("WR", lang)
            l_trades = localize_text("Trades", lang)
            lines = [localize_text("🔁 *Copy Trading*\n", lang)]
            for t in traders:
                curated = " ⭐" if t.get("is_curated") else ""
                wallet_snip = t["wallet"][:6] + "\\.\\.\\." + t["wallet"][-4:]
                stats = await run_blocking(get_trader_stats, t["id"])
                vol_str = f"${stats['volume_usd']:,.0f}" if stats["volume_usd"] else "\\-"
                wr_str = f"{stats['win_rate']:.0f}%" if stats["total_trades"] > 0 else "\\-"
                pnl = stats["pnl_usd"]
                pnl_str = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"
                lines.append(
                    f"• *{escape_md(t['label'])}*{curated} — `{wallet_snip}`\n"
                    f"  {l_pnl}: *{escape_md(pnl_str)}* \\| {l_vol}: {escape_md(vol_str)} \\| {l_wr}: {escape_md(wr_str)} \\| {l_trades}: {stats['total_trades']}"
                )
            lines.append("\n" + localize_text("Select a trader to view details and start copying\\.", lang))
        else:
            lines = ["🔁 *Copy Trading*\n", "No traders available yet\\. Add a custom wallet or ask an admin to add traders\\."]
        await _edit_loc(query,
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_hub_kb(traders, is_admin_user=admin_flag),
        )

    elif action == "trader" and len(parts) >= 3:
        trader_id = int(parts[2])
        traders = await run_blocking(get_available_traders)
        trader = next((t for t in traders if t["id"] == trader_id), None)
        if not trader:
            await _edit_loc(query, "⚠️ Trader not found\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return

        user = get_user(telegram_id)
        trader_network = user.network_mode.value if user else "mainnet"
        preview = await run_blocking(get_trader_preview, trader_id, trader_network)
        equity = float(preview.get("equity_usd") or 0.0)
        equity_str = f"${equity:,.0f}" if equity > 0 else "N/A"
        curated = " ⭐ Curated" if trader.get("is_curated") else ""
        open_count = int(preview.get("open_positions") or 0)

        stats = await run_blocking(get_trader_stats, trader_id)
        vol_str = f"${stats['volume_usd']:,.0f}" if stats["volume_usd"] else "N/A"
        wr_str = f"{stats['win_rate']:.0f}%" if stats["total_trades"] > 0 else "N/A"
        pnl_preview = stats["pnl_usd"]
        pnl_preview_str = f"+${pnl_preview:,.2f}" if pnl_preview >= 0 else f"-${abs(pnl_preview):,.2f}"

        wallet_snip = trader["wallet"][:6] + "..." + trader["wallet"][-4:]

        await _edit_loc(query,
            "🔁 *Trader Preview*{curated}\n\nLabel: *{label}*\nWallet: `{wallet}`\nEquity: *{equity}*\nOpen Positions: *{positions}*\nPnL: *{pnl}*\nVolume: *{volume}*\nWin Rate: *{winrate}* \\({filled} filled / {total} total\\)\n\nTap Start Copying to set your budget and risk parameters\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_trader_preview_kb(trader_id),
            curated=escape_md(curated), label=escape_md(trader['label']),
            wallet=escape_md(wallet_snip), equity=escape_md(equity_str),
            positions=open_count, pnl=escape_md(pnl_preview_str),
            volume=escape_md(vol_str), winrate=escape_md(wr_str),
            filled=stats['filled'], total=stats['total_trades'],
        )

    elif action == "start" and len(parts) >= 3:
        trader_id = int(parts[2])
        context.user_data["copy_setup"] = {"trader_id": trader_id, "step": "budget"}
        await _edit_loc(query,
            "💰 *Set Copy Budget*\n\nHow much USD to allocate for copy trading this trader?",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_budget_kb(),
        )

    elif action == "budget" and len(parts) >= 3:
        setup = context.user_data.get("copy_setup")
        if not setup:
            await _edit_loc(query, "⚠️ No setup in progress\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return
        setup["budget_usd"] = float(parts[2])
        setup["step"] = "risk"
        await _edit_loc(query,
            "⚖️ *Set Risk Factor*\n\nBudget: *${budget}*\n\nRisk factor scales the per\\-trade budget slice inside your total copy allocation\\. Higher values copy more aggressively\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_risk_kb(),
            budget=f"{setup['budget_usd']:.0f}",
        )

    elif action == "risk" and len(parts) >= 3:
        setup = context.user_data.get("copy_setup")
        if not setup:
            await _edit_loc(query, "⚠️ No setup in progress\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return
        setup["risk_factor"] = float(parts[2])
        setup["step"] = "leverage"
        await _edit_loc(query,
            "📐 *Set Max Leverage*\n\nBudget: *${budget}* \\| Risk: *{risk}x*\n\nSet the maximum leverage cap for copied trades\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_leverage_kb(),
            budget=f"{setup['budget_usd']:.0f}", risk=setup['risk_factor'],
        )

    elif action == "lev" and len(parts) >= 3:
        setup = context.user_data.get("copy_setup")
        if not setup:
            await _edit_loc(query, "⚠️ No setup in progress\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return
        setup["max_leverage"] = float(parts[2])
        setup["step"] = "cumulative_sl"
        from src.nadobro.handlers.keyboards import copy_cumulative_sl_kb
        await _edit_loc(query,
            "🛡 *Cumulative Stop Loss*\n\nBudget: *${budget}* \\| Risk: *{risk}x* \\| Leverage: *{leverage}x*\n\nSet a cumulative loss limit \\(% of budget\\)\\. Copying stops if total losses hit this threshold\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_cumulative_sl_kb(),
            budget=f"{setup['budget_usd']:.0f}",
            risk=setup['risk_factor'],
            leverage=f"{setup['max_leverage']:.0f}",
        )

    elif action == "csl" and len(parts) >= 3:
        setup = context.user_data.get("copy_setup")
        if not setup:
            await _edit_loc(query, "⚠️ No setup in progress\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return
        csl_pct = float(parts[2])
        setup["cumulative_stop_loss_pct"] = csl_pct if csl_pct > 0 else None
        setup["step"] = "cumulative_tp"
        from src.nadobro.handlers.keyboards import copy_cumulative_tp_kb
        sl_label = f"{csl_pct:.0f}%" if csl_pct > 0 else "None"
        await _edit_loc(query,
            "🎯 *Cumulative Take Profit*\n\nBudget: *${budget}* \\| SL: *{sl}*\n\nSet a cumulative profit target \\(% of budget\\)\\. Copying stops when total profits hit this threshold\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_cumulative_tp_kb(),
            budget=f"{setup['budget_usd']:.0f}",
            sl=escape_md(sl_label),
        )

    elif action == "ctp" and len(parts) >= 3:
        setup = context.user_data.get("copy_setup")
        if not setup:
            await _edit_loc(query, "⚠️ No setup in progress\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return
        ctp_pct = float(parts[2])
        setup["cumulative_take_profit_pct"] = ctp_pct if ctp_pct > 0 else None
        setup["step"] = "confirm"

        traders = await run_blocking(get_available_traders)
        trader = next((t for t in traders if t["id"] == setup["trader_id"]), None)
        trader_label = trader["label"] if trader else "Unknown"

        csl = setup.get("cumulative_stop_loss_pct")
        ctp = setup.get("cumulative_take_profit_pct")
        sl_str = f"{csl:.0f}%" if csl else "None"
        tp_str = f"{ctp:.0f}%" if ctp else "None"

        await _edit_loc(query,
            "✅ *Confirm Copy Setup*\n\nTrader: *{trader}*\nBudget: *${budget}*\nRisk Factor: *{risk}x*\nMax Leverage: *{leverage}x*\nCumulative SL: *{sl}*\nCumulative TP: *{tp}*\n\nReady to start?",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_confirm_kb(),
            trader=escape_md(trader_label),
            budget=f"{setup['budget_usd']:.0f}",
            risk=setup['risk_factor'],
            leverage=f"{setup['max_leverage']:.0f}",
            sl=escape_md(sl_str),
            tp=escape_md(tp_str),
        )

    elif action == "confirm":
        setup = context.user_data.get("copy_setup")
        if not setup:
            await _edit_loc(query, "⚠️ No setup to confirm\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return
        if not is_new_onboarding_complete(telegram_id):
            await _edit_loc(query,
                "⚠️ Complete setup first (language + accept terms).",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶ Complete setup", callback_data="onboarding:resume")],
                    [InlineKeyboardButton("Exit", callback_data="nav:main")],
                ]),
            )
            return

        wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
        if not wallet_ready:
            await _edit_loc(query,
                f"⚠️ {escape_md(wallet_msg)}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(),
            )
            return
        context.user_data.pop("copy_setup", None)

        from src.nadobro.handlers.messages import execute_action_directly
        action_data = {
            "type": "start_copy",
            "trader_id": setup["trader_id"],
            "budget_usd": setup["budget_usd"],
            "risk_factor": setup["risk_factor"],
            "max_leverage": setup["max_leverage"],
        }
        if setup.get("cumulative_stop_loss_pct"):
            action_data["cumulative_stop_loss_pct"] = setup["cumulative_stop_loss_pct"]
        if setup.get("cumulative_take_profit_pct"):
            action_data["cumulative_take_profit_pct"] = setup["cumulative_take_profit_pct"]
        await execute_action_directly(query, context, telegram_id, action_data)

    elif action == "pause" and len(parts) >= 3:
        mirror_id = int(parts[2])
        ok, msg = await run_blocking(pause_copy, telegram_id, mirror_id)
        prefix = "⏸" if ok else "⚠️"
        mirrors = await run_blocking(get_user_copies, telegram_id)
        await _edit_loc(query,
            f"{prefix} {escape_md(msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_dashboard_kb(mirrors, lang=get_active_language()),
        )

    elif action == "resume" and len(parts) >= 3:
        mirror_id = int(parts[2])
        ok, msg = await run_blocking(resume_copy, telegram_id, mirror_id)
        prefix = "▶" if ok else "⚠️"
        mirrors = await run_blocking(get_user_copies, telegram_id)
        await _edit_loc(query,
            f"{prefix} {escape_md(msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_dashboard_kb(mirrors, lang=get_active_language()),
        )

    elif action == "stop" and len(parts) >= 3:
        mirror_id = int(parts[2])
        ok, msg = await run_blocking(stop_copy, telegram_id, mirror_id)
        prefix = "✅" if ok else "⚠️"
        mirrors = await run_blocking(get_user_copies, telegram_id)
        await _edit_loc(query,
            f"{prefix} {escape_md(msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_dashboard_kb(mirrors, lang=get_active_language()),
        )

    elif action == "dashboard":
        mirrors = await run_blocking(get_user_copies, telegram_id)
        lang = get_active_language()
        if mirrors:
            l_alloc = localize_text("Allocated", lang)
            l_margin = localize_text("Margin/Trade", lang)
            l_lev = localize_text("Lev", lang)
            l_positions = localize_text("Open Positions", lang)
            l_pnl = localize_text("PnL", lang)
            lines = [localize_text("📋 *My Copy Trades*\n", lang)]
            for m in mirrors:
                status_label = localize_text("PAUSED", lang) if m.get("paused") else localize_text("ACTIVE", lang)
                status_icon = f"⏸ {status_label}" if m.get("paused") else f"🟢 {status_label}"
                pnl = float(m.get("cumulative_pnl", 0) or 0)
                pnl_str = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"
                pnl_str = escape_md(pnl_str)
                lines.append(
                    f"• *{escape_md(m['trader_label'])}* — {status_icon}\n"
                    f"  {l_alloc}: ${float(m.get('total_allocated_usd', 0) or 0):.0f} \\| "
                    f"{l_margin}: ${float(m.get('margin_per_trade', 0) or 0):.0f} \\| {l_lev}: {float(m.get('max_leverage', 0) or 0):.0f}x\n"
                    f"  {l_positions}: {int(m.get('open_positions', 0) or 0)}\n"
                    f"  {l_pnl}: *{pnl_str}*"
                )
        else:
            lines = [localize_text("📋 *My Copy Trades*\n", lang), localize_text("You have no active copy mirrors\\.", lang)]
        await _edit_loc(query,
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_dashboard_kb(mirrors, lang=get_active_language()),
        )

    elif action == "add_custom":
        context.user_data["pending_copy_wallet"] = True
        await _edit_loc(query,
            "➕ *Add Custom Wallet*\n\nSend the Ethereum wallet address \\(0x\\.\\.\\.\\) of the trader you want to copy\\.\n\nThe address must be 42 characters starting with `0x`\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="copy:hub")],
            ]),
        )

    elif action == "admin" and len(parts) >= 3:
        if not is_admin(telegram_id):
            await _edit_loc(query, "⚠️ Admin access required\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return

        sub = parts[2]
        if sub == "menu":
            traders = await run_blocking(list_copy_traders)
            await _edit_loc(query,
                "⚙️ *Manage Copy Traders*\n\nAdd or remove traders from the copy trading pool\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=copy_admin_menu_kb(traders, lang=get_active_language()),
            )
        elif sub == "add":
            context.user_data["pending_admin_copy_wallet"] = True
            await _edit_loc(query,
                "➕ *Add Trader*\n\nSend the Ethereum wallet address \\(0x\\.\\.\\.\\) and an optional label separated by a space\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel", callback_data="copy:admin:menu")],
                ]),
            )
        elif sub == "remove" and len(parts) >= 4:
            trader_id = int(parts[3])
            ok, msg = await run_blocking(remove_copy_trader, telegram_id, trader_id)
            prefix = "✅" if ok else "⚠️"
            traders = await run_blocking(list_copy_traders)
            await _edit_loc(query,
                f"{prefix} {escape_md(msg)}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=copy_admin_menu_kb(traders, lang=get_active_language()),
            )
