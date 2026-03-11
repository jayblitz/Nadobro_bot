import logging
import time
import asyncio
import secrets
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import CallbackContext
from telegram.constants import ParseMode, ChatAction
from eth_account import Account
from src.nadobro.handlers.formatters import (
    escape_md, fmt_positions,
    fmt_trade_preview, fmt_trade_result, fmt_pre_trade_analytics,
    fmt_wallet_info, fmt_alerts, fmt_portfolio,
    fmt_settings, fmt_help, fmt_price, fmt_status_overview, fmt_points_dashboard, fmt_position_pnl_panel,
)
from src.nadobro.handlers.keyboards import (
    persistent_menu_kb, trade_product_kb, trade_size_kb, trade_leverage_kb,
    trade_confirm_kb, positions_kb, wallet_kb, wallet_kb_not_linked, alerts_kb,
    alert_product_kb, alert_delete_kb, settings_kb, settings_leverage_kb,
    settings_slippage_kb, settings_language_kb, close_product_kb, confirm_close_all_kb, back_kb,
    risk_profile_kb, strategy_hub_kb, strategy_action_kb,
    strategy_status_kb,
    strategy_funding_kb,
    onboarding_language_kb,
    mode_kb,     home_card_kb, portfolio_kb,
    onboarding_accept_tos_kb,
    points_scope_kb,
    position_manage_kb,
)
from src.nadobro.handlers.trade_card import handle_trade_card_callback
from src.nadobro.handlers.home_card import build_home_card_text
from src.nadobro.services.user_service import (
    get_or_create_user, get_user_nado_client, get_user_readonly_client, get_user_wallet_info,
    switch_network, get_user, remove_user_private_key, ensure_active_wallet_ready, update_user_language,
)
from src.nadobro.services.trade_service import (
    execute_market_order, execute_limit_order, close_position,
    close_all_positions, get_trade_history, get_trade_analytics,
)
from src.nadobro.services.alert_service import create_alert, get_user_alerts, delete_alert
from src.nadobro.services.admin_service import is_trading_paused
from src.nadobro.services.bot_runtime import stop_user_bot, get_user_bot_status
from src.nadobro.services.settings_service import get_user_settings, update_user_settings
from src.nadobro.services.onboarding_service import (
    get_resume_step,
    evaluate_readiness,
    set_new_onboarding_language,
    set_new_onboarding_tos_accepted,
    is_new_onboarding_complete,
    get_new_onboarding_state,
)
from src.nadobro.config import get_product_name, get_product_id, get_product_max_leverage, PRODUCTS, VOL_MAX_LEVERAGE
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.perf import timed_metric, log_slow
from src.nadobro.services.points_service import get_points_dashboard, request_points_refresh
from src.nadobro.services.equity_snapshots import record_snapshot, get_1d_7d_changes, get_history_for_csv
from src.nadobro.i18n import (
    get_user_language,
    language_context,
    localize_payload,
)

logger = logging.getLogger(__name__)
WALLET_SEED_TTL_SECONDS = 900
_WALLET_SEED_CACHE: dict[str, dict] = {}


def _safe_float(value, default: float) -> float:
    """Parse numeric settings safely, tolerating legacy values like '3x'."""
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        cleaned = value.strip().lower().replace("x", "")
        try:
            return float(cleaned)
        except (TypeError, ValueError):
            pass
    return float(default)


def _prune_wallet_seed_cache() -> None:
    now = time.time()
    expired = [token for token, payload in _WALLET_SEED_CACHE.items() if now - float(payload.get("ts", 0)) > WALLET_SEED_TTL_SECONDS]
    for token in expired:
        _WALLET_SEED_CACHE.pop(token, None)


def _store_wallet_seed(pk_hex: str, linked_addr: str) -> str:
    _prune_wallet_seed_cache()
    token = secrets.token_urlsafe(24)
    _WALLET_SEED_CACHE[token] = {
        "pk_hex": pk_hex,
        "linked_addr": linked_addr,
        "ts": time.time(),
    }
    return token


def pop_wallet_seed(token: str | None) -> tuple[str, str] | tuple[None, None]:
    _prune_wallet_seed_cache()
    if not token:
        return None, None
    payload = _WALLET_SEED_CACHE.pop(token, None)
    if not payload:
        return None, None
    return payload.get("pk_hex"), payload.get("linked_addr")


def _wallet_setup_message(pk_hex: str) -> str:
    return (
        "👛 *Wallet Connect Guide*\n\n"
        "*Step 1:* Open https://app.nado.xyz and connect your main wallet.\n"
        "Deposit at least $5 USDT0 to activate trading.\n\n"
        "*Step 2:* Go to Settings → 1-Click Trading → Advanced 1CT.\n\n"
        "*Step 3:* Paste this *1CT private key* into the \"1CT Private Key\" field:\n\n"
        f"`{pk_hex}`\n\n"
        "_⚠️ Copy immediately, paste into Nado, then delete this message. Do not share._\n\n"
        "*Step 4:* Enable 1CT, click *Save*, and confirm the transaction in wallet "
        "(~1 USDT0 network/auth cost).\n\n"
        "*Step 5:* Return here and send your *main wallet address* (0x...).\n\n"
        "After you send your address, you'll set a passphrase to encrypt this key for future use."
    )


def seed_wallet_setup_flow(context: CallbackContext) -> str:
    account = Account.create()
    pk_hex = account.key.hex()
    if not pk_hex.startswith("0x"):
        pk_hex = "0x" + pk_hex
    seed_token = _store_wallet_seed(pk_hex, account.address)
    context.user_data["wallet_flow"] = "awaiting_main_address"
    context.user_data["wallet_seed_token"] = seed_token
    return _wallet_setup_message(pk_hex)


async def prompt_wallet_setup(target, context: CallbackContext, telegram_id: int, lead_text: str | None = None):
    msg = seed_wallet_setup_flow(context)
    if lead_text:
        msg = f"{lead_text}\n\n{msg}"

    if hasattr(target, "edit_message_text"):
        await target.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=wallet_kb_not_linked())
        return
    if hasattr(target, "reply_text"):
        await target.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=wallet_kb_not_linked())
        return
    logger.warning("prompt_wallet_setup called with unsupported target type for user %s", telegram_id)


async def handle_callback(update: Update, context: CallbackContext):
    started = time.perf_counter()
    query = update.callback_query
    data = query.data
    telegram_id = query.from_user.id
    try:
        try:
            original_edit = query.edit_message_text

            async def _localized_edit_message_text(text=None, *args, **kwargs):
                active_lang = get_user_language(telegram_id)
                loc_text, loc_kb = localize_payload(text, kwargs.get("reply_markup"), active_lang)
                kwargs["reply_markup"] = loc_kb
                return await original_edit(loc_text, *args, **kwargs)

            query.edit_message_text = _localized_edit_message_text
        except Exception:
            pass

        if query.message and hasattr(query.message, "reply_text"):
            try:
                original_reply = query.message.reply_text

                async def _localized_reply_text(text=None, *args, **kwargs):
                    active_lang = get_user_language(telegram_id)
                    loc_text, loc_kb = localize_payload(text, kwargs.get("reply_markup"), active_lang)
                    kwargs["reply_markup"] = loc_kb
                    return await original_reply(loc_text, *args, **kwargs)

                query.message.reply_text = _localized_reply_text
            except Exception:
                pass

        with language_context(get_user_language(telegram_id)):
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
                await _handle_portfolio(query, data, telegram_id, context)
            elif data.startswith("wallet:"):
                await _handle_wallet(query, data, telegram_id, context)
            elif data.startswith("alert:"):
                await _handle_alert(query, data, telegram_id, context)
            elif data.startswith("settings:"):
                await _handle_settings(query, data, telegram_id, context)
            elif data.startswith("points:"):
                await _handle_points(query, data, telegram_id, context)
            elif data.startswith("strategy:"):
                await _handle_strategy(query, data, context, telegram_id)
            elif data == "home:mode":
                user = get_user(telegram_id)
                current_network = user.network_mode.value if user else "testnet"
                network_label = "🧪 TESTNET" if current_network == "testnet" else "🌐 MAINNET"
                await query.edit_message_text(
                    f"🌐 *Execution Mode Control*\n\n"
                    f"Current Mode: *{escape_md(network_label)}*\n\n"
                    f"Switch mode below:",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=mode_kb(current_network),
                )
            elif data.startswith("mode:"):
                await _handle_mode(query, data, telegram_id, context)
            else:
                await query.edit_message_text(
                    "Unknown action\\.",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=back_kb(),
                )
    except Exception as e:
        if isinstance(e, BadRequest) and "Message is not modified" in str(e):
            # Harmless: user tapped a button that would render identical content.
            logger.info("Callback no-op for '%s': %s", data, e)
            return
        logger.error(f"Callback error for '{data}': {e}", exc_info=True)
        try:
            await query.edit_message_text(
                f"⚠️ An error occurred\\. Please try again\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(),
            )
        except Exception:
            pass
    finally:
        log_slow("callback.total", threshold_ms=800.0, started_at=started)


# New onboarding (language → ToS) message text
_ONB_WELCOME_LANG_MSG = """Yo what’s good, future Nado whale?! 👋💰

Welcome to Nadobro — the best Telegram bot for trading Perps on Nado.

We’re giving you pro tools in the palm of your hand:
• MM Bot (Grid + RGRID that prints)
• Delta Neutral Bot (spot + 1-5x short = easy funding)
• Volume Bot (farm leaderboards on autopilot)
• AI chat: just type your trade ideas in English

First, pick your language vibe:"""

_ONB_WELCOME_CARD = """🔥 Nadobro Activated! You’re in the squad 🔥

We run on Nado’s lightning CLOB with unified margin.

By tapping "Let’s Get It" you accept our Terms of Use & Privacy Policy.

⚡ Security First (this is why we’re better):
We generate a secure Linked Signer for your default subaccount only.
You paste the 1CT private key into Nado Settings -> 1-Click Trading -> Advanced 1CT (1 tx, 5 seconds).
Your main wallet key stays in your wallet. Revoke anytime. 100% self-custody.

Ready to start printing?"""

_ONB_DASHBOARD_MSG = """🚀 Nadobro Dashboard — You’re Live, Legend!

What we smashing today?"""


async def _handle_onb_new(query, data, telegram_id, context):
    if data == "onb:accept_tos":
        state = get_new_onboarding_state(telegram_id)
        if not state.get("language"):
            await query.edit_message_text(
                _ONB_WELCOME_LANG_MSG,
                reply_markup=onboarding_language_kb(),
            )
            return
        set_new_onboarding_tos_accepted(telegram_id)
        await query.edit_message_text(
            _ONB_DASHBOARD_MSG,
            reply_markup=home_card_kb(),
        )
        return
    if data.startswith("onb:lang:"):
        parts = data.split(":")
        if len(parts) < 3:
            return
        lang = parts[2]
        set_new_onboarding_language(telegram_id, lang)
        from src.nadobro.services.user_service import update_user_language
        update_user_language(telegram_id, lang)
        # Use the newly selected language immediately for this response.
        text, kb = localize_payload(_ONB_WELCOME_CARD, onboarding_accept_tos_kb(), lang)
        await query.edit_message_text(
            text,
            reply_markup=kb,
        )


async def _handle_points(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"
    scope = "week"
    if len(parts) > 2:
        raw_scope = (parts[2] or "week").lower()
        scope = "week" if raw_scope in ("week", "current", "all", "epoch") else "week"

    if action not in ("view", "refresh"):
        action = "view"

    points = await run_blocking(get_points_dashboard, telegram_id, scope)
    should_refresh = action == "refresh" or not points.get("ok")
    if should_refresh:
        bridge_result = await request_points_refresh(context=context, telegram_id=telegram_id, chat_id=query.message.chat_id)
        if not bridge_result.get("ok"):
            await query.edit_message_text(
                escape_md(bridge_result.get("error", "Could not fetch points right now.")),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=points_scope_kb("week"),
            )
            return
        if points.get("ok"):
            msg = (
                f"{fmt_points_dashboard(points)}\n\n"
                "🔄 Refreshing from bridge\\. Updated card will be posted below shortly\\."
            )
        else:
            msg = "🔄 Fetching your Nado points from the last week\\. Updated card will appear below\\."
    else:
        msg = fmt_points_dashboard(points)
    await query.edit_message_text(
        msg,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=points_scope_kb("week"),
    )


async def _show_dashboard(query, telegram_id):
    await query.edit_message_text(
        build_home_card_text(telegram_id),
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
            await query.edit_message_text(
                f"🌐 *Execution Mode Control*\n\n"
                f"Already on *{escape_md(network_label)}*\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=mode_kb(current_network),
            )
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                raise
        return

    success, result_msg = switch_network(telegram_id, target_network)
    if success:
        if context is not None:
            from src.nadobro.handlers.messages import clear_session_passphrase
            clear_session_passphrase(context, telegram_id=telegram_id)
        network_label = "🧪 TESTNET" if target_network == "testnet" else "🌐 MAINNET"
        await query.edit_message_text(
            f"✅ *Switched to {escape_md(network_label)}*\n\n{escape_md(result_msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=mode_kb(target_network),
        )
    else:
        await query.edit_message_text(
            f"❌ {escape_md(result_msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=mode_kb(current_network),
        )


async def _handle_nav(query, data, telegram_id, context=None):
    target = data.split(":")[1] if ":" in data else "main"

    if context is not None:
        context.user_data.pop("pending_passphrase_action", None)
        context.user_data.pop("pending_trade", None)

    if target in ("main", "refresh"):
        if context is not None:
            from src.nadobro.handlers.messages import terminate_active_processes
            terminate_active_processes(context, telegram_id=telegram_id)
        if target == "main":
            # UX: Home should always open a fresh dashboard at the bottom.
            try:
                await query.message.reply_text(
                    build_home_card_text(telegram_id),
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=home_card_kb(),
                )
            except BadRequest as e:
                if "Can't parse entities" not in str(e):
                    raise
                cleaned = build_home_card_text(telegram_id).replace("\\", "")
                try:
                    await query.message.reply_text(
                        cleaned,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=home_card_kb(),
                    )
                except BadRequest:
                    await query.message.reply_text(
                        cleaned,
                        reply_markup=home_card_kb(),
                    )
        else:
            await _show_dashboard(query, telegram_id)
    elif target == "help":
        try:
            await query.edit_message_text(
                fmt_help(),
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
        await query.edit_message_text(
            "🤖 *Nadobro Strategy Lab*\n\n"
            "Pick a strategy to open its cockpit dashboard, edit parameters, and launch with pre\\-trade analytics\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_hub_kb(),
        )
    elif target == "ask_nado" and context is not None:
        context.user_data["pending_question"] = True
        await query.edit_message_text(
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


async def _handle_trade(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    if not is_new_onboarding_complete(telegram_id):
        await query.edit_message_text(
            "⚠️ Complete setup first (language + accept terms).",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("▶ Complete setup", callback_data="onboarding:resume")],
                [InlineKeyboardButton("Exit", callback_data="nav:main")],
            ]),
        )
        return
    wallet_ready, _ = ensure_active_wallet_ready(telegram_id)
    if action in ("long", "short", "limit_long", "limit_short") and not wallet_ready:
        await prompt_wallet_setup(
            query,
            context,
            telegram_id,
            lead_text="⚠️ Wallet not linked yet. Complete this quick setup to start trading.",
        )
        return

    if action in ("long", "short"):
        action_label = "🟢 BUY / LONG" if action == "long" else "🔴 SELL / SHORT"
        await query.edit_message_text(
            f"*{escape_md(action_label)}*\n\nSelect a product:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_product_kb(action),
        )
    elif action in ("limit_long", "limit_short"):
        context.user_data["pending_trade"] = {"action": action, "step": "product_select"}
        action_label = "LIMIT LONG" if action == "limit_long" else "LIMIT SHORT"
        await query.edit_message_text(
            f"*{escape_md(action_label)}*\n\nSelect a product:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_product_kb(action),
        )
    elif action == "close":
        await query.edit_message_text(
            "*Close Position*\n\n*Select the product to close:*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=close_product_kb(),
        )
    elif action == "close_all":
        await query.edit_message_text(
            "⚠️ *Close All Positions*\n\nAre you sure you want to close *ALL open positions*\\?\n\n"
            "Type `confirm` to execute or `cancel` to discard\\.",
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
        await query.edit_message_text(
            f"*{escape_md(action.replace('_', ' ').upper())} {escape_md(product)}*\n\n"
            f"Enter size and price:\n"
            f"Example: `0\\.01 95000`",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )
        return

    action_label = "LONG" if action == "long" else "SHORT"
    await query.edit_message_text(
        f"*{escape_md(action_label)} {escape_md(product)}\\-PERP*\n\nSelect trade size:",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=trade_size_kb(product, action),
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
        await query.edit_message_text(
            f"*{escape_md(action.upper())} {escape_md(product)}\\-PERP*\n\n"
            f"Type the trade size \\(e\\.g\\. `0\\.01`\\):",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )
        return

    size = float(size_str)
    action_label = "LONG" if action == "long" else "SHORT"
    await query.edit_message_text(
        f"*{escape_md(action_label)} {escape_md(str(size))} {escape_md(product)}\\-PERP*\n\nSelect leverage:",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=trade_leverage_kb(product, action, size),
    )


async def _handle_leverage(query, data, telegram_id, context):
    parts = data.split(":")
    if len(parts) < 5:
        return

    action = parts[1]
    product = parts[2]
    size = float(parts[3])
    leverage = int(parts[4])
    max_leverage = get_product_max_leverage(product)
    if leverage > max_leverage:
        await query.edit_message_text(
            f"⚠️ Max leverage for *{escape_md(product)}* is *{escape_md(str(max_leverage))}x*\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_leverage_kb(product, action, size),
        )
        return

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

    from src.nadobro.handlers.messages import _compute_trade_analytics
    analytics = _compute_trade_analytics(size, price, leverage, None, action)
    preview = fmt_trade_preview(action, product, size, price, leverage, est_margin, analytics=analytics)
    await query.edit_message_text(
        preview,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=trade_confirm_kb(),
    )


async def _handle_exec_trade(query, data, telegram_id, context):
    pending = context.user_data.get("pending_trade")
    if not pending:
        await query.edit_message_text(
            "⚠️ No pending trade found\\. Please start again\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )
        return

    action = pending.get("action", "long")
    product = pending.get("product", "BTC")
    size = pending.get("size", 0)
    leverage = pending.get("leverage", 1)
    slippage_pct = pending.get("slippage_pct", _get_user_settings(telegram_id, context).get("slippage", 1))

    context.user_data.pop("pending_trade", None)

    if is_trading_paused():
        await query.edit_message_text(
            "⏸ Trading is temporarily paused by admin\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )
        return
    wallet_ready, _ = ensure_active_wallet_ready(telegram_id)
    if not wallet_ready:
        await prompt_wallet_setup(
            query,
            context,
            telegram_id,
            lead_text="⚠️ You need a linked signer before executing trades.",
        )
        return

    from src.nadobro.handlers.messages import authorize_or_prompt_passphrase
    await authorize_or_prompt_passphrase(query, context, telegram_id, {
        "type": "exec_trade_callback",
        "pending": {
            "action": action,
            "product": product,
            "size": size,
            "leverage": leverage,
            "slippage_pct": slippage_pct,
            "price": pending.get("price", 0),
            "reduce_only": bool(pending.get("reduce_only", False)),
        },
    })


async def _handle_positions(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"

    async def _render_position_panel(product: str):
        client = get_user_readonly_client(telegram_id)
        if not client:
            await query.edit_message_text(
                "⚠️ Wallet not initialized\\. Use /start first\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(),
            )
            return
        with timed_metric("cb.positions.manage"):
            positions = (await run_blocking(client.get_all_positions)) or []
        target = (product or "").replace("-PERP", "").upper()
        selected = None
        for p in positions:
            pname = str(p.get("product_name", "")).replace("-PERP", "").upper()
            if pname == target:
                selected = p
                break
        if not selected:
            await query.edit_message_text(
                f"⚠️ No open {escape_md(target)} position found\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=positions_kb(positions),
            )
            return

        current_price = 0.0
        try:
            prices = await run_blocking(client.get_all_market_prices)
            if prices and target in prices:
                current_price = float(prices[target].get("mid") or 0)
        except Exception:
            current_price = 0.0
        if current_price <= 0:
            try:
                pid = get_product_id(target)
                if pid is not None:
                    mp = await run_blocking(client.get_market_price, pid)
                    current_price = float(mp.get("mid") or 0)
            except Exception:
                current_price = 0.0

        msg = fmt_position_pnl_panel(selected, current_price)
        try:
            await query.edit_message_text(
                msg,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=position_manage_kb(target),
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                return
            raise

    if action == "view":
        client = get_user_readonly_client(telegram_id)
        if not client:
            await query.edit_message_text(
                "⚠️ Wallet not initialized\\. Use /start first\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(),
            )
            return

        with timed_metric("cb.positions.view"):
            positions = await run_blocking(client.get_all_positions)
        prices = None
        try:
            prices = await run_blocking(client.get_all_market_prices)
        except Exception:
            pass
        msg = fmt_positions(positions, prices)
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=positions_kb(positions or []),
        )

    elif action == "close" and len(parts) >= 3:
        product = parts[2]
        from src.nadobro.handlers.messages import authorize_or_prompt_passphrase
        await authorize_or_prompt_passphrase(query, context, telegram_id, {"type": "close_position", "product": product})

    elif action == "manage" and len(parts) >= 3:
        await _render_position_panel(parts[2])

    elif action == "refresh" and len(parts) >= 3:
        await _render_position_panel(parts[2])

    elif action == "close_market" and len(parts) >= 3:
        product = parts[2]
        from src.nadobro.handlers.messages import authorize_or_prompt_passphrase
        await authorize_or_prompt_passphrase(
            query, context, telegram_id, {"type": "close_position", "product": product}
        )

    elif action == "close_limit" and len(parts) >= 3:
        product = parts[2].replace("-PERP", "").upper()
        client = get_user_readonly_client(telegram_id)
        if not client:
            await query.edit_message_text(
                "⚠️ Wallet not initialized\\. Use /start first\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(),
            )
            return
        positions = (await run_blocking(client.get_all_positions)) or []
        selected = None
        for p in positions:
            pname = str(p.get("product_name", "")).replace("-PERP", "").upper()
            if pname == product:
                selected = p
                break
        if not selected:
            await query.edit_message_text(
                f"⚠️ No open {escape_md(product)} position found\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=positions_kb(positions),
            )
            return
        size = abs(float(selected.get("amount", 0) or 0))
        close_action = "limit_short" if str(selected.get("side", "LONG")).upper() == "LONG" else "limit_long"
        context.user_data["pending_trade"] = {
            "action": close_action,
            "product": product,
            "size": size,
            "step": "limit_close_price",
            "close_only": True,
            "reduce_only": True,
        }
        await query.edit_message_text(
            f"📉 *Limit Close {escape_md(product)}*\n\n"
            f"Position size: *{escape_md(f'{size:.6f}')}*\n"
            "Send limit close price only\\.\n"
            "Example: `67250\\.50`",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=position_manage_kb(product),
        )

    elif action == "close_all":
        await query.edit_message_text(
            "⚠️ *Close All Positions*\n\nAre you sure you want to close *ALL open positions*\\?\n\n"
            "Type `confirm` to execute or `cancel` to discard\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=confirm_close_all_kb(),
        )

    elif action == "confirm_close_all":
        from src.nadobro.handlers.messages import authorize_or_prompt_passphrase
        await authorize_or_prompt_passphrase(query, context, telegram_id, {"type": "close_all"})


async def _handle_portfolio(query, data, telegram_id, context):
    if data == "portfolio:csv_export":
        await _handle_portfolio_csv_export(query, telegram_id, context)
        return

    client = get_user_readonly_client(telegram_id)
    if not client:
        await query.edit_message_text(
            "⚠️ Wallet not initialized\\. Use /start first\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )
        return

    with timed_metric("cb.portfolio.view"):
        positions = (await run_blocking(client.get_all_positions)) or []
    prices = None
    balance = None
    try:
        prices = await run_blocking(client.get_all_market_prices)
    except Exception:
        pass
    try:
        balance = await run_blocking(client.get_balance)
    except Exception:
        pass
    stats = await run_blocking(get_trade_analytics, telegram_id)
    from src.nadobro.handlers.formatters import _compute_total_equity
    total_equity, _, _, _ = _compute_total_equity(balance, positions, prices)
    record_snapshot(telegram_id, total_equity)
    p1d, p7d = get_1d_7d_changes(telegram_id)
    msg = fmt_portfolio(stats, positions, prices, balance=balance, equity_1d_pct=p1d, equity_7d_pct=p7d)
    try:
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=portfolio_kb(has_positions=bool(positions)),
        )
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        raise


async def _handle_portfolio_csv_export(query, telegram_id, context):
    import io
    from datetime import datetime

    history = get_history_for_csv(telegram_id)
    if not history:
        await query.answer("No equity history yet. View portfolio to start recording.", show_alert=True)
        return
    buf = io.StringIO()
    buf.write("timestamp,datetime_utc,equity_usd\n")
    for h in history:
        ts = h.get("ts", 0)
        eq = h.get("equity", 0)
        dt = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else ""
        buf.write(f"{ts},{dt},{eq:.2f}\n")
    csv_bytes = io.BytesIO(buf.getvalue().encode("utf-8"))
    csv_bytes.seek(0)
    chat_id = query.message.chat_id
    try:
        await context.bot.send_document(
            chat_id=chat_id,
            document=csv_bytes,
            filename="equity_history.csv",
            caption="📥 Equity history export",
        )
        await query.answer("CSV sent.", show_alert=False)
    except Exception as e:
        logger.warning("Portfolio CSV export failed: %s", e)
        await query.answer("Could not send CSV. Try again.", show_alert=True)


async def _handle_wallet(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"

    if action in ("view", "setup"):
        info = get_user_wallet_info(telegram_id, verify_signer=True)
        if not info or not info.get("linked_signer_address"):
            await prompt_wallet_setup(query, context, telegram_id)
            return
        msg = fmt_wallet_info(info)
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=wallet_kb(),
        )
    elif action == "balance":
        client = get_user_readonly_client(telegram_id)
        if not client:
            await query.edit_message_text(
                "💰 Link your wallet first to check balance.",
                reply_markup=wallet_kb(),
            )
            return
        try:
            bal = client.get_balance()
            usdt = (bal.get("balances") or {}).get(0, 0) or (bal.get("balances") or {}).get("0", 0)
            msg = f"💰 Balance: ${float(usdt or 0):,.2f} USDT0"
        except Exception:
            msg = "Could not fetch balance. Try again."
        await query.edit_message_text(msg, reply_markup=wallet_kb())
    elif action == "revoke_steps":
        revoke_msg = (
            "🔄 *Revoke 1CT Key (Nado)*\n\n"
            "1. Open Nado → Settings\n"
            "2. 1-Click Trading → Advanced 1CT\n"
            "3. Disable the toggle and save\n\n"
            "Your main wallet and funds stay safe. You can link again anytime via Wallet."
        )
        await query.edit_message_text(revoke_msg, parse_mode=ParseMode.MARKDOWN, reply_markup=wallet_kb())
    elif action == "remove_active":
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "testnet"
        ok, msg = remove_user_private_key(telegram_id, network)
        if ok:
            from src.nadobro.handlers.messages import clear_session_passphrase
            clear_session_passphrase(context, telegram_id=telegram_id)
        prefix = "✅" if ok else "❌"
        await query.edit_message_text(
            f"{prefix} {escape_md(msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=wallet_kb(),
        )
    elif action == "network" and len(parts) >= 3:
        net = parts[2]
        if net not in ("testnet", "mainnet"):
            return

        success, result_msg = switch_network(telegram_id, net)

        if success:
            from src.nadobro.handlers.messages import clear_session_passphrase
            clear_session_passphrase(context, telegram_id=telegram_id)
            info = get_user_wallet_info(telegram_id)
            msg = fmt_wallet_info(info)
            await query.edit_message_text(
                f"{escape_md(result_msg)}\n\n{msg}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=wallet_kb(),
            )
        else:
            await query.edit_message_text(
                f"❌ {escape_md(result_msg)}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=wallet_kb(),
            )


async def _handle_alert(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "menu":
        await query.edit_message_text(
            "🔔 *Alert Engine*\n\nManage your trigger alerts\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=alerts_kb(),
        )

    elif action == "set":
        await query.edit_message_text(
            "🔔 *Set Alert*\n\nSelect a product:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=alert_product_kb(),
        )

    elif action == "product" and len(parts) >= 3:
        product = parts[2]
        context.user_data["pending_alert"] = {"product": product}
        await query.edit_message_text(
            f"🔔 *Alert for {escape_md(product)}\\-PERP*\n\n"
            f"Enter condition and price:\n"
            f"Example: `above 100000`\n"
            f"Example: `below 90000`",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )

    elif action == "view":
        alerts = get_user_alerts(telegram_id)
        msg = fmt_alerts(alerts)
        kb = alert_delete_kb(alerts) if alerts else back_kb()
        await query.edit_message_text(
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
        await query.edit_message_text(
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
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(lev, slip),
        )

    elif action == "leverage_menu":
        await query.edit_message_text(
            "⚡ *Leverage Control*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_leverage_kb(),
        )
    elif action == "risk_menu":
        await query.edit_message_text(
            "🛡 *Choose Risk Profile*\n\n"
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
        await query.edit_message_text(
            f"✅ Default leverage set to {escape_md(f'{lev}x')}\n\n{msg}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(lev, slip),
        )

    elif action == "slippage_menu":
        await query.edit_message_text(
            "📊 *Slippage Control*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_slippage_kb(),
        )
    elif action == "language_menu":
        user = get_user(telegram_id)
        lang = (getattr(user, "language", None) or "en").lower()
        await query.edit_message_text(
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
        user = get_user(telegram_id)
        current = (getattr(user, "language", None) or lang).lower()
        with language_context(current):
            msg = fmt_settings(user_settings)
            lev = user_settings.get("default_leverage", 1)
            slip = user_settings.get("slippage", 1)
            await query.edit_message_text(
                f"✅ Language updated to *{escape_md(current.upper())}*\\.\n\n{msg}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=settings_kb(lev, slip),
            )

    elif action == "slippage" and len(parts) >= 3:
        slip = float(parts[2])
        _, user_settings = update_user_settings(
            telegram_id, lambda s: s.update({"slippage": slip})
        )
        msg = fmt_settings(user_settings)
        lev = user_settings.get("default_leverage", 1)
        await query.edit_message_text(
            f"✅ Slippage set to {escape_md(f'{slip}%')}\n\n{msg}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(lev, slip),
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
        await query.edit_message_text(
            f"✅ Risk profile set to *{escape_md(profile.upper())}*\n\n{msg}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(chosen["default_leverage"], chosen["slippage"]),
        )


async def _handle_strategy(query, data, context, telegram_id):
    supported = ("mm", "grid", "dn", "vol")
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    strategy_id = parts[2] if len(parts) > 2 else ""

    if action == "preview":
        if strategy_id not in supported:
            return
        selected_product = context.user_data.get(f"strategy_pair:{strategy_id}", "BTC")
        with timed_metric("cb.strategy.preview"):
            preview_text = await run_blocking(_build_strategy_preview_text, telegram_id, strategy_id, selected_product)
        await query.edit_message_text(
            preview_text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_action_kb(strategy_id, selected_product),
        )
    elif action == "pair" and len(parts) >= 4:
        strategy_id = parts[2]
        selected_product = parts[3].upper()
        if strategy_id not in supported:
            return
        allowed_pairs = ("BTC", "ETH") if strategy_id == "dn" else ("BTC", "ETH", "SOL")
        if selected_product not in allowed_pairs:
            return
        context.user_data[f"strategy_pair:{strategy_id}"] = selected_product
        with timed_metric("cb.strategy.preview"):
            preview_text = await run_blocking(_build_strategy_preview_text, telegram_id, strategy_id, selected_product)
        await query.edit_message_text(
            preview_text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_action_kb(strategy_id, selected_product),
        )
    elif action == "config":
        if strategy_id not in supported:
            return
        network, settings = get_user_settings(telegram_id)
        conf = settings.get("strategies", {}).get(strategy_id, {})
        await query.edit_message_text(
            _fmt_strategy_config_text(strategy_id, conf, network),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_strategy_config_kb(strategy_id),
        )
    elif action == "set" and len(parts) >= 5:
        strategy_id = parts[2]
        field = parts[3]
        raw_value = parts[4]
        if strategy_id not in supported:
            return
        value = float(raw_value)
        int_fields = {
            "interval_seconds", "levels", "max_open_orders",
            "auto_close_on_maintenance", "is_long_bias",
        }

        def _mutate(s):
            strategies = s.setdefault("strategies", {})
            cfg = strategies.setdefault(strategy_id, {})
            if field in int_fields:
                cfg[field] = int(value)
            else:
                cfg[field] = value

        network, settings = update_user_settings(telegram_id, _mutate)
        conf = settings.get("strategies", {}).get(strategy_id, {})
        await query.edit_message_text(
            _fmt_strategy_config_text(strategy_id, conf, network),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_strategy_config_kb(strategy_id),
        )
    elif action == "set_text" and len(parts) >= 5:
        strategy_id = parts[2]
        field = parts[3]
        raw_value = parts[4]
        if strategy_id not in supported:
            return
        allowed_text = {
            "reference_mode": {"mid", "ema_fast", "ema_slow", "last_fill"},
            "directional_bias": {"neutral", "long_bias", "short_bias"},
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
        await query.edit_message_text(
            _fmt_strategy_config_text(strategy_id, conf, network),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_strategy_config_kb(strategy_id),
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
            "dn_perp_leverage", "grid_reset_pct",
            "target_volume_usd", "flip_size_usd",
        )
        if field not in allowed_inputs:
            return
        context.user_data["pending_strategy_input"] = {
            "strategy": strategy_id,
            "field": field,
        }
        help_text = {
            "notional_usd": "Enter notional in USD \\(example: `150`\\)",
            "spread_bp": "Enter spread in bps \\(example: `6`\\)",
            "interval_seconds": "Enter loop interval seconds \\(example: `45`\\)",
            "tp_pct": "Enter take profit % \\(example: `1\\.2`\\)",
            "sl_pct": "Enter stop loss % \\(example: `0\\.7`\\)",
            "levels": "Enter grid levels \\(example: `4`\\)",
            "min_range_pct": "Enter min range % \\(example: `1\\.0`\\)",
            "max_range_pct": "Enter max range % \\(example: `2\\.0`\\)",
            "threshold_bp": "Enter threshold in bps \\(example: `12`\\)",
            "close_offset_bp": "Enter close offset in bps \\(example: `25`\\)",
            "cycle_notional_usd": "Enter cycle notional in USD \\(example: `75`\\)",
            "session_notional_cap_usd": "Enter optional session cap in USD \\(example: `5000`, or `0` to disable\\)",
            "inventory_soft_limit_usd": "Enter inventory soft limit in USD \\(example: `45`\\)",
            "quote_ttl_seconds": "Enter quote TTL seconds \\(example: `90`\\)",
            "min_spread_bp": "Enter minimum spread in bps \\(example: `2`\\)",
            "max_spread_bp": "Enter maximum spread in bps \\(example: `20`\\)",
            "vol_sensitivity": "Enter volatility sensitivity \\(example: `0\\.02`\\)",
            "dn_perp_leverage": "Enter DN perp leverage from `1` to `5` \\(example: `3`\\)",
            "grid_reset_pct": "Enter grid reset % when mid drifts from last fill \\(example: `2`\\)",
            "target_volume_usd": "Enter target volume in USD to complete \\(example: `10000`\\)",
            "flip_size_usd": "Enter per\\-flip notional in USD \\(example: `200`\\)",
        }
        await query.edit_message_text(
            f"✏️ *Custom {escape_md(field)}*\n\n"
            f"{help_text.get(field, 'Enter value')}\n\n"
            "Your next message will be used as this value\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb("strategy_hub"),
        )
    elif action == "start" and len(parts) >= 4:
        strategy_id = parts[2]
        product = parts[3]
        if strategy_id not in supported:
            return
        if not is_new_onboarding_complete(telegram_id):
            await query.edit_message_text(
                "⚠️ Complete setup first (language + accept terms).",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶ Complete setup", callback_data="onboarding:resume")],
                    [InlineKeyboardButton("Exit", callback_data="nav:main")],
                ]),
            )
            return
        wallet_ready, _ = ensure_active_wallet_ready(telegram_id)
        if not wallet_ready:
            await prompt_wallet_setup(
                query,
                context,
                telegram_id,
                lead_text="⚠️ Link your wallet first to launch strategy automation.",
            )
            return
        settings = _get_user_settings(telegram_id, context)
        from src.nadobro.handlers.messages import authorize_or_prompt_passphrase
        if strategy_id == "mm":
            strategy_leverage = 1
        elif strategy_id == "vol":
            vol_conf = (settings.get("strategies", {}) or {}).get("vol", {}) or {}
            fs = float(vol_conf.get("flip_size_usd") or vol_conf.get("notional_usd") or 200.0)
            vn = float(vol_conf.get("notional_usd", 100.0))
            if vn > 0 and fs > vn:
                import math
                req = fs / vn
                strategy_leverage = min(VOL_MAX_LEVERAGE, max(1.0, math.ceil(req * 10) / 10))
            else:
                strategy_leverage = 1
        elif strategy_id == "dn":
            dn_cfg = ((settings.get("strategies", {}) or {}).get("dn", {}) or {})
            strategy_leverage = min(_safe_float(dn_cfg.get("dn_perp_leverage", 3), 3.0), 5.0)
        else:
            strategy_leverage = settings.get("default_leverage", 3)
        await authorize_or_prompt_passphrase(query, context, telegram_id, {
            "type": "start_strategy",
            "strategy": strategy_id,
            "product": product,
            "leverage": strategy_leverage,
            "slippage_pct": settings.get("slippage", 1),
        })
    elif action == "status":
        st = get_user_bot_status(telegram_id)
        readiness = evaluate_readiness(telegram_id)
        text = fmt_status_overview(st, readiness)
        if st.get("last_error"):
            text += f"\nLast error: {escape_md(str(st.get('last_error')))}"
        try:
            await query.edit_message_text(
                text,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=strategy_status_kb(st.get("strategy")),
            )
        except BadRequest as e:
            # Keep status usable even if a runtime error string carries markdown-unsafe chars.
            if "Can't parse entities" not in str(e):
                raise
            logger.warning("Status markdown parse failed, falling back to plain text: %s", e)
            cleaned = text.replace("\\", "")
            try:
                await query.edit_message_text(
                    cleaned,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=strategy_status_kb(st.get("strategy")),
                )
            except BadRequest:
                await query.edit_message_text(
                    cleaned,
                    reply_markup=strategy_status_kb(st.get("strategy")),
                )
    elif action == "funding":
        strategy_target = (parts[2] if len(parts) > 2 else "dn").lower()
        st = get_user_bot_status(telegram_id)
        active_strategy = str(st.get("strategy") or "").lower()
        if strategy_target != "dn" and active_strategy != "dn":
            await query.edit_message_text(
                "📈 Funding details are available for Delta Neutral runtime only.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=strategy_status_kb(st.get("strategy")),
            )
            return
        fr = float(st.get("dn_last_funding_rate") or 0.0)
        f_cycle = float(st.get("dn_last_funding_cycle") or 0.0)
        f_recv = float(st.get("dn_funding_received") or 0.0)
        f_paid = float(st.get("dn_funding_paid") or 0.0)
        f_net = float(st.get("dn_funding_net") or 0.0)
        spot_size = float(st.get("dn_spot_size") or 0.0)
        perp_size = float(st.get("dn_perp_size") or 0.0)
        hedge_diff = float(st.get("dn_hedge_diff_size") or 0.0)
        cycle_left = int(st.get("dn_cycle_remaining_seconds") or 0)
        f_cycle_str = f"+${f_cycle:,.4f}" if f_cycle >= 0 else f"-${abs(f_cycle):,.4f}"
        f_net_str = f"+${f_net:,.4f}" if f_net >= 0 else f"-${abs(f_net):,.4f}"
        runtime_state = "LIVE" if st.get("running") and active_strategy == "dn" else "IDLE"
        asset = str(st.get("product") or "BTC").upper()
        text = (
            "📈 *Delta Neutral Funding Details*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Runtime: *{escape_md(runtime_state)}*\n"
            f"Funding Rate: *{escape_md(f'{fr:.8f}')}*\n"
            f"Last Cycle Estimate: *{escape_md(f_cycle_str)}*\n\n"
            f"Total Received: *{escape_md(f'${f_recv:,.4f}')}*\n"
            f"Total Paid: *{escape_md(f'${f_paid:,.4f}')}*\n"
            f"Net Funding: *{escape_md(f_net_str)}*\n\n"
            f"Spot Size: *{escape_md(f'{spot_size:.6f} {asset}')}*\n"
            f"Perp Size: *{escape_md(f'{perp_size:.6f} {asset}')}*\n"
            f"Hedge Diff: *{escape_md(f'{hedge_diff:.6f} {asset}')}*\n"
            f"Cycle Remaining: *{escape_md(f'{max(0, cycle_left // 60)}m')}*"
        )
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_funding_kb("dn"),
        )
    elif action == "stop":
        ok, msg = stop_user_bot(telegram_id, cancel_orders=True)
        prefix = "🛑" if ok else "⚠️"
        await query.edit_message_text(
            f"{prefix} {escape_md(msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
        )


def _get_user_settings(telegram_id: int, context: CallbackContext) -> dict:
    from src.nadobro.handlers import shared_get_user_settings
    return shared_get_user_settings(telegram_id, context)


def _fmt_strategy_config_text(strategy: str, conf: dict, network: str) -> str:
    notional = float(conf.get("notional_usd", 100.0))
    spread_bp = float(conf.get("spread_bp", 5.0))
    interval_seconds = int(conf.get("interval_seconds", 60))
    tp_pct = float(conf.get("tp_pct", 1.0))
    sl_pct = float(conf.get("sl_pct", 0.5))
    base = (
        f"⚙️ *{escape_md(strategy.upper())} PARAMS \\| ROBOTIC MODE*\n\n"
        f"Mode: *{escape_md(network.upper())}*\n"
        f"Notional: *{escape_md(f'${notional:,.2f}')}* \\| Spread: *{escape_md(f'{spread_bp:.1f} bp')}*\n"
        f"Interval: *{escape_md(f'{interval_seconds}s')}*\n"
        f"TP/SL: *{escape_md(f'{tp_pct:.2f}%/{sl_pct:.2f}%')}*\n\n"
    )
    extra = ""
    if strategy == "grid":
        min_range = f"{float(conf.get('min_range_pct', 1.0)):.2f}%"
        max_range = f"{float(conf.get('max_range_pct', 1.0)):.2f}%"
        ref_mode = str(conf.get("reference_mode", "last_fill")).upper()
        bias = str(conf.get("directional_bias", "neutral")).upper()
        reset_pct = float(conf.get("grid_reset_pct", 2.0))
        extra = (
            f"Grid Levels: *{escape_md(str(int(conf.get('levels', 4))))}* \\| "
            f"Range: *{escape_md(min_range)} \\- {escape_md(max_range)}*\n"
            f"Ref: *{escape_md(ref_mode)}* \\| Bias: *{escape_md(bias)}* \\| Reset: *{escape_md(f'{reset_pct:.2f}%')}*\n\n"
        )
    elif strategy == "mm":
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
            f"Threshold: *{escape_md(threshold)}* \\| "
            f"Close Offset: *{escape_md(close_offset)}*\n"
            f"Ref Mode: *{escape_md(ref_mode)}* \\| Bias: *{escape_md(bias)}*\n"
            f"Cycle Notional: *{escape_md(f'${cycle_notional:,.2f}')}* \\| Session Cap: *{escape_md(cap_str)}*\n"
            f"Inv Soft Limit: *{escape_md(f'${inv_soft:,.2f}')}* \\| Quote TTL: *{escape_md(f'{quote_ttl}s')}*\n"
            f"Spread Band: *{escape_md(spread_band)}* \\| "
            f"Vol Sensitivity: *{escape_md(f'{vol_sensitivity:.3f}')}*\n\n"
        )
    elif strategy == "dn":
        auto_close = "ON" if float(conf.get("auto_close_on_maintenance", 1) or 0) >= 0.5 else "OFF"
        dn_lev = min(_safe_float(conf.get("dn_perp_leverage", 3), 3.0), 5.0)
        extra = (
            f"DN Perp Leverage: *{escape_md(f'{dn_lev:.0f}x')}* \\(max 5x\\)\n"
            f"Auto\\-close on maintenance: *{escape_md(auto_close)}*\n"
            f"Cycle window: *{escape_md('2.0h')}* \\(fixed auto close and restart\\)\n"
            "Execution: *Limit orders only* \\(spot \\+ perp at same limit price\\)\n\n"
        )
    elif strategy == "vol":
        target_vol = float(conf.get("target_volume_usd", 10000.0) or 10000.0)
        flip_size = float(conf.get("flip_size_usd", 200.0) or conf.get("notional_usd", 200.0) or 200.0)
        extra = (
            f"Target Volume: *{escape_md(f'${target_vol:,.0f}')}*\n"
            f"Flip Size \\(per cycle\\): *{escape_md(f'${flip_size:,.2f}')}*\n\n"
        )
    return base + extra + "Use presets or set custom values below\\."


def _strategy_config_kb(strategy: str):
    rows = [
        [
            InlineKeyboardButton("Notional $50", callback_data=f"strategy:set:{strategy}:notional_usd:50"),
            InlineKeyboardButton("Notional $100", callback_data=f"strategy:set:{strategy}:notional_usd:100"),
            InlineKeyboardButton("Notional $250", callback_data=f"strategy:set:{strategy}:notional_usd:250"),
        ],
        [
            InlineKeyboardButton("Custom Notional", callback_data=f"strategy:input:{strategy}:notional_usd"),
        ],
        [
            InlineKeyboardButton("Spread 2bp", callback_data=f"strategy:set:{strategy}:spread_bp:2"),
            InlineKeyboardButton("Spread 5bp", callback_data=f"strategy:set:{strategy}:spread_bp:5"),
            InlineKeyboardButton("Spread 10bp", callback_data=f"strategy:set:{strategy}:spread_bp:10"),
        ],
        [
            InlineKeyboardButton("Custom Spread", callback_data=f"strategy:input:{strategy}:spread_bp"),
        ],
        [
            InlineKeyboardButton("30s", callback_data=f"strategy:set:{strategy}:interval_seconds:30"),
            InlineKeyboardButton("60s", callback_data=f"strategy:set:{strategy}:interval_seconds:60"),
            InlineKeyboardButton("120s", callback_data=f"strategy:set:{strategy}:interval_seconds:120"),
        ],
        [
            InlineKeyboardButton("Custom Interval", callback_data=f"strategy:input:{strategy}:interval_seconds"),
        ],
        [
            InlineKeyboardButton("TP 0.5%", callback_data=f"strategy:set:{strategy}:tp_pct:0.5"),
            InlineKeyboardButton("TP 1.0%", callback_data=f"strategy:set:{strategy}:tp_pct:1.0"),
            InlineKeyboardButton("TP 2.0%", callback_data=f"strategy:set:{strategy}:tp_pct:2.0"),
        ],
        [
            InlineKeyboardButton("SL 0.25%", callback_data=f"strategy:set:{strategy}:sl_pct:0.25"),
            InlineKeyboardButton("SL 0.5%", callback_data=f"strategy:set:{strategy}:sl_pct:0.5"),
            InlineKeyboardButton("SL 1.0%", callback_data=f"strategy:set:{strategy}:sl_pct:1.0"),
        ],
        [
            InlineKeyboardButton("Custom TP", callback_data=f"strategy:input:{strategy}:tp_pct"),
            InlineKeyboardButton("Custom SL", callback_data=f"strategy:input:{strategy}:sl_pct"),
        ],
    ]
    if strategy == "grid":
        rows.extend([
            [
                InlineKeyboardButton("Levels 3", callback_data=f"strategy:set:{strategy}:levels:3"),
                InlineKeyboardButton("Levels 5", callback_data=f"strategy:set:{strategy}:levels:5"),
                InlineKeyboardButton("Levels 7", callback_data=f"strategy:set:{strategy}:levels:7"),
            ],
            [
                InlineKeyboardButton("Range 1%/1%", callback_data=f"strategy:set:{strategy}:min_range_pct:1"),
                InlineKeyboardButton("Range 1%/2%", callback_data=f"strategy:set:{strategy}:max_range_pct:2"),
            ],
            [
                InlineKeyboardButton("Ref Last Fill", callback_data=f"strategy:set_text:{strategy}:reference_mode:last_fill"),
                InlineKeyboardButton("Ref MID", callback_data=f"strategy:set_text:{strategy}:reference_mode:mid"),
            ],
            [
                InlineKeyboardButton("Bias Neutral", callback_data=f"strategy:set_text:{strategy}:directional_bias:neutral"),
                InlineKeyboardButton("Bias Long", callback_data=f"strategy:set_text:{strategy}:directional_bias:long_bias"),
                InlineKeyboardButton("Bias Short", callback_data=f"strategy:set_text:{strategy}:directional_bias:short_bias"),
            ],
            [
                InlineKeyboardButton("Reset 1.5%", callback_data=f"strategy:set:{strategy}:grid_reset_pct:1.5"),
                InlineKeyboardButton("Reset 2%", callback_data=f"strategy:set:{strategy}:grid_reset_pct:2"),
                InlineKeyboardButton("Reset 3%", callback_data=f"strategy:set:{strategy}:grid_reset_pct:3"),
            ],
            [
                InlineKeyboardButton("Custom Levels", callback_data=f"strategy:input:{strategy}:levels"),
                InlineKeyboardButton("Custom Range", callback_data=f"strategy:input:{strategy}:max_range_pct"),
                InlineKeyboardButton("Custom Reset %", callback_data=f"strategy:input:{strategy}:grid_reset_pct"),
            ],
        ])
    if strategy == "mm":
        rows.extend([
            [
                InlineKeyboardButton("Threshold 8bp", callback_data=f"strategy:set:{strategy}:threshold_bp:8"),
                InlineKeyboardButton("Threshold 12bp", callback_data=f"strategy:set:{strategy}:threshold_bp:12"),
                InlineKeyboardButton("Threshold 20bp", callback_data=f"strategy:set:{strategy}:threshold_bp:20"),
            ],
            [
                InlineKeyboardButton("Close 20bp", callback_data=f"strategy:set:{strategy}:close_offset_bp:20"),
                InlineKeyboardButton("Close 30bp", callback_data=f"strategy:set:{strategy}:close_offset_bp:30"),
            ],
            [
                InlineKeyboardButton("Custom Threshold", callback_data=f"strategy:input:{strategy}:threshold_bp"),
                InlineKeyboardButton("Custom Close", callback_data=f"strategy:input:{strategy}:close_offset_bp"),
            ],
            [
                InlineKeyboardButton("Ref MID", callback_data=f"strategy:set_text:{strategy}:reference_mode:mid"),
                InlineKeyboardButton("Ref EMA Fast", callback_data=f"strategy:set_text:{strategy}:reference_mode:ema_fast"),
                InlineKeyboardButton("Ref EMA Slow", callback_data=f"strategy:set_text:{strategy}:reference_mode:ema_slow"),
                InlineKeyboardButton("Ref Last Fill", callback_data=f"strategy:set_text:{strategy}:reference_mode:last_fill"),
            ],
            [
                InlineKeyboardButton("Bias Neutral", callback_data=f"strategy:set_text:{strategy}:directional_bias:neutral"),
                InlineKeyboardButton("Bias Long", callback_data=f"strategy:set_text:{strategy}:directional_bias:long_bias"),
                InlineKeyboardButton("Bias Short", callback_data=f"strategy:set_text:{strategy}:directional_bias:short_bias"),
            ],
            [
                InlineKeyboardButton("Cycle $50", callback_data=f"strategy:set:{strategy}:cycle_notional_usd:50"),
                InlineKeyboardButton("Cycle $100", callback_data=f"strategy:set:{strategy}:cycle_notional_usd:100"),
                InlineKeyboardButton("Cycle $250", callback_data=f"strategy:set:{strategy}:cycle_notional_usd:250"),
            ],
            [
                InlineKeyboardButton("Inv Limit $30", callback_data=f"strategy:set:{strategy}:inventory_soft_limit_usd:30"),
                InlineKeyboardButton("Inv Limit $60", callback_data=f"strategy:set:{strategy}:inventory_soft_limit_usd:60"),
                InlineKeyboardButton("Inv Limit $120", callback_data=f"strategy:set:{strategy}:inventory_soft_limit_usd:120"),
            ],
            [
                InlineKeyboardButton("Custom Cycle", callback_data=f"strategy:input:{strategy}:cycle_notional_usd"),
                InlineKeyboardButton("Custom Inv Limit", callback_data=f"strategy:input:{strategy}:inventory_soft_limit_usd"),
            ],
            [
                InlineKeyboardButton("TTL 60s", callback_data=f"strategy:set:{strategy}:quote_ttl_seconds:60"),
                InlineKeyboardButton("TTL 90s", callback_data=f"strategy:set:{strategy}:quote_ttl_seconds:90"),
                InlineKeyboardButton("TTL 120s", callback_data=f"strategy:set:{strategy}:quote_ttl_seconds:120"),
            ],
            [
                InlineKeyboardButton("Spread Min 2bp", callback_data=f"strategy:set:{strategy}:min_spread_bp:2"),
                InlineKeyboardButton("Spread Max 20bp", callback_data=f"strategy:set:{strategy}:max_spread_bp:20"),
            ],
            [
                InlineKeyboardButton("Custom TTL", callback_data=f"strategy:input:{strategy}:quote_ttl_seconds"),
                InlineKeyboardButton("Custom Session Cap", callback_data=f"strategy:input:{strategy}:session_notional_cap_usd"),
            ],
        ])
    if strategy == "dn":
        rows.extend([
            [
                InlineKeyboardButton("Leverage 2x", callback_data=f"strategy:set:{strategy}:dn_perp_leverage:2"),
                InlineKeyboardButton("Leverage 3x", callback_data=f"strategy:set:{strategy}:dn_perp_leverage:3"),
                InlineKeyboardButton("Leverage 5x", callback_data=f"strategy:set:{strategy}:dn_perp_leverage:5"),
            ],
            [
                InlineKeyboardButton("Custom Leverage", callback_data=f"strategy:input:{strategy}:dn_perp_leverage"),
            ],
            [
                InlineKeyboardButton("Auto-Close ON", callback_data=f"strategy:set:{strategy}:auto_close_on_maintenance:1"),
                InlineKeyboardButton("Auto-Close OFF", callback_data=f"strategy:set:{strategy}:auto_close_on_maintenance:0"),
            ],
        ])
    if strategy == "vol":
        rows.extend([
            [
                InlineKeyboardButton("Target $5K", callback_data=f"strategy:set:{strategy}:target_volume_usd:5000"),
                InlineKeyboardButton("Target $10K", callback_data=f"strategy:set:{strategy}:target_volume_usd:10000"),
                InlineKeyboardButton("Target $25K", callback_data=f"strategy:set:{strategy}:target_volume_usd:25000"),
            ],
            [
                InlineKeyboardButton("Flip $50", callback_data=f"strategy:set:{strategy}:flip_size_usd:50"),
                InlineKeyboardButton("Flip $100", callback_data=f"strategy:set:{strategy}:flip_size_usd:100"),
                InlineKeyboardButton("Flip $200", callback_data=f"strategy:set:{strategy}:flip_size_usd:200"),
            ],
            [
                InlineKeyboardButton("Custom Target", callback_data=f"strategy:input:{strategy}:target_volume_usd"),
                InlineKeyboardButton("Custom Flip", callback_data=f"strategy:input:{strategy}:flip_size_usd"),
            ],
        ])
    rows.append([InlineKeyboardButton("◀ Back", callback_data=f"strategy:preview:{strategy}")])
    return InlineKeyboardMarkup(rows)


def _build_strategy_preview_text(telegram_id: int, strategy_id: str, product: str) -> str:
    names = {
        "mm": "MM Bot",
        "grid": "Grid Reactor",
        "dn": "Delta Neutral",
        "vol": "Volume Bot",
    }
    network, settings = get_user_settings(telegram_id)
    conf = settings.get("strategies", {}).get(strategy_id, {})
    notional = float(conf.get("notional_usd", 100.0))
    cycle_notional = float(conf.get("cycle_notional_usd", notional))
    session_cap = float(conf.get("session_notional_cap_usd", 0) or 0)
    spread_bp = float(conf.get("spread_bp", 5.0))
    interval_seconds = int(conf.get("interval_seconds", 60))
    tp_pct = float(conf.get("tp_pct", 1.0))
    sl_pct = float(conf.get("sl_pct", 0.5))
    leverage = 1.0 if strategy_id in ("vol", "mm") else float(settings.get("default_leverage", 3))
    if strategy_id == "dn":
        leverage = min(_safe_float(conf.get("dn_perp_leverage", leverage), leverage), 5.0)
    elif strategy_id == "vol":
        # Auto-compute leverage: flip_size/notional (min to support flip with given margin), capped 1-5x
        flip_size_usd = float(conf.get("flip_size_usd") or conf.get("notional_usd") or 200.0)
        vol_notional = float(conf.get("notional_usd", 100.0))
        if vol_notional > 0 and flip_size_usd > vol_notional:
            import math
            required = flip_size_usd / vol_notional
            leverage = min(float(VOL_MAX_LEVERAGE), max(1.0, math.ceil(required * 10) / 10))
        else:
            leverage = 1.0
    slippage = float(settings.get("slippage", 1))
    runtime_status = get_user_bot_status(telegram_id)

    mid = 0.0
    funding_rate = 0.0
    client = get_user_readonly_client(telegram_id)
    if client:
        try:
            pid = get_product_id(product)
            if pid is not None:
                mp = client.get_market_price(pid)
                mid = float(mp.get("mid", 0) or 0)
                fr = client.get_funding_rate(pid) or {}
                funding_rate = float(fr.get("funding_rate", 0) or 0)
        except Exception:
            pass

    # Vol strategy: notional = margin/capital, flip_size = trade notional; leverage = flip/margin
    flip_size_usd = float(conf.get("flip_size_usd") or conf.get("notional_usd") or 200.0)
    target_volume_usd = float(conf.get("target_volume_usd", 10000.0) or 10000.0)
    if strategy_id == "vol":
        vol_margin = notional  # notional = capital per flip
        pre_trade_margin = vol_margin
    elif strategy_id in ("mm", "grid"):
        pre_trade_margin = notional / leverage if leverage > 0 else notional
    else:
        pre_trade_margin = notional / leverage if leverage > 0 else notional

    cycles_per_day = 86400 / max(interval_seconds, 10)
    if strategy_id in ("mm", "grid"):
        est_daily_volume = notional * 2.0 * cycles_per_day
    else:
        est_daily_volume = cycle_notional * 2.0 * cycles_per_day

    # Conservative fee estimate using builder fee (2 bps) + maker fee proxy (1 bp).
    from src.nadobro.config import EST_FEE_RATE, EST_FILL_EFFICIENCY
    est_fees = est_daily_volume * EST_FEE_RATE
    if strategy_id in ("mm", "grid"):
        # Per-cycle fees (bid + ask) for display, comparable to per-cycle margin
        est_fees_display = 2.0 * notional * EST_FEE_RATE
    else:
        est_fees_display = est_fees / cycles_per_day if cycles_per_day > 0 else est_fees

    est_spread_pnl = est_daily_volume * (spread_bp / 10000.0) * EST_FILL_EFFICIENCY
    est_funding = 0.0
    if strategy_id == "dn":
        est_funding = abs(funding_rate) * notional * 3
    max_loss = pre_trade_margin * (sl_pct / 100.0)
    est_fees_vol = target_volume_usd * EST_FEE_RATE if strategy_id == "vol" else 0.0
    if strategy_id == "vol":
        est_net = -est_fees_vol  # Vol bot pays fees on target volume; no spread capture
    else:
        est_net = est_spread_pnl + est_funding - est_fees

    mid_str = f"${fmt_price(mid, product)}" if mid > 0 else "N/A"

    pre_trade_block = ""
    if strategy_id == "vol":
        vol_fees_per_cycle = est_fees_vol / cycles_per_day if cycles_per_day > 0 else est_fees_vol
        pre_trade_block = fmt_pre_trade_analytics(
            margin=vol_margin,
            est_volume=target_volume_usd,
            max_loss=max_loss,
            estimated_fees=vol_fees_per_cycle,
            fees_label="Est\\. Fees/cycle",
        ) + "\n"
    else:
        pre_trade_block = fmt_pre_trade_analytics(
            margin=pre_trade_margin,
            est_volume=est_daily_volume,
            max_loss=max_loss,
            estimated_fees=est_fees_display,
            fees_label="Est\\. Fees/cycle",
        ) + "\n"
    status_dot = "🟢" if est_net >= 0 else "🟠"
    how_it_works = {
        "mm": "Quotes around mid price, captures spread, auto\\-reposts each cycle\\.",
        "grid": "Staggered levels above/below mid — buys low, sells high as price moves\\.",
        "dn": "Offsetting long/short to earn spread \\+ funding with reduced risk\\.",
        "vol": "Balanced two\\-sided flow with risk caps for consistent volume\\.",
    }
    selected_explainer = how_it_works.get(strategy_id, "Automates trade cycles with configured risk controls\\.")
    pair_support_line = ""
    if strategy_id == "dn":
        pair_support_line = "\nSupported DN Pairs: *BTC, ETH*"
    extra_cfg = ""
    if strategy_id == "grid":
        min_range = f"{float(conf.get('min_range_pct', 1.0)):.2f}%"
        max_range = f"{float(conf.get('max_range_pct', 1.0)):.2f}%"
        ref_mode = str(conf.get("reference_mode", "last_fill")).upper()
        bias = str(conf.get("directional_bias", "neutral")).upper()
        reset_pct = float(conf.get("grid_reset_pct", 2.0))
        extra_cfg = (
            f"\nGrid Levels: *{escape_md(str(int(conf.get('levels', 4))))}* \\| "
            f"Range: *{escape_md(min_range)} \\- {escape_md(max_range)}*"
            f"\nRef: *{escape_md(ref_mode)}* \\| Bias: *{escape_md(bias)}* \\| Reset: *{escape_md(f'{reset_pct:.2f}%')}*"
        )
    elif strategy_id == "mm":
        threshold = f"{float(conf.get('threshold_bp', 12.0)):.1f} bp"
        close_offset = f"{float(conf.get('close_offset_bp', 24.0)):.1f} bp"
        ref_mode = str(conf.get("reference_mode", "ema_fast")).upper()
        bias = str(conf.get("directional_bias", "neutral")).upper()
        inv_soft = float(conf.get("inventory_soft_limit_usd", notional * 0.6))
        min_spread = float(conf.get("min_spread_bp", 2.0))
        max_spread = float(conf.get("max_spread_bp", 20.0))
        quote_ttl = int(conf.get("quote_ttl_seconds", max(60, interval_seconds * 2)))
        cap_str = f"${session_cap:,.0f}" if session_cap > 0 else "OFF"
        spread_band = f"{min_spread:.1f} - {max_spread:.1f} bp"
        extra_cfg = (
            f"\nThreshold: *{escape_md(threshold)}* \\| "
            f"Close Offset: *{escape_md(close_offset)}*"
            f"\nRef Mode: *{escape_md(ref_mode)}* \\| Bias: *{escape_md(bias)}*"
            f"\nSpread Band: *{escape_md(spread_band)}* \\| "
            f"Quote TTL: *{escape_md(f'{quote_ttl}s')}*"
            f"\nCycle Notional: *{escape_md(f'${cycle_notional:,.2f}')}* \\| "
            f"Session Cap: *{escape_md(cap_str)}*"
            f"\nInv Soft Limit: *{escape_md(f'${inv_soft:,.2f}')}*"
        )
    elif strategy_id == "vol":
        target_vol = float(conf.get("target_volume_usd", 10000.0) or 10000.0)
        max_flip_at_cap = notional * VOL_MAX_LEVERAGE
        flip_exceeds = flip_size_usd > max_flip_at_cap
        extra_cfg = (
            f"\nTarget Volume: *{escape_md(f'${target_vol:,.0f}')}* \\| "
            f"Flip Size: *{escape_md(f'${flip_size_usd:,.2f}')}*"
        )
        if flip_exceeds:
            extra_cfg += (
                f"\n⚠️ Flip size exceeds max at 5x leverage with margin "
                f"{escape_md(f'${notional:,.0f}')}\\. "
                f"Reduce flip to {escape_md(f'${max_flip_at_cap:,.0f}')} or increase margin\\."
            )
    elif strategy_id == "dn":
        auto_close = "ON" if float(conf.get("auto_close_on_maintenance", 1) or 0) >= 0.5 else "OFF"
        dn_margin = notional / leverage if leverage > 0 else notional
        extra_cfg = (
            f"\nPerp Leverage: *{escape_md(f'{leverage:.0f}x')}* \\| Perp Margin: *{escape_md(f'${dn_margin:,.2f}')}*"
            f"\nAuto\\-close on maintenance: *{escape_md(auto_close)}*"
            f"\nCycle window: *{escape_md('2h')}* \\(fixed auto roll\\)"
            f"\nExecution: *{escape_md('Limit orders only')}*"
        )
        if runtime_status.get("running") and str(runtime_status.get("strategy", "")).lower() == "dn":
            f_recv = float(runtime_status.get("dn_funding_received") or 0.0)
            f_paid = float(runtime_status.get("dn_funding_paid") or 0.0)
            f_net = float(runtime_status.get("dn_funding_net") or 0.0)
            f_net_str = f"+${f_net:,.4f}" if f_net >= 0 else f"-${abs(f_net):,.4f}"
            cycle_left = int(runtime_status.get("dn_cycle_remaining_seconds") or 0)
            extra_cfg += (
                f"\nFunding Received/Paid: *{escape_md(f'${f_recv:,.4f}')}"
                f" / {escape_md(f'${f_paid:,.4f}')}*"
                f"\nFunding Net: *{escape_md(f_net_str)}*"
                f"\nCycle Remaining: *{escape_md(f'{max(0, cycle_left // 60)}m')}*"
            )
    return (
        f"🤖 *{escape_md(names.get(strategy_id, strategy_id.upper()))} Dashboard*\n"
        f"Status: {status_dot} *READY*\n"
        f"{escape_md(selected_explainer)}\n\n"
        f"📊 *Settings*\n"
        f"{pair_support_line}\n"
        f"Pair: *{escape_md(product)}\\-PERP* \\| Mid: *{escape_md(mid_str)}*\n"
        f"Mode: *{escape_md(network.upper())}* \\| "
        f"Leverage: *{escape_md(f'{leverage:.1f}x' if strategy_id == 'vol' and leverage != int(leverage) else f'{leverage:.0f}x')}* \\| "
        f"Slippage: *{escape_md(f'{slippage:.2f}%')}*\n"
        f"{'Notional \\(margin\\)' if strategy_id == 'vol' else 'Notional'}: *{escape_md(f'${notional:,.2f}')}* \\| Spread: *{escape_md(f'{spread_bp:.1f} bp')}* \\| Interval: *{escape_md(f'{interval_seconds}s')}*\n"
        f"TP/SL: *{escape_md(f'{tp_pct:.2f}%/{sl_pct:.2f}%')}*"
        f"{extra_cfg}\n\n"
        f"{pre_trade_block}\n"
        "Edit parameters or launch below\\."
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
        await query.edit_message_text(
            _ONB_WELCOME_LANG_MSG,
            reply_markup=onboarding_language_kb(),
        )
    else:
        await query.edit_message_text(
            _ONB_WELCOME_CARD,
            reply_markup=onboarding_accept_tos_kb(),
        )


async def _delete_message_later(query, chat_id: int, message_id: int, delay_seconds: int = 30):
    await asyncio.sleep(delay_seconds)
    try:
        await query.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass
