import logging
import time
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import CallbackContext
from telegram.constants import ParseMode, ChatAction
from src.nadobro.handlers.formatters import (
    escape_md, fmt_positions,
    fmt_prices, fmt_funding, fmt_trade_preview, fmt_trade_result,
    fmt_wallet_info, fmt_alerts, fmt_portfolio,
    fmt_settings, fmt_help, fmt_price, fmt_status_overview,
)
from src.nadobro.handlers.keyboards import (
    persistent_menu_kb, trade_product_kb, trade_size_kb, trade_leverage_kb,
    trade_confirm_kb, positions_kb, wallet_kb, wallet_kb_not_linked, alerts_kb,
    alert_product_kb, alert_delete_kb, settings_kb, settings_leverage_kb,
    settings_slippage_kb, settings_language_kb, close_product_kb, confirm_close_all_kb, back_kb,
    risk_profile_kb, strategy_hub_kb, strategy_action_kb,
    onboarding_language_kb,
    markets_kb, live_price_asset_kb, live_price_controls_kb,
    mode_kb,     home_card_kb, portfolio_kb,
    onboarding_accept_tos_kb,
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
from src.nadobro.config import get_product_name, get_product_id, get_product_max_leverage, PRODUCTS
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.perf import timed_metric, log_slow, summary_lines

logger = logging.getLogger(__name__)
LIVE_PRICE_TASKS = {}


async def handle_callback(update: Update, context: CallbackContext):
    started = time.perf_counter()
    query = update.callback_query
    data = query.data
    telegram_id = query.from_user.id

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
        elif data.startswith("wallet:"):
            await _handle_wallet(query, data, telegram_id, context)
        elif data.startswith("mkt:"):
            await _handle_market(query, data, telegram_id)
        elif data.startswith("alert:"):
            await _handle_alert(query, data, telegram_id, context)
        elif data.startswith("settings:"):
            await _handle_settings(query, data, telegram_id, context)
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
_ONB_WELCOME_LANG_MSG = """Yo what's good, future Nado whale?! 👋💰

Welcome to **Nadobro** — your ultimate trading bro for Perps on Nado!

We're building the dopest Telegram bot on the planet:
• MM Bot (Grid + RGRID that actually prints)
• Delta Neutral (spot + short = chill funding gains)
• Volume Bot (farm that leaderboard volume like a boss)

Unified margin. 5-15ms execution. Zero drama.

First, pick your language vibe:"""

_ONB_WELCOME_CARD = """🔥 Nadobro Activated! You're in the squad bro 🔥

Sup, you're now locked in.
We run on Nado's lightning CLOB with unified margin — the cleanest perps game in crypto.

By tapping **"Let's Get It"** you're saying:
✅ I accept the Terms of Use & Privacy Policy

⚡ Bro-Note (read this):
We'll generate a secure 1CT key for your default subaccount (we NEVER touch your main wallet keys).
You paste the key into Nado → Settings → 1-Click Trading → Advanced 1CT (1 tx, 1 USDT0).
Main wallet stays untouched. Revoke anytime. Funds 100% yours.

Ready to start printing money?"""

_ONB_DASHBOARD_MSG = """🚀 Nadobro Dashboard — You're In, Legend!

What we smashing today?"""


async def _handle_onb_new(query, data, telegram_id, context):
    if data == "onb:accept_tos":
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
        await query.edit_message_text(
            _ONB_WELCOME_CARD,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=onboarding_accept_tos_kb(),
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
            from src.nadobro.handlers.messages import clear_session_passphrase
            clear_session_passphrase(context, telegram_id=telegram_id)
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
            "Pick a strategy to open its cockpit dashboard, tune risk, and launch with pre\\-trade analytics\\.",
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
    wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
    if action in ("long", "short", "limit_long", "limit_short") and not wallet_ready:
        await query.edit_message_text(
            f"⚠️ {escape_md(wallet_msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
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
            "*Close Position*\n\nSelect the product to close:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=close_product_kb(),
        )
    elif action == "close_all":
        await query.edit_message_text(
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

    preview = fmt_trade_preview(action, product, size, price, leverage, est_margin)
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
    wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
    if not wallet_ready:
        await query.edit_message_text(
            f"⚠️ {escape_md(wallet_msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
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
        },
    })


async def _handle_positions(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"

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

    elif action == "close_all":
        await query.edit_message_text(
            "⚠️ *Close All Orders*\n\nAre you sure?",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=confirm_close_all_kb(),
        )

    elif action == "confirm_close_all":
        from src.nadobro.handlers.messages import authorize_or_prompt_passphrase
        await authorize_or_prompt_passphrase(query, context, telegram_id, {"type": "close_all"})


async def _handle_portfolio(query, data, telegram_id):
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
    try:
        prices = await run_blocking(client.get_all_market_prices)
    except Exception:
        pass
    stats = await run_blocking(get_trade_analytics, telegram_id)
    msg = fmt_portfolio(stats, positions, prices)
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


async def _handle_wallet(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"

    if action == "view":
        info = get_user_wallet_info(telegram_id, verify_signer=True)
        if not info or not info.get("linked_signer_address"):
            from eth_account import Account
            account = Account.create()
            pk_hex = account.key.hex()
            if not pk_hex.startswith("0x"):
                pk_hex = "0x" + pk_hex
            context.user_data["wallet_flow"] = "awaiting_main_address"
            context.user_data["wallet_linked_signer_pk"] = pk_hex
            context.user_data["wallet_linked_signer_address"] = account.address
            msg = (
                "👛 *Wallet Connect*\n\n"
                "*Step 1:* Go to https://app.nado.xyz → connect wallet → deposit ≥ $5 USDT0\n\n"
                "*Step 2:* Go to Settings → 1-Click Trading → Advanced 1CT\n\n"
                "*Step 3:* Paste this key into the *1CT Private Key* field:\n\n"
                f"`{pk_hex}`\n\n"
                "*Step 4:* Enable the toggle, click *Save*, and confirm the transaction in your wallet (1 USDT0 fee)\n\n"
                "Once saved, reply here with your *main wallet address* (0x...).\n\n"
                "_This key is for trading only — it cannot withdraw your funds._"
            )
            await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=wallet_kb_not_linked())
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


async def _handle_market(query, data, telegram_id):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    task_key = _live_task_key(query, telegram_id)

    client = get_user_readonly_client(telegram_id)
    if not client:
        await query.edit_message_text(
            "⚠️ Wallet not initialized\\. Use /start first\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )
        return

    if action == "menu":
        await _stop_live_task(task_key)
        await query.edit_message_text(
            "📡 *Market Radar*\n\nPick a market view:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=markets_kb(),
        )

    elif action == "prices":
        await _stop_live_task(task_key)
        with timed_metric("cb.market.prices"):
            prices = await run_blocking(client.get_all_market_prices)
        msg = fmt_prices(prices)
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=markets_kb(),
        )

    elif action == "funding":
        await _stop_live_task(task_key)
        funding = {}
        with timed_metric("cb.market.funding"):
            all_rates = await run_blocking(client.get_all_funding_rates)
        for name, info in PRODUCTS.items():
            if info["type"] == "perp":
                fr = all_rates.get(info["id"])
                if fr:
                    funding[name] = fr.get("funding_rate", 0)

        msg = fmt_funding(funding)
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=markets_kb(),
        )
    elif action == "live_menu":
        await _stop_live_task(task_key)
        await query.edit_message_text(
            "🔴 *Live Last Price*\n\nSelect an asset:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=live_price_asset_kb(),
        )
    elif action == "live_stop":
        await _stop_live_task(task_key)
        await query.edit_message_text(
            "🛑 Live price updates stopped\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=markets_kb(),
        )
    elif action == "live" and len(parts) >= 3:
        product = parts[2].upper()
        if product not in PRODUCTS or PRODUCTS[product]["type"] != "perp":
            await query.edit_message_text(
                "⚠️ Unsupported product\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=live_price_asset_kb(),
            )
            return

        await _stop_live_task(task_key)
        pid = get_product_id(product)
        mp = await run_blocking(client.get_market_price, pid) if pid is not None else {"mid": 0}
        initial = _fmt_live_last_price(product, mp.get("mid", 0))
        message = await query.edit_message_text(
            initial,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=live_price_controls_kb(product),
        )
        LIVE_PRICE_TASKS[task_key] = asyncio.create_task(
            _live_price_loop(
                query.bot,
                telegram_id=telegram_id,
                chat_id=message.chat_id,
                message_id=message.message_id,
                product=product,
                task_key=task_key,
            )
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
        await query.edit_message_text(
            f"✅ Language updated to *{escape_md(lang.upper())}*\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_language_kb(current),
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
        if selected_product not in ("BTC", "ETH", "SOL"):
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
            "reference_mode": {"mid", "ema_fast", "ema_slow"},
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
        }
        await query.edit_message_text(
            f"✏️ *Custom {escape_md(field)}*\n\n"
            f"{help_text.get(field, 'Enter value')}\n\n"
            "Your next message will be used as this value\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb("strategy_hub"),
        )
    elif action == "activate":
        context.user_data["active_setup"] = strategy_id
        await query.edit_message_text(
            f"✅ Active setup is now *{escape_md(strategy_id.upper())}*\\.\n\n"
            "Next: open Buy/Long or Sell/Short and execute with preview\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
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
        wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
        if not wallet_ready:
            await query.edit_message_text(
                f"⚠️ {escape_md(wallet_msg)}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(),
            )
            return
        settings = _get_user_settings(telegram_id, context)
        from src.nadobro.handlers.messages import authorize_or_prompt_passphrase
        strategy_leverage = 1 if strategy_id in ("vol", "mm") else settings.get("default_leverage", 3)
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
        perf_lines = summary_lines(top_n=5)
        if perf_lines:
            text += "\n\n*Perf Snapshot*"
            for line in perf_lines:
                text += f"\n• {escape_md(line)}"
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN_V2,
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
        extra = (
            f"Grid Levels: *{escape_md(str(int(conf.get('levels', 4))))}* \\| "
            f"Range: *{escape_md(min_range)} \\- {escape_md(max_range)}*\n\n"
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
        extra = f"Auto-close on maintenance: *{escape_md(auto_close)}*\n\n"
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
                InlineKeyboardButton("Custom Levels", callback_data=f"strategy:input:{strategy}:levels"),
                InlineKeyboardButton("Custom Range", callback_data=f"strategy:input:{strategy}:max_range_pct"),
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
                InlineKeyboardButton("Auto-Close ON", callback_data=f"strategy:set:{strategy}:auto_close_on_maintenance:1"),
                InlineKeyboardButton("Auto-Close OFF", callback_data=f"strategy:set:{strategy}:auto_close_on_maintenance:0"),
            ],
        ])
    rows.append([InlineKeyboardButton("◀ Back", callback_data=f"strategy:preview:{strategy}")])
    return InlineKeyboardMarkup(rows)


def _build_strategy_preview_text(telegram_id: int, strategy_id: str, product: str) -> str:
    names = {
        "mm": "MM Bot",
        "grid": "Grid Reactor",
        "dn": "Mirror Delta Neutral",
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
    slippage = float(settings.get("slippage", 1))

    available_margin = 0.0
    mid = 0.0
    funding_rate = 0.0
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
        try:
            pid = get_product_id(product)
            if pid is not None:
                mp = client.get_market_price(pid)
                mid = float(mp.get("mid", 0) or 0)
                fr = client.get_funding_rate(pid) or {}
                funding_rate = float(fr.get("funding_rate", 0) or 0)
        except Exception:
            pass

    required_margin = cycle_notional / leverage if leverage > 0 else cycle_notional
    cycles_per_day = 86400 / max(interval_seconds, 10)
    est_daily_volume = cycle_notional * 2.0 * cycles_per_day

    # Conservative fee estimate using builder fee (2 bps) + maker fee proxy (1 bp).
    from src.nadobro.config import EST_FEE_RATE, EST_FILL_EFFICIENCY
    est_fees = est_daily_volume * EST_FEE_RATE

    est_spread_pnl = est_daily_volume * (spread_bp / 10000.0) * EST_FILL_EFFICIENCY
    est_funding = 0.0
    if strategy_id == "dn":
        est_funding = abs(funding_rate) * notional * 3
    max_loss = required_margin * (sl_pct / 100.0)
    est_net = est_spread_pnl + est_funding - est_fees

    margin_flag = "✅" if available_margin >= required_margin else "⚠️"
    mid_str = f"${fmt_price(mid, product)}" if mid > 0 else "N/A"
    funding_str = f"{funding_rate:.6f}"
    net_str = f"+${est_net:,.2f}" if est_net >= 0 else f"-${abs(est_net):,.2f}"
    status_dot = "🟢" if est_net >= 0 else "🟠"
    how_it_works = {
        "mm": (
            "Places maker buy/sell quotes around mid price, "
            "captures spread, then auto\\-reposts every cycle\\."
        ),
        "grid": (
            "Builds staggered levels above and below mid, "
            "buys lower and sells higher as price oscillates\\."
        ),
        "dn": (
            "Runs offsetting long/short exposure to reduce directional risk "
            "while aiming to earn spread \\+ funding edge\\."
        ),
        "vol": (
            "Executes balanced two\\-sided flow with risk caps "
            "to generate consistent trading activity and volume\\."
        ),
    }
    selected_explainer = how_it_works.get(strategy_id, "Automates recurring trade cycles with configured risk controls\\.")
    extra_cfg = ""
    if strategy_id == "grid":
        min_range = f"{float(conf.get('min_range_pct', 1.0)):.2f}%"
        max_range = f"{float(conf.get('max_range_pct', 1.0)):.2f}%"
        extra_cfg = (
            f"\nGrid Levels: *{escape_md(str(int(conf.get('levels', 4))))}* \\| "
            f"Range: *{escape_md(min_range)} \\- {escape_md(max_range)}*"
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
    elif strategy_id == "dn":
        auto_close = "ON" if float(conf.get("auto_close_on_maintenance", 1) or 0) >= 0.5 else "OFF"
        extra_cfg = f"\nAuto-close on maintenance: *{escape_md(auto_close)}*"
    return (
        f"🤖 *{escape_md(names.get(strategy_id, strategy_id.upper()))} Dashboard*\n"
        f"Strategy Status: {status_dot} *READY*\n\n"
        "*01 SETUP FLOW*\n"
        "1\\. Account/Mode\n"
        "2\\. Pair\n"
        "3\\. Margin \\& Risk\n"
        "4\\. Exit Controls\n"
        "5\\. Review analytics \\& launch\n\n"
        "*02 HOW IT WORKS*\n"
        f"{escape_md(selected_explainer)}\n\n"
        "*03 CURRENT SETTINGS*\n"
        f"Mode: *{escape_md(network.upper())}* \\| "
        f"Risk: *{escape_md(settings.get('risk_profile', 'balanced').upper())}* \\| "
        f"Leverage: *{escape_md(f'{leverage:.0f}x')}* \\| "
        f"Slippage: *{escape_md(f'{slippage:.2f}%')}*\n"
        f"Pair: *{escape_md(product)}\\-PERP* \\| Mid: *{escape_md(mid_str)}*\n\n"
        "*04 STRATEGY CONFIGURATION*\n"
        f"Notional: *{escape_md(f'${notional:,.2f}')}* \\| Spread: *{escape_md(f'{spread_bp:.1f} bp')}*\n"
        f"Interval: *{escape_md(f'{interval_seconds}s')}* \\| TP/SL: *{escape_md(f'{tp_pct:.2f}%/{sl_pct:.2f}%')}*"
        f"{extra_cfg}\n\n"
        "*05 PRE\\-TRADE ANALYTICS*\n"
        f"Available Margin: {margin_flag} *{escape_md(f'${available_margin:,.2f}')}*\n"
        f"Required Margin: *{escape_md(f'${required_margin:,.2f}')}*\n"
        f"Est\\. Daily Volume: *{escape_md(f'${est_daily_volume:,.2f}')}*\n"
        f"Est\\. Fees \\(builder\\+maker\\): *{escape_md(f'${est_fees:,.2f}')}*\n"
        f"Est\\. Spread PnL: *{escape_md(f'${est_spread_pnl:,.2f}')}*\n"
        f"Est\\. Funding \\(DN\\): *{escape_md(f'${est_funding:,.2f}')}* \\| Funding index: *{escape_md(funding_str)}*\n"
        f"Max Loss \\(from SL\\): *{escape_md(f'${max_loss:,.2f}')}*\n"
        f"Net Estimate: *{escape_md(net_str)}*\n\n"
        "Use controls below to tune risk, edit parameters, and launch\\."
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
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=onboarding_language_kb(),
        )
    else:
        await query.edit_message_text(
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


def _live_task_key(query, telegram_id: int):
    chat_id = query.message.chat_id if query and query.message else telegram_id
    return chat_id, telegram_id


async def _stop_live_task(task_key):
    task = LIVE_PRICE_TASKS.pop(task_key, None)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass


def _fmt_live_last_price(product: str, last_price: float) -> str:
    last_str = "$" + fmt_price(last_price, product) if last_price else "N/A"
    ts = time.strftime("%H:%M:%S UTC", time.gmtime())
    return (
        "🔴 *Live Last Price*\n\n"
        f"Asset: *{escape_md(product)}\\-PERP*\n"
        f"Last: *{escape_md(last_str)}*\n"
        f"Updated: {escape_md(ts)}"
    )


async def _live_price_loop(bot, telegram_id: int, chat_id: int, message_id: int, product: str, task_key):
    try:
        while True:
            client = get_user_readonly_client(telegram_id)
            if not client:
                break

            pid = get_product_id(product)
            if pid is None:
                break

            mp = await run_blocking(client.get_market_price, pid)
            text = _fmt_live_last_price(product, mp.get("mid", 0))
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=text,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=live_price_controls_kb(product),
                )
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    break
            except Exception:
                logger.warning("Live price loop error for %s, stopping", product, exc_info=True)
                break
            await asyncio.sleep(2)
    except asyncio.CancelledError:
        pass
    finally:
        LIVE_PRICE_TASKS.pop(task_key, None)
