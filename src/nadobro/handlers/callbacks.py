import logging
import time
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.handlers.formatters import (
    escape_md, fmt_dashboard, fmt_positions, fmt_balance,
    fmt_prices, fmt_funding, fmt_trade_preview, fmt_trade_result,
    fmt_wallet_info, fmt_alerts, fmt_history, fmt_analytics,
    fmt_settings, fmt_help, fmt_price, fmt_onboarding_step, fmt_status_overview,
)
from src.nadobro.handlers.keyboards import (
    main_menu_kb, trade_product_kb, trade_size_kb, trade_leverage_kb,
    trade_confirm_kb, positions_kb, wallet_kb, alerts_kb,
    alert_product_kb, alert_delete_kb, settings_kb, settings_leverage_kb,
    settings_slippage_kb, close_product_kb, confirm_close_all_kb, back_kb,
    risk_profile_kb, strategy_hub_kb, strategy_action_kb,
    onboarding_mode_kb, onboarding_key_kb, onboarding_funding_kb,
    onboarding_risk_kb, onboarding_template_kb, onboarding_nav_kb,
)
from src.nadobro.services.user_service import (
    get_or_create_user, get_user_nado_client, get_user_wallet_info,
    switch_network, get_user, remove_user_private_key, ensure_active_wallet_ready,
    import_user_private_key, get_user_private_key,
)
from src.nadobro.services.trade_service import (
    execute_market_order, execute_limit_order, close_position,
    close_all_positions, get_trade_history, get_trade_analytics,
)
from src.nadobro.services.alert_service import create_alert, get_user_alerts, delete_alert
from src.nadobro.services.admin_service import is_trading_paused
from src.nadobro.services.bot_runtime import start_user_bot, stop_user_bot, get_user_bot_status
from src.nadobro.services.settings_service import get_user_settings, update_user_settings
from src.nadobro.services.onboarding_service import (
    get_resume_step,
    get_onboarding_progress,
    evaluate_readiness,
    set_current_step,
    mark_step_completed,
    skip_step,
    set_selected_template,
)
from src.nadobro.config import get_product_name, get_product_id, PRODUCTS

logger = logging.getLogger(__name__)


async def handle_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()

    data = query.data
    telegram_id = query.from_user.id

    try:
        if data.startswith("nav:"):
            await _handle_nav(query, data, telegram_id, context)
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
        elif data.startswith("keyimp:"):
            await _handle_key_import_confirm(query, data, context, telegram_id)
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
                reply_markup=main_menu_kb(),
            )
        except Exception:
            pass


async def _show_dashboard(query, telegram_id):
    user = get_user(telegram_id)
    if not user:
        await query.edit_message_text(
            "User not found\\. Use /start first\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    network = user.network_mode.value
    balance = None
    positions = None
    prices = None

    try:
        client = get_user_nado_client(telegram_id)
        if client:
            balance = client.get_balance()
            positions = client.get_all_positions()
            prices = client.get_all_market_prices()
    except Exception as e:
        logger.warning(f"Dashboard data fetch error: {e}")

    dashboard = fmt_dashboard(user, balance, positions, prices, network)
    readiness = evaluate_readiness(telegram_id)
    if readiness.get("onboarding_complete"):
        dashboard += "\n\n✅ *Setup:* Complete"
    else:
        next_step = readiness.get("missing_step", "welcome")
        dashboard += (
            f"\n\n⚠️ *Setup:* Incomplete\n"
            f"Next step: *{escape_md(str(next_step).upper())}*"
        )
    await query.edit_message_text(
        dashboard,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=main_menu_kb(),
    )


async def _handle_nav(query, data, telegram_id, context=None):
    target = data.split(":")[1] if ":" in data else "main"

    if target in ("main", "refresh"):
        await _show_dashboard(query, telegram_id)
    elif target == "help":
        await query.edit_message_text(
            fmt_help(),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
    elif target == "quick_start":
        await _handle_onboarding(query, "onboarding:resume", telegram_id, context)
    elif target == "strategy_hub":
        await query.edit_message_text(
            "🧭 *Strategy Hub*\n\n"
            "Pick a strategy, review setup, then start with pre\\-trade analytics\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_hub_kb(),
        )
    elif target == "ask_nado" and context is not None:
        context.user_data["pending_question"] = True
        await query.edit_message_text(
            "🧠 *Ask Nado*\n\n"
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
    resume_step = get_resume_step(telegram_id)
    if resume_step != "complete":
        await query.edit_message_text(
            f"⚠️ Setup incomplete\\. Resume onboarding at *{escape_md(resume_step.upper())}*\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🧭 Continue Setup", callback_data="onboarding:resume")],
                [InlineKeyboardButton("Exit", callback_data="nav:main")],
            ]),
        )
        return
    wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
    if action in ("long", "short", "limit_long", "limit_short") and not wallet_ready:
        await query.edit_message_text(
            f"⚠️ {escape_md(wallet_msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
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
            reply_markup=main_menu_kb(),
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
            reply_markup=main_menu_kb(),
        )
        return
    wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
    if not wallet_ready:
        await query.edit_message_text(
            f"⚠️ {escape_md(wallet_msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
        return

    if action in ("limit_long", "limit_short"):
        price = pending.get("price", 0)
        is_long = action == "limit_long"
        result = execute_limit_order(telegram_id, product, size, price, is_long=is_long, leverage=leverage)
    else:
        is_long = action == "long"
        result = execute_market_order(
            telegram_id,
            product,
            size,
            is_long=is_long,
            leverage=leverage,
            slippage_pct=slippage_pct,
        )

    msg = fmt_trade_result(result)
    await query.edit_message_text(
        msg,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=main_menu_kb(),
    )


async def _handle_positions(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"

    if action == "view":
        client = get_user_nado_client(telegram_id)
        if not client:
            await query.edit_message_text(
                "⚠️ Wallet not initialized\\. Use /start first\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(),
            )
            return

        positions = client.get_all_positions()
        prices = None
        try:
            prices = client.get_all_market_prices()
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
        result = close_position(telegram_id, product)

        if result["success"]:
            msg = f"✅ Closed {escape_md(str(result['cancelled']))} order\\(s\\) on {escape_md(result['product'])}\\."
        else:
            msg = f"❌ Close failed: {escape_md(result['error'])}"

        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )

    elif action == "close_all":
        await query.edit_message_text(
            "⚠️ *Close All Orders*\n\nAre you sure?",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=confirm_close_all_kb(),
        )

    elif action == "confirm_close_all":
        result = close_all_positions(telegram_id)

        if result["success"]:
            products = ", ".join(result.get("products", []))
            msg = f"✅ Closed {escape_md(str(result['cancelled']))} order\\(s\\) on {escape_md(products)}\\."
        else:
            msg = f"❌ Close failed: {escape_md(result['error'])}"

        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )


async def _handle_wallet(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"

    if action == "view":
        info = get_user_wallet_info(telegram_id)
        msg = fmt_wallet_info(info)
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=wallet_kb(),
        )
    elif action == "import" and len(parts) >= 3:
        network = parts[2]
        if network not in ("testnet", "mainnet"):
            return
        context.user_data["pending_key_import"] = {"network": network, "started_at": time.time()}
        set_current_step(telegram_id, "key")
        await query.edit_message_text(
            f"🔑 *Import Dedicated Trading Key* \\({escape_md(network.upper())}\\)\n\n"
            "Send your *private key* now \\(64 hex chars, with or without `0x`\\)\\.\n\n"
            "⚠️ Never send a seed phrase\\. Never send your main wallet key\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=wallet_kb(),
        )
    elif action == "rotate":
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "testnet"
        context.user_data["pending_key_import"] = {"network": network, "started_at": time.time()}
        set_current_step(telegram_id, "key")
        await query.edit_message_text(
            f"♻️ *Rotate Active Key* \\({escape_md(network.upper())}\\)\n\n"
            "Send the *new dedicated private key*\\. Old key will be replaced\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=wallet_kb(),
        )
    elif action == "remove_active":
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "testnet"
        ok, msg = remove_user_private_key(telegram_id, network)
        prefix = "✅" if ok else "❌"
        await query.edit_message_text(
            f"{prefix} {escape_md(msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=wallet_kb(),
        )
    elif action == "view_key":
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "testnet"
        if not query.message or getattr(query.message.chat, "type", "") != "private":
            await query.edit_message_text(
                "⚠️ For safety, private key viewing is allowed only in private chat with the bot\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=wallet_kb(),
            )
            return

        ok, value = get_user_private_key(telegram_id, network)
        if not ok:
            await query.edit_message_text(
                f"❌ {escape_md(value)}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=wallet_kb(),
            )
            return

        secret_msg = await query.message.reply_text(
            f"PRIVATE KEY ({network.upper()}):\n{value}\n\n"
            "This message will self-delete in 30 seconds.",
        )
        asyncio.create_task(
            _delete_message_later(query, secret_msg.chat_id, secret_msg.message_id, delay_seconds=30)
        )
        await query.edit_message_text(
            "✅ Private key sent as temporary message\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=wallet_kb(),
        )

    elif action == "balance":
        client = get_user_nado_client(telegram_id)
        if not client:
            await query.edit_message_text(
                "⚠️ Wallet not initialized\\. Use /start first\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(),
            )
            return

        balance = client.get_balance()
        info = get_user_wallet_info(telegram_id)
        addr = info.get("active_address") if info else None
        msg = fmt_balance(balance, addr)
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=wallet_kb(),
        )

    elif action == "network" and len(parts) >= 3:
        net = parts[2]
        if net not in ("testnet", "mainnet"):
            return

        success, result_msg = switch_network(telegram_id, net)

        if success:
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

    client = get_user_nado_client(telegram_id)
    if not client:
        await query.edit_message_text(
            "⚠️ Wallet not initialized\\. Use /start first\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )
        return

    if action == "prices":
        prices = client.get_all_market_prices()
        msg = fmt_prices(prices)
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )

    elif action == "funding":
        funding = {}
        all_rates = client.get_all_funding_rates()
        for name, info in PRODUCTS.items():
            if info["type"] == "perp":
                fr = all_rates.get(info["id"])
                if fr:
                    funding[name] = fr.get("funding_rate", 0)

        msg = fmt_funding(funding)
        await query.edit_message_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )


async def _handle_alert(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "menu":
        await query.edit_message_text(
            "🔔 *Alerts*\n\nManage your price alerts\\.",
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
            "⚡ *Select Default Leverage*",
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
            "📊 *Select Slippage Tolerance*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_slippage_kb(),
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
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    strategy_id = parts[2] if len(parts) > 2 else ""

    if action == "preview":
        if strategy_id not in ("mm", "grid", "dn"):
            return
        selected_product = context.user_data.get(f"strategy_pair:{strategy_id}", "BTC")
        await query.edit_message_text(
            _build_strategy_preview_text(telegram_id, strategy_id, selected_product),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_action_kb(strategy_id, selected_product),
        )
    elif action == "pair" and len(parts) >= 4:
        strategy_id = parts[2]
        selected_product = parts[3].upper()
        if strategy_id not in ("mm", "grid", "dn"):
            return
        if selected_product not in ("BTC", "ETH", "SOL"):
            return
        context.user_data[f"strategy_pair:{strategy_id}"] = selected_product
        await query.edit_message_text(
            _build_strategy_preview_text(telegram_id, strategy_id, selected_product),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_action_kb(strategy_id, selected_product),
        )
    elif action == "config":
        if strategy_id not in ("mm", "grid", "dn"):
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
        if strategy_id not in ("mm", "grid", "dn"):
            return
        value = float(raw_value)

        def _mutate(s):
            strategies = s.setdefault("strategies", {})
            cfg = strategies.setdefault(strategy_id, {})
            if field == "interval_seconds":
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
    elif action == "input" and len(parts) >= 4:
        strategy_id = parts[2]
        field = parts[3]
        if strategy_id not in ("mm", "grid", "dn"):
            return
        if field not in ("notional_usd", "spread_bp", "interval_seconds", "tp_pct", "sl_pct"):
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
            reply_markup=main_menu_kb(),
        )
    elif action == "start" and len(parts) >= 4:
        strategy_id = parts[2]
        product = parts[3]
        resume_step = get_resume_step(telegram_id)
        if resume_step != "complete":
            await query.edit_message_text(
                f"⚠️ Setup incomplete\\. Resume onboarding at *{escape_md(resume_step.upper())}*\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🧭 Continue Setup", callback_data="onboarding:resume")],
                    [InlineKeyboardButton("Exit", callback_data="nav:main")],
                ]),
            )
            return
        wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
        if not wallet_ready:
            await query.edit_message_text(
                f"⚠️ {escape_md(wallet_msg)}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=main_menu_kb(),
            )
            return
        settings = _get_user_settings(telegram_id, context)
        ok, msg = start_user_bot(
            telegram_id,
            strategy=strategy_id,
            product=product,
            leverage=settings.get("default_leverage", 3),
            slippage_pct=settings.get("slippage", 1),
        )
        if ok:
            reply = f"🚀 {escape_md(msg)}\n\nUse /status to monitor live loop health\\."
        else:
            reply = f"❌ {escape_md(msg)}"
        await query.edit_message_text(
            reply,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
    elif action == "status":
        st = get_user_bot_status(telegram_id)
        readiness = evaluate_readiness(telegram_id)
        text = fmt_status_overview(st, readiness)
        if st.get("last_error"):
            text += f"\nLast error: {escape_md(str(st.get('last_error')))}"
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
    elif action == "stop":
        ok, msg = stop_user_bot(telegram_id, cancel_orders=True)
        prefix = "🛑" if ok else "⚠️"
        await query.edit_message_text(
            f"{prefix} {escape_md(msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )


def _get_user_settings(telegram_id: int, context: CallbackContext) -> dict:
    network, settings = get_user_settings(telegram_id)
    cache_key = f"settings:{network}"
    context.user_data[cache_key] = settings
    context.user_data["settings"] = settings
    return settings


def _fmt_strategy_config_text(strategy: str, conf: dict, network: str) -> str:
    notional = float(conf.get("notional_usd", 100.0))
    spread_bp = float(conf.get("spread_bp", 5.0))
    interval_seconds = int(conf.get("interval_seconds", 60))
    tp_pct = float(conf.get("tp_pct", 1.0))
    sl_pct = float(conf.get("sl_pct", 0.5))
    return (
        f"⚙️ *{escape_md(strategy.upper())} Settings*\n\n"
        f"Mode: *{escape_md(network.upper())}*\n"
        f"Notional: *{escape_md(f'${notional:,.2f}')}*\n"
        f"Spread: *{escape_md(f'{spread_bp:.1f} bp')}*\n"
        f"Interval: *{escape_md(f'{interval_seconds}s')}*\n\n"
        f"Take Profit: *{escape_md(f'{tp_pct:.2f}%')}*\n"
        f"Stop Loss: *{escape_md(f'{sl_pct:.2f}%')}*\n\n"
        "Use presets or custom input below\\."
    )


def _strategy_config_kb(strategy: str):
    return InlineKeyboardMarkup([
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
        [
            InlineKeyboardButton("◀ Back", callback_data=f"strategy:preview:{strategy}"),
        ],
    ])


def _build_strategy_preview_text(telegram_id: int, strategy_id: str, product: str) -> str:
    names = {
        "mm": "Market Maker \\(Mid Mode\\)",
        "grid": "Grid Bot",
        "dn": "Delta Neutral",
    }
    network, settings = get_user_settings(telegram_id)
    conf = settings.get("strategies", {}).get(strategy_id, {})
    notional = float(conf.get("notional_usd", 100.0))
    spread_bp = float(conf.get("spread_bp", 5.0))
    interval_seconds = int(conf.get("interval_seconds", 60))
    tp_pct = float(conf.get("tp_pct", 1.0))
    sl_pct = float(conf.get("sl_pct", 0.5))
    leverage = float(settings.get("default_leverage", 3))
    slippage = float(settings.get("slippage", 1))

    available_margin = 0.0
    mid = 0.0
    funding_rate = 0.0
    client = get_user_nado_client(telegram_id)
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

    required_margin = notional / leverage if leverage > 0 else notional
    cycles_per_day = 86400 / max(interval_seconds, 10)
    est_daily_volume = notional * 2.0 * cycles_per_day

    # Conservative fee estimate using builder fee (2 bps) + maker fee proxy (1 bp).
    est_fee_rate = 0.0003
    est_fees = est_daily_volume * est_fee_rate

    # Approximate spread capture potential before funding and slippage.
    fill_efficiency = 0.45
    est_spread_pnl = est_daily_volume * (spread_bp / 10000.0) * fill_efficiency
    est_funding = 0.0
    if strategy_id == "dn":
        est_funding = abs(funding_rate) * notional * 3
    max_loss = required_margin * (sl_pct / 100.0)
    est_net = est_spread_pnl + est_funding - est_fees

    margin_flag = "✅" if available_margin >= required_margin else "⚠️"
    mid_str = f"${fmt_price(mid, product)}" if mid > 0 else "N/A"
    funding_str = f"{funding_rate:.6f}"
    net_str = f"+${est_net:,.2f}" if est_net >= 0 else f"-${abs(est_net):,.2f}"

    return (
        f"🧭 *{escape_md(names.get(strategy_id, strategy_id.upper()))}*\n\n"
        "*Setup Flow*\n"
        "1\\. Account/Mode\n"
        "2\\. Pair\n"
        "3\\. Margin & Risk\n"
        "4\\. Exit Controls\n"
        "5\\. Review analytics and start\n\n"
        f"Mode: *{escape_md(network.upper())}* \\| "
        f"Risk: *{escape_md(settings.get('risk_profile', 'balanced').upper())}* \\| "
        f"Leverage: *{escape_md(f'{leverage:.0f}x')}* \\| "
        f"Slippage: *{escape_md(f'{slippage:.2f}%')}*\n"
        f"Pair: *{escape_md(product)}\\-PERP* \\| Mid: *{escape_md(mid_str)}*\n\n"
        "*Configuration*\n"
        f"Notional: *{escape_md(f'${notional:,.2f}')}* \\| Spread: *{escape_md(f'{spread_bp:.1f} bp')}*\n"
        f"Interval: *{escape_md(f'{interval_seconds}s')}* \\| TP/SL: *{escape_md(f'{tp_pct:.2f}%/{sl_pct:.2f}%')}*\n\n"
        "*Pre\\-Trade Analytics*\n"
        f"Available Margin: {margin_flag} *{escape_md(f'${available_margin:,.2f}')}*\n"
        f"Required Margin: *{escape_md(f'${required_margin:,.2f}')}*\n"
        f"Est\\. Daily Volume: *{escape_md(f'${est_daily_volume:,.2f}')}*\n"
        f"Est\\. Fees \\(builder\\+maker\\): *{escape_md(f'${est_fees:,.2f}')}*\n"
        f"Est\\. Spread PnL: *{escape_md(f'${est_spread_pnl:,.2f}')}*\n"
        f"Est\\. Funding \\(DN\\): *{escape_md(f'${est_funding:,.2f}')}* \\| Funding index: *{escape_md(funding_str)}*\n"
        f"Max Loss \\(from SL\\): *{escape_md(f'${max_loss:,.2f}')}*\n"
        f"Net Estimate: *{escape_md(net_str)}*"
    )


async def _handle_key_import_confirm(query, data, context, telegram_id):
    action = data.split(":")[1] if ":" in data else ""
    pending = context.user_data.get("pending_key_confirm")
    if not pending:
        await query.edit_message_text(
            "⚠️ No pending key import found\\. Use /import\\_key to start again\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
        return

    started_at = float(pending.get("started_at") or 0)
    if started_at and time.time() - started_at > 300:
        context.user_data.pop("pending_key_confirm", None)
        await query.edit_message_text(
            "⌛ Key import confirmation expired\\. Use /import\\_key again\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
        return

    if action == "cancel":
        context.user_data.pop("pending_key_confirm", None)
        context.user_data.pop("pending_key_import", None)
        await query.edit_message_text(
            "❌ Key import cancelled\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
        return

    if action != "confirm":
        return

    network = pending.get("network", "testnet")
    private_key = pending.get("private_key", "")
    address = pending.get("address", "unknown")
    fingerprint = pending.get("fingerprint", "unknown")
    ok, msg = import_user_private_key(telegram_id, private_key, network)
    context.user_data.pop("pending_key_confirm", None)
    if ok:
        mark_step_completed(telegram_id, "key")
        await query.edit_message_text(
            f"✅ {escape_md(msg)}\n\n"
            f"Address: `{escape_md(address)}`\n"
            f"Fingerprint: `fp\\-{escape_md(fingerprint)}`\n\n"
            "Next: fund this wallet on Nado, then start trading\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
    else:
        await query.edit_message_text(
            f"❌ {escape_md(msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )


async def _handle_onboarding(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    network, state = get_onboarding_progress(telegram_id)["network"], get_onboarding_progress(telegram_id)["state"]
    step = state.get("current_step", "welcome")

    if action == "resume":
        step = get_resume_step(telegram_id)
        if step == "complete":
            await _show_dashboard(query, telegram_id)
            return
        set_current_step(telegram_id, step)
        await _render_onboarding(query, telegram_id, step)
        return

    if action == "back":
        order = ["welcome", "mode", "key", "funding", "risk", "template"]
        idx = max(order.index(step) - 1, 0) if step in order else 0
        step = order[idx]
        set_current_step(telegram_id, step)
        await _render_onboarding(query, telegram_id, step)
        return

    if action == "skip":
        if step in ("risk", "template"):
            skip_step(telegram_id, step)
            await _render_onboarding(query, telegram_id, get_resume_step(telegram_id))
            return
        await query.edit_message_text(
            "This step cannot be skipped\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=onboarding_nav_kb(step, allow_skip=False, allow_back=True),
        )
        return

    if action == "next":
        await _onboarding_next(query, telegram_id)
        return

    if action == "set_mode" and len(parts) >= 3:
        mode = parts[2]
        ok, msg = switch_network(telegram_id, mode)
        if ok:
            mark_step_completed(telegram_id, "mode")
            set_current_step(telegram_id, "key")
            await query.edit_message_text(
                f"✅ {escape_md(msg)}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Continue ▶", callback_data="onboarding:resume")],
                ]),
            )
        else:
            await query.edit_message_text(
                f"❌ {escape_md(msg)}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=onboarding_mode_kb(),
            )
        return

    if action == "check_funding":
        await _render_onboarding(query, telegram_id, "funding")
        return

    if action == "set_risk" and len(parts) >= 3:
        profile = parts[2]
        presets = {
            "conservative": {"default_leverage": 2, "slippage": 0.5},
            "balanced": {"default_leverage": 5, "slippage": 1.0},
            "aggressive": {"default_leverage": 10, "slippage": 2.0},
        }
        chosen = presets.get(profile)
        if chosen:
            update_user_settings(
                telegram_id,
                lambda s: s.update(
                    {
                        "default_leverage": chosen["default_leverage"],
                        "slippage": chosen["slippage"],
                        "risk_profile": profile,
                    }
                ),
            )
            mark_step_completed(telegram_id, "risk")
        await _onboarding_next(query, telegram_id)
        return

    if action == "set_template" and len(parts) >= 3:
        tpl = parts[2]
        if tpl in ("mm", "grid", "dn"):
            set_selected_template(telegram_id, tpl)
        await _onboarding_next(query, telegram_id)
        return

    await _render_onboarding(query, telegram_id, get_resume_step(telegram_id))


async def _onboarding_next(query, telegram_id):
    readiness = evaluate_readiness(telegram_id)
    _, state = get_onboarding_progress(telegram_id)["network"], get_onboarding_progress(telegram_id)["state"]
    step = state.get("current_step", "welcome")

    if step == "welcome":
        mark_step_completed(telegram_id, "welcome")
        set_current_step(telegram_id, "mode")
    elif step == "mode":
        mark_step_completed(telegram_id, "mode")
        set_current_step(telegram_id, "key")
    elif step == "key":
        if not readiness.get("has_key"):
            await query.edit_message_text(
                "⚠️ Import a dedicated key first to continue\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=onboarding_key_kb(readiness.get("network", "testnet")),
            )
            return
        mark_step_completed(telegram_id, "key")
        set_current_step(telegram_id, "funding")
    elif step == "funding":
        if not readiness.get("funded"):
            await query.edit_message_text(
                "⚠️ Wallet is not funded yet\\. Fund first, then continue\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=onboarding_funding_kb(readiness.get("network", "testnet")),
            )
            return
        mark_step_completed(telegram_id, "funding")
        set_current_step(telegram_id, "risk")
    elif step == "risk":
        mark_step_completed(telegram_id, "risk")
        set_current_step(telegram_id, "template")
    elif step == "template":
        mark_step_completed(telegram_id, "template")

    next_step = get_resume_step(telegram_id)
    if next_step == "complete":
        await query.edit_message_text(
            "✅ Onboarding complete\\. You are ready to trade\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Open Dashboard", callback_data="nav:main")],
            ]),
        )
        return
    await _render_onboarding(query, telegram_id, next_step)


async def _render_onboarding(query, telegram_id: int, step: str):
    progress = get_onboarding_progress(telegram_id)
    readiness = evaluate_readiness(telegram_id)
    progress_text = f"{progress.get('done', 0)}/{progress.get('total', 0)}"
    text = fmt_onboarding_step(
        step,
        readiness.get("network", "testnet"),
        readiness,
        {"progress": progress_text, "selected_template": progress.get("state", {}).get("selected_template")},
    )
    network = readiness.get("network", "testnet")
    if step == "mode":
        kb = onboarding_mode_kb()
    elif step == "key":
        kb = onboarding_key_kb(network)
    elif step == "funding":
        kb = onboarding_funding_kb(network)
    elif step == "risk":
        kb = onboarding_risk_kb()
    elif step == "template":
        kb = onboarding_template_kb()
    else:
        kb = onboarding_nav_kb(step, allow_skip=False, allow_back=False)
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=kb,
    )


async def _delete_message_later(query, chat_id: int, message_id: int, delay_seconds: int = 30):
    await asyncio.sleep(delay_seconds)
    try:
        await query.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass
