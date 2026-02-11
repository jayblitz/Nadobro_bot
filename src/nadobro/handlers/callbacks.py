import logging
from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.handlers.formatters import (
    escape_md, fmt_dashboard, fmt_positions, fmt_balance,
    fmt_prices, fmt_funding, fmt_trade_preview, fmt_trade_result,
    fmt_wallet_info, fmt_alerts, fmt_history, fmt_analytics,
    fmt_settings, fmt_help, fmt_price,
)
from src.nadobro.handlers.keyboards import (
    main_menu_kb, trade_product_kb, trade_size_kb, trade_leverage_kb,
    trade_confirm_kb, positions_kb, wallet_kb, alerts_kb,
    alert_product_kb, alert_delete_kb, settings_kb, settings_leverage_kb,
    settings_slippage_kb, close_product_kb, confirm_close_all_kb, back_kb,
)
from src.nadobro.services.user_service import (
    get_or_create_user, get_user_nado_client, get_user_wallet_info,
    switch_network, get_user,
)
from src.nadobro.services.trade_service import (
    execute_market_order, execute_limit_order, close_position,
    close_all_positions, get_trade_history, get_trade_analytics,
)
from src.nadobro.services.alert_service import create_alert, get_user_alerts, delete_alert
from src.nadobro.services.admin_service import is_trading_paused
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
            await _handle_wallet(query, data, telegram_id)
        elif data.startswith("mkt:"):
            await _handle_market(query, data, telegram_id)
        elif data.startswith("alert:"):
            await _handle_alert(query, data, telegram_id, context)
        elif data.startswith("settings:"):
            await _handle_settings(query, data, telegram_id, context)
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
    elif target == "ask_nado" and context is not None:
        context.user_data["pending_question"] = True
        await query.edit_message_text(
            "🧠 *Ask Nado*\n\n"
            "Ask me anything about Nado DEX\\!\n\n"
            "Examples:\n"
            "  • `What is unified margin?`\n"
            "  • `How do liquidations work?`\n"
            "  • `What order types are available?`\n"
            "  • `How do I deposit funds?`\n"
            "  • `What is the NLP vault?`\n"
            "  • `What are the trading fees?`\n\n"
            "Type your question below:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )


async def _handle_trade(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""

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

    context.user_data.pop("pending_trade", None)

    if is_trading_paused():
        await query.edit_message_text(
            "⏸ Trading is temporarily paused by admin\\.",
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
        result = execute_market_order(telegram_id, product, size, is_long=is_long, leverage=leverage)

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


async def _handle_wallet(query, data, telegram_id):
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
            if "SAVE YOUR RECOVERY PHRASE" in result_msg or "Recovery" in result_msg:
                await query.edit_message_text(
                    escape_md(result_msg),
                    parse_mode=ParseMode.MARKDOWN_V2,
                )
                await query.message.reply_text(
                    msg,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=wallet_kb(),
                )
            else:
                await query.edit_message_text(
                    msg,
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

    user_settings = context.user_data.get("settings", {"default_leverage": 1, "slippage": 1})

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

    elif action == "leverage" and len(parts) >= 3:
        lev = int(parts[2])
        if "settings" not in context.user_data:
            context.user_data["settings"] = {"default_leverage": 1, "slippage": 1}
        context.user_data["settings"]["default_leverage"] = lev

        user_settings = context.user_data["settings"]
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
        if "settings" not in context.user_data:
            context.user_data["settings"] = {"default_leverage": 1, "slippage": 1}
        context.user_data["settings"]["slippage"] = slip

        user_settings = context.user_data["settings"]
        msg = fmt_settings(user_settings)
        lev = user_settings.get("default_leverage", 1)
        await query.edit_message_text(
            f"✅ Slippage set to {escape_md(f'{slip}%')}\n\n{msg}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(lev, slip),
        )
