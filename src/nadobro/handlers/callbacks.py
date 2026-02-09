import logging
from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.handlers.keyboards import (
    main_menu_keyboard, trade_menu_keyboard, product_keyboard,
    portfolio_menu_keyboard, market_menu_keyboard, alerts_menu_keyboard,
    alert_product_keyboard, account_menu_keyboard, network_keyboard,
    confirm_keyboard, close_product_keyboard, back_to_menu_keyboard,
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

MAIN_MENU_TEXT = (
    "*Nadobro Trading Bot*\n\n"
    "Select an option below to get started."
)

TRADE_MENU_TEXT = (
    "*Trading Menu*\n\n"
    "Select a trading action."
)

PORTFOLIO_MENU_TEXT = (
    "*Portfolio Menu*\n\n"
    "View your positions, balance, and trading history."
)

MARKET_MENU_TEXT = (
    "*Market Menu*\n\n"
    "View live prices and funding rates."
)

ALERTS_MENU_TEXT = (
    "*Alerts Menu*\n\n"
    "Manage your price alerts."
)

ACCOUNT_MENU_TEXT = (
    "*Account Menu*\n\n"
    "Manage your wallet and network settings."
)

HELP_TEXT = (
    "*Nadobro Commands*\n\n"
    "*Trading:*\n"
    "/long <product> <size> \u2014 Market long\n"
    "/short <product> <size> \u2014 Market short\n"
    "/limit\\_long <product> <size> <price> \u2014 Limit buy\n"
    "/limit\\_short <product> <size> <price> \u2014 Limit sell\n"
    "/tp <product> <size> <price> \u2014 Take profit\n"
    "/sl <product> <size> <price> \u2014 Stop loss\n"
    "/close <product> \u2014 Close position\n"
    "/close\\_all \u2014 Close all positions\n\n"
    "*Info:*\n"
    "/positions \u2014 View open positions\n"
    "/balance \u2014 Check margin balance\n"
    "/price <product> \u2014 Live market price\n"
    "/funding <product> \u2014 Funding rate\n"
    "/history \u2014 Trade history\n"
    "/analytics \u2014 PNL analytics\n\n"
    "*Alerts:*\n"
    "/alert <product> above/below <price>\n"
    "/my\\_alerts \u2014 View active alerts\n"
    "/del\\_alert <id> \u2014 Delete alert\n\n"
    "*Account:*\n"
    "/wallet \u2014 Wallet info\n"
    "/mode testnet/mainnet \u2014 Switch network\n\n"
    "*Products:* BTC, ETH, SOL, ARB, OP, DOGE, LINK, AVAX\n\n"
    "Or just chat naturally! I understand things like:\n"
    "\"Long ETH 0.05\" or \"What's BTC funding?\""
)

ACTION_LABELS = {
    "long": "LONG",
    "short": "SHORT",
    "limit_long": "LIMIT LONG",
    "limit_short": "LIMIT SHORT",
    "tp": "TAKE PROFIT",
    "sl": "STOP LOSS",
}


async def handle_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()

    data = query.data
    parts = data.split(":")
    prefix = parts[0]

    if prefix in ("nav", "menu", "cancel"):
        context.user_data.pop("pending_trade", None)
        context.user_data.pop("pending_alert", None)

    try:
        if prefix == "nav":
            await _handle_nav(query, parts)
        elif prefix == "menu":
            await _handle_menu(query, parts)
        elif prefix == "trade":
            await _handle_trade(query, parts)
        elif prefix == "product":
            await _handle_product(query, parts, context)
        elif prefix == "close":
            await _handle_close(query, parts, update)
        elif prefix == "confirm":
            await _handle_confirm(query, parts, update)
        elif prefix == "cancel":
            await _handle_cancel(query, parts)
        elif prefix == "portfolio":
            await _handle_portfolio(query, parts, update)
        elif prefix == "market":
            await _handle_market(query, parts, update)
        elif prefix == "alerts":
            await _handle_alerts(query, parts, update)
        elif prefix == "alert_product":
            await _handle_alert_product(query, parts, context)
        elif prefix == "account":
            await _handle_account(query, parts, update)
        elif prefix == "network":
            await _handle_network(query, parts, update)
        else:
            await query.edit_message_text("Unknown action.", reply_markup=back_to_menu_keyboard())
    except Exception as e:
        logger.error(f"Callback error for '{data}': {e}", exc_info=True)
        try:
            await query.edit_message_text(
                f"An error occurred. Please try again.",
                reply_markup=back_to_menu_keyboard(),
            )
        except Exception:
            pass


async def _handle_nav(query, parts):
    target = parts[1] if len(parts) > 1 else "main"
    nav_map = {
        "main": (MAIN_MENU_TEXT, main_menu_keyboard()),
        "trade": (TRADE_MENU_TEXT, trade_menu_keyboard()),
        "portfolio": (PORTFOLIO_MENU_TEXT, portfolio_menu_keyboard()),
        "market": (MARKET_MENU_TEXT, market_menu_keyboard()),
        "alerts": (ALERTS_MENU_TEXT, alerts_menu_keyboard()),
        "account": (ACCOUNT_MENU_TEXT, account_menu_keyboard()),
    }
    text, keyboard = nav_map.get(target, (MAIN_MENU_TEXT, main_menu_keyboard()))
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)


async def _handle_menu(query, parts):
    action = parts[1] if len(parts) > 1 else "main"
    menu_map = {
        "trade": (TRADE_MENU_TEXT, trade_menu_keyboard()),
        "portfolio": (PORTFOLIO_MENU_TEXT, portfolio_menu_keyboard()),
        "market": (MARKET_MENU_TEXT, market_menu_keyboard()),
        "alerts": (ALERTS_MENU_TEXT, alerts_menu_keyboard()),
        "account": (ACCOUNT_MENU_TEXT, account_menu_keyboard()),
        "help": (HELP_TEXT, back_to_menu_keyboard()),
    }
    text, keyboard = menu_map.get(action, (MAIN_MENU_TEXT, main_menu_keyboard()))
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)


async def _handle_trade(query, parts):
    action = parts[1] if len(parts) > 1 else ""

    if action in ("long", "short", "limit_long", "limit_short", "tp", "sl"):
        label = ACTION_LABELS.get(action, action.upper())
        await query.edit_message_text(
            f"*{label}*\n\nSelect a product.",
            reply_markup=product_keyboard(action),
            parse_mode=ParseMode.MARKDOWN,
        )
    elif action == "close":
        await query.edit_message_text(
            "*Close Position*\n\nSelect the product to close.",
            reply_markup=close_product_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
    elif action == "close_all":
        await query.edit_message_text(
            "*Close All Positions*\n\n"
            "Are you sure you want to close all open positions?",
            reply_markup=confirm_keyboard("close_all"),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await query.edit_message_text(TRADE_MENU_TEXT, reply_markup=trade_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)


async def _handle_product(query, parts, context):
    if len(parts) < 3:
        await query.edit_message_text("Invalid selection.", reply_markup=back_to_menu_keyboard())
        return

    action = parts[1]
    product = parts[2]

    context.user_data["pending_trade"] = {"action": action, "product": product}

    label = ACTION_LABELS.get(action, action.upper())

    if action in ("long", "short"):
        msg = (
            f"Enter size for {label} {product}:\n\n"
            f"Example: 0.001\n\n"
            f"Optionally add leverage: 0.001 10x"
        )
    else:
        msg = (
            f"Enter size and price for {label} {product}:\n\n"
            f"Example: 0.001 95000"
        )

    await query.edit_message_text(msg, reply_markup=back_to_menu_keyboard())


async def _handle_close(query, parts, update):
    if len(parts) < 3 or parts[1] != "product":
        await query.edit_message_text("Invalid selection.", reply_markup=back_to_menu_keyboard())
        return

    product = parts[2]
    telegram_id = update.effective_user.id

    await query.edit_message_text(f"Closing {product} positions...")
    result = close_position(telegram_id, product)

    if result["success"]:
        msg = f"Closed {result['cancelled']} orders on {result['product']}."
    else:
        msg = f"Close failed: {result['error']}"

    await query.edit_message_text(msg, reply_markup=back_to_menu_keyboard())


async def _handle_confirm(query, parts, update):
    action = parts[1] if len(parts) > 1 else ""
    telegram_id = update.effective_user.id

    if action == "close_all":
        await query.edit_message_text("Closing all positions...")
        result = close_all_positions(telegram_id)

        if result["success"]:
            msg = f"Closed {result['cancelled']} orders across {', '.join(result['products'])}."
        else:
            msg = f"Close failed: {result['error']}"

        await query.edit_message_text(msg, reply_markup=back_to_menu_keyboard())
    else:
        await query.edit_message_text("Unknown confirmation.", reply_markup=back_to_menu_keyboard())


async def _handle_cancel(query, parts):
    action = parts[1] if len(parts) > 1 else ""

    if action == "close_all":
        await query.edit_message_text(TRADE_MENU_TEXT, reply_markup=trade_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)
    else:
        await query.edit_message_text(MAIN_MENU_TEXT, reply_markup=main_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)


async def _handle_portfolio(query, parts, update):
    action = parts[1] if len(parts) > 1 else ""
    telegram_id = update.effective_user.id

    if action == "positions":
        client = get_user_nado_client(telegram_id)
        if not client:
            await query.edit_message_text("Wallet not initialized. Use /start first.", reply_markup=back_to_menu_keyboard())
            return

        positions = client.get_all_positions()
        if not positions:
            await query.edit_message_text("No open positions.", reply_markup=back_to_menu_keyboard())
            return

        lines = ["*Your Open Positions:*\n"]
        for p in positions:
            side_label = "LONG" if p["side"] == "LONG" else "SHORT"
            lines.append(
                f"{side_label} {abs(p['amount']):.4f} {p['product_name']} "
                f"@ ${p['price']:,.2f}"
            )
        await query.edit_message_text("\n".join(lines), reply_markup=back_to_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)

    elif action == "balance":
        client = get_user_nado_client(telegram_id)
        if not client:
            await query.edit_message_text("Wallet not initialized. Use /start first.", reply_markup=back_to_menu_keyboard())
            return

        balance = client.get_balance()
        if not balance.get("exists"):
            user = get_user(telegram_id)
            addr = user.wallet_address_testnet if user.network_mode.value == "testnet" else user.wallet_address_mainnet
            await query.edit_message_text(
                f"No subaccount found.\n\n"
                f"Deposit >= $5 USDT0 to your wallet:\n`{addr}`\n\n"
                f"Testnet faucet: https://testnet.nado.xyz/portfolio/faucet",
                reply_markup=back_to_menu_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
            )
            return

        usdt = balance.get("balances", {}).get(0, 0)
        msg = f"*Unified Margin Balance:*\n${usdt:,.2f} USDT0"

        for pid, bal in balance.get("balances", {}).items():
            if pid != 0 and bal != 0:
                msg += f"\n{get_product_name(pid)}: {bal:.6f}"

        await query.edit_message_text(msg, reply_markup=back_to_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)

    elif action == "history":
        trades = get_trade_history(telegram_id)
        if not trades:
            await query.edit_message_text("No trade history yet.", reply_markup=back_to_menu_keyboard())
            return

        lines = ["*Recent Trades:*\n"]
        for t in trades[:10]:
            status_map = {"filled": "OK", "failed": "FAIL", "pending": "PEND", "cancelled": "CNCL"}
            status_label = status_map.get(t["status"], t["status"])
            price_str = f"${t['price']:,.2f}" if t["price"] else "N/A"
            lines.append(
                f"[{status_label}] {t['side'].upper()} {t['size']} {t['product']} "
                f"@ {price_str} ({t['network']})"
            )
        await query.edit_message_text("\n".join(lines), reply_markup=back_to_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)

    elif action == "analytics":
        stats = get_trade_analytics(telegram_id)
        if stats["total_trades"] == 0:
            await query.edit_message_text("No trades yet to analyze.", reply_markup=back_to_menu_keyboard())
            return

        msg = (
            f"*Trading Analytics:*\n\n"
            f"Total Trades: {stats['total_trades']}\n"
            f"Filled: {stats['filled']} | Failed: {stats['failed']}\n"
            f"Win Rate: {stats['win_rate']:.1f}%\n"
            f"Wins: {stats['wins']} | Losses: {stats['losses']}\n"
            f"Total PNL: ${stats['total_pnl']:,.2f}\n"
            f"Total Volume: ${stats['total_volume']:,.2f}"
        )
        await query.edit_message_text(msg, reply_markup=back_to_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)

    else:
        await query.edit_message_text(PORTFOLIO_MENU_TEXT, reply_markup=portfolio_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)


async def _handle_market(query, parts, update):
    action = parts[1] if len(parts) > 1 else ""
    telegram_id = update.effective_user.id

    if action == "all_prices":
        client = get_user_nado_client(telegram_id)
        if not client:
            await query.edit_message_text("Wallet not initialized. Use /start first.", reply_markup=back_to_menu_keyboard())
            return

        prices = client.get_all_market_prices()
        if not prices:
            await query.edit_message_text("Could not fetch prices.", reply_markup=back_to_menu_keyboard())
            return

        lines = ["*Market Prices:*\n"]
        for name, p in prices.items():
            lines.append(f"{name}-PERP: ${p['mid']:,.2f}")
        await query.edit_message_text("\n".join(lines), reply_markup=back_to_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)

    elif action == "funding":
        client = get_user_nado_client(telegram_id)
        if not client:
            await query.edit_message_text("Wallet not initialized. Use /start first.", reply_markup=back_to_menu_keyboard())
            return

        lines = ["*Funding Rates:*\n"]
        for name, info in PRODUCTS.items():
            if info["type"] == "perp":
                fr = client.get_funding_rate(info["id"])
                if fr:
                    lines.append(f"{name}-PERP: {fr['funding_rate']:.6f}")

        if len(lines) <= 1:
            await query.edit_message_text("Could not fetch funding rates.", reply_markup=back_to_menu_keyboard())
        else:
            await query.edit_message_text("\n".join(lines), reply_markup=back_to_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)

    elif action == "price" and len(parts) >= 3:
        product = parts[2]
        client = get_user_nado_client(telegram_id)
        if not client:
            await query.edit_message_text("Wallet not initialized. Use /start first.", reply_markup=back_to_menu_keyboard())
            return

        pid = get_product_id(product)
        if pid is None:
            await query.edit_message_text(f"Unknown product '{product}'.", reply_markup=back_to_menu_keyboard())
            return

        mp = client.get_market_price(pid)
        msg = (
            f"*{product}-PERP:*\n"
            f"Bid: ${mp['bid']:,.2f}\n"
            f"Ask: ${mp['ask']:,.2f}\n"
            f"Mid: ${mp['mid']:,.2f}"
        )
        await query.edit_message_text(msg, reply_markup=back_to_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)

    else:
        await query.edit_message_text(MARKET_MENU_TEXT, reply_markup=market_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)


async def _handle_alerts(query, parts, update):
    action = parts[1] if len(parts) > 1 else ""
    telegram_id = update.effective_user.id

    if action == "set":
        await query.edit_message_text(
            "*Set Alert*\n\nSelect a product.",
            reply_markup=alert_product_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )

    elif action == "view":
        alerts = get_user_alerts(telegram_id)
        if not alerts:
            await query.edit_message_text("No active alerts. Use the Set Alert button to create one.", reply_markup=back_to_menu_keyboard())
            return

        lines = ["*Active Alerts:*\n"]
        for a in alerts:
            lines.append(f"#{a['id']}: {a['product']} {a['condition']} ${a['target']:,.2f} ({a['network']})")

        await query.edit_message_text("\n".join(lines), reply_markup=back_to_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)

    else:
        await query.edit_message_text(ALERTS_MENU_TEXT, reply_markup=alerts_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)


async def _handle_alert_product(query, parts, context):
    if len(parts) < 2:
        await query.edit_message_text("Invalid selection.", reply_markup=back_to_menu_keyboard())
        return

    product = parts[1]
    context.user_data["pending_alert"] = {"product": product}

    msg = (
        f"Set alert for {product}.\n\n"
        f"Enter condition and price:\n\n"
        f"Example: above 100000\n"
        f"Example: below 90000"
    )
    await query.edit_message_text(msg, reply_markup=back_to_menu_keyboard())


async def _handle_account(query, parts, update):
    action = parts[1] if len(parts) > 1 else ""
    telegram_id = update.effective_user.id

    if action == "wallet":
        info = get_user_wallet_info(telegram_id)
        if not info:
            await query.edit_message_text("User not found. Use /start first.", reply_markup=back_to_menu_keyboard())
            return

        msg = (
            f"*Wallet Info:*\n\n"
            f"Network: *{info['network']}*\n"
            f"Active: `{info['active_address']}`\n"
        )
        if info["testnet_address"]:
            msg += f"Testnet: `{info['testnet_address']}`\n"
        if info["mainnet_address"]:
            msg += f"Mainnet: `{info['mainnet_address']}`\n"

        await query.edit_message_text(msg, reply_markup=back_to_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)

    elif action == "mode":
        await query.edit_message_text(
            "*Switch Network*\n\nSelect a network.",
            reply_markup=network_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )

    else:
        await query.edit_message_text(ACCOUNT_MENU_TEXT, reply_markup=account_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)


async def _handle_network(query, parts, update):
    if len(parts) < 2:
        await query.edit_message_text("Invalid selection.", reply_markup=back_to_menu_keyboard())
        return

    network = parts[1]
    telegram_id = update.effective_user.id

    if network not in ("testnet", "mainnet"):
        await query.edit_message_text("Invalid network.", reply_markup=back_to_menu_keyboard())
        return

    success, msg = switch_network(telegram_id, network)
    await query.edit_message_text(msg, reply_markup=back_to_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)


async def handle_pending_input(update: Update, context: CallbackContext) -> bool:
    pending_trade = context.user_data.get("pending_trade")
    if pending_trade:
        text = update.message.text.strip()
        action = pending_trade["action"]
        product = pending_trade["product"]
        label = ACTION_LABELS.get(action, action.upper())

        try:
            if action in ("long", "short"):
                parts = text.split()
                size = float(parts[0])
                leverage = 1.0
                if len(parts) >= 2:
                    leverage = float(parts[1].replace("x", ""))

                if is_trading_paused():
                    await update.message.reply_text("Trading is temporarily paused by admin.", reply_markup=back_to_menu_keyboard())
                    context.user_data.pop("pending_trade", None)
                    return True

                is_long = action == "long"
                await update.message.reply_text(f"Placing {label} {size} {product}...")
                result = execute_market_order(update.effective_user.id, product, size, is_long=is_long, leverage=leverage)

                if result["success"]:
                    msg = (
                        f"{result['side']} {result['size']} {result['product']} filled!\n"
                        f"Price: ${result['price']:,.2f}\n"
                        f"Network: {result['network']}"
                    )
                else:
                    msg = f"Order failed: {result['error']}"

                await update.message.reply_text(msg, reply_markup=back_to_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)

            else:
                parts = text.split()
                if len(parts) < 2:
                    await update.message.reply_text(
                        f"Please enter both size and price.\n\nExample: 0.001 95000",
                        reply_markup=back_to_menu_keyboard(),
                    )
                    return True

                size = float(parts[0])
                price = float(parts[1])

                if is_trading_paused():
                    await update.message.reply_text("Trading is temporarily paused by admin.", reply_markup=back_to_menu_keyboard())
                    context.user_data.pop("pending_trade", None)
                    return True

                is_long = action in ("limit_long", "tp")
                if action == "tp":
                    is_long = False
                elif action == "sl":
                    is_long = False

                await update.message.reply_text(f"Placing {label} {size} {product} @ ${price:,.2f}...")
                result = execute_limit_order(update.effective_user.id, product, size, price, is_long=is_long)

                if result["success"]:
                    msg = (
                        f"{label} {result['size']} {result['product']} placed!\n"
                        f"Price: ${result['price']:,.2f}\n"
                        f"Network: {result['network']}"
                    )
                else:
                    msg = f"Order failed: {result['error']}"

                await update.message.reply_text(msg, reply_markup=back_to_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)

        except (ValueError, IndexError):
            await update.message.reply_text(
                "Invalid input. Please check the format and try again.",
                reply_markup=back_to_menu_keyboard(),
            )

        context.user_data.pop("pending_trade", None)
        return True

    pending_alert = context.user_data.get("pending_alert")
    if pending_alert:
        text = update.message.text.strip()
        product = pending_alert["product"]

        try:
            parts = text.split()
            if len(parts) < 2:
                await update.message.reply_text(
                    "Please enter condition and price.\n\nExample: above 100000",
                    reply_markup=back_to_menu_keyboard(),
                )
                return True

            condition = parts[0].lower()
            target = float(parts[1])

            if condition not in ("above", "below"):
                await update.message.reply_text(
                    "Condition must be 'above' or 'below'.\n\nExample: above 100000",
                    reply_markup=back_to_menu_keyboard(),
                )
                return True

            result = create_alert(update.effective_user.id, product, condition, target)
            if result["success"]:
                msg = (
                    f"Alert set!\n"
                    f"{result['product']} {result['condition']} ${result['target']:,.2f}\n"
                    f"ID: #{result['alert_id']}"
                )
            else:
                msg = f"Failed: {result['error']}"

            await update.message.reply_text(msg, reply_markup=back_to_menu_keyboard())

        except (ValueError, IndexError):
            await update.message.reply_text(
                "Invalid input. Please enter: above/below price\n\nExample: above 100000",
                reply_markup=back_to_menu_keyboard(),
            )

        context.user_data.pop("pending_alert", None)
        return True

    return False
