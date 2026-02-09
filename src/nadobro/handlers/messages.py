import logging
from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.services.ai_parser import parse_user_message
from src.nadobro.services.user_service import get_or_create_user, get_user_nado_client, get_user
from src.nadobro.services.trade_service import (
    execute_market_order, execute_limit_order, close_position,
    close_all_positions, get_trade_history, get_trade_analytics,
)
from src.nadobro.services.alert_service import create_alert, get_user_alerts
from src.nadobro.services.admin_service import is_trading_paused
from src.nadobro.config import get_product_name, PRODUCTS
from src.nadobro.handlers.callbacks import handle_pending_input
from src.nadobro.handlers.keyboards import main_menu_keyboard

logger = logging.getLogger(__name__)


async def handle_message(update: Update, context: CallbackContext):
    if not update.message or not update.message.text:
        return

    if await handle_pending_input(update, context):
        return

    telegram_id = update.effective_user.id
    username = update.effective_user.username
    text = update.message.text.strip()

    user, is_new, mnemonic = get_or_create_user(telegram_id, username)
    if is_new:
        msg = (
            "Welcome! Your wallet has been created.\n"
            f"Address: `{user.wallet_address_testnet}`\n\n"
        )
        if mnemonic:
            msg += (
                "SAVE YOUR RECOVERY PHRASE:\n"
                f"`{mnemonic}`\n\n"
            )
        msg += "Type /help for commands."
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        return

    parsed = parse_user_message(text)
    intent = parsed.get("intent", "chat")
    action = parsed.get("action")
    ai_message = parsed.get("message", "")
    confidence = parsed.get("confidence", 0)

    if intent == "trade":
        if is_trading_paused():
            await update.message.reply_text("Trading is temporarily paused by admin.")
            return

        product = parsed.get("product")
        size = parsed.get("size")
        price = parsed.get("price")
        leverage = parsed.get("leverage", 1.0) or 1.0

        if not product or not size:
            suggestion = ai_message or "I need a product and size. Example: \"Long BTC 0.001\""
            if confidence > 0.3:
                suggestion = f"AI understood: {ai_message}\n\nPlease specify: /long <product> <size>"
            await update.message.reply_text(suggestion)
            return

        try:
            if action in ["long", "limit_long"]:
                if price and action == "limit_long":
                    await update.message.reply_text(f"Placing LIMIT LONG {size} {product} @ ${price:,.2f}...")
                    result = execute_limit_order(telegram_id, product, size, price, is_long=True, leverage=leverage)
                else:
                    await update.message.reply_text(f"Placing LONG {size} {product}...")
                    result = execute_market_order(telegram_id, product, size, is_long=True, leverage=leverage)

                if result["success"]:
                    msg = (
                        f"LONG {result['size']} {result['product']} filled!\n"
                        f"Price: ${result['price']:,.2f}\n"
                        f"Network: {result['network']}"
                    )
                else:
                    msg = f"Order failed: {result['error']}"
                await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

            elif action in ["short", "limit_short"]:
                if price and action == "limit_short":
                    await update.message.reply_text(f"Placing LIMIT SHORT {size} {product} @ ${price:,.2f}...")
                    result = execute_limit_order(telegram_id, product, size, price, is_long=False, leverage=leverage)
                else:
                    await update.message.reply_text(f"Placing SHORT {size} {product}...")
                    result = execute_market_order(telegram_id, product, size, is_long=False, leverage=leverage)

                if result["success"]:
                    msg = (
                        f"SHORT {result['size']} {result['product']} filled!\n"
                        f"Price: ${result['price']:,.2f}\n"
                        f"Network: {result['network']}"
                    )
                else:
                    msg = f"Order failed: {result['error']}"
                await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

            elif action == "close":
                if product:
                    result = close_position(telegram_id, product)
                else:
                    result = close_all_positions(telegram_id)

                if result["success"]:
                    msg = f"Closed {result['cancelled']} orders."
                else:
                    msg = f"Close failed: {result['error']}"
                await update.message.reply_text(msg)

            elif action == "close_all":
                result = close_all_positions(telegram_id)
                if result["success"]:
                    msg = f"Closed {result['cancelled']} orders across {', '.join(result.get('products', []))}."
                else:
                    msg = f"Close failed: {result['error']}"
                await update.message.reply_text(msg)

            else:
                await update.message.reply_text(
                    f"I understood you want to trade, but I'm not sure about the action '{action}'.\n"
                    f"Try: /long {product} {size} or /short {product} {size}"
                )
        except Exception as e:
            logger.error(f"Trade execution error: {e}", exc_info=True)
            await update.message.reply_text(f"Something went wrong executing your trade. Please try again or use a command like /long {product} {size}.")

    elif intent == "query":
        client = get_user_nado_client(telegram_id)
        if not client:
            await update.message.reply_text("Wallet not set up. Use /start first.")
            return

        if action == "positions":
            positions = client.get_all_positions()
            if not positions:
                await update.message.reply_text("No open positions.")
            else:
                lines = ["*Your Positions:*\n"]
                for p in positions:
                    side_emoji = "\U0001F7E2" if p["side"] == "LONG" else "\U0001F534"
                    lines.append(f"{side_emoji} {p['side']} {abs(p['amount']):.4f} {p['product_name']} @ ${p['price']:,.2f}")
                await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

        elif action == "balance":
            balance = client.get_balance()
            if not balance.get("exists"):
                user_data = get_user(telegram_id)
                addr = user_data.wallet_address_testnet if user_data.network_mode.value == "testnet" else user_data.wallet_address_mainnet
                await update.message.reply_text(
                    f"No subaccount. Deposit >= $5 USDT0 to:\n`{addr}`",
                    parse_mode=ParseMode.MARKDOWN,
                )
            else:
                usdt = balance.get("balances", {}).get(0, 0)
                await update.message.reply_text(
                    f"*Balance:* ${usdt:,.2f} USDT0",
                    parse_mode=ParseMode.MARKDOWN,
                )

        elif action == "price":
            product = parsed.get("product")
            if product:
                from src.nadobro.config import get_product_id
                pid = get_product_id(product)
                if pid is not None:
                    mp = client.get_market_price(pid)
                    msg = f"*{product}-PERP:*\nBid: ${mp['bid']:,.2f} | Ask: ${mp['ask']:,.2f} | Mid: ${mp['mid']:,.2f}"
                else:
                    msg = f"Unknown product '{product}'."
            else:
                prices = client.get_all_market_prices()
                lines = ["*Market Prices:*\n"]
                for name, p in prices.items():
                    lines.append(f"{name}-PERP: ${p['mid']:,.2f}")
                msg = "\n".join(lines)
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

        elif action == "funding":
            product = parsed.get("product")
            if product:
                from src.nadobro.config import get_product_id
                pid = get_product_id(product)
                if pid is not None:
                    fr = client.get_funding_rate(pid)
                    if fr:
                        msg = f"*{product}-PERP Funding:*\nCumulative: {fr['funding_rate']:.6f}"
                    else:
                        msg = f"Could not fetch funding for {product}."
                else:
                    msg = f"Unknown product '{product}'."
            else:
                lines = ["*Funding Rates:*\n"]
                for name, info in PRODUCTS.items():
                    if info["type"] == "perp":
                        fr = client.get_funding_rate(info["id"])
                        if fr:
                            lines.append(f"{name}-PERP: {fr['funding_rate']:.6f}")
                msg = "\n".join(lines)
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

        elif action == "history":
            trades = get_trade_history(telegram_id)
            if not trades:
                await update.message.reply_text("No trade history.")
            else:
                lines = ["*Recent Trades:*\n"]
                for t in trades[:10]:
                    status_emoji = {
                        "filled": "\u2705", "failed": "\u274C",
                        "pending": "\u23F3", "cancelled": "\U0001F6AB",
                    }.get(t["status"], "\u2753")
                    price_str = f"${t['price']:,.2f}" if t["price"] else "N/A"
                    lines.append(f"{status_emoji} {t['side'].upper()} {t['size']} {t['product']} @ {price_str}")
                await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

        else:
            if ai_message:
                await update.message.reply_text(ai_message)
            else:
                await update.message.reply_text("Try /positions, /balance, /price, or /help")

    elif intent == "command":
        if action == "alerts":
            product = parsed.get("product")
            condition = parsed.get("alert_condition")
            value = parsed.get("alert_value")

            if product and condition and value:
                result = create_alert(telegram_id, product, condition, value)
                if result["success"]:
                    msg = f"Alert set: {result['product']} {result['condition']} ${result['target']:,.2f}"
                else:
                    msg = f"Failed: {result['error']}"
            else:
                alerts = get_user_alerts(telegram_id)
                if alerts:
                    lines = ["*Your Alerts:*\n"]
                    for a in alerts:
                        lines.append(f"#{a['id']}: {a['product']} {a['condition']} ${a['target']:,.2f}")
                    msg = "\n".join(lines)
                else:
                    msg = "No active alerts. Use: /alert BTC above 100000"
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

        elif ai_message:
            await update.message.reply_text(ai_message)
        else:
            await update.message.reply_text("Try /help for available commands.")

    elif intent == "chat":
        if ai_message and confidence > 0.3:
            await update.message.reply_text(ai_message)
        else:
            await update.message.reply_text(
                "I'm your Nado trading assistant!\n\n"
                "Chat naturally or use the menu below:",
                reply_markup=main_menu_keyboard(),
            )
