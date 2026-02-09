import logging
from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.services.user_service import (
    get_or_create_user, get_user_nado_client, get_user_wallet_info,
    switch_network, recover_user_wallet, get_user,
)
from src.nadobro.services.trade_service import (
    execute_market_order, execute_limit_order, close_position,
    close_all_positions, get_trade_history, get_trade_analytics,
)
from src.nadobro.services.alert_service import create_alert, get_user_alerts, delete_alert
from src.nadobro.services.admin_service import (
    is_admin, get_bot_stats, is_trading_paused, set_trading_paused,
    get_recent_admin_logs, get_recent_trades_all, log_admin_action,
)
from src.nadobro.config import get_product_name, PRODUCTS

logger = logging.getLogger(__name__)


async def cmd_start(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    username = update.effective_user.username

    user, is_new, mnemonic = get_or_create_user(telegram_id, username)

    if is_new:
        msg = (
            "Welcome to *Nadobro* \u2014 your AI trading companion for Nado DEX!\n\n"
            "Your testnet wallet has been created:\n"
            f"`{user.wallet_address_testnet}`\n\n"
        )
        if mnemonic:
            msg += (
                "SAVE YOUR RECOVERY PHRASE (shown only once):\n"
                f"`{mnemonic}`\n\n"
                "Store this safely \u2014 it's the ONLY way to recover your wallet.\n\n"
            )
        msg += (
            "Get started:\n"
            "1. Get testnet ETH: https://docs.inkonchain.com/tools/faucets\n"
            "2. Get USDT0: https://testnet.nado.xyz/portfolio/faucet\n"
            "3. Deposit >= $5 USDT0 on Nado testnet\n\n"
            "Then try:\n"
            "/balance \u2014 check your margin\n"
            "/price BTC \u2014 live prices\n"
            "/long BTC 0.001 \u2014 market long\n"
            "/help \u2014 all commands\n\n"
            "Or just chat naturally: \"What's the price of ETH?\""
        )
    else:
        mode = user.network_mode.value
        addr = user.wallet_address_mainnet if mode == "mainnet" else user.wallet_address_testnet
        msg = (
            f"Welcome back! You're on *{mode}* mode.\n"
            f"Wallet: `{addr}`\n\n"
            "Type /help for commands or just chat naturally."
        )

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_help(update: Update, context: CallbackContext):
    msg = (
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
        "/mode testnet/mainnet \u2014 Switch network\n"
        "/recover <mnemonic> \u2014 Recover wallet\n\n"
        "*Products:* BTC, ETH, SOL, ARB, OP, DOGE, LINK, AVAX\n\n"
        "Or just chat naturally! I understand things like:\n"
        "\"Long ETH 0.05\" or \"What's BTC funding?\""
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_long(update: Update, context: CallbackContext):
    if is_trading_paused():
        await update.message.reply_text("Trading is temporarily paused by admin.")
        return

    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /long <product> <size>\nExample: /long BTC 0.001")
        return

    product = args[0].upper()
    try:
        size = float(args[1])
    except ValueError:
        await update.message.reply_text("Invalid size. Example: /long BTC 0.001")
        return

    leverage = 1.0
    if len(args) >= 3:
        try:
            leverage = float(args[2].replace("x", ""))
        except ValueError:
            pass

    await update.message.reply_text(f"Placing market LONG {size} {product}...")
    result = execute_market_order(update.effective_user.id, product, size, is_long=True, leverage=leverage)

    if result["success"]:
        msg = (
            f"LONG {result['size']} {result['product']} filled!\n"
            f"Price: ${result['price']:,.2f}\n"
            f"Digest: `{result.get('digest', 'N/A')}`\n"
            f"Network: {result['network']}"
        )
    else:
        msg = f"Order failed: {result['error']}"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_short(update: Update, context: CallbackContext):
    if is_trading_paused():
        await update.message.reply_text("Trading is temporarily paused by admin.")
        return

    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /short <product> <size>\nExample: /short ETH 0.05")
        return

    product = args[0].upper()
    try:
        size = float(args[1])
    except ValueError:
        await update.message.reply_text("Invalid size. Example: /short ETH 0.05")
        return

    leverage = 1.0
    if len(args) >= 3:
        try:
            leverage = float(args[2].replace("x", ""))
        except ValueError:
            pass

    await update.message.reply_text(f"Placing market SHORT {size} {product}...")
    result = execute_market_order(update.effective_user.id, product, size, is_long=False, leverage=leverage)

    if result["success"]:
        msg = (
            f"SHORT {result['size']} {result['product']} filled!\n"
            f"Price: ${result['price']:,.2f}\n"
            f"Digest: `{result.get('digest', 'N/A')}`\n"
            f"Network: {result['network']}"
        )
    else:
        msg = f"Order failed: {result['error']}"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_limit_long(update: Update, context: CallbackContext):
    if is_trading_paused():
        await update.message.reply_text("Trading is temporarily paused by admin.")
        return

    args = context.args
    if len(args) < 3:
        await update.message.reply_text("Usage: /limit_long <product> <size> <price>\nExample: /limit_long BTC 0.001 90000")
        return

    product = args[0].upper()
    try:
        size = float(args[1])
        price = float(args[2])
    except ValueError:
        await update.message.reply_text("Invalid size or price.")
        return

    await update.message.reply_text(f"Placing LIMIT LONG {size} {product} @ ${price:,.2f}...")
    result = execute_limit_order(update.effective_user.id, product, size, price, is_long=True)

    if result["success"]:
        msg = (
            f"LIMIT LONG {result['size']} {result['product']} placed!\n"
            f"Price: ${result['price']:,.2f}\n"
            f"Digest: `{result.get('digest', 'N/A')}`\n"
            f"Network: {result['network']}"
        )
    else:
        msg = f"Order failed: {result['error']}"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_limit_short(update: Update, context: CallbackContext):
    if is_trading_paused():
        await update.message.reply_text("Trading is temporarily paused by admin.")
        return

    args = context.args
    if len(args) < 3:
        await update.message.reply_text("Usage: /limit_short <product> <size> <price>\nExample: /limit_short ETH 0.05 4000")
        return

    product = args[0].upper()
    try:
        size = float(args[1])
        price = float(args[2])
    except ValueError:
        await update.message.reply_text("Invalid size or price.")
        return

    await update.message.reply_text(f"Placing LIMIT SHORT {size} {product} @ ${price:,.2f}...")
    result = execute_limit_order(update.effective_user.id, product, size, price, is_long=False)

    if result["success"]:
        msg = (
            f"LIMIT SHORT {result['size']} {result['product']} placed!\n"
            f"Price: ${result['price']:,.2f}\n"
            f"Digest: `{result.get('digest', 'N/A')}`\n"
            f"Network: {result['network']}"
        )
    else:
        msg = f"Order failed: {result['error']}"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_tp(update: Update, context: CallbackContext):
    if is_trading_paused():
        await update.message.reply_text("Trading is temporarily paused by admin.")
        return

    args = context.args
    if len(args) < 3:
        await update.message.reply_text(
            "Usage: /tp <product> <size> <trigger_price>\n"
            "Example: /tp BTC 0.001 105000\n\n"
            "Places a sell limit order at the trigger price to take profit on a long position."
        )
        return

    product = args[0].upper()
    try:
        size = float(args[1])
        trigger_price = float(args[2])
    except ValueError:
        await update.message.reply_text("Invalid size or price.")
        return

    await update.message.reply_text(f"Placing Take-Profit: SELL {size} {product} @ ${trigger_price:,.2f}...")
    result = execute_limit_order(update.effective_user.id, product, size, trigger_price, is_long=False)

    if result["success"]:
        msg = (
            f"Take-Profit set!\n"
            f"SELL {result['size']} {result['product']} @ ${result['price']:,.2f}\n"
            f"Digest: `{result.get('digest', 'N/A')}`\n"
            f"Network: {result['network']}"
        )
    else:
        msg = f"TP order failed: {result['error']}"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_sl(update: Update, context: CallbackContext):
    if is_trading_paused():
        await update.message.reply_text("Trading is temporarily paused by admin.")
        return

    args = context.args
    if len(args) < 3:
        await update.message.reply_text(
            "Usage: /sl <product> <size> <trigger_price>\n"
            "Example: /sl BTC 0.001 90000\n\n"
            "Places a sell limit order at the trigger price to stop loss on a long position."
        )
        return

    product = args[0].upper()
    try:
        size = float(args[1])
        trigger_price = float(args[2])
    except ValueError:
        await update.message.reply_text("Invalid size or price.")
        return

    await update.message.reply_text(f"Placing Stop-Loss: SELL {size} {product} @ ${trigger_price:,.2f}...")
    result = execute_limit_order(update.effective_user.id, product, size, trigger_price, is_long=False)

    if result["success"]:
        msg = (
            f"Stop-Loss set!\n"
            f"SELL {result['size']} {result['product']} @ ${result['price']:,.2f}\n"
            f"Digest: `{result.get('digest', 'N/A')}`\n"
            f"Network: {result['network']}"
        )
    else:
        msg = f"SL order failed: {result['error']}"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_close(update: Update, context: CallbackContext):
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /close <product>\nExample: /close BTC")
        return

    product = args[0].upper()
    await update.message.reply_text(f"Closing {product} positions...")
    result = close_position(update.effective_user.id, product)

    if result["success"]:
        msg = f"Closed {result['cancelled']} orders on {result['product']}."
    else:
        msg = f"Close failed: {result['error']}"

    await update.message.reply_text(msg)


async def cmd_close_all(update: Update, context: CallbackContext):
    await update.message.reply_text("Closing all positions...")
    result = close_all_positions(update.effective_user.id)

    if result["success"]:
        msg = f"Closed {result['cancelled']} orders across {', '.join(result['products'])}."
    else:
        msg = f"Close failed: {result['error']}"

    await update.message.reply_text(msg)


async def cmd_positions(update: Update, context: CallbackContext):
    client = get_user_nado_client(update.effective_user.id)
    if not client:
        await update.message.reply_text("Wallet not initialized. Use /start first.")
        return

    await update.message.reply_text("Fetching positions...")
    positions = client.get_all_positions()

    if not positions:
        await update.message.reply_text("No open positions.")
        return

    lines = ["*Your Open Positions:*\n"]
    for p in positions:
        side_emoji = "\U0001F7E2" if p["side"] == "LONG" else "\U0001F534"
        lines.append(
            f"{side_emoji} {p['side']} {abs(p['amount']):.4f} {p['product_name']} "
            f"@ ${p['price']:,.2f}"
        )

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_balance(update: Update, context: CallbackContext):
    client = get_user_nado_client(update.effective_user.id)
    if not client:
        await update.message.reply_text("Wallet not initialized. Use /start first.")
        return

    balance = client.get_balance()
    if not balance.get("exists"):
        user = get_user(update.effective_user.id)
        addr = user.wallet_address_testnet if user.network_mode.value == "testnet" else user.wallet_address_mainnet
        await update.message.reply_text(
            f"No subaccount found.\n\n"
            f"Deposit >= $5 USDT0 to your wallet:\n`{addr}`\n\n"
            f"Testnet faucet: https://testnet.nado.xyz/portfolio/faucet",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    usdt = balance.get("balances", {}).get(0, 0)
    msg = f"*Unified Margin Balance:*\n${usdt:,.2f} USDT0"

    for pid, bal in balance.get("balances", {}).items():
        if pid != 0 and bal != 0:
            msg += f"\n{get_product_name(pid)}: {bal:.6f}"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_price(update: Update, context: CallbackContext):
    args = context.args
    client = get_user_nado_client(update.effective_user.id)
    if not client:
        await update.message.reply_text("Wallet not initialized. Use /start first.")
        return

    if args:
        product = args[0].upper()
        from src.nadobro.config import get_product_id
        pid = get_product_id(product)
        if pid is None:
            await update.message.reply_text(f"Unknown product '{product}'.")
            return
        mp = client.get_market_price(pid)
        msg = (
            f"*{product}-PERP:*\n"
            f"Bid: ${mp['bid']:,.2f}\n"
            f"Ask: ${mp['ask']:,.2f}\n"
            f"Mid: ${mp['mid']:,.2f}"
        )
    else:
        prices = client.get_all_market_prices()
        if not prices:
            await update.message.reply_text("Could not fetch prices.")
            return
        lines = ["*Market Prices:*\n"]
        for name, p in prices.items():
            lines.append(f"{name}-PERP: ${p['mid']:,.2f}")
        msg = "\n".join(lines)

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_funding(update: Update, context: CallbackContext):
    args = context.args
    client = get_user_nado_client(update.effective_user.id)
    if not client:
        await update.message.reply_text("Wallet not initialized. Use /start first.")
        return

    if args:
        product = args[0].upper()
        from src.nadobro.config import get_product_id
        pid = get_product_id(product)
        if pid is None:
            await update.message.reply_text(f"Unknown product '{product}'.")
            return
        fr = client.get_funding_rate(pid)
        if fr:
            msg = f"*{product}-PERP Funding:*\nCumulative: {fr['funding_rate']:.6f}"
        else:
            msg = f"Could not fetch funding for {product}."
    else:
        lines = ["*Funding Rates:*\n"]
        for name, info in PRODUCTS.items():
            if info["type"] == "perp":
                fr = client.get_funding_rate(info["id"])
                if fr:
                    lines.append(f"{name}-PERP: {fr['funding_rate']:.6f}")
        msg = "\n".join(lines) if len(lines) > 1 else "Could not fetch funding rates."

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_history(update: Update, context: CallbackContext):
    trades = get_trade_history(update.effective_user.id)
    if not trades:
        await update.message.reply_text("No trade history yet.")
        return

    lines = ["*Recent Trades:*\n"]
    for t in trades[:10]:
        status_emoji = {
            "filled": "\u2705",
            "failed": "\u274C",
            "pending": "\u23F3",
            "cancelled": "\U0001F6AB",
        }.get(t["status"], "\u2753")
        price_str = f"${t['price']:,.2f}" if t["price"] else "N/A"
        lines.append(
            f"{status_emoji} {t['side'].upper()} {t['size']} {t['product']} "
            f"@ {price_str} [{t['type']}] ({t['network']})"
        )

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_analytics(update: Update, context: CallbackContext):
    stats = get_trade_analytics(update.effective_user.id)
    if stats["total_trades"] == 0:
        await update.message.reply_text("No trades yet to analyze.")
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
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_wallet(update: Update, context: CallbackContext):
    info = get_user_wallet_info(update.effective_user.id)
    if not info:
        await update.message.reply_text("User not found. Use /start first.")
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

    msg += "\nUse /mode testnet or /mode mainnet to switch."
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_mode(update: Update, context: CallbackContext):
    args = context.args
    if not args or args[0].lower() not in ["testnet", "mainnet"]:
        await update.message.reply_text("Usage: /mode testnet or /mode mainnet")
        return

    network = args[0].lower()
    success, msg = switch_network(update.effective_user.id, network)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_recover(update: Update, context: CallbackContext):
    args = context.args
    if not args or len(args) < 12:
        await update.message.reply_text(
            "Usage: /recover <12-word mnemonic phrase>\n"
            "Example: /recover word1 word2 word3 ... word12"
        )
        return

    mnemonic = " ".join(args[:12])
    user = get_user(update.effective_user.id)
    if not user:
        await update.message.reply_text("Use /start first.")
        return

    network = user.network_mode.value
    success, msg = recover_user_wallet(update.effective_user.id, mnemonic, network)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    await update.message.reply_text(
        "For security, please delete the message containing your recovery phrase.",
    )


async def cmd_alert(update: Update, context: CallbackContext):
    args = context.args
    if len(args) < 3:
        await update.message.reply_text(
            "Usage: /alert <product> above/below <price>\n"
            "Example: /alert BTC above 100000"
        )
        return

    product = args[0].upper()
    condition = args[1].lower()
    try:
        target = float(args[2])
    except ValueError:
        await update.message.reply_text("Invalid price value.")
        return

    result = create_alert(update.effective_user.id, product, condition, target)
    if result["success"]:
        msg = (
            f"Alert set!\n"
            f"{result['product']} {result['condition']} ${result['target']:,.2f}\n"
            f"ID: #{result['alert_id']}"
        )
    else:
        msg = f"Failed: {result['error']}"

    await update.message.reply_text(msg)


async def cmd_my_alerts(update: Update, context: CallbackContext):
    alerts = get_user_alerts(update.effective_user.id)
    if not alerts:
        await update.message.reply_text("No active alerts. Use /alert to create one.")
        return

    lines = ["*Active Alerts:*\n"]
    for a in alerts:
        lines.append(f"#{a['id']}: {a['product']} {a['condition']} ${a['target']:,.2f} ({a['network']})")

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_del_alert(update: Update, context: CallbackContext):
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /del_alert <alert_id>")
        return

    try:
        alert_id = int(args[0].replace("#", ""))
    except ValueError:
        await update.message.reply_text("Invalid alert ID.")
        return

    result = delete_alert(update.effective_user.id, alert_id)
    await update.message.reply_text(result.get("message", result.get("error", "Unknown error")))


async def cmd_admin_stats(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Admin access required.")
        return

    stats = get_bot_stats()
    msg = (
        f"*Admin Stats:*\n\n"
        f"Total Users: {stats['total_users']}\n"
        f"Active (7d): {stats['active_users_7d']}\n"
        f"Total Trades: {stats['total_trades']}\n"
        f"Filled: {stats['filled_trades']}\n"
        f"Failed: {stats['failed_trades']}\n"
        f"Total Volume: ${stats['total_volume_usd']:,.2f}"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cmd_admin_pause(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Admin access required.")
        return

    currently_paused = is_trading_paused()
    set_trading_paused(not currently_paused, update.effective_user.id)

    if currently_paused:
        await update.message.reply_text("Trading RESUMED.")
    else:
        await update.message.reply_text("Trading PAUSED. All trade commands disabled.")


async def cmd_admin_logs(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Admin access required.")
        return

    args = context.args
    if args and args[0] == "trades":
        trades = get_recent_trades_all()
        if not trades:
            await update.message.reply_text("No trades found.")
            return
        lines = ["*Recent Trades (All Users):*\n"]
        for t in trades:
            lines.append(
                f"User {t['user_id']}: {t['side']} {t['size']} {t['product']} "
                f"@ ${t['price'] or 0:,.2f} [{t['status']}]"
            )
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
    else:
        logs = get_recent_admin_logs()
        if not logs:
            await update.message.reply_text("No admin logs.")
            return
        lines = ["*Recent Admin Logs:*\n"]
        for l in logs:
            lines.append(f"{l['created_at']}: {l['action']} (by {l['admin_id']})")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
