import logging
from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.services.user_service import (
    get_or_create_user, get_user_nado_client, get_user,
)
from src.nadobro.handlers.formatters import (
    escape_md, fmt_dashboard, fmt_help,
)
from src.nadobro.handlers.keyboards import main_menu_kb, back_kb

logger = logging.getLogger(__name__)


async def cmd_start(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    username = update.effective_user.username

    user, is_new, mnemonic = get_or_create_user(telegram_id, username)

    if is_new:
        welcome = (
            "🔷 *Welcome to NADOBRO\\!*\n"
            "Your trading bot for Nado DEX perpetual futures\\.\n\n"
            "🔑 *Your testnet wallet has been created:*\n"
            f"`{escape_md(user.wallet_address_testnet)}`\n\n"
        )
        if mnemonic:
            welcome += (
                "⚠️ *SAVE YOUR RECOVERY PHRASE \\(shown only once\\):*\n"
                f"`{escape_md(mnemonic)}`\n\n"
                "Store this safely \\— it's the ONLY way to recover your wallet\\.\n\n"
            )
        welcome += (
            "📌 *Getting started:*\n"
            "1\\. Get testnet ETH: docs\\.inkonchain\\.com/tools/faucets\n"
            "2\\. Get USDT0: testnet\\.nado\\.xyz/portfolio/faucet\n"
            "3\\. Deposit ≥ \\$5 USDT0 on Nado testnet\n"
        )

        await update.message.reply_text(
            welcome,
            parse_mode=ParseMode.MARKDOWN_V2,
        )

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
        logger.warning(f"Failed to fetch data for dashboard: {e}")

    dashboard = fmt_dashboard(user, balance, positions, prices, network)
    await update.message.reply_text(
        dashboard,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=main_menu_kb(),
    )


async def cmd_help(update: Update, context: CallbackContext):
    await update.message.reply_text(
        fmt_help(),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=main_menu_kb(),
    )
