import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.services.user_service import (
    get_or_create_user, get_user,
)

logger = logging.getLogger(__name__)

WEBAPP_URL = f"https://{os.environ.get('REPLIT_DEV_DOMAIN', os.environ.get('REPLIT_DOMAINS', 'localhost:5000'))}"


def _get_webapp_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "Open Nadobro",
            web_app=WebAppInfo(url=WEBAPP_URL)
        )],
    ])


async def cmd_start(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    username = update.effective_user.username

    user, is_new, mnemonic = get_or_create_user(telegram_id, username)

    if is_new:
        msg = (
            "*Welcome to Nadobro!*\n"
            "Your AI trading companion for Nado DEX.\n\n"
            "Your testnet wallet has been created:\n"
            f"`{user.wallet_address_testnet}`\n\n"
        )
        if mnemonic:
            msg += (
                "SAVE YOUR RECOVERY PHRASE (shown only once):\n"
                f"`{mnemonic}`\n\n"
                "Store this safely - it's the ONLY way to recover your wallet.\n\n"
            )
        msg += (
            "*Getting started:*\n"
            "1. Get testnet ETH: https://docs.inkonchain.com/tools/faucets\n"
            "2. Get USDT0: https://testnet.nado.xyz/portfolio/faucet\n"
            "3. Deposit >= $5 USDT0 on Nado testnet\n\n"
            "Tap the button below to open the trading app:"
        )
    else:
        mode = user.network_mode.value
        badge = "MAINNET" if mode == "mainnet" else "TESTNET"
        addr = user.wallet_address_mainnet if mode == "mainnet" else user.wallet_address_testnet
        msg = (
            f"*Welcome back!* [{badge}]\n"
            f"Wallet: `{addr}`\n\n"
            "Tap below to open the trading app:"
        )

    await update.message.reply_text(
        msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_get_webapp_keyboard(),
    )


async def cmd_help(update: Update, context: CallbackContext):
    msg = (
        "*Nadobro - Nado DEX Trading App*\n\n"
        "Tap the button below to open the full trading interface.\n\n"
        "You can trade perpetual futures on:\n"
        "BTC, ETH, SOL, XRP, BNB, LINK, DOGE, AVAX\n\n"
        "Features:\n"
        "- Market & limit orders\n"
        "- Real-time prices\n"
        "- Position management\n"
        "- Price alerts\n"
        "- Testnet & mainnet support"
    )
    await update.message.reply_text(
        msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_get_webapp_keyboard(),
    )
