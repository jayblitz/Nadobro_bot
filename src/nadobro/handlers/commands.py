import logging
import time
from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.services.user_service import get_or_create_user, get_user
from src.nadobro.handlers.formatters import escape_md, fmt_help, fmt_status_overview
from src.nadobro.handlers.keyboards import (
    persistent_menu_kb,
    onboarding_language_kb,
    onboarding_accept_tos_kb,
    home_card_kb,
)
from src.nadobro.services.bot_runtime import get_user_bot_status, stop_all_user_bots
from src.nadobro.services.trade_service import close_all_positions
from src.nadobro.services.onboarding_service import (
    is_new_onboarding_complete,
    get_new_onboarding_state,
    evaluate_readiness,
)
from src.nadobro.config import DUAL_MODE_CARD_FLOW
from src.nadobro.handlers.home_card import (
    open_home_card_from_command,
    open_help_card_from_command,
    open_status_card_from_command,
)

logger = logging.getLogger(__name__)


# New onboarding messages (exact copy from spec)
WELCOME_MSG = """Yo what's good, future Nado whale?! 👋💰

Welcome to **Nadobro** — your ultimate trading bro for Perps on Nado!

We're building the dopest Telegram bot on the planet:
• MM Bot (Grid + RGRID that actually prints)
• Delta Neutral (spot + short = chill funding gains)
• Volume Bot (farm that leaderboard volume like a boss)

Unified margin. 5-15ms execution. Zero drama.

First, pick your language vibe:"""

WELCOME_CARD_MSG = """🔥 Nadobro Activated! You're in the squad bro 🔥

Sup, you're now locked in.
We run on Nado's lightning CLOB with unified margin — the cleanest perps game in crypto.

By tapping **"Let's Get It"** you're saying:
✅ I accept the Terms of Use & Privacy Policy

⚡ Bro-Note (read this):
We'll generate a secure Linked Signer address for your default subaccount (we NEVER touch your private keys).
You just paste the PUBLIC address into Nado → Settings → 1-Click Trading (1 tx, 5 seconds).
Main wallet stays untouched. Revoke anytime. Funds 100% yours.

Ready to start printing money?"""

DASHBOARD_MSG = """🚀 Nadobro Dashboard — You're In, Legend!

What we smashing today?"""


async def cmd_start(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    username = update.effective_user.username

    user, is_new, _ = get_or_create_user(telegram_id, username)

    if not is_new_onboarding_complete(telegram_id):
        state = get_new_onboarding_state(telegram_id)
        if not state.get("language"):
            await update.message.reply_text(
                WELCOME_MSG,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=onboarding_language_kb(),
            )
            return
        await update.message.reply_text(
            WELCOME_CARD_MSG,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=onboarding_accept_tos_kb(),
        )
        return

    # Onboarding complete → show dashboard (8 buttons)
    if DUAL_MODE_CARD_FLOW:
        await _send_dashboard_card(update, context, telegram_id)
        return
    await update.message.reply_text(
        DASHBOARD_MSG,
        reply_markup=persistent_menu_kb(),
    )


async def _send_dashboard_card(update: Update, context: CallbackContext, telegram_id: int):
    """Send dashboard text + home card inline keyboard (8 buttons)."""
    await update.message.reply_text(DASHBOARD_MSG, reply_markup=home_card_kb())


async def cmd_help(update: Update, context: CallbackContext):
    if DUAL_MODE_CARD_FLOW:
        await open_help_card_from_command(update, context)
        return
    await update.message.reply_text(
        fmt_help(),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=persistent_menu_kb(),
    )


async def cmd_status(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    status = get_user_bot_status(telegram_id)
    onboarding = evaluate_readiness(telegram_id)
    text = fmt_status_overview(status, onboarding)
    if status.get("last_error"):
        text += f"\nLast error: {escape_md(str(status.get('last_error')))}"

    if DUAL_MODE_CARD_FLOW:
        await open_status_card_from_command(update, context, text)
        return

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=persistent_menu_kb(),
    )


async def cmd_stop_all(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    ok, msg = stop_all_user_bots(telegram_id, cancel_orders=False)
    close_result = close_all_positions(telegram_id)
    if close_result.get("success"):
        closed_msg = f"Closed total position size {close_result.get('cancelled', 0):.8f}."
    else:
        closed_msg = f"No open orders closed ({close_result.get('error', 'none')})."
    prefix = "🛑" if ok else "⚠️"
    await update.message.reply_text(
        f"{prefix} {msg}\n\n{closed_msg}",
        reply_markup=persistent_menu_kb(),
    )


async def cmd_revoke(update: Update, context: CallbackContext):
    """Show Nado revoke steps for linked signer."""
    msg = (
        "🔄 *Revoke Linked Signer (Nado)*\n\n"
        "1. Open Nado → Settings\n"
        "2. 1-Click Trading\n"
        "3. Remove the linked signer address\n\n"
        "Your main wallet and funds stay safe. You can link again anytime via Wallet."
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=persistent_menu_kb())


