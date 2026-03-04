import logging
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.services.user_service import get_or_create_user, get_user, ensure_active_wallet_ready

INTRO_VIDEO_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "intro_video.mov")
START_IMAGE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "start-bot.png")
from src.nadobro.handlers.formatters import escape_md, fmt_help, fmt_status_overview
from src.nadobro.handlers.keyboards import (
    persistent_menu_kb,
    onboarding_language_kb,
    onboarding_accept_tos_kb,
    home_card_kb,
)
from src.nadobro.services.bot_runtime import get_user_bot_status, stop_all_user_bots
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
from src.nadobro.services.perf import summary_lines

logger = logging.getLogger(__name__)


# New onboarding messages (exact copy from spec)
WELCOME_MSG = """Welcome to Nadobro 👋

Your trading companion for perps on Nado DEX — fast execution, automated strategies, and AI-powered insights, all from Telegram.

Pick your language:"""

WELCOME_CARD_MSG = """🔥 You're in!

By tapping **"Let's Get It"** you accept the Terms of Use & Privacy Policy.

🔐 How it works:
We generate a secure 1CT signing key for your account. Your main wallet keys are never touched. Revoke anytime.

Ready?"""

DASHBOARD_MSG = """🚀 *Nadobro Command Center is live\\!*

Yo legend, your trading copilot is online and ready to cook\\.

Pick a module below and let's trade smarter \\(and faster\\) ⚡"""

WALLET_SETUP_CTA_MSG = """👛 *Let's connect your wallet first*

Before trading, you need to link your signer once.
Tap below and follow the guided steps to finish setup in a minute."""


async def _send_start_image(update: Update):
    if not update.message or not os.path.exists(START_IMAGE_PATH):
        return
    try:
        with open(START_IMAGE_PATH, "rb") as img:
            await update.message.reply_photo(photo=img)
    except Exception as e:
        logger.warning("Failed to send start image: %s", e)


async def cmd_start(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    username = update.effective_user.username

    user, is_new, _ = get_or_create_user(telegram_id, username)
    await _send_start_image(update)

    if not is_new_onboarding_complete(telegram_id):
        state = get_new_onboarding_state(telegram_id)
        if not state.get("language"):
            if is_new and os.path.exists(INTRO_VIDEO_PATH):
                try:
                    with open(INTRO_VIDEO_PATH, "rb") as vf:
                        await update.message.reply_video(video=vf)
                except Exception as e:
                    logger.warning("Failed to send intro video: %s", e)
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
    wallet_ready, _ = ensure_active_wallet_ready(telegram_id)
    if not wallet_ready:
        await update.message.reply_text(
            WALLET_SETUP_CTA_MSG,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👛 Start Wallet Setup", callback_data="wallet:setup")],
                [InlineKeyboardButton("🏠 Open Dashboard", callback_data="nav:main")],
            ]),
        )
        return

    if DUAL_MODE_CARD_FLOW:
        await _send_dashboard_card(update, context, telegram_id)
        return
    await update.message.reply_text(
        DASHBOARD_MSG,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=persistent_menu_kb(),
    )


async def _send_dashboard_card(update: Update, context: CallbackContext, telegram_id: int):
    """Send dashboard text + home card inline keyboard (8 buttons)."""
    await update.message.reply_text(
        DASHBOARD_MSG,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=home_card_kb(),
    )


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
    perf_lines = summary_lines(top_n=5)
    if perf_lines:
        text += "\n\n*Perf Snapshot*"
        for line in perf_lines:
            text += f"\n• {escape_md(line)}"

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
    prefix = "🛑" if ok else "⚠️"
    await update.message.reply_text(
        f"{prefix} {msg}\n\nTo close open positions, use the Positions menu.",
        reply_markup=persistent_menu_kb(),
    )


async def cmd_revoke(update: Update, context: CallbackContext):
    """Show Nado revoke steps for 1CT key."""
    msg = (
        "🔄 *Revoke 1CT Key (Nado)*\n\n"
        "1. Open Nado → Settings\n"
        "2. 1-Click Trading → Advanced 1CT\n"
        "3. Disable the toggle and save\n\n"
        "Your main wallet and funds stay safe. You can link again anytime via Wallet."
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=persistent_menu_kb())


