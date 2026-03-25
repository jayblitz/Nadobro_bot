import logging
import os
from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.i18n import language_context, get_user_language, localize_text, localize_markup, get_active_language
from src.nadobro.services.user_service import get_or_create_user, get_user

INTRO_VIDEO_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "intro_video.mov")
from src.nadobro.handlers.formatters import escape_md, fmt_help, fmt_status_overview
from src.nadobro.handlers.keyboards import (
    persistent_menu_kb,
    onboarding_language_kb,
    onboarding_accept_tos_kb,
    home_card_kb,
    status_kb,
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
)
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

DASHBOARD_MSG = """🤖 Nadobro Command Center online.

Select a module below to trade, monitor, and run strategy automation."""


async def cmd_start(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    username = update.effective_user.username

    user, is_new, _ = get_or_create_user(telegram_id, username)

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
        with language_context(get_user_language(telegram_id)):
            lang = get_active_language()
            await update.message.reply_text(
                localize_text(WELCOME_CARD_MSG, lang),
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=localize_markup(onboarding_accept_tos_kb(), lang),
            )
        return

    with language_context(get_user_language(telegram_id)):
        lang = get_active_language()
        if DUAL_MODE_CARD_FLOW:
            await _send_dashboard_card(update, context, telegram_id)
            return
        await update.message.reply_text(
            localize_text(DASHBOARD_MSG, lang),
            reply_markup=localize_markup(persistent_menu_kb(), lang),
        )


async def _send_dashboard_card(update: Update, context: CallbackContext, telegram_id: int):
    lang = get_active_language()
    await update.message.reply_text(localize_text(DASHBOARD_MSG, lang), reply_markup=localize_markup(home_card_kb(), lang))


async def cmd_help(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    with language_context(get_user_language(telegram_id)):
        if DUAL_MODE_CARD_FLOW:
            await open_help_card_from_command(update, context)
            return
        lang = get_active_language()
        await update.message.reply_text(
            localize_text(fmt_help(), lang),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=localize_markup(persistent_menu_kb(), lang),
        )


async def cmd_status(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    with language_context(get_user_language(telegram_id)):
        status = get_user_bot_status(telegram_id)
        onboarding = evaluate_readiness(telegram_id)
        text = fmt_status_overview(status, onboarding)

        lang = get_active_language()
        localized = localize_text(text, lang)
        reply_markup = localize_markup(status_kb(), lang)
        # Always send a visible reply so /status works even when the home card is off-screen
        # or edit-in-place fails (webhook / concurrent updates).
        try:
            await update.message.reply_text(
                localized,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=reply_markup,
            )
        except Exception:
            from src.nadobro.handlers.home_card import _plain_text_fallback

            await update.message.reply_text(
                _plain_text_fallback(localized),
                reply_markup=reply_markup,
            )


async def cmd_stop_all(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    with language_context(get_user_language(telegram_id)):
        lang = get_active_language()
        ok, msg = stop_all_user_bots(telegram_id, cancel_orders=False)
        prefix = "🛑" if ok else "⚠️"
        close_msg = localize_text("To close open positions, use the Positions menu.", lang)
        await update.message.reply_text(
            f"{prefix} {msg}\n\n{close_msg}",
            reply_markup=localize_markup(persistent_menu_kb(), lang),
        )


async def cmd_revoke(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    with language_context(get_user_language(telegram_id)):
        lang = get_active_language()
        msg = localize_text(
            "🔄 *Revoke 1CT Key (Nado)*\n\n"
            "1. Open Nado → Settings\n"
            "2. 1-Click Trading → Advanced 1CT\n"
            "3. Disable the toggle and save\n\n"
            "Your main wallet and funds stay safe. You can link again anytime via Wallet.", lang
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=localize_markup(persistent_menu_kb(), lang))


