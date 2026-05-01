import logging
import os
from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.i18n import language_context, get_user_language, localize_text, localize_markup, get_active_language
from src.nadobro.handlers.render_utils import plain_text_fallback
from src.nadobro.services.user_service import get_or_create_user, get_user

INTRO_VIDEO_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "intro_video.mov")
from src.nadobro.handlers.formatters import (
    fmt_dashboard_home,
    fmt_help,
    fmt_ops_overview,
    fmt_revoke_card,
    fmt_status_overview,
    fmt_stop_all_result,
)
from src.nadobro.handlers.keyboards import (
    persistent_menu_kb,
    onboarding_language_kb,
    onboarding_accept_tos_kb,
    home_card_kb,
    status_kb,
    private_access_kb,
)
from src.nadobro.services.bot_runtime import get_user_bot_status, stop_all_user_bots
from src.nadobro.services.nado_tooling_service import get_ops_diagnostics
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
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.invite_service import has_private_access, redeem_invite_code
logger = logging.getLogger(__name__)


# New onboarding messages (exact copy from spec)
WELCOME_MSG = """Welcome to Nadobro 👋

Trade perps on Nado DEX from Telegram with guided execution, portfolio tools, automation, and AI support.

Pick your language:"""

WELCOME_CARD_MSG = """🔥 You're in!

By tapping *"Let's Get It"* you accept the Terms of Use & Privacy Policy.

🔐 How it works:
We generate a secure 1CT signing key for your account. Your main wallet keys are never touched. Revoke anytime.

Ready?"""

PRIVATE_ACCESS_MSG = """🔐 Private Alpha Access

Welcome to Nadobro Bot!

This is a private alpha version. To access the bot, please enter your access code.

If you don't have an access code, please contact @jaynadobro to request one.

Enter your 8-character access code below:"""

async def _ensure_private_access(update: Update, telegram_id: int) -> bool:
    if await run_blocking(has_private_access, telegram_id):
        return True
    await update.message.reply_text(PRIVATE_ACCESS_MSG, reply_markup=private_access_kb())
    return False


async def cmd_start(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    username = update.effective_user.username

    user, is_new, _ = get_or_create_user(telegram_id, username)

    invite_arg = context.args[0] if getattr(context, "args", None) else None
    if not await run_blocking(has_private_access, telegram_id):
        if invite_arg:
            ok, msg = await run_blocking(redeem_invite_code, telegram_id, username, invite_arg)
            if ok:
                await update.message.reply_text(msg)
            else:
                await update.message.reply_text(msg, reply_markup=private_access_kb())
                return
        else:
            await update.message.reply_text(PRIVATE_ACCESS_MSG, reply_markup=private_access_kb())
            return

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
            localize_text(fmt_dashboard_home(), lang),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=localize_markup(persistent_menu_kb(), lang),
        )


async def _send_dashboard_card(update: Update, context: CallbackContext, telegram_id: int):
    lang = get_active_language()
    await update.message.reply_text(
        localize_text(fmt_dashboard_home(), lang),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=localize_markup(home_card_kb(), lang),
    )


async def cmd_help(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    if not await _ensure_private_access(update, telegram_id):
        return
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
    if not await _ensure_private_access(update, telegram_id):
        return
    with language_context(get_user_language(telegram_id)):
        status = await run_blocking(get_user_bot_status, telegram_id)
        onboarding = await run_blocking(evaluate_readiness, telegram_id)
        text = fmt_status_overview(status, onboarding)
        try:
            from src.nadobro.services.feature_flags import studio_enabled
            from src.nadobro.studio.status import build_status_cards

            if studio_enabled():
                studio_text, studio_markup = await run_blocking(
                    build_status_cards,
                    telegram_id,
                    onboarding.get("network"),
                )
                text = f"{text}\n\n{studio_text}"
            else:
                studio_markup = None
        except Exception as e:
            logger.warning("Studio status render failed: %s", e)
            studio_markup = None

        lang = get_active_language()
        localized = localize_text(text, lang)
        reply_markup = studio_markup or localize_markup(
            status_kb(
                is_running=bool(status.get("running")),
                strategy_label=str(status.get("strategy") or "").upper() or None,
            ),
            lang,
        )
        # Always send a visible reply so /status works even when the home card is off-screen
        # or edit-in-place fails (webhook / concurrent updates).
        try:
            await update.message.reply_text(
                localized,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=reply_markup,
            )
        except Exception:
            await update.message.reply_text(
                plain_text_fallback(localized),
                reply_markup=reply_markup,
            )


async def cmd_ops(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    if not await _ensure_private_access(update, telegram_id):
        return
    with language_context(get_user_language(telegram_id)):
        status = await run_blocking(get_user_bot_status, telegram_id)
        ops = await run_blocking(get_ops_diagnostics, telegram_id)
        text = fmt_ops_overview(status, ops)

        lang = get_active_language()
        localized = localize_text(text, lang)
        reply_markup = localize_markup(
            status_kb(
                is_running=bool(status.get("running")),
                strategy_label=str(status.get("strategy") or "").upper() or None,
            ),
            lang,
        )
        try:
            await update.message.reply_text(
                localized,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=reply_markup,
            )
        except Exception:
            await update.message.reply_text(
                plain_text_fallback(localized),
                reply_markup=reply_markup,
            )


async def cmd_stop_all(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    if not await _ensure_private_access(update, telegram_id):
        return
    with language_context(get_user_language(telegram_id)):
        lang = get_active_language()
        ok, msg = stop_all_user_bots(telegram_id, cancel_orders=False)
        close_msg = localize_text("To close open positions, use the Positions menu.", lang)
        await update.message.reply_text(
            localize_text(fmt_stop_all_result(ok, msg, close_msg), lang),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=localize_markup(persistent_menu_kb(), lang),
        )


async def cmd_revoke(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    if not await _ensure_private_access(update, telegram_id):
        return
    with language_context(get_user_language(telegram_id)):
        lang = get_active_language()
        await update.message.reply_text(
            localize_text(fmt_revoke_card(), lang),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=localize_markup(persistent_menu_kb(), lang),
        )


