"""Slash-command + intent handlers for the Morning Brief / market news."""

from __future__ import annotations

import logging
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import CallbackContext

from src.nadobro.handlers.formatters import escape_md, fmt_bro_answer_card
from src.nadobro.handlers.keyboards import bro_answer_kb
from src.nadobro.services.morning_brief import render_morning_brief
from src.nadobro.services.user_service import get_or_create_user

logger = logging.getLogger(__name__)


async def _run_blocking_safe(fn, *args):
    try:
        from src.nadobro.services.async_utils import run_blocking

        return await run_blocking(fn, *args)
    except Exception:  # noqa: BLE001
        logger.warning("night howl command read failed", exc_info=True)
        return None


async def cmd_night_howl(update: Update, context: CallbackContext) -> None:
    """``/howl`` — show the user's saved Night HOWL report(s).

    ``/howl``            → latest saved report
    ``/howl list``       → the dates of saved reports
    ``/howl YYYY-MM-DD`` → a specific saved report
    """
    chat = update.effective_chat
    if chat is None or update.effective_user is None:
        return
    telegram_id = update.effective_user.id
    network = _network_for(telegram_id)
    from src.nadobro.services.night_howl_service import get_report, list_report_dates

    arg = str(context.args[0]).strip().lower() if context.args else ""

    if arg == "list":
        dates = await _run_blocking_safe(list_report_dates, telegram_id, network)
        if not dates:
            await chat.send_message(
                "🌙 No Night HOWL reports saved yet. You'll get your first one at "
                "8am local time after a day of trading."
            )
            return
        body = "🌙 *Saved Night HOWL reports*\n" + "\n".join(f"• `{d}`" for d in dates)
        body += "\n\nView one with `/howl YYYY-MM-DD`."
        await chat.send_message(body, parse_mode=ParseMode.MARKDOWN)
        return

    date = arg or None
    report = await _run_blocking_safe(get_report, telegram_id, network, date)
    if not report or not report.get("markdown"):
        await chat.send_message(
            "🌙 No Night HOWL report found"
            + (f" for `{date}`." if date else " yet.")
            + " Reports are generated each morning at your local 8am after you've "
            "traded. See dates with `/howl list`.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    await chat.send_message(report["markdown"], parse_mode=ParseMode.MARKDOWN)

_BRIEF_RATE_LIMIT_SECONDS = 300
_LAST_BRIEF_KEY = "last_brief_ts"


def _is_rate_limited(context: CallbackContext) -> bool:
    last = context.user_data.get(_LAST_BRIEF_KEY) if context.user_data is not None else None
    if last is None:
        return False
    try:
        return (time.time() - float(last)) < _BRIEF_RATE_LIMIT_SECONDS
    except Exception:
        return False


def _mark_used(context: CallbackContext) -> None:
    if context.user_data is not None:
        context.user_data[_LAST_BRIEF_KEY] = time.time()


def _network_for(telegram_id: int) -> str:
    try:
        user = get_or_create_user(telegram_id, None)[0]
        mode = getattr(user, "network_mode", None) or "mainnet"
        # Coerce enum (NetworkMode.MAINNET) → "mainnet"; passes plain strings through.
        value = getattr(mode, "value", mode)
        return str(value).lower() or "mainnet"
    except Exception:
        return "mainnet"


async def _send_brief(
    update: Update,
    context: CallbackContext,
    *,
    categories: list[str] | None = None,
) -> None:
    chat = update.effective_chat
    if chat is None or update.effective_user is None:
        return
    telegram_id = update.effective_user.id

    if _is_rate_limited(context):
        message = update.effective_message
        if message is not None:
            await message.reply_text(
                escape_md("🕐 Morning Brief was just generated. Try again in a few minutes."),
                parse_mode=ParseMode.MARKDOWN_V2,
            )
        return

    try:
        await chat.send_action(ChatAction.TYPING)
    except Exception:
        pass

    network = _network_for(telegram_id)

    try:
        body, sources = await render_morning_brief(
            telegram_id=telegram_id,
            user_name=update.effective_user.first_name,
            network=network,
            categories=categories,
        )
    except Exception as exc:
        logger.exception("morning brief render failed: %s", exc)
        message = update.effective_message
        if message is not None:
            await message.reply_text(
                escape_md("⚠️ Couldn't compose the brief right now. Try again in a moment."),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🏠 Home", callback_data="nav:main")]]
                ),
            )
        return

    card = fmt_bro_answer_card(body, mode="morning_brief", sources=sources)
    message = update.effective_message
    if message is not None:
        await message.reply_text(
            card,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=bro_answer_kb("morning_brief"),
        )
    _mark_used(context)


async def cmd_morning_brief(update: Update, context: CallbackContext) -> None:
    """/brief — full morning brief across all categories."""
    await _send_brief(update, context, categories=None)


async def cmd_market_news(update: Update, context: CallbackContext) -> None:
    """/news [category] — same composer; optional category filter."""
    args = list(getattr(context, "args", None) or [])
    categories: list[str] | None = None
    if args:
        valid = {"crypto", "stocks", "tradfi", "rwa", "geopolitics", "economics", "ft"}
        picked = [a.lower() for a in args if a.lower() in valid]
        if picked:
            categories = picked
    await _send_brief(update, context, categories=categories)
