"""Slash-command + intent handlers for the Morning Brief / market news."""

from __future__ import annotations

import logging
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import CallbackContext

from src.nadobro.handlers.formatters import escape_md, fmt_bro_answer_card
from src.nadobro.handlers.keyboards import bro_answer_kb
from src.nadobro.llm.morning_brief import render_morning_brief
from src.nadobro.users.user_service import get_or_create_user

logger = logging.getLogger(__name__)


async def _run_blocking_safe(fn, *args):
    try:
        from src.nadobro.core.async_utils import run_blocking

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
    from src.nadobro.llm.night_howl_service import get_report, list_report_dates

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
        # Plain text on purpose: ``date`` is the raw user argument — sending
        # it inside Markdown lets a stray backtick unbalance the entities,
        # Telegram rejects the message, and the user gets total silence.
        await chat.send_message(
            "🌙 No Night HOWL report found"
            + (f" for {date}." if date else " yet.")
            + " Reports are generated each morning at your local 8am after you've "
            "traded. See dates with /howl list."
        )
        return
    try:
        await chat.send_message(report["markdown"], parse_mode=ParseMode.MARKDOWN)
    except Exception:  # policy: degrade-ok(saved report may carry unbalanced markdown)
        from src.nadobro.handlers.render_utils import plain_text_fallback

        await chat.send_message(plain_text_fallback(report["markdown"]))

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
    message = update.effective_message
    first_name = update.effective_user.first_name

    # Mark BEFORE launching so a second /brief inside the window is throttled
    # even while the first is still rendering; cleared again on failure so a
    # failed brief doesn't lock the user out for 5 minutes.
    _mark_used(context)

    async def _render_and_send() -> None:
        try:
            body, sources = await render_morning_brief(
                telegram_id=telegram_id,
                user_name=first_name,
                network=network,
                categories=categories,
            )
        except Exception as exc:
            logger.exception("morning brief render failed: %s", exc)
            if context.user_data is not None:
                context.user_data.pop(_LAST_BRIEF_KEY, None)
            if message is not None:
                try:
                    await message.reply_text(
                        escape_md("⚠️ Couldn't compose the brief right now. Try again in a moment."),
                        parse_mode=ParseMode.MARKDOWN_V2,
                        reply_markup=InlineKeyboardMarkup(
                            [[InlineKeyboardButton("🏠 Home", callback_data="nav:main")]]
                        ),
                    )
                except Exception:  # policy: degrade-ok(error notice is best-effort)
                    logger.warning("brief failure notice send failed", exc_info=True)
            return

        card = fmt_bro_answer_card(body, mode="morning_brief", sources=sources)
        if message is not None:
            try:
                await message.reply_text(
                    card,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=bro_answer_kb("morning_brief"),
                )
            except Exception:  # policy: degrade-ok(fall back to plain text)
                from src.nadobro.handlers.render_utils import plain_text_fallback

                await message.reply_text(
                    plain_text_fallback(card),
                    reply_markup=bro_answer_kb("morning_brief"),
                )

    # Render OUTSIDE the per-user serialization lock: a cold brief fans out
    # ~13 news connectors plus an LLM call (up to ~45-60s). Holding the lock
    # for that long queued /stop_all and Stop-button taps behind it — an
    # emergency flatten must never wait on a news round trip.
    context.application.create_task(_render_and_send(), update=update)


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
