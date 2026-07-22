"""Telegram handler for the Official Links card.

A static menu of URL buttons pointing at NadoBro's official channels. There is
no state machine here — one callback ``resources:home`` opens the card.
"""

from __future__ import annotations

from telegram.constants import ParseMode
from telegram.ext import CallbackContext

from src.nadobro.handlers.keyboards import resources_kb


_RESOURCES_TEXT = (
    "🔗 *Official Links*\n\n"
    "Stay plugged into NadoBro — always use these official links:\n\n"
    "• *Bot* — the official NadoBro Telegram bot\n"
    "• *Twitter* — news and announcements\n"
    "• *Docs* — guides and how-tos\n"
    "• *Community* — the NadoBro Telegram group\n\n"
    "Tap a button below to open it."
)


async def handle_resources_callback(query, context: CallbackContext) -> bool:
    data = (query.data or "").strip()
    if not data.startswith("resources:"):
        return False
    await query.edit_message_text(
        _RESOURCES_TEXT,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=resources_kb(),
        disable_web_page_preview=True,
    )
    return True
