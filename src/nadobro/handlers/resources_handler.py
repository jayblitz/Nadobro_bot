"""Telegram handler for the Resources card.

The Resources card is a static menu of URL buttons pointing at the Nado
documentation, developer docs, and NadoBro's own docs/X. There is no state
machine here — one callback ``resources:home`` opens the card.
"""

from __future__ import annotations

from telegram.constants import ParseMode
from telegram.ext import CallbackContext

from src.nadobro.handlers.keyboards import resources_kb


_RESOURCES_TEXT = (
    "📚 *Resources*\n\n"
    "Everything you need to learn the Nado stack and stay plugged into NadoBro.\n\n"
    "• *Nado Docs* · exchange overview, products, NLP vault mechanics.\n"
    "• *Developer Docs* · Get started, REST/WebSocket API, CLI & MCP server, "
    "TypeScript SDK.\n"
    "• *NadoBro* · our user docs and announcements on X.\n\n"
    "Tap any link below to open it in your browser."
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
