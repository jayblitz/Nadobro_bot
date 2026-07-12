"""Points/LOWIQPTS inline keyboards.

Domain-owned UI fragments: points_service (the relay-driven points flow) sends
these itself, and the Telegram handlers reuse them. They live here — not in
handlers/keyboards.py — so the users/ domain never imports the handlers layer.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def points_scope_kb() -> InlineKeyboardMarkup:
    """LOWIQPTS flow: only weekly snapshot via relay; no period toggles in UI."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🏆 Refresh points", callback_data="points:refresh"),
            InlineKeyboardButton("❌ Cancel LOWIQPTS", callback_data="points:cancel"),
        ],
        [
            InlineKeyboardButton("🏠 Home", callback_data="nav:main"),
        ],
    ])


def points_followup_options_kb(options: list[str]):
    rows = []
    clean = [str(opt).strip() for opt in (options or []) if str(opt).strip()]
    for i in range(0, len(clean), 2):
        row = []
        for j, label in enumerate(clean[i:i + 2]):
            row.append(InlineKeyboardButton(label[:32], callback_data=f"points:replyopt:{i + j}"))
        if row:
            rows.append(row)
    rows.append([InlineKeyboardButton("❌ Cancel LOWIQPTS", callback_data="points:cancel")])
    rows.append([InlineKeyboardButton("🏠 Home", callback_data="nav:main")])
    return InlineKeyboardMarkup(rows)
