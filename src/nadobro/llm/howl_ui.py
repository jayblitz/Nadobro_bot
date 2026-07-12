"""HOWL suggestion-approval inline keyboard.

Domain-owned UI fragment: the scheduler's HOWL notification job and the
Telegram handlers both send it. It lives here — not in handlers/keyboards.py —
so runtime/ never imports the handlers layer.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def howl_approval_kb(suggestion_count: int):
    rows = []
    for i in range(suggestion_count):
        rows.append([
            InlineKeyboardButton(f"✅ Apply #{i+1}", callback_data=f"howl:approve:{i}"),
            InlineKeyboardButton(f"❌ Reject #{i+1}", callback_data=f"howl:reject:{i}"),
        ])
    rows.append([
        InlineKeyboardButton("✅ Apply All", callback_data="howl:approve_all"),
        InlineKeyboardButton("❌ Dismiss All", callback_data="howl:dismiss"),
    ])
    rows.append([
        InlineKeyboardButton("◀ Back", callback_data="strategy:preview:bro"),
    ])
    return InlineKeyboardMarkup(rows)
