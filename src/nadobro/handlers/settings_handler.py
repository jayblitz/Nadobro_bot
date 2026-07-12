"""Settings callback handlers (settings:* callback data).

Extracted from callbacks.py (decomposition slice, 2026-06). May import
shared utils from callbacks at module level — callbacks only imports this
module lazily inside its _handle_settings shim, so there is no cycle.
"""
from __future__ import annotations

import logging

from src.nadobro.handlers.formatters import escape_md, fmt_settings
from src.nadobro.handlers.keyboards import settings_kb, settings_leverage_kb, settings_slippage_kb, settings_language_kb, risk_profile_kb
from src.nadobro.i18n import _ACTIVE_LANG
from src.nadobro.users.settings_service import update_user_settings
from src.nadobro.users.user_service import get_user, update_user_language
from telegram.constants import ParseMode

from src.nadobro.handlers.callbacks import _edit_loc, _get_user_settings  # noqa: E402

logger = logging.getLogger(__name__)


async def _handle_settings(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"

    user_settings = _get_user_settings(telegram_id, context)

    if action == "view":
        msg = fmt_settings(user_settings)
        lev = user_settings.get("default_leverage", 1)
        slip = user_settings.get("slippage", 1)
        await _edit_loc(query, 
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(lev, slip),
        )

    elif action == "leverage_menu":
        await _edit_loc(query, 
            "⚡ *Default Leverage*\n\nChoose the leverage Nadobro should prefill for manual trades\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_leverage_kb(),
        )
    elif action == "risk_menu":
        await _edit_loc(query, 
            "🛡 *Risk Profile*\n\n"
            "This presets leverage and slippage so trades are faster and more consistent\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=risk_profile_kb(),
        )

    elif action == "leverage" and len(parts) >= 3:
        lev = int(parts[2])
        _, user_settings = update_user_settings(
            telegram_id, lambda s: s.update({"default_leverage": lev})
        )
        msg = fmt_settings(user_settings)
        slip = user_settings.get("slippage", 1)
        await _edit_loc(query, 
            "✅ *Default leverage updated*\n\n{settings}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(lev, slip),
            lev=escape_md(str(lev)),
            settings=msg,
        )

    elif action == "slippage_menu":
        await _edit_loc(query, 
            "📊 *Slippage*\n\nChoose the default slippage tolerance used for manual trades\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_slippage_kb(),
        )
    elif action == "language_menu":
        user = get_user(telegram_id)
        lang = (getattr(user, "language", None) or "en").lower()
        await _edit_loc(query, 
            "🌐 *Select Language*\n\nChoose your preferred language for onboarding and UI copy\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_language_kb(lang),
        )
    elif action == "language" and len(parts) >= 3:
        lang = (parts[2] or "").lower()
        supported = {"en", "zh", "fr", "ar", "ru", "ko"}
        if lang not in supported:
            return
        update_user_language(telegram_id, lang)
        _ACTIVE_LANG.set(lang)
        user = get_user(telegram_id)
        current = (getattr(user, "language", None) or lang).lower()
        await _edit_loc(query,
            "✅ *Language updated* to *{lang}*\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            lang=escape_md(lang.upper()),
            reply_markup=settings_language_kb(current),
        )

    elif action == "slippage" and len(parts) >= 3:
        slip = float(parts[2])
        _, user_settings = update_user_settings(
            telegram_id, lambda s: s.update({"slippage": slip})
        )
        msg = fmt_settings(user_settings)
        lev = user_settings.get("default_leverage", 1)
        await _edit_loc(query, 
            "✅ *Slippage updated*\n\n{settings}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(lev, slip),
            slip=escape_md(str(slip)),
            settings=msg,
        )
    elif action == "risk" and len(parts) >= 3:
        profile = parts[2]
        presets = {
            "conservative": {"default_leverage": 2, "slippage": 0.5},
            "balanced": {"default_leverage": 5, "slippage": 1.0},
            "aggressive": {"default_leverage": 10, "slippage": 2.0},
        }
        chosen = presets.get(profile)
        if not chosen:
            return
        _, saved = update_user_settings(
            telegram_id,
            lambda s: s.update(
                {
                    "default_leverage": chosen["default_leverage"],
                    "slippage": chosen["slippage"],
                    "risk_profile": profile,
                }
            ),
        )
        context.user_data["settings"] = saved
        msg = fmt_settings(chosen)
        await _edit_loc(query, 
            "✅ *Risk profile updated:* *{profile}*\n\n{settings}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(chosen["default_leverage"], chosen["slippage"]),
            profile=escape_md(profile.upper()),
            settings=msg,
        )
