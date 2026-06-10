"""Price/funding alert callback handlers (alert:* callback data).

Extracted from callbacks.py (decomposition slice, 2026-06). May import
shared utils from callbacks at module level — callbacks only imports this
module lazily inside its _handle_alert shim, so there is no cycle.
"""
from __future__ import annotations

import logging

from src.nadobro.handlers.formatters import escape_md, fmt_alert_menu_intro, fmt_alert_product_prompt, fmt_alert_target_prompt, fmt_alerts
from src.nadobro.handlers.keyboards import alerts_kb, alert_product_kb, alert_condition_kb, alert_delete_kb, back_kb
from src.nadobro.i18n import localize_text, get_active_language
from src.nadobro.services.alert_service import get_user_alerts, delete_alert
from src.nadobro.services.user_service import get_user
from telegram.constants import ParseMode

from src.nadobro.handlers.callbacks import _edit_loc  # noqa: E402

logger = logging.getLogger(__name__)


async def _handle_alert(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"

    if action == "menu":
        await _edit_loc(query, 
            fmt_alert_menu_intro(),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=alerts_kb(),
        )

    elif action == "set":
        await _edit_loc(query, 
            fmt_alert_product_prompt(),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=alert_product_kb(network=network),
        )

    elif action == "product" and len(parts) >= 3:
        product = parts[2]
        context.user_data["pending_alert"] = {"product": product}
        await _edit_loc(query,
            fmt_alert_condition_prompt(product),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=alert_condition_kb(product),
        )

    elif action == "cond" and len(parts) >= 4:
        product = parts[2]
        condition = parts[3]
        context.user_data["pending_alert"] = {"product": product, "condition": condition}
        _lang = get_active_language()
        condition_labels = {
            "above": "Price Above",
            "below": "Price Below",
            "funding_above": "Funding Rate Above",
            "funding_below": "Funding Rate Below",
            "pnl_above": "PnL Above",
            "pnl_below": "PnL Below",
        }
        label = condition_labels.get(condition, condition)
        if condition.startswith("funding"):
            example = localize_text("Example: `0.01` (funding rate in %)", _lang)
        elif condition.startswith("pnl"):
            example = localize_text("Example: `50` (PnL in USD)", _lang)
        else:
            example = localize_text("Example: `100000` (price in USD)", _lang)
        await _edit_loc(query,
            fmt_alert_target_prompt(product, label, example),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(),
        )

    elif action == "view":
        alerts = get_user_alerts(telegram_id)
        msg = fmt_alerts(alerts)
        kb = alert_delete_kb(alerts) if alerts else back_kb()
        await _edit_loc(query,
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=kb,
        )

    elif action == "del" and len(parts) >= 3:
        alert_id = int(parts[2])
        result = delete_alert(telegram_id, alert_id)

        if result["success"]:
            msg = f"✅ {escape_md(result['message'])}"
        else:
            msg = f"❌ {escape_md(result['error'])}"

        alerts = get_user_alerts(telegram_id)
        alerts_msg = fmt_alerts(alerts)
        final_msg = f"{msg}\n\n{alerts_msg}"

        kb = alert_delete_kb(alerts) if alerts else back_kb()
        await _edit_loc(query, 
            final_msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=kb,
        )
