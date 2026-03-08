import logging

from telegram.constants import ParseMode

from src.nadobro.handlers.formatters import fmt_points_dashboard
from src.nadobro.handlers.keyboards import points_scope_kb
from src.nadobro.handlers.points_mascot import mascot_caption_for_cost, mascot_path_for_cost
from src.nadobro.services.points_service import (
    parse_lowiq_points_reply,
    claim_pending_request,
    build_dashboard_payload,
    save_points_snapshot,
)

logger = logging.getLogger(__name__)


async def lowiqpts_bridge_reply_handler(update, context):
    message = update.effective_message
    if not message:
        return
    from_user = getattr(message, "from_user", None)
    if not from_user or not getattr(from_user, "is_bot", False):
        return
    if (getattr(from_user, "username", "") or "").lower() != "lowiqpts_bot":
        return

    text = message.text or message.caption or ""
    parsed = parse_lowiq_points_reply(text)
    req = claim_pending_request(context.application.bot_data, text)
    if not req:
        return
    chat_id = int(req.get("chat_id"))
    telegram_id = int(req.get("telegram_id"))
    if not parsed:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "⚠️ Could not parse points response from bridge bot.\n"
                "Try again with `🏆 Nado Points`."
            ),
        )
        return

    payload = build_dashboard_payload(parsed)
    save_points_snapshot(telegram_id, payload)
    await context.bot.send_message(
        chat_id=chat_id,
        text=fmt_points_dashboard(payload),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=points_scope_kb("week"),
    )

    if bool(payload.get("no_activity")):
        return

    mascot_path = mascot_path_for_cost(float(payload.get("cost_per_point", 0) or 0))
    if mascot_path:
        try:
            with open(mascot_path, "rb") as img:
                await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=img,
                    caption=mascot_caption_for_cost(float(payload.get("cost_per_point", 0) or 0)),
                )
        except Exception:
            logger.warning("Failed to send points mascot image", exc_info=True)
