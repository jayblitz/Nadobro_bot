import logging
from telegram import Update
from telegram.ext import CallbackContext
from src.nadobro.services.user_service import get_or_create_user
from src.nadobro.handlers.commands import _get_webapp_keyboard

logger = logging.getLogger(__name__)


async def handle_message(update: Update, context: CallbackContext):
    if not update.message or not update.message.text:
        return

    telegram_id = update.effective_user.id
    username = update.effective_user.username

    get_or_create_user(telegram_id, username)

    await update.message.reply_text(
        "Tap the button below to open the Nadobro trading app:",
        reply_markup=_get_webapp_keyboard(),
    )
