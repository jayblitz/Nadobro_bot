from telegram.ext import CallbackContext
from src.nadobro.services.settings_service import get_user_settings


def get_cached_user_settings(telegram_id: int, context: CallbackContext) -> dict:
    network, settings = get_user_settings(telegram_id)
    context.user_data[f"settings:{network}"] = settings
    context.user_data["settings"] = settings
    return settings
