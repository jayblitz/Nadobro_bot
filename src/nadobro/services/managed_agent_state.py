import os
from datetime import datetime

from src.nadobro.services.settings_service import get_user_settings, update_user_settings


def _default_state() -> dict:
    return {
        "enabled": False,
        "updated_at": None,
    }


def is_managed_agent_globally_enabled() -> bool:
    raw = (os.environ.get("NADO_MANAGED_AGENT_ENABLED", "true") or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def get_managed_agent_state(telegram_id: int) -> dict:
    _, settings = get_user_settings(telegram_id)
    state = settings.get("managed_agent")
    if not isinstance(state, dict):
        return _default_state()
    merged = _default_state()
    merged.update(state)
    return merged


def is_managed_agent_enabled(telegram_id: int) -> bool:
    if not is_managed_agent_globally_enabled():
        return False
    return bool(get_managed_agent_state(telegram_id).get("enabled"))


def set_managed_agent_enabled(telegram_id: int, enabled: bool) -> dict:
    now_iso = datetime.utcnow().isoformat()

    def _mutator(settings: dict) -> None:
        state = settings.get("managed_agent")
        if not isinstance(state, dict):
            state = _default_state()
        state["enabled"] = bool(enabled)
        state["updated_at"] = now_iso
        settings["managed_agent"] = state

    _, settings = update_user_settings(telegram_id, _mutator)
    state = settings.get("managed_agent")
    if not isinstance(state, dict):
        return _default_state()
    return state
