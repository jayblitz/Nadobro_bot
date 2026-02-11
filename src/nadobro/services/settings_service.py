import json
from datetime import datetime

from src.nadobro.models.database import BotState, get_session
from src.nadobro.services.user_service import get_user

SETTINGS_PREFIX = "user_settings:"


def _settings_key(telegram_id: int, network: str) -> str:
    return f"{SETTINGS_PREFIX}{telegram_id}:{network}"


def _default_strategy_settings() -> dict:
    return {
        "mm": {"notional_usd": 75.0, "spread_bp": 4.0, "interval_seconds": 45, "tp_pct": 0.6, "sl_pct": 0.5},
        "grid": {"notional_usd": 100.0, "spread_bp": 10.0, "interval_seconds": 60, "tp_pct": 1.2, "sl_pct": 0.8},
        "dn": {"notional_usd": 50.0, "spread_bp": 3.0, "interval_seconds": 90, "tp_pct": 0.8, "sl_pct": 0.6},
    }


def _default_settings() -> dict:
    return {
        "default_leverage": 3.0,
        "slippage": 1.0,
        "risk_profile": "balanced",
        "strategies": _default_strategy_settings(),
    }


def get_user_settings(telegram_id: int) -> tuple[str, dict]:
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "testnet"
    key = _settings_key(telegram_id, network)
    settings = _default_settings()

    with get_session() as session:
        row = session.query(BotState).filter_by(key=key).first()
        if row and row.value:
            try:
                loaded = json.loads(row.value)
                if isinstance(loaded, dict):
                    settings.update(loaded)
                    default_strats = _default_strategy_settings()
                    loaded_strats = loaded.get("strategies", {})
                    if isinstance(loaded_strats, dict):
                        for sid, base in default_strats.items():
                            if sid in loaded_strats and isinstance(loaded_strats[sid], dict):
                                base.update(loaded_strats[sid])
                        settings["strategies"] = default_strats
            except Exception:
                pass

    return network, settings


def save_user_settings(telegram_id: int, network: str, settings: dict):
    key = _settings_key(telegram_id, network)
    with get_session() as session:
        row = session.query(BotState).filter_by(key=key).first()
        payload = json.dumps(settings)
        if row:
            row.value = payload
            row.updated_at = datetime.utcnow()
        else:
            row = BotState(key=key, value=payload)
            session.add(row)
        session.commit()


def update_user_settings(telegram_id: int, mutator):
    network, settings = get_user_settings(telegram_id)
    mutator(settings)
    save_user_settings(telegram_id, network, settings)
    return network, settings


def get_strategy_settings(telegram_id: int, strategy: str) -> tuple[str, dict]:
    network, settings = get_user_settings(telegram_id)
    strategies = settings.get("strategies", {})
    strat = strategies.get(strategy, _default_strategy_settings().get(strategy, {}))
    return network, strat
