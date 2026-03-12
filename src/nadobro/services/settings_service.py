import json
from datetime import datetime

from src.nadobro.models.database import get_bot_state_raw, set_bot_state
from src.nadobro.services.user_service import get_user

SETTINGS_PREFIX = "user_settings:"


def _settings_key(telegram_id: int, network: str) -> str:
    return f"{SETTINGS_PREFIX}{telegram_id}:{network}"


def _default_strategy_settings() -> dict:
    return {
        "mm": {
            "notional_usd": 75.0, "spread_bp": 4.0, "interval_seconds": 45, "tp_pct": 0.6, "sl_pct": 0.5,
            "levels": 2, "threshold_bp": 12.0, "close_offset_bp": 24.0,
            "reference_mode": "ema_fast",
            "ema_fast_alpha": 0.45,
            "ema_slow_alpha": 0.20,
            "vol_window_points": 12,
            "vol_sensitivity": 0.02,
            "min_spread_bp": 2.0,
            "max_spread_bp": 20.0,
            "quote_ttl_seconds": 90,
            "directional_bias": "neutral",
            "inventory_soft_limit_usd": 45.0,
            "cycle_notional_usd": 75.0,
            "session_notional_cap_usd": 0.0,
        },
        "grid": {
            "notional_usd": 100.0, "spread_bp": 10.0, "interval_seconds": 60, "tp_pct": 1.2, "sl_pct": 0.8,
            "levels": 4, "min_range_pct": 1.0, "max_range_pct": 1.0,
        },
        "dn": {
            "notional_usd": 50.0, "spread_bp": 3.0, "interval_seconds": 90, "tp_pct": 0.8, "sl_pct": 0.6,
            "auto_close_on_maintenance": 1.0,
        },
        "vol": {"notional_usd": 100.0, "spread_bp": 6.0, "interval_seconds": 30, "tp_pct": 0.8, "sl_pct": 0.6},
        "bro": {
            "budget_usd": 500.0,
            "risk_level": "balanced",
            "max_positions": 3,
            "cycle_seconds": 300,
            "tp_pct": 2.0,
            "sl_pct": 1.5,
            "max_loss_pct": 15.0,
            "leverage_cap": 5,
            "products": ["BTC", "ETH", "SOL"],
            "use_sentiment": True,
            "use_cmc": True,
            "min_confidence": 0.65,
            "howl_enabled": True,
            "howl_hour_utc": 2,
        },
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
    network = user.network_mode.value if user else "mainnet"
    key = _settings_key(telegram_id, network)
    settings = _default_settings()
    raw = get_bot_state_raw(key)
    if raw:
        try:
            loaded = json.loads(raw)
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
    set_bot_state(key, settings)


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
