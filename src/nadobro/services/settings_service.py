import json
from datetime import datetime

from src.nadobro.models.database import get_bot_state_raw, set_bot_state
from src.nadobro.services.strategy_registry import (
    MARKET_MAKING_STRATEGIES,
    normalize_strategy_id,
    settings_strategy_defaults,
)
from src.nadobro.services.user_service import get_user

SETTINGS_PREFIX = "user_settings:"


def _settings_key(telegram_id: int, network: str) -> str:
    return f"{SETTINGS_PREFIX}{telegram_id}:{network}"


def _default_strategy_settings() -> dict:
    return settings_strategy_defaults()


def _default_settings() -> dict:
    return {
        "default_leverage": 3.0,
        "slippage": 1.0,
        "risk_profile": "balanced",
        "strategies": _default_strategy_settings(),
    }


def _looks_like_rgrid_config(cfg: dict) -> bool:
    if not isinstance(cfg, dict):
        return False
    if any(k.startswith("rgrid_") for k in cfg.keys()):
        return True
    legacy_keys = {"grid_spread_bp", "grid_stop_loss_pct", "grid_take_profit_pct", "grid_reset_threshold_pct", "grid_discretion"}
    return any(k in cfg for k in legacy_keys)


def _looks_like_grid_config(cfg: dict) -> bool:
    if not isinstance(cfg, dict):
        return False
    marker_keys = {"threshold_bp", "close_offset_bp", "reference_mode", "directional_bias", "min_spread_bp", "max_spread_bp"}
    return any(k in cfg for k in marker_keys)


def _normalize_strategy_id(strategy: str) -> str:
    return normalize_strategy_id(strategy)


def _migrate_loaded_strategies(loaded_strats: dict) -> dict:
    migrated = dict(loaded_strats or {})
    mm_cfg = migrated.get("mm") if isinstance(migrated.get("mm"), dict) else None
    grid_cfg = migrated.get("grid") if isinstance(migrated.get("grid"), dict) else None
    rgrid_cfg = migrated.get("rgrid") if isinstance(migrated.get("rgrid"), dict) else None

    # Requested migration policy:
    # - mm -> grid
    # - legacy grid (reverse-grid payload) -> rgrid
    if mm_cfg is not None:
        migrated["grid"] = dict(mm_cfg)
    if rgrid_cfg is None and grid_cfg is not None:
        # Legacy "grid" strategy payloads were reverse-grid style.
        if _looks_like_rgrid_config(grid_cfg):
            migrated["rgrid"] = dict(grid_cfg)
            if mm_cfg is None and "grid" in migrated:
                migrated.pop("grid", None)

    migrated.pop("mm", None)
    return migrated


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
                loaded_strats = _migrate_loaded_strategies(loaded.get("strategies", {}))
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


def sync_cycle_notional_with_margin(strategies: dict, strategy_id: str) -> None:
    """When margin (notional_usd) changes, keep per-cycle budget aligned for MM/Grid."""
    strategy_id = _normalize_strategy_id(strategy_id)
    if strategy_id not in MARKET_MAKING_STRATEGIES:
        return
    cfg = strategies.get(strategy_id)
    if not isinstance(cfg, dict):
        return
    if "notional_usd" not in cfg:
        return
    try:
        n = float(cfg["notional_usd"])
    except (TypeError, ValueError):
        return
    cfg["notional_usd"] = n
    cfg["cycle_notional_usd"] = n


def get_strategy_settings(telegram_id: int, strategy: str) -> tuple[str, dict]:
    network, settings = get_user_settings(telegram_id)
    strategies = settings.get("strategies", {})
    strategy = _normalize_strategy_id(strategy)
    strat = strategies.get(strategy, _default_strategy_settings().get(strategy, {}))
    return network, strat
