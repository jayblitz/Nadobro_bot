"""Canonical strategy identity and defaults.

Keep aliases, display names, supported ids, and default payloads in one place so
runtime, settings, and managed-agent surfaces do not drift.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Mapping


STRATEGY_GRID = "grid"
STRATEGY_RGRID = "rgrid"
STRATEGY_DGRID = "dgrid"
STRATEGY_DN = "dn"
STRATEGY_VOL = "vol"
STRATEGY_BRO = "bro"

SUPPORTED_STRATEGIES: tuple[str, ...] = (
    STRATEGY_GRID,
    STRATEGY_RGRID,
    STRATEGY_DGRID,
    STRATEGY_DN,
    STRATEGY_VOL,
    STRATEGY_BRO,
)

MARKET_MAKING_STRATEGIES: tuple[str, ...] = (STRATEGY_GRID, STRATEGY_RGRID, STRATEGY_DGRID)

STRATEGY_ALIASES: Mapping[str, str] = {
    "mm": STRATEGY_GRID,
    "market_making": STRATEGY_GRID,
    "market-making": STRATEGY_GRID,
    "reverse_grid": STRATEGY_RGRID,
    "reverse-grid": STRATEGY_RGRID,
    "r-grid": STRATEGY_RGRID,
    "dynamic_grid": STRATEGY_DGRID,
    "dynamic-grid": STRATEGY_DGRID,
    "d-grid": STRATEGY_DGRID,
    "delta-neutral": STRATEGY_DN,
    "delta_neutral": STRATEGY_DN,
    "volume": STRATEGY_VOL,
    "volume_bot": STRATEGY_VOL,
    "vol-bot": STRATEGY_VOL,
    "alpha": STRATEGY_BRO,
    "alpha_agent": STRATEGY_BRO,
    "bro_mode": STRATEGY_BRO,
}

STRATEGY_DISPLAY_NAMES: Mapping[str, str] = {
    STRATEGY_GRID: "GRID",
    STRATEGY_RGRID: "REVERSE GRID",
    STRATEGY_DGRID: "DYNAMIC GRID",
    STRATEGY_DN: "DELTA NEUTRAL",
    STRATEGY_VOL: "VOLUME",
    STRATEGY_BRO: "BRO MODE",
}


@dataclass(frozen=True)
class StrategyPhrase:
    strategy_id: str
    phrases: tuple[str, ...]


NATURAL_LANGUAGE_STRATEGY_PHRASES: tuple[StrategyPhrase, ...] = (
    StrategyPhrase(STRATEGY_BRO, ("alpha agent", "bro mode", "bro strategy", "alpha mode")),
    StrategyPhrase(STRATEGY_DN, ("delta neutral", "delta-neutral", " dn ")),
    StrategyPhrase(STRATEGY_VOL, ("volume bot", "vol bot", " vol ")),
    StrategyPhrase(STRATEGY_RGRID, ("r-grid", "rgrid", "reverse grid", "reverse-grid")),
    StrategyPhrase(STRATEGY_DGRID, ("dynamic grid", "dgrid", "d-grid")),
    StrategyPhrase(STRATEGY_GRID, ("grid",)),
)


SETTINGS_STRATEGY_DEFAULTS: Mapping[str, dict] = {
    "grid": {
        "notional_usd": 75.0, "spread_bp": 4.0, "interval_seconds": 45, "tp_pct": 0.6, "sl_pct": 0.5,
        "levels": 2, "threshold_bp": 0.0, "close_offset_bp": 24.0,
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
        "grid_reset_threshold_pct": 0.8,
        "grid_reset_timeout_seconds": 120,
    },
    "rgrid": {
        "notional_usd": 100.0, "spread_bp": 10.0, "rgrid_spread_bp": 10.0, "interval_seconds": 60,
        "tp_pct": 1.2, "sl_pct": 0.8, "rgrid_stop_loss_pct": 0.8, "rgrid_take_profit_pct": 1.2,
        "levels": 4, "rgrid_discretion": 0.06,
        "rgrid_reset_threshold_pct": 1.0, "rgrid_reset_timeout_seconds": 120,
        # Legacy keys kept for one migration cycle.
        "min_range_pct": 1.0, "max_range_pct": 1.0,
    },
    "dgrid": {
        "notional_usd": 100.0, "cycle_notional_usd": 100.0, "spread_bp": 8.0,
        "interval_seconds": 30, "tp_pct": 1.2, "sl_pct": 0.8, "levels": 4,
        "dgrid_trend_on_variance_ratio": 1.25,
        "dgrid_range_on_variance_ratio": 1.15,
        "dgrid_min_spread_bp": 2.0,
        "dgrid_max_spread_bp": 50.0,
        "dgrid_short_window_points": 4,
        "dgrid_long_window_points": 12,
        "rgrid_stop_loss_pct": 0.8,
        "rgrid_take_profit_pct": 1.2,
        "rgrid_discretion": 0.06,
        "grid_reset_threshold_pct": 0.2,
        "rgrid_reset_threshold_pct": 0.2,
        "grid_reset_timeout_seconds": 120,
        "rgrid_reset_timeout_seconds": 120,
    },
    "dn": {
        "notional_usd": 50.0, "spread_bp": 3.0, "interval_seconds": 90, "tp_pct": 0.8, "sl_pct": 0.6,
        "auto_close_on_maintenance": 1.0,
        "funding_entry_mode": "wait",
    },
    "vol": {
        "notional_usd": 100.0,
        "fixed_margin_usd": 100.0,
        "target_volume_usd": 10000.0,
        "interval_seconds": 10,
        "tp_pct": 1.0,
        "sl_pct": 1.0,
        "vol_direction": "long",
        "vol_signal_filter_enabled": True,
        "vol_trade_exits_enabled": True,
        "vol_direction_mode": "signal",
        "vol_trade_tp_pct": 0.4,
        "vol_trade_sl_pct": 0.2,
        "vol_hold_min_seconds": 60,
        "vol_hold_max_seconds": 540,
        "vol_ema_len": 50,
        "vol_rsi_len": 14,
        "vol_rsi_long_max": 50.0,
        "vol_rsi_short_min": 50.0,
        "vol_max_spread_bp": 12.0,
        "vol_min_edge_bp": 4.0,
    },
    "bro": {
        "budget_usd": 500.0,
        "risk_level": "balanced",
        "max_positions": 3,
        "cycle_seconds": 300,
        "tp_pct": 2.0,
        "sl_pct": 1.5,
        "max_loss_pct": 15.0,
        "leverage_cap": 5,
        "min_reward_risk": 1.2,
        "products": ["BTC", "ETH", "SOL"],
        "use_sentiment": True,
        "use_cmc": True,
        "min_confidence": 0.65,
        "howl_enabled": True,
        "howl_hour_utc": 2,
    },
}

RUNTIME_STRATEGY_DEFAULTS: Mapping[str, dict] = {
    "grid": {
        "notional_usd": 400.0,
        "cycle_notional_usd": 400.0,
        "spread_bp": 4.0,
        "interval_seconds": 45,
        "threshold_bp": 12.0,
        "close_offset_bp": 24.0,
    },
    "rgrid": {
        "notional_usd": 100.0,
        "spread_bp": 10.0,
        "rgrid_spread_bp": 10.0,
        "interval_seconds": 60,
        "levels": 4,
        "rgrid_stop_loss_pct": 0.8,
        "rgrid_take_profit_pct": 1.2,
        "rgrid_discretion": 0.06,
        "rgrid_reset_threshold_pct": 1.0,
        "rgrid_reset_timeout_seconds": 120,
        # Legacy fallback (read-only compatibility).
        "min_range_pct": 1.0,
        "max_range_pct": 1.0,
    },
    "dgrid": {
        "notional_usd": 100.0,
        "cycle_notional_usd": 100.0,
        "spread_bp": 8.0,
        "interval_seconds": 30,
        "levels": 4,
        "tp_pct": 1.2,
        "sl_pct": 0.8,
        "dgrid_trend_on_variance_ratio": 1.25,
        "dgrid_range_on_variance_ratio": 1.15,
        "dgrid_min_spread_bp": 2.0,
        "dgrid_max_spread_bp": 50.0,
        "dgrid_short_window_points": 4,
        "dgrid_long_window_points": 12,
        "rgrid_stop_loss_pct": 0.8,
        "rgrid_take_profit_pct": 1.2,
        "rgrid_discretion": 0.06,
        "grid_reset_threshold_pct": 0.2,
        "rgrid_reset_threshold_pct": 0.2,
        "grid_reset_timeout_seconds": 120,
        "rgrid_reset_timeout_seconds": 120,
    },
    "dn": {
        "notional_usd": 50.0,
        "spread_bp": 3.0,
        "interval_seconds": 90,
        "auto_close_on_maintenance": 1.0,
        "funding_entry_mode": "wait",
    },
    "vol": {
        "notional_usd": 100.0,
        "fixed_margin_usd": 100.0,
        "target_volume_usd": 10000.0,
        "leverage": 1.0,
        "interval_seconds": 10,
        "vol_direction": "long",
        "vol_signal_filter_enabled": True,
        "vol_trade_exits_enabled": True,
        "vol_direction_mode": "signal",
        "vol_trade_tp_pct": 0.4,
        "vol_trade_sl_pct": 0.2,
        "vol_hold_min_seconds": 60,
        "vol_hold_max_seconds": 540,
        "vol_ema_len": 50,
        "vol_rsi_len": 14,
        "vol_rsi_long_max": 50.0,
        "vol_rsi_short_min": 50.0,
        "vol_max_spread_bp": 12.0,
        "vol_min_edge_bp": 4.0,
        "tp_pct": 1.0,
        "sl_pct": 1.0,
    },
    "bro": {
        "budget_usd": 500.0, "risk_level": "balanced", "max_positions": 3,
        "interval_seconds": 300, "cycle_seconds": 300,
        "tp_pct": 2.0, "sl_pct": 1.5, "max_loss_pct": 15.0,
        "leverage_cap": 5,
        "min_reward_risk": 1.2,
        "use_sentiment": True, "use_cmc": True, "min_confidence": 0.65,
        "howl_enabled": True, "howl_hour_utc": 2,
    },
}


def normalize_strategy_id(strategy: str | None) -> str:
    sid = str(strategy or "").lower().strip()
    return STRATEGY_ALIASES.get(sid, sid)


def is_supported_strategy(strategy: str | None) -> bool:
    return normalize_strategy_id(strategy) in SUPPORTED_STRATEGIES


def strategy_display_name(strategy: str | None) -> str:
    sid = normalize_strategy_id(strategy)
    return STRATEGY_DISPLAY_NAMES.get(sid, sid.upper() if sid else "STRATEGY")


def infer_strategy_from_text(text: str | None) -> str | None:
    t = f" {(text or '').lower()} "
    for spec in NATURAL_LANGUAGE_STRATEGY_PHRASES:
        if any(phrase in t for phrase in spec.phrases):
            return spec.strategy_id
    return None


def migrate_state_strategy(state: dict) -> dict:
    if not isinstance(state, dict):
        return state
    sid = normalize_strategy_id(str(state.get("strategy") or ""))
    if sid:
        state["strategy"] = sid
    if sid in SUPPORTED_STRATEGIES:
        state["strategy_id_v2"] = int(state.get("strategy_id_v2") or 1)
    return state


def settings_strategy_defaults() -> dict:
    return deepcopy(dict(SETTINGS_STRATEGY_DEFAULTS))


def runtime_strategy_defaults(default_bro_products: list[str] | tuple[str, ...] | None = None) -> dict:
    defaults = deepcopy(dict(RUNTIME_STRATEGY_DEFAULTS))
    if default_bro_products:
        defaults.setdefault("bro", {})["products"] = list(default_bro_products)
    return defaults


def runtime_strategy_default(strategy: str, default_bro_products: list[str] | tuple[str, ...] | None = None) -> dict:
    sid = normalize_strategy_id(strategy)
    defaults = runtime_strategy_defaults(default_bro_products)
    return deepcopy(defaults.get(sid, {"notional_usd": 100.0, "spread_bp": 5.0, "interval_seconds": 60}))
