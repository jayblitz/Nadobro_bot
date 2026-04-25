import asyncio
import json
import logging
import os
import time
from datetime import datetime

# Keys that must never be overwritten from persisted strategy *settings* into live runtime state.
_STRATEGY_SETTINGS_RUNTIME_BLOCKLIST = frozenset({
    "running", "strategy", "strategy_id_v2", "product", "last_run_ts", "runs", "started_at",
    "reference_price", "last_error", "error_streak", "last_action", "last_action_detail",
    "strategy_session_id", "bro_state", "mm_paused", "mm_pause_reason", "mm_last_metrics",
    "worker_group", "worker_last_heartbeat", "last_dispatch_ts", "last_cycle_ms",
    "last_cycle_result", "worker_pid", "grid_anchor_price", "grid_buy_exposure_price",
    "grid_sell_exposure_price", "grid_drift_from_anchor_pct", "grid_reset_active",
    "grid_reset_side", "grid_last_cycle_pnl_usd", "dn_last_funding_rate", "dn_unfavorable_count",
    "dn_mode", "order_observability",
    "last_error_category", "vol_order_attempts", "vol_order_failures", "last_order_error", "last_order_ts",
    "vol_market",
    # VOL perp direction is chosen at start (Start Long / Start Short). Saved dashboard
    # `vol_direction` must not overwrite live runtime each tick — that caused opposite-side
    # entries when UI showed one direction and stale settings said another.
    "vol_direction",
})

from src.nadobro.config import (
    get_dn_pair,
    get_dn_products,
    get_product_id,
    get_product_max_leverage,
    get_spot_product_id,
    get_spot_metadata,
    get_perp_products,
    list_volume_spot_product_names,
    normalize_volume_spot_symbol,
)
from src.nadobro.models.database import (
    get_bot_state_raw, set_bot_state,
    insert_strategy_session, update_strategy_session, increment_session_metrics,
    get_running_strategy_sessions,
)
from src.nadobro.db import query_all
from src.nadobro.services.admin_service import is_trading_paused
from src.nadobro.services.settings_service import get_strategy_settings
from src.nadobro.services.trade_service import (
    close_all_positions,
    close_delta_neutral_legs,
    get_trade_analytics,
    stop_volume_spot_cleanup,
)
from src.nadobro.services.user_service import (
    get_user_nado_client,
    get_user_readonly_client,
    get_user,
    run_strategy_start_preflight,
)
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.perf import timed_metric, record_metric
from src.nadobro.services.execution_queue import enqueue_strategy

logger = logging.getLogger(__name__)

STATE_PREFIX = "strategy_bot:"
RUNTIME_TICK_SECONDS = 20


def _strategy_cycle_timeout_seconds() -> float | None:
    """Wall-clock cap for one strategy cycle (single-process path). 0 or unset disables."""
    raw = (os.environ.get("NADO_STRATEGY_CYCLE_TIMEOUT_SECONDS") or "180").strip()
    try:
        v = float(raw)
    except ValueError:
        return 180.0
    if v <= 0:
        return None
    return v


def _vol_use_multiprocess() -> bool:
    raw = (os.environ.get("NADO_VOL_USE_MULTIPROCESS") or "false").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _strategy_use_multiprocess(strategy: str) -> bool:
    strategy_key = str(strategy or "").lower().strip()
    specific = (os.environ.get(f"NADO_{strategy_key.upper()}_USE_MULTIPROCESS") or "").strip().lower()
    if specific in ("1", "true", "yes", "on"):
        return True
    if specific in ("0", "false", "no", "off"):
        return False
    if strategy_key == "vol":
        return _vol_use_multiprocess()
    raw = (os.environ.get("NADO_USE_MULTIPROCESS_STRATEGIES") or "false").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _vol_call_timeout_seconds() -> float:
    raw = (os.environ.get("NADO_VOL_CALL_TIMEOUT_SECONDS") or "12").strip()
    try:
        val = float(raw)
    except Exception:
        return 12.0
    return max(3.0, val)


MAX_OPEN_ORDERS_PER_PRODUCT = 6
STRATEGY_ERROR_ALERT_STREAK = 3

_bot_app = None
_runtime_loop: asyncio.AbstractEventLoop | None = None
_tasks: dict[str, asyncio.Task] = {}
_job_locks: dict[str, asyncio.Lock] = {}
_job_pending_payloads: dict[str, dict] = {}
_job_coalesce_counts: dict[str, int] = {}
_job_stats: dict[str, int] = {
    "cycles_started": 0,
    "cycles_ok": 0,
    "cycles_failed": 0,
    "coalesced_ticks": 0,
    "deferred_cycles": 0,
    "cycle_timeouts": 0,
    "vol_coalesced_ticks": 0,
    "vol_deferred_cycles": 0,
    "vol_overlap_skips": 0,
}
_process_worker_mode = False
_CYCLE_SKIP_MARKERS = frozenset({"maintenance_pause", "skipped_interval"})


def _normalize_strategy_id(strategy: str) -> str:
    sid = str(strategy or "").lower().strip()
    if sid == "mm":
        return "grid"
    if sid in ("reverse_grid", "reverse-grid"):
        return "rgrid"
    return sid


def _migrate_state_strategy(state: dict) -> dict:
    if not isinstance(state, dict):
        return state
    sid = str(state.get("strategy") or "").lower().strip()
    if sid == "mm":
        state["strategy"] = "grid"
        state["strategy_id_v2"] = 1
    elif sid in ("grid", "rgrid", "dn", "vol", "bro"):
        state["strategy_id_v2"] = int(state.get("strategy_id_v2") or 1)
    return state


def _safe_last_run_ts(raw) -> float:
    """Parse last_run_ts from JSON state (float, ISO string, or datetime)."""
    if raw is None:
        return 0.0
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, datetime):
        try:
            return float(raw.timestamp())
        except Exception:
            return 0.0
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return 0.0
        try:
            return float(s)
        except ValueError:
            pass
        try:
            iso = s.replace("Z", "+00:00") if s.endswith("Z") else s
            return float(datetime.fromisoformat(iso).timestamp())
        except Exception:
            return 0.0
    return 0.0


def _strategy_display_name(strategy: str) -> str:
    sid = _normalize_strategy_id(strategy)
    if sid == "grid":
        return "GRID"
    if sid == "rgrid":
        return "REVERSE GRID"
    return sid.upper() if sid else "STRATEGY"


def _update_order_observability(state: dict, result: dict) -> None:
    obs = state.get("order_observability")
    if not isinstance(obs, dict):
        obs = {
            "cycles": 0,
            "ok_cycles": 0,
            "failed_cycles": 0,
            "cycles_with_orders": 0,
            "zero_order_cycles": 0,
            "orders_placed": 0,
            "orders_filled": 0,
            "orders_cancelled": 0,
            "last_action": "",
            "last_reason": "",
            "last_ts": 0.0,
        }

    orders_placed = int(result.get("orders_placed", 0) or 0)
    orders_filled = int(result.get("orders_filled", 0) or 0)
    orders_cancelled = int(result.get("orders_cancelled", 0) or 0)
    success = bool(result.get("success", True))

    obs["cycles"] = int(obs.get("cycles", 0) or 0) + 1
    if success:
        obs["ok_cycles"] = int(obs.get("ok_cycles", 0) or 0) + 1
    else:
        obs["failed_cycles"] = int(obs.get("failed_cycles", 0) or 0) + 1

    if orders_placed > 0:
        obs["cycles_with_orders"] = int(obs.get("cycles_with_orders", 0) or 0) + 1
    else:
        obs["zero_order_cycles"] = int(obs.get("zero_order_cycles", 0) or 0) + 1

    obs["orders_placed"] = int(obs.get("orders_placed", 0) or 0) + orders_placed
    obs["orders_filled"] = int(obs.get("orders_filled", 0) or 0) + orders_filled
    obs["orders_cancelled"] = int(obs.get("orders_cancelled", 0) or 0) + orders_cancelled
    obs["last_action"] = str(result.get("action") or "")
    obs["last_reason"] = str(
        result.get("reason")
        or result.get("detail")
        or result.get("error")
        or result.get("order_error")
        or ""
    )[:200]
    obs["last_ts"] = time.time()
    state["order_observability"] = obs


def _categorize_runtime_error(error_msg: str) -> str:
    msg = str(error_msg or "").lower()
    if not msg or msg == "none":
        return "unknown"
    if "wallet not initialized" in msg or "wallet client unavailable" in msg:
        return "wallet_not_initialized"
    if "wallet not linked" in msg:
        return "wallet_not_linked"
    if "builder routing misconfigured" in msg:
        return "builder_misconfigured"
    if "market price" in msg:
        return "market_data"
    if "timed out" in msg:
        return "timeout"
    if "network mismatch" in msg or "mode switched" in msg:
        return "network_mismatch"
    return "execution"


def _merge_vol_order_counters(state: dict, result: dict) -> None:
    attempts = int(result.get("vol_order_attempts") or 0)
    failures = int(result.get("vol_order_failures") or 0)
    if attempts > 0:
        state["vol_order_attempts"] = int(state.get("vol_order_attempts") or 0) + attempts
        state["last_order_ts"] = time.time()
    if failures > 0:
        state["vol_order_failures"] = int(state.get("vol_order_failures") or 0) + failures
    last_order_error = str(result.get("last_order_error") or "").strip()
    if last_order_error:
        state["last_order_error"] = last_order_error[:220]
        state["last_error_category"] = _categorize_runtime_error(last_order_error)


def _format_cycle_failure_error(strategy: str, result: dict) -> str:
    raw = str(result.get("error") or result.get("order_error") or "unknown").strip()
    category = _categorize_runtime_error(raw)
    action = str(result.get("action") or "cycle")
    return f"{strategy.upper()}[{action}/{category}]: {raw}"[:300]


def _cycle_result_label(ok: bool, error_msg: str | None) -> str:
    if not ok:
        return "error"
    marker = str(error_msg or "").strip().lower()
    if marker in _CYCLE_SKIP_MARKERS:
        return "skipped"
    return "ok"


def set_bot_app(app):
    global _bot_app, _runtime_loop
    _bot_app = app
    try:
        _runtime_loop = asyncio.get_running_loop()
    except RuntimeError:
        # Called outside of event loop; _ensure_task will latch loop later.
        pass


def _task_key(telegram_id: int, network: str) -> str:
    return f"{telegram_id}:{network}"


def _state_key(telegram_id: int, network: str) -> str:
    return f"{STATE_PREFIX}{telegram_id}:{network}"


def _default_state() -> dict:
    return {
        "running": False,
        "strategy": None,
        "product": "BTC",
        "notional_usd": 100.0,
        "spread_bp": 5.0,
        "tp_pct": 1.0,
        "sl_pct": 0.5,
        "leverage": 3.0,
        "slippage_pct": 1.0,
        "interval_seconds": 60,
        "reference_price": 0.0,
        "started_at": None,
        "last_run_ts": 0.0,
        "last_error": None,
        "error_streak": 0,
        "runs": 0,
        "worker_group": None,
        "worker_last_heartbeat": 0.0,
        "last_dispatch_ts": 0.0,
        "last_cycle_ms": 0.0,
        "last_cycle_result": "",
        "worker_pid": None,
        "last_error_category": "",
        "vol_order_attempts": 0,
        "vol_order_failures": 0,
        "last_order_error": "",
        "last_order_ts": 0.0,
    }


def _load_state(telegram_id: int, network: str) -> dict:
    raw = get_bot_state_raw(_state_key(telegram_id, network))
    if not raw:
        return _default_state()
    try:
        loaded = json.loads(raw)
        state = _default_state()
        state.update(loaded if isinstance(loaded, dict) else {})
        _migrate_state_strategy(state)
        return state
    except Exception:
        logger.warning("Invalid bot state JSON for user %s", telegram_id)
        return _default_state()


def _save_state(telegram_id: int, network: str, state: dict):
    set_bot_state(_state_key(telegram_id, network), state)


async def _notify(telegram_id: int, text: str, **fmt_kwargs):
    if not _bot_app:
        return
    try:
        from src.nadobro.i18n import language_context, get_user_language, localize_text, get_active_language
        with language_context(get_user_language(telegram_id)):
            lang = get_active_language()
            translated = localize_text(text, lang)
            if fmt_kwargs:
                translated = translated.format(**fmt_kwargs)
            await _bot_app.bot.send_message(chat_id=telegram_id, text=translated)
    except Exception as e:
        logger.warning("Notify failed for %s: %s", telegram_id, e)


def _strategy_defaults(strategy: str) -> dict:
    strategy = _normalize_strategy_id(strategy)
    default_bro_products = get_perp_products()[:6] or ["BTC", "ETH", "SOL"]
    presets = {
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
        "dn": {
            "notional_usd": 50.0,
            "spread_bp": 3.0,
            "interval_seconds": 90,
            "auto_close_on_maintenance": 1.0,
            "funding_entry_mode": "enter_anyway",
        },
        "vol": {
            "notional_usd": 100.0,
            "fixed_margin_usd": 100.0,
            "target_volume_usd": 10000.0,
            "leverage": 1.0,
            "interval_seconds": 10,
            "vol_direction": "long",
            "tp_pct": 1.0,
            "sl_pct": 1.0,
        },
        "bro": {
            "budget_usd": 500.0, "risk_level": "balanced", "max_positions": 3,
            "interval_seconds": 300, "cycle_seconds": 300,
            "tp_pct": 2.0, "sl_pct": 1.5, "max_loss_pct": 15.0,
            "leverage_cap": 5, "products": default_bro_products,
            "use_sentiment": True, "use_cmc": True, "min_confidence": 0.65,
            "howl_enabled": True, "howl_hour_utc": 2,
        },
    }
    return presets.get(strategy, {"notional_usd": 100.0, "spread_bp": 5.0, "interval_seconds": 60})


SUPPORTED_STRATEGIES = ("grid", "rgrid", "dn", "vol", "bro")


def _mark_previous_sessions_superseded(telegram_id: int, network: str) -> None:
    """Stop duplicate 'running' DB rows when user starts a new strategy on the same network."""
    try:
        rows = get_running_strategy_sessions(telegram_id, network)
        for row in rows:
            sid = row.get("id")
            if not sid:
                continue
            update_strategy_session(int(sid), {
                "status": "stopped",
                "stopped_at": datetime.utcnow().isoformat(),
                "stop_reason": "superseded_by_new_strategy",
            })
    except Exception as e:
        logger.warning("Could not supersede old sessions for user %s: %s", telegram_id, e)


def _create_session(telegram_id: int, strategy: str, product: str, network: str, state: dict) -> int | None:
    """Create a strategy_sessions row and return the session_id."""
    try:
        session_id = insert_strategy_session({
            "user_id": telegram_id,
            "strategy": strategy,
            "product_name": product,
            "product_id": get_product_id(product, network=network) if product != "MULTI" else None,
            "network": network,
            "config_snapshot": json.dumps({
                k: v for k, v in state.items()
                if k in (
                    "notional_usd", "cycle_notional_usd", "spread_bp", "leverage",
                    "slippage_pct", "interval_seconds", "tp_pct", "sl_pct", "levels",
                    "budget_usd", "risk_level", "max_positions", "products",
                    "rgrid_spread_bp", "rgrid_stop_loss_pct", "rgrid_take_profit_pct",
                    "rgrid_discretion", "rgrid_reset_threshold_pct",
                    "target_volume_usd", "funding_entry_mode", "fixed_margin_usd", "vol_direction",
                )
            }),
        })
        if session_id:
            logger.info("Created strategy session #%s for user %s (%s/%s)", session_id, telegram_id, strategy, network)
        return session_id
    except Exception as e:
        logger.warning("Failed to create strategy session for user %s: %s", telegram_id, e)
        return None


def _finalize_session(state: dict, stop_reason: str = "stopped"):
    """Mark the strategy session as completed/stopped."""
    session_id = state.get("strategy_session_id")
    if not session_id:
        return
    try:
        update_strategy_session(int(session_id), {
            "status": "completed" if stop_reason in ("tp_hit", "target_reached", "target_volume_hit") else "stopped",
            "stopped_at": datetime.utcnow().isoformat(),
            "stop_reason": str(stop_reason)[:200],
        })
    except Exception as e:
        logger.warning("Failed to finalize session #%s: %s", session_id, e)


def _available_quote_balance_for_network(client) -> float:
    try:
        bal = client.get_balance() or {}
        balances = bal.get("balances", {}) or {}
        return float(balances.get(0, balances.get("0", 0.0)) or 0.0)
    except Exception:
        return 0.0


def _active_position_size_for_product(client, product_id: int) -> float:
    try:
        for pos in client.get_all_positions() or []:
            if int(pos.get("product_id", -1)) != int(product_id):
                continue
            signed_amount = float(pos.get("signed_amount", 0) or 0.0)
            amount = abs(float(pos.get("amount", 0) or 0.0))
            size = abs(signed_amount) if abs(signed_amount) > 0 else amount
            if size > 1e-9:
                return size
    except Exception:
        return 0.0
    return 0.0


def _run_mm_start_guard(telegram_id: int, network: str, product: str, leverage: float, state: dict) -> tuple[bool, str]:
    client = get_user_readonly_client(telegram_id, network=network) or get_user_nado_client(telegram_id, network=network)
    if not client:
        return False, "Could not initialize market-maker account checks. Please retry."

    product_id = get_product_id(product, network=network, client=client)
    if product_id is None:
        return False, f"Unknown product '{product}'."

    existing_position = _active_position_size_for_product(client, product_id)
    if existing_position > 0:
        return (
            False,
            f"Close your existing {product.upper()}-PERP position before starting the MM bot. "
            f"Detected open size {existing_position:.6f}."
        )

    try:
        open_orders = client.get_open_orders(product_id) or []
    except Exception:
        open_orders = []
    if open_orders:
        return (
            False,
            f"Cancel existing {product.upper()}-PERP open orders before starting the MM bot. "
            f"Detected {len(open_orders)} open order(s)."
        )

    margin_usd = float(state.get("notional_usd") or 0.0)
    cycle_notional_cfg = float(state.get("cycle_notional_usd") or margin_usd or 0.0)
    cycle_notional = max(cycle_notional_cfg, margin_usd * max(1.0, leverage))
    inventory_soft_limit = float(
        state.get("inventory_soft_limit_usd")
        or (margin_usd * 0.60)
    )
    lev = max(1.0, float(leverage or 1.0))
    required_margin = margin_usd if margin_usd > 0 else (cycle_notional / lev if cycle_notional > 0 else 0.0)
    rebalance_buffer = inventory_soft_limit / lev if inventory_soft_limit > 0 else 0.0
    safety_buffer = max(5.0, required_margin * 0.20)
    recommended_available = required_margin + rebalance_buffer + safety_buffer
    available_quote = _available_quote_balance_for_network(client)
    if available_quote + 1e-9 < recommended_available:
        return (
            False,
            f"Insufficient margin buffer for MM on {product.upper()}-PERP. "
            f"Available ${available_quote:,.2f}, recommended about ${recommended_available:,.2f} "
            f"(trade margin ${required_margin:,.2f} + rebalance buffer ${rebalance_buffer:,.2f} + safety ${safety_buffer:,.2f}). "
            f"Reduce margin or deposit more funds."
        )

    return True, ""


def start_user_bot(
    telegram_id: int,
    strategy: str,
    product: str,
    leverage: float = 3.0,
    slippage_pct: float = 1.0,
    **kwargs,
) -> tuple[bool, str]:
    strategy = _normalize_strategy_id(strategy)
    if strategy not in SUPPORTED_STRATEGIES:
        return False, "Unknown strategy."
    try:
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "mainnet"
    except Exception:
        user = None
        network = "mainnet"

    if strategy == "bro":
        _mark_previous_sessions_superseded(telegram_id, network)
        _, strat_cfg = get_strategy_settings(telegram_id, strategy)
        state = _default_state()
        state.update(_strategy_defaults(strategy))
        state.update(strat_cfg)
        from src.nadobro.services.runtime_supervisor import strategy_worker_group

        state.update(
            {
                "running": True,
                "strategy": "bro",
                "product": "MULTI",
                "leverage": float(strat_cfg.get("leverage_cap", 5)),
                "slippage_pct": float(slippage_pct or 1.0),
                "reference_price": 0.0,
                "started_at": datetime.utcnow().isoformat(),
                "last_run_ts": 0.0,
                "last_error": None,
                "runs": 0,
                "bro_state": {
                    "started_at": datetime.utcnow().isoformat(),
                    "total_pnl": 0.0,
                    "trade_count": 0,
                    "active_positions": [],
                    "paused": False,
                },
            }
        )
        state["worker_group"] = strategy_worker_group("bro")
        session_id = _create_session(telegram_id, "bro", "MULTI", network, state)
        if session_id:
            state["strategy_session_id"] = session_id
        _save_state(telegram_id, network, state)
        _ensure_task(telegram_id, network)
        return True, "Bro Mode activated 🧠"

    vol_market_kw = "perp"
    if strategy == "vol":
        vol_market_kw = str(kwargs.get("vol_market") or "perp").strip().lower()
        if vol_market_kw not in ("perp", "spot"):
            vol_market_kw = "perp"
        if vol_market_kw == "spot":
            product = normalize_volume_spot_symbol(str(product or "").strip())
            allowed_vol_spot = list_volume_spot_product_names(network=network)
            if product not in allowed_vol_spot:
                return False, (
                    f"Volume spot supports only {', '.join(allowed_vol_spot) or 'no resolved spot pairs'} on {network}."
                )
            dir_spot = str(kwargs.get("direction") or "long").strip().lower()
            if dir_spot == "short":
                return False, "Volume spot mode is buy-then-sell only (direction must be long)."

    dn_pair = get_dn_pair(product, network=network) if strategy == "dn" else {}
    if strategy == "dn":
        product_id = int(dn_pair.get("perp_product_id")) if dn_pair.get("perp_product_id") is not None else None
    elif strategy == "vol" and vol_market_kw == "spot":
        product_id = get_spot_product_id(product, network=network)
    else:
        product_id = get_product_id(product, network=network)
    if product_id is None:
        return False, f"Unknown product '{product}'."
    if strategy == "dn" and not dn_pair:
        supported_products = ", ".join(get_dn_products(network=network) or ["BTC", "ETH"])
        return False, (
            "Delta Neutral currently supports assets with both Nado spot and perp markets. "
            f"Available now: {supported_products}."
        )
    if strategy == "dn" and not bool(dn_pair.get("entry_allowed", True)):
        return False, str(dn_pair.get("entry_block_reason") or f"{product.upper()} is not currently tradable for Delta Neutral.")
    if strategy == "vol" and vol_market_kw == "spot":
        max_leverage = 1
    else:
        max_leverage = get_product_max_leverage(product, network=network)
        if strategy == "dn":
            max_leverage = min(max_leverage, 5)
        if strategy == "vol":
            max_leverage = 1
    if float(leverage or 0) > max_leverage:
        return False, f"Max leverage for {product.upper()} is {max_leverage}x."
    if float(leverage or 0) < 1:
        return False, "Leverage must be at least 1x."
    preflight_ok, preflight_msg = run_strategy_start_preflight(
        telegram_id, product, network, vol_market=vol_market_kw if strategy == "vol" else "perp"
    )
    if not preflight_ok:
        return False, preflight_msg

    _mark_previous_sessions_superseded(telegram_id, network)
    _, strat_cfg = get_strategy_settings(telegram_id, strategy)
    state = _default_state()
    state.update(_strategy_defaults(strategy))
    state.update(strat_cfg)
    state.update(
        {
            "running": True,
            "strategy": strategy,
            "strategy_id_v2": 1,
            "product": (product if (strategy == "vol" and vol_market_kw == "spot") else str(product).upper()),
            "leverage": 1.0 if strategy == "vol" else float(leverage or 3.0),
            "slippage_pct": float(slippage_pct or 1.0),
            "reference_price": 0.0,
            "started_at": datetime.utcnow().isoformat(),
            "last_run_ts": 0.0,
            "last_error": None,
            "runs": 0,
        }
    )
    if strategy in ("grid", "rgrid"):
        mm_ok, mm_msg = _run_mm_start_guard(telegram_id, network, product.upper(), float(state.get("leverage") or leverage or 1.0), state)
        if not mm_ok:
            return False, mm_msg
    if strategy == "vol":
        state["vol_market"] = vol_market_kw
        if vol_market_kw == "spot":
            state["vol_direction"] = "long"
            state["direction"] = "long"
        else:
            direction = str(kwargs.get("direction") or strat_cfg.get("vol_direction") or "long").strip().lower()
            direction = "short" if direction == "short" else "long"
            state["vol_direction"] = direction
            state["direction"] = direction
        state["fixed_margin_usd"] = 100.0
        state["notional_usd"] = 100.0
        state["vol_phase"] = "idle"
        state["session_realized_pnl_usd"] = 0.0
        # Keep saved VOL prefs aligned with this run so dashboards / Advanced match execution.
        if vol_market_kw == "perp":
            try:
                from src.nadobro.services.settings_service import update_user_settings

                d = str(state.get("vol_direction") or "long").strip().lower()
                d = "short" if d == "short" else "long"

                def _sync_vol_direction_pref(settings: dict) -> None:
                    strategies = settings.setdefault("strategies", {})
                    vol_cfg = strategies.setdefault("vol", {})
                    vol_cfg["vol_direction"] = d

                update_user_settings(telegram_id, _sync_vol_direction_pref)
            except Exception:
                logger.warning(
                    "Could not sync vol_direction into user strategy prefs for user=%s",
                    telegram_id,
                    exc_info=True,
                )
    from src.nadobro.services.runtime_supervisor import strategy_worker_group

    state["worker_group"] = strategy_worker_group(strategy)
    session_id = _create_session(
        telegram_id,
        strategy,
        (product if (strategy == "vol" and vol_market_kw == "spot") else str(product).upper()),
        network,
        state,
    )
    if session_id:
        state["strategy_session_id"] = session_id
    _save_state(telegram_id, network, state)
    _ensure_task(telegram_id, network)
    if strategy == "dn":
        return (
            True,
            f"DN bot started with {str(dn_pair.get('spot_symbol') or product.upper())} spot long + "
            f"{str(dn_pair.get('perp_symbol') or f'{product.upper()}-PERP')} short ({network}) "
            f"| TP {state.get('tp_pct')}% / SL {state.get('sl_pct')}% | Leverage {state.get('leverage')}x",
        )
    if strategy == "vol":
        direction = str(state.get("vol_direction") or "long").upper()
        if str(state.get("vol_market") or "perp") == "spot":
            return (
                True,
                f"VOL spot bot started on {str(product).upper()} spot ({network}) "
                f"| Buy/sell limit loop | Margin $100 @ 1x | TP {state.get('tp_pct')}% / SL {state.get('sl_pct')}%",
            )
        return (
            True,
            f"VOL bot started on {str(product).upper()}-PERP ({network}) "
            f"| Direction {direction} | Margin $100 @ 1x | TP {state.get('tp_pct')}% / SL {state.get('sl_pct')}%",
        )
    if strategy in ("grid", "rgrid"):
        spread_key = "rgrid_spread_bp" if strategy == "rgrid" else "spread_bp"
        margin_usd = float(state.get("notional_usd") or 0.0)
        cycle_notional_cfg = float(state.get("cycle_notional_usd") or margin_usd or 0.0)
        cycle_notional = max(cycle_notional_cfg, margin_usd * max(1.0, float(state.get("leverage") or 1.0)))
        spread_bp = float(state.get(spread_key) or state.get("spread_bp") or 0.0)
        return (
            True,
            f"{_strategy_display_name(strategy)} bot started on {product.upper()}-PERP ({network}) "
            f"| Maker-only quotes | Margin ${margin_usd:,.0f} | Notional ${cycle_notional:,.0f} / cycle | Spread {spread_bp:.0f}bp",
        )
    return (
        True,
        f"{_strategy_display_name(strategy)} bot started on {product.upper()}-PERP ({network}) "
        f"| TP {state.get('tp_pct')}% / SL {state.get('sl_pct')}%",
    )


def set_process_worker_mode(enabled: bool) -> None:
    global _process_worker_mode
    _process_worker_mode = bool(enabled)


def run_cycle_job_sync(payload: dict) -> dict:
    telegram_id = int(payload.get("telegram_id"))
    network = str(payload.get("network"))
    started = time.perf_counter()
    set_process_worker_mode(True)
    try:
        state = _load_state(telegram_id, network)
        if not state.get("running"):
            return {
                "ok": True,
                "skipped": "not_running",
                "worker_pid": os.getpid(),
                "elapsed_ms": (time.perf_counter() - started) * 1000.0,
                "completed_at": time.time(),
            }
        ok, error_msg = asyncio.run(_run_cycle(telegram_id, network, state))
        return {
            "ok": bool(ok),
            "error": error_msg,
            "worker_pid": os.getpid(),
            "elapsed_ms": (time.perf_counter() - started) * 1000.0,
            "completed_at": time.time(),
        }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "worker_pid": os.getpid(),
            "elapsed_ms": (time.perf_counter() - started) * 1000.0,
            "completed_at": time.time(),
        }
    finally:
        set_process_worker_mode(False)


def stop_user_bot(telegram_id: int, cancel_orders: bool = True) -> tuple[bool, str]:
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    state = _load_state(telegram_id, network)
    if not state.get("running"):
        return False, "No running strategy bot found."

    _finalize_session(state, stop_reason="user_stop")
    state["running"] = False
    _save_state(telegram_id, network, state)

    tk = _task_key(telegram_id, network)
    task = _tasks.pop(tk, None)
    if task:
        task.cancel()

    if cancel_orders:
        strategy = str(state.get("strategy") or "").lower()
        if strategy == "dn":
            close_res = close_delta_neutral_legs(
                telegram_id,
                str(state.get("product") or ""),
                network=network,
                slippage_pct=float(state.get("slippage_pct") or 1.0),
                strategy_session_id=state.get("strategy_session_id"),
            )
        elif strategy == "vol" and str(state.get("vol_market") or "perp") == "spot":
            prod = normalize_volume_spot_symbol(str(state.get("product") or ""))
            spot_pid = get_spot_product_id(prod, network=network)
            if spot_pid is None:
                close_res = {"success": True, "skipped": True}
            else:
                sym = str((get_spot_metadata(prod, network=network) or {}).get("symbol") or prod).upper()
                close_res = stop_volume_spot_cleanup(
                    telegram_id,
                    int(spot_pid),
                    sym,
                    network=network,
                    slippage_pct=float(state.get("slippage_pct") or 1.0),
                    strategy_session_id=state.get("strategy_session_id"),
                )
        else:
            close_res = close_all_positions(telegram_id, network=network)
        if not close_res.get("success"):
            return False, f"Strategy loop stopped, but cleanup failed: {close_res.get('error', 'unknown')}"

    return True, "Strategy bot stopped. Open orders cancellation requested."


def stop_all_user_bots(telegram_id: int, cancel_orders: bool = False) -> tuple[bool, str]:
    stopped = 0
    close_errors: list[str] = []
    rows = query_all(
        "SELECT key, value FROM bot_state WHERE key LIKE %s",
        (f"{STATE_PREFIX}{telegram_id}:%",),
    )
    for row in rows:
        try:
            key = row.get("key", "")
            user_network = key.replace(STATE_PREFIX, "")
            user_id_str, network = user_network.split(":", 1)
            if int(user_id_str) != int(telegram_id):
                continue
            state = json.loads(row.get("value") or "{}")
            if not state.get("running"):
                continue
            _finalize_session(state, stop_reason="user_stop_all")
            state["running"] = False
            set_bot_state(key, state)
            tk = _task_key(telegram_id, network)
            task = _tasks.pop(tk, None)
            if task:
                task.cancel()
            if cancel_orders:
                strategy = str(state.get("strategy") or "").lower()
                if strategy == "dn":
                    close_res = close_delta_neutral_legs(
                        telegram_id,
                        str(state.get("product") or ""),
                        network=network,
                        slippage_pct=float(state.get("slippage_pct") or 1.0),
                        strategy_session_id=state.get("strategy_session_id"),
                    )
                elif strategy == "vol" and str(state.get("vol_market") or "perp") == "spot":
                    prod = normalize_volume_spot_symbol(str(state.get("product") or ""))
                    spot_pid = get_spot_product_id(prod, network=network)
                    if spot_pid is None:
                        close_res = {"success": True}
                    else:
                        sym = str((get_spot_metadata(prod, network=network) or {}).get("symbol") or prod).upper()
                        close_res = stop_volume_spot_cleanup(
                            telegram_id,
                            int(spot_pid),
                            sym,
                            network=network,
                            slippage_pct=float(state.get("slippage_pct") or 1.0),
                            strategy_session_id=state.get("strategy_session_id"),
                        )
                else:
                    close_res = close_all_positions(telegram_id, network=network)
                if not close_res.get("success"):
                    close_errors.append(f"{network}: {close_res.get('error', 'close_all_positions failed')}")
            stopped += 1
        except Exception:
            continue
    if stopped > 0:
        if cancel_orders and close_errors:
            return True, f"Stopped {stopped} running strategy loop(s). Some close-all actions failed: {'; '.join(close_errors)}"
        return True, f"Stopped {stopped} running strategy loop(s)."
    return False, "No running strategy bot found."


def get_user_bot_status(telegram_id: int) -> dict:
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    state = _load_state(telegram_id, network)
    global_pause_active = bool(is_trading_paused())
    last_run = _safe_last_run_ts(state.get("last_run_ts"))
    interval = int(state.get("interval_seconds") or 60)
    next_cycle_in = max(0, int(interval - (time.time() - last_run))) if last_run > 0 else 0
    other_running_networks: list[str] = []
    try:
        rows = query_all(
            "SELECT key, value FROM bot_state WHERE key LIKE %s",
            (f"{STATE_PREFIX}{telegram_id}:%",),
        )
        for row in rows:
            key = str(row.get("key", ""))
            key_part = key.replace(STATE_PREFIX, "")
            _, row_network = key_part.split(":", 1)
            if row_network == network:
                continue
            row_state = json.loads(row.get("value") or "{}")
            if row_state.get("running"):
                other_running_networks.append(row_network)
    except Exception:
        pass

    running_sessions: list[dict] = []
    try:
        for row in get_running_strategy_sessions(telegram_id, network):
            running_sessions.append({
                "id": row.get("id"),
                "strategy": row.get("strategy"),
                "product_name": row.get("product_name"),
                "network": row.get("network"),
                "started_at": row.get("started_at"),
                "total_cycles": int(row.get("total_cycles") or 0),
            })
    except Exception:
        pass

    session_analytics = {"total_trades": 0}
    strategy_session_id = state.get("strategy_session_id")
    if strategy_session_id:
        try:
            session_analytics = get_trade_analytics(telegram_id, strategy_session_id=int(strategy_session_id))
        except Exception:
            session_analytics = {"total_trades": 0}

    try:
        from src.nadobro.services.strategy_fsm import infer_phase

        strategy_phase = infer_phase(state).to_dict()
    except Exception:
        strategy_phase = {"phase": "unknown", "detail": "", "recoverable": True, "allowed_actions": []}

    return {
        "network": network,
        "running": bool(state.get("running")),
        "global_pause_active": global_pause_active,
        "strategy": state.get("strategy"),
        "product": state.get("product"),
        "vol_market": state.get("vol_market") or "perp",
        "notional_usd": state.get("notional_usd"),
        "cycle_notional_usd": state.get("cycle_notional_usd"),
        "spread_bp": state.get("spread_bp"),
        "tp_pct": state.get("tp_pct"),
        "sl_pct": state.get("sl_pct"),
        "interval_seconds": interval,
        "started_at": state.get("started_at"),
        "runs": state.get("runs", 0),
        "last_error": state.get("last_error"),
        "last_error_category": state.get("last_error_category") or "",
        "last_action": state.get("last_action"),
        "last_action_detail": state.get("last_action_detail"),
        "strategy_phase": strategy_phase,
        "last_run_ts": last_run,
        "next_cycle_in": next_cycle_in,
        "error_streak": int(state.get("error_streak") or 0),
        "pause_reason": state.get("mm_pause_reason") or "",
        "is_paused": bool(state.get("mm_paused")),
        "bro_state": state.get("bro_state"),
        "maker_fill_ratio": (state.get("mm_last_metrics") or {}).get("maker_fill_ratio"),
        "cancellation_ratio": (state.get("mm_last_metrics") or {}).get("cancellation_ratio"),
        "avg_quote_distance_bp": (state.get("mm_last_metrics") or {}).get("avg_quote_distance_bp"),
        "quote_refresh_rate": (state.get("mm_last_metrics") or {}).get("quote_refresh_rate"),
        "inventory_skew_usd": (state.get("mm_last_metrics") or {}).get("inventory_skew_usd"),
        "inventory_source": (state.get("mm_last_metrics") or {}).get("inventory_source") or state.get("mm_last_inventory_source"),
        "session_notional_done_usd": (state.get("mm_last_metrics") or {}).get("session_notional_done_usd"),
        "worker_group": state.get("worker_group"),
        "worker_last_heartbeat": float(state.get("worker_last_heartbeat") or 0.0),
        "last_dispatch_ts": float(state.get("last_dispatch_ts") or 0.0),
        "last_cycle_ms": float(state.get("last_cycle_ms") or 0.0),
        "last_cycle_result": state.get("last_cycle_result") or "",
        "worker_pid": state.get("worker_pid"),
        "dn_last_funding_rate": state.get("dn_last_funding_rate"),
        "dn_unfavorable_count": state.get("dn_unfavorable_count"),
        "dn_mode": state.get("dn_mode") or state.get("funding_entry_mode"),
        # Reverse GRID telemetry surface for runtime status.
        "rgrid_anchor_price": state.get("grid_anchor_price"),
        "rgrid_buy_exposure_price": state.get("grid_buy_exposure_price"),
        "rgrid_sell_exposure_price": state.get("grid_sell_exposure_price"),
        "rgrid_drift_from_anchor_pct": state.get("grid_drift_from_anchor_pct"),
        "rgrid_reset_active": bool(state.get("grid_reset_active")),
        "rgrid_reset_side": state.get("grid_reset_side"),
        "rgrid_last_cycle_pnl_usd": state.get("grid_last_cycle_pnl_usd"),
        "rgrid_stop_loss_pct": state.get("rgrid_stop_loss_pct") or state.get("grid_stop_loss_pct") or state.get("sl_pct"),
        "rgrid_take_profit_pct": state.get("rgrid_take_profit_pct") or state.get("grid_take_profit_pct") or state.get("tp_pct"),
        "rgrid_reset_threshold_pct": state.get("rgrid_reset_threshold_pct") or state.get("grid_reset_threshold_pct"),
        "rgrid_reset_timeout_seconds": state.get("rgrid_reset_timeout_seconds") or state.get("grid_reset_timeout_seconds"),
        "rgrid_discretion": state.get("rgrid_discretion") or state.get("grid_discretion"),
        "other_running_networks": other_running_networks,
        "strategy_session_id": strategy_session_id,
        "running_sessions": running_sessions,
        "order_observability": state.get("order_observability") or {},
        "session_trade_count": int(session_analytics.get("total_trades") or 0),
        "session_filled_trades": int(session_analytics.get("filled") or 0),
        "session_closed_trades": int(session_analytics.get("closed") or 0),
        "session_failed_trades": int(session_analytics.get("failed") or 0),
        "session_volume_usd": float(session_analytics.get("total_volume") or 0.0),
        "session_fees_usd": float(session_analytics.get("total_fees") or 0.0),
        "session_funding_usd": float(session_analytics.get("total_funding") or 0.0),
        "session_analytics_pnl_usd": float(session_analytics.get("total_pnl") or 0.0),
        "vol_order_attempts": int(state.get("vol_order_attempts") or 0),
        "vol_order_failures": int(state.get("vol_order_failures") or 0),
        "last_order_error": state.get("last_order_error") or "",
        "last_order_ts": float(state.get("last_order_ts") or 0.0),
        "target_volume_usd": float(state.get("target_volume_usd") or 0.0),
        "volume_done_usd": float(state.get("volume_done_usd") or 0.0),
        "volume_remaining_usd": float(state.get("volume_remaining_usd") or 0.0),
        "session_realized_pnl_usd": float(state.get("session_realized_pnl_usd") or 0.0),
        "vol_phase": state.get("vol_phase") or "",
        "vol_last_order_digest": state.get("vol_last_order_digest") or "",
        "vol_last_order_kind": state.get("vol_last_order_kind") or "",
        "vol_entry_digest": state.get("vol_entry_digest") or "",
        "vol_close_digest": state.get("vol_close_digest") or "",
        "vol_effective_margin_usd": float(state.get("vol_effective_margin_usd") or 0.0),
        "runtime_diagnostics": get_runtime_diagnostics(),
    }


def get_runtime_diagnostics() -> dict:
    from src.nadobro.services.execution_queue import get_queue_diagnostics
    from src.nadobro.services.runtime_supervisor import runtime_mode

    active_loops = len([t for t in _tasks.values() if not t.done()])
    pending_keys = len(_job_pending_payloads)
    pending_coalesced_ticks = sum(int(v or 0) for v in _job_coalesce_counts.values())
    return {
        "active_strategy_loops": active_loops,
        "tracked_job_locks": len(_job_locks),
        "pending_keys": pending_keys,
        "pending_coalesced_ticks": pending_coalesced_ticks,
        "queue": get_queue_diagnostics(),
        "stats": dict(_job_stats),
        "env": {
            "NADO_RUNTIME_MODE": runtime_mode(),
            "NADO_STRATEGY_WORKERS": (os.environ.get("NADO_STRATEGY_WORKERS") or "2").strip(),
            "NADO_STRATEGY_CYCLE_TIMEOUT_SECONDS": (os.environ.get("NADO_STRATEGY_CYCLE_TIMEOUT_SECONDS") or "180").strip(),
        },
    }


def stop_all_strategies_for_user(telegram_id: int) -> None:
    """Stop all running strategies for a given user. Used on network switch."""
    rows = query_all(
        "SELECT key, value FROM bot_state WHERE key LIKE %s",
        (f"{STATE_PREFIX}{telegram_id}:%",),
    )
    stopped = []
    for row in rows:
        try:
            key = row.get("key", "")
            user_network = key.replace(STATE_PREFIX, "")
            user_id_str, network = user_network.split(":", 1)
            if int(user_id_str) != int(telegram_id):
                continue
            state = json.loads(row.get("value") or "{}")
            if not state.get("running"):
                continue
            strategy = state.get("strategy", "unknown")
            _finalize_session(state, stop_reason="network_switch")
            state["running"] = False
            state["last_error"] = "Stopped due to network switch"
            set_bot_state(key, state)
            tk = _task_key(telegram_id, network)
            task = _tasks.pop(tk, None)
            if task:
                task.cancel()
            stopped.append(f"{strategy}@{network}")
        except Exception as e:
            logger.warning("Error stopping strategy for user %s: %s", telegram_id, e)
            continue
    if stopped:
        logger.info(
            "stop_all_strategies_for_user: stopped %d strategy/strategies for user %s: %s",
            len(stopped), telegram_id, ", ".join(stopped),
        )
    else:
        logger.debug("stop_all_strategies_for_user: no running strategies found for user %s", telegram_id)


def stop_runtime():
    for task_id, task in list(_tasks.items()):
        task.cancel()
        _tasks.pop(task_id, None)


def restore_running_bots(enabled: bool = False):
    if not enabled:
        logger.info("Skipping strategy auto-restore on startup (disabled).")
        return
    rows = query_all(
        "SELECT key, value FROM bot_state WHERE key LIKE %s",
        (f"{STATE_PREFIX}%",),
    )
    for row in rows:
        try:
            key = row.get("key", "")
            user_network = key.replace(STATE_PREFIX, "")
            user_id_str, network = user_network.split(":", 1)
            user_id = int(user_id_str)
            state = json.loads(row.get("value") or "{}")
            if state.get("running"):
                _ensure_task(user_id, network)
        except Exception:
            continue


def _schedule_task_on_loop(telegram_id: int, network: str):
    tk = _task_key(telegram_id, network)
    task = _tasks.get(tk)
    if task and not task.done():
        return
    _tasks[tk] = asyncio.create_task(_bot_loop(telegram_id, network))


def _ensure_task(telegram_id: int, network: str):
    global _runtime_loop
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        _runtime_loop = loop
        _schedule_task_on_loop(telegram_id, network)
        return

    if _runtime_loop and _runtime_loop.is_running():
        _runtime_loop.call_soon_threadsafe(_schedule_task_on_loop, telegram_id, network)
        return

    logger.error(
        "Could not start strategy loop for user %s on %s: no running event loop",
        telegram_id,
        network,
    )


async def _bot_loop(telegram_id: int, network: str):
    logger.info("Starting strategy loop for user %s on %s", telegram_id, network)
    await _notify(telegram_id, "Strategy loop started on {network}.", network=network)
    try:
        while True:
            state = _load_state(telegram_id, network)
            if not state.get("running"):
                break
            interval = max(1, int(state.get("interval_seconds") or 60))
            last_run = _safe_last_run_ts(state.get("last_run_ts"))
            now = time.time()
            # Avoid flooding the global strategy queue: only tick when the strategy interval has elapsed.
            if last_run > 0 and (now - last_run) < interval:
                wait_for = interval - (now - last_run)
                sleep_s = min(float(RUNTIME_TICK_SECONDS), max(0.5, wait_for))
                await asyncio.sleep(sleep_s)
                continue
            try:
                strategy = str(state.get("strategy") or "").lower().strip()
                lock = _job_locks.get(_task_key(telegram_id, network))
                if strategy == "vol" and lock and lock.locked():
                    _job_stats["vol_overlap_skips"] += 1
                    logger.info(
                        "Vol tick skipped while prior cycle still running user=%s network=%s",
                        telegram_id,
                        network,
                    )
                    await asyncio.sleep(min(float(RUNTIME_TICK_SECONDS), max(1.0, interval / 2.0)))
                    continue
                now_bucket = int(time.time() / max(1, RUNTIME_TICK_SECONDS))
                enqueued = await enqueue_strategy(
                    {"telegram_id": telegram_id, "network": network, "strategy": strategy},
                    dedupe_key=f"{telegram_id}:{network}:{now_bucket}",
                )
                if not enqueued:
                    logger.debug(
                        "Strategy tick deduped user=%s network=%s bucket=%s",
                        telegram_id,
                        network,
                        now_bucket,
                    )
            except Exception as cycle_error:
                logger.error("Cycle failure for user %s: %s", telegram_id, cycle_error, exc_info=True)
                state["last_error"] = str(cycle_error)
                _save_state(telegram_id, network, state)
            await asyncio.sleep(RUNTIME_TICK_SECONDS)
    except asyncio.CancelledError:
        logger.info("Strategy loop cancelled for user %s", telegram_id)
    finally:
        _tasks.pop(_task_key(telegram_id, network), None)


async def handle_strategy_job(payload: dict):
    telegram_id = int(payload.get("telegram_id"))
    network = str(payload.get("network"))
    key = _task_key(telegram_id, network)
    lock = _job_locks.setdefault(key, asyncio.Lock())
    if lock.locked():
        payload_strategy = str(payload.get("strategy") or "").lower().strip()
        if payload_strategy == "vol" and key in _job_pending_payloads:
            _job_stats["vol_overlap_skips"] += 1
            logger.info(
                "Dropping extra overlapping vol tick user=%s network=%s pending_already_set",
                telegram_id,
                network,
            )
            return
        _job_pending_payloads[key] = payload
        _job_coalesce_counts[key] = int(_job_coalesce_counts.get(key, 0)) + 1
        _job_stats["coalesced_ticks"] += 1
        if payload_strategy == "vol":
            _job_stats["vol_coalesced_ticks"] += 1
        logger.info(
            "Coalescing overlapping strategy cycle for user %s on %s (pending=%s)",
            telegram_id,
            network,
            _job_coalesce_counts[key],
        )
        return

    async with lock:
        while True:
            cycle_started = time.perf_counter()
            _job_stats["cycles_started"] += 1
            state = _load_state(telegram_id, network)
            if not state.get("running"):
                _job_pending_payloads.pop(key, None)
                _job_coalesce_counts.pop(key, None)
                return
            try:
                from src.nadobro.services.execution_queue import get_queue_diagnostics
                from src.nadobro.services.runtime_supervisor import (
                    is_multiprocess_enabled,
                    strategy_worker_group,
                    submit_cycle_job,
                )

                qsz = int((get_queue_diagnostics() or {}).get("strategy_qsize") or 0)
                logger.info(
                    "Strategy cycle start user=%s network=%s strategy=%s queue_depth=%s",
                    telegram_id,
                    network,
                    state.get("strategy"),
                    qsz,
                )

                strategy = str(state.get("strategy") or "").lower().strip()
                use_multiprocess = is_multiprocess_enabled() and _strategy_use_multiprocess(strategy)
                if use_multiprocess:
                    strategy = str(state.get("strategy") or "")
                    worker_group = strategy_worker_group(strategy)
                    state["worker_group"] = worker_group
                    state["last_dispatch_ts"] = time.time()
                    _save_state(telegram_id, network, state)
                    timeout_sec = _strategy_cycle_timeout_seconds() or 180.0
                    if str(strategy).lower().strip() == "vol":
                        timeout_sec = min(timeout_sec, 45.0)
                    delegated: dict = {}
                    delegated_timed_out = False
                    try:
                        delegated = await asyncio.wait_for(
                            submit_cycle_job(
                                {
                                    "telegram_id": telegram_id,
                                    "network": network,
                                    "strategy": strategy,
                                    "worker_group": worker_group,
                                }
                            ),
                            timeout=timeout_sec,
                        )
                    except asyncio.TimeoutError:
                        delegated_timed_out = True
                        _job_stats["cycle_timeouts"] += 1
                        logger.error(
                            "Delegated strategy cycle timeout user=%s network=%s strategy=%s timeout=%.0fs; falling back to local run",
                            telegram_id,
                            network,
                            strategy,
                            timeout_sec,
                        )

                    if delegated_timed_out:
                        try:
                            ok, error_msg = await asyncio.wait_for(
                                _run_cycle(telegram_id, network, state),
                                timeout=timeout_sec,
                            )
                        except asyncio.TimeoutError:
                            _job_stats["cycle_timeouts"] += 1
                            ok = False
                            error_msg = f"Strategy cycle timed out after {timeout_sec:.0f}s (delegated + local fallback)"
                        refreshed = _load_state(telegram_id, network)
                        refreshed["worker_group"] = worker_group
                        refreshed["worker_last_heartbeat"] = time.time()
                        refreshed["last_cycle_ms"] = (time.perf_counter() - cycle_started) * 1000.0
                        refreshed["last_cycle_result"] = _cycle_result_label(ok, error_msg)
                        _save_state(telegram_id, network, refreshed)
                    else:
                        ok = bool(delegated.get("ok", True))
                        error_msg = delegated.get("error")
                        refreshed = _load_state(telegram_id, network)
                        refreshed["worker_group"] = worker_group
                        refreshed["worker_last_heartbeat"] = float(delegated.get("completed_at") or time.time())
                        refreshed["last_cycle_ms"] = float(delegated.get("elapsed_ms") or 0.0)
                        refreshed["last_cycle_result"] = _cycle_result_label(ok, error_msg)
                        refreshed["worker_pid"] = delegated.get("worker_pid")
                        _save_state(telegram_id, network, refreshed)
                else:
                    timeout_sec = _strategy_cycle_timeout_seconds()
                    strategy = str(state.get("strategy") or "").lower().strip()
                    if timeout_sec and strategy == "vol":
                        timeout_sec = min(timeout_sec, 45.0)
                    try:
                        if timeout_sec:
                            ok, error_msg = await asyncio.wait_for(
                                _run_cycle(telegram_id, network, state),
                                timeout=timeout_sec,
                            )
                        else:
                            ok, error_msg = await _run_cycle(telegram_id, network, state)
                    except asyncio.TimeoutError:
                        _job_stats["cycle_timeouts"] += 1
                        tsec = timeout_sec or 180.0
                        ok = False
                        error_msg = (
                            f"Strategy cycle timed out after {tsec:.0f}s "
                            "(NADO_STRATEGY_CYCLE_TIMEOUT_SECONDS)"
                        )
                        logger.error(
                            "Strategy cycle timeout user=%s network=%s strategy=%s",
                            telegram_id,
                            network,
                            state.get("strategy"),
                        )
                    hb_state = _load_state(telegram_id, network)
                    hb_state["worker_last_heartbeat"] = time.time()
                    hb_state["last_cycle_ms"] = (time.perf_counter() - cycle_started) * 1000.0
                    hb_state["last_cycle_result"] = _cycle_result_label(ok, error_msg)
                    _save_state(telegram_id, network, hb_state)
                if ok:
                    _job_stats["cycles_ok"] += 1
                    if _cycle_result_label(ok, error_msg) == "ok":
                        refreshed = _load_state(telegram_id, network)
                        if refreshed.get("error_streak") or refreshed.get("last_error"):
                            refreshed["error_streak"] = 0
                            refreshed["last_error"] = None
                            refreshed["last_error_category"] = ""
                            _save_state(telegram_id, network, refreshed)
                else:
                    _job_stats["cycles_failed"] += 1
                    await _mark_cycle_error(telegram_id, network, error_msg or "unknown cycle error")
            except Exception as e:
                _job_stats["cycles_failed"] += 1
                logger.error("Strategy cycle crash for user %s on %s: %s", telegram_id, network, e, exc_info=True)
                strategy_name = str(state.get("strategy") or "strategy").upper()
                await _mark_cycle_error(telegram_id, network, f"{strategy_name}[cycle/crash]: {str(e)[:220]}")
            finally:
                elapsed_ms = (time.perf_counter() - cycle_started) * 1000.0
                record_metric("runtime.strategy_cycle.total", elapsed_ms)
                logger.info(
                    "Strategy cycle end user=%s network=%s elapsed_ms=%.1f",
                    telegram_id,
                    network,
                    elapsed_ms,
                )

            pending_payload = _job_pending_payloads.pop(key, None)
            coalesced_count = int(_job_coalesce_counts.pop(key, 0))
            if not pending_payload:
                break
            _job_stats["deferred_cycles"] += 1
            strategy_for_pending = str((pending_payload or {}).get("strategy") or state.get("strategy") or "").lower().strip()
            if strategy_for_pending == "vol":
                _job_stats["vol_deferred_cycles"] += 1
            logger.info(
                "Running deferred coalesced strategy cycle for user %s on %s (%s coalesced tick(s))",
                telegram_id,
                network,
                coalesced_count or 1,
            )


async def _mark_cycle_error(telegram_id: int, network: str, error_msg: str):
    state = _load_state(telegram_id, network)
    if not state.get("running"):
        return
    state["last_error"] = str(error_msg)[:300]
    state["last_error_category"] = _categorize_runtime_error(error_msg)
    streak = int(state.get("error_streak") or 0) + 1
    state["error_streak"] = streak
    _save_state(telegram_id, network, state)
    if streak == STRATEGY_ERROR_ALERT_STREAK:
        strategy = str(state.get("strategy") or "").upper() or "STRATEGY"
        product = str(state.get("product") or "?")
        await _notify(
            telegram_id,
            "⚠️ {strategy} health alert on {product}-PERP ({network}): "
            "{streak} consecutive cycle errors.\n"
            "Latest: {error}",
            strategy=strategy, product=product, network=network,
            streak=streak, error=str(error_msg)[:220],
        )


def _dispatch_strategy(strategy: str, telegram_id: int, network: str, state: dict,
                       client, mid: float, product_id: int, product: str, open_orders: list) -> dict:
    from src.nadobro.strategies import mm_bot, delta_neutral, volume_bot

    if strategy in ("grid", "rgrid"):
        return mm_bot.run_cycle(
            telegram_id, network, state,
            client=client, mid=mid, open_orders=open_orders,
        )
    elif strategy == "dn":
        return delta_neutral.run_cycle(
            telegram_id, network, state,
            client=client, mid=mid, product_id=product_id,
            product=product, open_orders=open_orders,
        )
    elif strategy == "vol":
        return volume_bot.run_cycle(
            telegram_id,
            network,
            state,
            client=client,
            mid=mid,
        )
    elif strategy == "bro":
        from src.nadobro.strategies import bro_mode
        return bro_mode.run_cycle(
            telegram_id, network, state,
            client=client,
        )
    else:
        return {"success": False, "error": f"Unknown strategy '{strategy}'"}


async def _run_cycle(telegram_id: int, network: str, state: dict) -> tuple[bool, str | None]:
    try:
        from src.nadobro.services.strategy_fsm import PHASE_SCANNING, apply_phase

        apply_phase(state, PHASE_SCANNING, "Strategy cycle started.")
    except Exception:
        pass
    # Apply latest saved strategy parameters (margin, spreads, etc.) every cycle so edits
    # in the UI take effect without restarting the loop.
    try:
        strategy_key = str(state.get("strategy") or "").lower()
        if strategy_key:
            from src.nadobro.services.settings_service import get_strategy_settings

            _, cfg = get_strategy_settings(telegram_id, strategy_key)
            if isinstance(cfg, dict):
                for k, v in cfg.items():
                    if k in _STRATEGY_SETTINGS_RUNTIME_BLOCKLIST:
                        continue
                    state[k] = v
    except Exception:
        logger.warning("Failed to merge strategy settings into runtime state for user %s", telegram_id, exc_info=True)

    if is_trading_paused():
        state["last_action"] = "maintenance_pause"
        state["last_action_detail"] = "Global trading pause active. Strategy execution is suspended."
        if str(state.get("strategy", "")).lower() == "dn" and float(state.get("auto_close_on_maintenance") or 0) >= 0.5:
            _finalize_session(state, stop_reason="maintenance_pause")
            state["running"] = False
            state["last_error"] = "Auto-closed on maintenance pause."
            _save_state(telegram_id, network, state)
            close_res = await run_blocking(
                close_delta_neutral_legs,
                telegram_id,
                str(state.get("product") or ""),
                network,
                float(state.get("slippage_pct") or 1.0),
                "dn",
                state.get("strategy_session_id"),
            )
            if close_res.get("success"):
                await _notify(telegram_id, "Delta Neutral stopped and auto-closed due to maintenance pause.")
            else:
                await _notify(
                    telegram_id,
                    "Delta Neutral stop triggered, but auto-close cleanup failed. "
                    "Please close manually and check open orders. Error: {error}",
                    error=close_res.get('error', 'unknown'),
                )
        else:
            _save_state(telegram_id, network, state)
        return True, "maintenance_pause"
    user = await run_blocking(get_user, telegram_id)
    if not user:
        _finalize_session(state, stop_reason="user_deleted")
        state["running"] = False
        _save_state(telegram_id, network, state)
        return True, None
    if user.network_mode.value != network:
        _finalize_session(state, stop_reason="mode_switch")
        state["running"] = False
        state["last_error"] = f"Stopped because active mode switched to {user.network_mode.value}"
        _save_state(telegram_id, network, state)
        await _notify(
            telegram_id,
            "Stopped {strategy} loop on {network}: active mode changed to {new_mode}.",
            strategy=state.get('strategy', '').upper(), network=network, new_mode=user.network_mode.value,
        )
        return True, None

    last_run = _safe_last_run_ts(state.get("last_run_ts"))
    interval = int(state.get("interval_seconds") or 60)
    if last_run > 0 and time.time() - last_run < interval:
        return True, "skipped_interval"

    product = state.get("product", "BTC")
    strategy = _normalize_strategy_id(state.get("strategy"))
    state["strategy"] = strategy

    if strategy == "bro":
        client = await run_blocking(get_user_nado_client, telegram_id)
        if not client:
            client = await run_blocking(get_user_readonly_client, telegram_id)
        if not client:
            raise RuntimeError("Wallet client unavailable")
        if hasattr(client, "network") and client.network != network:
            raise RuntimeError(f"Network mismatch: expected {network}, got {client.network}")
        with timed_metric("runtime.strategy.dispatch.bro"):
            result = await run_blocking(
                _dispatch_strategy,
                strategy, telegram_id, network, state,
                client, 0.0, 0, "MULTI", [],
            )
        prev_runs = int(state.get("runs") or 0)
        state["last_run_ts"] = time.time()
        bro_action = result.get("action", "")
        bro_detail = result.get("detail", "")
        bro_confidence = result.get("confidence", 0)
        state["runs"] = prev_runs + 1
        state["last_error"] = result.get("error") or result.get("order_error")
        state["last_action"] = bro_action
        state["last_action_detail"] = bro_detail[:200] if bro_detail else ""
        _update_order_observability(state, result)
        _save_state(telegram_id, network, state)
        if prev_runs == 0 or bro_action in ("open_long", "open_short", "close", "emergency_flatten", "blocked"):
            if bro_action == "hold":
                await _notify(
                    telegram_id,
                    "🧠 Bro Mode cycle #{cycle} ({network}): "
                    "HOLD — {detail}\n"
                    "Confidence: {confidence} | Next scan in {interval}s",
                    cycle=state['runs'], network=network, detail=bro_detail[:150],
                    confidence=f"{bro_confidence:.0%}", interval=int(state.get('interval_seconds', 300)),
                )
            elif bro_action in ("open_long", "open_short"):
                side_label = "LONG 📈" if bro_action == "open_long" else "SHORT 📉"
                await _notify(
                    telegram_id,
                    "🧠 Bro Mode cycle #{cycle} ({network}): "
                    "{side} — {detail}",
                    cycle=state['runs'], network=network, side=side_label, detail=bro_detail[:150],
                )
            elif bro_action == "blocked":
                await _notify(
                    telegram_id,
                    "🧠 Bro Mode cycle #{cycle} ({network}): "
                    "Blocked — {detail}",
                    cycle=state['runs'], network=network, detail=bro_detail[:150],
                )
            elif bro_action:
                await _notify(
                    telegram_id,
                    "🧠 Bro Mode cycle #{cycle} ({network}): "
                    "{action} — {detail}",
                    cycle=state['runs'], network=network, action=bro_action.upper(), detail=bro_detail[:150],
                )
        elif bro_action == "hold" and state["runs"] % 6 == 0:
            await _notify(
                telegram_id,
                "🧠 Bro Mode update ({network}): Still scanning, {cycles} cycles run. "
                "Last: HOLD — {detail}",
                network=network, cycles=state['runs'], detail=bro_detail[:100],
            )
        logger.info(
            "Bro Mode cycle #%d for user %s: action=%s confidence=%.2f detail=%s",
            state["runs"], telegram_id, bro_action, bro_confidence, bro_detail[:100],
        )

        # Increment bro session metrics
        session_id = state.get("strategy_session_id")
        if session_id:
            try:
                orders = 1 if bro_action in ("open_long", "open_short", "close") else 0
                filled = orders if result.get("success") else 0
                increment_session_metrics(
                    int(session_id),
                    cycles=1,
                    orders_placed=orders,
                    orders_filled=filled,
                    pnl=float(result.get("pnl", 0) or 0),
                    volume=float(result.get("placed_notional_usd", 0) or 0),
                )
            except Exception:
                pass

        if not result.get("success", True):
            return False, str(result.get("error") or result.get("order_error") or "unknown")[:300]
        return True, None

    dn_pair = get_dn_pair(product, network=network, client=None) if strategy == "dn" else {}
    if strategy == "vol" and str(state.get("vol_market") or "perp") == "spot":
        prod = normalize_volume_spot_symbol(str(product or ""))
        product_id = get_spot_product_id(prod, network=network)
    else:
        product_id = int(dn_pair.get("perp_product_id")) if dn_pair.get("perp_product_id") is not None else get_product_id(product, network=network)
    if product_id is None:
        raise RuntimeError(f"Invalid product '{product}'")

    # Use signing client so strategies can cancel orders and perform writes.
    # Falls back to readonly if signing client isn't available (read-only mode).
    client = await run_blocking(get_user_nado_client, telegram_id)
    if not client:
        client = await run_blocking(get_user_readonly_client, telegram_id)
    if not client:
        raise RuntimeError("Wallet client unavailable")

    with timed_metric("runtime.market_price.fetch"):
        if strategy == "vol":
            try:
                mp = await asyncio.wait_for(
                    run_blocking(client.get_market_price, product_id),
                    timeout=_vol_call_timeout_seconds(),
                )
            except asyncio.TimeoutError:
                raise RuntimeError("VOL market price call timed out")
        else:
            mp = await run_blocking(client.get_market_price, product_id)
    mid = float(mp.get("mid") or 0.0)
    if mid <= 0:
        raise RuntimeError("Could not fetch market price")

    reference_price = float(state.get("reference_price") or 0.0)
    if reference_price <= 0:
        state["reference_price"] = mid
        _save_state(telegram_id, network, state)
        reference_price = mid

    if strategy not in ("dn", "vol"):
        move_pct = abs((mid - reference_price) / reference_price) * 100.0 if reference_price > 0 else 0.0
        sl_pct = float(state.get("sl_pct") or 0.0)
        tp_pct = float(state.get("tp_pct") or 0.0)
        if sl_pct > 0 and move_pct >= sl_pct:
            _finalize_session(state, stop_reason="sl_hit")
            state["running"] = False
            state["last_error"] = f"Stopped by SL at {move_pct:.2f}% move from reference."
            _save_state(telegram_id, network, state)
            close_res = await run_blocking(close_all_positions, telegram_id, network)
            if close_res.get("success"):
                await _notify(
                    telegram_id,
                    "🛑 {strategy} stopped on {product}-PERP ({network}) - SL hit ({pct}%).",
                    strategy=_strategy_display_name(strategy), product=product, network=network, pct=f"{move_pct:.2f}",
                )
            else:
                logger.warning(
                    "SL stop close_all_positions reported failure for user %s on %s: %s",
                    telegram_id,
                    network,
                    close_res.get("error", "unknown"),
                )
                await _notify(
                    telegram_id,
                    "⚠️ SL triggered for {strategy} on {product}-PERP ({network}), "
                    "but full cleanup failed. Please close remaining exposure on Nado. "
                    "Error: {error}",
                    strategy=_strategy_display_name(strategy), product=product, network=network, error=close_res.get('error', 'unknown'),
                )
            return True, None
        if tp_pct > 0 and move_pct >= tp_pct:
            _finalize_session(state, stop_reason="tp_hit")
            state["running"] = False
            state["last_error"] = None
            _save_state(telegram_id, network, state)
            close_res = await run_blocking(close_all_positions, telegram_id, network)
            if close_res.get("success"):
                await _notify(
                    telegram_id,
                    "✅ {strategy} target reached on {product}-PERP ({network}) - TP hit ({pct}%).",
                    strategy=_strategy_display_name(strategy), product=product, network=network, pct=f"{move_pct:.2f}",
                )
            else:
                logger.warning(
                    "TP stop close_all_positions reported failure for user %s on %s: %s",
                    telegram_id,
                    network,
                    close_res.get("error", "unknown"),
                )
                await _notify(
                    telegram_id,
                    "⚠️ TP triggered for {strategy} on {product}-PERP ({network}), "
                    "but full cleanup failed. Please close remaining exposure on Nado. "
                    "Error: {error}",
                    strategy=_strategy_display_name(strategy), product=product, network=network, error=close_res.get('error', 'unknown'),
                )
            return True, None

    with timed_metric("runtime.open_orders.fetch"):
        if strategy == "vol":
            try:
                open_orders = await asyncio.wait_for(
                    run_blocking(client.get_open_orders, product_id),
                    timeout=_vol_call_timeout_seconds(),
                )
            except asyncio.TimeoutError:
                raise RuntimeError("VOL open-orders call timed out")
        else:
            open_orders = await run_blocking(client.get_open_orders, product_id)

    with timed_metric(f"runtime.strategy.dispatch.{strategy}"):
        if strategy == "vol":
            try:
                result = await asyncio.wait_for(
                    run_blocking(
                        _dispatch_strategy,
                        strategy, telegram_id, network, state,
                        client, mid, product_id, product, open_orders,
                    ),
                    timeout=max(_vol_call_timeout_seconds(), 20.0),
                )
            except asyncio.TimeoutError:
                raise RuntimeError("VOL strategy dispatch timed out")
        else:
            result = await run_blocking(
                _dispatch_strategy,
                strategy, telegram_id, network, state,
                client, mid, product_id, product, open_orders,
            )

    if strategy in ("grid", "rgrid") and result.get("action") == "grid_stop_loss_hit":
        _finalize_session(state, stop_reason="grid_sl_hit")
        state["running"] = False
        strategy_label = _strategy_display_name(strategy)
        state["last_error"] = result.get("detail") or f"{strategy_label} stop-loss triggered."
        _save_state(telegram_id, network, state)
        close_res = await run_blocking(close_all_positions, telegram_id, network)
        if close_res.get("success"):
            await _notify(
                telegram_id,
                "🛑 {strategy} stopped on {product}-PERP ({network}) - PnL stop-loss triggered.\n{detail}",
                strategy=strategy_label,
                product=product, network=network, detail=(result.get("detail") or "")[:180],
            )
        else:
            logger.warning(
                "%s PnL stop close_all_positions failed for user %s on %s: %s",
                strategy_label,
                telegram_id,
                network,
                close_res.get("error", "unknown"),
            )
            await _notify(
                telegram_id,
                "⚠️ {strategy} PnL stop-loss triggered on {product}-PERP ({network}), "
                "but cleanup failed. Error: {error}",
                strategy=strategy_label,
                product=product, network=network, error=close_res.get("error", "unknown"),
            )
        return True, None
    if strategy in ("grid", "rgrid") and result.get("action") == "grid_take_profit_hit":
        _finalize_session(state, stop_reason="tp_hit")
        state["running"] = False
        strategy_label = _strategy_display_name(strategy)
        state["last_error"] = None
        _save_state(telegram_id, network, state)
        close_res = await run_blocking(close_all_positions, telegram_id, network)
        if close_res.get("success"):
            await _notify(
                telegram_id,
                "✅ {strategy} completed on {product}-PERP ({network}) - PnL take-profit triggered.\n{detail}",
                strategy=strategy_label,
                product=product, network=network, detail=(result.get("detail") or "")[:180],
            )
        else:
            logger.warning(
                "%s PnL take-profit close_all_positions failed for user %s on %s: %s",
                strategy_label,
                telegram_id,
                network,
                close_res.get("error", "unknown"),
            )
            await _notify(
                telegram_id,
                "⚠️ {strategy} PnL take-profit triggered on {product}-PERP ({network}), "
                "but cleanup failed. Error: {error}",
                strategy=strategy_label,
                product=product, network=network, error=close_res.get("error", "unknown"),
            )
        return True, None
    if strategy in ("grid", "rgrid") and result.get("action") == "circuit_breaker":
        _finalize_session(state, stop_reason="circuit_breaker")
        state["running"] = False
        strategy_label = _strategy_display_name(strategy)
        state["last_error"] = result.get("error") or f"{strategy_label} circuit breaker triggered."
        _save_state(telegram_id, network, state)
        close_res = await run_blocking(close_all_positions, telegram_id, network)
        if close_res.get("success"):
            await _notify(
                telegram_id,
                "🛑 {strategy} stopped on {product}-PERP ({network}) - risk circuit breaker triggered.\n{detail}",
                strategy=strategy_label,
                product=product,
                network=network,
                detail=(result.get("error") or "")[:180],
            )
        else:
            await _notify(
                telegram_id,
                "⚠️ {strategy} circuit breaker triggered on {product}-PERP ({network}), but cleanup failed. Error: {error}",
                strategy=strategy_label,
                product=product,
                network=network,
                error=close_res.get("error", "unknown"),
            )
        return True, None
    if strategy in ("grid", "rgrid") and result.get("done") and str(result.get("reason") or "") == "session notional cap reached":
        _finalize_session(state, stop_reason="session_cap_hit")
        state["running"] = False
        strategy_label = _strategy_display_name(strategy)
        state["last_error"] = None
        _save_state(telegram_id, network, state)
        close_res = await run_blocking(close_all_positions, telegram_id, network)
        if close_res.get("success"):
            await _notify(
                telegram_id,
                "✅ {strategy} stopped on {product}-PERP ({network}) - session volume cap reached.",
                strategy=strategy_label,
                product=product,
                network=network,
            )
        else:
            await _notify(
                telegram_id,
                "⚠️ {strategy} hit its session cap on {product}-PERP ({network}), but cleanup failed. Error: {error}",
                strategy=strategy_label,
                product=product,
                network=network,
                error=close_res.get("error", "unknown"),
            )
        return True, None
    if strategy == "vol" and result.get("done") and str(result.get("stop_reason") or "") in ("tp_hit", "sl_hit", "target_volume_hit"):
        stop_reason = str(result.get("stop_reason"))
        _finalize_session(state, stop_reason=stop_reason)
        state["running"] = False
        state["last_error"] = None if stop_reason in ("tp_hit", "target_volume_hit") else (
            result.get("error")
            or f"Stopped by session SL ({float(state.get('session_realized_pnl_usd') or 0.0):.4f} USD)."
        )
        _save_state(telegram_id, network, state)
        close_res = await run_blocking(close_all_positions, telegram_id, network)
        if close_res.get("success"):
            await _notify(
                telegram_id,
                "✅ Volume strategy stopped on {product}-PERP ({network}) - {reason}.",
                product=product,
                network=network,
                reason=(
                    "session TP hit" if stop_reason == "tp_hit"
                    else "target volume hit" if stop_reason == "target_volume_hit"
                    else "session SL hit"
                ),
            )
        else:
            await _notify(
                telegram_id,
                "⚠️ Volume strategy stop triggered on {product}-PERP ({network}), but cleanup failed. Error: {error}",
                product=product,
                network=network,
                error=close_res.get("error", "unknown"),
            )
        return True, None

    prev_runs = int(state.get("runs") or 0)
    state["last_run_ts"] = time.time()
    state["runs"] = prev_runs + 1
    cycle_error = result.get("error") or result.get("order_error")
    state["last_error"] = cycle_error
    state["last_error_category"] = _categorize_runtime_error(cycle_error) if cycle_error else ""
    state["last_action"] = result.get("action", "cycle")
    state["last_action_detail"] = str(result.get("detail", ""))[:200]
    _merge_vol_order_counters(state, result)
    _update_order_observability(state, result)
    _save_state(telegram_id, network, state)
    drift_seconds = max(0.0, state["last_run_ts"] - last_run - interval) if last_run > 0 else 0.0
    if drift_seconds > 0:
        record_metric("runtime.cycle_drift_ms", drift_seconds * 1000.0)

    # Notify user on first successful cycle with order placement confirmation
    orders_placed = int(result.get("orders_placed", 0))
    if prev_runs == 0 and result.get("success", True):
        if orders_placed > 0:
            await _notify(
                telegram_id,
                "{strategy} is live on {product}-PERP ({network}) — {n} order(s) placed.",
                strategy=_strategy_display_name(strategy), product=product,
                network=network, n=orders_placed,
            )
        else:
            reason = result.get("reason") or result.get("detail") or result.get("action") or "waiting for conditions"
            await _notify(
                telegram_id,
                "{strategy} cycle #1 on {product}-PERP ({network}): no orders placed — {reason}",
                strategy=_strategy_display_name(strategy), product=product,
                network=network, reason=str(reason)[:150],
            )

    # Surface persistent order-placement failures (cycles 2-5)
    if 1 <= prev_runs <= 4 and result.get("success", True) and orders_placed == 0:
        reason = result.get("reason") or result.get("detail") or result.get("action") or ""
        if reason and reason not in ("below threshold", "wait", "waiting"):
            logger.warning(
                "%s cycle #%d for user %s: 0 orders placed — %s",
                strategy, prev_runs + 1, telegram_id, reason,
            )

    # Increment strategy session metrics from cycle result
    session_id = state.get("strategy_session_id")
    if session_id and result.get("success", True):
        try:
            increment_session_metrics(
                int(session_id),
                cycles=1,
                orders_placed=orders_placed,
                orders_filled=int(result.get("orders_filled", 0)),
                orders_cancelled=int(result.get("orders_cancelled", 0)),
                volume=float(
                    result.get("placed_notional_usd", 0)
                    or result.get("cycle_placed_notional_usd", 0)
                    or 0
                ),
            )
        except Exception:
            pass

    if not result.get("success", True):
        error_msg = _format_cycle_failure_error(strategy, result)
        return False, error_msg
    return True, None
