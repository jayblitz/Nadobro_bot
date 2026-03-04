import asyncio
import json
import logging
import time
from datetime import datetime

from src.nadobro.config import get_product_id, get_product_max_leverage
from src.nadobro.models.database import get_bot_state_raw, set_bot_state
from src.nadobro.db import query_all
from src.nadobro.services.admin_service import is_trading_paused
from src.nadobro.services.settings_service import get_strategy_settings
from src.nadobro.services.trade_service import close_all_positions
from src.nadobro.services.user_service import get_user_nado_client, get_user_readonly_client, get_user
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.perf import timed_metric, record_metric
from src.nadobro.services.execution_queue import enqueue_strategy

logger = logging.getLogger(__name__)

STATE_PREFIX = "strategy_bot:"
RUNTIME_TICK_SECONDS = 20
MAX_OPEN_ORDERS_PER_PRODUCT = 6
STRATEGY_ERROR_ALERT_STREAK = 3

_bot_app = None
_runtime_loop: asyncio.AbstractEventLoop | None = None
_tasks: dict[str, asyncio.Task] = {}
_session_passphrases: dict[str, str] = {}
_manual_session_passphrases: dict[str, dict] = {}
_MANUAL_SESSION_TTL_SECONDS = 1800


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


def set_runtime_passphrase(telegram_id: int, network: str, passphrase: str):
    if not passphrase:
        return
    _session_passphrases[_task_key(telegram_id, network)] = passphrase


def set_manual_passphrase(telegram_id: int, network: str, passphrase: str):
    if not passphrase:
        return
    _manual_session_passphrases[_task_key(telegram_id, network)] = {
        "passphrase": passphrase,
        "set_at": time.time(),
    }


def clear_manual_passphrase(telegram_id: int, network: str):
    _manual_session_passphrases.pop(_task_key(telegram_id, network), None)


def get_runtime_passphrase(telegram_id: int, network: str) -> str | None:
    tk = _task_key(telegram_id, network)
    strategy_passphrase = _session_passphrases.get(tk)
    if strategy_passphrase:
        return strategy_passphrase

    payload = _manual_session_passphrases.get(tk)
    if not payload:
        return None
    passphrase = payload.get("passphrase")
    set_at = float(payload.get("set_at") or 0)
    if not passphrase or (time.time() - set_at > _MANUAL_SESSION_TTL_SECONDS):
        _manual_session_passphrases.pop(tk, None)
        return None
    return passphrase


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
    }


def _load_state(telegram_id: int, network: str) -> dict:
    raw = get_bot_state_raw(_state_key(telegram_id, network))
    if not raw:
        return _default_state()
    try:
        loaded = json.loads(raw)
        state = _default_state()
        state.update(loaded if isinstance(loaded, dict) else {})
        return state
    except Exception:
        logger.warning("Invalid bot state JSON for user %s", telegram_id)
        return _default_state()


def _save_state(telegram_id: int, network: str, state: dict):
    set_bot_state(_state_key(telegram_id, network), state)


async def _notify(telegram_id: int, text: str):
    if not _bot_app:
        return
    try:
        await _bot_app.bot.send_message(chat_id=telegram_id, text=text)
    except Exception as e:
        logger.warning("Notify failed for %s: %s", telegram_id, e)


def _strategy_defaults(strategy: str) -> dict:
    presets = {
        "mm": {"notional_usd": 75.0, "spread_bp": 4.0, "interval_seconds": 45, "threshold_bp": 12.0, "close_offset_bp": 24.0},
        "grid": {"notional_usd": 100.0, "spread_bp": 10.0, "interval_seconds": 60, "levels": 4, "min_range_pct": 1.0, "max_range_pct": 1.0},
        "dn": {"notional_usd": 50.0, "spread_bp": 3.0, "interval_seconds": 90, "auto_close_on_maintenance": 1.0},
        "vol": {"notional_usd": 200.0, "target_volume_usd": 10000.0, "interval_seconds": 30},
    }
    return presets.get(strategy, {"notional_usd": 100.0, "spread_bp": 5.0, "interval_seconds": 60})


SUPPORTED_STRATEGIES = ("mm", "grid", "dn", "vol")


def start_user_bot(
    telegram_id: int,
    strategy: str,
    product: str,
    leverage: float = 3.0,
    slippage_pct: float = 1.0,
    passphrase: str | None = None,
) -> tuple[bool, str]:
    strategy = (strategy or "").lower()
    if strategy not in SUPPORTED_STRATEGIES:
        return False, "Unknown strategy."

    product_id = get_product_id(product)
    if product_id is None:
        return False, f"Unknown product '{product}'."
    max_leverage = get_product_max_leverage(product)
    if float(leverage or 0) > max_leverage:
        return False, f"Max leverage for {product.upper()} is {max_leverage}x."
    if float(leverage or 0) < 1:
        return False, "Leverage must be at least 1x."

    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    _, strat_cfg = get_strategy_settings(telegram_id, strategy)
    state = _default_state()
    state.update(_strategy_defaults(strategy))
    state.update(strat_cfg)
    state.update(
        {
            "running": True,
            "strategy": strategy,
            "product": product.upper(),
            "leverage": float(leverage or 3.0),
            "slippage_pct": float(slippage_pct or 1.0),
            "reference_price": 0.0,
            "started_at": datetime.utcnow().isoformat(),
            "last_run_ts": 0.0,
            "last_error": None,
            "runs": 0,
        }
    )
    _save_state(telegram_id, network, state)
    if passphrase:
        set_runtime_passphrase(telegram_id, network, passphrase)
    _ensure_task(telegram_id, network)
    return (
        True,
        f"{strategy.upper()} bot started on {product.upper()}-PERP ({network}) "
        f"| TP {state.get('tp_pct')}% / SL {state.get('sl_pct')}%",
    )


def stop_user_bot(telegram_id: int, cancel_orders: bool = True) -> tuple[bool, str]:
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    state = _load_state(telegram_id, network)
    if not state.get("running"):
        return False, "No running strategy bot found."

    state["running"] = False
    _save_state(telegram_id, network, state)

    tk = _task_key(telegram_id, network)
    task = _tasks.pop(tk, None)
    if task:
        task.cancel()

    if cancel_orders:
        close_all_positions(telegram_id, passphrase=_session_passphrases.get(tk))
    _session_passphrases.pop(tk, None)

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
            state["running"] = False
            set_bot_state(key, state)
            tk = _task_key(telegram_id, network)
            task = _tasks.pop(tk, None)
            if task:
                task.cancel()
            if cancel_orders:
                passphrase = _session_passphrases.get(tk)
                if passphrase:
                    close_res = close_all_positions(telegram_id, passphrase=passphrase)
                    if not close_res.get("success"):
                        close_errors.append(f"{network}: {close_res.get('error', 'close_all_positions failed')}")
                else:
                    close_errors.append(f"{network}: no active strategy session passphrase; skipped close-all")
            _session_passphrases.pop(tk, None)
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
    return {
        "network": network,
        "running": bool(state.get("running")),
        "strategy": state.get("strategy"),
        "product": state.get("product"),
        "notional_usd": state.get("notional_usd"),
        "spread_bp": state.get("spread_bp"),
        "tp_pct": state.get("tp_pct"),
        "sl_pct": state.get("sl_pct"),
        "interval_seconds": state.get("interval_seconds"),
        "started_at": state.get("started_at"),
        "runs": state.get("runs", 0),
        "last_error": state.get("last_error"),
        "pause_reason": state.get("mm_pause_reason") or "",
        "is_paused": bool(state.get("mm_paused")),
        "maker_fill_ratio": (state.get("mm_last_metrics") or {}).get("maker_fill_ratio"),
        "cancellation_ratio": (state.get("mm_last_metrics") or {}).get("cancellation_ratio"),
        "avg_quote_distance_bp": (state.get("mm_last_metrics") or {}).get("avg_quote_distance_bp"),
        "quote_refresh_rate": (state.get("mm_last_metrics") or {}).get("quote_refresh_rate"),
        "inventory_skew_usd": (state.get("mm_last_metrics") or {}).get("inventory_skew_usd"),
        "session_notional_done_usd": (state.get("mm_last_metrics") or {}).get("session_notional_done_usd"),
    }


def stop_runtime():
    for task_id, task in list(_tasks.items()):
        task.cancel()
        _tasks.pop(task_id, None)
    _session_passphrases.clear()
    _manual_session_passphrases.clear()


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
    await _notify(telegram_id, f"Strategy loop started on {network}.")
    try:
        while True:
            state = _load_state(telegram_id, network)
            if not state.get("running"):
                break
            try:
                now_bucket = int(time.time() / max(1, RUNTIME_TICK_SECONDS))
                await enqueue_strategy(
                    {"telegram_id": telegram_id, "network": network},
                    dedupe_key=f"{telegram_id}:{network}:{now_bucket}",
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
    state = _load_state(telegram_id, network)
    if not state.get("running"):
        return
    cycle_started = time.perf_counter()
    try:
        ok, error_msg = await _run_cycle(telegram_id, network, state)
        if ok:
            refreshed = _load_state(telegram_id, network)
            if refreshed.get("error_streak") or refreshed.get("last_error"):
                refreshed["error_streak"] = 0
                refreshed["last_error"] = None
                _save_state(telegram_id, network, refreshed)
        else:
            await _mark_cycle_error(telegram_id, network, error_msg or "unknown cycle error")
    except Exception as e:
        logger.error("Strategy cycle crash for user %s on %s: %s", telegram_id, network, e, exc_info=True)
        await _mark_cycle_error(telegram_id, network, str(e))
    finally:
        record_metric("runtime.strategy_cycle.total", (time.perf_counter() - cycle_started) * 1000.0)


async def _mark_cycle_error(telegram_id: int, network: str, error_msg: str):
    state = _load_state(telegram_id, network)
    if not state.get("running"):
        return
    state["last_error"] = str(error_msg)[:300]
    streak = int(state.get("error_streak") or 0) + 1
    state["error_streak"] = streak
    _save_state(telegram_id, network, state)
    if streak == STRATEGY_ERROR_ALERT_STREAK:
        strategy = str(state.get("strategy") or "").upper() or "STRATEGY"
        product = str(state.get("product") or "?")
        await _notify(
            telegram_id,
            (
                f"⚠️ {strategy} health alert on {product}-PERP ({network}): "
                f"{streak} consecutive cycle errors.\n"
                f"Latest: {str(error_msg)[:220]}"
            ),
        )


def _dispatch_strategy(strategy: str, telegram_id: int, network: str, state: dict,
                       client, mid: float, product_id: int, product: str, open_orders: list,
                       passphrase: str) -> dict:
    from src.nadobro.strategies import mm_bot, delta_neutral, volume_bot

    if strategy in ("mm", "grid"):
        return mm_bot.run_cycle(
            telegram_id, network, state,
            client=client, mid=mid, open_orders=open_orders, passphrase=passphrase,
        )
    elif strategy == "dn":
        return delta_neutral.run_cycle(
            telegram_id, network, state,
            client=client, mid=mid, product_id=product_id,
            product=product, open_orders=open_orders, passphrase=passphrase,
        )
    elif strategy == "vol":
        return volume_bot.run_cycle(telegram_id, network, state, passphrase=passphrase)
    else:
        return {"success": False, "error": f"Unknown strategy '{strategy}'"}


async def _run_cycle(telegram_id: int, network: str, state: dict) -> tuple[bool, str | None]:
    if is_trading_paused():
        if str(state.get("strategy", "")).lower() == "dn" and float(state.get("auto_close_on_maintenance") or 0) >= 0.5:
            state["running"] = False
            state["last_error"] = "Auto-closed on maintenance pause."
            _save_state(telegram_id, network, state)
            tk = _task_key(telegram_id, network)
            await run_blocking(close_all_positions, telegram_id, _session_passphrases.get(tk))
            await _notify(telegram_id, "Delta Neutral stopped and auto-closed due to maintenance pause.")
        return True, None
    user = await run_blocking(get_user, telegram_id)
    if not user:
        state["running"] = False
        _save_state(telegram_id, network, state)
        return True, None
    if user.network_mode.value != network:
        state["running"] = False
        state["last_error"] = f"Stopped because active mode switched to {user.network_mode.value}"
        _save_state(telegram_id, network, state)
        await _notify(
            telegram_id,
            f"Stopped {state.get('strategy', '').upper()} loop on {network}: active mode changed to {user.network_mode.value}.",
        )
        return True, None

    tk = _task_key(telegram_id, network)
    session_passphrase = _session_passphrases.get(tk)
    if not session_passphrase:
        state["running"] = False
        state["last_error"] = "Strategy session expired. Restart strategy and enter passphrase again."
        _save_state(telegram_id, network, state)
        await _notify(
            telegram_id,
            f"⚠️ {str(state.get('strategy', '')).upper()} stopped on {network}: strategy session expired. "
            f"Restart strategy and enter passphrase again.",
        )
        return True, None

    last_run = float(state.get("last_run_ts") or 0.0)
    interval = int(state.get("interval_seconds") or 60)
    if time.time() - last_run < interval:
        return True, None

    product = state.get("product", "BTC")
    strategy = state.get("strategy")
    product_id = get_product_id(product)
    if product_id is None:
        raise RuntimeError(f"Invalid product '{product}'")

    client = await run_blocking(get_user_readonly_client, telegram_id)
    if not client:
        raise RuntimeError("Wallet client unavailable")

    with timed_metric("runtime.market_price.fetch"):
        mp = await run_blocking(client.get_market_price, product_id)
    mid = float(mp.get("mid") or 0.0)
    if mid <= 0:
        raise RuntimeError("Could not fetch market price")

    reference_price = float(state.get("reference_price") or 0.0)
    if reference_price <= 0:
        state["reference_price"] = mid
        _save_state(telegram_id, network, state)
        reference_price = mid

    move_pct = abs((mid - reference_price) / reference_price) * 100.0 if reference_price > 0 else 0.0
    sl_pct = float(state.get("sl_pct") or 0.0)
    tp_pct = float(state.get("tp_pct") or 0.0)
    if sl_pct > 0 and move_pct >= sl_pct:
        state["running"] = False
        state["last_error"] = f"Stopped by SL at {move_pct:.2f}% move from reference."
        _save_state(telegram_id, network, state)
        await run_blocking(close_all_positions, telegram_id, session_passphrase)
        await _notify(
            telegram_id,
            f"🛑 {strategy.upper()} stopped on {product}-PERP ({network}) - SL hit ({move_pct:.2f}%).",
        )
        return True, None
    if tp_pct > 0 and move_pct >= tp_pct:
        state["running"] = False
        state["last_error"] = None
        _save_state(telegram_id, network, state)
        await run_blocking(close_all_positions, telegram_id, session_passphrase)
        await _notify(
            telegram_id,
            f"✅ {strategy.upper()} target reached on {product}-PERP ({network}) - TP hit ({move_pct:.2f}%).",
        )
        return True, None

    with timed_metric("runtime.open_orders.fetch"):
        open_orders = await run_blocking(client.get_open_orders, product_id)

    with timed_metric(f"runtime.strategy.dispatch.{strategy}"):
        result = await run_blocking(
            _dispatch_strategy,
            strategy, telegram_id, network, state,
            client, mid, product_id, product, open_orders, session_passphrase,
        )

    if not state.get("running", True):
        _save_state(telegram_id, network, state)
        await _notify(telegram_id, f"✅ {strategy.upper()} completed on {product}-PERP ({network}).")
        return True, None

    state["last_run_ts"] = time.time()
    state["runs"] = int(state.get("runs") or 0) + 1
    state["last_error"] = result.get("error")
    _save_state(telegram_id, network, state)
    drift_seconds = max(0.0, state["last_run_ts"] - last_run - interval) if last_run > 0 else 0.0
    if drift_seconds > 0:
        record_metric("runtime.cycle_drift_ms", drift_seconds * 1000.0)

    if not result.get("success", True):
        error_msg = str(result.get("error", "unknown"))[:300]
        return False, error_msg
    return True, None
