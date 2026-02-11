import asyncio
import json
import logging
import time
from datetime import datetime

from src.nadobro.config import get_product_id
from src.nadobro.models.database import BotState, get_session
from src.nadobro.services.admin_service import is_trading_paused
from src.nadobro.services.settings_service import get_strategy_settings
from src.nadobro.services.trade_service import execute_limit_order, close_all_positions
from src.nadobro.services.user_service import get_user_nado_client, get_user

logger = logging.getLogger(__name__)

STATE_PREFIX = "strategy_bot:"
RUNTIME_TICK_SECONDS = 20
MAX_OPEN_ORDERS_PER_PRODUCT = 6

_bot_app = None
_tasks: dict[str, asyncio.Task] = {}


def set_bot_app(app):
    global _bot_app
    _bot_app = app


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
        "runs": 0,
    }


def _load_state(telegram_id: int, network: str) -> dict:
    with get_session() as session:
        row = session.query(BotState).filter_by(key=_state_key(telegram_id, network)).first()
        if not row or not row.value:
            return _default_state()
        try:
            loaded = json.loads(row.value)
            state = _default_state()
            state.update(loaded if isinstance(loaded, dict) else {})
            return state
        except Exception:
            logger.warning("Invalid bot state JSON for user %s", telegram_id)
            return _default_state()


def _save_state(telegram_id: int, network: str, state: dict):
    with get_session() as session:
        row = session.query(BotState).filter_by(key=_state_key(telegram_id, network)).first()
        payload = json.dumps(state)
        if row:
            row.value = payload
            row.updated_at = datetime.utcnow()
        else:
            row = BotState(key=_state_key(telegram_id, network), value=payload)
            session.add(row)
        session.commit()


async def _notify(telegram_id: int, text: str):
    if not _bot_app:
        return
    try:
        await _bot_app.bot.send_message(chat_id=telegram_id, text=text)
    except Exception as e:
        logger.warning("Notify failed for %s: %s", telegram_id, e)


def _strategy_defaults(strategy: str) -> dict:
    presets = {
        "mm": {"notional_usd": 75.0, "spread_bp": 4.0, "interval_seconds": 45},
        "grid": {"notional_usd": 100.0, "spread_bp": 10.0, "interval_seconds": 60},
        "dn": {"notional_usd": 50.0, "spread_bp": 3.0, "interval_seconds": 90},
    }
    return presets.get(strategy, {"notional_usd": 100.0, "spread_bp": 5.0, "interval_seconds": 60})


def start_user_bot(
    telegram_id: int,
    strategy: str,
    product: str,
    leverage: float = 3.0,
    slippage_pct: float = 1.0,
) -> tuple[bool, str]:
    strategy = (strategy or "").lower()
    if strategy not in ("mm", "grid", "dn"):
        return False, "Unknown strategy."

    product_id = get_product_id(product)
    if product_id is None:
        return False, f"Unknown product '{product}'."

    user = get_user(telegram_id)
    network = user.network_mode.value if user else "testnet"
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
    _ensure_task(telegram_id, network)
    return (
        True,
        f"{strategy.upper()} bot started on {product.upper()}-PERP ({network}) "
        f"| TP {state.get('tp_pct')}% / SL {state.get('sl_pct')}%",
    )


def stop_user_bot(telegram_id: int, cancel_orders: bool = True) -> tuple[bool, str]:
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "testnet"
    state = _load_state(telegram_id, network)
    if not state.get("running"):
        return False, "No running strategy bot found."

    state["running"] = False
    _save_state(telegram_id, network, state)

    task = _tasks.pop(_task_key(telegram_id, network), None)
    if task:
        task.cancel()

    if cancel_orders:
        close_all_positions(telegram_id)

    return True, "Strategy bot stopped. Open orders cancellation requested."


def get_user_bot_status(telegram_id: int) -> dict:
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "testnet"
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
    }


def stop_runtime():
    for task_id, task in list(_tasks.items()):
        task.cancel()
        _tasks.pop(task_id, None)


def restore_running_bots():
    with get_session() as session:
        rows = session.query(BotState).filter(BotState.key.like(f"{STATE_PREFIX}%")).all()
        for row in rows:
            try:
                user_network = row.key.replace(STATE_PREFIX, "")
                user_id_str, network = user_network.split(":")
                user_id = int(user_id_str)
                state = json.loads(row.value or "{}")
                if state.get("running"):
                    _ensure_task(user_id, network)
            except Exception:
                continue


def _ensure_task(telegram_id: int, network: str):
    tk = _task_key(telegram_id, network)
    task = _tasks.get(tk)
    if task and not task.done():
        return
    _tasks[tk] = asyncio.create_task(_bot_loop(telegram_id, network))


async def _bot_loop(telegram_id: int, network: str):
    logger.info("Starting strategy loop for user %s on %s", telegram_id, network)
    await _notify(telegram_id, f"Strategy loop started on {network}.")
    try:
        while True:
            state = _load_state(telegram_id, network)
            if not state.get("running"):
                break
            try:
                await _run_cycle(telegram_id, network, state)
            except Exception as cycle_error:
                logger.error("Cycle failure for user %s: %s", telegram_id, cycle_error, exc_info=True)
                state["last_error"] = str(cycle_error)
                _save_state(telegram_id, network, state)
            await asyncio.sleep(RUNTIME_TICK_SECONDS)
    except asyncio.CancelledError:
        logger.info("Strategy loop cancelled for user %s", telegram_id)
    finally:
        _tasks.pop(_task_key(telegram_id, network), None)


async def _run_cycle(telegram_id: int, network: str, state: dict):
    if is_trading_paused():
        return
    user = get_user(telegram_id)
    if not user:
        state["running"] = False
        _save_state(telegram_id, network, state)
        return
    if user.network_mode.value != network:
        state["running"] = False
        state["last_error"] = f"Stopped because active mode switched to {user.network_mode.value}"
        _save_state(telegram_id, network, state)
        await _notify(
            telegram_id,
            f"Stopped {state.get('strategy', '').upper()} loop on {network}: active mode changed to {user.network_mode.value}.",
        )
        return

    last_run = float(state.get("last_run_ts") or 0.0)
    interval = int(state.get("interval_seconds") or 60)
    if time.time() - last_run < interval:
        return

    product = state.get("product", "BTC")
    strategy = state.get("strategy")
    leverage = float(state.get("leverage") or 3.0)
    product_id = get_product_id(product)
    if product_id is None:
        raise RuntimeError(f"Invalid product '{product}'")

    client = get_user_nado_client(telegram_id)
    if not client:
        raise RuntimeError("Wallet client unavailable")

    mp = client.get_market_price(product_id)
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
        close_all_positions(telegram_id)
        await _notify(
            telegram_id,
            f"🛑 {strategy.upper()} stopped on {product}-PERP ({network}) - SL hit ({move_pct:.2f}%).",
        )
        return
    if tp_pct > 0 and move_pct >= tp_pct:
        state["running"] = False
        state["last_error"] = None
        _save_state(telegram_id, network, state)
        close_all_positions(telegram_id)
        await _notify(
            telegram_id,
            f"✅ {strategy.upper()} target reached on {product}-PERP ({network}) - TP hit ({move_pct:.2f}%).",
        )
        return

    open_orders = client.get_open_orders(product_id)
    if len(open_orders) >= MAX_OPEN_ORDERS_PER_PRODUCT:
        state["last_run_ts"] = time.time()
        _save_state(telegram_id, network, state)
        return

    notional = float(state.get("notional_usd") or 100.0)
    size = max(notional / mid, 0.0001)
    spread_bp = float(state.get("spread_bp") or 5.0)

    if strategy == "grid":
        spread_bp = max(spread_bp, 8.0)
    elif strategy == "dn":
        spread_bp = max(2.0, min(spread_bp, 4.0))

    half_spread = spread_bp / 10000.0
    buy_price = mid * (1.0 - half_spread)
    sell_price = mid * (1.0 + half_spread)

    # Keep each cycle bounded to 2 maker orders.
    buy_result = execute_limit_order(telegram_id, product, size, buy_price, is_long=True, leverage=leverage)
    sell_result = execute_limit_order(telegram_id, product, size, sell_price, is_long=False, leverage=leverage)

    state["last_run_ts"] = time.time()
    state["runs"] = int(state.get("runs") or 0) + 1
    state["last_error"] = None
    _save_state(telegram_id, network, state)

    if not buy_result.get("success") or not sell_result.get("success"):
        errs = []
        if not buy_result.get("success"):
            errs.append(f"buy: {buy_result.get('error')}")
        if not sell_result.get("success"):
            errs.append(f"sell: {sell_result.get('error')}")
        msg = "; ".join(errs)[:300]
        await _notify(telegram_id, f"{strategy.upper()} cycle had errors: {msg}")
