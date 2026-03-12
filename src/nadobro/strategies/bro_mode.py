import logging
import time
import json
from datetime import datetime

from src.nadobro.config import get_product_id, get_product_name, get_product_max_leverage
from src.nadobro.services.trade_service import execute_market_order
from src.nadobro.services.budget_guard import (
    check_can_open_position,
    should_emergency_flatten,
    get_budget_status,
    get_risk_limits,
    compute_position_size,
)
from src.nadobro.services.market_scanner import build_market_snapshot, format_snapshot_for_llm
from src.nadobro.services.bro_llm import make_decision

logger = logging.getLogger(__name__)

BRO_CYCLE_SECONDS = 300
LLM_FAIL_PAUSE_AFTER = 3
LLM_FAIL_PAUSE_CYCLES = 6


def run_cycle(
    telegram_id: int,
    network: str,
    state: dict,
    client=None,
    passphrase: str = None,
    **kwargs,
) -> dict:
    if not client:
        return {"success": False, "error": "No trading client available"}

    bro_state = state.get("bro_state", {})
    if not bro_state:
        bro_state = _init_bro_state()
        state["bro_state"] = bro_state

    llm_fail_count = int(bro_state.get("llm_fail_streak", 0))
    if llm_fail_count >= LLM_FAIL_PAUSE_AFTER:
        pause_until = float(bro_state.get("llm_pause_until", 0))
        if time.time() < pause_until:
            remaining = int(pause_until - time.time())
            return {"success": True, "action": "paused", "detail": f"LLM failing, paused for {remaining}s"}
        bro_state["llm_fail_streak"] = 0
        bro_state["llm_pause_until"] = 0

    products = state.get("products") or ["BTC", "ETH", "SOL"]
    risk_level = state.get("risk_level", "balanced")
    budget = float(state.get("budget_usd", 500))
    min_confidence = float(state.get("min_confidence", 0.65))
    use_cmc = bool(state.get("use_cmc", True))
    use_sentiment = bool(state.get("use_sentiment", True))
    leverage_cap = int(state.get("leverage_cap", 5))
    max_loss_pct = float(state.get("max_loss_pct", 15))

    bro_settings = {
        "budget_usd": budget,
        "risk_level": risk_level,
        "max_loss_pct": max_loss_pct,
    }

    flatten, flatten_reason = should_emergency_flatten(telegram_id, bro_settings)
    if flatten:
        logger.warning("Bro Mode emergency flatten for user %s: %s", telegram_id, flatten_reason)
        _emergency_close_all(telegram_id, network, state, passphrase, products)
        state["running"] = False
        state["last_error"] = f"Emergency flatten: {flatten_reason}"
        bro_state["stopped_reason"] = flatten_reason
        bro_state["stopped_at"] = datetime.utcnow().isoformat()
        return {"success": True, "action": "emergency_flatten", "detail": flatten_reason}

    budget_status = get_budget_status(telegram_id, bro_settings)
    if budget_status.get("error"):
        return {"success": False, "error": f"Budget check failed: {budget_status['error']}"}

    limits = get_risk_limits(risk_level)
    max_positions = min(int(state.get("max_positions", 3)), limits["max_positions"])
    max_leverage = min(leverage_cap, limits["max_leverage"])

    try:
        snapshot = build_market_snapshot(
            client=client,
            products=products,
            use_cmc=use_cmc,
            use_sentiment=use_sentiment,
        )
    except Exception as e:
        logger.error("Market scan failed: %s", e)
        return {"success": False, "error": f"Market scan failed: {str(e)[:100]}"}

    snapshot_text = format_snapshot_for_llm(snapshot)

    positions = budget_status.get("positions", [])
    exposure = budget_status.get("current_exposure", 0)
    remaining = budget_status.get("remaining_budget", 0)

    try:
        decision = make_decision(
            market_snapshot_text=snapshot_text,
            products=products,
            risk_level=risk_level,
            budget=budget,
            exposure=exposure,
            remaining=remaining,
            max_leverage=max_leverage,
            max_positions=max_positions,
            positions=positions,
            min_confidence=min_confidence,
        )
        bro_state["llm_fail_streak"] = 0
    except Exception as e:
        logger.error("LLM decision failed: %s", e)
        bro_state["llm_fail_streak"] = llm_fail_count + 1
        if bro_state["llm_fail_streak"] >= LLM_FAIL_PAUSE_AFTER:
            bro_state["llm_pause_until"] = time.time() + BRO_CYCLE_SECONDS * LLM_FAIL_PAUSE_CYCLES
        return {"success": False, "error": f"LLM decision failed: {str(e)[:100]}"}

    action = decision.get("action", "hold")

    bro_state["last_decision"] = {
        "action": action,
        "confidence": decision.get("confidence", 0),
        "reasoning": decision.get("reasoning", ""),
        "product": decision.get("product", ""),
        "ts": time.time(),
    }

    decisions_log = bro_state.get("decisions_log", [])
    decisions_log.append({
        "action": action,
        "confidence": decision.get("confidence", 0),
        "product": decision.get("product", ""),
        "reasoning": decision.get("reasoning", "")[:150],
        "ts": time.time(),
    })
    bro_state["decisions_log"] = decisions_log[-50:]

    if action == "hold":
        bro_state["consecutive_holds"] = int(bro_state.get("consecutive_holds", 0)) + 1
        return {
            "success": True,
            "action": "hold",
            "detail": decision.get("reasoning", "No good setup"),
            "confidence": decision.get("confidence", 0),
        }

    bro_state["consecutive_holds"] = 0

    if action in ("open_long", "open_short"):
        return _handle_open(
            telegram_id, network, state, decision,
            budget, remaining, max_leverage, max_positions,
            bro_settings, passphrase,
        )

    if action == "close":
        return _handle_close(
            telegram_id, network, state, decision, passphrase, positions,
        )

    return {"success": True, "action": action, "detail": decision.get("reasoning", "")}


def _init_bro_state() -> dict:
    return {
        "llm_fail_streak": 0,
        "llm_pause_until": 0,
        "consecutive_holds": 0,
        "total_decisions": 0,
        "total_trades": 0,
        "total_pnl": 0.0,
        "decisions_log": [],
        "trades_log": [],
        "last_decision": None,
        "started_at": datetime.utcnow().isoformat(),
    }


def _handle_open(
    telegram_id, network, state, decision,
    budget, remaining, max_leverage, max_positions,
    bro_settings, passphrase,
) -> dict:
    product = decision.get("product", "BTC").upper()
    is_long = decision["action"] == "open_long"
    confidence = decision.get("confidence", 0)
    leverage = min(int(decision.get("leverage", 3)), max_leverage)
    size_pct = float(decision.get("size_pct", 0.3))
    tp_pct = float(decision.get("tp_pct", 2.0))
    sl_pct = float(decision.get("sl_pct", 1.0))

    product_id = get_product_id(product)
    if product_id is None:
        return {"success": False, "error": f"Unknown product '{product}'"}

    notional_usd = remaining * size_pct
    if notional_usd < 10:
        return {"success": True, "action": "hold", "detail": "Position too small (< $10)"}

    can_open, reason = check_can_open_position(
        telegram_id, notional_usd, leverage, bro_settings,
    )
    if not can_open:
        return {"success": True, "action": "blocked", "detail": reason}

    from src.nadobro.services.user_service import get_user_readonly_client
    ro_client = get_user_readonly_client(telegram_id)
    if not ro_client:
        return {"success": False, "error": "Cannot get price client"}

    mp = ro_client.get_market_price(product_id)
    mid = float(mp.get("mid", 0))
    if mid <= 0:
        return {"success": False, "error": f"Cannot fetch {product} price"}

    size = notional_usd / mid

    if is_long:
        tp_price = mid * (1 + tp_pct / 100)
        sl_price = mid * (1 - sl_pct / 100)
    else:
        tp_price = mid * (1 - tp_pct / 100)
        sl_price = mid * (1 + sl_pct / 100)

    result = execute_market_order(
        telegram_id=telegram_id,
        product=product,
        size=size,
        is_long=is_long,
        leverage=leverage,
        slippage_pct=1.0,
        enforce_rate_limit=False,
        passphrase=passphrase,
        tp_price=tp_price,
        sl_price=sl_price,
    )

    bro_state = state.get("bro_state", {})

    if result.get("success"):
        bro_state["total_trades"] = int(bro_state.get("total_trades", 0)) + 1
        trade_record = {
            "product": product,
            "side": "long" if is_long else "short",
            "size": size,
            "notional_usd": notional_usd,
            "entry_price": mid,
            "leverage": leverage,
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "confidence": confidence,
            "reasoning": decision.get("reasoning", "")[:200],
            "signals": decision.get("signals", []),
            "ts": time.time(),
            "digest": result.get("digest", ""),
        }
        trades_log = bro_state.get("trades_log", [])
        trades_log.append(trade_record)
        bro_state["trades_log"] = trades_log[-100:]

        logger.info(
            "Bro Mode OPENED %s %s: $%.0f @ $%,.2f (%dx) conf=%.0f%% — %s",
            product, "LONG" if is_long else "SHORT",
            notional_usd, mid, leverage, confidence * 100,
            decision.get("reasoning", "")[:100],
        )

        return {
            "success": True,
            "action": decision["action"],
            "product": product,
            "side": "long" if is_long else "short",
            "size": size,
            "notional_usd": notional_usd,
            "entry_price": mid,
            "leverage": leverage,
            "confidence": confidence,
            "reasoning": decision.get("reasoning", ""),
            "tp_price": tp_price,
            "sl_price": sl_price,
        }
    else:
        return {"success": False, "error": f"Order failed: {result.get('error', 'unknown')}"}


def _handle_close(
    telegram_id, network, state, decision, passphrase, positions,
) -> dict:
    close_product = decision.get("close_product", decision.get("product", "")).upper()
    if not close_product:
        return {"success": True, "action": "hold", "detail": "No product specified for close"}

    matching = [p for p in positions if p.get("product", "").upper() == close_product]
    if not matching:
        return {"success": True, "action": "hold", "detail": f"No open position for {close_product}"}

    pos = matching[0]
    product_id = get_product_id(close_product)
    if product_id is None:
        return {"success": False, "error": f"Unknown product '{close_product}'"}

    is_long = pos.get("side", "").lower() == "long"
    size = pos.get("size", 0)

    result = execute_market_order(
        telegram_id=telegram_id,
        product=close_product,
        size=size,
        is_long=not is_long,
        leverage=1.0,
        slippage_pct=1.5,
        enforce_rate_limit=False,
        passphrase=passphrase,
    )

    if result.get("success"):
        bro_state = state.get("bro_state", {})
        pnl = pos.get("unrealized_pnl", 0)
        bro_state["total_pnl"] = float(bro_state.get("total_pnl", 0)) + pnl

        logger.info(
            "Bro Mode CLOSED %s %s: PnL=$%.2f — %s",
            close_product, pos.get("side", "?").upper(),
            pnl, decision.get("reasoning", "")[:100],
        )

        return {
            "success": True,
            "action": "close",
            "product": close_product,
            "pnl": pnl,
            "reasoning": decision.get("reasoning", ""),
        }

    return {"success": False, "error": f"Close failed: {result.get('error', 'unknown')}"}


def _emergency_close_all(telegram_id, network, state, passphrase, products):
    from src.nadobro.services.trade_service import close_all_positions
    try:
        result = close_all_positions(telegram_id, passphrase=passphrase)
        if result.get("success"):
            logger.info("Bro Mode emergency flatten successful for user %s", telegram_id)
        else:
            logger.error("Bro Mode emergency flatten failed for user %s: %s", telegram_id, result.get("error"))
    except Exception as e:
        logger.error("Bro Mode emergency flatten exception for user %s: %s", telegram_id, e)


def get_bro_status(state: dict) -> dict:
    bro_state = state.get("bro_state", {})
    last_decision = bro_state.get("last_decision", {})
    return {
        "active": bool(state.get("running")),
        "started_at": bro_state.get("started_at"),
        "total_decisions": len(bro_state.get("decisions_log", [])),
        "total_trades": bro_state.get("total_trades", 0),
        "total_pnl": bro_state.get("total_pnl", 0),
        "consecutive_holds": bro_state.get("consecutive_holds", 0),
        "last_action": last_decision.get("action", "none"),
        "last_confidence": last_decision.get("confidence", 0),
        "last_reasoning": last_decision.get("reasoning", ""),
        "last_product": last_decision.get("product", ""),
        "last_ts": last_decision.get("ts", 0),
        "llm_fail_streak": bro_state.get("llm_fail_streak", 0),
        "risk_level": state.get("risk_level", "balanced"),
        "budget_usd": state.get("budget_usd", 500),
        "products": state.get("products", ["BTC", "ETH", "SOL"]),
    }
