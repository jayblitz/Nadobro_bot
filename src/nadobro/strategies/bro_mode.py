import logging
import time
import json
from datetime import datetime

from src.nadobro.config import get_product_id, get_product_name, get_product_max_leverage, get_perp_products
from src.nadobro.services.trade_service import execute_market_order
from src.nadobro.services.budget_guard import (
    check_can_open_position,
    should_emergency_flatten,
    get_budget_status,
    get_risk_limits,
    compute_position_size,
    get_bro_profile,
    get_bro_profile_limits,
    get_copy_exposure,
)
from src.nadobro.services.market_scanner import build_market_snapshot, format_snapshot_for_llm
from src.nadobro.services.bro_llm import make_decision

logger = logging.getLogger(__name__)

BRO_CYCLE_SECONDS = 300
LLM_FAIL_PAUSE_AFTER = 3
LLM_FAIL_PAUSE_CYCLES = 6

COOLDOWN_CYCLES = 3
COOLDOWN_CONFIDENCE_BUMP = 0.15


def _normalize_product_symbol(product: str) -> str:
    raw = str(product or "").upper().strip()
    if raw.endswith("-PERP"):
        raw = raw[:-5]
    return raw


def run_cycle(
    telegram_id: int,
    network: str,
    state: dict,
    client=None,
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

    products = state.get("products") or get_perp_products(network=network)[:6] or ["BTC", "ETH", "SOL"]
    risk_level = state.get("risk_level", "balanced")
    budget = float(state.get("budget_usd", 500))
    min_confidence = float(state.get("min_confidence", 0.65))
    use_cmc = bool(state.get("use_cmc", True))
    use_sentiment = bool(state.get("use_sentiment", True))
    leverage_cap = int(state.get("leverage_cap", 5))
    max_loss_pct = float(state.get("max_loss_pct", 15))
    bro_profile = state.get("bro_profile", "normal")

    profile_data = get_bro_profile(bro_profile)
    risk_level = profile_data.get("risk_level", risk_level)
    min_confidence = max(min_confidence, profile_data.get("min_confidence", 0))
    leverage_cap = min(leverage_cap, profile_data.get("leverage_cap", leverage_cap))
    max_loss_pct = profile_data.get("max_loss_pct", max_loss_pct)

    bro_settings = {
        "budget_usd": budget,
        "risk_level": risk_level,
        "max_loss_pct": max_loss_pct,
        "max_daily_loss_usd": profile_data.get("max_daily_loss_usd"),
    }

    flatten, flatten_reason = should_emergency_flatten(telegram_id, bro_settings)
    if flatten:
        logger.warning("Bro Mode emergency flatten for user %s: %s", telegram_id, flatten_reason)
        _emergency_close_all(telegram_id, network, state, products)
        state["running"] = False
        state["last_error"] = f"Emergency flatten: {flatten_reason}"
        bro_state["stopped_reason"] = flatten_reason
        bro_state["stopped_at"] = datetime.utcnow().isoformat()
        return {"success": True, "action": "emergency_flatten", "detail": flatten_reason}

    budget_status = get_budget_status(telegram_id, bro_settings)
    if budget_status.get("error"):
        return {"success": False, "error": f"Budget check failed: {budget_status['error']}"}

    profile_limits = get_bro_profile_limits(bro_profile)
    max_positions = min(int(state.get("max_positions", 3)), profile_limits["max_positions"])
    max_leverage = min(leverage_cap, profile_limits["max_leverage"])

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

    if profile_data.get("block_high_vol"):
        for asset in snapshot.get("assets", []):
            regime = asset.get("regime")
            if regime in ("high_vol_chop", "news_spike"):
                logger.info("Chill Bro: skipping cycle — %s in %s regime", asset.get("product"), regime)
                return {"success": True, "action": "hold", "detail": f"Chill mode: {asset.get('product')} in {regime} — staying safe"}

    if profile_data.get("block_extreme_funding"):
        for asset in snapshot.get("assets", []):
            fr = asset.get("funding_rate")
            if fr is not None and abs(fr) > 0.001:
                logger.info("Chill Bro: extreme funding on %s (%.6f), holding", asset.get("product"), fr)
                return {"success": True, "action": "hold", "detail": f"Chill mode: extreme funding on {asset.get('product')} ({fr:.6f}) — staying safe"}

    positions = budget_status.get("positions", [])
    exposure = budget_status.get("current_exposure", 0)
    remaining = budget_status.get("remaining_budget", 0)
    copy_exposure = budget_status.get("copy_exposure", 0)

    recent_closes = bro_state.get("recent_closes", [])

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
            bro_profile=bro_profile,
            copy_exposure=copy_exposure,
            recent_closes=recent_closes,
        )
        bro_state["llm_fail_streak"] = 0
    except Exception as e:
        logger.error("LLM decision failed: %s", e)
        bro_state["llm_fail_streak"] = llm_fail_count + 1
        if bro_state["llm_fail_streak"] >= LLM_FAIL_PAUSE_AFTER:
            bro_state["llm_pause_until"] = time.time() + BRO_CYCLE_SECONDS * LLM_FAIL_PAUSE_CYCLES
        return {"success": False, "error": f"LLM decision failed: {str(e)[:100]}"}

    if (
        decision.get("action") == "hold"
        and any(
            marker in str(decision.get("reasoning", "")).lower()
            for marker in ("llm client not available", "llm error", "failed to parse llm")
        )
    ):
        decision = _fallback_bro_decision(snapshot, positions, min_confidence, max_leverage)

    action = decision.get("action", "hold")

    bro_state["last_decision"] = {
        "action": action,
        "confidence": decision.get("confidence", 0),
        "reasoning": decision.get("reasoning", ""),
        "product": decision.get("product", ""),
        "composite_score": decision.get("composite_score", 0),
        "risk_score": decision.get("risk_score", 0),
        "expected_pnl_pct": decision.get("expected_pnl_pct", 0),
        "ts": time.time(),
    }

    decisions_log = bro_state.get("decisions_log", [])
    decisions_log.append({
        "action": action,
        "confidence": decision.get("confidence", 0),
        "product": decision.get("product", ""),
        "reasoning": decision.get("reasoning", "")[:150],
        "composite_score": decision.get("composite_score", 0),
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
        cooldown_block = _check_cooldown(bro_state, decision)
        if cooldown_block:
            return cooldown_block

        return _handle_open(
            telegram_id, network, state, decision,
            budget, remaining, max_leverage, max_positions,
            bro_settings,
        )

    if action == "close":
        return _handle_close(
            telegram_id, network, state, decision, positions,
        )

    return {"success": True, "action": action, "detail": decision.get("reasoning", "")}


def _check_cooldown(bro_state: dict, decision: dict) -> dict | None:
    recent_closes = bro_state.get("recent_closes", [])
    if not recent_closes:
        return None

    product = decision.get("product", "").upper()
    action = decision.get("action", "")
    new_side = "long" if action == "open_long" else "short"
    confidence = decision.get("confidence", 0)

    now = time.time()
    for rc in recent_closes:
        if rc.get("product", "").upper() != product:
            continue
        if rc.get("side", "") != new_side:
            continue
        cycles_ago = (now - rc.get("ts", 0)) / BRO_CYCLE_SECONDS
        if cycles_ago < COOLDOWN_CYCLES:
            required = rc.get("exit_confidence", 0.5) + COOLDOWN_CONFIDENCE_BUMP
            if confidence < required:
                return {
                    "success": True,
                    "action": "hold",
                    "detail": f"Cooldown: recently closed {product} {new_side} ({cycles_ago:.0f} cycles ago). "
                              f"Need conf>{required:.0%}, got {confidence:.0%}",
                }
    return None


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
        "recent_closes": [],
        "last_decision": None,
        "started_at": datetime.utcnow().isoformat(),
    }


def _score_bro_asset(asset: dict) -> tuple[int, int, list[str]]:
    bull = 0
    bear = 0
    signals: list[str] = []

    signal_1h = str(asset.get("signal_1h") or "").lower()
    if any(token in signal_1h for token in ("bull", "buy", "long", "uptrend")):
        bull += 2
        signals.append("bullish_1h_signal")
    if any(token in signal_1h for token in ("bear", "sell", "short", "downtrend")):
        bear += 2
        signals.append("bearish_1h_signal")

    ema_9 = float(asset.get("ema_9") or 0.0)
    ema_21 = float(asset.get("ema_21") or 0.0)
    ema_50 = float(asset.get("ema_50") or 0.0)
    if ema_9 > ema_21 > ema_50 > 0:
        bull += 2
        signals.append("ema_stack_bullish")
    elif ema_9 < ema_21 < ema_50 and ema_9 > 0:
        bear += 2
        signals.append("ema_stack_bearish")

    ch_1h = float(asset.get("change_1h") or 0.0)
    ch_4h = float(asset.get("change_4h") or 0.0)
    if ch_1h > 0 and ch_4h > 0:
        bull += 1
        signals.append("positive_momentum")
    elif ch_1h < 0 and ch_4h < 0:
        bear += 1
        signals.append("negative_momentum")

    rsi = asset.get("rsi_14")
    if rsi is not None:
        rsi = float(rsi)
        if 52.0 <= rsi <= 68.0:
            bull += 1
            signals.append("rsi_supports_long")
        elif 32.0 <= rsi <= 48.0:
            bear += 1
            signals.append("rsi_supports_short")

    funding_rate = asset.get("funding_rate")
    if funding_rate is not None:
        funding_rate = float(funding_rate)
        if funding_rate > 0:
            bear += 1
            signals.append("positive_funding_favors_short")
        elif funding_rate < 0:
            bull += 1
            signals.append("negative_funding_favors_long")

    regime = str(asset.get("regime") or "").lower()
    if regime == "trending_up":
        bull += 1
        signals.append("regime_trending_up")
    elif regime == "trending_down":
        bear += 1
        signals.append("regime_trending_down")

    return bull, bear, signals


def _fallback_bro_decision(snapshot: dict, positions: list[dict], min_confidence: float, max_leverage: int) -> dict:
    assets = snapshot.get("assets", []) or []
    assets_by_product = {str(asset.get("product") or "").upper(): asset for asset in assets}

    for pos in positions or []:
        product = _normalize_product_symbol(pos.get("product", ""))
        asset = assets_by_product.get(product)
        if not asset:
            continue
        bull, bear, signals = _score_bro_asset(asset)
        pos_side = str(pos.get("side") or "").lower()
        if pos_side == "long" and bear >= 3 and bear > bull:
            return {
                "action": "close",
                "product": product,
                "close_product": product,
                "confidence": min(0.55 + (0.08 * bear), 0.85),
                "reasoning": f"Fallback close: bearish reversal signals detected for {product}.",
                "signals": signals[:6],
            }
        if pos_side == "short" and bull >= 3 and bull > bear:
            return {
                "action": "close",
                "product": product,
                "close_product": product,
                "confidence": min(0.55 + (0.08 * bull), 0.85),
                "reasoning": f"Fallback close: bullish reversal signals detected for {product}.",
                "signals": signals[:6],
            }

    open_products = {_normalize_product_symbol(p.get("product", "")) for p in positions or []}
    best_decision = {
        "action": "hold",
        "reasoning": "Fallback model found no high-conviction setup.",
        "confidence": 0.0,
    }
    best_score = 0
    for asset in assets:
        product = _normalize_product_symbol(asset.get("product", ""))
        if not product or product in open_products:
            continue
        regime = str(asset.get("regime") or "").lower()
        if regime in {"high_vol_chop", "news_spike"}:
            continue
        bull, bear, signals = _score_bro_asset(asset)
        score = max(bull, bear)
        if score < 3:
            continue
        confidence = min(0.55 + (0.08 * score), 0.88)
        if confidence < min_confidence or score < best_score:
            continue
        best_score = score
        best_decision = {
            "action": "open_long" if bull >= bear else "open_short",
            "product": product,
            "confidence": confidence,
            "leverage": min(max_leverage, 3 if score < 5 else 4),
            "size_pct": 0.25 if score < 5 else 0.35,
            "tp_pct": 1.5 if score < 5 else 2.0,
            "sl_pct": 0.8 if score < 5 else 1.0,
            "reasoning": f"Fallback setup: {product} has aligned directional signals.",
            "signals": signals[:6],
            "expected_pnl_pct": min(4.0, 0.8 * score),
            "risk_score": 0.35 if regime.startswith("trending") else 0.5,
        }
    return best_decision


def _handle_open(
    telegram_id, network, state, decision,
    budget, remaining, max_leverage, max_positions,
    bro_settings,
) -> dict:
    product = _normalize_product_symbol(decision.get("product", "BTC"))
    allowed_products = {
        _normalize_product_symbol(p)
        for p in (state.get("products") or get_perp_products(network=network)[:6] or [])
    }
    if allowed_products and product not in allowed_products:
        return {
            "success": True,
            "action": "blocked",
            "detail": f"{product} is outside this Bro Mode product list.",
        }
    is_long = decision["action"] == "open_long"
    confidence = decision.get("confidence", 0)
    leverage = min(int(decision.get("leverage", 3)), max_leverage)
    size_pct = float(decision.get("size_pct", 0.3))
    tp_pct = float(decision.get("tp_pct", 2.0))
    sl_pct = float(decision.get("sl_pct", 1.0))
    expected_pnl_pct = float(decision.get("expected_pnl_pct") or 0.0)
    min_reward_risk = float(state.get("min_reward_risk") or 1.2)
    if expected_pnl_pct > 0 and sl_pct > 0 and (expected_pnl_pct / sl_pct) < min_reward_risk:
        return {
            "success": True,
            "action": "blocked",
            "detail": (
                f"Reward/risk too weak for {product}: expected {expected_pnl_pct:.2f}% "
                f"vs SL {sl_pct:.2f}% (need >= {min_reward_risk:.1f}x)."
            ),
        }

    # Risk guardrails: clamp leverage to product-specific max
    from src.nadobro.config import get_product_max_leverage
    max_product_lev = get_product_max_leverage(product, network=network)
    if leverage > max_product_lev:
        leverage = max_product_lev
        logger.info("Bro Mode: clamped leverage to %sx for %s", leverage, product)

    product_id = get_product_id(product, network=network)
    if product_id is None:
        return {"success": False, "error": f"Unknown product '{product}'"}

    from src.nadobro.services.budget_guard import get_account_snapshot
    snapshot = get_account_snapshot(telegram_id)
    if snapshot:
        current_positions = snapshot.get("position_count", 0)
        if current_positions >= max_positions:
            return {"success": True, "action": "blocked",
                    "detail": f"Max positions ({max_positions}) reached, holding"}

        existing_products = {
            _normalize_product_symbol(p.get("product", ""))
            for p in snapshot.get("positions", [])
        }
        if product in existing_products:
            return {"success": True, "action": "blocked",
                    "detail": f"Already have a position in {product}, holding"}

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

    # Don't risk more than 50% of available balance on a single trade
    try:
        balance = ro_client.get_balance() or {}
        available = float(balance.get("available_balance", 0) or balance.get("equity", 0) or 0)
        max_notional = available * 0.5
        if mid > 0 and size * mid > max_notional and max_notional > 0:
            size = max_notional / mid
            notional_usd = size * mid
            logger.info("Bro Mode: clamped size to %.6f (50%% of balance)", size)
    except Exception as e:
        logger.warning("Bro Mode: balance check failed: %s", e)
    if size <= 0 or notional_usd <= 0:
        return {"success": True, "action": "blocked", "detail": "Computed position size is zero; holding."}

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
        tp_price=tp_price,
        sl_price=sl_price,
        source="bro",
        strategy_session_id=state.get("strategy_session_id"),
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
            "composite_score": decision.get("composite_score", 0),
            "risk_score": decision.get("risk_score", 0),
            "expected_pnl_pct": decision.get("expected_pnl_pct", 0),
            "reasoning": decision.get("reasoning", "")[:200],
            "signals": decision.get("signals", []),
            "ts": time.time(),
            "digest": result.get("digest", ""),
        }
        trades_log = bro_state.get("trades_log", [])
        trades_log.append(trade_record)
        bro_state["trades_log"] = trades_log[-100:]

        logger.info(
            "Bro Mode OPENED %s %s: $%.0f @ $%.2f (%dx) conf=%.0f%% score=%.2f — %s",
            product, "LONG" if is_long else "SHORT",
            notional_usd, mid, leverage, confidence * 100,
            decision.get("composite_score", 0),
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
            "composite_score": decision.get("composite_score", 0),
            "reasoning": decision.get("reasoning", ""),
            "tp_price": tp_price,
            "sl_price": sl_price,
        }
    else:
        return {"success": False, "error": f"Order failed: {result.get('error', 'unknown')}"}


def _handle_close(
    telegram_id, network, state, decision, positions,
) -> dict:
    close_product = _normalize_product_symbol(
        decision.get("close_product", decision.get("product", ""))
    )
    if not close_product:
        return {"success": True, "action": "hold", "detail": "No product specified for close"}

    matching = [
        p
        for p in positions
        if _normalize_product_symbol(p.get("product", "")) == close_product
    ]
    if not matching:
        return {"success": True, "action": "hold", "detail": f"No open position for {close_product}"}

    pos = matching[0]
    product_id = get_product_id(close_product, network=network)
    if product_id is None:
        return {"success": False, "error": f"Unknown product '{close_product}'"}

    is_long = pos.get("side", "").lower() == "long"
    size = float(pos.get("size", pos.get("amount", 0)) or 0)
    if size <= 0:
        return {"success": True, "action": "hold", "detail": f"No closeable size for {close_product}"}

    result = execute_market_order(
        telegram_id=telegram_id,
        product=close_product,
        size=size,
        is_long=not is_long,
        leverage=1.0,
        slippage_pct=1.5,
        reduce_only=True,
        enforce_rate_limit=False,
        source="bro",
        strategy_session_id=state.get("strategy_session_id"),
    )

    if result.get("success"):
        bro_state = state.get("bro_state", {})
        pnl = pos.get("unrealized_pnl", 0)
        bro_state["total_pnl"] = float(bro_state.get("total_pnl", 0)) + pnl

        recent_closes = bro_state.get("recent_closes", [])
        recent_closes.append({
            "product": close_product,
            "side": "long" if is_long else "short",
            "exit_confidence": decision.get("confidence", 0.5),
            "pnl": pnl,
            "ts": time.time(),
        })
        bro_state["recent_closes"] = recent_closes[-20:]

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


def _emergency_close_all(telegram_id, network, state, products):
    from src.nadobro.services.trade_service import close_all_positions
    try:
        result = close_all_positions(telegram_id, network=network)
        if result.get("success"):
            logger.info("Bro Mode emergency flatten successful for user %s", telegram_id)
            closed = result.get("closed", [])
            failed = result.get("failed", [])
            if failed:
                logger.warning(
                    "Bro Mode emergency flatten partial failure for user %s: %d closed, %d failed — %s",
                    telegram_id, len(closed), len(failed),
                    "; ".join(str(f)[:80] for f in failed[:3]),
                )
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
        "products": state.get("products", get_perp_products()[:6] or ["BTC", "ETH", "SOL"]),
        "bro_profile": state.get("bro_profile", "normal"),
        "composite_score": last_decision.get("composite_score", 0),
    }


# === AUDIT COMPLETE: Bro Mode ===
# Status: Improved & Ready
# Key Changes: Opens are constrained to configured products and reward/risk, closes are reduce-only, and emergency flatten uses the active strategy network.
