"""
MM Bot (Grid + MM) — places symmetric limit orders around mid price.
MM mode: tight spread, 1-2 levels each side.
Grid mode: wider spacing, 3-5 levels each side.
Cancels stale orders that drifted beyond threshold from current mid.
"""
import logging
import time
from src.nadobro.config import get_product_id
from src.nadobro.services.trade_service import execute_limit_order

logger = logging.getLogger(__name__)

STALE_DRIFT_MULTIPLIER = 3.0
MM_DEFAULT_LEVELS = 2
GRID_DEFAULT_LEVELS = 4
MM_MIN_SPREAD_BP = 2.0
GRID_MIN_SPREAD_BP = 8.0


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(v, hi))


def _update_mid_history(state: dict, mid: float, max_points: int = 40) -> list[float]:
    history = [float(x) for x in (state.get("mm_mid_history") or []) if float(x) > 0]
    history.append(float(mid))
    history = history[-max_points:]
    state["mm_mid_history"] = history
    return history


def _compute_realized_vol_bp(history: list[float]) -> float:
    if len(history) < 2:
        return 0.0
    returns = []
    for i in range(1, len(history)):
        prev = float(history[i - 1] or 0)
        cur = float(history[i] or 0)
        if prev <= 0 or cur <= 0:
            continue
        returns.append(abs((cur - prev) / prev))
    if not returns:
        return 0.0
    avg_abs_return = sum(returns) / len(returns)
    return avg_abs_return * 10000.0


def _compute_reference_price(state: dict, mid: float, mode: str, ema_fast_alpha: float, ema_slow_alpha: float) -> float:
    mode = (mode or "mid").lower().strip()
    if mode == "mid":
        return mid
    if mode == "ema_fast":
        prev = float(state.get("mm_ref_ema_fast") or mid)
        ema = (ema_fast_alpha * mid) + ((1.0 - ema_fast_alpha) * prev)
        state["mm_ref_ema_fast"] = ema
        return ema
    if mode == "ema_slow":
        prev = float(state.get("mm_ref_ema_slow") or mid)
        ema = (ema_slow_alpha * mid) + ((1.0 - ema_slow_alpha) * prev)
        state["mm_ref_ema_slow"] = ema
        return ema
    return mid


def _resolve_side_multipliers(directional_bias: str, net_units: float, inv_soft_usd: float, mid: float) -> tuple[float, float, bool, str]:
    bias = (directional_bias or "neutral").lower().strip()
    buy_mult = 1.0
    sell_mult = 1.0
    inv_usd = abs(net_units) * mid
    pause_to_flatten_only = False
    pause_reason = ""
    inv_hard_usd = max(inv_soft_usd * 1.8, inv_soft_usd + 1.0)

    if bias == "long_bias":
        buy_mult *= 1.15
        sell_mult *= 0.85
    elif bias == "short_bias":
        buy_mult *= 0.85
        sell_mult *= 1.15

    if inv_usd >= inv_hard_usd:
        pause_to_flatten_only = True
        pause_reason = f"Inventory hard limit breached (${inv_usd:,.2f} >= ${inv_hard_usd:,.2f})"
    elif inv_usd >= inv_soft_usd:
        # Soften one-sided exposure by skewing quote budget toward flattening side.
        if net_units > 0:
            # Long inventory -> bias to sell.
            buy_mult *= 0.55
            sell_mult *= 1.45
        elif net_units < 0:
            # Short inventory -> bias to buy.
            buy_mult *= 1.45
            sell_mult *= 0.55

    return buy_mult, sell_mult, pause_to_flatten_only, pause_reason


def _compute_grid_prices(
    reference_price: float,
    spread_bp: float,
    levels: int,
    strategy: str,
    min_range_pct: float = 1.0,
    max_range_pct: float = 1.0,
) -> list[dict]:
    half_spread = spread_bp / 10000.0
    orders = []
    if strategy == "grid":
        min_off = max(0.0001, min_range_pct / 100.0)
        max_off = max(min_off, max_range_pct / 100.0)
        step = (max_off - min_off) / max(levels - 1, 1)
        for i in range(levels):
            offset = min_off + (step * i)
            buy_price = reference_price * (1.0 - offset)
            sell_price = reference_price * (1.0 + offset)
            lvl = i + 1
            orders.append({"price": buy_price, "is_long": True, "level": lvl})
            orders.append({"price": sell_price, "is_long": False, "level": lvl})
        return orders
    for i in range(1, levels + 1):
        offset = half_spread * i
        buy_price = reference_price * (1.0 - offset)
        sell_price = reference_price * (1.0 + offset)
        orders.append({"price": buy_price, "is_long": True, "level": i})
        orders.append({"price": sell_price, "is_long": False, "level": i})
    return orders


def _cancel_stale_orders(
    client,
    product_id: int,
    mid: float,
    spread_bp: float,
    levels: int,
    open_orders: list,
    order_birth_ts: dict,
    quote_ttl_seconds: int,
    close_offset_bp: float = 0.0,
) -> int:
    if not open_orders:
        return 0
    max_offset = (spread_bp / 10000.0) * levels * STALE_DRIFT_MULTIPLIER
    if close_offset_bp > 0:
        max_offset = max(max_offset, close_offset_bp / 10000.0)
    cancelled = 0
    now_ts = time.time()
    for order in open_orders:
        order_price = float(order.get("price", 0))
        if order_price <= 0:
            continue
        digest = order.get("digest")
        drift = abs(order_price - mid) / mid
        age_seconds = None
        if digest and digest in order_birth_ts:
            age_seconds = max(0.0, now_ts - float(order_birth_ts.get(digest) or now_ts))
        is_ttl_stale = bool(age_seconds is not None and quote_ttl_seconds > 0 and age_seconds >= quote_ttl_seconds)
        if drift > max_offset or is_ttl_stale:
            if digest:
                result = client.cancel_order(product_id, digest)
                if result.get("success"):
                    cancelled += 1
                    order_birth_ts.pop(digest, None)
                    stale_reason = (
                        f"TTL {age_seconds:.0f}s >= {quote_ttl_seconds}s"
                        if is_ttl_stale
                        else f"drift {drift * 100:.4f}% > {max_offset * 100:.4f}%"
                    )
                    logger.info("Cancelled stale order %s (%s)", digest[:12], stale_reason)
    return cancelled


def run_cycle(
    telegram_id: int,
    network: str,
    state: dict,
    client=None,
    mid: float = 0.0,
    open_orders: list = None,
    passphrase: str = None,
) -> dict:
    product = state.get("product", "BTC")
    strategy = state.get("strategy", "mm")
    product_id = get_product_id(product)
    if product_id is None:
        return {"success": False, "error": f"Invalid product '{product}'", "orders_placed": 0}

    if client is None:
        from src.nadobro.services.user_service import get_user_nado_client
        client = get_user_nado_client(telegram_id, passphrase=passphrase)
    if not client:
        return {"success": False, "error": "Wallet client unavailable", "orders_placed": 0}

    if mid <= 0:
        mp = client.get_market_price(product_id)
        mid = float(mp.get("mid") or 0.0)
    if mid <= 0:
        return {"success": False, "error": "Could not fetch market price", "orders_placed": 0}

    if open_orders is None:
        open_orders = client.get_open_orders(product_id)

    spread_bp = float(state.get("spread_bp") or 5.0)
    notional = float(state.get("notional_usd") or 100.0)
    leverage = float(state.get("leverage") or 3.0)
    max_orders = int(state.get("max_open_orders", 6))
    min_range_pct = float(state.get("min_range_pct") or 1.0)
    max_range_pct = float(state.get("max_range_pct") or 1.0)
    threshold_bp = float(state.get("threshold_bp") or 0.0)
    close_offset_bp = float(state.get("close_offset_bp") or 0.0)
    interval_seconds = max(1, int(state.get("interval_seconds") or 60))
    quote_ttl_seconds = max(0, int(state.get("quote_ttl_seconds") or (interval_seconds * 2)))
    directional_bias = str(state.get("directional_bias") or "neutral")
    reference_mode = str(state.get("reference_mode") or "mid")
    ema_fast_alpha = _clamp(float(state.get("ema_fast_alpha") or 0.45), 0.05, 0.95)
    ema_slow_alpha = _clamp(float(state.get("ema_slow_alpha") or 0.20), 0.05, 0.95)
    vol_window = max(4, int(state.get("vol_window_points") or 12))
    vol_sensitivity = _clamp(float(state.get("vol_sensitivity") or 0.02), 0.0, 0.5)
    min_spread_bp = max(0.5, float(state.get("min_spread_bp") or MM_MIN_SPREAD_BP))
    max_spread_bp = max(min_spread_bp, float(state.get("max_spread_bp") or 30.0))
    inv_soft_limit_usd = max(1.0, float(state.get("inventory_soft_limit_usd") or (notional * 0.60)))
    cycle_notional = max(0.0, float(state.get("cycle_notional_usd") or notional))
    session_cap_notional = max(0.0, float(state.get("session_notional_cap_usd") or 0.0))
    carry_notional = max(0.0, float(state.get("mm_notional_carry_usd") or 0.0))
    session_done = max(0.0, float(state.get("mm_session_notional_done_usd") or 0.0))
    order_birth_ts = state.setdefault("mm_order_birth_ts", {})
    if not isinstance(order_birth_ts, dict):
        order_birth_ts = {}
        state["mm_order_birth_ts"] = order_birth_ts

    if strategy == "grid":
        levels = max(1, int(state.get("levels", GRID_DEFAULT_LEVELS)))
        spread_bp = max(spread_bp, GRID_MIN_SPREAD_BP)
    else:
        levels = max(1, int(state.get("levels", MM_DEFAULT_LEVELS)))
        spread_bp = max(spread_bp, MM_MIN_SPREAD_BP)

    if session_cap_notional > 0 and session_done >= session_cap_notional:
        state["running"] = False
        state["mm_pause_reason"] = "Session notional cap reached"
        return {
            "success": True,
            "done": True,
            "orders_placed": 0,
            "orders_cancelled": 0,
            "reason": "session notional cap reached",
        }

    history = _update_mid_history(state, mid, max_points=max(vol_window, 8))
    vol_bp = _compute_realized_vol_bp(history[-vol_window:])
    dynamic_spread_bp = spread_bp * (1.0 + (vol_bp * vol_sensitivity / 100.0))
    dynamic_spread_bp = _clamp(dynamic_spread_bp, min_spread_bp, max_spread_bp)
    reference_price = _compute_reference_price(state, mid, reference_mode, ema_fast_alpha, ema_slow_alpha)
    reference_price = reference_price if reference_price > 0 else mid

    if threshold_bp > 0 and strategy == "mm":
        reference = float(state.get("reference_price") or mid)
        moved_bp = abs(mid - reference) / max(reference, 1e-9) * 10000.0
        if moved_bp < threshold_bp:
            return {
                "success": True,
                "orders_placed": 0,
                "orders_cancelled": 0,
                "reason": "below threshold",
                "spread_bp": dynamic_spread_bp,
                "reference_price": reference_price,
            }

    # Position-aware risk guardrails.
    positions = client.get_all_positions() or []
    net_units = 0.0
    for p in positions:
        if int(p.get("product_id", -1)) != product_id:
            continue
        amt = abs(float(p.get("amount", 0) or 0))
        side = str(p.get("side", "") or "").upper()
        net_units += amt if side == "LONG" else -amt
    inv_usd = abs(net_units) * mid

    buy_mult, sell_mult, pause_flatten_only, pause_reason = _resolve_side_multipliers(
        directional_bias, net_units, inv_soft_limit_usd, mid
    )
    if pause_flatten_only:
        state["mm_paused"] = True
        state["mm_pause_reason"] = pause_reason
    elif state.get("mm_paused") and inv_usd <= (inv_soft_limit_usd * 0.8):
        state["mm_paused"] = False
        state["mm_pause_reason"] = ""

    active_digests = {str(o.get("digest")) for o in open_orders if o.get("digest")}
    stale_digests = [d for d in list(order_birth_ts.keys()) if d not in active_digests]
    for d in stale_digests:
        order_birth_ts.pop(d, None)

    pre_cancel_open_count = len(open_orders)
    orders_cancelled = _cancel_stale_orders(
        client,
        product_id,
        mid,
        dynamic_spread_bp,
        levels,
        open_orders,
        order_birth_ts=order_birth_ts,
        quote_ttl_seconds=quote_ttl_seconds,
        close_offset_bp=close_offset_bp,
    )

    if orders_cancelled > 0:
        open_orders = client.get_open_orders(product_id)

    available_slots = max(0, max_orders - len(open_orders))
    if available_slots == 0:
        return {
            "success": True,
            "orders_placed": 0,
            "orders_cancelled": orders_cancelled,
            "reason": "max open orders reached",
            "spread_bp": dynamic_spread_bp,
            "reference_price": reference_price,
        }

    existing_prices = set()
    for o in open_orders:
        p = float(o.get("price", 0))
        if p > 0:
            existing_prices.add(round(p, 8))

    grid_orders = _compute_grid_prices(
        reference_price,
        dynamic_spread_bp,
        levels,
        strategy=strategy,
        min_range_pct=min_range_pct,
        max_range_pct=max_range_pct,
    )
    if session_cap_notional > 0:
        available_session = max(0.0, session_cap_notional - session_done)
    else:
        available_session = cycle_notional + carry_notional
    cycle_target_notional = min(cycle_notional + carry_notional, available_session)
    cycle_target_notional = max(0.0, cycle_target_notional)
    if cycle_target_notional <= 0:
        state["running"] = False if session_cap_notional > 0 else state.get("running", True)
        return {
            "success": True,
            "done": bool(session_cap_notional > 0),
            "orders_placed": 0,
            "orders_cancelled": orders_cancelled,
            "reason": "cycle notional exhausted",
            "spread_bp": dynamic_spread_bp,
            "reference_price": reference_price,
        }

    side_budget_weight_total = max(1e-9, buy_mult + sell_mult)
    buy_budget_usd = cycle_target_notional * (buy_mult / side_budget_weight_total)
    sell_budget_usd = cycle_target_notional * (sell_mult / side_budget_weight_total)
    per_level_buy_size = max(buy_budget_usd / max(1, levels) / mid, 0.0001)
    per_level_sell_size = max(sell_budget_usd / max(1, levels) / mid, 0.0001)

    orders_placed = 0
    errors = []
    quote_distances_bp = []
    placed_notional_usd = 0.0

    for order_spec in grid_orders:
        if orders_placed >= available_slots:
            break
        order_price = order_spec["price"]
        rounded_price = round(order_price, 8)
        if rounded_price in existing_prices:
            continue
        if pause_flatten_only:
            if net_units > 0 and order_spec["is_long"]:
                continue
            if net_units < 0 and not order_spec["is_long"]:
                continue

        size_to_use = per_level_buy_size if order_spec["is_long"] else per_level_sell_size
        result = execute_limit_order(
            telegram_id,
            product,
            size_to_use,
            order_price,
            is_long=order_spec["is_long"],
            leverage=leverage,
            enforce_rate_limit=False,
            passphrase=passphrase,
        )

        if result.get("success"):
            orders_placed += 1
            existing_prices.add(rounded_price)
            digest = result.get("digest")
            if digest and digest != "unknown":
                order_birth_ts[str(digest)] = time.time()
            placed_notional_usd += max(0.0, float(size_to_use) * float(order_price))
            if reference_price > 0:
                quote_distances_bp.append(abs(order_price - reference_price) / reference_price * 10000.0)
            logger.info("Placed %s order at %.6f (level %d) for user %s",
                        "BUY" if order_spec["is_long"] else "SELL",
                        order_price, order_spec["level"], telegram_id)
        else:
            errors.append(f"L{order_spec['level']} {'BUY' if order_spec['is_long'] else 'SELL'}: {result.get('error', 'unknown')}")

    final_open_orders = client.get_open_orders(product_id)
    final_open_count = len(final_open_orders or [])
    est_fills = max(0, (len(open_orders) + orders_placed) - final_open_count)
    maker_fill_ratio = est_fills / max(1, est_fills + orders_placed)
    cancellation_ratio = orders_cancelled / max(1, orders_cancelled + est_fills)
    avg_quote_distance_bp = sum(quote_distances_bp) / len(quote_distances_bp) if quote_distances_bp else 0.0
    quote_refresh_rate = (orders_cancelled + orders_placed) / max(1, interval_seconds)

    new_session_done = session_done + placed_notional_usd
    state["mm_session_notional_done_usd"] = round(new_session_done, 6)
    unspent_notional = max(0.0, cycle_target_notional - placed_notional_usd)
    state["mm_notional_carry_usd"] = min(unspent_notional, cycle_notional * 3.0)
    state["mm_last_cycle_notional_usd"] = round(placed_notional_usd, 6)
    state["mm_last_inventory_skew_usd"] = round(inv_usd, 6)
    state["mm_last_ref_price"] = round(reference_price, 8)
    state["mm_last_spread_bp"] = round(dynamic_spread_bp, 4)
    state["mm_last_metrics"] = {
        "maker_fill_ratio": round(maker_fill_ratio, 4),
        "cancellation_ratio": round(cancellation_ratio, 4),
        "avg_quote_distance_bp": round(avg_quote_distance_bp, 3),
        "quote_refresh_rate": round(quote_refresh_rate, 4),
        "inventory_skew_usd": round(inv_usd, 4),
        "estimated_fills": int(est_fills),
        "open_orders_before": int(pre_cancel_open_count),
        "open_orders_after": int(final_open_count),
        "session_notional_done_usd": round(new_session_done, 4),
    }

    state["mm_paused"] = bool(pause_flatten_only)
    if pause_flatten_only:
        state["mm_pause_reason"] = pause_reason
    elif not state.get("mm_paused"):
        state["mm_pause_reason"] = ""

    success = not (orders_placed == 0 and errors and len(open_orders) == 0)
    return {
        "success": success,
        "error": errors[0] if not success else None,
        "orders_placed": orders_placed,
        "orders_cancelled": orders_cancelled,
        "levels": levels,
        "spread_bp": dynamic_spread_bp,
        "mid_price": mid,
        "reference_price": reference_price,
        "volatility_bp": vol_bp,
        "cycle_target_notional_usd": cycle_target_notional,
        "cycle_placed_notional_usd": placed_notional_usd,
        "maker_fill_ratio": maker_fill_ratio,
        "cancellation_ratio": cancellation_ratio,
        "avg_quote_distance_bp": avg_quote_distance_bp,
        "quote_refresh_rate": quote_refresh_rate,
        "inventory_skew_usd": inv_usd,
        "pause_reason": state.get("mm_pause_reason") or None,
        "errors": errors if errors else None,
    }
