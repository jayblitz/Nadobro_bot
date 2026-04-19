"""
Strategy engine for GRID + Reverse GRID (RGRID).
GRID mode: symmetric maker grid around reference (former mm behavior).
RGRID mode: reverse-grid quoting with exposure anchor and PnL risk controls.
Cancels stale orders that drift beyond threshold from current mid.
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
DEFAULT_MIN_ORDER_NOTIONAL_USD = 100.0
GRID_SOFT_RESET_MIN_TIMEOUT_SECONDS = 15


MOMENTUM_EMA_CROSSOVER_BP = 5.0       # EMA fast-slow divergence threshold (bp)
MOMENTUM_VOLUME_SURGE_MULT = 1.8       # Volume must be this multiple of recent avg
MOMENTUM_FUNDING_SHIFT_BP = 2.0        # Funding rate shift threshold (bp)
MOMENTUM_COOLDOWN_SECONDS = 300        # Min time between mode switches


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


def _detect_ema_crossover(state: dict, mid: float, ema_fast_alpha: float, ema_slow_alpha: float) -> dict:
    """Detect EMA fast/slow crossover for momentum detection."""
    prev_fast = float(state.get("mm_ref_ema_fast") or mid)
    prev_slow = float(state.get("mm_ref_ema_slow") or mid)
    ema_fast = (ema_fast_alpha * mid) + ((1.0 - ema_fast_alpha) * prev_fast)
    ema_slow = (ema_slow_alpha * mid) + ((1.0 - ema_slow_alpha) * prev_slow)

    divergence_bp = abs(ema_fast - ema_slow) / max(ema_slow, 1e-9) * 10000.0
    prev_divergence_sign = 1 if prev_fast > prev_slow else -1
    curr_divergence_sign = 1 if ema_fast > ema_slow else -1
    crossed = prev_divergence_sign != curr_divergence_sign

    return {
        "ema_fast": ema_fast,
        "ema_slow": ema_slow,
        "divergence_bp": divergence_bp,
        "bullish": ema_fast > ema_slow,
        "crossed": crossed,
        "strong": divergence_bp >= MOMENTUM_EMA_CROSSOVER_BP,
    }


def _detect_volume_surge(history: list[float], window: int = 10) -> dict:
    """Detect if recent volume (price movement) is surging compared to average."""
    if len(history) < window + 2:
        return {"surge": False, "ratio": 0.0}
    recent_returns = []
    for i in range(len(history) - window, len(history)):
        prev = float(history[i - 1] or 0)
        cur = float(history[i] or 0)
        if prev > 0 and cur > 0:
            recent_returns.append(abs((cur - prev) / prev))
    older_returns = []
    start = max(1, len(history) - window * 3)
    end = len(history) - window
    for i in range(start, end):
        prev = float(history[i - 1] or 0)
        cur = float(history[i] or 0)
        if prev > 0 and cur > 0:
            older_returns.append(abs((cur - prev) / prev))
    avg_recent = sum(recent_returns) / max(len(recent_returns), 1)
    avg_older = sum(older_returns) / max(len(older_returns), 1)
    ratio = avg_recent / max(avg_older, 1e-12)
    return {
        "surge": ratio >= MOMENTUM_VOLUME_SURGE_MULT,
        "ratio": ratio,
    }


def _detect_funding_shift(state: dict, client, product_id: int) -> dict:
    """Detect funding rate shift indicating directional bias."""
    try:
        fr_data = client.get_funding_rate(product_id)
        if not fr_data:
            return {"shift": False, "rate_bp": 0.0}
        rate = float(fr_data.get("funding_rate", 0) or 0)
        rate_bp = rate * 10000.0
        prev_rate_raw = state.get("rgrid_prev_funding_bp")
        prev_rate_bp = float(prev_rate_raw) if prev_rate_raw is not None else None
        state["rgrid_prev_funding_bp"] = rate_bp
        if prev_rate_bp is None:
            return {
                "shift": False,
                "rate_bp": rate_bp,
                "shift_bp": 0.0,
                "direction": "long" if rate_bp < 0 else "short",
            }
        shift_bp = abs(rate_bp - prev_rate_bp)
        return {
            "shift": shift_bp >= MOMENTUM_FUNDING_SHIFT_BP,
            "rate_bp": rate_bp,
            "shift_bp": shift_bp,
            "direction": "long" if rate_bp < 0 else "short",
        }
    except Exception:
        return {"shift": False, "rate_bp": 0.0}


def _evaluate_rgrid_momentum(state: dict, mid: float, history: list[float],
                              ema_fast_alpha: float, ema_slow_alpha: float,
                              client=None, product_id: int = 0) -> dict:
    """Evaluate all momentum signals for R-GRID dynamic mode switching."""
    ema_signal = _detect_ema_crossover(state, mid, ema_fast_alpha, ema_slow_alpha)
    vol_signal = _detect_volume_surge(history)
    funding_signal = _detect_funding_shift(state, client, product_id) if client else {"shift": False, "rate_bp": 0.0}

    signals_active = sum([
        ema_signal.get("strong", False),
        vol_signal.get("surge", False),
        funding_signal.get("shift", False),
    ])

    # Momentum break requires at least 2 of 3 signals
    momentum_break = signals_active >= 2

    # Determine momentum direction
    direction = None
    if momentum_break:
        if ema_signal.get("bullish"):
            direction = "bullish"
        else:
            direction = "bearish"

    # Check cooldown
    now = time.time()
    last_switch_ts = float(state.get("rgrid_last_mode_switch_ts") or 0.0)
    in_cooldown = (now - last_switch_ts) < MOMENTUM_COOLDOWN_SECONDS

    return {
        "momentum_break": momentum_break and not in_cooldown,
        "signals_active": signals_active,
        "direction": direction,
        "ema": ema_signal,
        "volume": vol_signal,
        "funding": funding_signal,
        "in_cooldown": in_cooldown,
    }


def _compute_grid_prices(
    reference_price: float,
    spread_bp: float,
    levels: int,
    strategy: str,
    net_units: float = 0.0,
    mid_price: float | None = None,
    best_bid: float | None = None,
    best_ask: float | None = None,
    soft_reset_side: str | None = None,
    min_range_pct: float = 1.0,
    max_range_pct: float = 1.0,
) -> list[dict]:
    half_spread = spread_bp / 10000.0
    orders = []
    if strategy == "rgrid":
        base_off = max(0.0, abs(spread_bp) / 10000.0)
        for i in range(levels):
            lvl = i + 1
            offset = base_off * lvl
            # Reverse Grid:
            # - buy quote at/above anchor (fills on upward continuation)
            # - sell quote at/below anchor (fills on downward continuation)
            buy_price = reference_price * (1.0 + offset)
            sell_price = reference_price * (1.0 - offset)

            if soft_reset_side and mid_price and mid_price > 0:
                reset_nudge = (0.1 * lvl) / 10000.0
                if soft_reset_side == "buy":
                    buy_price = float(best_bid or (mid_price * (1.0 - reset_nudge)))
                elif soft_reset_side == "sell":
                    sell_price = float(best_ask or (mid_price * (1.0 + reset_nudge)))

            if best_bid and best_bid > 0:
                buy_price = min(buy_price, float(best_bid))
            if best_ask and best_ask > 0:
                sell_price = max(sell_price, float(best_ask))

            lvl = i + 1
            orders.append({"price": buy_price, "is_long": True, "level": lvl})
            orders.append({"price": sell_price, "is_long": False, "level": lvl})
        return orders
    for i in range(1, levels + 1):
        offset = half_spread * i
        buy_price = reference_price * (1.0 - offset)
        sell_price = reference_price * (1.0 + offset)
        if soft_reset_side and mid_price and mid_price > 0:
            reset_nudge = (0.1 * i) / 10000.0
            if soft_reset_side == "buy":
                buy_price = float(best_bid or (mid_price * (1.0 - reset_nudge)))
            elif soft_reset_side == "sell":
                sell_price = float(best_ask or (mid_price * (1.0 + reset_nudge)))
        if best_bid and best_bid > 0:
            buy_price = min(buy_price, float(best_bid))
        if best_ask and best_ask > 0:
            sell_price = max(sell_price, float(best_ask))
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
    cancelled_digests: set[str] | None = None,
) -> int:
    if not open_orders:
        return 0
    # RGRID can use signed spread values; stale drift bounds should use magnitude.
    max_offset = (abs(spread_bp) / 10000.0) * levels * STALE_DRIFT_MULTIPLIER
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
                    if cancelled_digests is not None:
                        cancelled_digests.add(str(digest))
                    stale_reason = (
                        f"TTL {age_seconds:.0f}s >= {quote_ttl_seconds}s"
                        if is_ttl_stale
                        else f"drift {drift * 100:.4f}% > {max_offset * 100:.4f}%"
                    )
                    logger.info("Cancelled stale order %s (%s)", digest[:12], stale_reason)
    return cancelled


def _compute_grid_cycle_pnl_usd(positions: list, product_id: int) -> float:
    total = 0.0
    for p in positions:
        if int(p.get("product_id", -1)) != product_id:
            continue
        # Avoid double counting when exchanges expose overlapping pnl fields.
        selected = None
        for key in ("net_pnl", "unrealized_pnl", "pnl", "realized_pnl"):
            v = p.get(key)
            if v is None:
                continue
            try:
                selected = float(v)
                break
            except (TypeError, ValueError):
                continue
        if selected is not None:
            total += selected
    return total


def _append_grid_exposure_fill(state: dict, quote: dict) -> None:
    side_key = "grid_buy_fills" if bool(quote.get("is_long")) else "grid_sell_fills"
    fills = state.setdefault(side_key, [])
    if not isinstance(fills, list):
        fills = []
        state[side_key] = fills
    fills.append(
        {
            "price": float(quote.get("price") or 0.0),
            "size": max(0.0, float(quote.get("size") or 0.0)),
            "ts": float(quote.get("placed_ts") or time.time()),
        }
    )
    if len(fills) > 300:
        del fills[:-300]


def _rolling_vwap_recent_fraction(fills: list, fraction: float) -> float:
    if not fills:
        return 0.0
    safe_fraction = _clamp(float(fraction or 0.12), 0.02, 1.0)
    total_size = 0.0
    for f in fills:
        try:
            total_size += max(0.0, float(f.get("size") or 0.0))
        except Exception:
            continue
    if total_size <= 0:
        return 0.0
    target = total_size * safe_fraction
    if target <= 0:
        return 0.0
    acc_size = 0.0
    acc_notional = 0.0
    for f in reversed(fills):
        px = float(f.get("price") or 0.0)
        sz = max(0.0, float(f.get("size") or 0.0))
        if px <= 0 or sz <= 0:
            continue
        take = min(sz, max(0.0, target - acc_size))
        if take <= 0:
            break
        acc_size += take
        acc_notional += px * take
        if acc_size >= target:
            break
    if acc_size <= 0:
        return 0.0
    return acc_notional / acc_size


def _estimate_net_units_from_fill_history(state: dict) -> float:
    buy_total = 0.0
    sell_total = 0.0
    for fill in state.get("grid_buy_fills") or []:
        try:
            buy_total += max(0.0, float(fill.get("size") or 0.0))
        except Exception:
            continue
    for fill in state.get("grid_sell_fills") or []:
        try:
            sell_total += max(0.0, float(fill.get("size") or 0.0))
        except Exception:
            continue
    return buy_total - sell_total


def _reprice_post_only_quote(client, product_id: int, is_long: bool, fallback_price: float) -> float:
    try:
        mp = client.get_market_price(product_id) or {}
        bid = float(mp.get("bid") or 0.0)
        ask = float(mp.get("ask") or 0.0)
        nudge = 0.00002  # 0.2 bp inside the safe side of book
        if is_long and bid > 0:
            return bid * (1.0 - nudge)
        if (not is_long) and ask > 0:
            return ask * (1.0 + nudge)
    except Exception:
        pass
    return float(fallback_price or 0.0)


def _reconcile_executed_quotes(
    state: dict,
    network: str,
    open_orders: list,
    cancelled_digests: set[str],
) -> list[dict]:
    tracked = state.setdefault("mm_tracked_quotes", {})
    if not isinstance(tracked, dict):
        tracked = {}
        state["mm_tracked_quotes"] = tracked
    active_digests = {str(o.get("digest")) for o in (open_orders or []) if o.get("digest")}
    executed: list[dict] = []
    now_ts = time.time()
    for digest, meta in list(tracked.items()):
        if digest in active_digests:
            continue
        if digest in cancelled_digests:
            tracked.pop(digest, None)
            continue
        filled = False
        try:
            from src.nadobro.services.nado_archive import query_order_by_digest
            fill_data = query_order_by_digest(network, str(digest), 2.0, 0.4)
            filled = bool(fill_data and fill_data.get("is_filled"))
        except Exception:
            filled = False

        if filled:
            executed.append(meta if isinstance(meta, dict) else {})
            tracked.pop(digest, None)
            continue

        placed_ts = 0.0
        if isinstance(meta, dict):
            try:
                placed_ts = float(meta.get("placed_ts") or 0.0)
            except Exception:
                placed_ts = 0.0
        # If an order disappeared and remains unconfirmed for too long, drop stale
        # tracker metadata without treating it as a fill. Anchor logic should move
        # only on confirmed executions.
        if placed_ts > 0 and (now_ts - placed_ts) > 1800:
            tracked.pop(digest, None)
    return executed


def run_cycle(
    telegram_id: int,
    network: str,
    state: dict,
    client=None,
    mid: float = 0.0,
    open_orders: list = None,
    **kwargs,
) -> dict:
    product = state.get("product", "BTC")
    strategy = str(state.get("strategy", "grid") or "grid").lower()
    if strategy == "mm":
        strategy = "grid"
    product_id = get_product_id(product, network=network)
    if product_id is None:
        return {"success": False, "error": f"Invalid product '{product}'", "orders_placed": 0}

    if client is None:
        from src.nadobro.services.user_service import get_user_nado_client
        client = get_user_nado_client(telegram_id)
    if not client:
        return {"success": False, "error": "Wallet client unavailable", "orders_placed": 0}

    # Circuit breaker: check cumulative drawdown
    mm_initial_equity = float(state.get("mm_initial_equity") or 0)
    mm_cumulative_pnl = float(state.get("mm_cumulative_pnl") or 0)
    mm_max_drawdown_pct = float(state.get("mm_max_drawdown_pct") or 20.0)

    if mm_initial_equity <= 0:
        # First run - record initial equity
        try:
            balance = client.get_balance() or {}
            mm_initial_equity = float(balance.get("equity", 0) or balance.get("available_balance", 0) or 0)
            state["mm_initial_equity"] = mm_initial_equity
        except Exception:
            pass

    if mm_initial_equity > 0 and mm_cumulative_pnl < 0:
        drawdown_pct = abs(mm_cumulative_pnl) / mm_initial_equity * 100
        if drawdown_pct >= mm_max_drawdown_pct:
            return {
                "success": False,
                "action": "circuit_breaker",
                "error": f"MM Bot stopped: drawdown {drawdown_pct:.1f}% exceeds {mm_max_drawdown_pct}% limit",
                "cumulative_pnl": mm_cumulative_pnl,
            }

    mp = client.get_market_price(product_id) or {}
    if mid <= 0:
        mid = float(mp.get("mid") or 0.0)
    if mid <= 0:
        return {"success": False, "error": "Could not fetch market price", "orders_placed": 0}
    best_bid = float(mp.get("bid") or 0.0)
    best_ask = float(mp.get("ask") or 0.0)

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
    cycle_notional_cfg = max(0.0, float(state.get("cycle_notional_usd") or notional))
    cycle_notional = max(cycle_notional_cfg, notional * max(1.0, leverage))
    session_cap_notional = max(0.0, float(state.get("session_notional_cap_usd") or 0.0))
    carry_notional = max(0.0, float(state.get("mm_notional_carry_usd") or 0.0))
    session_done = max(0.0, float(state.get("mm_session_notional_done_usd") or 0.0))
    min_order_notional_usd = max(
        1.0,
        float(state.get("min_order_notional_usd") or DEFAULT_MIN_ORDER_NOTIONAL_USD),
    )
    order_birth_ts = state.setdefault("mm_order_birth_ts", {})
    if not isinstance(order_birth_ts, dict):
        order_birth_ts = {}
        state["mm_order_birth_ts"] = order_birth_ts

    if strategy == "rgrid":
        levels = max(1, int(state.get("levels", GRID_DEFAULT_LEVELS)))
        if "rgrid_spread_bp" in state:
            spread_bp = float(state.get("rgrid_spread_bp") or 0.0)
        elif "grid_spread_bp" in state:
            spread_bp = float(state.get("grid_spread_bp") or 0.0)
        elif "spread_bp" in state:
            spread_bp = float(state.get("spread_bp") or 0.0)
        else:
            # Legacy fallback: map old range pct to bps scale.
            spread_bp = float(state.get("max_range_pct") or state.get("min_range_pct") or GRID_MIN_SPREAD_BP * 2.0) * 100.0
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
    if strategy == "rgrid":
        max_abs = max(abs(min_spread_bp), abs(max_spread_bp))
        dynamic_spread_bp = _clamp(dynamic_spread_bp, -max_abs, max_abs)

        # --- R-GRID Momentum Detection & Dynamic Mode Switching ---
        momentum = _evaluate_rgrid_momentum(
            state, mid, history, ema_fast_alpha, ema_slow_alpha,
            client=client, product_id=product_id,
        )
        rgrid_mode = str(state.get("rgrid_active_mode") or "classic")
        if momentum["momentum_break"] and rgrid_mode == "classic":
            # Momentum break detected — switch to reversed mode
            rgrid_mode = "reversed"
            state["rgrid_active_mode"] = "reversed"
            state["rgrid_momentum_direction"] = momentum["direction"]
            state["rgrid_last_mode_switch_ts"] = time.time()
            logger.info(
                "RGRID momentum break: switching to reversed mode (direction=%s, signals=%d/3, "
                "ema_div=%.1fbp, vol_ratio=%.2f, funding_shift=%.1fbp)",
                momentum["direction"], momentum["signals_active"],
                momentum["ema"]["divergence_bp"], momentum["volume"]["ratio"],
                momentum["funding"].get("shift_bp", 0),
            )
        elif rgrid_mode == "reversed" and not momentum["momentum_break"] and momentum["signals_active"] == 0:
            # Momentum has faded — revert to classic mode
            rgrid_mode = "classic"
            state["rgrid_active_mode"] = "classic"
            state["rgrid_momentum_direction"] = None
            state["rgrid_directional_bias"] = None
            state["rgrid_last_mode_switch_ts"] = time.time()
            logger.info("RGRID momentum faded: reverting to classic mode (signals=%d/3)", momentum["signals_active"])
        state["rgrid_momentum_signals"] = momentum["signals_active"]

        # In reversed mode, flip the grid direction to follow momentum
        if rgrid_mode == "reversed":
            mom_dir = state.get("rgrid_momentum_direction")
            if mom_dir == "bullish":
                directional_bias = "long_bias"
            elif mom_dir == "bearish":
                directional_bias = "short_bias"
            else:
                # Keep the last resolved reversed-mode bias sticky when direction is temporarily unknown.
                sticky_bias = str(state.get("rgrid_directional_bias") or "").strip()
                if sticky_bias in {"long_bias", "short_bias", "neutral"}:
                    directional_bias = sticky_bias
            state["rgrid_directional_bias"] = directional_bias
        # --- End Momentum Detection ---
    else:
        dynamic_spread_bp = _clamp(dynamic_spread_bp, min_spread_bp, max_spread_bp)
    reference_price = _compute_reference_price(state, mid, reference_mode, ema_fast_alpha, ema_slow_alpha)
    reference_price = reference_price if reference_price > 0 else mid

    threshold_wait_result = None
    if threshold_bp > 0 and strategy == "grid":
        # Avoid a cold-start deadlock: while mid history is short, price vs session ref is ~0 bp,
        # so we would never place quotes until the market drifts. Allow quotes until history warms up
        # or when no quotes have been placed yet (need to seed the grid).
        hist = state.get("mm_mid_history") or []
        has_active_quotes = bool(open_orders) or bool(state.get("mm_tracked_quotes"))
        if len(hist) >= 4 and has_active_quotes:
            reference = float(state.get("reference_price") or mid)
            moved_bp = abs(mid - reference) / max(reference, 1e-9) * 10000.0
            if moved_bp < threshold_bp:
                threshold_wait_result = {
                    "success": True,
                    "orders_placed": 0,
                    "orders_cancelled": 0,
                    "reason": "below threshold",
                    "action": "wait",
                    "detail": f"Price moved {moved_bp:.1f} bp vs ref (need {threshold_bp:.0f} bp)",
                    "spread_bp": dynamic_spread_bp,
                    "reference_price": reference_price,
                }

    # Position-aware risk guardrails.
    positions = client.get_all_positions() or []
    live_position_rows = []
    net_units = 0.0
    for p in positions:
        if int(p.get("product_id", -1)) != product_id:
            continue
        amt = abs(float(p.get("amount", 0) or 0))
        side = str(p.get("side", "") or "").upper()
        if amt <= 0 or side not in {"LONG", "SHORT"}:
            continue
        live_position_rows.append(p)
        net_units += amt if side == "LONG" else -amt
    inventory_source = "exchange"
    if not live_position_rows:
        net_units = _estimate_net_units_from_fill_history(state)
        inventory_source = "fills" if abs(net_units) > 0 else "none"
    inv_usd = abs(net_units) * mid

    cancelled_digests = set(str(d) for d in (state.get("mm_recently_cancelled_digests") or []))
    executed_quotes = _reconcile_executed_quotes(state, network, open_orders, cancelled_digests)
    state["mm_recently_cancelled_digests"] = []
    if executed_quotes:
        for q in executed_quotes:
            _append_grid_exposure_fill(state, q)
    grid_anchor_price = float(state.get("grid_anchor_price") or 0.0)
    discretion = _clamp(float(state.get("rgrid_discretion") or state.get("grid_discretion") or 0.06), 0.01, 0.5)
    recent_fraction = _clamp(discretion * 2.0, 0.02, 0.5)
    buy_exposure = _rolling_vwap_recent_fraction(state.get("grid_buy_fills") or [], recent_fraction)
    sell_exposure = _rolling_vwap_recent_fraction(state.get("grid_sell_fills") or [], recent_fraction)
    if buy_exposure > 0 and sell_exposure > 0:
        grid_anchor_price = (buy_exposure + sell_exposure) / 2.0
    elif buy_exposure > 0:
        grid_anchor_price = buy_exposure
    elif sell_exposure > 0:
        grid_anchor_price = sell_exposure
    if grid_anchor_price <= 0:
        grid_anchor_price = float(state.get("grid_last_fill_price") or 0.0)
    if grid_anchor_price <= 0:
        grid_anchor_price = reference_price
    if executed_quotes:
        last_exec = executed_quotes[-1]
        exec_price = float(last_exec.get("price") or 0.0)
        if exec_price > 0:
            state["grid_last_fill_price"] = exec_price
            grid_anchor_price = exec_price
    elif float(state.get("grid_last_fill_price") or 0.0) <= 0 and grid_anchor_price > 0:
        state["grid_last_fill_price"] = grid_anchor_price
    state["grid_prev_net_units"] = net_units
    state["grid_anchor_price"] = grid_anchor_price
    state["grid_buy_exposure_price"] = round(buy_exposure, 8) if buy_exposure > 0 else 0.0
    state["grid_sell_exposure_price"] = round(sell_exposure, 8) if sell_exposure > 0 else 0.0
    if strategy == "rgrid":
        reference_price = grid_anchor_price

    pnl_stop_pct = max(
        0.0,
        float(
            state.get("rgrid_stop_loss_pct") if strategy == "rgrid"
            else state.get("grid_stop_loss_pct")
            or state.get("sl_pct")
            or 0.0
        ),
    )
    pnl_take_pct = max(
        0.0,
        float(
            state.get("rgrid_take_profit_pct") if strategy == "rgrid"
            else state.get("grid_take_profit_pct")
            or state.get("tp_pct")
            or 0.0
        ),
    )
    max_loss_usd = (pnl_stop_pct / 100.0) * max(0.0, notional)
    max_profit_usd = (pnl_take_pct / 100.0) * max(0.0, notional)
    grid_cycle_pnl = _compute_grid_cycle_pnl_usd(positions, product_id)
    state["grid_last_cycle_pnl_usd"] = round(grid_cycle_pnl, 6)
    if max_loss_usd > 0 and grid_cycle_pnl <= (-max_loss_usd):
        return {
            "success": True,
            "orders_placed": 0,
            "orders_cancelled": 0,
            "action": "grid_stop_loss_hit",
            "detail": (
                f"{'Reverse GRID' if strategy == 'rgrid' else 'GRID'} PnL stop-loss hit: "
                f"pnl ${grid_cycle_pnl:,.2f} <= -${max_loss_usd:,.2f} "
                f"({pnl_stop_pct:.2f}% of margin)"
            ),
            "grid_cycle_pnl_usd": grid_cycle_pnl,
            "grid_max_loss_usd": max_loss_usd,
            "reference_price": reference_price,
            "spread_bp": dynamic_spread_bp,
        }
    if max_profit_usd > 0 and grid_cycle_pnl >= max_profit_usd:
        return {
            "success": True,
            "orders_placed": 0,
            "orders_cancelled": 0,
            "action": "grid_take_profit_hit",
            "detail": (
                f"{'Reverse GRID' if strategy == 'rgrid' else 'GRID'} take-profit hit: "
                f"pnl ${grid_cycle_pnl:,.2f} >= ${max_profit_usd:,.2f} "
                f"({pnl_take_pct:.2f}% of margin)"
            ),
            "grid_cycle_pnl_usd": grid_cycle_pnl,
            "grid_take_profit_usd": max_profit_usd,
            "reference_price": reference_price,
            "spread_bp": dynamic_spread_bp,
        }

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
        cancelled_digests=cancelled_digests,
    )
    if cancelled_digests:
        state["mm_recently_cancelled_digests"] = list(cancelled_digests)

    if orders_cancelled > 0:
        open_orders = client.get_open_orders(product_id)

    if threshold_wait_result is not None:
        threshold_wait_result["orders_cancelled"] = int(orders_cancelled)
        return threshold_wait_result

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

    existing_prices = []
    for o in open_orders:
        p = float(o.get("price", 0))
        if p > 0:
            existing_prices.append(p)

    soft_reset_side = None
    if strategy in ("grid", "rgrid"):
        reset_threshold_pct = max(
            0.0,
            float(
                state.get("rgrid_reset_threshold_pct") if strategy == "rgrid"
                else state.get("grid_reset_threshold_pct")
                or 0.0
            ),
        )
        reset_timeout_s = max(
            GRID_SOFT_RESET_MIN_TIMEOUT_SECONDS,
            int(
                state.get("rgrid_reset_timeout_seconds") if strategy == "rgrid"
                else state.get("grid_reset_timeout_seconds")
                or 120
            ),
        )
        reset_active = bool(state.get("grid_reset_active"))
        reset_started = float(state.get("grid_reset_started_ts") or 0.0)
        now_ts = time.time()
        drift_from_anchor_pct = 0.0
        if reference_price > 0:
            drift_from_anchor_pct = abs(mid - reference_price) / reference_price * 100.0
        state["grid_drift_from_anchor_pct"] = round(drift_from_anchor_pct, 6)
        if reset_threshold_pct > 0 and reference_price > 0 and inv_usd > 0:
            if net_units > 0 and ((reference_price - mid) / reference_price * 100.0 >= reset_threshold_pct):
                reset_active = True
                if not reset_started:
                    reset_started = now_ts
                soft_reset_side = "sell"
            elif net_units < 0 and ((mid - reference_price) / reference_price * 100.0 >= reset_threshold_pct):
                reset_active = True
                if not reset_started:
                    reset_started = now_ts
                soft_reset_side = "buy"

        if reset_active and not soft_reset_side:
            soft_reset_side = str(state.get("grid_reset_side") or "")
            if soft_reset_side not in ("buy", "sell"):
                soft_reset_side = "sell" if net_units > 0 else ("buy" if net_units < 0 else None)

        if reset_active:
            timed_out = bool(reset_started and (now_ts - reset_started) >= reset_timeout_s)
            rebalanced = inv_usd <= max(1.0, inv_soft_limit_usd * 0.20)
            if timed_out or rebalanced:
                reset_active = False
                reset_started = 0.0
                soft_reset_side = None

        state["grid_reset_active"] = bool(reset_active)
        state["grid_reset_side"] = soft_reset_side or ""
        state["grid_reset_started_ts"] = float(reset_started or 0.0)

    grid_orders = _compute_grid_prices(
        reference_price,
        dynamic_spread_bp,
        levels,
        strategy=strategy,
        net_units=net_units,
        mid_price=mid,
        best_bid=best_bid,
        best_ask=best_ask,
        soft_reset_side=soft_reset_side,
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
    buy_levels = min(levels, int(buy_budget_usd // min_order_notional_usd))
    sell_levels = min(levels, int(sell_budget_usd // min_order_notional_usd))
    if buy_levels <= 0 and sell_levels <= 0:
        needed_cycle_notional = min_order_notional_usd * 2.0
        return {
            "success": False,
            "error": (
                f"MM cycle notional is too small for exchange minimum order size. "
                f"Set cycle notional to at least ${needed_cycle_notional:.0f}."
            ),
            "orders_placed": 0,
            "orders_cancelled": orders_cancelled,
            "reason": "cycle notional below exchange minimum",
            "spread_bp": dynamic_spread_bp,
            "reference_price": reference_price,
        }
    per_level_buy_size = (buy_budget_usd / max(1, buy_levels) / mid) if buy_levels > 0 else 0.0
    per_level_sell_size = (sell_budget_usd / max(1, sell_levels) / mid) if sell_levels > 0 else 0.0

    orders_placed = 0
    errors = []
    quote_distances_bp = []
    placed_notional_usd = 0.0

    for order_spec in grid_orders:
        if orders_placed >= available_slots:
            break
        order_price = order_spec["price"]
        price_tol = order_price * 1e-6
        if any(abs(order_price - ep) < price_tol for ep in existing_prices):
            continue
        if pause_flatten_only:
            if net_units > 0 and order_spec["is_long"]:
                continue
            if net_units < 0 and not order_spec["is_long"]:
                continue
        if order_spec["is_long"] and order_spec["level"] > buy_levels:
            continue
        if (not order_spec["is_long"]) and order_spec["level"] > sell_levels:
            continue

        size_to_use = per_level_buy_size if order_spec["is_long"] else per_level_sell_size
        if size_to_use <= 0:
            continue
        result = execute_limit_order(
            telegram_id,
            product,
            size_to_use,
            order_price,
            is_long=order_spec["is_long"],
            leverage=leverage,
            enforce_rate_limit=False,
            post_only=True,
            source=strategy,
            strategy_session_id=state.get("strategy_session_id"),
            reduce_only=bool(
                pause_flatten_only
                and abs(net_units) > 0
                and inventory_source == "exchange"
            ),
        )
        if not result.get("success"):
            err_text = str(result.get("error") or "").lower()
            if "post-only" in err_text and "crosses the book" in err_text:
                retry_price = _reprice_post_only_quote(
                    client,
                    product_id,
                    bool(order_spec["is_long"]),
                    order_price,
                )
                retry_tol = max(abs(order_price) * 1e-6, 1e-9)
                if retry_price > 0 and abs(retry_price - order_price) > retry_tol:
                    result = execute_limit_order(
                        telegram_id,
                        product,
                        size_to_use,
                        retry_price,
                        is_long=order_spec["is_long"],
                        leverage=leverage,
                        enforce_rate_limit=False,
                        post_only=True,
                        source=strategy,
                        strategy_session_id=state.get("strategy_session_id"),
                        reduce_only=bool(
                            pause_flatten_only
                            and abs(net_units) > 0
                            and inventory_source == "exchange"
                        ),
                    )
                    if result.get("success"):
                        order_price = retry_price

        if result.get("success"):
            orders_placed += 1
            existing_prices.append(order_price)
            digest = result.get("digest")
            if digest and digest != "unknown":
                digest_s = str(digest)
                now_placed = time.time()
                order_birth_ts[digest_s] = now_placed
                tracked_quotes = state.setdefault("mm_tracked_quotes", {})
                if not isinstance(tracked_quotes, dict):
                    tracked_quotes = {}
                    state["mm_tracked_quotes"] = tracked_quotes
                tracked_quotes[digest_s] = {
                    "digest": digest_s,
                    "price": float(order_price),
                    "is_long": bool(order_spec["is_long"]),
                    "size": float(size_to_use),
                    "placed_ts": now_placed,
                }
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
    state["mm_last_inventory_source"] = inventory_source
    state["mm_last_ref_price"] = round(reference_price, 8)
    state["mm_last_spread_bp"] = round(dynamic_spread_bp, 4)
    state["mm_last_metrics"] = {
        "maker_fill_ratio": round(maker_fill_ratio, 4),
        "cancellation_ratio": round(cancellation_ratio, 4),
        "avg_quote_distance_bp": round(avg_quote_distance_bp, 3),
        "quote_refresh_rate": round(quote_refresh_rate, 4),
        "inventory_skew_usd": round(inv_usd, 4),
        "inventory_source": inventory_source,
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
    result = {
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
        "inventory_source": inventory_source,
        "pause_reason": state.get("mm_pause_reason") or None,
        "errors": errors if errors else None,
        "grid_reset_active": bool(state.get("grid_reset_active", False)) if strategy in ("grid", "rgrid") else None,
        "grid_reset_side": state.get("grid_reset_side") if strategy in ("grid", "rgrid") else None,
        "grid_cycle_pnl_usd": state.get("grid_last_cycle_pnl_usd") if strategy in ("grid", "rgrid") else None,
    }

    # Update cumulative PnL tracking
    cycle_pnl = float(result.get("pnl", 0) or 0)
    if strategy == "rgrid":
        current_grid_pnl = float(state.get("grid_last_cycle_pnl_usd") or 0.0)
        previous_grid_pnl = float(state.get("grid_prev_cycle_pnl_usd") or 0.0)
        cycle_pnl = current_grid_pnl - previous_grid_pnl
        state["grid_prev_cycle_pnl_usd"] = current_grid_pnl
    state["mm_cumulative_pnl"] = mm_cumulative_pnl + cycle_pnl
    result["cycle_pnl_usd"] = cycle_pnl

    return result
