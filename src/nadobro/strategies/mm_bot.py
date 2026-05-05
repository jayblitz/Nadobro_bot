"""
Strategy engine for GRID + Reverse GRID (RGRID).
GRID mode: symmetric maker grid around reference (former mm behavior).
RGRID mode: reverse-grid quoting with exposure anchor and PnL risk controls.
Cancels stale orders that drift beyond threshold from current mid.
"""
import logging
import math
import time
from src.nadobro.config import get_product_id, get_product_max_leverage
from src.nadobro.services.trade_service import execute_limit_order

logger = logging.getLogger(__name__)

STALE_DRIFT_MULTIPLIER = 2.0
MM_DEFAULT_LEVELS = 2
GRID_DEFAULT_LEVELS = 4
MM_MIN_SPREAD_BP = 2.0
GRID_MIN_SPREAD_BP = 8.0
DEFAULT_MIN_ORDER_NOTIONAL_USD = 100.0
# Rough cushion vs venue maintenance / account-health buffers when translating
# per-quote notional into a collateral budget (tunable via state mm_collateral_safety_factor).
MM_COLLATERAL_SAFETY_FACTOR = 1.75
GRID_SOFT_RESET_MIN_TIMEOUT_SECONDS = 15
POST_ONLY_REPRICE_MAX_RETRIES = 3
POST_ONLY_REPRICE_STEP_BP = 0.5
DGRID_TREND_ON_VARIANCE_RATIO = 1.25
DGRID_RANGE_ON_VARIANCE_RATIO = 1.15
DGRID_MAX_SPREAD_BP = 50.0
DGRID_RESET_OPTIONS_BP = (5.0, 12.5, 25.0, 50.0, 100.0)


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


def _compute_variance_ratio(history: list[float], short_window: int = 4, long_window: int = 12) -> float:
    """Approximate a variance ratio from recent absolute log returns.

    Tread DGRID uses a volatility proxy variance ratio to distinguish ranging
    vs trending regimes. Nadobro may not have a full external volatility proxy
    for every market yet, so we derive a local proxy from the same mid history
    already used by the MM engine.
    """
    returns = []
    for i in range(1, len(history)):
        prev = float(history[i - 1] or 0.0)
        cur = float(history[i] or 0.0)
        if prev <= 0 or cur <= 0:
            continue
        returns.append((cur - prev) / prev)
    if len(returns) < max(3, short_window):
        return 1.0

    short = returns[-max(2, short_window):]
    long = returns[-max(short_window, long_window):]

    def _variance(values: list[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        return sum((v - mean) ** 2 for v in values) / max(1, len(values) - 1)

    long_var = _variance(long)
    if long_var <= 1e-18:
        return 1.0
    return max(0.0, _variance(short) / long_var)


def _snap_reset_threshold_bp(value_bp: float) -> float:
    value = max(0.0, float(value_bp or 0.0))
    return min(DGRID_RESET_OPTIONS_BP, key=lambda opt: abs(opt - value))


def _apply_dgrid_controls(state: dict, history: list[float], base_spread_bp: float) -> dict:
    variance_ratio = _compute_variance_ratio(
        history,
        short_window=int(state.get("dgrid_short_window_points") or 4),
        long_window=int(state.get("dgrid_long_window_points") or 12),
    )
    realized_move_bp = _compute_realized_vol_bp(history)
    previous_phase = str(state.get("dgrid_phase") or "grid").lower()
    if previous_phase not in ("grid", "rgrid"):
        previous_phase = "grid"

    phase = previous_phase
    trend_on = float(state.get("dgrid_trend_on_variance_ratio") or DGRID_TREND_ON_VARIANCE_RATIO)
    range_on = float(state.get("dgrid_range_on_variance_ratio") or DGRID_RANGE_ON_VARIANCE_RATIO)
    if variance_ratio >= trend_on:
        phase = "rgrid"
    elif variance_ratio <= range_on:
        phase = "grid"

    spread_from_move = _clamp(
        max(float(state.get("dgrid_min_spread_bp") or 0.0), realized_move_bp),
        0.0,
        float(state.get("dgrid_max_spread_bp") or DGRID_MAX_SPREAD_BP),
    )
    if spread_from_move <= 0:
        spread_from_move = max(0.0, float(base_spread_bp or 0.0))
    quote_spread_bp = 0.0 if phase == "rgrid" else spread_from_move
    reset_bp = _snap_reset_threshold_bp(max(spread_from_move * 4.0, 5.0))
    reset_pct = reset_bp / 100.0

    changed = phase != previous_phase
    now = time.time()
    if changed:
        state["dgrid_last_switch_ts"] = now
    state["dgrid_phase"] = phase
    state["dgrid_variance_ratio"] = round(variance_ratio, 6)
    state["dgrid_realized_move_bp"] = round(realized_move_bp, 4)
    state["dgrid_dynamic_spread_bp"] = round(quote_spread_bp, 4)
    state["dgrid_reset_threshold_bp"] = reset_bp
    state["dgrid_last_eval_ts"] = now
    state["dgrid_phase_changed"] = bool(changed)
    state["grid_reset_threshold_pct"] = reset_pct
    state["rgrid_reset_threshold_pct"] = reset_pct
    return {
        "phase": phase,
        "variance_ratio": variance_ratio,
        "realized_move_bp": realized_move_bp,
        "spread_bp": quote_spread_bp,
        "reset_threshold_bp": reset_bp,
        "phase_changed": changed,
    }


def _update_both_emas(state: dict, mid: float, ema_fast_alpha: float, ema_slow_alpha: float) -> tuple[float, float]:
    """Advance and persist both EMAs exactly once per cycle.

    Idempotent within a single run_cycle via `_mm_ema_updated_mid` tag so subsequent
    callers (reference price, crossover detector) read consistent values without
    double-mixing the alpha.
    """
    tag = float(state.get("_mm_ema_updated_mid") or 0.0)
    if tag > 0 and abs(tag - mid) < mid * 1e-12:
        return (
            float(state.get("mm_ref_ema_fast") or mid),
            float(state.get("mm_ref_ema_slow") or mid),
        )
    prev_fast = float(state.get("mm_ref_ema_fast") or mid)
    prev_slow = float(state.get("mm_ref_ema_slow") or mid)
    ema_fast = (ema_fast_alpha * mid) + ((1.0 - ema_fast_alpha) * prev_fast)
    ema_slow = (ema_slow_alpha * mid) + ((1.0 - ema_slow_alpha) * prev_slow)
    state["mm_ref_ema_fast"] = ema_fast
    state["mm_ref_ema_slow"] = ema_slow
    state["_mm_ema_updated_mid"] = float(mid)
    return ema_fast, ema_slow


def _compute_reference_price(state: dict, mid: float, mode: str, ema_fast_alpha: float, ema_slow_alpha: float) -> float:
    mode = (mode or "mid").lower().strip()
    if mode == "mid":
        return mid
    ema_fast, ema_slow = _update_both_emas(state, mid, ema_fast_alpha, ema_slow_alpha)
    if mode == "ema_fast":
        return ema_fast
    if mode == "ema_slow":
        return ema_slow
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


def _mm_margin_per_quote_estimate_usd(
    min_order_notional_usd: float,
    leverage: float,
    safety_factor: float,
) -> float:
    lev = max(1.0, float(leverage or 1.0))
    mn = max(1.0, float(min_order_notional_usd))
    sf = max(1.0, float(safety_factor))
    return (mn / lev) * sf


def _mm_spot_usdt_balance(client) -> float:
    try:
        bal = client.get_balance() or {}
        if not bal.get("exists"):
            return 0.0
        b = bal.get("balances") or {}
        return float(b.get(0, b.get("0", 0)) or 0.0)
    except Exception:
        return 0.0


def _mm_allocate_quote_levels(
    max_slots: int,
    levels: int,
    buy_mult: float,
    sell_mult: float,
) -> tuple[int, int]:
    """Split concurrent quote slots across buy/sell ladders using side weights."""
    max_slots = max(0, int(max_slots))
    levels = max(1, int(levels))
    tw = max(1e-12, float(buy_mult) + float(sell_mult))
    raw_b = max_slots * float(buy_mult) / tw
    raw_s = max_slots * float(sell_mult) / tw
    bi = min(levels, int(math.floor(raw_b)))
    si = min(levels, int(math.floor(raw_s)))
    while bi + si > max_slots:
        if bi >= si and bi > 0:
            bi -= 1
        elif si > 0:
            si -= 1
        else:
            break
    while bi + si < max_slots:
        rb = raw_b - bi if bi < levels else -1.0
        rs = raw_s - si if si < levels else -1.0
        if rb <= 0 and rs <= 0:
            break
        if rb >= rs and bi < levels:
            bi += 1
        elif si < levels:
            si += 1
        else:
            break
    return bi, si


def estimate_mm_quote_capacity(
    collateral_usd: float,
    min_order_notional_usd: float,
    leverage: float,
    max_open_orders: int = 6,
    *,
    safety_factor: float | None = None,
) -> dict[str, float | int]:
    """Estimate resting-quote concurrency from collateral budget (for previews / preflight).

    Each resting quote still carries ~min_order_notional_usd notional; leverage only
    reduces estimated margin per quote for budgeting purposes.
    """
    sf = MM_COLLATERAL_SAFETY_FACTOR if safety_factor is None else float(safety_factor)
    margin_per = _mm_margin_per_quote_estimate_usd(min_order_notional_usd, leverage, sf)
    if margin_per <= 0:
        return {"margin_per_quote_est_usd": 0.0, "max_resting_quotes": 0, "min_collateral_1_quote_usd": 0.0, "min_collateral_2_quote_usd": 0.0}
    raw_slots = int(max(0.0, float(collateral_usd)) / margin_per)
    max_slots = max(0, min(int(max_open_orders), raw_slots))
    return {
        "margin_per_quote_est_usd": round(margin_per, 4),
        "max_resting_quotes": max_slots,
        "min_collateral_1_quote_usd": round(margin_per, 4),
        "min_collateral_2_quote_usd": round(2.0 * margin_per, 4),
    }


def _detect_ema_crossover(state: dict, mid: float, ema_fast_alpha: float, ema_slow_alpha: float) -> dict:
    """Detect EMA fast/slow crossover for momentum detection.

    Captures previous-cycle EMAs BEFORE advancing them, so the crossover is measured
    across cycles (not within-cycle). Advancement is delegated to `_update_both_emas`,
    which is idempotent per-mid so this stays consistent with the reference-price path.
    """
    prev_fast = float(state.get("mm_ref_ema_fast") or mid)
    prev_slow = float(state.get("mm_ref_ema_slow") or mid)
    ema_fast, ema_slow = _update_both_emas(state, mid, ema_fast_alpha, ema_slow_alpha)

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
    # GRID and RGRID share one price shape under POSITIVE spread: buys BELOW
    # anchor, sells ABOVE anchor. RGRID's "reversal" is applied via the
    # side-budget skew (directional_bias), not by flipping the prices — the
    # previous implementation flipped them and then the post-only clamp below
    # collapsed every RGRID level to top-of-book.
    #
    # NEGATIVE spread_bp is a separate, intentional mode: the bot concedes the
    # trade by crossing the book (buys ABOVE anchor, sells BELOW anchor). In
    # that mode we deliberately skip the post-only clamp; we're not trying to
    # be a maker.
    signed_half_spread = float(spread_bp) / 10000.0
    is_concede = signed_half_spread < 0.0
    orders = []
    for i in range(1, levels + 1):
        offset = signed_half_spread * i
        buy_price = reference_price * (1.0 - offset)
        sell_price = reference_price * (1.0 + offset)
        if soft_reset_side and mid_price and mid_price > 0:
            reset_nudge = (0.1 * i) / 10000.0
            if soft_reset_side == "buy":
                buy_price = float(best_bid or (mid_price * (1.0 - reset_nudge)))
            elif soft_reset_side == "sell":
                sell_price = float(best_ask or (mid_price * (1.0 + reset_nudge)))
        # Post-only safety (positive-spread maker mode only): buys can't sit
        # at/above best_bid, sells can't sit at/below best_ask.
        if not is_concede:
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
        # Prefer venue-reported total/net PnL, but do not let a zero net value hide
        # meaningful unrealized or realized PnL carried in separate fields.
        selected = None
        try:
            net_pnl = p.get("net_pnl")
            if net_pnl is not None:
                net_value = float(net_pnl)
                if abs(net_value) > 1e-12:
                    selected = net_value
                else:
                    unrealized = p.get("unrealized_pnl")
                    realized = p.get("realized_pnl")
                    if unrealized is not None or realized is not None:
                        selected = float(unrealized or 0.0) + float(realized or 0.0)
                    else:
                        selected = net_value
        except (TypeError, ValueError):
            selected = None
        if selected is None:
            for key in ("unrealized_pnl", "pnl", "realized_pnl"):
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


def _reprice_post_only_quote(
    client,
    product_id: int,
    is_long: bool,
    fallback_price: float,
    attempt: int = 0,
) -> float:
    """Step the post-only quote further inside the safe side of the book on each retry.

    Attempt 0 = 0.5 bp inside, attempt 1 = 1.0 bp, attempt 2 = 2.0 bp. This gives the
    retry a real chance of clearing during fast markets instead of bouncing off the
    same level we just failed on.
    """
    try:
        mp = client.get_market_price(product_id) or {}
        bid = float(mp.get("bid") or 0.0)
        ask = float(mp.get("ask") or 0.0)
        step_bp = POST_ONLY_REPRICE_STEP_BP * (2.0 ** max(0, attempt))
        nudge = step_bp / 10000.0
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
    # Clear per-cycle EMA idempotency tag so `_update_both_emas` will advance the
    # EMAs on this cycle's mid (the tag is only to prevent double-mixing within
    # the SAME cycle when both the crossover detector and the reference-price
    # path want the latest EMAs).
    state.pop("_mm_ema_updated_mid", None)

    product = state.get("product", "BTC")
    strategy = str(state.get("strategy", "grid") or "grid").lower()
    if strategy == "mm":
        strategy = "grid"
    configured_strategy = strategy
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
    # CEO directive: MM strategies always run at per-asset MAX leverage so margin
    # shrinks proportionally and post-only refresh quotes can sit alongside closing
    # legs. The user-configured leverage in state is overwritten here for transparency.
    try:
        leverage = float(get_product_max_leverage(product, network=network, client=client))
    except Exception:
        # Defensive fallback: keep prior behavior if catalog is unavailable.
        leverage = max(1.0, float(state.get("leverage") or 3.0))
    state["leverage"] = leverage
    state["leverage_mode"] = "MAX"
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
    # Use the larger of the user's explicit per-cycle notional or their `notional_usd`
    # directly. Leverage is applied separately at place-order (per Nado risk weights);
    # multiplying it in here would double-count margin and blow through level sizing.
    cycle_notional = max(cycle_notional_cfg, notional)
    session_cap_notional = max(0.0, float(state.get("session_notional_cap_usd") or 0.0))
    carry_notional = max(0.0, float(state.get("mm_notional_carry_usd") or 0.0))
    session_done = max(0.0, float(state.get("mm_session_notional_done_usd") or 0.0))
    min_order_notional_usd = max(
        1.0,
        float(state.get("min_order_notional_usd") or DEFAULT_MIN_ORDER_NOTIONAL_USD),
    )
    leverage_for_budget = max(1.0, float(leverage or 1.0))
    # Venue min-notional is USD per resting quote after bumps — leverage reduces *margin*
    # per dollar notional but does not shrink how much notional each quote carries.
    order_birth_ts = state.setdefault("mm_order_birth_ts", {})
    if not isinstance(order_birth_ts, dict):
        order_birth_ts = {}
        state["mm_order_birth_ts"] = order_birth_ts

    if strategy == "dgrid":
        levels = max(1, int(state.get("levels", GRID_DEFAULT_LEVELS)))
        spread_bp = max(float(state.get("spread_bp") or GRID_MIN_SPREAD_BP), MM_MIN_SPREAD_BP)
    elif strategy == "rgrid":
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
        # Maker PnL needs spread wide enough to beat fees + adverse selection; very tight bps looks
        # profitable on volume but bleeds on taker/queue losses.
        if spread_bp != 0:
            sign = 1.0 if spread_bp > 0 else -1.0
            spread_bp = sign * max(abs(float(spread_bp)), GRID_MIN_SPREAD_BP)
        else:
            spread_bp = float(GRID_MIN_SPREAD_BP)
    else:
        levels = max(1, int(state.get("levels", MM_DEFAULT_LEVELS)))
        spread_bp = max(spread_bp, MM_MIN_SPREAD_BP)

    if session_cap_notional > 0 and session_done >= session_cap_notional:
        state["running"] = False
        state["mm_pause_reason"] = "Session notional cap reached"
        return {
            "success": True,
            "done": True,
            "action": "session_cap_reached",
            "orders_placed": 0,
            "orders_cancelled": 0,
            "reason": "session notional cap reached",
        }

    history = _update_mid_history(
        state,
        mid,
        max_points=max(vol_window, 8, int(state.get("dgrid_long_window_points") or 12) + 1),
    )
    if configured_strategy == "dgrid":
        dgrid_state = _apply_dgrid_controls(state, history, spread_bp)
        strategy = dgrid_state["phase"]
        spread_bp = float(dgrid_state["spread_bp"])
        # Preserve user-visible strategy identity while running through the
        # existing GRID/RGRID engine internally.
        state["strategy"] = "dgrid"
    vol_bp = _compute_realized_vol_bp(history[-vol_window:])
    dynamic_spread_bp = spread_bp * (1.0 + (vol_bp * vol_sensitivity / 100.0))
    if configured_strategy == "dgrid":
        dynamic_spread_bp = float(spread_bp)
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
            (
                state.get("rgrid_stop_loss_pct") if strategy == "rgrid" else state.get("grid_stop_loss_pct")
            )
            or state.get("sl_pct")
            or 0.0
        ),
    )
    pnl_take_pct = max(
        0.0,
        float(
            (
                state.get("rgrid_take_profit_pct") if strategy == "rgrid" else state.get("grid_take_profit_pct")
            )
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
                (
                    state.get("rgrid_reset_threshold_pct") if strategy == "rgrid" else state.get("grid_reset_threshold_pct")
                )
                or 0.0
            ),
        )
        reset_timeout_s = max(
            GRID_SOFT_RESET_MIN_TIMEOUT_SECONDS,
            int(
                (
                    state.get("rgrid_reset_timeout_seconds") if strategy == "rgrid" else state.get("grid_reset_timeout_seconds")
                )
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

    safety_mm = MM_COLLATERAL_SAFETY_FACTOR
    try:
        sfx = float(state.get("mm_collateral_safety_factor") or 0.0)
        if sfx >= 1.0:
            safety_mm = sfx
    except (TypeError, ValueError):
        pass
    margin_per_quote_est = _mm_margin_per_quote_estimate_usd(
        min_order_notional_usd, leverage_for_budget, safety_mm
    )
    collateral_budget_cfg = max(0.0, float(notional))
    spot_usdt = _mm_spot_usdt_balance(client)
    effective_collateral = collateral_budget_cfg
    if spot_usdt > 0:
        effective_collateral = min(effective_collateral, spot_usdt)

    max_by_collateral = (
        int(effective_collateral / margin_per_quote_est) if margin_per_quote_est > 0 else 0
    )
    max_resting_quotes = max(0, min(max_orders, max_by_collateral))
    if session_cap_notional > 0:
        cycle_slot_cap = int(cycle_target_notional // min_order_notional_usd)
        max_resting_quotes = min(max_resting_quotes, cycle_slot_cap)

    state["mm_effective_collateral_usd"] = round(effective_collateral, 6)
    state["mm_margin_per_quote_est_usd"] = round(margin_per_quote_est, 6)
    state["mm_max_resting_quotes_cap"] = int(max_resting_quotes)

    if max_resting_quotes <= 0:
        if session_cap_notional > 0 and cycle_target_notional > 0:
            return {
                "success": True,
                "orders_placed": 0,
                "orders_cancelled": orders_cancelled,
                "reason": "session remainder below venue min quote notional",
                "spread_bp": dynamic_spread_bp,
                "reference_price": reference_price,
                "mm_effective_collateral_usd": round(effective_collateral, 4),
                "mm_margin_per_quote_est_usd": round(margin_per_quote_est, 4),
                "mm_max_resting_quotes_cap": 0,
            }
        need = margin_per_quote_est
        return {
            "success": False,
            "error": (
                f"MM collateral budget (~${effective_collateral:.0f}) is too small for even one "
                f"venue-sized quote at ~${min_order_notional_usd:.0f} notional with "
                f"{leverage_for_budget:.1f}x leverage (~${need:.2f} estimated margin per quote "
                f"including safety buffer). Add collateral, raise configured margin, or use a "
                f"market with lower minimum size."
            ),
            "orders_placed": 0,
            "orders_cancelled": orders_cancelled,
            "reason": "collateral below estimated per-quote margin",
            "spread_bp": dynamic_spread_bp,
            "reference_price": reference_price,
            "mm_effective_collateral_usd": round(effective_collateral, 4),
            "mm_margin_per_quote_est_usd": round(margin_per_quote_est, 4),
            "mm_max_resting_quotes_cap": 0,
        }

    buy_levels, sell_levels = _mm_allocate_quote_levels(
        max_resting_quotes, levels, buy_mult, sell_mult
    )
    if buy_levels <= 0 and sell_levels <= 0:
        return {
            "success": False,
            "error": "Could not allocate grid levels for MM quote slots (check levels / inventory pause state).",
            "orders_placed": 0,
            "orders_cancelled": orders_cancelled,
            "reason": "zero allocated quote levels",
            "spread_bp": dynamic_spread_bp,
            "reference_price": reference_price,
        }

    min_quote_size = min_order_notional_usd / max(mid, 1e-12)
    per_level_buy_size = float(min_quote_size) if buy_levels > 0 else 0.0
    per_level_sell_size = float(min_quote_size) if sell_levels > 0 else 0.0

    orders_placed = 0
    errors = []
    quote_distances_bp = []
    placed_notional_usd = 0.0
    # Maker-only mandate: every quote is post-only. The retry ladder below
    # already uses post_only=True; aligning the first attempt prevents the
    # RGRID-concede branch (negative dynamic_spread_bp) from ever taking
    # liquidity. A crossing post-only is rejected by the exchange and the
    # ladder reprices on the safe side. (Audit 2026-05.)
    post_only_quotes = True

    # Look up the product's price tick so the dedupe tolerance matches Nado's
    # enforced price_increment. A 1e-6 relative tolerance is finer than the tick
    # on most markets, which let same-tick duplicate orders slip through.
    price_increment = 0.0
    try:
        from src.nadobro.services.nado_client import _price_increment_cache  # type: ignore
        price_increment = float(_price_increment_cache.get((network, product_id)) or 0.0)
    except Exception:
        price_increment = 0.0

    for order_spec in grid_orders:
        if orders_placed >= available_slots:
            break
        order_price = order_spec["price"]
        # Use the larger of the relative 1e-6 or half a tick so orders that round
        # to the same tick are correctly treated as duplicates.
        price_tol = max(order_price * 1e-6, price_increment * 0.5)
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
        size_to_use = max(float(size_to_use), min_order_notional_usd / max(mid, 1e-12))
        result = execute_limit_order(
            telegram_id,
            product,
            size_to_use,
            order_price,
            is_long=order_spec["is_long"],
            leverage=leverage,
            enforce_rate_limit=False,
            post_only=post_only_quotes,
            source=configured_strategy,
            strategy_session_id=state.get("strategy_session_id"),
            reduce_only=bool(
                pause_flatten_only
                and abs(net_units) > 0
                and inventory_source == "exchange"
            ),
        )
        if not result.get("success"):
            err_text = str(result.get("error") or "").lower()
            if post_only_quotes and "post-only" in err_text and "crosses the book" in err_text:
                # Retry with progressively wider nudges inside the safe side of the
                # book. A single 0.2-bp nudge used to silently lose the quote whenever
                # the book flickered during repricing.
                last_price = order_price
                for retry_attempt in range(POST_ONLY_REPRICE_MAX_RETRIES):
                    retry_price = _reprice_post_only_quote(
                        client,
                        product_id,
                        bool(order_spec["is_long"]),
                        last_price,
                        attempt=retry_attempt,
                    )
                    retry_tol = max(abs(last_price) * 1e-6, 1e-9)
                    if retry_price <= 0 or abs(retry_price - last_price) <= retry_tol:
                        break
                    last_price = retry_price
                    result = execute_limit_order(
                        telegram_id,
                        product,
                        size_to_use,
                        retry_price,
                        is_long=order_spec["is_long"],
                        leverage=leverage,
                        enforce_rate_limit=False,
                        post_only=True,
                        source=configured_strategy,
                        strategy_session_id=state.get("strategy_session_id"),
                        reduce_only=bool(
                            pause_flatten_only
                            and abs(net_units) > 0
                            and inventory_source == "exchange"
                        ),
                    )
                    if result.get("success"):
                        order_price = retry_price
                        break
                    retry_err = str(result.get("error") or "").lower()
                    if "post-only" not in retry_err or "crosses the book" not in retry_err:
                        break

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
        "strategy": configured_strategy,
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
        "mm_buy_quote_levels": buy_levels,
        "mm_sell_quote_levels": sell_levels,
        "mm_max_resting_quotes_cap": max_resting_quotes,
        "mm_margin_per_quote_est_usd": round(margin_per_quote_est, 4),
        "mm_effective_collateral_usd": round(effective_collateral, 4),
        "grid_reset_active": bool(state.get("grid_reset_active", False)) if strategy in ("grid", "rgrid") else None,
        "grid_reset_side": state.get("grid_reset_side") if strategy in ("grid", "rgrid") else None,
        "grid_cycle_pnl_usd": state.get("grid_last_cycle_pnl_usd") if strategy in ("grid", "rgrid") else None,
    }
    if configured_strategy == "dgrid":
        result.update({
            "dgrid_phase": state.get("dgrid_phase"),
            "dgrid_variance_ratio": state.get("dgrid_variance_ratio"),
            "dgrid_realized_move_bp": state.get("dgrid_realized_move_bp"),
            "dgrid_reset_threshold_bp": state.get("dgrid_reset_threshold_bp"),
            "dgrid_phase_changed": state.get("dgrid_phase_changed"),
        })

    # Update cumulative PnL tracking. Previously this only worked for RGRID; GRID
    # fell through to `result.get("pnl", 0)` on a dict that never had a "pnl" key,
    # so the circuit breaker's drawdown check was always a no-op on GRID.
    current_grid_pnl = float(state.get("grid_last_cycle_pnl_usd") or 0.0)
    previous_grid_pnl = float(state.get("grid_prev_cycle_pnl_usd") or 0.0)
    cycle_pnl = current_grid_pnl - previous_grid_pnl
    state["grid_prev_cycle_pnl_usd"] = current_grid_pnl
    state["mm_cumulative_pnl"] = mm_cumulative_pnl + cycle_pnl
    result["cycle_pnl_usd"] = cycle_pnl

    return result


# === AUDIT COMPLETE: Market Making Bot (GRID / RGRID / DGRID) ===
# Status: Improved & Ready
# Key Changes: Runtime now relies on PnL-aware grid exits, preserves maker sizing without leverage double-counting, and surfaces quote-health telemetry.
