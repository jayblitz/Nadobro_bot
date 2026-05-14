"""
Strategy engine for GRID + Reverse GRID (RGRID).
GRID mode: symmetric maker grid around reference (former mm behavior).
RGRID mode: reverse-grid quoting with exposure anchor and PnL risk controls.
Cancels stale orders that drift beyond threshold from current mid.
"""
import logging
import math
import os
import time
from src.nadobro.config import get_product_id, get_product_max_leverage
from src.nadobro.services.product_catalog import get_product_min_quote_notional_usd
from src.nadobro.services.trade_service import execute_limit_order
from src.nadobro.services import pov_engine
from src.nadobro.services.nado_archive import get_pair_24h_volume_usd
from src.nadobro.services.rate_limit import (
    call_with_retry,
    market_price_is_empty,
)
# --- D-Grid intelligence upgrade (additive) ---
# These three modules are no-ops when ``state["dgrid_intelligence_enabled"]``
# is False, so importing them at top-level is safe for existing GRID/RGRID
# users. See docs/dynamic_grid_strategy.md (upgrade section) for the full plan.
from src.nadobro.strategies import _regime as _dgrid_regime
from src.nadobro.strategies import _layer_sizing as _dgrid_layer_sizing
from src.nadobro.strategies import _position_manager as _dgrid_pm
from src.nadobro.strategies import _quote_gate
from src.nadobro.strategies import _quote_economics

try:
    from src.nadobro.services.feature_flags import dgrid_intelligence_enabled as _dgi_env_default
except Exception:
    def _dgi_env_default() -> bool:
        return False

logger = logging.getLogger(__name__)


def _dgrid_intelligence_on(state: dict) -> bool:
    """Per-session flag with env-var default. State overrides env.

    Kept for backward compatibility with the original D-Grid wiring;
    new call sites should use ``_strategy_intelligence_on(state, strategy)``.
    """
    val = state.get("dgrid_intelligence_enabled")
    if val is None:
        return bool(_dgi_env_default())
    return bool(val)


def _strategy_intelligence_on(state: dict, strategy: str) -> bool:
    """Per-strategy intelligence flag check.

    Resolution order (highest first):
      1. ``state["<strategy>_intelligence_enabled"]`` — strategy-specific override.
      2. ``state["dgrid_intelligence_enabled"]`` — legacy global override (covers
         dgrid and any strategy that opts into the same flag).
      3. ``NADO_DGRID_INTELLIGENCE`` env var via feature_flags.
    """
    strategy = str(strategy or "").lower()
    if not strategy:
        return False
    explicit = state.get(f"{strategy}_intelligence_enabled")
    if explicit is not None:
        return bool(explicit)
    legacy = state.get("dgrid_intelligence_enabled")
    if legacy is not None:
        return bool(legacy)
    return bool(_dgi_env_default())

STALE_DRIFT_MULTIPLIER = 2.0
# Floor for the stale-quote drift threshold. Without this, ``spread_bp=0`` (or
# a tiny dgrid-internal spread) collapses ``max_offset`` to 0, and every
# resting order gets cancel-and-replaced every cycle — see the production
# logs that showed ``drift 0.0007% > 0.0000%`` triggering nonstop. 3 bp ~
# round-trip taker fee; cancelling below that is never economical.
STALE_DRIFT_FLOOR_BP = float(os.environ.get("NADO_STALE_DRIFT_FLOOR_BP", "3.0") or "3.0")
MM_DEFAULT_LEVELS = 2
GRID_DEFAULT_LEVELS = 4
MM_MIN_SPREAD_BP = 2.0
GRID_MIN_SPREAD_BP = 8.0
DEFAULT_MIN_ORDER_NOTIONAL_USD = 100.0
# Cushion vs venue maintenance / account-health buffers when translating per-quote
# notional into a collateral budget (tunable via state mm_collateral_safety_factor).
# 1.25 leaves enough headroom for funding/fees while not over-reserving budget.
MM_COLLATERAL_SAFETY_FACTOR = 1.25
GRID_SOFT_RESET_MIN_TIMEOUT_SECONDS = 15
# Minimum edge (bp) the soft-reset unwind rungs must keep vs the original
# entry VWAP. Prevents the "buy low, sell below entry" pattern where a
# soft reset replaced sell rungs with top-of-book and printed guaranteed
# losses. Configurable via state["grid_soft_reset_min_exit_edge_bp"].
GRID_SOFT_RESET_MIN_EXIT_EDGE_BP = 3.0
POST_ONLY_REPRICE_MAX_RETRIES = 3
POST_ONLY_REPRICE_STEP_BP = 0.5
DGRID_TREND_ON_VARIANCE_RATIO = 1.25
DGRID_RANGE_ON_VARIANCE_RATIO = 1.15
DGRID_MAX_SPREAD_BP = 50.0
DGRID_RESET_OPTIONS_BP = (5.0, 12.5, 25.0, 50.0, 100.0)

# Phase 4 reliability: track which strategy_session_ids we've already
# reconciled in *this* Python process. Persisted state survives a kill+restart,
# so a saved ``mm_resume_reconcile_session_id`` matching the live
# ``strategy_session_id`` is NOT a signal that we already reconciled in the
# current process — it just means the previous process did. We therefore key
# off this in-memory set so the resume marker re-stamps after every restart.
# Tests can clear this set to simulate a fresh process.
_PROCESS_RECONCILED_SESSIONS: set[str] = set()

# Tread Mid Mode parity. Mid Mode quotes pure mid ± spread×level with no anchor
# logic, no soft-reset, and accepts a continuous directional_bias in [-1.0, +1.0].
MID_DEFAULT_SPREAD_BP = 5.0
MID_MIN_SPREAD_BP = -10.0
MID_MAX_SPREAD_BP = 100.0
MID_BIAS_ALPHA_TILT = 0.20  # ±0.2 side multiplier per unit of bias.
MID_FULL_BIAS_MARGIN_UPLIFT = 0.20  # +20% margin requirement when |bias|=1.0.


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


def _resolve_directional_bias_value(directional_bias) -> float:
    """Convert a directional_bias config value into a float in [-1.0, +1.0].

    Mid Mode (Tread parity) accepts a continuous float; legacy GRID/RGRID/DGRID
    accept the strings ``neutral``, ``long_bias``, ``short_bias`` and map them to
    discrete ±1 / 0 values for the purpose of margin uplift accounting. Side-
    multiplier shaping for the legacy string form preserves the old discrete
    1.15/0.85 magnitudes — see _resolve_side_multipliers.
    """
    if directional_bias is None:
        return 0.0
    if isinstance(directional_bias, (int, float)) and not isinstance(directional_bias, bool):
        try:
            return _clamp(float(directional_bias), -1.0, 1.0)
        except (TypeError, ValueError):
            return 0.0
    text = str(directional_bias).lower().strip()
    if text in ("long_bias", "long", "+long"):
        return 1.0
    if text in ("short_bias", "short", "-short"):
        return -1.0
    if text in ("neutral", "", "none", "off"):
        return 0.0
    # Numeric strings ("0.4", "-0.7") are accepted too.
    try:
        return _clamp(float(text), -1.0, 1.0)
    except (TypeError, ValueError):
        return 0.0


def _resolve_side_multipliers(
    directional_bias,
    net_units: float,
    inv_soft_usd: float,
    mid: float,
    *,
    use_continuous_bias: bool = False,
) -> tuple[float, float, bool, str]:
    """Resolve buy/sell quote-budget multipliers.

    ``directional_bias`` may be a legacy string (``neutral`` / ``long_bias`` /
    ``short_bias``) — used by GRID/RGRID/DGRID — or a float in [-1, +1] — used
    by Mid Mode. Pass ``use_continuous_bias=True`` to apply the Tread Mid Mode
    linear ±0.2 alpha tilt instead of the discrete legacy mapping.
    """
    buy_mult = 1.0
    sell_mult = 1.0
    inv_usd = abs(net_units) * mid
    pause_to_flatten_only = False
    pause_reason = ""
    inv_hard_usd = max(inv_soft_usd * 1.8, inv_soft_usd + 1.0)

    if use_continuous_bias:
        # Tread Mid Mode: bias in [-1, +1] → ±0.2 linear tilt per side.
        # Positive bias front-loads buys, back-loads sells.
        bias_value = _resolve_directional_bias_value(directional_bias)
        buy_mult *= max(0.0, 1.0 + MID_BIAS_ALPHA_TILT * bias_value)
        sell_mult *= max(0.0, 1.0 - MID_BIAS_ALPHA_TILT * bias_value)
    else:
        # Legacy discrete mapping for GRID/RGRID/DGRID — preserved exactly so the
        # previously-locked 1.15/0.85 side weights do not shift under live users.
        bias = (str(directional_bias) if directional_bias is not None else "").lower().strip()
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
    exit_floor_buy_vwap: float | None = None,
    exit_floor_sell_vwap: float | None = None,
    min_exit_edge_bp: float = GRID_SOFT_RESET_MIN_EXIT_EDGE_BP,
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
    #
    # Soft-reset (when inventory is heavy on one side and the market drifted
    # away from our anchor): the historical behaviour was to replace the rungs
    # on the unwind side with top-of-book — i.e. dump long inventory at
    # ``best_ask`` even when ``best_ask`` was *below* the average entry. That
    # baked the "buy low, sell below entry" pattern into every grid strategy.
    # We now clamp the unwind rungs to ``entry_vwap +/- min_exit_edge_bp`` so
    # the soft reset can chase the market *toward* breakeven without ever
    # printing a guaranteed-loss exit. If the market never revisits that
    # range, the session-level SL still fires as a true backstop.
    signed_half_spread = float(spread_bp) / 10000.0
    is_concede = signed_half_spread < 0.0
    exit_edge = max(0.0, float(min_exit_edge_bp)) / 10000.0
    orders = []
    for i in range(1, levels + 1):
        offset = signed_half_spread * i
        buy_price = reference_price * (1.0 - offset)
        sell_price = reference_price * (1.0 + offset)
        if soft_reset_side and mid_price and mid_price > 0:
            reset_nudge = (0.1 * i) / 10000.0
            if soft_reset_side == "buy":
                market_chase = float(best_bid or (mid_price * (1.0 - reset_nudge)))
                if exit_floor_sell_vwap and exit_floor_sell_vwap > 0:
                    # Closing a short: never buy back above sell_vwap - exit edge.
                    max_exit = exit_floor_sell_vwap * (1.0 - exit_edge)
                    buy_price = min(market_chase, max_exit)
                else:
                    buy_price = market_chase
            elif soft_reset_side == "sell":
                market_chase = float(best_ask or (mid_price * (1.0 + reset_nudge)))
                if exit_floor_buy_vwap and exit_floor_buy_vwap > 0:
                    # Closing a long: never sell below buy_vwap + exit edge.
                    min_exit = exit_floor_buy_vwap * (1.0 + exit_edge)
                    sell_price = max(market_chase, min_exit)
                else:
                    sell_price = market_chase
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
    # Enforce the configured floor (default 3 bp). DGRID can collapse spread_bp
    # toward zero in tight regimes, which made every resting order qualify as
    # stale and got cancel-and-replaced every cycle.
    max_offset = max(max_offset, STALE_DRIFT_FLOOR_BP / 10000.0)
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
    # F7 (Phase 5 audit): clear last cycle's skipped-levels marker up-front so
    # any early-return path (cycle-notional exhausted / max-orders reached /
    # threshold wait / etc.) leaves the dashboard with an empty list rather
    # than stale skips from the previous cycle's placement loop.
    state["mm_skipped_levels"] = []

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

    # Phase 4: bounded retry around the gateway market_price call so a transient
    # 429 / blip doesn't kill the cycle. Empty-result predicate also catches the
    # nado_client's swallowed-exception sentinel ({"bid":0,"ask":0,"mid":0}).
    mp_errors: list[str] = []
    try:
        mp, mp_errors = call_with_retry(
            client.get_market_price,
            product_id,
            max_retries=2,
            is_empty_result=market_price_is_empty,
            label="get_market_price",
        )
    except Exception as exc:
        return {
            "success": False,
            "error": f"market_price unavailable after retries: {exc}",
            "orders_placed": 0,
        }
    mp = mp or {}
    if mp_errors:
        state["mm_market_price_retries"] = list(mp_errors[-3:])
    else:
        state.pop("mm_market_price_retries", None)
    if mid <= 0:
        mid = float(mp.get("mid") or 0.0)
    if mid <= 0:
        return {"success": False, "error": "Could not fetch market price", "orders_placed": 0}
    best_bid = float(mp.get("bid") or 0.0)
    best_ask = float(mp.get("ask") or 0.0)

    if open_orders is None:
        try:
            open_orders, oo_errors = call_with_retry(
                client.get_open_orders,
                product_id,
                max_retries=2,
                label="get_open_orders",
            )
        except Exception as exc:
            return {
                "success": False,
                "error": f"open_orders unavailable after retries: {exc}",
                "orders_placed": 0,
            }
        if oo_errors:
            state["mm_open_orders_retries"] = list(oo_errors[-3:])
        else:
            state.pop("mm_open_orders_retries", None)
        open_orders = open_orders or []

    spread_bp = float(state.get("spread_bp") or 5.0)
    notional = float(state.get("notional_usd") or 100.0)
    # MM strategies normally run at per-asset MAX leverage so margin shrinks
    # proportionally and post-only refresh quotes can sit alongside closing legs.
    # The Tiny Budget Preset overrides this with the minimum leverage needed to
    # clear the venue min_size floor (state["mm_leverage_override"] set by the
    # preset handler — see handlers.callbacks).
    leverage_override = state.get("mm_leverage_override")
    leverage_mode = "MAX"
    try:
        max_leverage = float(get_product_max_leverage(product, network=network, client=client))
    except Exception:
        max_leverage = max(1.0, float(state.get("leverage") or 3.0))
    if leverage_override is not None:
        try:
            requested_lev = max(1.0, float(leverage_override))
            leverage = min(requested_lev, max_leverage)
            # F10 (Phase 5 audit): when the user's override exceeds the pair's
            # cap we clamp silently — surface that in the mode label so the
            # dashboard doesn't claim "TINY_BUDGET" while the engine ran at MAX.
            if requested_lev > max_leverage:
                leverage_mode = "TINY_BUDGET_CAPPED"
            else:
                leverage_mode = "TINY_BUDGET"
        except (TypeError, ValueError):
            leverage = max_leverage
    else:
        leverage = max_leverage
    state["leverage"] = leverage
    state["leverage_mode"] = leverage_mode
    max_orders = int(state.get("max_open_orders", 6))
    min_range_pct = float(state.get("min_range_pct") or 1.0)
    max_range_pct = float(state.get("max_range_pct") or 1.0)
    threshold_bp = float(state.get("threshold_bp") or 0.0)
    close_offset_bp = float(state.get("close_offset_bp") or 0.0)
    interval_seconds = max(1, int(state.get("interval_seconds") or 60))
    quote_ttl_seconds = max(0, int(state.get("quote_ttl_seconds") or (interval_seconds * 2)))

    # Phase 2: Tread Fi POV / participation engine. When the user opts into a
    # preset (Aggressive / Normal / Passive), derive interval cadence and
    # per-cycle notional from the pair's rolling 24h volume on the Nado archive.
    # Falls back transparently to user-pinned values if the archive is offline.
    participation_preset_raw = state.get("participation_preset")
    pov_cycle_notional_override: float | None = None
    if participation_preset_raw:
        preset_resolved = pov_engine.normalize_preset(str(participation_preset_raw))
        pair_24h_volume_usd: float | None = None
        try:
            pair_24h_volume_usd = get_pair_24h_volume_usd(
                network=network, product_id=product_id
            )
        except Exception:
            pair_24h_volume_usd = None
        if pair_24h_volume_usd and pair_24h_volume_usd > 0:
            pov_meta = pov_engine.compute_pov_duration(
                notional_usd=notional,
                preset=preset_resolved,
                pair_24h_volume_usd=pair_24h_volume_usd,
            )
            interval_seconds = max(1, int(pov_meta["interval_seconds"]))
            pov_cycle_notional_override = max(0.0, float(pov_meta["cycle_notional_usd"]))
            if not state.get("quote_ttl_seconds"):
                quote_ttl_seconds = max(0, interval_seconds * 2)
            state["mm_pov_engine"] = {
                "preset": pov_meta["preset"],
                "multiplier": pov_meta["multiplier"],
                "duration_minutes": round(pov_meta["duration_minutes"], 4),
                "interval_seconds": int(pov_meta["interval_seconds"]),
                "cycle_notional_usd": round(pov_meta["cycle_notional_usd"], 6),
                "pair_24h_volume_usd": round(pov_meta["pair_24h_volume_usd"], 2),
            }
            state.pop("mm_pov_engine_warning", None)
        else:
            state["mm_pov_engine_warning"] = (
                f"Nado archive returned no 24h volume for product_id={product_id} "
                f"on {network}; participation preset '{preset_resolved}' inactive this cycle."
            )
            state.pop("mm_pov_engine", None)
    else:
        state.pop("mm_pov_engine", None)
        state.pop("mm_pov_engine_warning", None)
    # GRID/RGRID/DGRID retain string semantics; Mid Mode reads the same field as a
    # float in [-1, +1] (see _resolve_directional_bias_value). Don't coerce here —
    # _resolve_side_multipliers branches on use_continuous_bias so both shapes work.
    directional_bias = state.get("directional_bias")
    if directional_bias is None:
        directional_bias = "neutral" if configured_strategy != "mid" else 0.0
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
    if pov_cycle_notional_override is not None:
        # POV scheduling: each cycle should slice the target into the
        # pov-engine-computed chunk, not the user's full notional. Floor at the
        # venue minimum so the bot still places at least one quote per cycle if
        # the schedule rounds below it.
        cycle_notional = max(pov_cycle_notional_override, 0.0)
    session_cap_notional = max(0.0, float(state.get("session_notional_cap_usd") or 0.0))
    carry_notional = max(0.0, float(state.get("mm_notional_carry_usd") or 0.0))
    session_done = max(0.0, float(state.get("mm_session_notional_done_usd") or 0.0))
    # Resolve venue minimum notional. Priority:
    # 1) Caller override (state["min_order_notional_usd"]) — for tests / power users.
    # 2) Live Nado `symbols` payload (canonical, per-pair, x18 USDT0 floor).
    # 3) Static fallback so a transient catalog miss does not crash the cycle.
    state_min = state.get("min_order_notional_usd")
    catalog_min = None
    try:
        catalog_min = get_product_min_quote_notional_usd(product, network=network, client=client)
    except Exception:
        catalog_min = None
    if state_min:
        try:
            min_order_notional_usd = max(1.0, float(state_min))
        except (TypeError, ValueError):
            min_order_notional_usd = float(catalog_min or DEFAULT_MIN_ORDER_NOTIONAL_USD)
    elif catalog_min and catalog_min > 0:
        min_order_notional_usd = float(catalog_min)
    else:
        min_order_notional_usd = DEFAULT_MIN_ORDER_NOTIONAL_USD
    state["mm_min_order_notional_usd_resolved"] = round(float(min_order_notional_usd), 6)
    leverage_for_budget = max(1.0, float(leverage or 1.0))
    # Venue min-notional is the hard USD floor on *order value* per resting quote.
    # Leverage does not reduce the floor — it lets a small collateral wallet *reach*
    # the floor (notional = collateral × leverage). Per-quote *margin* requirement
    # is min_notional / leverage × safety_factor.
    order_birth_ts = state.setdefault("mm_order_birth_ts", {})
    if not isinstance(order_birth_ts, dict):
        order_birth_ts = {}
        state["mm_order_birth_ts"] = order_birth_ts

    if strategy == "mid":
        levels = max(1, int(state.get("levels", MM_DEFAULT_LEVELS)))
        # Mid Mode: spread is signed, default 5 bps, range [-10, +100] per Tread.
        spread_bp = float(state.get("spread_bp") or MID_DEFAULT_SPREAD_BP)
        spread_bp = _clamp(spread_bp, MID_MIN_SPREAD_BP, MID_MAX_SPREAD_BP)
        # Mid Mode tightens its own min/max envelope so dgrid-style realized-vol
        # widening cannot push beyond the documented Tread range.
        min_spread_bp = max(MID_MIN_SPREAD_BP, float(state.get("min_spread_bp") or MID_MIN_SPREAD_BP))
        max_spread_bp = min(MID_MAX_SPREAD_BP, float(state.get("max_spread_bp") or MID_MAX_SPREAD_BP))
        if min_spread_bp > max_spread_bp:
            min_spread_bp, max_spread_bp = MID_MIN_SPREAD_BP, MID_MAX_SPREAD_BP
    elif strategy == "dgrid":
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

    # --- MM intelligence: regime classifier (Layer B) -------------------
    # Fires for DGRID, R-GRID, Classic GRID, and MID Mode when intelligence is
    # enabled. The same classifier output is consumed by sizing (Layer A) and
    # PM (Layer C). Each strategy gets its own size table and PM defaults via
    # _layer_sizing.STRATEGY_REGIME_TABLES and _position_manager.STRATEGY_DEFAULTS.
    regime_info: dict = {}
    _intel_strategy = configured_strategy if configured_strategy in ("dgrid", "rgrid", "grid", "mid") else ""
    _intel_active = bool(_intel_strategy) and _strategy_intelligence_on(state, _intel_strategy)
    if _intel_active:
        ema_fast_now, ema_slow_now = _update_both_emas(state, mid, ema_fast_alpha, ema_slow_alpha)
        # For dgrid, use the variance ratio computed by _apply_dgrid_controls.
        # For rgrid, compute it inline against the same mid history because the
        # rgrid path doesn't call _apply_dgrid_controls.
        _vr = float(state.get("dgrid_variance_ratio") or 0.0)
        if _vr <= 0:
            _vr = _compute_variance_ratio(
                history,
                short_window=int(state.get("dgrid_short_window_points") or 4),
                long_window=int(state.get("dgrid_long_window_points") or 12),
            )
        _realized_move = float(state.get("dgrid_realized_move_bp") or 0.0)
        if _realized_move <= 0:
            _realized_move = _compute_realized_vol_bp(history)
        regime_info = _dgrid_regime.classify_regime(
            state,
            history,
            product=str(product),
            variance_ratio=_vr,
            realized_vol_bp=_realized_move if _realized_move > 0 else vol_bp,
            ema_fast=ema_fast_now,
            ema_slow=ema_slow_now,
            short_window=int(state.get("dgrid_short_window_points") or 4),
            long_window=int(state.get("dgrid_long_window_points") or 12),
            config=state,
        )
    if strategy == "rgrid":
        max_abs = max(abs(min_spread_bp), abs(max_spread_bp))
        dynamic_spread_bp = _clamp(dynamic_spread_bp, -max_abs, max_abs)

        # --- R-GRID Momentum Detection & Dynamic Mode Switching ---
        momentum = _evaluate_rgrid_momentum(
            state, mid, history, ema_fast_alpha, ema_slow_alpha,
            client=client, product_id=product_id,
        )
        rgrid_mode = str(state.get("rgrid_active_mode") or "classic")
        # R-GRID intelligence veto: when the legacy 2-of-3 momentum-break rule
        # says "flip to reversed" but the regime classifier disagrees with
        # high confidence (range_tight / chop_high_vol), suppress the flip.
        # This catches the case where 2 signals fire but the broader regime
        # context shows we're not actually in a directional trend.
        _veto_flip = False
        _veto_reason = ""
        if _intel_active and momentum["momentum_break"] and rgrid_mode == "classic":
            _veto_regimes = {
                _dgrid_regime.REGIME_RANGE_TIGHT,
                _dgrid_regime.REGIME_CHOP_HIGH_VOL,
            }
            _veto_confidence_min = float(state.get("rgrid_veto_confidence_min") or 0.60)
            _veto_strategy_confidence = float((regime_info or {}).get("confidence") or 0.0)
            _veto_regime_label = str((regime_info or {}).get("regime") or "")
            if _veto_regime_label in _veto_regimes and _veto_strategy_confidence >= _veto_confidence_min:
                _veto_flip = True
                _veto_reason = (
                    f"regime={_veto_regime_label} conf={_veto_strategy_confidence:.2f} "
                    f">= veto_min={_veto_confidence_min:.2f}"
                )
                state["rgrid_momentum_veto_count"] = int(state.get("rgrid_momentum_veto_count") or 0) + 1
                state["rgrid_momentum_veto_last_ts"] = time.time()
                state["rgrid_momentum_veto_last_reason"] = _veto_reason
                logger.info(
                    "RGRID intelligence veto: momentum break suppressed (signals=%d/3, %s)",
                    momentum["signals_active"], _veto_reason,
                )

        if momentum["momentum_break"] and rgrid_mode == "classic" and not _veto_flip:
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

    # --- MM intelligence: position manager (Layer C) --------------------
    # Fires for DGRID and R-GRID. Per-strategy defaults (e.g., R-Grid's higher
    # partial_tp_bp and trail_give_back_fraction) live in _position_manager.STRATEGY_DEFAULTS.
    pm_summary: dict = {"enabled": False, "actions": [], "size_dampener": 1.0,
                        "cooldown_active": False}
    if _intel_active:
        try:
            pm_summary = _dgrid_pm.manage_positions(
                telegram_id=telegram_id,
                product=str(product),
                product_id=product_id,
                state=state,
                positions=positions,
                regime_info=regime_info or {},
                enabled=bool(state.get("pm_enabled", True)),
                mid=mid,
            )
        except Exception as _pm_exc:
            logger.exception("dgrid PM cycle failed: %s", _pm_exc)
            pm_summary = {"enabled": False, "actions": [],
                          "size_dampener": 1.0, "cooldown_active": False,
                          "error": str(_pm_exc)}
        # Re-read positions after any PM-driven closes so subsequent sizing
        # sees the new inventory.
        if pm_summary.get("actions"):
            try:
                positions = client.get_all_positions() or []
            except Exception:
                pass
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
            inv_usd = abs(net_units) * mid

    cancelled_digests = set(str(d) for d in (state.get("mm_recently_cancelled_digests") or []))
    # Phase 4: resume reconciliation. ``_reconcile_executed_quotes`` already
    # queries the archive for any tracked digest that is no longer in the open-
    # orders list (Phase 0 work). On the *first cycle of this Python process*
    # for a given strategy_session_id we additionally stamp
    # ``mm_resume_reconciled_*`` so /mm_status can show that we ran the
    # reconcile pass before re-quoting (no orphan orders, no double-quoting).
    #
    # We key this off ``_PROCESS_RECONCILED_SESSIONS`` rather than the
    # persisted ``mm_resume_reconcile_session_id`` because the latter survives
    # restarts — comparing it against the live session_id would always show
    # equality after a kill+restart and the marker would never re-stamp.
    session_id = state.get("strategy_session_id")
    session_key = str(session_id) if session_id is not None else ""
    is_resume_first_cycle = bool(
        session_key
        and session_key not in _PROCESS_RECONCILED_SESSIONS
        and (state.get("mm_tracked_quotes") or state.get("grid_buy_fills") or state.get("grid_sell_fills"))
    )
    pre_reconcile_tracked = len(state.get("mm_tracked_quotes") or {})
    executed_quotes = _reconcile_executed_quotes(state, network, open_orders, cancelled_digests)
    state["mm_recently_cancelled_digests"] = []
    if executed_quotes:
        for q in executed_quotes:
            _append_grid_exposure_fill(state, q)
        # Track last fill timestamp so the PM's stale-flatten rule has a clock.
        state["grid_last_fill_ts"] = float(executed_quotes[-1].get("placed_ts") or time.time())
    if session_key:
        # Mark this session as reconciled-in-this-process unconditionally on
        # the first cycle we see it, even if there was no persisted state to
        # reconcile. That way we don't repeat the bookkeeping on every cycle.
        _PROCESS_RECONCILED_SESSIONS.add(session_key)
    if is_resume_first_cycle:
        state["mm_resume_reconciled_at"] = time.time()
        state["mm_resume_reconcile_session_id"] = session_key
        state["mm_resume_tracked_count"] = pre_reconcile_tracked
        state["mm_resume_executed_count"] = len(executed_quotes)
        logger.info(
            "MM resume reconcile complete: session=%s tracked=%d executed=%d (user=%s)",
            session_key,
            pre_reconcile_tracked,
            len(executed_quotes),
            telegram_id,
        )
    if configured_strategy == "mid":
        # Mid Mode (Tread parity) intentionally ignores grid_anchor_price,
        # discretion, and the rolling-VWAP exposure anchor. The centerline is
        # purely _compute_reference_price(mid) — no fill-following, no soft-reset.
        grid_anchor_price = 0.0
        buy_exposure = 0.0
        sell_exposure = 0.0
        state["grid_prev_net_units"] = net_units
        state["grid_anchor_price"] = 0.0
        state["grid_buy_exposure_price"] = 0.0
        state["grid_sell_exposure_price"] = 0.0
    else:
        grid_anchor_price = float(state.get("grid_anchor_price") or 0.0)
        discretion = _clamp(float(state.get("rgrid_discretion") or state.get("grid_discretion") or 0.06), 0.01, 0.5)
        recent_fraction = _clamp(discretion * 2.0, 0.02, 0.5)
        buy_exposure = _rolling_vwap_recent_fraction(state.get("grid_buy_fills") or [], recent_fraction)
        sell_exposure = _rolling_vwap_recent_fraction(state.get("grid_sell_fills") or [], recent_fraction)
        # Anchor resolution.
        #
        # Legacy behaviour ("fill_vwap"): the anchor was whichever side had the
        # most recent fills. In a one-sided drift this dragged the anchor in
        # the same direction as the trend — RGRID's "reference_price" then
        # pointed *down* in a downtrend, sells got re-centered onto the new
        # lower anchor, and the bot booked guaranteed losses.
        #
        # Default ("ema_mid"): when only one side has filled, hold the prior
        # anchor (or fall back to the externally-computed EMA reference_price)
        # so the anchor doesn't chase the trend that is filling the bot. Only
        # blend in the two-sided fill center when *both* sides have traded —
        # that's a genuine range center, not a one-sided drift.
        anchor_mode = str(state.get("anchor_mode") or "ema_mid").strip().lower()
        if anchor_mode not in ("fill_vwap", "ema_mid"):
            anchor_mode = "ema_mid"
        state["anchor_mode_resolved"] = anchor_mode
        if anchor_mode == "fill_vwap":
            if buy_exposure > 0 and sell_exposure > 0:
                grid_anchor_price = (buy_exposure + sell_exposure) / 2.0
            elif buy_exposure > 0:
                grid_anchor_price = buy_exposure
            elif sell_exposure > 0:
                grid_anchor_price = sell_exposure
        else:
            if buy_exposure > 0 and sell_exposure > 0:
                grid_anchor_price = (buy_exposure + sell_exposure) / 2.0
            # One-sided fills: keep the previous anchor; fall through to the
            # cold-start fallback chain below when there is no prior anchor.
        if grid_anchor_price <= 0:
            grid_anchor_price = float(state.get("grid_last_fill_price") or 0.0)
        if grid_anchor_price <= 0:
            grid_anchor_price = reference_price
        if executed_quotes:
            last_exec = executed_quotes[-1]
            exec_price = float(last_exec.get("price") or 0.0)
            if exec_price > 0:
                state["grid_last_fill_price"] = exec_price
                # In fill_vwap mode the anchor jumps to the last fill (legacy).
                # In ema_mid mode we deliberately do NOT jump — the two-sided
                # VWAP center already adapts to the genuine range.
                if anchor_mode == "fill_vwap":
                    grid_anchor_price = exec_price
        elif float(state.get("grid_last_fill_price") or 0.0) <= 0 and grid_anchor_price > 0:
            state["grid_last_fill_price"] = grid_anchor_price
        state["grid_prev_net_units"] = net_units
        state["grid_anchor_price"] = grid_anchor_price
        state["grid_buy_exposure_price"] = round(buy_exposure, 8) if buy_exposure > 0 else 0.0
        state["grid_sell_exposure_price"] = round(sell_exposure, 8) if sell_exposure > 0 else 0.0
    if strategy == "rgrid":
        # Legacy: RGRID used the fill-VWAP anchor directly as reference_price.
        # In ema_mid mode we keep the EMA-derived reference and only let the
        # fill-VWAP center *nudge* it on confirmed two-sided fills — that
        # preserves R-Grid's "anchor near the recent trade range" intent
        # without dragging the anchor in a one-sided drift.
        rgrid_anchor_mode = str(state.get("anchor_mode") or "ema_mid").strip().lower()
        if rgrid_anchor_mode == "fill_vwap":
            reference_price = grid_anchor_price
        elif grid_anchor_price > 0 and buy_exposure > 0 and sell_exposure > 0:
            blend = _clamp(float(state.get("rgrid_anchor_blend") or 0.30), 0.0, 1.0)
            reference_price = (1.0 - blend) * reference_price + blend * grid_anchor_price

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
    # SL/TP percentages are applied to *margin* (notional / leverage), matching the
    # log message "of margin" below and Tread Fi's documented perp formula:
    #   Max Loss = (SL% / 100) × (Notional / Leverage)
    # Earlier code applied the percentage to raw notional, which made stops fire at
    # a much larger absolute drawdown than the user configured for leveraged pairs.
    margin_for_pnl = max(0.0, notional) / max(1.0, float(leverage or 1.0))
    max_loss_usd = (pnl_stop_pct / 100.0) * margin_for_pnl
    max_profit_usd = (pnl_take_pct / 100.0) * margin_for_pnl
    grid_cycle_pnl = _compute_grid_cycle_pnl_usd(positions, product_id)
    state["grid_last_cycle_pnl_usd"] = round(grid_cycle_pnl, 6)
    if max_loss_usd > 0 and grid_cycle_pnl <= (-max_loss_usd):
        # MM intelligence: instead of just halting, fire PM cooldown so the
        # next cycle re-engages at reduced size rather than slamming back in.
        if _intel_active:
            _dgrid_pm.trigger_cooldown(state, reason="grid_stop_loss_hit")
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
            "pm_cooldown_until": state.get("pm_cooldown_until"),
        }
    if max_profit_usd > 0 and grid_cycle_pnl >= max_profit_usd:
        if _intel_active:
            _dgrid_pm.trigger_cooldown(state, reason="grid_take_profit_hit")
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
            "pm_cooldown_until": state.get("pm_cooldown_until"),
        }

    buy_mult, sell_mult, pause_flatten_only, pause_reason = _resolve_side_multipliers(
        directional_bias,
        net_units,
        inv_soft_limit_usd,
        mid,
        use_continuous_bias=(configured_strategy == "mid"),
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
        try:
            # F4 (Phase 5 audit): bypass nado_client's open-orders cache after a
            # cancel — otherwise the post-cancel snapshot can return the cached
            # pre-cancel list (or, on a transient 429 the SDK swallowed
            # internally, an empty list cached for the TTL window) and the
            # placement loop would believe quotes are gone when they aren't.
            open_orders, _refresh_errors = call_with_retry(
                client.get_open_orders,
                product_id,
                max_retries=1,
                label="get_open_orders.refresh",
                refresh=True,
            )
            open_orders = open_orders or []
        except Exception:
            # Soft-fail: fall back to the pre-cancel snapshot rather than aborting.
            logger.warning("post-cancel open_orders refresh failed; using stale list", exc_info=True)

    if threshold_wait_result is not None:
        threshold_wait_result["orders_cancelled"] = int(orders_cancelled)
        return threshold_wait_result

    max_open_order_slots = max(0, max_orders - len(open_orders))
    if max_open_order_slots == 0:
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
        # Drift / soft-reset trigger compares mid vs the *persistent* grid
        # anchor — not the cycle's quote reference. In ema_mid anchor mode the
        # quote reference is the slow EMA of mid (so quote prices don't drag
        # with fills), but the soft-reset arm/disarm still needs the sticky
        # anchor as its "where the grid was centered" reference. Falling back
        # to reference_price keeps cold-start behaviour intact.
        drift_anchor = float(grid_anchor_price or reference_price or 0.0)
        drift_from_anchor_pct = 0.0
        if drift_anchor > 0:
            drift_from_anchor_pct = abs(mid - drift_anchor) / drift_anchor * 100.0
        state["grid_drift_from_anchor_pct"] = round(drift_from_anchor_pct, 6)
        if reset_threshold_pct > 0 and drift_anchor > 0 and inv_usd > 0:
            if net_units > 0 and ((drift_anchor - mid) / drift_anchor * 100.0 >= reset_threshold_pct):
                reset_active = True
                if not reset_started:
                    reset_started = now_ts
                soft_reset_side = "sell"
            elif net_units < 0 and ((mid - drift_anchor) / drift_anchor * 100.0 >= reset_threshold_pct):
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

    # Phase 3: regime-aware quote gate. Veto entire-cycle quoting in regimes
    # where the math is structurally negative (REGIME_RANGE_TIGHT with high
    # confidence), cap levels + widen spread in CHOP_HIGH_VOL, and skip the
    # adverse side in confirmed trends. Existing PM rules continue to manage
    # inventory either way. Quote-gate is a no-op when intelligence is off.
    quote_gate_decision = _quote_gate.QuoteGateDecision()
    if _intel_active:
        quote_gate_decision = _quote_gate.evaluate_quote_gate(
            strategy=strategy,
            regime_info=regime_info,
            state=state,
        )
    if quote_gate_decision.active:
        state["mm_quote_gate"] = quote_gate_decision.to_dict()
        if quote_gate_decision.spread_widen_mult > 1.0:
            dynamic_spread_bp = dynamic_spread_bp * float(
                quote_gate_decision.spread_widen_mult
            )
        if quote_gate_decision.skip_buy and quote_gate_decision.skip_sell:
            logger.info(
                "Quote gate veto (%s): %s — skipping new quotes this cycle",
                strategy, quote_gate_decision.reason,
            )
            return {
                "success": True,
                "orders_placed": 0,
                "orders_cancelled": orders_cancelled,
                "action": "quote_gate_veto",
                "reason": quote_gate_decision.reason,
                "spread_bp": dynamic_spread_bp,
                "reference_price": reference_price,
                "quote_gate": quote_gate_decision.to_dict(),
            }
    else:
        state.pop("mm_quote_gate", None)

    # Phase 4: per-quote expectancy filter. The regime gate above stops
    # quoting when the *market state* is wrong; this stops quoting (or
    # auto-widens) when the *spread itself* is too tight to clear
    # fees + slippage + funding + a minimum required edge. Funding is
    # read from the rgrid funding cache when present; absent that, the
    # funding leg is 0 and the fee/slippage legs drive the decision.
    if _intel_active:
        _funding_bp_per_hour = 0.0
        _funding_cached = state.get("rgrid_prev_funding_bp")
        if _funding_cached is not None:
            try:
                # rgrid_prev_funding_bp is the funding rate in bp. Treat it
                # as a per-hour magnitude (conservative — Nado funding
                # accrues continuously); the expected-hold scaling in
                # _quote_economics keeps this bounded.
                _funding_bp_per_hour = abs(float(_funding_cached))
            except (TypeError, ValueError):
                _funding_bp_per_hour = 0.0
        quote_econ = _quote_economics.evaluate_quote_economics(
            strategy=strategy,
            spread_bp=dynamic_spread_bp,
            regime=str((regime_info or {}).get("regime") or ""),
            funding_bp_per_hour=_funding_bp_per_hour,
            max_spread_bp=max_spread_bp,
            state=state,
        )
        state["mm_quote_economics"] = quote_econ.to_dict()
        if quote_econ.widened and quote_econ.recommended_spread_bp > dynamic_spread_bp:
            dynamic_spread_bp = float(quote_econ.recommended_spread_bp)
        if not quote_econ.viable:
            logger.info(
                "Quote economics veto (%s): %s — skipping new quotes this cycle",
                strategy, quote_econ.reason,
            )
            return {
                "success": True,
                "orders_placed": 0,
                "orders_cancelled": orders_cancelled,
                "action": "quote_economics_skip",
                "reason": quote_econ.reason,
                "spread_bp": dynamic_spread_bp,
                "reference_price": reference_price,
                "quote_economics": quote_econ.to_dict(),
            }
    else:
        state.pop("mm_quote_economics", None)

    # Pass the per-side entry VWAPs so the soft-reset unwind never prints a
    # guaranteed-loss exit (see _compute_grid_prices for the floor logic).
    # Mid Mode runs without grid VWAP bookkeeping, so these stay None.
    exit_floor_buy_vwap = float(state.get("grid_buy_exposure_price") or 0.0) or None
    exit_floor_sell_vwap = float(state.get("grid_sell_exposure_price") or 0.0) or None
    soft_reset_exit_edge_bp = float(
        state.get("grid_soft_reset_min_exit_edge_bp")
        or GRID_SOFT_RESET_MIN_EXIT_EDGE_BP
    )
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
        exit_floor_buy_vwap=exit_floor_buy_vwap,
        exit_floor_sell_vwap=exit_floor_sell_vwap,
        min_exit_edge_bp=soft_reset_exit_edge_bp,
    )
    if quote_gate_decision.active:
        grid_orders = _quote_gate.apply_gate_to_orders(quote_gate_decision, grid_orders)
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
    if configured_strategy == "mid":
        # Tread Mid Mode: linear margin uplift up to +20% at |bias|=1.0.
        bias_value = _resolve_directional_bias_value(directional_bias)
        bias_margin_uplift = 1.0 + (MID_FULL_BIAS_MARGIN_UPLIFT * abs(bias_value))
        margin_per_quote_est *= bias_margin_uplift
        state["mm_bias_margin_uplift"] = round(bias_margin_uplift, 6)
        state["mm_directional_bias_resolved"] = round(bias_value, 6)
    collateral_budget_cfg = max(0.0, float(notional))
    spot_usdt = _mm_spot_usdt_balance(client)
    effective_collateral = collateral_budget_cfg
    if spot_usdt > 0:
        effective_collateral = min(effective_collateral, spot_usdt)

    max_by_collateral = (
        int(effective_collateral / margin_per_quote_est) if margin_per_quote_est > 0 else 0
    )
    gross_resting_cap = max(0, min(max_orders, max_by_collateral))
    max_resting_quotes = gross_resting_cap
    cycle_slot_cap = None
    if session_cap_notional > 0:
        cycle_slot_cap = int(cycle_target_notional // min_order_notional_usd)
        max_resting_quotes = min(max_resting_quotes, cycle_slot_cap)
    available_collateral_slots = max(0, gross_resting_cap - len(open_orders))
    quote_slot_budget = min(max_open_order_slots, available_collateral_slots)
    if cycle_slot_cap is not None:
        quote_slot_budget = min(quote_slot_budget, cycle_slot_cap)

    state["mm_effective_collateral_usd"] = round(effective_collateral, 6)
    state["mm_margin_per_quote_est_usd"] = round(margin_per_quote_est, 6)
    state["mm_max_resting_quotes_cap"] = int(max_resting_quotes)

    if max_resting_quotes <= 0:
        if session_cap_notional > 0 and (cycle_slot_cap or 0) <= 0:
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
        # Compute the leverage that *would* let this collateral reach the venue floor,
        # so the user gets actionable guidance instead of a dead-end error.
        try:
            required_lev = max(1.0, math.ceil(min_order_notional_usd / max(1.0, effective_collateral)))
            try:
                lev_cap = float(get_product_max_leverage(product, network=network, client=client))
            except Exception:
                lev_cap = float(leverage_for_budget)
        except Exception:
            required_lev = leverage_for_budget
            lev_cap = leverage_for_budget
        if required_lev > lev_cap:
            guidance = (
                f"This pair's max leverage ({lev_cap:.0f}x) cannot lift ${effective_collateral:.0f} "
                f"to the ${min_order_notional_usd:.0f} venue floor — pick a pair with a smaller min_size "
                f"or add collateral to ${math.ceil(min_order_notional_usd / lev_cap):.0f}+."
            )
        else:
            guidance = (
                f"Increase leverage to >= {required_lev:.0f}x via the Tiny Budget Preset to reach the "
                f"${min_order_notional_usd:.0f} venue floor, or pick a pair with a smaller min_size."
            )
        return {
            "success": False,
            "error": (
                f"MM collateral budget (~${effective_collateral:.0f}) is too small for even one "
                f"venue-sized quote at ~${min_order_notional_usd:.0f} notional with "
                f"{leverage_for_budget:.1f}x leverage (~${need:.2f} estimated margin per quote "
                f"including safety buffer). {guidance}"
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
    if quote_slot_budget <= 0:
        return {
            "success": True,
            "orders_placed": 0,
            "orders_cancelled": orders_cancelled,
            "reason": "max collateral-backed quote slots reached",
            "spread_bp": dynamic_spread_bp,
            "reference_price": reference_price,
            "mm_effective_collateral_usd": round(effective_collateral, 4),
            "mm_margin_per_quote_est_usd": round(margin_per_quote_est, 4),
            "mm_max_resting_quotes_cap": int(max_resting_quotes),
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

    # --- MM intelligence: per-level adaptive sizing (Layer A) -----------
    # When enabled (DGRID or R-GRID), ``_dgrid_layer_size_for`` below is called
    # per order spec and overrides ``per_level_*_size``. R-Grid pulls its own
    # regime table via _layer_sizing.STRATEGY_REGIME_TABLES["rgrid"].
    _intel_on = bool(_intel_active)
    _pm_dampener = float(pm_summary.get("size_dampener") or 1.0) if _intel_on else 1.0
    _max_per_level_usd = float(state.get("layer_max_per_level_usd") or (notional * 3.0))
    _layer_regime = str((regime_info or {}).get("regime") or _dgrid_regime.REGIME_RANGE_WIDE)
    _layer_realized_vol_bp = float((regime_info or {}).get("realized_vol_bp") or vol_bp or 0.0)

    # Mid Mode resolves directional_bias to a float in [-1, +1] and passes it
    # to size_quote_level as ``bias_value``. The result is a multiplicative
    # tilt of +/- 20% per side that LAYERS on top of the regime size mult.
    # Other strategies pass 0.0 so the bias term is neutral.
    _intel_bias_value = 0.0
    if _intel_on and configured_strategy == "mid":
        try:
            _intel_bias_value = _resolve_directional_bias_value(directional_bias)
        except Exception:
            _intel_bias_value = 0.0

    def _dgrid_layer_size_for(is_long: bool, level: int) -> float:
        if not _intel_on:
            return per_level_buy_size if is_long else per_level_sell_size
        side = "buy" if is_long else "sell"
        res = _dgrid_layer_sizing.size_quote_level(
            side=side,
            level=level,
            base_notional_usd=float(notional),
            mid_price=float(mid),
            inv_usd=float(inv_usd),
            net_units=float(net_units),
            inv_soft_usd=float(inv_soft_limit_usd),
            regime=_layer_regime,
            realized_vol_bp=_layer_realized_vol_bp,
            min_order_notional_usd=float(min_order_notional_usd),
            max_per_level_usd=_max_per_level_usd,
            state=state,
            config=state,
            bias_value=_intel_bias_value,
        )
        size_base = float(res["size_base"]) * _pm_dampener
        # Floor at venue minimum, ceiling at config max.
        floor_base = float(min_order_notional_usd) / max(float(mid), 1e-12)
        return max(floor_base, size_base)

    orders_placed = 0
    errors = []
    quote_distances_bp = []
    placed_notional_usd = 0.0
    # Phase 4: levels whose post-only retry ladder exhausted are tracked here so
    # /mm_status and the cycle result can surface the skip rather than dropping
    # silently. Reset every cycle.
    skipped_levels: list[dict] = []
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
        if orders_placed >= quote_slot_budget:
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

        if _intel_on:
            size_to_use = _dgrid_layer_size_for(bool(order_spec["is_long"]), int(order_spec["level"]))
        else:
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
                retry_attempts_used = 0
                for retry_attempt in range(POST_ONLY_REPRICE_MAX_RETRIES):
                    retry_attempts_used = retry_attempt + 1
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
                # Phase 4: if all retries failed with the same post-only error,
                # surface this as a "skipped" level rather than a silent drop.
                if not result.get("success"):
                    skipped_levels.append({
                        "level": int(order_spec["level"]),
                        "side": "BUY" if order_spec["is_long"] else "SELL",
                        "intended_price": float(order_spec["price"]),
                        "last_attempted_price": float(last_price),
                        "attempts": int(retry_attempts_used),
                        "reason": "post_only_retries_exhausted",
                        "error": str(result.get("error") or "")[:200],
                    })

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

    try:
        final_open_orders, _final_errors = call_with_retry(
            client.get_open_orders,
            product_id,
            max_retries=1,
            label="get_open_orders.final",
        )
    except Exception:
        # Final post-cycle measurement is for analytics; degrade gracefully.
        logger.warning("final open_orders fetch failed; using empty list", exc_info=True)
        final_open_orders = []
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
    # Phase 4: persist this cycle's skipped levels (cleared each cycle so the
    # dashboard always shows the latest state, not stale skips).
    state["mm_skipped_levels"] = list(skipped_levels)

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
        "skipped_levels": list(skipped_levels) if skipped_levels else [],
        "skipped_levels_count": len(skipped_levels),
    }
    if configured_strategy == "dgrid":
        result.update({
            "dgrid_phase": state.get("dgrid_phase"),
            "dgrid_variance_ratio": state.get("dgrid_variance_ratio"),
            "dgrid_realized_move_bp": state.get("dgrid_realized_move_bp"),
            "dgrid_reset_threshold_bp": state.get("dgrid_reset_threshold_bp"),
            "dgrid_phase_changed": state.get("dgrid_phase_changed"),
        })

    # Intelligence telemetry: same fields for DGRID and R-GRID so the
    # dashboard renders identically. R-Grid also gets the veto counters.
    if _intel_active:
        result.update({
            f"{_intel_strategy}_intelligence_enabled": True,
            "intelligence_strategy": _intel_strategy,
            "regime": (regime_info or {}).get("regime"),
            "regime_confidence": (regime_info or {}).get("confidence"),
            "regime_drift_bp": (regime_info or {}).get("drift_bp"),
            "regime_votes": (regime_info or {}).get("votes"),
            "regime_changed": (regime_info or {}).get("regime_changed"),
            "pm_actions": pm_summary.get("actions"),
            "pm_cooldown_active": pm_summary.get("cooldown_active"),
            "pm_size_dampener": pm_summary.get("size_dampener"),
            "layer_sizing_telemetry": (state.get("mm_layer_sizing_telemetry") or [])[-8:],
            "quote_gate": state.get("mm_quote_gate"),
            "quote_economics": state.get("mm_quote_economics"),
        })
        if _intel_strategy == "rgrid":
            result.update({
                "rgrid_momentum_veto_count": state.get("rgrid_momentum_veto_count") or 0,
                "rgrid_momentum_veto_last_reason": state.get("rgrid_momentum_veto_last_reason"),
                "rgrid_momentum_veto_last_ts": state.get("rgrid_momentum_veto_last_ts"),
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
