"""
Dynamic Grid — multi-signal regime classifier.

Promotes the variance-ratio gate in ``_apply_dgrid_controls`` from a binary
range/trend switch to a five-state classifier consumed by the sizing engine
(_layer_sizing.py) and the position manager (_position_manager.py).

Data sources, in priority order:

1. ``services.price_tracker`` — already populated every 60s by
   ``scheduler.tick_price_tracker``. Provides RSI/EMA/MACD/Bollinger/vol via
   the in-process technicals cache (same one volume_bot.py uses).
2. The strategy's own ``state["mm_mid_history"]`` rolling buffer — present
   from cycle 2 onward, no external dependency.
3. ``services.nado_client.get_funding_rate`` and ``get_candlesticks`` — used
   only for warm-up enrichment when price_tracker hasn't accumulated yet.

All signals degrade to neutral on missing data; we never raise out of this
module. ``classify_regime`` is pure-function w.r.t. the inputs we pass in,
so it is straightforward to unit-test with synthetic mid history.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)


# --- Regime labels ---------------------------------------------------------
REGIME_RANGE_TIGHT = "range_tight"
REGIME_RANGE_WIDE = "range_wide"
REGIME_TREND_UP = "trend_up"
REGIME_TREND_DOWN = "trend_down"
REGIME_CHOP_HIGH_VOL = "chop_high_vol"

ALL_REGIMES = (
    REGIME_RANGE_TIGHT,
    REGIME_RANGE_WIDE,
    REGIME_TREND_UP,
    REGIME_TREND_DOWN,
    REGIME_CHOP_HIGH_VOL,
)


# --- Defaults --------------------------------------------------------------
DEFAULT_TREND_VR_THRESHOLD = 1.25       # carried from existing DGRID config
DEFAULT_RANGE_VR_THRESHOLD = 1.15
DEFAULT_DRIFT_TREND_BP = 8.0            # min |drift| to call it directional
DEFAULT_EMA_DIV_BP_STRONG = 5.0         # mirrors mm_bot.MOMENTUM_EMA_CROSSOVER_BP
DEFAULT_RANGE_EXPANSION_BP_TREND = 25.0 # rolling high-low expansion to call trend
DEFAULT_RANGE_TIGHT_VOL_BP = 4.0        # realized vol below this -> range_tight
DEFAULT_MIN_AGREEMENT = 3               # secondary signals needed to override VR


def _safe_mean(xs: list[float]) -> float:
    if not xs:
        return 0.0
    return sum(xs) / len(xs)


def _drift_bp(history: list[float], window: int) -> float:
    """Signed drift in bp over the last ``window`` mid points."""
    if not history or window <= 0:
        return 0.0
    n = len(history)
    if n < 2:
        return 0.0
    lookback = min(window, n - 1)
    old = float(history[-(lookback + 1)] or 0.0)
    cur = float(history[-1] or 0.0)
    if old <= 0:
        return 0.0
    return (cur - old) / old * 1e4


def _variance_ratio_from_history(
    history: list[float], short_window: int, long_window: int
) -> float:
    """Compute a variance ratio (short-window var / long-window var) of returns.

    Mirrors mm_bot._compute_variance_ratio so this module is self-sufficient
    when callers don't have a precomputed value to pass in.
    """
    returns: list[float] = []
    for i in range(1, len(history or [])):
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


def _realized_vol_bp_from_history(history: list[float]) -> float:
    """Average absolute return in bp. Mirrors mm_bot._compute_realized_vol_bp."""
    returns: list[float] = []
    for i in range(1, len(history or [])):
        prev = float(history[i - 1] or 0.0)
        cur = float(history[i] or 0.0)
        if prev <= 0 or cur <= 0:
            continue
        returns.append(abs((cur - prev) / prev))
    if not returns:
        return 0.0
    return (sum(returns) / len(returns)) * 1e4


def _range_expansion_bp(history: list[float], window: int) -> float:
    """Recent (high - low) / mid in bp over the last ``window`` mid points."""
    if not history or window <= 0:
        return 0.0
    win = history[-window:]
    valid = [float(v) for v in win if float(v or 0.0) > 0]
    if not valid:
        return 0.0
    hi, lo = max(valid), min(valid)
    mid = valid[-1]
    if mid <= 0:
        return 0.0
    return (hi - lo) / mid * 1e4


def _fill_asymmetry(state: dict) -> float:
    """Asymmetry score of recent fills, in [-1, +1].

    +1 means recent fills were heavily on the BUY side (we're getting hit on
    bids -> price is falling against us). -1 means heavily on the SELL side.
    Computed from grid_buy_fills / grid_sell_fills (already tracked by
    ``_append_grid_exposure_fill`` in mm_bot.py).
    """
    try:
        buys = state.get("grid_buy_fills") or []
        sells = state.get("grid_sell_fills") or []
        # Only consider the most recent 30 fills overall.
        recent = sorted(
            [{**f, "_side": "buy"} for f in buys[-30:]]
            + [{**f, "_side": "sell"} for f in sells[-30:]],
            key=lambda f: float(f.get("ts") or 0.0),
        )[-30:]
        if not recent:
            return 0.0
        b = sum(1 for f in recent if f["_side"] == "buy")
        s = sum(1 for f in recent if f["_side"] == "sell")
        total = b + s
        if total == 0:
            return 0.0
        return (b - s) / total
    except Exception:
        return 0.0


def _try_price_tracker(product: str) -> dict[str, Any]:
    """Pull RSI/EMA/vol/MACD from the shared price_tracker. Best-effort."""
    if not product:
        return {}
    try:
        from src.nadobro.services import price_tracker  # type: ignore
    except Exception:
        return {}
    out: dict[str, Any] = {}
    try:
        out["rsi_14"] = price_tracker.compute_rsi(product, 14)
    except Exception:
        out["rsi_14"] = None
    try:
        out["ema_9"] = price_tracker.compute_ema(product, 9)
        out["ema_21"] = price_tracker.compute_ema(product, 21)
        out["ema_50"] = price_tracker.compute_ema(product, 50)
    except Exception:
        out["ema_9"] = out["ema_21"] = out["ema_50"] = None
    try:
        out["volatility_pct"] = price_tracker.compute_volatility(product, 20)
    except Exception:
        out["volatility_pct"] = None
    try:
        out["macd"] = price_tracker.compute_macd(product)
    except Exception:
        out["macd"] = None
    try:
        out["tracker_regime"] = price_tracker.classify_regime(product)
    except Exception:
        out["tracker_regime"] = None
    return out


def _funding_bp(state: dict) -> float:
    """Last funding rate (bp) recorded by mm_bot._detect_funding_shift."""
    try:
        return float(state.get("rgrid_prev_funding_bp") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _signal_directions(
    *,
    drift_bp: float,
    drift_min: float,
    ema_div_bp: float,
    ema_bullish: bool,
    rsi: Optional[float],
    macd: Optional[dict],
    fill_asym: float,
    funding_bp: float,
) -> dict[str, int]:
    """Convert raw signals into directional votes in {-1, 0, +1}.

    +1 = bullish/trend_up vote, -1 = bearish/trend_down vote, 0 = no vote.
    """
    votes: dict[str, int] = {}

    # 1. Drift
    if drift_bp > drift_min:
        votes["drift"] = +1
    elif drift_bp < -drift_min:
        votes["drift"] = -1
    else:
        votes["drift"] = 0

    # 2. EMA crossover (sign + magnitude). Need both magnitude and direction
    # to count as a strong vote; otherwise no vote.
    if ema_div_bp >= DEFAULT_EMA_DIV_BP_STRONG:
        votes["ema"] = +1 if ema_bullish else -1
    else:
        votes["ema"] = 0

    # 3. RSI extreme = directional vote (overbought/oversold are mean-reversion
    # candidates, treated as *opposite* to the recent move).
    if rsi is None:
        votes["rsi"] = 0
    elif rsi >= 70:
        votes["rsi"] = -1  # overbought, lean bearish
    elif rsi <= 30:
        votes["rsi"] = +1  # oversold, lean bullish
    else:
        votes["rsi"] = 0

    # 4. MACD histogram sign and crossover.
    if macd and isinstance(macd, dict):
        cross = macd.get("crossover")
        hist = macd.get("histogram")
        if cross == "bullish":
            votes["macd"] = +1
        elif cross == "bearish":
            votes["macd"] = -1
        elif hist is not None and hist > 0:
            votes["macd"] = +1
        elif hist is not None and hist < 0:
            votes["macd"] = -1
        else:
            votes["macd"] = 0
    else:
        votes["macd"] = 0

    # 5. Fill asymmetry. Heavy buy-side fills means price is falling against
    # us (we're getting filled because we're the bid that nobody can beat) →
    # bearish vote. Convention chosen so this lines up with drift.
    if abs(fill_asym) >= 0.4:
        votes["fill_asym"] = -1 if fill_asym > 0 else +1
    else:
        votes["fill_asym"] = 0

    # 6. Funding sign (tie-breaker only). Positive funding => longs pay shorts
    # => slight bearish lean. We give this half weight in the agreement count.
    if funding_bp > 1.0:
        votes["funding"] = -1
    elif funding_bp < -1.0:
        votes["funding"] = +1
    else:
        votes["funding"] = 0

    return votes


def classify_regime(
    state: dict,
    history: list[float],
    *,
    product: str = "",
    variance_ratio: float = 1.0,
    realized_vol_bp: float = 0.0,
    ema_fast: float = 0.0,
    ema_slow: float = 0.0,
    short_window: int = 4,
    long_window: int = 12,
    config: Optional[dict] = None,
) -> dict[str, Any]:
    """Compute a five-state regime label from available signals.

    Returns a dict with at minimum:
      regime         : one of ALL_REGIMES
      confidence     : float in [0, 1]
      regime_changed : bool
      And all per-signal values for telemetry.

    The caller (run_cycle) already has ``variance_ratio`` and
    ``realized_vol_bp`` from ``_apply_dgrid_controls`` — pass them through.
    """
    cfg = config or {}
    trend_vr = float(cfg.get("regime_trend_vr") or DEFAULT_TREND_VR_THRESHOLD)
    range_vr = float(cfg.get("regime_range_vr") or DEFAULT_RANGE_VR_THRESHOLD)
    drift_min = float(cfg.get("regime_drift_min_bp") or DEFAULT_DRIFT_TREND_BP)
    range_expand_trend = float(
        cfg.get("regime_range_expansion_bp_trend") or DEFAULT_RANGE_EXPANSION_BP_TREND
    )
    range_tight_vol = float(
        cfg.get("regime_range_tight_vol_bp") or DEFAULT_RANGE_TIGHT_VOL_BP
    )
    min_agreement = int(cfg.get("regime_min_signal_agreement") or DEFAULT_MIN_AGREEMENT)

    # Self-sufficient input handling: if the caller passes vr <= 0 or
    # realized_vol_bp <= 0, compute them from the history we have. Lets the
    # classifier be called from contexts that don't already maintain those
    # numbers (volume_bot, ad-hoc dashboards, tests).
    if not variance_ratio or float(variance_ratio) <= 0:
        variance_ratio = _variance_ratio_from_history(history, short_window, long_window)
    if not realized_vol_bp or float(realized_vol_bp) <= 0:
        realized_vol_bp = _realized_vol_bp_from_history(history)

    drift_bp = _drift_bp(history, short_window)
    range_exp_bp = _range_expansion_bp(history, long_window)
    fill_asym = _fill_asymmetry(state)
    funding_bp = _funding_bp(state)

    # EMA divergence: prefer in-state EMAs maintained by mm_bot, fall back to
    # price_tracker if mm_bot hasn't filled them yet.
    ema_div_bp = 0.0
    ema_bullish = False
    if ema_fast > 0 and ema_slow > 0:
        ema_div_bp = abs(ema_fast - ema_slow) / max(ema_slow, 1e-9) * 1e4
        ema_bullish = ema_fast > ema_slow

    tracker = _try_price_tracker(product) if product else {}
    rsi_14 = tracker.get("rsi_14")
    macd = tracker.get("macd")

    if ema_div_bp == 0.0:
        t_fast = tracker.get("ema_9")
        t_slow = tracker.get("ema_21") or tracker.get("ema_50")
        if t_fast and t_slow and t_slow > 0:
            ema_div_bp = abs(t_fast - t_slow) / max(t_slow, 1e-9) * 1e4
            ema_bullish = t_fast > t_slow

    votes = _signal_directions(
        drift_bp=drift_bp,
        drift_min=drift_min,
        ema_div_bp=ema_div_bp,
        ema_bullish=ema_bullish,
        rsi=rsi_14,
        macd=macd,
        fill_asym=fill_asym,
        funding_bp=funding_bp,
    )

    # Aggregate
    bullish_votes = sum(1 for v in votes.values() if v > 0)
    bearish_votes = sum(1 for v in votes.values() if v < 0)
    net = bullish_votes - bearish_votes

    # --- Decide regime --------------------------------------------------
    primary_trend = variance_ratio >= trend_vr
    primary_range = variance_ratio <= range_vr
    high_range_expansion = range_exp_bp >= range_expand_trend

    regime = state.get("regime") or REGIME_RANGE_WIDE
    if regime not in ALL_REGIMES:
        regime = REGIME_RANGE_WIDE

    # Strong-drift override. A smooth multiplicative trend (steady gain/loss
    # per bar) produces near-constant returns and therefore a variance ratio
    # around 1.0 — which would otherwise wrongly classify as range. When drift
    # is much larger than the trend threshold AND at least one aligned vote
    # agrees, declare trend regardless of vr. This is the case that caught
    # us on WTI (169 bp move with low vr).
    strong_drift_min = drift_min * 3.0  # default 24 bp
    if abs(drift_bp) >= strong_drift_min:
        if drift_bp > 0 and bullish_votes >= 1 and bearish_votes == 0:
            regime = REGIME_TREND_UP
            primary_trend = True  # affects vr_aligned for confidence calc
        elif drift_bp < 0 and bearish_votes >= 1 and bullish_votes == 0:
            regime = REGIME_TREND_DOWN
            primary_trend = True
        elif drift_bp > 0:
            # No conflicting votes? Still call trend on raw drift magnitude.
            if bearish_votes == 0:
                regime = REGIME_TREND_UP
                primary_trend = True
        elif drift_bp < 0:
            if bullish_votes == 0:
                regime = REGIME_TREND_DOWN
                primary_trend = True
        if regime in (REGIME_TREND_UP, REGIME_TREND_DOWN):
            # Skip the normal decision tree — we've already decided.
            primary_range = False

    if primary_trend and regime not in (REGIME_TREND_UP, REGIME_TREND_DOWN, REGIME_CHOP_HIGH_VOL):
        # In a trend regime, prefer trend_up/down if direction agrees,
        # otherwise call it chop_high_vol.
        if net >= 2 and bullish_votes >= min_agreement:
            regime = REGIME_TREND_UP
        elif net <= -2 and bearish_votes >= min_agreement:
            regime = REGIME_TREND_DOWN
        elif high_range_expansion and abs(drift_bp) >= drift_min:
            regime = REGIME_TREND_UP if drift_bp > 0 else REGIME_TREND_DOWN
        else:
            regime = REGIME_CHOP_HIGH_VOL
    elif primary_range:
        if realized_vol_bp <= range_tight_vol:
            regime = REGIME_RANGE_TIGHT
        else:
            regime = REGIME_RANGE_WIDE
    else:
        # In hysteresis band — secondary signals can override but only if
        # min_agreement is met.
        if bullish_votes >= min_agreement and net >= 2:
            regime = REGIME_TREND_UP
        elif bearish_votes >= min_agreement and net <= -2:
            regime = REGIME_TREND_DOWN
        elif realized_vol_bp <= range_tight_vol and abs(drift_bp) < drift_min:
            regime = REGIME_RANGE_TIGHT
        # else: keep previous regime

    # Confidence: combination of vote agreement and signal availability.
    available = sum(1 for v in votes.values() if v != 0)
    agreement = abs(net) / max(1, available) if available else 0.0
    # Boost when variance_ratio and votes agree
    vr_aligned = (
        (regime in (REGIME_TREND_UP, REGIME_TREND_DOWN) and primary_trend)
        or (regime in (REGIME_RANGE_TIGHT, REGIME_RANGE_WIDE) and primary_range)
    )
    confidence = (
        0.4 * agreement
        + 0.3 * (available / max(1, len(votes)))
        + (0.3 if vr_aligned else 0.0)
    )

    # Range-expansion confidence boost: a wide intra-window range is itself
    # evidence of chop, AND amplifies trend confidence when drift is strong.
    # range_expand_trend is the threshold (default 25 bp); we treat 4x that
    # as full confidence saturation for this signal.
    if regime == REGIME_CHOP_HIGH_VOL:
        chop_evidence = min(1.0, range_exp_bp / max(1e-9, range_expand_trend * 4.0))
        confidence = max(confidence, 0.4 + 0.5 * chop_evidence)
    elif regime in (REGIME_TREND_UP, REGIME_TREND_DOWN):
        # When drift is much larger than the trend min, that alone is strong
        # evidence the regime is real even if external indicators (RSI/MACD)
        # haven't warmed up.
        drift_evidence = min(1.0, abs(drift_bp) / max(1e-9, drift_min * 6.0))
        confidence = max(confidence, 0.35 + 0.55 * drift_evidence)

    confidence = min(1.0, max(0.0, confidence))

    previous = str(state.get("regime") or "")
    regime_changed = bool(previous and previous != regime)

    state["regime"] = regime
    state["regime_confidence"] = round(confidence, 4)
    state["regime_drift_bp"] = round(drift_bp, 4)
    state["regime_range_expansion_bp"] = round(range_exp_bp, 4)
    state["regime_ema_div_bp"] = round(ema_div_bp, 4)
    state["regime_fill_asym"] = round(fill_asym, 4)
    state["regime_funding_bp"] = round(funding_bp, 4)
    state["regime_votes"] = dict(votes)
    state["regime_changed"] = regime_changed
    state["regime_last_eval_ts"] = time.time()

    if regime_changed:
        logger.info(
            "Regime change: %s -> %s (vr=%.3f drift_bp=%.1f votes=%s conf=%.2f)",
            previous or "init",
            regime,
            float(variance_ratio or 0.0),
            drift_bp,
            votes,
            confidence,
        )

    return {
        "regime": regime,
        "confidence": confidence,
        "variance_ratio": float(variance_ratio or 0.0),
        "drift_bp": drift_bp,
        "range_expansion_bp": range_exp_bp,
        "ema_div_bp": ema_div_bp,
        "ema_bullish": ema_bullish,
        "rsi_14": rsi_14,
        "macd": macd,
        "fill_asym": fill_asym,
        "funding_bp": funding_bp,
        "votes": votes,
        "regime_changed": regime_changed,
        "tracker_regime": tracker.get("tracker_regime"),
        "realized_vol_bp": realized_vol_bp,
    }
