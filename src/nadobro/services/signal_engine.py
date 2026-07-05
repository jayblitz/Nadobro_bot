"""Deterministic signal-fusion engine — the financial overlay's brain.

Fuses the multi-timeframe feature dicts (from ``market_features``) plus optional
funding context into ONE structured :class:`Signal` that the overlay actuator
maps onto the MM controllers. Pure and deterministic: same inputs -> same
Signal, no LLM, no I/O, no clock. The slow DMind analyst annotates
``reasons``/``risks`` and may nudge ``confidence`` separately; it never gates a
tick.

Fusion is a transparent weighted vote across timeframes and indicators, biased
toward higher timeframes (a 4h trend outweighs a 15m wobble). Every contributing
indicator is optional — a ``None`` feature (cold history) simply abstains, so a
freshly-started pair degrades to a low-confidence neutral signal instead of a
confident wrong one.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional

# Higher timeframes carry more weight in the vote.
_TF_WEIGHT: Dict[str, float] = {"15m": 1.0, "1h": 1.6, "4h": 2.2}

# RSI bounds for the mean-reversion tilt.
_RSI_OVERBOUGHT = 70.0
_RSI_OVERSOLD = 30.0
# Variance ratio above this reads as trending (bursty), below as ranging.
_VR_TREND = 1.25


@dataclass
class Signal:
    """One fused market read. All fields are bounded and safe to act on.

    ``bias``        -1..+1 directional conviction (short..long).
    ``regime``      "trend_up" | "trend_down" | "range" | "chop".
    ``entry_ok``    False -> the overlay must not ADD exposure this tick.
    ``scale``       -1..+1 reduce / hold / add on the favoured side.
    ``spread_mult`` >0 quote-spread multiplier (widen in volatility).
    ``sl_pct`` / ``tp_pct``  regime-adjusted barrier suggestions (% of margin).
    ``confidence``  0..1 how strongly the timeframes agree.
    ``reasons`` / ``risks``  human strings (deterministic here; DMind may add).
    """
    bias: float = 0.0
    regime: str = "range"
    entry_ok: bool = True
    scale: float = 0.0
    spread_mult: float = 1.0
    sl_pct: Optional[float] = None
    tp_pct: Optional[float] = None
    confidence: float = 0.0
    reasons: List[str] = field(default_factory=list)
    risks: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, object]:
        return {
            "bias": self.bias,
            "regime": self.regime,
            "entry_ok": self.entry_ok,
            "scale": self.scale,
            "spread_mult": self.spread_mult,
            "sl_pct": self.sl_pct,
            "tp_pct": self.tp_pct,
            "confidence": self.confidence,
            "reasons": list(self.reasons),
            "risks": list(self.risks),
        }


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _f(value: object) -> Optional[float]:
    try:
        return None if value is None else float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _tf_directional_score(feat: Mapping[str, object]) -> Optional[float]:
    """Directional score in [-1, +1] for one timeframe, averaging the
    indicators that have data. ``None`` when the timeframe has no usable read."""
    parts: List[float] = []

    trend = str(feat.get("trend") or "flat")
    if trend == "up":
        parts.append(1.0)
    elif trend == "down":
        parts.append(-1.0)

    hist = _f(feat.get("macd_hist"))
    if hist is not None:
        # ``macd_hist`` arrives price-normalized (histogram / close), so it is
        # comparable across products. ~0.33% of price maps to a full vote.
        parts.append(_clamp(hist * 300.0, -1.0, 1.0) if hist != 0 else 0.0)

    rsi = _f(feat.get("rsi"))
    if rsi is not None:
        # Momentum read: >50 bullish, <50 bearish, centred and scaled.
        parts.append(_clamp((rsi - 50.0) / 30.0, -1.0, 1.0))

    drift = _f(feat.get("drift"))
    if drift is not None:
        parts.append(_clamp(drift * 100.0, -1.0, 1.0))

    if not parts:
        return None
    return _clamp(sum(parts) / len(parts), -1.0, 1.0)


def _regime_of(features_by_tf: Mapping[str, Mapping[str, object]], bias: float) -> str:
    """Classify the dominant regime from the highest timeframe that has a
    variance ratio, falling back to the bias sign."""
    for tf in ("4h", "1h", "15m"):
        feat = features_by_tf.get(tf)
        if not feat:
            continue
        vr = _f(feat.get("variance_ratio"))
        if vr is None:
            continue
        if vr >= _VR_TREND:
            if bias > 0.15:
                return "trend_up"
            if bias < -0.15:
                return "trend_down"
            return "chop"
        return "range"
    if bias > 0.15:
        return "trend_up"
    if bias < -0.15:
        return "trend_down"
    return "range"


def build_signal(
    features_by_tf: Mapping[str, Mapping[str, object]],
    *,
    funding_rate: Optional[float] = None,
    position_side: Optional[str] = None,
    base_sl_pct: float = 0.5,
    base_tp_pct: float = 1.0,
) -> Signal:
    """Fuse per-timeframe features into a :class:`Signal`.

    ``funding_rate`` is the signed daily perp funding (positive => longs pay).
    ``position_side`` ("long"/"short"/None) lets the engine flag when funding is
    hostile to the CURRENT exposure. Barriers scale off ``base_*`` by regime.
    """
    reasons: List[str] = []
    risks: List[str] = []

    # Weighted directional vote across timeframes.
    weighted_sum = 0.0
    weight_total = 0.0
    per_tf_scores: Dict[str, float] = {}
    for tf, feat in features_by_tf.items():
        score = _tf_directional_score(feat)
        if score is None:
            continue
        w = _TF_WEIGHT.get(str(tf), 1.0)
        weighted_sum += score * w
        weight_total += w
        per_tf_scores[str(tf)] = score

    if weight_total <= 0:
        return Signal(
            bias=0.0, regime="range", entry_ok=False, scale=0.0,
            confidence=0.0,
            reasons=["Not enough candle history yet — neutral until data warms up."],
        )

    bias = _clamp(weighted_sum / weight_total, -1.0, 1.0)

    # Agreement: how aligned are the timeframes in sign? Full agreement -> high
    # confidence; conflicting timeframes -> low.
    signs = [1 if s > 0.05 else (-1 if s < -0.05 else 0) for s in per_tf_scores.values()]
    nonzero = [s for s in signs if s != 0]
    if nonzero:
        agree = abs(sum(nonzero)) / len(nonzero)
    else:
        agree = 0.0
    confidence = _clamp(abs(bias) * 0.5 + agree * 0.5, 0.0, 1.0)

    regime = _regime_of(features_by_tf, bias)

    # Spread multiplier from the fastest timeframe's ATR% (widen when volatile).
    atr_pct = None
    for tf in ("15m", "1h", "4h"):
        a = _f((features_by_tf.get(tf) or {}).get("atr_pct"))
        if a is not None:
            atr_pct = a
            break
    if atr_pct is not None:
        spread_mult = _clamp(1.0 + atr_pct * 30.0, 0.75, 3.0)
    else:
        spread_mult = 1.0

    # Scale: add on the favoured side only when trend + agreement are strong;
    # reduce when the higher-timeframe trend opposes the position/ bias.
    scale = 0.0
    if regime in ("trend_up", "trend_down") and confidence >= 0.5:
        scale = _clamp(bias * confidence, -1.0, 1.0)
        reasons.append(f"{regime.replace('_', ' ')} with {confidence:.0%} timeframe agreement.")
    elif regime == "range":
        reasons.append("Ranging — mean-reversion favoured, no directional add.")
    elif regime == "chop":
        risks.append("Choppy / conflicting timeframes — sizing held back.")

    # RSI extremes: fade into the band (reduce adds against an overbought/oversold
    # read), and flag the risk.
    rsi_fast = _f((features_by_tf.get("15m") or {}).get("rsi"))
    if rsi_fast is not None:
        if rsi_fast >= _RSI_OVERBOUGHT and scale > 0:
            scale *= 0.4
            risks.append(f"15m RSI {rsi_fast:.0f} overbought — trimming long add.")
        elif rsi_fast <= _RSI_OVERSOLD and scale < 0:
            scale *= 0.4
            risks.append(f"15m RSI {rsi_fast:.0f} oversold — trimming short add.")

    # entry_ok: block NEW exposure in chop, or when the top timeframe trend
    # opposes the bias hard (a breakout against inventory).
    entry_ok = True
    if regime == "chop":
        entry_ok = False
    top = features_by_tf.get("4h") or features_by_tf.get("1h") or {}
    top_trend = str(top.get("trend") or "flat")
    if (top_trend == "down" and bias > 0.3) or (top_trend == "up" and bias < -0.3):
        entry_ok = False
        risks.append("Higher-timeframe trend opposes the short-term bias — entries paused.")

    # Funding context: flag when funding is hostile to the current position.
    if funding_rate is not None and position_side in ("long", "short"):
        fr = float(funding_rate)
        if position_side == "long" and fr > 0.0005:
            risks.append(f"Funding {fr * 100:+.3f}%/day — longs are paying; carry is a cost.")
        elif position_side == "short" and fr < -0.0005:
            risks.append(f"Funding {fr * 100:+.3f}%/day — shorts are paying; carry is a cost.")

    # Barriers scale by regime: wider in a trend (let winners run), tighter in a
    # range (book the oscillation).
    if regime in ("trend_up", "trend_down"):
        sl_pct = base_sl_pct * 1.3
        tp_pct = base_tp_pct * 1.6
    elif regime == "chop":
        sl_pct = base_sl_pct * 0.8
        tp_pct = base_tp_pct * 0.8
    else:  # range
        sl_pct = base_sl_pct
        tp_pct = base_tp_pct

    if not reasons:
        reasons.append("Neutral read — holding current posture.")

    return Signal(
        bias=round(bias, 4),
        regime=regime,
        entry_ok=entry_ok,
        scale=round(scale, 4),
        spread_mult=round(spread_mult, 4),
        sl_pct=round(sl_pct, 4),
        tp_pct=round(tp_pct, 4),
        confidence=round(confidence, 4),
        reasons=reasons,
        risks=risks,
    )
