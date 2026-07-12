"""Slow DMind analyst rail — narrative + recommendations grounded in the
financial overlay's signal history and the user's real trading activity.

This is the LLM tier of the overlay: it never touches the trade loop. Night HOWL
(daily, per user) calls :func:`analyze_activity`, which asks the finance model
(DMind via NanoGPT, ``dmind_service.analyze_financial_context``) to explain what
the overlay saw and did and to recommend adjustments. When the finance LLM is
not configured or fails, it falls back to the deterministic heuristics so the
report is never empty or blocked.

``summarize_overlay_signals`` is pure and testable; the LLM call is isolated so
a provider outage degrades gracefully.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import Any, Dict, List, Mapping, Optional, Sequence

logger = logging.getLogger(__name__)


def _f(value: object, default: float = 0.0) -> float:
    try:
        return default if value is None else float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def summarize_overlay_signals(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """Reduce overlay_signals rows to a compact summary: regime distribution,
    average bias/confidence, and how often the overlay suppressed or scaled.
    Pure — pass the DB rows, get a dict back."""
    n = len(rows or [])
    if n == 0:
        return {
            "signals": 0, "dominant_regime": None, "regime_counts": {},
            "avg_bias": 0.0, "avg_confidence": 0.0,
            "suppressed": 0, "scaled_up": 0, "scaled_down": 0,
        }
    regime_counts: Counter = Counter()
    bias_sum = conf_sum = 0.0
    suppressed = scaled_up = scaled_down = 0
    for r in rows:
        regime_counts[str(r.get("regime") or "unknown")] += 1
        bias_sum += _f(r.get("bias"))
        conf_sum += _f(r.get("confidence"))
        action = r.get("action_json") or {}
        if isinstance(action, dict):
            if action.get("suppress_new_entries"):
                suppressed += 1
        sc = _f(r.get("scale"))
        if sc > 0.05:
            scaled_up += 1
        elif sc < -0.05:
            scaled_down += 1
    dominant = regime_counts.most_common(1)[0][0] if regime_counts else None
    return {
        "signals": n,
        "dominant_regime": dominant,
        "regime_counts": dict(regime_counts),
        "avg_bias": round(bias_sum / n, 4),
        "avg_confidence": round(conf_sum / n, 4),
        "suppressed": suppressed,
        "scaled_up": scaled_up,
        "scaled_down": scaled_down,
    }


def _fallback_recommendations(
    pattern: Mapping[str, Any],
    signal_summary: Mapping[str, Any],
    backtests: Optional[List[Dict[str, Any]]],
) -> List[str]:
    """Deterministic recommendations (the existing heuristics) plus a one-line
    overlay note, used when the finance LLM is unavailable."""
    from src.nadobro.llm.night_howl_service import derive_recommendations

    recs = list(derive_recommendations(pattern, backtests))
    dom = signal_summary.get("dominant_regime")
    if signal_summary.get("signals", 0) > 0 and dom:
        recs.append(
            f"Overlay read the market as mostly {str(dom).replace('_', ' ')} over the "
            f"session (avg confidence {float(signal_summary.get('avg_confidence') or 0) * 100:.0f}%); "
            f"it suppressed new adds {int(signal_summary.get('suppressed') or 0)}x."
        )
    return recs


def analyze_activity(
    pattern: Mapping[str, Any],
    signal_summary: Mapping[str, Any],
    *,
    backtests: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Produce recommendations + narrative grounded in the user's activity and
    the overlay's signal history. Uses the finance LLM when configured; always
    falls back to deterministic heuristics so the report is never blocked.

    Returns ``{recommendations: [str], narrative: str, risks: [str],
    provider: str, degraded: bool}``.
    """
    fallback = _fallback_recommendations(pattern, signal_summary, backtests)
    try:
        from src.nadobro.llm.dmind_service import (
            analyze_financial_context,
            is_finance_expert_configured,
        )
    except Exception:  # noqa: BLE001
        return {"recommendations": fallback, "narrative": "", "risks": [],
                "provider": "none", "degraded": True}

    if not is_finance_expert_configured():
        return {"recommendations": fallback, "narrative": "", "risks": [],
                "provider": "none", "degraded": True}

    import json as _json

    context = _json.dumps({
        "activity_24h": {
            "trades": pattern.get("trades"),
            "volume_usd": pattern.get("volume_usd"),
            "net_pnl_usd": pattern.get("net_pnl_usd"),
            "realized_pnl_usd": pattern.get("realized_pnl_usd"),
            "fees_usd": pattern.get("fees_usd"),
            "win_rate": pattern.get("win_rate"),
            "top_pairs": pattern.get("top_pairs"),
        },
        "overlay_signals": dict(signal_summary),
        "backtests": backtests or [],
    }, ensure_ascii=True)

    prompt = (
        "You are reviewing one user's last 24h of perps trading on Nado plus the "
        "financial overlay's signal history. Give at most 4 concrete, cautious, "
        "plain-English recommendations to improve net-of-fees results, and list "
        "key risks. Ground every point ONLY in the provided data. Reply as JSON: "
        '{"recommendations": ["..."], "risks": ["..."], "narrative": "one short paragraph"}.'
    )
    schema_hint = {
        "recommendations": ["string"], "risks": ["string"], "narrative": "string",
    }
    try:
        result = analyze_financial_context(
            prompt, context=context, task="night_howl_analysis", schema_hint=schema_hint,
        )
    except Exception:  # noqa: BLE001 - never let the analyst break the report
        logger.debug("signal analyst LLM call failed", exc_info=True)
        return {"recommendations": fallback, "narrative": "", "risks": [],
                "provider": "none", "degraded": True}

    if not result.get("ok") or not result.get("text"):
        return {"recommendations": fallback, "narrative": "", "risks": [],
                "provider": str(result.get("provider") or "none"), "degraded": True}

    from src.nadobro.llm.nanogpt_client import extract_json_object

    parsed = extract_json_object(str(result.get("text") or "")) or {}
    recs = [str(r).strip() for r in (parsed.get("recommendations") or []) if str(r).strip()]
    risks = [str(r).strip() for r in (parsed.get("risks") or []) if str(r).strip()]
    narrative = str(parsed.get("narrative") or "").strip()
    if not recs:
        # LLM answered but not in the expected shape — keep the deterministic list.
        recs = fallback
    return {
        "recommendations": recs[:4],
        "narrative": narrative,
        "risks": risks[:4],
        "provider": str(result.get("provider") or "finance"),
        "degraded": False,
    }
