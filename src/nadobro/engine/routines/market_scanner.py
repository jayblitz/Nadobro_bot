"""Market-scanner routine — ranks candidate pairs by a liquidity / volume /
volatility composite to bound a strategy universe. Deterministic given the
candidate metrics (the live universe is supplied by the caller / adapter).

Implemented in Phase 4.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence


def _f(x: object) -> float:
    return float(x)  # type: ignore[arg-type]


def _norm(values: List[float]) -> List[float]:
    lo, hi = min(values), max(values)
    if hi <= lo:
        return [0.0 for _ in values]
    return [(v - lo) / (hi - lo) for v in values]


async def run(
    candidates: Sequence[Dict[str, object]],
    *,
    top_n: int = 10,
    weights: Optional[Dict[str, float]] = None,
) -> List[Dict[str, object]]:
    """``candidates``: dicts with ``pair`` + numeric ``liquidity`` / ``volume``
    / ``volatility``. Returns the top-N by weighted composite score."""
    if not candidates:
        return []
    w = {"liquidity": 0.4, "volume": 0.4, "volatility": 0.2, **(weights or {})}
    metrics = ("liquidity", "volume", "volatility")
    cols = {m: _norm([_f(c.get(m, 0.0)) for c in candidates]) for m in metrics}
    ranked: List[Dict[str, object]] = []
    for i, c in enumerate(candidates):
        score = sum(cols[m][i] * w[m] for m in metrics)
        top_metric = max(metrics, key=lambda m: cols[m][i])
        ranked.append({"pair": c.get("pair"), "score": score, "reason": f"high {top_metric}"})
    ranked.sort(key=lambda r: _f(r["score"]), reverse=True)
    return ranked[:top_n]
