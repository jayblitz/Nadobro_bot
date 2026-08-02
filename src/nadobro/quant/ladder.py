"""Quote-ladder planning — pure math, no venue access.

Turns "I have D dollars to deploy on this side" into a list of
``(offset_bp, size_quote)`` levels. Used by Mid (reference = book mid) and
fill-anchored Grid (reference = last fill); only the reference differs, the
laddering is identical.

Why this exists
===============
Both modes shipped ONE order per side and then waited. Mid did not even read
``levels`` — the mapping said outright that a single bid/ask carries the full
notional — so the user's setting was dead. Nothing scaled into a move or out
of one.

The level count is bounded by the venue minimum, which is the constraint that
killed the original attempt: dividing a small notional across levels silently
produced sub-minimum orders the venue rejects. Here the clamp is explicit and
the caller is told what it got.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import List, Sequence

FLAT = "flat"
LINEAR = "linear"
GEOMETRIC = "geometric"
CURVES = (FLAT, LINEAR, GEOMETRIC)

# Geometric growth factor per level. At 2.0 the weights are 1:2:4:8, giving a
# deepest/nearest size ratio of 8 against linear's 4 — a curve that is
# genuinely more back-loaded. A ratio of 1.6 was tried first and produced a
# ratio of 4.096 vs linear's 4.0, i.e. an option that did nothing.
_GEOMETRIC_RATIO = Decimal("2.0")

# "No venue minimum reported" — expressed as a large bound rather than a
# sentinel so callers keep using plain min() against their requested count.
NO_FLOOR_LEVEL_BOUND = 1000


def _dec(value: object, default: str = "0") -> Decimal:
    try:
        if value is None:
            return Decimal(default)
        return Decimal(str(value))
    except Exception:  # noqa: BLE001  # policy: degrade-ok(malformed input -> default)
        return Decimal(default)


@dataclass(frozen=True)
class LadderLevel:
    index: int
    offset_bp: Decimal      # distance from the reference, always >= 0
    size_quote: Decimal     # notional for this level


def max_levels(deployed_quote: object, min_notional: object) -> int:
    """How many levels this side can carry with every level above the venue floor.

    Always >= 1: a deployment smaller than the minimum still gets ONE order and
    lets the venue be the arbiter, rather than silently placing nothing.

    A floor of zero means we have no minimum to respect, so it cannot constrain
    the count — returning 1 there would collapse every ladder on any venue or
    adapter that reports no minimum, which is the opposite of the intent.
    """
    deployed = _dec(deployed_quote)
    floor = _dec(min_notional)
    if deployed <= 0:
        return 0
    if floor <= 0:
        return NO_FLOOR_LEVEL_BOUND
    return max(1, int((deployed / floor).to_integral_value(rounding=ROUND_DOWN)))


def _weights(n: int, curve: str) -> List[Decimal]:
    """Relative size per level, index 0 = nearest the reference."""
    if n <= 1:
        return [Decimal(1)]
    c = str(curve or FLAT).strip().lower()
    if c == LINEAR:
        return [Decimal(i + 1) for i in range(n)]
    if c == GEOMETRIC:
        return [_GEOMETRIC_RATIO ** i for i in range(n)]
    return [Decimal(1)] * n


def plan_ladder(
    deployed_quote: object,
    *,
    levels: object = 1,
    step_bp: object = 0,
    first_offset_bp: object = 0,
    curve: str = FLAT,
    min_notional: object = 0,
) -> List[LadderLevel]:
    """Plan one side of the ladder.

    ``levels`` is a REQUEST; the returned list may be shorter because every
    level must clear ``min_notional``. Sizes always sum to exactly
    ``deployed_quote`` — the rounding remainder rides the LAST (deepest) level,
    so the near touch level is never inflated above plan.

    Deeper levels are further from the reference (``offset_bp`` increasing),
    which is what makes the ladder scale INTO an adverse move and out of a
    favourable one.
    """
    deployed = _dec(deployed_quote)
    if deployed <= 0:
        return []
    want = int(_dec(levels, "1") or 1)
    n = max(1, min(want, max_levels(deployed, min_notional)))

    w = _weights(n, curve)
    total_w = sum(w, Decimal(0))
    first = _dec(first_offset_bp)
    step = _dec(step_bp)

    out: List[LadderLevel] = []
    assigned = Decimal(0)
    for i in range(n):
        if i < n - 1:
            size = (deployed * w[i] / total_w).quantize(
                Decimal("0.00000001"), rounding=ROUND_DOWN
            )
        else:
            size = deployed - assigned      # exact: remainder on the deepest level
        assigned += size
        out.append(LadderLevel(
            index=i,
            offset_bp=first + step * Decimal(i),
            size_quote=size,
        ))
    return out


def ladder_notional(levels: Sequence[LadderLevel]) -> Decimal:
    return sum((lv.size_quote for lv in levels), Decimal(0))


def describe(levels: Sequence[LadderLevel]) -> str:
    """One-line human summary for logs and the strategy card."""
    if not levels:
        return "no levels"
    return " | ".join(
        f"L{lv.index}@{float(lv.offset_bp):.1f}bp ${float(lv.size_quote):,.0f}"
        for lv in levels
    )
