"""TWAP scheduling math for the spot Volume Bot — pure, no venue access.

Separates the SCHEDULE (how much should be done by now, in how many pieces)
from the PLACEMENT (where to rest, when to chase, when to cross). Placement
lives in the controller; everything here is a pure function so the pacing can
be unit-tested against a clock with no venue and no mocks.

Why this is small
=================
On Nado spot the venue minimum notional is $100 and the product's per-cycle
band is $100-$500, so a $100 clip **cannot be sliced at all** and a $500 clip
supports at most 5 pieces. Real TWAP slicing therefore only exists at the top
of the band; at $100 the schedule correctly degenerates to a single slice.

That is fine, because the measured prize is not the slicing. On KBTC spot
(2026-07-31) impact at $100-$5,000 is 0.00 bp, while maker vs taker is
1.8 bp vs 4.3 bp against a 5.7 bp spread — a taker round trip costs 14.3 bp
and a maker round trip 3.6 bp. Resting instead of crossing is worth ~4x the
volume per unit of the session loss budget; the pacing just decides how long
we are willing to wait for that fill before conceding and crossing.
"""
from __future__ import annotations

from decimal import ROUND_DOWN, Decimal
from typing import List

# Never emit a slice this close to the venue floor — a slice that rounds under
# the minimum is rejected, which silently stalls the parent.
_MIN_SLICE_SAFETY = Decimal("1.02")


def _dec(value: object, default: str = "0") -> Decimal:
    try:
        if value is None:
            return Decimal(default)
        return Decimal(str(value))
    except Exception:  # noqa: BLE001  # policy: degrade-ok(malformed input -> default)
        return Decimal(default)


def max_slices(total_quote: object, min_notional: object) -> int:
    """How many pieces this parent can be cut into and still clear the venue
    minimum. Always >= 1: a parent smaller than the minimum is still attempted
    as ONE order (the venue, not us, is the arbiter of whether it is placeable).
    """
    total = _dec(total_quote)
    floor = _dec(min_notional)
    if total <= 0:
        return 0
    if floor <= 0:
        return 1
    n = int((total / (floor * _MIN_SLICE_SAFETY)).to_integral_value(rounding=ROUND_DOWN))
    return max(1, n)


def plan_slices(
    total_quote: object,
    *,
    min_notional: object,
    requested_slices: object = 4,
) -> List[Decimal]:
    """Split a parent into equal slices that each clear the venue minimum.

    Returns ``[total]`` (a single slice) whenever the parent cannot support two
    placeable pieces — the honest outcome at the bottom of the $100-$500 band,
    not an error. The slices always sum to exactly ``total_quote``: any
    remainder from the division is added to the LAST slice, so a parent is
    never left partially unscheduled.
    """
    total = _dec(total_quote)
    if total <= 0:
        return []
    want = int(_dec(requested_slices, "1") or 1)
    n = min(max(1, want), max_slices(total, min_notional))
    if n <= 1:
        return [total]
    each = (total / Decimal(n)).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    slices = [each] * (n - 1)
    slices.append(total - sum(slices))   # exact: remainder rides the last slice
    return slices


def target_done_quote(
    total_quote: object,
    *,
    elapsed_seconds: object,
    horizon_seconds: object,
) -> Decimal:
    """How much of the parent a linear TWAP says should be done by now.

    Clamped to [0, total]. A non-positive horizon means "no pacing" and returns
    the full parent immediately, so a misconfigured horizon can never stall a
    leg — it degrades to "execute now", which is the safe direction for a bot
    whose product is turnover.
    """
    total = _dec(total_quote)
    if total <= 0:
        return Decimal(0)
    horizon = _dec(horizon_seconds)
    if horizon <= 0:
        return total
    elapsed = _dec(elapsed_seconds)
    if elapsed <= 0:
        return Decimal(0)
    if elapsed >= horizon:
        return total
    return total * (elapsed / horizon)


def schedule_debt_quote(
    total_quote: object,
    *,
    filled_quote: object,
    elapsed_seconds: object,
    horizon_seconds: object,
) -> Decimal:
    """Quote notional we are BEHIND the linear schedule (0 when on/ahead).

    This is the single number the placement ladder escalates on: while it is
    zero the leg can keep resting as a maker; once it grows past the caller's
    tolerance the leg has to stop being patient.
    """
    target = target_done_quote(
        total_quote, elapsed_seconds=elapsed_seconds, horizon_seconds=horizon_seconds
    )
    done = _dec(filled_quote)
    debt = target - done
    return debt if debt > 0 else Decimal(0)


def should_cross(
    total_quote: object,
    *,
    filled_quote: object,
    elapsed_seconds: object,
    horizon_seconds: object,
    tolerance_frac: object = "0.5",
    crossed_quote: object = 0,
    max_taker_frac: object = "0.25",
) -> bool:
    """Has the leg fallen far enough behind to justify paying the taker fee?

    Two independent brakes, both required to be satisfied:

    1. ``debt > tolerance_frac x total`` — we are materially behind, not just
       a slice's worth of jitter.
    2. ``crossed_quote < max_taker_frac x total`` — we have not already spent
       the parent's taker budget. Past that we deliberately stay behind
       schedule rather than silently revert to the 14.3 bp cost curve that
       maker execution exists to avoid.
    """
    total = _dec(total_quote)
    if total <= 0:
        return False
    debt = schedule_debt_quote(
        total,
        filled_quote=filled_quote,
        elapsed_seconds=elapsed_seconds,
        horizon_seconds=horizon_seconds,
    )
    if debt <= total * _dec(tolerance_frac, "0.5"):
        return False
    budget = total * _dec(max_taker_frac, "0.25")
    return _dec(crossed_quote) < budget
