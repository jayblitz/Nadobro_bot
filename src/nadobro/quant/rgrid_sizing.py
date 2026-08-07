"""R-Grid step sizing against the stop budget. Pure math — no I/O, no config.

Reverse Grid crosses the spread on BOTH legs by design, and it trades
``margin x leverage / levels`` per break. The session stop is a % of MARGIN
judged NET of fees, so leverage buys size but not stop budget — and past a point
the two collide:

    $100 margin, 49x, 4 levels  →  $1,225 per break
    one taker round trip         →  $1,225 x 8.6bp = $1.05
    0.8%-of-margin stop          →  $0.80

The session then stops out on the FIRST entry+exit whichever way price went. The
user never sees a losing trade, just a strategy that "keeps stopping".

So the step is capped: one round trip may consume at most ``max_fee_share`` of the
stop budget, which guarantees ``1 / max_fee_share`` round trips fit inside the
stop before costs alone close the session. The cap can only ever SHRINK the step
(it is a min), and it is skipped entirely when the user disarmed their stop —
there is no budget to size against, and inventing one would silently override
their choice.

A floor stops the cap turning into a different failure: below the venue's minimum
order notional an order simply cannot be placed. When the budget implies a step
under that floor, sizing stops at the floor and ``floored`` is set so the caller
can tell the user the configuration cannot work rather than shipping an
unplaceable size.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from src.nadobro.quant.vol_fee_estimator import (
    DEFAULT_BUILDER_FEE_RATE,
    DEFAULT_SPOT_TAKER_FEE_RATE,
)

# All-in taker rate actually charged: catalog base + the 1bp builder routing that
# policy locks on. Both legs cross, so a round trip pays it twice.
TAKER_ALL_IN_RATE = DEFAULT_SPOT_TAKER_FEE_RATE + DEFAULT_BUILDER_FEE_RATE   # 4.3 bp
TAKER_ROUND_TRIP_RATE = TAKER_ALL_IN_RATE * Decimal(2)                        # 8.6 bp

# One round trip may eat at most this share of the stop budget ⇒ at least three
# fit before fees alone close the session.
DEFAULT_MAX_FEE_SHARE = Decimal("0.33")


@dataclass(frozen=True)
class StepPlan:
    """The resolved per-break size, and why it is what it is."""
    step: Decimal                 # what to trade per break
    uncapped: Decimal             # what sizing asked for before the stop budget
    stop_budget_usd: Decimal      # SL% x margin (0 ⇒ stop disarmed, no cap applied)
    round_trip_cost: Decimal      # taker fees for one entry + exit at ``step``
    capped: bool                  # the stop budget shrank the step
    floored: bool                 # the budget wanted LESS than min_step_usd

    @property
    def round_trips_in_budget(self) -> Decimal:
        """How many entry+exit round trips fit inside the stop before costs alone
        close the session. Infinite when the stop is disarmed."""
        if self.round_trip_cost <= 0:
            return Decimal("Infinity")
        if self.stop_budget_usd <= 0:
            return Decimal("Infinity")
        return self.stop_budget_usd / self.round_trip_cost


def taker_round_trip_cost(step_quote: object) -> Decimal:
    """Fee cost of one entry + exit at this step size."""
    try:
        step = Decimal(str(step_quote or 0))
    except Exception:  # noqa: BLE001 - a bad size costs nothing to trade
        return Decimal(0)
    return max(Decimal(0), step) * TAKER_ROUND_TRIP_RATE


def max_step_for_stop_budget(
    stop_budget_usd: object, *, max_fee_share: Decimal = DEFAULT_MAX_FEE_SHARE
) -> Optional[Decimal]:
    """Largest step whose round-trip fee stays within ``max_fee_share`` of the
    stop budget. ``None`` when there is no budget to size against."""
    try:
        budget = Decimal(str(stop_budget_usd or 0))
    except Exception:  # noqa: BLE001
        return None
    if budget <= 0 or max_fee_share <= 0 or TAKER_ROUND_TRIP_RATE <= 0:
        return None
    return (budget * max_fee_share) / TAKER_ROUND_TRIP_RATE


def resolve_step_quote(
    *,
    deployed_quote: object,
    levels: int,
    chunk_quote: object = None,
    stop_budget_usd: object = 0,
    min_step_usd: object = 0,
    max_fee_share: Decimal = DEFAULT_MAX_FEE_SHARE,
) -> StepPlan:
    """Resolve R-Grid's per-break size.

    ``deployed_quote / levels`` is the base step. A participation chunk may only
    make it SMALLER. The stop budget may only make it smaller again. ``min_step_usd``
    is the venue's minimum order notional — the cap never goes below it, because an
    order that cannot be placed is a worse outcome than one that is too big.
    """
    deployed = max(Decimal(0), Decimal(str(deployed_quote or 0)))
    lv = max(1, int(levels or 1))
    step = deployed / Decimal(lv)
    if chunk_quote is not None:
        try:
            chunk = Decimal(str(chunk_quote))
            if chunk > 0:
                step = min(step, chunk)
        except Exception:  # noqa: BLE001 - an unusable chunk simply does not apply
            pass
    uncapped = step

    floor = max(Decimal(0), Decimal(str(min_step_usd or 0)))
    capped = floored = False
    budget_cap = max_step_for_stop_budget(stop_budget_usd, max_fee_share=max_fee_share)
    if budget_cap is not None and budget_cap < step:
        if budget_cap < floor:
            # The stop is too tight for ANY placeable size. Stop at the floor and
            # let the caller say so — shrinking further just yields rejected orders.
            step = min(step, floor) if floor > 0 else budget_cap
            floored = True
            capped = step < uncapped
        else:
            step = budget_cap
            capped = True

    try:
        budget = max(Decimal(0), Decimal(str(stop_budget_usd or 0)))
    except Exception:  # noqa: BLE001
        budget = Decimal(0)
    return StepPlan(
        step=step,
        uncapped=uncapped,
        stop_budget_usd=budget,
        round_trip_cost=taker_round_trip_cost(step),
        capped=capped,
        floored=floored,
    )
