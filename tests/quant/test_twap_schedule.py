"""TWAP scheduling math — pure, no venue.

The schedule decides how much should be done by now and when patience has to
end. It never places an order; the placement ladder does. Keeping it pure is
what makes the pacing testable against a clock instead of a mock venue.
"""
from decimal import Decimal

from src.nadobro.quant.twap_schedule import (
    max_slices,
    plan_slices,
    schedule_debt_quote,
    should_cross,
    target_done_quote,
)


# --------------------------------------------------------------------------
# Slicing under the venue minimum — the constraint that shapes this product
# --------------------------------------------------------------------------
def test_a_100_dollar_clip_cannot_be_sliced():
    """KBTC spot min notional is $100 and the product band starts at $100, so
    the bottom of the band is a SINGLE order. That is the honest outcome, not
    an error — TWAP there is just 'rest instead of cross'."""
    assert max_slices(100, 100) == 1
    assert plan_slices(100, min_notional=100, requested_slices=4) == [Decimal(100)]


def test_slicing_appears_only_higher_in_the_band():
    assert max_slices(500, 100) == 4      # 1.02 safety margin over the floor
    sl = plan_slices(500, min_notional=100, requested_slices=4)
    assert len(sl) == 4
    assert all(s >= Decimal(100) for s in sl), "every slice must clear the venue floor"


def test_slices_sum_exactly_to_the_parent():
    """A rounding remainder must never leave part of the parent unscheduled."""
    for total in (100, 250, 333.33, 500):
        for want in (1, 2, 3, 4, 5):
            sl = plan_slices(total, min_notional=100, requested_slices=want)
            assert sum(sl) == Decimal(str(total)), (total, want, sl)


def test_requested_slices_are_capped_never_inflated():
    sl = plan_slices(250, min_notional=100, requested_slices=10)
    assert len(sl) == max_slices(250, 100) == 2
    assert sum(sl) == Decimal(250)


def test_degenerate_inputs_are_safe():
    assert plan_slices(0, min_notional=100) == []
    assert plan_slices(-5, min_notional=100) == []
    assert max_slices(100, 0) == 1          # unknown floor -> one order
    assert plan_slices(100, min_notional=0) == [Decimal(100)]


# --------------------------------------------------------------------------
# Linear pacing
# --------------------------------------------------------------------------
def test_target_is_linear_in_time():
    assert target_done_quote(100, elapsed_seconds=0, horizon_seconds=100) == Decimal(0)
    assert target_done_quote(100, elapsed_seconds=50, horizon_seconds=100) == Decimal(50)
    assert target_done_quote(100, elapsed_seconds=100, horizon_seconds=100) == Decimal(100)


def test_target_clamps_past_the_horizon():
    assert target_done_quote(100, elapsed_seconds=999, horizon_seconds=100) == Decimal(100)


def test_no_horizon_means_execute_now_not_stall():
    """A misconfigured horizon must degrade to 'do it now'. For a bot whose
    product is turnover, stalling is the dangerous direction."""
    assert target_done_quote(100, elapsed_seconds=0, horizon_seconds=0) == Decimal(100)
    assert target_done_quote(100, elapsed_seconds=0, horizon_seconds=-5) == Decimal(100)


def test_debt_is_zero_when_on_or_ahead_of_schedule():
    # Half the horizon gone, half done => on schedule.
    assert schedule_debt_quote(
        100, filled_quote=50, elapsed_seconds=50, horizon_seconds=100
    ) == Decimal(0)
    # Ahead of schedule is still zero debt, never negative.
    assert schedule_debt_quote(
        100, filled_quote=90, elapsed_seconds=50, horizon_seconds=100
    ) == Decimal(0)


def test_debt_measures_how_far_behind_we_are():
    assert schedule_debt_quote(
        100, filled_quote=10, elapsed_seconds=50, horizon_seconds=100
    ) == Decimal(40)


# --------------------------------------------------------------------------
# When patience ends
# --------------------------------------------------------------------------
def test_does_not_cross_while_on_schedule():
    assert not should_cross(
        100, filled_quote=50, elapsed_seconds=50, horizon_seconds=100,
    )


def test_does_not_cross_for_mere_jitter():
    """Being slightly behind must not trigger the 4x-cost taker path."""
    assert not should_cross(
        100, filled_quote=40, elapsed_seconds=50, horizon_seconds=100,
        tolerance_frac="0.5",
    )


def test_crosses_when_materially_behind():
    assert should_cross(
        100, filled_quote=0, elapsed_seconds=90, horizon_seconds=100,
        tolerance_frac="0.5",
    )


def test_taker_budget_is_a_hard_brake():
    """Once the parent's taker budget is spent we deliberately run LATE rather
    than silently revert to the taker cost curve maker execution exists to
    avoid."""
    common = dict(
        filled_quote=0, elapsed_seconds=95, horizon_seconds=100, tolerance_frac="0.5",
    )
    assert should_cross(100, crossed_quote=0, max_taker_frac="0.25", **common)
    assert should_cross(100, crossed_quote=24, max_taker_frac="0.25", **common)
    assert not should_cross(100, crossed_quote=25, max_taker_frac="0.25", **common)
    assert not should_cross(100, crossed_quote=99, max_taker_frac="0.25", **common)


def test_zero_parent_never_crosses():
    assert not should_cross(0, filled_quote=0, elapsed_seconds=99, horizon_seconds=100)
