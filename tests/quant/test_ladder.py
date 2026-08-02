"""Quote-ladder planning — pure math.

The constraint that killed the first attempt at this feature was the venue
minimum: dividing a small notional across levels silently produces
sub-minimum orders the venue rejects, which is why Mid's ``levels`` input
ended up dead. These tests pin the clamp and the exact-sum property.
"""
from decimal import Decimal

from src.nadobro.quant.ladder import (
    FLAT, GEOMETRIC, LINEAR,
    LadderLevel, ladder_notional, max_levels, plan_ladder,
)


# --------------------------------------------------------------------------
# The venue-minimum clamp
# --------------------------------------------------------------------------
def test_level_count_is_bounded_by_the_venue_minimum():
    # BTC-PERP min notional is $100.
    assert max_levels(1000, 100) == 10
    assert max_levels(500, 100) == 5
    assert max_levels(250, 100) == 2
    assert max_levels(100, 100) == 1


def test_a_deployment_below_the_minimum_still_gets_one_order():
    """Placing nothing is worse than letting the venue reject one order."""
    assert max_levels(50, 100) == 1
    lad = plan_ladder(50, levels=4, min_notional=100)
    assert len(lad) == 1
    assert lad[0].size_quote == Decimal(50)


def test_requested_levels_are_clamped_never_inflated():
    lad = plan_ladder(250, levels=10, step_bp=1, min_notional=100)
    assert len(lad) == 2, "250/100 = 2 levels max"
    assert all(lv.size_quote >= Decimal(100) for lv in lad)


def test_one_times_leverage_cannot_ladder():
    """$100 margin at 1x deploys $100 = exactly one level. The UI must say so
    rather than silently shrinking sizes under the floor."""
    assert max_levels(100, 100) == 1
    assert len(plan_ladder(100, levels=5, min_notional=100)) == 1


# --------------------------------------------------------------------------
# Sizing
# --------------------------------------------------------------------------
def test_sizes_sum_exactly_to_the_deployment():
    for curve in (FLAT, LINEAR, GEOMETRIC):
        for dep in (1000, 500, 333.33):
            lad = plan_ladder(dep, levels=4, step_bp=2, curve=curve, min_notional=1)
            assert ladder_notional(lad) == Decimal(str(dep)), (curve, dep)


def test_remainder_rides_the_deepest_level_not_the_touch():
    """The near-touch level must never exceed plan — it is the one that fills
    most often, so an inflated L0 would overshoot the intended exposure."""
    lad = plan_ladder(100, levels=3, curve=FLAT, min_notional=1)
    assert lad[0].size_quote <= lad[-1].size_quote
    assert ladder_notional(lad) == Decimal(100)


def test_flat_curve_is_equal_size():
    lad = plan_ladder(400, levels=4, curve=FLAT, min_notional=1)
    assert [lv.size_quote for lv in lad] == [Decimal(100)] * 4


def test_linear_curve_scales_into_depth():
    lad = plan_ladder(1000, levels=4, curve=LINEAR, min_notional=1)
    sizes = [lv.size_quote for lv in lad]
    assert sizes == sorted(sizes), "deeper levels must be larger"
    assert sizes[-1] > sizes[0] * 3, "linear 1:2:3:4 weighting"


def test_geometric_curve_scales_harder():
    """The meaningful invariant is the deepest/nearest RATIO, not absolute
    sizes: geometric must concentrate more of the deployment at depth than
    linear, or the option is decoration."""
    lin = plan_ladder(1000, levels=4, curve=LINEAR, min_notional=1)
    geo = plan_ladder(1000, levels=4, curve=GEOMETRIC, min_notional=1)
    lin_ratio = lin[-1].size_quote / lin[0].size_quote
    geo_ratio = geo[-1].size_quote / geo[0].size_quote
    assert geo_ratio > lin_ratio * Decimal("1.5"), (
        f"geometric ({geo_ratio:.2f}x) must be meaningfully more back-loaded "
        f"than linear ({lin_ratio:.2f}x)"
    )
    assert geo[-1].size_quote > lin[-1].size_quote
    assert geo[0].size_quote < lin[0].size_quote


def test_unknown_curve_falls_back_to_flat():
    lad = plan_ladder(400, levels=4, curve="nonsense", min_notional=1)
    assert [lv.size_quote for lv in lad] == [Decimal(100)] * 4


# --------------------------------------------------------------------------
# Geometry
# --------------------------------------------------------------------------
def test_offsets_step_away_from_the_reference():
    lad = plan_ladder(1000, levels=4, step_bp=3, first_offset_bp=2, min_notional=1)
    assert [lv.offset_bp for lv in lad] == [Decimal(2), Decimal(5), Decimal(8), Decimal(11)]


def test_offsets_are_never_negative():
    """A negative offset would put a bid ABOVE the reference — crossing."""
    lad = plan_ladder(1000, levels=3, step_bp=2, first_offset_bp=0, min_notional=1)
    assert all(lv.offset_bp >= 0 for lv in lad)


def test_single_level_reproduces_todays_behaviour():
    """levels=1 must be byte-identical to the shipped single-quote behaviour,
    so the ladder is opt-in and cannot regress existing users."""
    lad = plan_ladder(1000, levels=1, step_bp=5, first_offset_bp=7, min_notional=100)
    assert len(lad) == 1
    assert lad[0].size_quote == Decimal(1000)
    assert lad[0].offset_bp == Decimal(7)


def test_degenerate_inputs_are_safe():
    assert plan_ladder(0, levels=4, min_notional=100) == []
    assert plan_ladder(-10, levels=4, min_notional=100) == []
    assert len(plan_ladder(1000, levels=0, min_notional=100)) == 1
    assert len(plan_ladder(1000, levels=-3, min_notional=100)) == 1


def test_no_reported_minimum_does_not_collapse_the_ladder():
    """A venue/adapter that reports no minimum cannot constrain the count.
    Returning 1 here would silently collapse every ladder to a single order —
    the same silent-failure shape that killed the ``levels`` input before."""
    from src.nadobro.quant.ladder import NO_FLOOR_LEVEL_BOUND

    assert max_levels(1000, 0) == NO_FLOOR_LEVEL_BOUND
    assert max_levels(1000, None) == NO_FLOOR_LEVEL_BOUND
    assert len(plan_ladder(1000, levels=4, step_bp=1, min_notional=0)) == 4
