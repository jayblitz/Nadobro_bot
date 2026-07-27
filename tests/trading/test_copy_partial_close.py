"""Partial-close mirroring math + the $100 minimum copy margin.

The baseline rule matters: ``leader_size`` on the copy row is the leader size
we LAST mirrored. ``_partial_close_fraction`` only ever reports REDUCTIONS —
it returns 0.0 for an add. Scaling UP is mirrored separately by
``_mirror_leader_add_if_needed`` (COPY-NO-SCALE-UP, 2026-07-27), which also
raises the baseline, so an add followed by a trim back now mirrors BOTH legs
instead of silently ignoring the add.
"""

import pytest

from src.nadobro.trading.copy_service import (
    MIN_MARGIN_PER_TRADE,
    PARTIAL_CLOSE_DUST_FRACTION,
    PARTIAL_CLOSE_MIN_DELTA_PCT,
    _partial_close_fraction,
)


def test_min_margin_is_100():
    assert MIN_MARGIN_PER_TRADE == 100.0


def test_no_action_when_leader_unchanged():
    assert _partial_close_fraction(10.0, 10.0) == 0.0


def test_close_fraction_reports_no_reduction_when_leader_adds():
    # This helper only measures REDUCTIONS. An add is 0.0 here and is mirrored
    # by _mirror_leader_add_if_needed instead (see test_copy_scale_up.py).
    assert _partial_close_fraction(10.0, 15.0) == 0.0


def test_trim_below_baseline_mirrors_fraction():
    # Leader halves the position -> close half the copy.
    assert _partial_close_fraction(10.0, 5.0) == pytest.approx(0.5)
    # 25% trim -> 25% close.
    assert _partial_close_fraction(8.0, 6.0) == pytest.approx(0.25)


def test_small_trims_below_threshold_are_noise():
    # A trim under PARTIAL_CLOSE_MIN_DELTA_PCT (default 5%) is ignored —
    # venue rounding and dust reconciliation should not churn orders.
    just_under = 1.0 - (PARTIAL_CLOSE_MIN_DELTA_PCT - 0.1) / 100.0
    assert _partial_close_fraction(100.0, 100.0 * just_under) == 0.0


def test_trim_to_dust_closes_fully():
    # Remainder at/below the dust fraction becomes a full close.
    remaining = PARTIAL_CLOSE_DUST_FRACTION * 0.5
    assert _partial_close_fraction(10.0, 10.0 * remaining) == 1.0


def test_zero_or_unknown_baseline_is_inert():
    assert _partial_close_fraction(0.0, 5.0) == 0.0
    assert _partial_close_fraction(-1.0, 5.0) == 0.0


def test_trim_is_measured_against_the_current_baseline():
    # After a mirrored add the baseline moves to 15, so a trim back to 10 is a
    # real 1/3 reduction (previously the add was ignored, the baseline stayed
    # 10, and the trim mirrored nothing).
    assert _partial_close_fraction(15.0, 10.0) == pytest.approx(1.0 / 3.0)
    assert _partial_close_fraction(10.0, 10.0) == 0.0
    assert _partial_close_fraction(10.0, 7.0) == pytest.approx(0.3)
