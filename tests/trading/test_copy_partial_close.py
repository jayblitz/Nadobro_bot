"""Partial-close mirroring math + the $100 minimum copy margin.

The baseline rule matters: ``leader_size`` on the copy row is the leader size
we LAST mirrored (at open, or after the previous partial). A leader ADD does
not raise the baseline, so add-then-trim-back mirrors nothing; only trims
BELOW the baseline reduce the copy, by the same fraction.
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


def test_no_action_when_leader_adds():
    # Baseline 10, leader scales in to 15 — we never scale in, and the
    # baseline must not move (caller keeps stored leader_size at 10).
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


def test_add_then_trim_back_to_baseline_is_inert():
    # Leader: 10 -> 15 (ignored, baseline stays 10) -> back to 10.
    # The comparison is always against the stored baseline of 10.
    assert _partial_close_fraction(10.0, 10.0) == 0.0
    # ...and a trim below the original baseline still mirrors correctly.
    assert _partial_close_fraction(10.0, 7.0) == pytest.approx(0.3)
