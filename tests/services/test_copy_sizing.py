"""COPY-SIZE + COPY-LEVERAGE guardrails for the pure copy-sizing helpers.

Before the fix, every mirror was a fixed notional (margin_per_trade * max_lev /
entry): a leader's tiny probe and their max-conviction position were copied
identically, always at the user's max leverage. Now the copy scales with the
leader's conviction (size as a fraction of their largest position) and mirrors
the leader's leverage, capped by the user's max.
"""
from __future__ import annotations

from src.nadobro.trading.copy_service import (
    _compute_copy_sizing,
    _entry_deviation_too_far,
    _leader_max_notional,
)


def test_leader_max_notional_picks_the_biggest_position():
    book = {
        2: {"size": 1.0, "entry_price": 60000.0},   # 60,000
        4: {"size": 2.0, "entry_price": 3000.0},    #  6,000
    }
    assert _leader_max_notional(book) == 60000.0


def test_probe_is_copied_proportionally_smaller_than_max_conviction():
    maxn = 60000.0
    big, _ = _compute_copy_sizing(
        leader_size=1.0, leader_entry=60000.0, leader_leverage=4.0,
        leader_max_notional=maxn, margin_per_trade=50.0,
        max_leverage=10.0, product_max_leverage=20.0,
    )
    probe, _ = _compute_copy_sizing(
        leader_size=2.0, leader_entry=3000.0, leader_leverage=4.0,
        leader_max_notional=maxn, margin_per_trade=50.0,
        max_leverage=10.0, product_max_leverage=20.0,
    )
    # Big = full margin (50) * 4x / 60000 -> notional 200.
    assert round(big * 60000.0, 2) == 200.0
    # Probe is 10% of the leader's biggest -> 10% of the notional.
    assert round(probe * 3000.0, 2) == 20.0


def test_leverage_mirrors_leader_capped_by_user_max():
    _, lev = _compute_copy_sizing(
        leader_size=1.0, leader_entry=100.0, leader_leverage=50.0,
        leader_max_notional=100.0, margin_per_trade=50.0,
        max_leverage=10.0, product_max_leverage=20.0,
    )
    assert lev == 10.0  # leader 50x capped at the user's 10x


def test_leverage_uses_leader_when_below_user_max():
    _, lev = _compute_copy_sizing(
        leader_size=1.0, leader_entry=100.0, leader_leverage=3.0,
        leader_max_notional=100.0, margin_per_trade=50.0,
        max_leverage=10.0, product_max_leverage=20.0,
    )
    assert lev == 3.0  # mirror the leader's modest leverage, not the max


def test_leverage_falls_back_when_leader_leverage_unknown():
    _, lev = _compute_copy_sizing(
        leader_size=1.0, leader_entry=100.0, leader_leverage=0.0,
        leader_max_notional=100.0, margin_per_trade=50.0,
        max_leverage=10.0, product_max_leverage=20.0,
    )
    assert lev == 10.0  # min(user max 10, product max 20)


def test_entry_deviation_gate_blocks_far_entries_only():
    # within tolerance -> allowed
    assert _entry_deviation_too_far(mid=101.0, leader_entry=100.0, max_dev_pct=1.5) is False
    # exactly at tolerance -> allowed (not strictly greater)
    assert _entry_deviation_too_far(mid=101.5, leader_entry=100.0, max_dev_pct=1.5) is False
    # beyond tolerance -> blocked (would enter ~2% worse than the leader)
    assert _entry_deviation_too_far(mid=102.0, leader_entry=100.0, max_dev_pct=1.5) is True
    # short side / price below entry also measured by magnitude
    assert _entry_deviation_too_far(mid=97.0, leader_entry=100.0, max_dev_pct=1.5) is True


def test_entry_deviation_gate_allows_when_inputs_unknown():
    # Can't assess -> don't block (degrade gracefully).
    assert _entry_deviation_too_far(mid=0.0, leader_entry=100.0, max_dev_pct=1.5) is False
    assert _entry_deviation_too_far(mid=100.0, leader_entry=0.0, max_dev_pct=1.5) is False
    assert _entry_deviation_too_far(mid=100.0, leader_entry=100.0, max_dev_pct=0.0) is False


def test_zero_entry_or_size_yields_no_position():
    assert _compute_copy_sizing(
        leader_size=0.0, leader_entry=100.0, leader_leverage=4.0,
        leader_max_notional=100.0, margin_per_trade=50.0,
        max_leverage=10.0, product_max_leverage=20.0,
    ) == (0.0, 0.0)
    assert _compute_copy_sizing(
        leader_size=1.0, leader_entry=0.0, leader_leverage=4.0,
        leader_max_notional=100.0, margin_per_trade=50.0,
        max_leverage=10.0, product_max_leverage=20.0,
    ) == (0.0, 0.0)
