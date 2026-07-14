"""Copy-trading accounting accuracy: derived PnL, the cumulative rail, and
external-close classification.

Context that makes these load-bearing: the venue reports NO per-fill realized
PnL (realized_pnl_x18 is always 0), so every copy PnL number must be DERIVED
by pairing the close fill price against the recorded entry. The cumulative
SL/TP rail judges net-of-fees including unrealized PnL — the same contract as
every other session rail in the product.
"""

import pytest

from src.nadobro.trading.copy_service import (
    _classify_external_change,
    _close_pnl_gross,
    _rail_decision,
)


# ── derived close PnL (gross; fees tracked separately) ──────────────────

def test_long_profit():
    assert _close_pnl_gross(100.0, 110.0, 2.0, "long") == pytest.approx(20.0)


def test_long_loss():
    assert _close_pnl_gross(100.0, 95.0, 2.0, "LONG") == pytest.approx(-10.0)


def test_short_profit():
    assert _close_pnl_gross(100.0, 90.0, 3.0, "short") == pytest.approx(30.0)


def test_short_loss():
    assert _close_pnl_gross(100.0, 104.0, 3.0, "SHORT") == pytest.approx(-12.0)


def test_unknown_inputs_book_zero_not_garbage():
    assert _close_pnl_gross(0.0, 110.0, 2.0, "long") == 0.0
    assert _close_pnl_gross(100.0, 0.0, 2.0, "long") == 0.0
    assert _close_pnl_gross(100.0, 110.0, 0.0, "long") == 0.0


# ── the cumulative SL/TP rail (pure decision) ────────────────────────────

def test_rail_stop_loss_fires_on_net_loss():
    # $500 allocated, 10% SL → fires at -$50 net.
    assert _rail_decision(-50.0, 500.0, 10.0, 100.0) == "auto_stop_loss"
    assert _rail_decision(-49.9, 500.0, 10.0, 100.0) is None


def test_rail_take_profit_fires_on_net_gain():
    assert _rail_decision(100.0, 500.0, 10.0, 20.0) == "auto_take_profit"
    assert _rail_decision(99.9, 500.0, 10.0, 20.0) is None


def test_rail_includes_unrealized_by_construction():
    """The rail input is net = realized + unrealized - fees. A position that
    is down 60% UNREALIZED with zero realized closes must trip a 50% SL —
    the exact blind spot the old realized-only rail had."""
    realized, unrealized, fees, allocated = 0.0, -300.0, 5.0, 500.0
    net = realized + unrealized - fees
    assert _rail_decision(net, allocated, 50.0, 100.0) == "auto_stop_loss"


def test_rail_fees_push_a_flat_book_toward_stop():
    # Churned fees alone can breach the SL — judged NET, like live_session.
    assert _rail_decision(0.0 + 0.0 - 51.0, 500.0, 10.0, 0.0) == "auto_stop_loss"


def test_rail_disabled_thresholds_never_fire():
    assert _rail_decision(-400.0, 500.0, 0.0, 0.0) is None
    assert _rail_decision(400.0, 500.0, 0.0, 0.0) is None


def test_rail_zero_allocation_is_inert():
    assert _rail_decision(-400.0, 0.0, 10.0, 10.0) is None


# ── external-close classification (bracket fills / manual closes) ───────

CP = {"size": 2.0, "side": "long"}


def test_missing_venue_position_is_closed():
    assert _classify_external_change(CP, None) == ("closed", 0.0)


def test_side_flip_on_venue_is_closed():
    vp = {"signed_amount": -2.0, "amount": 2.0, "side": "SHORT"}
    assert _classify_external_change(CP, vp)[0] == "closed"


def test_reduced_below_tolerance_is_reduced():
    vp = {"signed_amount": 1.0, "amount": 1.0, "side": "LONG"}
    kind, venue_size = _classify_external_change(CP, vp)
    assert kind == "reduced" and venue_size == pytest.approx(1.0)


def test_within_rounding_tolerance_is_ok():
    vp = {"signed_amount": 1.97, "amount": 1.97, "side": "LONG"}
    assert _classify_external_change(CP, vp)[0] == "ok"


def test_full_size_match_is_ok():
    vp = {"signed_amount": 2.0, "amount": 2.0, "side": "LONG"}
    assert _classify_external_change(CP, vp)[0] == "ok"
