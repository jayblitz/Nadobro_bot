"""Volume Bot fee estimator — the numbers behind the charges agreement.

The user presses "Agree & Start" against this output, so the invariants are:
never quote 0, never under-quote, and never double-count the legs.
"""
from __future__ import annotations

from decimal import Decimal

from src.nadobro.quant.vol_fee_estimator import (
    DEFAULT_BUILDER_FEE_RATE,
    DEFAULT_SPOT_TAKER_FEE_RATE,
    MAX_MARGIN_USD,
    MIN_MARGIN_USD,
    clamp_margin_usd,
    estimate_vol_fees,
    taker_breakeven_price,
    taker_sell_target,
)


def test_margin_band_is_100_to_500():
    assert clamp_margin_usd(25) == MIN_MARGIN_USD == Decimal("100")
    assert clamp_margin_usd(5000) == MAX_MARGIN_USD == Decimal("500")
    assert clamp_margin_usd(250) == Decimal("250")
    assert clamp_margin_usd(None) == Decimal("100")
    assert clamp_margin_usd("garbage") == Decimal("100")


def test_fee_is_volume_times_rate_not_double_counted():
    """Both legs already count toward session_volume_usd, so the target volume
    IS the fee-bearing notional. Doubling it (once per leg) would over-quote by
    2x; using only the sell leg would halve it."""
    est = estimate_vol_fees(
        margin_usd=100, target_volume_usd=10_000,
        taker_fee_rate=Decimal("0.0004"), builder_fee_rate=Decimal("0.0001"),
    )
    assert est.total_rate == Decimal("0.0005")
    assert est.total_rate_bp == Decimal("5")
    assert est.estimated_fee_usd == Decimal("5.0")   # 10_000 x 5bp
    assert est.rate_source == "venue"


def test_missing_venue_rate_falls_back_to_the_measured_default():
    """A None/0 catalog rate must never quote a free round trip."""
    for missing in (None, 0, Decimal(0), "0"):
        est = estimate_vol_fees(
            margin_usd=100, target_volume_usd=10_000, taker_fee_rate=missing,
        )
        assert est.taker_fee_rate == DEFAULT_SPOT_TAKER_FEE_RATE
        assert est.rate_source == "measured_default"
        assert est.estimated_fee_usd > 0


def test_builder_fee_defaults_to_the_locked_policy_rate():
    est = estimate_vol_fees(margin_usd=100, target_volume_usd=1000)
    assert est.builder_fee_rate == DEFAULT_BUILDER_FEE_RATE == Decimal("0.0001")
    # Testnet routes without a builder and pays 0 — explicitly passable.
    testnet = estimate_vol_fees(
        margin_usd=100, target_volume_usd=1000, builder_fee_rate=0,
    )
    assert testnet.builder_fee_rate == Decimal(0)
    assert testnet.estimated_fee_usd < est.estimated_fee_usd


def test_cycle_count_rounds_up_and_uses_both_legs():
    """Each cycle trades ~margin buying and ~margin selling = ~2x margin of
    counted volume. A partial final cycle still trades and still pays."""
    est = estimate_vol_fees(margin_usd=100, target_volume_usd=1000)
    assert est.estimated_cycles == 5          # 1000 / (2 x 100)
    partial = estimate_vol_fees(margin_usd=100, target_volume_usd=1050)
    assert partial.estimated_cycles == 6      # rounds UP
    assert estimate_vol_fees(margin_usd=100, target_volume_usd=0).estimated_cycles == 0


def test_fee_as_pct_of_margin_is_the_affordability_number():
    est = estimate_vol_fees(
        margin_usd=100, target_volume_usd=100_000,
        taker_fee_rate=Decimal("0.0004"), builder_fee_rate=Decimal("0.0001"),
    )
    assert est.estimated_fee_usd == Decimal("50.0")
    assert est.fee_pct_of_margin == Decimal("50.0")


def test_out_of_band_margin_is_clamped_inside_the_estimate():
    est = estimate_vol_fees(margin_usd=5000, target_volume_usd=10_000)
    assert est.margin_usd == Decimal("500")


def test_negative_target_volume_cannot_produce_a_negative_charge():
    est = estimate_vol_fees(margin_usd=100, target_volume_usd=-500)
    assert est.target_volume_usd == Decimal(0)
    assert est.estimated_fee_usd == Decimal(0)


def test_breakeven_is_above_entry_and_covers_both_legs():
    be = taker_breakeven_price(
        100, taker_fee_rate=Decimal("0.001"), builder_fee_rate=Decimal(0)
    )
    # sell x (1-r) == entry x (1+r)  →  100 x 1.001 / 0.999
    assert be == Decimal(100) * Decimal("1.001") / Decimal("0.999")
    assert be > Decimal(100)
    # Selling AT breakeven nets ~zero after both legs.
    net = be * (Decimal(1) - Decimal("0.001")) - Decimal(100) * (Decimal(1) + Decimal("0.001"))
    assert abs(net) < Decimal("0.000001")


def test_breakeven_of_no_entry_is_zero_not_a_tradable_price():
    assert taker_breakeven_price(0, taker_fee_rate=Decimal("0.001")) == Decimal(0)
    assert taker_breakeven_price(-5, taker_fee_rate=Decimal("0.001")) == Decimal(0)


def test_sell_target_satisfies_both_requirements():
    """max(above-entry, breakeven): strictly higher than the buy price AND not
    a fee-funded illusion."""
    # Fees dominate: breakeven wins.
    fee_heavy = taker_sell_target(
        100, taker_fee_rate=Decimal("0.005"), builder_fee_rate=Decimal(0),
        min_profit_bp=1,
    )
    assert fee_heavy > Decimal("100.5")

    # Fees ~0: the min-profit edge is what keeps it above the entry.
    edge_led = taker_sell_target(
        100, taker_fee_rate=Decimal("0"), builder_fee_rate=Decimal(0),
        min_profit_bp=50,
    )
    assert edge_led == Decimal("100.5")

    # Either way it is never at or below the entry.
    for bp in (0, 1, 50):
        t = taker_sell_target(
            100, taker_fee_rate=Decimal("0.0009"),
            builder_fee_rate=Decimal("0.0001"), min_profit_bp=bp,
        )
        assert t > Decimal(100), f"min_profit_bp={bp} produced a non-profitable target"
