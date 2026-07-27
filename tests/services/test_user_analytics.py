"""Per-user analytics: Nado vs Nadobro volume, perp vs spot, windowed.

Pins the 2026-07-26 portfolio bug: the card showed "Volume $0.00" next to
"Realized +$16.73" for the same 24h window, because Volume/Fees/Funding were
aggregated from a transient client.get_matches(limit=200) list while Realized
PnL was overwritten from the full trades_<network> history. One ledger now
feeds both, so volume and PnL always describe the SAME rows.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src.nadobro.quant.user_analytics import (
    aggregate_user_analytics,
    is_perp_fill,
)

NOW = datetime(2026, 7, 26, 12, 0, tzinfo=timezone.utc)


def _fill(*, hours_ago, product, quote, fee="0", via_nadobro=False, side="long", base="1"):
    return {
        "product_id": 2,
        "product_name": product,
        "side": side,
        "base_filled_x18": str(int(Decimal(base) * 10**18)),
        "quote_filled_x18": str(int(Decimal(quote) * 10**18)),
        "fee_x18": str(int(Decimal(fee) * 10**18)),
        "via_nadobro": via_nadobro,
        "filled_at": NOW - timedelta(hours=hours_ago),
        "submission_idx": f"{hours_ago}{product}{quote}",
    }


# ── classification ──────────────────────────────────────────────

def test_perp_vs_spot_classification():
    assert is_perp_fill({"product_name": "BTC-PERP"}) is True
    assert is_perp_fill({"product_name": "ETH:PERP-USDC"}) is True
    assert is_perp_fill({"product_name": "KBTC"}) is False       # spot book
    assert is_perp_fill({"product_name": "WETH"}) is False
    assert is_perp_fill({"product_name": "USDT0"}) is False
    # An explicitly stored flag always wins (historical rows cannot drift).
    assert is_perp_fill({"product_name": "KBTC", "is_perp": True}) is True


# ── windows ─────────────────────────────────────────────────────

def test_volume_windows_are_cumulative_and_scoped():
    fills = [
        _fill(hours_ago=1, product="BTC-PERP", quote="1000"),
        _fill(hours_ago=48, product="BTC-PERP", quote="500"),     # outside 24h
        _fill(hours_ago=24 * 10, product="BTC-PERP", quote="200"),  # outside 7d
        _fill(hours_ago=24 * 60, product="BTC-PERP", quote="50"),   # outside 30d
    ]
    out = aggregate_user_analytics(fills, [], now=NOW)
    v = out["nado_volume"]
    assert v["24h"]["total_usd"] == Decimal("1000")
    assert v["7d"]["total_usd"] == Decimal("1500")
    assert v["30d"]["total_usd"] == Decimal("1700")
    assert v["all"]["total_usd"] == Decimal("1750")   # All is NOT capped


def test_perp_and_spot_split_per_window():
    fills = [
        _fill(hours_ago=1, product="BTC-PERP", quote="1000"),
        _fill(hours_ago=2, product="KBTC", quote="250"),
    ]
    out = aggregate_user_analytics(fills, [], now=NOW)
    day = out["nado_volume"]["24h"]
    assert day["perp_usd"] == Decimal("1000")
    assert day["spot_usd"] == Decimal("250")
    assert day["total_usd"] == Decimal("1250")


def test_nadobro_volume_is_a_subset_of_nado_volume():
    """Nado volume = everything on the account (incl. Nado-UI trades);
    Nadobro volume = only fills the bot routed."""
    fills = [
        _fill(hours_ago=1, product="BTC-PERP", quote="1000", via_nadobro=True),
        _fill(hours_ago=2, product="BTC-PERP", quote="400"),   # traded on Nado UI
    ]
    out = aggregate_user_analytics(fills, [], now=NOW)
    assert out["nado_volume"]["24h"]["total_usd"] == Decimal("1400")
    assert out["nadobro_volume"]["24h"]["total_usd"] == Decimal("1000")
    assert out["nadobro_volume"]["24h"]["fills"] == 1
    assert out["nado_volume"]["24h"]["fills"] == 2


def test_volume_and_realized_pnl_describe_the_same_rows():
    """The reported bug: volume $0.00 alongside a non-zero realized PnL. Both
    now derive from the one ledger, so a round trip produces BOTH."""
    fills = [
        _fill(hours_ago=3, product="BTC-PERP", quote="60000", base="1", side="long"),
        _fill(hours_ago=1, product="BTC-PERP", quote="60100", base="1", side="short"),
    ]
    out = aggregate_user_analytics(fills, [], now=NOW)
    assert out["nado_volume"]["24h"]["total_usd"] == Decimal("120100")
    assert out["realized_pnl"]["24h"] == Decimal("100")      # 60100 - 60000
    # Never one without the other.
    assert (out["nado_volume"]["24h"]["total_usd"] > 0) == (out["realized_pnl"]["24h"] != 0)


def test_fees_are_not_double_counted_across_overlapping_columns():
    """The recorder stores fill_fee == fees == fee + builder; summing them all
    (the Night HOWL bug) inflated fees ~2.2x."""
    row = {
        "product_name": "BTC-PERP", "side": "long",
        "base_filled_x18": str(10**18), "quote_filled_x18": str(1000 * 10**18),
        "fill_fee": "2.0", "fees": "2.0", "builder_fee": "0.5",
        "filled_at": NOW - timedelta(hours=1), "submission_idx": "1",
    }
    out = aggregate_user_analytics([row], [], now=NOW)
    assert out["fees"]["24h"] == Decimal("2.0")


def test_funding_is_reported_paid_positive_as_a_cost():
    # amount_x18 positive = paid by the user (indexer convention).
    payments = [{"amount_x18": str(3 * 10**18), "paid_at": NOW - timedelta(hours=2)}]
    out = aggregate_user_analytics([], payments, now=NOW)
    assert out["funding"]["24h"] == Decimal("3")


def test_empty_ledger_is_all_zero_not_an_error():
    out = aggregate_user_analytics([], [], now=NOW)
    for w in ("24h", "7d", "30d", "all"):
        assert out["nado_volume"][w]["total_usd"] == Decimal("0")
        assert out["nadobro_volume"][w]["total_usd"] == Decimal("0")
        assert out["realized_pnl"][w] == Decimal("0")
