"""Unit tests for the position-aware realized-PnL engine (Layer 3).

Pure, no DB. Verifies that realized PnL is DERIVED correctly from fills by
replaying running position + average entry cost, since this venue reports no
per-fill realized PnL.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src.nadobro.quant.portfolio_calculator import realized_pnl_windows_from_rows

NOW = datetime(2026, 6, 21, 12, 0, 0, tzinfo=timezone.utc)


def _fill(side, base, price, *, pid=2, ago_seconds=0):
    return {
        "product_id": pid,
        "side": side,
        "fill_size": str(base),
        "fill_price": str(price),
        "filled_at": NOW - timedelta(seconds=ago_seconds),
    }


def _pnl(rows):
    return realized_pnl_windows_from_rows(rows, now=NOW)


def test_long_round_trip_profit():
    r = _pnl([_fill("long", 1, 100), _fill("short", 1, 110)])
    assert r["total_pnl"] == Decimal("10")
    assert r["wins"] == 1 and r["losses"] == 0


def test_short_round_trip_profit():
    r = _pnl([_fill("short", 1, 100), _fill("long", 1, 90)])
    assert r["total_pnl"] == Decimal("10")
    assert r["wins"] == 1


def test_long_round_trip_loss():
    r = _pnl([_fill("long", 1, 100), _fill("short", 1, 90)])
    assert r["total_pnl"] == Decimal("-10")
    assert r["losses"] == 1 and r["wins"] == 0


def test_partial_close_leaves_open_remainder():
    # buy 2 @100, sell 1 @110 -> realize +10 on the closed unit; 1 long remains.
    r = _pnl([_fill("long", 2, 100), _fill("short", 1, 110)])
    assert r["total_pnl"] == Decimal("10")


def test_average_entry_cost():
    # buy 1@100, buy 1@200 -> avg 150; sell 2@160 -> (160-150)*2 = +20.
    r = _pnl([_fill("long", 1, 100), _fill("long", 1, 200), _fill("short", 2, 160)])
    assert r["total_pnl"] == Decimal("20")


def test_flip_realizes_then_reopens():
    # long 1@100; sell 2@120 -> close 1 (+20) and open short 1@120;
    # buy 1@110 -> close short (+10). Total +30.
    r = _pnl([_fill("long", 1, 100), _fill("short", 2, 120), _fill("long", 1, 110)])
    assert r["total_pnl"] == Decimal("30")
    assert r["wins"] == 2


def test_open_only_has_no_realized():
    r = _pnl([_fill("long", 1, 100)])
    assert r["total_pnl"] == Decimal("0")
    assert r["wins"] == 0 and r["losses"] == 0


def test_products_are_independent():
    rows = [
        _fill("long", 1, 100, pid=2), _fill("short", 1, 110, pid=2),   # BTC +10
        _fill("short", 1, 50, pid=4), _fill("long", 1, 55, pid=4),     # ETH -5
    ]
    r = _pnl(rows)
    assert r["total_pnl"] == Decimal("5")
    assert r["wins"] == 1 and r["losses"] == 1


def test_time_windows_bucket_by_close_time():
    # Round trip closed 10 days ago: in 30d + all, NOT in 24h/7d.
    rows = [_fill("long", 1, 100, ago_seconds=10 * 86400 + 60),
            _fill("short", 1, 110, ago_seconds=10 * 86400)]
    r = _pnl(rows)
    assert r["pnl_windows"]["all"] == Decimal("10")
    assert r["pnl_windows"]["30d"] == Decimal("10")
    assert r["pnl_windows"]["7d"] == Decimal("0")
    assert r["pnl_windows"]["24h"] == Decimal("0")


def test_win_rate():
    rows = [
        _fill("long", 1, 100), _fill("short", 1, 110),   # win
        _fill("long", 1, 100), _fill("short", 1, 90),    # loss
        _fill("long", 1, 100), _fill("short", 1, 105),   # win
    ]
    r = _pnl(rows)
    assert r["wins"] == 2 and r["losses"] == 1
    assert r["win_rate"] == Decimal(2) / Decimal(3) * Decimal(100)


def test_x18_columns_preferred_for_price():
    # base_filled_x18 / quote_filled_x18 (18-dp fixed point) drive base & price.
    rows = [
        {"product_id": 2, "side": "long",
         "base_filled_x18": str(1 * 10**18), "quote_filled_x18": str(100 * 10**18),
         "filled_at": NOW - timedelta(seconds=120)},
        {"product_id": 2, "side": "short",
         "base_filled_x18": str(1 * 10**18), "quote_filled_x18": str(110 * 10**18),
         "filled_at": NOW - timedelta(seconds=60)},
    ]
    r = _pnl(rows)
    assert r["total_pnl"] == Decimal("10")
