"""Unit tests for per-position trade pairing (normal/desk PnL card data).

Pure, no DB. Verifies fills are paired into open/close round-trips with correct
average entry/exit, realized PnL (gross of fees), and fees. The engine returns
``PairedTrade`` dataclasses (the DB layer calls ``.to_dict()`` for renderers).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src.nadobro.quant.portfolio_calculator import pair_fills_into_trades

T0 = datetime(2026, 6, 21, 0, 0, 0, tzinfo=timezone.utc)


def _fill(side, base, price, *, pid=2, t=0, fee=0.0):
    return {
        "product_id": pid,
        "side": side,
        "fill_size": str(base),
        "fill_price": str(price),
        "fill_fee": str(fee),
        "submission_idx": t,
        "filled_at": T0 + timedelta(seconds=t),
    }


def test_simple_long_round_trip():
    trades = pair_fills_into_trades([_fill("long", 1, 100, t=1), _fill("short", 1, 110, t=2)])
    assert len(trades) == 1
    tr = trades[0]
    assert tr.closed is True
    assert tr.direction == "long"
    assert tr.size == Decimal("1")
    assert tr.entry_price == Decimal("100")
    assert tr.exit_price == Decimal("110")
    assert tr.realized_pnl == Decimal("10")


def test_short_round_trip():
    trades = pair_fills_into_trades([_fill("short", 1, 100, t=1), _fill("long", 1, 90, t=2)])
    assert len(trades) == 1 and trades[0].direction == "short"
    assert trades[0].realized_pnl == Decimal("10")


def test_loss_trade():
    trades = pair_fills_into_trades([_fill("long", 1, 100, t=1), _fill("short", 1, 90, t=2)])
    assert trades[0].realized_pnl == Decimal("-10")


def test_partial_closes_aggregate_into_one_trade():
    # buy 2@100; sell 1@110; sell 1@120 -> one closed trade, exit avg 115, +30.
    trades = pair_fills_into_trades([
        _fill("long", 2, 100, t=1), _fill("short", 1, 110, t=2), _fill("short", 1, 120, t=3),
    ])
    assert len(trades) == 1
    tr = trades[0]
    assert tr.size == Decimal("2")
    assert tr.entry_price == Decimal("100")
    assert tr.exit_price == Decimal("115")
    assert tr.realized_pnl == Decimal("30")


def test_multiple_entries_average_cost():
    trades = pair_fills_into_trades([
        _fill("long", 1, 100, t=1), _fill("long", 1, 200, t=2), _fill("short", 2, 160, t=3),
    ])
    assert len(trades) == 1
    assert trades[0].entry_price == Decimal("150")
    assert trades[0].realized_pnl == Decimal("20")


def test_flip_emits_two_trades():
    # long 1@100; sell 2@120 -> close long (+20) and open short 1@120;
    # buy 1@110 -> close short (+10).
    trades = pair_fills_into_trades([
        _fill("long", 1, 100, t=1), _fill("short", 2, 120, t=2), _fill("long", 1, 110, t=3),
    ])
    assert len(trades) == 2
    first, second = trades[0], trades[1]
    assert first.direction == "long" and first.realized_pnl == Decimal("20")
    assert second.direction == "short" and second.realized_pnl == Decimal("10")
    assert all(t.closed for t in trades)


def test_fees_accumulate_and_net_pnl():
    trades = pair_fills_into_trades([
        _fill("long", 1, 100, t=1, fee=0.1), _fill("short", 1, 110, t=2, fee=0.1),
    ])
    tr = trades[0]
    assert tr.fees == Decimal("0.2")
    assert tr.realized_pnl == Decimal("10")
    assert tr.net_pnl == Decimal("9.8")


def test_open_position_emitted_last_as_unclosed():
    trades = pair_fills_into_trades([
        _fill("long", 1, 100, t=1), _fill("short", 1, 110, t=2),  # closed +10
        _fill("long", 2, 50, t=3),                                 # still open
    ])
    assert len(trades) == 2
    closed = [t for t in trades if t.closed]
    opens = [t for t in trades if not t.closed]
    assert len(closed) == 1 and len(opens) == 1
    op = opens[0]
    assert op.size == Decimal("2") and op.entry_price == Decimal("50")
    assert op.exit_price is None and op.realized_pnl == Decimal("0")


def test_products_independent():
    trades = pair_fills_into_trades([
        _fill("long", 1, 100, pid=2, t=1), _fill("long", 1, 50, pid=4, t=2),
        _fill("short", 1, 110, pid=2, t=3), _fill("short", 1, 45, pid=4, t=4),
    ])
    closed = [t for t in trades if t.closed]
    assert len(closed) == 2
    by_pid = {t.product_id: t for t in closed}
    assert by_pid[2].realized_pnl == Decimal("10")
    assert by_pid[4].realized_pnl == Decimal("-5")


def test_fills_sorted_by_time_before_pairing():
    # Provided out of order; must still pair buy-then-sell correctly.
    trades = pair_fills_into_trades([_fill("short", 1, 110, t=2), _fill("long", 1, 100, t=1)])
    assert len(trades) == 1 and trades[0].realized_pnl == Decimal("10")


def test_to_dict_exposes_net_pnl():
    tr = pair_fills_into_trades([
        _fill("long", 1, 100, t=1, fee=0.1), _fill("short", 1, 110, t=2, fee=0.1),
    ])[0]
    d = tr.to_dict()
    assert d["closed"] is True
    assert d["net_pnl"] == Decimal("9.8")
    assert d["realized_pnl"] == Decimal("10")


def test_x18_columns_drive_base_price_and_fee():
    rows = [
        {"product_id": 2, "side": "long", "base_filled_x18": str(1 * 10**18),
         "quote_filled_x18": str(100 * 10**18), "fee_x18": str(5 * 10**16),  # 0.05
         "submission_idx": 1, "filled_at": T0 + timedelta(seconds=1)},
        {"product_id": 2, "side": "short", "base_filled_x18": str(1 * 10**18),
         "quote_filled_x18": str(110 * 10**18), "fee_x18": str(5 * 10**16),
         "submission_idx": 2, "filled_at": T0 + timedelta(seconds=2)},
    ]
    tr = pair_fills_into_trades(rows)[0]
    assert tr.entry_price == Decimal("100") and tr.exit_price == Decimal("110")
    assert tr.realized_pnl == Decimal("10") and tr.fees == Decimal("0.10")
