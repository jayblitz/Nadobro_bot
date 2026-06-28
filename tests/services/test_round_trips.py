"""Tests for trade_service.compute_round_trips.

The History tab pairs non-strategy fills into round-trips via a FIFO
inventory model. These tests verify:

  - Simple open + close pair emits a single round-trip with realized PnL.
  - Partial closes consume FIFO lots correctly.
  - Cross and isolated keys are tracked independently.
  - Strategy-tagged rows are excluded from the source set.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from _stubs import install_test_stubs  # noqa: F401

install_test_stubs()

from src.nadobro.services import trade_service


def _row(
    trade_id: int,
    side: str,
    size: float,
    price: float,
    *,
    fee: float = 0.0,
    realized_pnl: float = 0.0,
    funding_paid: float = 0.0,
    isolated: bool = False,
    order_type: str = "MARKET",
    product_id: int = 1,
    product_name: str = "BTC-PERP",
    ts: datetime | None = None,
) -> dict:
    return {
        "id": trade_id,
        "product_id": product_id,
        "product_name": product_name,
        "side": side,
        "status": "filled",
        "sz": size,
        "px": price,
        "fee": fee,
        "realized_pnl": realized_pnl,
        "funding_paid": funding_paid,
        "isolated": isolated,
        "order_type": order_type,
        "ts": ts or datetime(2026, 5, 1, tzinfo=timezone.utc),
    }


def test_compute_round_trips_derives_pnl_from_prices_not_void_field():
    # Regression: this venue reports no per-fill realized PnL, so the close row's
    # realized_pnl is 0. PnL must be DERIVED from prices (gross of fees) = +10,
    # NOT read as 0 from the void field (the "manual trade PnL shown as 0" bug).
    open_ts = datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)
    close_ts = datetime(2026, 5, 1, 14, 0, tzinfo=timezone.utc)
    rows = [
        _row(1, "long", 1.0, 100.0, fee=0.10, ts=open_ts),
        _row(2, "short", 1.0, 110.0, fee=0.20, realized_pnl=0.0, order_type="CLOSE_MARKET", ts=close_ts),
    ]
    with patch("src.nadobro.db.query_all", return_value=rows):
        trips = trade_service.compute_round_trips(42, "mainnet")

    assert len(trips) == 1
    trip = trips[0]
    assert trip["trip_key"] == "2"  # close trade id
    assert trip["side"] == "long"
    assert trip["realized_pnl"] == pytest.approx(10.0)   # (110-100)*1, gross
    assert trip["fees"] == pytest.approx(0.30)
    assert trip["avg_open_price"] == pytest.approx(100.0)
    assert trip["avg_close_price"] == pytest.approx(110.0)
    assert trip["open_ts"] == open_ts
    assert trip["close_ts"] == close_ts


def test_compute_round_trips_pairs_venue_fill_close_opposite_side():
    # A close that arrives as a venue 'match' fill (order_type not CLOSE) must
    # still pair against the open via opposite-side detection and derive PnL.
    open_ts = datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)
    close_ts = datetime(2026, 5, 1, 14, 0, tzinfo=timezone.utc)
    rows = [
        _row(1, "short", 2.0, 200.0, fee=0.10, order_type="match", ts=open_ts),
        _row(2, "long", 2.0, 180.0, fee=0.10, realized_pnl=0.0, order_type="match", ts=close_ts),
    ]
    with patch("src.nadobro.db.query_all", return_value=rows):
        trips = trade_service.compute_round_trips(42, "mainnet")

    assert len(trips) == 1
    trip = trips[0]
    assert trip["side"] == "short"
    assert trip["realized_pnl"] == pytest.approx(40.0)   # (200-180)*2 short profit
    assert trip["size"] == pytest.approx(2.0)


def test_compute_round_trips_consumes_fifo_lots_on_partial_close():
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    rows = [
        _row(1, "long", 1.0, 100.0, ts=base_ts),
        _row(2, "long", 1.0, 120.0, ts=base_ts + timedelta(hours=1)),
        # Close 1.5: consumes lot #1 entirely + 0.5 of lot #2.
        _row(
            3, "short", 1.5, 130.0,
            realized_pnl=35.0,
            order_type="CLOSE_MARKET",
            ts=base_ts + timedelta(hours=2),
        ),
    ]
    with patch("src.nadobro.db.query_all", return_value=rows):
        trips = trade_service.compute_round_trips(42, "mainnet")

    assert len(trips) == 1
    trip = trips[0]
    assert trip["size"] == pytest.approx(1.5)
    # Weighted average open price: (1*100 + 0.5*120) / 1.5
    assert trip["avg_open_price"] == pytest.approx((100.0 + 60.0) / 1.5)


def test_compute_round_trips_tracks_isolated_and_cross_independently():
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    rows = [
        _row(1, "long", 1.0, 100.0, isolated=False, ts=base_ts),
        _row(2, "long", 1.0, 100.0, isolated=True, ts=base_ts + timedelta(minutes=1)),
        _row(
            3, "short", 1.0, 110.0, isolated=False,
            realized_pnl=10.0, order_type="CLOSE_MARKET",
            ts=base_ts + timedelta(minutes=2),
        ),
        _row(
            4, "short", 1.0, 90.0, isolated=True,
            realized_pnl=-10.0, order_type="CLOSE_MARKET",
            ts=base_ts + timedelta(minutes=3),
        ),
    ]
    with patch("src.nadobro.db.query_all", return_value=rows):
        trips = trade_service.compute_round_trips(42, "mainnet")

    assert len(trips) == 2
    # Newest first.
    assert trips[0]["trip_key"] == "4"
    assert trips[0]["isolated"] is True
    assert trips[1]["trip_key"] == "3"
    assert trips[1]["isolated"] is False


def test_compute_round_trips_query_filters_strategy_session_rows():
    """The aggregator must NOT pull rows tagged with a strategy session —
    those live under the Performance tab. We assert the WHERE clause
    indirectly: if the query SQL doesn't mention ``strategy_session_id IS NULL``
    History would leak strategy fills into manual round-trips."""
    captured: dict[str, str] = {}

    def _fake_query(sql, *params):
        captured["sql"] = sql
        return []

    with patch("src.nadobro.db.query_all", side_effect=_fake_query):
        trade_service.compute_round_trips(42, "mainnet")

    assert "strategy_session_id IS NULL" in captured["sql"]
    assert "source" in captured["sql"]
    assert "manual" in captured["sql"]


def test_compute_round_trips_returns_empty_on_db_error():
    def _boom(*args, **kwargs):
        raise RuntimeError("db down")

    with patch("src.nadobro.db.query_all", side_effect=_boom):
        assert trade_service.compute_round_trips(42, "mainnet") == []


def test_compute_round_trips_reads_network_scoped_table():
    """History must never mix testnet and mainnet trades.

    Compute on each network and assert the SQL targets the matching
    ``trades_<network>`` table.
    """
    captured: list[str] = []

    def _capture(sql, *params):
        captured.append(sql)
        return []

    with patch("src.nadobro.db.query_all", side_effect=_capture):
        trade_service.compute_round_trips(42, "testnet")
        trade_service.compute_round_trips(42, "mainnet")

    assert "trades_testnet" in captured[0]
    assert "trades_mainnet" not in captured[0]
    assert "trades_mainnet" in captured[1]
    assert "trades_testnet" not in captured[1]
