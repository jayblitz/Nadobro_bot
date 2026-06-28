"""Night HOWL tests — pattern metrics, recommendations, local-8am scheduling,
report persistence, and a backtest-backed comparison.

All pure logic is exercised directly; persistence is tested against an in-memory
stand-in for the bot_state key-value store.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.nadobro.services import night_howl_service as nh

NOW = datetime(2026, 6, 20, 12, 0, tzinfo=timezone.utc).timestamp()


def _trades():
    return [
        {"created_at": NOW - 3600, "product_name": "BTC", "side": "long", "size": 0.1,
         "price": 60000, "fees": 2.0, "realized_pnl": 15.0, "source": "grid", "status": "filled"},
        {"created_at": NOW - 7200, "product_name": "BTC", "side": "short", "size": 0.1,
         "price": 60000, "fees": 2.0, "realized_pnl": -8.0, "source": "grid", "status": "filled"},
        {"created_at": NOW - 1800, "product_name": "ETH", "side": "long", "size": 1.0,
         "price": 3000, "fees": 1.0, "realized_pnl": 3.0, "source": "manual", "status": "filled"},
        {"created_at": NOW - 99999, "product_name": "OLD", "side": "long", "size": 1,
         "price": 1, "status": "filled"},                        # outside 24h window
        {"created_at": NOW - 100, "product_name": "SOL", "side": "long", "size": 1,
         "price": 150, "status": "failed"},                       # excluded (failed)
    ]


# --------------------------------------------------------------------------- #
# pattern metrics                                                              #
# --------------------------------------------------------------------------- #

def test_pattern_windows_and_aggregates():
    p = nh.compute_user_pattern(_trades(), now_ts=NOW)
    assert p["trades"] == 3                       # old + failed excluded
    assert p["volume_usd"] == 15000.0
    assert p["fees_usd"] == 5.0
    assert p["realized_pnl_usd"] == 10.0
    assert p["net_pnl_usd"] == 5.0                # 10 realized - 5 fees
    assert p["wins"] == 2 and p["losses"] == 1
    assert abs(p["win_rate"] - 2 / 3) < 1e-3
    assert p["top_pairs"][0]["pair"] == "BTC"


def test_pattern_empty_is_safe():
    p = nh.compute_user_pattern([], now_ts=NOW)
    assert p["trades"] == 0 and p["win_rate"] is None and p["top_pairs"] == []


def test_pattern_accepts_iso_and_epoch_timestamps():
    iso = datetime.fromtimestamp(NOW - 600, timezone.utc).isoformat()
    trades = [
        {"created_at": iso, "product_name": "BTC", "size": 1, "price": 100, "status": "filled"},
        {"created_at": int((NOW - 600) * 1000), "product_name": "ETH", "size": 1, "price": 50, "status": "filled"},
    ]
    assert nh.compute_user_pattern(trades, now_ts=NOW)["trades"] == 2


# --------------------------------------------------------------------------- #
# recommendations                                                             #
# --------------------------------------------------------------------------- #

def test_recommendations_flag_fee_drag_turning_net_negative():
    p = {"trades": 50, "volume_usd": 100000.0, "fees_usd": 300.0, "funding_usd": 0.0,
         "realized_pnl_usd": 120.0, "net_pnl_usd": -180.0, "win_rate": 0.55,
         "wins": 27, "losses": 23, "top_pairs": [{"pair": "BTC", "volume_usd": 40000.0}]}
    recs = nh.derive_recommendations(p, None)
    blob = " ".join(recs).lower()
    assert "fee" in blob and "net" in blob


def test_recommendations_use_backtest_when_a_variant_wins():
    p = nh.compute_user_pattern(_trades(), now_ts=NOW)
    backtests = [
        {"name": "wider step", "net_pnl": 14.5, "gross_pnl": 16, "fees": 1.5, "funding": 0, "max_drawdown": 2},
        {"name": "current", "net_pnl": 12.0, "gross_pnl": 16, "fees": 4.0, "funding": 0, "max_drawdown": 3},
    ]
    recs = nh.derive_recommendations(p, backtests)
    assert any("backtest" in r.lower() and "wider step" in r.lower() for r in recs)


def test_recommendations_hold_course_when_current_is_best():
    p = nh.compute_user_pattern(_trades(), now_ts=NOW)
    backtests = [
        {"name": "current", "net_pnl": 20.0, "gross_pnl": 22, "fees": 2, "funding": 0, "max_drawdown": 1},
        {"name": "wider step", "net_pnl": 10.0, "gross_pnl": 12, "fees": 2, "funding": 0, "max_drawdown": 1},
    ]
    recs = nh.derive_recommendations(p, backtests)
    assert any("hold course" in r.lower() for r in recs)


def test_recommendations_empty_day():
    recs = nh.derive_recommendations(nh.compute_user_pattern([], now_ts=NOW), None)
    assert len(recs) == 1 and "no trades" in recs[0].lower()


# --------------------------------------------------------------------------- #
# scheduling                                                                  #
# --------------------------------------------------------------------------- #

def test_due_only_at_local_8am_and_deduped():
    utc8 = datetime(2026, 6, 20, 8, 0, tzinfo=timezone.utc)
    assert nh.night_howl_due(utc8, 0.0, None) is True
    assert nh.night_howl_due(utc8.replace(hour=9), 0.0, None) is False
    # already sent today -> not due again
    assert nh.night_howl_due(utc8, 0.0, "2026-06-20") is False


def test_due_respects_user_timezone_offset():
    # offset -5: local 8am happens at 13:00 UTC
    assert nh.night_howl_due(datetime(2026, 6, 20, 13, 0, tzinfo=timezone.utc), -5.0, None) is True
    assert nh.night_howl_due(datetime(2026, 6, 20, 8, 0, tzinfo=timezone.utc), -5.0, None) is False
    # offset +9: local 8am happens at 23:00 UTC the prior day
    assert nh.night_howl_due(datetime(2026, 6, 19, 23, 0, tzinfo=timezone.utc), 9.0, None) is True


# --------------------------------------------------------------------------- #
# persistence (in-memory bot_state)                                           #
# --------------------------------------------------------------------------- #

@pytest.fixture
def memstore(monkeypatch):
    store: dict = {}

    def _get(key):
        return store.get(key)

    def _set(key, value):
        if value is None:
            store.pop(key, None)
        else:
            store[key] = value

    import src.nadobro.models.database as db
    monkeypatch.setattr(db, "get_bot_state", _get, raising=False)
    monkeypatch.setattr(db, "set_bot_state", _set, raising=False)
    return store


def test_save_get_list_reports_roundtrip(memstore):
    nh.save_report(7, "mainnet", {"date": "2026-06-18", "markdown": "a"})
    nh.save_report(7, "mainnet", {"date": "2026-06-19", "markdown": "b"})
    nh.save_report(7, "mainnet", {"date": "2026-06-20", "markdown": "c"})
    assert nh.list_report_dates(7, "mainnet") == ["2026-06-20", "2026-06-19", "2026-06-18"]
    assert nh.get_report(7, "mainnet")["markdown"] == "c"           # latest
    assert nh.get_report(7, "mainnet", "2026-06-18")["markdown"] == "a"
    assert nh.get_report(7, "mainnet", "2999-01-01") is None


def test_index_caps_and_evicts_oldest(memstore):
    for i in range(1, nh.MAX_SAVED_REPORTS + 6):
        nh.save_report(7, "mainnet", {"date": f"2026-07-{i:02d}", "markdown": str(i)})
    dates = nh.list_report_dates(7, "mainnet")
    assert len(dates) == nh.MAX_SAVED_REPORTS
    # oldest ones evicted from the store entirely
    assert nh.get_report(7, "mainnet", "2026-07-01") is None


def test_mark_and_read_last_sent(memstore):
    assert nh.last_sent_date(7, "mainnet") is None
    nh.mark_sent(7, "mainnet", "2026-06-20")
    assert nh.last_sent_date(7, "mainnet") == "2026-06-20"


def test_build_report_bounds_trade_query_to_last_24h(monkeypatch, memstore):
    import src.nadobro.models.database as db

    captured = {}

    def _trades(telegram_id, *, limit, network, since_created_at=None, **_kwargs):
        captured["telegram_id"] = telegram_id
        captured["limit"] = limit
        captured["network"] = network
        captured["since_created_at"] = since_created_at
        return []

    now = datetime(2026, 6, 20, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(db, "get_trades_by_user", _trades)

    report = nh.build_report(7, "mainnet", now_utc=now)

    assert report is not None
    assert captured == {
        "telegram_id": 7,
        "limit": None,
        "network": "mainnet",
        "since_created_at": now - timedelta(hours=24),
    }


# --------------------------------------------------------------------------- #
# backtest-backed comparison (uses the real backtester)                       #
# --------------------------------------------------------------------------- #

def test_compare_configs_ranks_by_net_pnl():
    from src.nadobro.engine.backtester import candles_from_prices
    prices = [100 + 3 * math.sin(i / 3.0) for i in range(80)]
    candles = candles_from_prices(prices, interval_s=3600, wick_pct=Decimal("0.001"))
    base = {"trading_pair": "BTC", "total_amount_quote": Decimal("1000"),
            "start_price": Decimal("97"), "end_price": Decimal("100"),
            "min_spread_between_orders": Decimal("0.01"), "max_open_orders": 5,
            "levels_count": 5, "step_pct": Decimal("0.01"), "leverage": 1,
            "sl_pct": 0.0, "tp_pct": 0.0}
    variants = {"wider step": {"min_spread_between_orders": Decimal("0.02"), "step_pct": Decimal("0.02")}}
    ranked = nh.compare_configs("grid", base, variants, candles)
    assert {r["name"] for r in ranked} == {"current", "wider step"}
    # sorted best-net-first
    assert ranked == sorted(ranked, key=lambda r: r["net_pnl"], reverse=True)
