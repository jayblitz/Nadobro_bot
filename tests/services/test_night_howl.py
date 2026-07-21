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

from src.nadobro.llm import night_howl_service as nh

NOW = datetime(2026, 6, 20, 12, 0, tzinfo=timezone.utc).timestamp()


def _trades():
    # Venue-confirmed fills (submission_idx + product_id) — the ONLY rows that
    # count now. Realized PnL is derived position-aware from open/close legs,
    # NOT read from the void per-fill ``realized_pnl`` column.
    return [
        # BTC (pid 2): open long then close +$15 -> 1 win.
        {"created_at": NOW - 7200, "product_id": 2, "product_name": "BTC-PERP", "side": "long",
         "fill_size": 0.1, "fill_price": 60000, "fill_fee": 2.0, "submission_idx": 1,
         "source": "grid", "status": "filled"},
        {"created_at": NOW - 3600, "product_id": 2, "product_name": "BTC-PERP", "side": "short",
         "fill_size": 0.1, "fill_price": 60150, "fill_fee": 2.0, "submission_idx": 2,
         "source": "grid", "status": "filled"},
        # ETH (pid 4): open long then close -$10 -> 1 loss.
        {"created_at": NOW - 1800, "product_id": 4, "product_name": "ETH-PERP", "side": "long",
         "fill_size": 1.0, "fill_price": 3000, "fill_fee": 0.5, "submission_idx": 3,
         "source": "manual", "status": "filled"},
        {"created_at": NOW - 900, "product_id": 4, "product_name": "ETH-PERP", "side": "short",
         "fill_size": 1.0, "fill_price": 2990, "fill_fee": 0.5, "submission_idx": 4,
         "source": "manual", "status": "filled"},
        # Outside 24h window (still counts toward entry basis, not the window).
        {"created_at": NOW - 99999, "product_id": 9, "product_name": "OLD-PERP", "side": "long",
         "fill_size": 1, "fill_price": 1, "submission_idx": 5, "status": "filled"},
        # Excluded: failed, and a pending (no submission_idx) row.
        {"created_at": NOW - 100, "product_id": 3, "product_name": "SOL-PERP", "side": "long",
         "fill_size": 1, "fill_price": 150, "submission_idx": 6, "status": "failed"},
        {"created_at": NOW - 50, "product_id": 2, "product_name": "BTC-PERP", "side": "long",
         "fill_size": 0.5, "fill_price": 61000, "status": "filled"},   # no submission_idx
    ]


# --------------------------------------------------------------------------- #
# pattern metrics                                                              #
# --------------------------------------------------------------------------- #

def test_pattern_windows_and_aggregates():
    # Degraded mode (no account_pnl) replays the given rows position-aware.
    p = nh.compute_user_pattern(_trades(), now_ts=NOW)
    assert p["trades"] == 4                       # 4 venue-confirmed in-window; old/failed/pending excluded
    assert p["volume_usd"] == 0.1 * 60000 + 0.1 * 60150 + 1 * 3000 + 1 * 2990
    assert p["fees_usd"] == 5.0
    # Realized: BTC +15, ETH -10 = +5 (derived, not the per-fill column).
    assert abs(p["realized_pnl_usd"] - 5.0) < 1e-6
    assert abs(p["net_pnl_usd"] - 0.0) < 1e-6     # 5 realized - 5 fees
    assert p["wins"] == 1 and p["losses"] == 1
    assert abs(p["win_rate"] - 0.5) < 1e-3
    assert p["top_pairs"][0]["pair"] == "BTC-PERP"


def test_pattern_win_loss_from_account_pnl_window():
    # When account_pnl is supplied (production path), wins/losses/realized come
    # from ITS 24h window, never re-derived from the rows.
    account_pnl = {
        "pnl_windows": {"24h": 42.0, "7d": 100.0, "30d": 100.0, "all": 100.0},
        "wins_windows": {"24h": 3, "7d": 5, "30d": 5, "all": 5},
        "losses_windows": {"24h": 1, "7d": 2, "30d": 2, "all": 2},
    }
    p = nh.compute_user_pattern(_trades(), now_ts=NOW, account_pnl=account_pnl)
    assert p["realized_pnl_usd"] == 42.0
    assert p["wins"] == 3 and p["losses"] == 1
    assert abs(p["win_rate"] - 0.75) < 1e-6


def test_pattern_resolves_pair_names_no_id_zero():
    # A product-less fill (pid 0) never reaches the leaderboard; a stale stored
    # name is superseded by the resolver.
    trades = [
        {"created_at": NOW - 100, "product_id": 2, "product_name": "BTC", "side": "long",
         "fill_size": 0.1, "fill_price": 60000, "submission_idx": 1, "status": "filled"},
        {"created_at": NOW - 90, "product_id": 0, "product_name": "ID:0", "side": "long",
         "fill_size": 0.1, "fill_price": 60000, "submission_idx": 2, "status": "filled"},
    ]
    p = nh.compute_user_pattern(
        trades, now_ts=NOW, resolve_pair=lambda pid, stored: "BTC-PERP" if pid == 2 else f"ID:{pid}"
    )
    pairs = {tp["pair"] for tp in p["top_pairs"]}
    assert pairs == {"BTC-PERP"}                  # ID:0 dropped, BTC canonicalized


def test_pattern_empty_is_safe():
    p = nh.compute_user_pattern([], now_ts=NOW)
    assert p["trades"] == 0 and p["win_rate"] is None and p["top_pairs"] == []


def test_pattern_accepts_iso_and_epoch_timestamps():
    iso = datetime.fromtimestamp(NOW - 600, timezone.utc).isoformat()
    trades = [
        {"created_at": iso, "product_id": 2, "product_name": "BTC", "side": "long",
         "fill_size": 1, "fill_price": 100, "submission_idx": 1, "status": "filled"},
        {"created_at": int((NOW - 600) * 1000), "product_id": 4, "product_name": "ETH", "side": "long",
         "fill_size": 1, "fill_price": 50, "submission_idx": 2, "status": "filled"},
    ]
    assert nh.compute_user_pattern(trades, now_ts=NOW)["trades"] == 2


# --------------------------------------------------------------------------- #
# recommendations                                                             #
# --------------------------------------------------------------------------- #

def test_fees_are_not_double_counted():
    # The recorder stores fill_fee == fees == (fee + builder), so summing
    # fees + fill_fee + builder over-counted ~2.2x. The canonical reader picks
    # ONE (fee_x18 when present) — here fee_x18 = $1.20 for the whole fill.
    trades = [{
        "created_at": NOW - 600, "filled_at": NOW - 600,
        "product_id": 2, "product_name": "BTC-PERP", "side": "long",
        "fill_size": 0.1, "fill_price": 60000, "submission_idx": 1, "status": "filled",
        "fee_x18": str(12 * 10**17),          # $1.20 (authoritative venue fee)
        "fill_fee": 1.20, "fees": 1.20, "builder_fee": 0.20,   # overlapping human cols
    }]
    p = nh.compute_user_pattern(trades, now_ts=NOW)
    assert abs(p["fees_usd"] - 1.20) < 1e-6   # once, from fee_x18 — not 2.60


def test_funding_paid_is_subtracted_from_net():
    # funding_paid is PAID-POSITIVE (a cost); net must SUBTRACT it.
    account_pnl = {
        "pnl_windows": {"24h": 20.0, "7d": 20.0, "30d": 20.0, "all": 20.0},
        "wins_windows": {"24h": 1, "all": 1}, "losses_windows": {"24h": 0, "all": 0},
    }
    trades = [{
        "created_at": NOW - 600, "filled_at": NOW - 600,
        "product_id": 2, "product_name": "BTC-PERP", "side": "long",
        "fill_size": 0.1, "fill_price": 60000, "fill_fee": 2.0, "funding_paid": 5.0,
        "submission_idx": 1, "status": "filled",
    }]
    p = nh.compute_user_pattern(trades, now_ts=NOW, account_pnl=account_pnl)
    # 20 realized - 2 fees - 5 funding = 13 (funding is a cost, subtracted).
    assert abs(p["net_pnl_usd"] - 13.0) < 1e-6
    assert abs(p["funding_usd"] - 5.0) < 1e-6


def test_recommendations_losing_session_is_actionable_not_generic():
    # A losing session must NEVER get the hollow "no changes recommended" line —
    # it gets concrete, quant feedback.
    p = {"trades": 30, "volume_usd": 17955.0, "fees_usd": 4.2, "funding_usd": 0.0,
         "realized_pnl_usd": -30.41, "net_pnl_usd": -34.61, "win_rate": 0.43,
         "wins": 9, "losses": 12, "avg_win_usd": 3.0, "avg_loss_usd": -5.0,
         "payoff_ratio": 0.6, "expectancy_usd": -1.56,
         "top_pairs": [{"pair": "BTC-PERP", "volume_usd": 15000.0}]}
    recs = nh.derive_recommendations(p, None)
    blob = " ".join(recs).lower()
    assert "no changes recommended" not in blob
    assert "expectancy" in blob or "payoff" in blob or "break even" in blob
    assert any("net" in r.lower() for r in recs)


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

    def _trades(telegram_id, *, limit, network, since_created_at=None,
                since_filled_at=None, **_kwargs):
        captured["telegram_id"] = telegram_id
        captured["limit"] = limit
        captured["network"] = network
        captured["since_created_at"] = since_created_at
        captured["since_filled_at"] = since_filled_at
        return []

    now = datetime(2026, 6, 20, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(db, "get_trades_by_user", _trades)
    monkeypatch.setattr(db, "get_account_realized_pnl_windows", lambda *a, **k: {})
    # DB-independent: stub the live-state read the backtest branch would make.
    import src.nadobro.strategy.bot_runtime as br
    monkeypatch.setattr(br, "_load_state", lambda *a, **k: {})

    report = nh.build_report(7, "mainnet", now_utc=now)

    assert report is not None
    # Windows on FILL time (not sync/record time) so the recent-fill set matches
    # the fill-time-bucketed realized-PnL replay.
    assert captured == {
        "telegram_id": 7,
        "limit": None,
        "network": "mainnet",
        "since_created_at": None,
        "since_filled_at": now - timedelta(hours=24),
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
