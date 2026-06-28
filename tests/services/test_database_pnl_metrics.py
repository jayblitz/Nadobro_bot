from __future__ import annotations

from datetime import datetime, timezone

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.models import database as db  # noqa: E402


def test_session_live_metrics_realizes_partial_close_with_open_remainder():
    original_query_one = db.query_one
    original_query_all = db.query_all

    def fake_query_one(_sql, _params):
        return {
            "fills": 2,
            "volume": 290,
            "fees": 0,
            "net_base": 1,
            "signed_cash": -110,
        }

    def fake_query_all(_sql, _params):
        ts = datetime(2026, 6, 21, tzinfo=timezone.utc)
        return [
            {"product_id": 2, "side": "long", "fill_size": "2", "fill_price": "100", "filled_at": ts},
            {"product_id": 2, "side": "short", "fill_size": "1", "fill_price": "90", "filled_at": ts},
        ]

    try:
        db.query_one = fake_query_one
        db.query_all = fake_query_all

        metrics = db.get_session_live_metrics(123, "mainnet", user_id=7)
    finally:
        db.query_one = original_query_one
        db.query_all = original_query_all

    assert metrics["net_base"] == 1
    assert metrics["realized_pnl"] == -10


def test_account_realized_pnl_query_excludes_unpairable_productless_rows():
    original_query_all = db.query_all
    captured = {}

    def fake_query_all(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return []

    try:
        db.query_all = fake_query_all

        result = db.get_account_realized_pnl_windows(7, "mainnet")
    finally:
        db.query_all = original_query_all

    assert result["total_pnl"] == 0
    assert "submission_idx IS NOT NULL" in captured["sql"]
    assert "COALESCE(product_id, 0) <> 0" in captured["sql"]
