from pathlib import Path
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.handlers.history_view import render_history_view
from src.nadobro.handlers.orders_view import render_orders_view
from src.nadobro.handlers.performance_view import ensure_session_card_template, generate_session_card
from src.nadobro.handlers.portfolio_deck import render_loading, render_portfolio_deck
from src.nadobro.handlers.positions_view import render_positions_view
from src.nadobro.utils.x18 import to_x18


def _snapshot():
    return {
        "user_id": 42,
        "network": "testnet",
        "last_sync": "2026-01-01T00:00:00+00:00",
        "equity": {"spot": "1000", "cross": "500", "isolated": "0", "total": "1500"},
        "positions": [
            {
                "product_id": 1,
                "symbol": "BTC",
                "isolated": False,
                "is_long": True,
                "amount": "1",
                "notional_value": "100000",
                "avg_entry_price": "90000",
                "est_pnl": "100",
                "est_liq_price": None,
                "margin_used": "10000",
                "leverage": "10",
                "upnl_pct": "1",
            }
        ],
        "open_orders": [
            {"product_id": 1, "product_name": "BTC", "side": "LONG", "amount": "1", "price": "100000", "digest": "0xabc"}
        ],
        "matches": [
            {
                "submission_idx": "1",
                "product_id": 1,
                "product_name": "BTC",
                "base_filled": str(to_x18("1")),
                "quote_filled": str(to_x18("-100")),
                "fee": str(to_x18("1")),
                "timestamp": "2026-01-01T00:00:00Z",
            }
        ],
        "stats": {
            "volume_windows": {"24h": "100", "7d": "200", "30d": "300", "all": "400"},
            "total_fees": "2",
            "total_funding": "-1",
            "total_pnl": "5",
            "total_trades": 2,
            "win_rate": "50",
        },
    }


def test_portfolio_deck_shows_trigger_order_type():
    snapshot = _snapshot()
    snapshot["open_orders"] = [
        {
            "product_id": 1,
            "product_name": "BTC",
            "side": "LONG",
            "price": "90000",
            "type": "STOP",
            "is_trigger": True,
            "digest": "0xtrg",
        }
    ]
    text, _ = render_portfolio_deck(snapshot)
    assert "⚡ STOP" in text


def test_portfolio_deck_and_subviews_render_without_local_sync_text():
    text, kb = render_portfolio_deck(_snapshot())
    assert "Portfolio · TESTNET" in text
    assert "local ledger" not in text
    assert "Total Balance" in text
    assert kb.inline_keyboard
    assert render_loading() == "⏳ Loading portfolio…"

    for renderer in (render_positions_view, render_orders_view):
        view_text, view_kb = renderer(_snapshot())
        assert "TESTNET" in view_text
        assert view_kb.inline_keyboard

    # History view pulls round-trips from the DB; stub them out for the
    # smoke test so the renderer still produces a valid card.
    with patch("src.nadobro.services.trade_service.compute_round_trips", return_value=[]):
        view_text, view_kb = render_history_view(_snapshot())
    assert "TESTNET" in view_text
    assert view_kb.inline_keyboard


def test_history_renders_round_trips_newest_first():
    """History now displays round-trips computed from manual fills.

    The renderer must surface ``trip_key`` on the Share PnL button so the
    callback can mint the per-trade card.
    """
    from datetime import datetime, timezone

    round_trips = [
        {
            "trip_key": "200",
            "pair": "NEW",
            "side": "long",
            "size": 1.0,
            "avg_open_price": 100.0,
            "avg_close_price": 110.0,
            "realized_pnl": 10.0,
            "fees": 0.2,
            "funding_paid": 0.0,
            "volume_usd": 210.0,
            "open_ts": datetime(2026, 1, 2, tzinfo=timezone.utc),
            "close_ts": datetime(2026, 1, 2, 1, tzinfo=timezone.utc),
        },
        {
            "trip_key": "100",
            "pair": "OLD",
            "side": "short",
            "size": 0.5,
            "avg_open_price": 90.0,
            "avg_close_price": 80.0,
            "realized_pnl": 5.0,
            "fees": 0.1,
            "funding_paid": 0.0,
            "volume_usd": 85.0,
            "open_ts": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "close_ts": datetime(2026, 1, 1, 1, tzinfo=timezone.utc),
        },
    ]
    with patch(
        "src.nadobro.services.trade_service.compute_round_trips",
        return_value=round_trips,
    ):
        text, kb = render_history_view(_snapshot())

    assert text.index("NEW") < text.index("OLD")
    callback_data = [
        btn.callback_data for row in kb.inline_keyboard for btn in row
    ]
    assert any("portfolio:share_pnl:rt:200" in cb for cb in callback_data)


def test_order_cancel_indices_follow_sorted_order():
    snapshot = _snapshot()
    snapshot["open_orders"] = [
        {"product_id": 1, "product_name": "OLD", "created_at": "2026-01-01T00:00:00Z", "digest": "0xold"},
        {"product_id": 1, "product_name": "NEW", "created_at": "2026-01-02T00:00:00Z", "digest": "0xnew"},
    ]

    text, kb = render_orders_view(snapshot)

    assert text.index("NEW") < text.index("OLD")
    assert kb.inline_keyboard[0][0].callback_data == "portfolio:cancel_order:0"


def test_positions_cancel_indices_paginate_correctly():
    snapshot = _snapshot()
    snapshot["open_orders"] = [
        {
            "product_id": 1,
            "product_name": f"ORD{i}",
            "created_at": f"2026-01-{i:02d}T00:00:00Z",
            "digest": f"0x{i}",
        }
        for i in range(1, 8)
    ]

    _, kb = render_positions_view(snapshot, ord_page=1, page_size=6)

    cancel_callbacks = [
        btn.callback_data
        for row in kb.inline_keyboard
        for btn in row
        if btn.callback_data and btn.callback_data.startswith("portfolio:cancel_order:")
    ]
    assert cancel_callbacks == ["portfolio:cancel_order:4", "portfolio:cancel_order:5", "portfolio:cancel_order:6"]


def test_session_card_template_and_card_generation(tmp_path):
    template = ensure_session_card_template(tmp_path / "template.png")
    assert template.exists()
    out = generate_session_card(
        {
            "strategy_label": "Test Strategy",
            "realized_pnl": "12.5",
            "fees": "1",
            "funding": "-0.5",
            "volume": "1000",
            "win_count": 2,
            "loss_count": 1,
        },
        "testnet",
        tmp_path / "card.png",
    )
    assert Path(out).exists()
    assert Path(out).stat().st_size > 0
