from pathlib import Path

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
        "network": "testnet",
        "last_sync": "2026-01-01T00:00:00+00:00",
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


def test_portfolio_deck_and_subviews_render_without_local_sync_text():
    text, kb = render_portfolio_deck(_snapshot())
    assert "Portfolio · TESTNET" in text
    assert "local ledger" not in text
    assert kb.inline_keyboard
    assert render_loading() == "⏳ Loading portfolio…"

    for renderer in (render_positions_view, render_orders_view, render_history_view):
        view_text, view_kb = renderer(_snapshot())
        assert "TESTNET" in view_text
        assert view_kb.inline_keyboard


def test_history_sorts_submission_idx_numerically():
    snapshot = _snapshot()
    snapshot["matches"] = [
        {**snapshot["matches"][0], "submission_idx": "99", "product_name": "OLD"},
        {**snapshot["matches"][0], "submission_idx": "100", "product_name": "NEW"},
    ]

    text, _ = render_history_view(snapshot)

    assert text.index("NEW") < text.index("OLD")


def test_order_cancel_indices_follow_sorted_order():
    snapshot = _snapshot()
    snapshot["open_orders"] = [
        {"product_id": 1, "product_name": "OLD", "created_at": "2026-01-01T00:00:00Z", "digest": "0xold"},
        {"product_id": 1, "product_name": "NEW", "created_at": "2026-01-02T00:00:00Z", "digest": "0xnew"},
    ]

    text, kb = render_orders_view(snapshot)

    assert text.index("NEW") < text.index("OLD")
    assert kb.inline_keyboard[0][0].callback_data == "portfolio:cancel_order:0"


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
