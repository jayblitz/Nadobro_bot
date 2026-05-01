from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src.nadobro.services.portfolio_calculator import (
    account_leverage,
    aggregate_trading_stats,
    clamp_margin_usage,
    fill_price,
    funding_payment_label,
    funding_rate_conversions,
    positions_from_account_summary,
    unrealized_pnl_pct,
)
from src.nadobro.utils.x18 import to_x18


def test_positions_from_account_summary_handles_cross_and_isolated_sdk_shapes():
    positions = positions_from_account_summary(
        {
            "cross_positions": [
                {
                    "product_id": 1,
                    "symbol": "BTC",
                    "position_size": Decimal("0.5"),
                    "notional_value": Decimal("50000"),
                    "avg_entry_price": Decimal("90000"),
                    "est_liq_price": None,
                    "est_pnl": Decimal("1000"),
                    "margin_used": Decimal("10000"),
                    "leverage": Decimal("5"),
                    "initial_health": Decimal("2000"),
                    "maintenance_health": Decimal("1500"),
                }
            ],
            "isolated_positions": [
                {
                    "product_id": 2,
                    "symbol": "ETH",
                    "position_size": Decimal("-2"),
                    "notional_value": Decimal("6000"),
                    "net_margin": Decimal("1000"),
                    "leverage": Decimal("6"),
                    "initial_health": Decimal("200"),
                    "maintenance_health": Decimal("100"),
                }
            ],
        }
    )

    assert len(positions) == 2
    assert positions[0].isolated is False
    assert positions[0].is_long is True
    assert positions[0].upnl_pct == Decimal("10.0")
    assert positions[1].isolated is True
    assert positions[1].is_long is False
    assert positions[1].margin_used == Decimal("1000")
    assert positions[1].est_pnl is None


def test_fill_price_includes_fee_and_uses_absolute_values():
    assert fill_price(to_x18("2"), to_x18("-200"), to_x18("1")) == Decimal("99.5")


def test_funding_payment_label_uses_nado_sign_convention():
    paid_amount, paid_label = funding_payment_label({"amount": str(to_x18("3.25"))})
    recv_amount, recv_label = funding_payment_label({"amount": str(to_x18("-1.5"))})

    assert paid_amount == Decimal("3.25")
    assert paid_label == "paid"
    assert recv_amount == Decimal("1.5")
    assert recv_label == "received"


def test_rate_conversions_and_pnl_percent_formulas():
    rates = funding_rate_conversions(Decimal("0.24"))
    assert rates["1h"] == Decimal("0.01")
    assert rates["8h"] == Decimal("0.08")
    assert rates["1y"] == Decimal("87.60")
    assert unrealized_pnl_pct(
        est_pnl=Decimal("50"),
        margin_used=Decimal("1000"),
        notional_value=Decimal("10000"),
        leverage=Decimal("5"),
        isolated=True,
    ) == Decimal("5.00")
    assert unrealized_pnl_pct(
        est_pnl=Decimal("50"),
        margin_used=None,
        notional_value=Decimal("10000"),
        leverage=Decimal("5"),
        isolated=False,
    ) == Decimal("2.500")


def test_account_leverage_and_margin_usage_edges():
    assert account_leverage([Decimal("100"), Decimal("-50")], Decimal("25")) == Decimal("6")
    assert account_leverage([Decimal("100")], Decimal("0")) == Decimal("0")
    assert clamp_margin_usage(Decimal("-0.1")) == Decimal("0")
    assert clamp_margin_usage(Decimal("1.2")) == Decimal("1")
    assert clamp_margin_usage(Decimal("0.4")) == Decimal("0.4")


def test_aggregate_trading_stats_uses_x18_rows_and_windows():
    now = datetime(2026, 1, 2, tzinfo=timezone.utc)
    fills = [
        {
            "quote_filled": str(to_x18("-1000")),
            "fee": str(to_x18("2")),
            "realized_pnl": str(to_x18("10")),
            "filled_at": now - timedelta(hours=1),
        },
        {
            "quote_filled": str(to_x18("500")),
            "fee": str(to_x18("1")),
            "realized_pnl": str(to_x18("-5")),
            "filled_at": now - timedelta(days=10),
        },
    ]
    funding = [{"amount": str(to_x18("3"))}, {"amount": str(to_x18("-1"))}]

    stats = aggregate_trading_stats(fills, funding, now=now)

    assert stats["total_volume"] == Decimal("1500")
    assert stats["volume_windows"]["24h"] == Decimal("1000")
    assert stats["volume_windows"]["7d"] == Decimal("1000")
    assert stats["volume_windows"]["30d"] == Decimal("1500")
    assert stats["total_fees"] == Decimal("3")
    assert stats["total_funding"] == Decimal("2")
    assert stats["total_pnl"] == Decimal("5")
    assert stats["wins"] == 1
    assert stats["losses"] == 1
    assert stats["total_trades"] == 2
    assert stats["win_rate"] == Decimal("50.0")


def test_aggregate_trading_stats_accepts_human_decimal_fallbacks():
    stats = aggregate_trading_stats(
        [
            {
                "submission_idx": "100",
                "quote_filled": "-125.5",
                "fee": "0.25",
                "realized_pnl": "0",
            }
        ],
        [{"amount": "-1.5"}],
    )

    assert stats["total_volume"] == Decimal("125.5")
    assert stats["total_fees"] == Decimal("0.25")
    assert stats["total_funding"] == Decimal("-1.5")
    assert stats["total_trades"] == 1
