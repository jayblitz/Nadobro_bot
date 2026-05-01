from datetime import datetime, timedelta, timezone

import pytest

from src.nadobro.studio.intent import Quantity, TradingIntent


def test_past_time_limit_rejected():
    with pytest.raises(ValueError):
        TradingIntent(action="buy", raw_input="x", time_limit=datetime.now(timezone.utc) - timedelta(minutes=1))


def test_limit_requires_entry_price():
    with pytest.raises(ValueError):
        TradingIntent(action="buy", symbol="BTC", order_type="limit", raw_input="limit btc")


def test_leverage_cap(monkeypatch):
    monkeypatch.setattr("src.nadobro.studio.intent.get_product_max_leverage", lambda *a, **k: 5)
    with pytest.raises(ValueError):
        TradingIntent(
            action="buy",
            symbol="BTC",
            network="mainnet",
            order_type="market",
            quantity=Quantity(type="contracts", value=1),
            leverage=10,
            raw_input="long btc 10x",
        )


def test_conditions_force_conditional():
    intent = TradingIntent(
        action="buy",
        symbol="BTC",
        raw_input="buy rsi",
        conditions=[{"indicator": "rsi", "operator": "<", "value": 30}],
    )
    assert intent.order_type == "conditional"
