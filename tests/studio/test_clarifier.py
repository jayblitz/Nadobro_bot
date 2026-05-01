from src.nadobro.studio.clarifier import next_question
from src.nadobro.studio.intent import Quantity, TradingIntent


def test_priority_symbol_before_quantity():
    intent = TradingIntent(action="buy", raw_input="buy")
    assert "market" in next_question(intent).lower()


def test_order_type_question():
    intent = TradingIntent(
        action="buy",
        symbol="BTC",
        quantity=Quantity(type="contracts", value=1),
        raw_input="buy btc limit",
    )
    assert "market, limit" in next_question(intent).lower()


def test_complete_returns_none():
    intent = TradingIntent(
        action="buy",
        symbol="BTC",
        quantity=Quantity(type="contracts", value=1),
        order_type="market",
        leverage=2,
        take_profit={"type": "percent", "value": 2},
        stop_loss={"type": "percent", "value": -1},
        raw_input="complete",
    )
    assert next_question(intent) is None
