from src.nadobro.studio.execution_bridge import execute_intent
from src.nadobro.studio.intent import Quantity, TradingIntent


def test_market_execution_routes_to_trade_service(monkeypatch):
    monkeypatch.setattr("src.nadobro.studio.execution_bridge._ensure_strategy_session", lambda *a, **k: 123)
    monkeypatch.setattr("src.nadobro.studio.execution_bridge._quantity_to_contracts", lambda *a, **k: 0.1)
    called = {}
    monkeypatch.setattr(
        "src.nadobro.studio.execution_bridge.execute_market_order",
        lambda *args, **kwargs: called.setdefault("result", {"success": True}),
    )
    intent = TradingIntent(
        action="buy",
        symbol="BTC",
        order_type="market",
        quantity=Quantity(type="contracts", value=0.1),
        leverage=2,
        raw_input="long btc",
    )
    assert execute_intent(1, intent)["success"] is True


def test_conditional_persists(monkeypatch):
    monkeypatch.setattr("src.nadobro.studio.execution_bridge._ensure_strategy_session", lambda *a, **k: 123)
    monkeypatch.setattr("src.nadobro.studio.execution_bridge.insert_conditional_order", lambda data: 55)
    intent = TradingIntent(
        action="buy",
        symbol="BTC",
        quantity=Quantity(type="contracts", value=1),
        raw_input="rsi",
        conditions=[{"indicator": "RSI", "operator": "<", "value": 30}],
    )
    result = execute_intent(1, intent, 9)
    assert result["conditional_order_id"] == 55
