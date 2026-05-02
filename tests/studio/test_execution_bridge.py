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


def test_limit_short_percent_tp_sl_are_inverted(monkeypatch):
    monkeypatch.setattr("src.nadobro.studio.execution_bridge._ensure_strategy_session", lambda *a, **k: 123)
    monkeypatch.setattr("src.nadobro.studio.execution_bridge._quantity_to_contracts", lambda *a, **k: 0.1)
    captured = {}

    def fake_limit(*args, **kwargs):
        captured.update(kwargs)
        return {"success": True}

    monkeypatch.setattr("src.nadobro.studio.execution_bridge.execute_limit_order", fake_limit)
    intent = TradingIntent(
        action="sell",
        symbol="BTC",
        order_type="limit",
        entry_price=100.0,
        quantity=Quantity(type="contracts", value=0.1),
        leverage=2,
        take_profit={"type": "percent", "value": 2},
        stop_loss={"type": "percent", "value": 1},
        raw_input="short btc",
    )

    assert execute_intent(1, intent)["success"] is True
    assert captured["tp_price"] == 98.0
    assert captured["sl_price"] == 101.0
