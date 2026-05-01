from src.nadobro.studio.status import build_status_cards


def test_status_zero_sessions(monkeypatch):
    monkeypatch.setattr("src.nadobro.studio.status.get_active_studio_sessions_for_user", lambda *a, **k: [])
    monkeypatch.setattr("src.nadobro.studio.status.get_active_conditional_orders_for_user", lambda *a, **k: [])
    text, markup = build_status_cards(1)
    assert "No active strategies" in text
    assert markup is None


def test_status_one_live_session(monkeypatch):
    monkeypatch.setattr(
        "src.nadobro.studio.status.get_active_studio_sessions_for_user",
        lambda *a, **k: [
            {
                "id": 123456,
                "network": "mainnet",
                "state": "CONFIRMING",
                "created_at": "2026-01-01T00:00:00+00:00",
                "intent_json": {
                    "action": "buy",
                    "symbol": "BTC",
                    "order_type": "market",
                    "quantity": {"type": "contracts", "value": 1},
                    "leverage": 2,
                    "take_profit": {"value": 2},
                    "stop_loss": {"value": -1},
                },
            }
        ],
    )
    monkeypatch.setattr("src.nadobro.studio.status.get_active_conditional_orders_for_user", lambda *a, **k: [])
    text, _markup = build_status_cards(1)
    assert "BTC" in text
    assert "Auto-close" in text


def test_status_armed_conditional(monkeypatch):
    monkeypatch.setattr("src.nadobro.studio.status.get_active_studio_sessions_for_user", lambda *a, **k: [])
    monkeypatch.setattr(
        "src.nadobro.studio.status.get_active_conditional_orders_for_user",
        lambda *a, **k: [{"id": 99, "network": "testnet", "symbol": "ETH", "conditions_json": [{"indicator": "RSI", "period": 14, "timeframe": "1h", "operator": "<", "value": 30}]}],
    )
    text, _markup = build_status_cards(1)
    assert "armed" in text
    assert "RSI" in text
