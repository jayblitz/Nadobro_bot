from types import SimpleNamespace

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import order_intents, trade_service


def test_build_intent_id_uses_manual_nonce_to_allow_repeated_orders():
    base = {
        "user_id": 1,
        "network": "mainnet",
        "strategy_session_id": None,
        "source": "manual",
        "product": "BTC",
        "side": "long",
        "size": 1.0,
    }

    first = order_intents.build_intent_id(**base, order_nonce="manual-a")
    second = order_intents.build_intent_id(**base, order_nonce="manual-b")

    assert first != second


def test_reserve_order_intent_blocks_active_existing_intent(monkeypatch):
    monkeypatch.setattr(order_intents, "reserve_order_intent_row", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        order_intents,
        "get_order_intent_row",
        lambda _intent_id: {
            "intent_id": "intent-1",
            "status": "pending",
            "updated_at_ts": 9999999999.0,
        },
    )

    reserved, payload = order_intents.reserve_order_intent("intent-1", {"user_id": 1})

    assert reserved is False
    assert payload["status"] == "pending"


def test_market_order_duplicate_pending_intent_does_not_insert_trade(monkeypatch):
    captured = {}

    monkeypatch.setattr(trade_service, "_builder_route_payload", lambda: {})
    monkeypatch.setattr(trade_service, "validate_trade", lambda *args, **kwargs: (True, ""))
    monkeypatch.setattr(
        trade_service,
        "get_user",
        lambda _telegram_id: SimpleNamespace(network_mode=SimpleNamespace(value="mainnet")),
    )
    monkeypatch.setattr(trade_service, "get_product_id", lambda *args, **kwargs: 1)
    monkeypatch.setattr(trade_service, "get_user_nado_client", lambda _telegram_id: object())
    monkeypatch.setattr(
        trade_service,
        "insert_trade",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("duplicate intent must not insert a trade")),
    )

    def _reserve(intent_id, data, *args, **kwargs):
        captured["intent_id"] = intent_id
        captured["data"] = data
        return False, {"intent_id": intent_id, "status": "pending"}

    monkeypatch.setattr(order_intents, "reserve_order_intent", _reserve)

    result = trade_service.execute_market_order(
        telegram_id=1,
        product="BTC",
        size=1.0,
        is_long=True,
        source="manual",
    )

    assert result["success"] is False
    assert result["duplicate"] is True
    assert captured["data"]["order_nonce"].startswith("manual:")
