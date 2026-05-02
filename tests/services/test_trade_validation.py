from types import SimpleNamespace

from src.nadobro.services import trade_service


class _ReadonlyClient:
    def get_market_price(self, product_id):
        return {"mid": 100.0}

    def get_balance(self):
        raise AssertionError("reduce-only validation should not require opening margin")


def test_reduce_only_validation_skips_opening_margin_check(monkeypatch):
    monkeypatch.setattr(
        trade_service,
        "get_user",
        lambda telegram_id: SimpleNamespace(network_mode=SimpleNamespace(value="mainnet")),
    )
    monkeypatch.setattr(trade_service, "get_user_readonly_client", lambda telegram_id: _ReadonlyClient())
    monkeypatch.setattr(trade_service, "get_product_id", lambda *args, **kwargs: 1)
    monkeypatch.setattr(trade_service, "get_product_max_leverage", lambda *args, **kwargs: 10)
    monkeypatch.setattr(trade_service, "ensure_active_wallet_ready", lambda telegram_id: (True, ""))

    ok, msg = trade_service.validate_trade(
        123,
        "BTC",
        1.0,
        leverage=5,
        enforce_rate_limit=False,
        reduce_only=True,
    )

    assert ok is True
    assert msg == ""
