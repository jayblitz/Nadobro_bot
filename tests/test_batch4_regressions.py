from _stubs import install_test_stubs

install_test_stubs()


def test_update_trade_stats_uses_atomic_increment_sql(monkeypatch):
    from src.nadobro.services import user_service

    captured = {}
    monkeypatch.setattr(user_service, "invalidate_user_cache", lambda _telegram_id: None)
    monkeypatch.setattr(user_service, "record_referred_volume", lambda *args, **kwargs: None, raising=False)

    def _execute(sql, params):
        captured["sql"] = sql
        captured["params"] = params

    monkeypatch.setattr(user_service, "execute", _execute)

    user_service.update_trade_stats(123, 45.5, increment_trade_count=True, network="testnet")

    assert "COALESCE(total_trades, 0) + %s" in captured["sql"]
    assert "COALESCE(total_volume_usd, 0) + %s" in captured["sql"]
    assert "COALESCE(testnet_volume_usd, 0) + %s" in captured["sql"]
    assert captured["params"][0:3] == (1, 45.5, 45.5)


def test_copy_tpsl_refresh_cancels_only_tracked_digests(monkeypatch):
    from src.nadobro.services import copy_service
    import src.nadobro.db as db

    cancelled = []
    executed = []

    class _Client:
        def cancel_order(self, product_id, digest):
            cancelled.append((product_id, digest))

    monkeypatch.setattr(copy_service, "get_user_nado_client", lambda *args, **kwargs: _Client())
    monkeypatch.setattr(
        copy_service,
        "_place_tp_sl_orders",
        lambda *args, **kwargs: {"tp_order_digest": "new-tp", "sl_order_digest": "new-sl"},
    )
    monkeypatch.setattr(db, "execute", lambda sql, params=None: executed.append((sql, params)))

    copy_service._update_tp_sl_if_changed(
        {
            "id": 9,
            "product_name": "BTC-PERP",
            "product_id": 1,
            "size": 0.5,
            "side": "LONG",
            "leverage": 2,
            "tp_price": 120,
            "sl_price": 90,
            "tp_order_digest": "old-tp",
            "sl_order_digest": "old-sl",
        },
        {"tp_price": 125, "sl_price": 85},
        user_id=123,
        network="mainnet",
    )

    assert sorted(cancelled) == [(1, "old-sl"), (1, "old-tp")]
    assert any(params == ("new-tp", "new-sl", 9) for _sql, params in executed)

