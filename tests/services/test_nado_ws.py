from src.nadobro.services.nado_ws import _should_invalidate, ws_url_for_network


def test_ws_url_for_network():
    assert ws_url_for_network("testnet") == "wss://gateway.test.nado.xyz/v1/ws"
    assert ws_url_for_network("mainnet") == "wss://gateway.prod.nado.xyz/v1/ws"


def test_should_invalidate_portfolio_events():
    assert _should_invalidate({"type": "position_change"})
    assert _should_invalidate({"event": "order_update"})
    assert _should_invalidate({"type": "fill"})
    assert _should_invalidate({"type": "funding_payment"})
    assert _should_invalidate({"payload": {"event": "fill"}})
    assert not _should_invalidate({"type": "heartbeat"})
