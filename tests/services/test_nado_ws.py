import json

from src.nadobro.venue.nado_ws import (
    PortfolioWsSubscription,
    _PORTFOLIO_STREAMS,
    _is_auth_or_error,
    _subscription_streams,
    _should_invalidate,
    subscribe_url_for_network,
    ws_url_for_network,
)


def test_subscribe_url_is_the_subscriptions_endpoint():
    # Streams live at /v1/subscribe, NOT the gateway action socket /v1/ws.
    assert subscribe_url_for_network("testnet") == "wss://gateway.test.nado.xyz/v1/subscribe"
    assert subscribe_url_for_network("mainnet") == "wss://gateway.prod.nado.xyz/v1/subscribe"
    # Back-compat alias resolves to the same corrected endpoint.
    assert ws_url_for_network("mainnet") == subscribe_url_for_network("mainnet")


def test_should_invalidate_portfolio_events():
    assert _should_invalidate({"type": "position_change"})
    assert _should_invalidate({"event": "order_update"})
    assert _should_invalidate({"type": "fill"})
    assert _should_invalidate({"type": "funding_payment"})
    assert _should_invalidate({"payload": {"event": "fill"}})
    assert not _should_invalidate({"type": "heartbeat"})


def test_control_frames_are_recognized_and_not_invalidating():
    # authenticate/subscribe acks and error responses must not trigger a sync.
    assert _is_auth_or_error({"method": "authenticate", "result": True})
    assert _is_auth_or_error({"method": "subscribe", "id": 1})
    assert _is_auth_or_error({"error": "bad signature"})
    assert not _is_auth_or_error({"type": "fill"})
    assert not _should_invalidate({"method": "subscribe", "id": 1})


def test_portfolio_streams_are_documented_types():
    valid = {
        "order_update", "trade", "best_bid_offer", "fill", "position_change",
        "book_depth", "liquidation", "latest_candlestick", "funding_payment", "funding_rate",
    }
    # Each entry is (stream_type, requires_auth, per_subaccount).
    names = {name for name, _requires_auth, _per_subaccount in _PORTFOLIO_STREAMS}
    assert names <= valid
    # order_update + fill are the lifecycle streams we need.
    assert "order_update" in names
    assert "fill" in names
    # Documented semantics: order_update is the ONLY auth-required stream;
    # funding_payment is per-product (no subaccount field).
    flags = {name: (auth, per_sub) for name, auth, per_sub in _PORTFOLIO_STREAMS}
    assert flags["order_update"] == (True, True)
    assert flags["fill"] == (False, True)
    assert flags["funding_payment"] == (False, False)
    auth_required = {name for name, auth, _per_sub in _PORTFOLIO_STREAMS if auth}
    assert auth_required == {"order_update"}


def test_fill_nudge_subscription_is_fill_only_without_portfolio_ws():
    sub = PortfolioWsSubscription(
        user_id=7,
        network="mainnet",
        subaccount="0x" + "ab" * 32,
        sync_portfolio=False,
    )
    streams = _subscription_streams(sub)
    assert streams == (("fill", False, True),)
    assert not any(requires_auth for _name, requires_auth, _per_subaccount in streams)


def test_subscribe_message_schema_matches_docs():
    # Reconstruct the exact subscribe frame the client sends and assert shape.
    subaccount = "0x" + "ab" * 32
    for idx, (stream_type, _requires_auth, per_subaccount) in enumerate(
        _PORTFOLIO_STREAMS, start=1
    ):
        stream = {"type": stream_type, "product_id": None}
        # ``subaccount`` only where the stream accepts it — an extra field
        # gets the subscription rejected (funding_payment is per-product).
        if per_subaccount:
            stream["subaccount"] = subaccount
        frame = {"method": "subscribe", "stream": stream, "id": idx}
        # Must be JSON-serializable and use method/stream/id (not type/channels).
        decoded = json.loads(json.dumps(frame))
        assert decoded["method"] == "subscribe"
        assert decoded["stream"]["type"] == stream_type
        if per_subaccount:
            assert decoded["stream"]["subaccount"] == subaccount
        else:
            assert "subaccount" not in decoded["stream"]
        assert "product_id" in decoded["stream"]
        assert isinstance(decoded["id"], int)


def test_route_lifecycle_feeds_order_update_and_fill():
    from src.nadobro.engine import order_lifecycle, order_tags
    from src.nadobro.engine.adapter.base import OrderState
    from src.nadobro.venue.nado_ws import _route_lifecycle

    order_lifecycle.clear()
    order_tags.clear()
    try:
        # order_update carries the digest directly.
        _route_lifecycle({"type": "order_update", "digest": "0xabc", "reason": "placed", "id": 7})
        e = order_lifecycle.get("0xabc")
        assert e is not None and e.state is OrderState.OPEN
        # fill carries only the id (tag) — resolved to the digest via the registry.
        _route_lifecycle({"type": "fill", "id": 7, "filled_qty": "1000"})
        e = order_lifecycle.get("0xabc")
        assert e.state is OrderState.PARTIALLY_FILLED
        # Non-lifecycle events are ignored.
        _route_lifecycle({"type": "position_change", "product_id": 1})
    finally:
        order_lifecycle.clear()
        order_tags.clear()


def test_sign_stream_authentication_message_shape(monkeypatch):
    """The signed authenticate frame uses method=authenticate + id + tx{sender,
    expiration} + signature (ws-v2 frame shape), signed with the
    StreamAuthentication EIP-712 type. expiration is a stringified ms value."""
    from eth_account import Account
    from src.nadobro.venue.nado_client import NadoClient

    subaccount = "0x" + "11" * 32

    class _FakeEngineClient:
        endpoint_addr = "0x" + "ab" * 20
        chain_id = 763373
        linked_signer = Account.create()
        signer = linked_signer

    class _FakeCtx:
        engine_client = _FakeEngineClient()

    class _FakeSDK:
        context = _FakeCtx()

    c = NadoClient.__new__(NadoClient)
    c._initialized = True
    c.client = _FakeSDK()
    c.subaccount_hex = subaccount

    msg = c.sign_stream_authentication(
        expiration_ms=1900000000000, sender=subaccount, auth_id=0
    )
    assert msg["method"] == "authenticate"
    assert msg["id"] == 0
    assert msg["tx"]["sender"] == subaccount
    assert msg["tx"]["expiration"] == "1900000000000"
    assert msg["signature"].startswith("0x")
    assert len(msg["signature"]) == 132  # 65-byte ECDSA sig hex + 0x
