"""Phase B: unique-ID tagging + correlation registry for MM orders."""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.nadobro.engine import order_tags
from src.nadobro.engine.adapter.nado import NadoAdapter, ProductMeta
from src.nadobro.engine.types import OrderType, TradeType

PAIR = "KBTC-USDC"
META = {PAIR: ProductMeta(product_id=2, tick_size=Decimal("0.01"),
                          lot_size=Decimal("0.001"), min_notional=Decimal(1))}


@pytest.fixture(autouse=True)
def _reset_tags():
    order_tags.clear()
    yield
    order_tags.clear()


def test_allocate_tag_is_unique_and_20_bit():
    seen = set()
    for _ in range(1000):
        t = order_tags.allocate_tag()
        assert 1 <= t < (1 << 20)
        assert t not in seen
        seen.add(t)


def test_allocate_wraps_without_yielding_zero(monkeypatch):
    # Force the counter near the top of the 20-bit range and confirm wrap skips 0.
    order_tags._counter = (1 << 20) - 1
    assert order_tags.allocate_tag() == 1  # wrapped from max -> 0 -> bumped to 1


def test_nonce_embeds_tag_in_low_20_bits():
    """The mechanism contract: a tag placed via gen_order_nonce(random_int=tag)
    is recoverable from the last 20 bits of the nonce (authoritative, in-digest)."""
    from nado_protocol.utils.nonce import gen_order_nonce

    for tag in (1, 42, 1000, (1 << 20) - 1):
        nonce = gen_order_nonce(random_int=tag)
        assert (nonce & 0xFFFFF) == tag


def test_register_resolve_and_bind_digest():
    tag = order_tags.allocate_tag()
    order_tags.register(tag, trading_pair=PAIR, side="BUY", level=3)
    meta = order_tags.resolve_tag(tag)
    assert meta["trading_pair"] == PAIR and meta["side"] == "BUY" and meta["level"] == 3
    assert meta["tag"] == tag

    order_tags.bind_digest(tag, "0xdigest")
    by_d = order_tags.resolve_digest("0xdigest")
    assert by_d is not None and by_d["tag"] == tag and by_d["level"] == 3


def test_forget_clears_both_directions():
    tag = order_tags.allocate_tag()
    order_tags.register(tag, x=1)
    order_tags.bind_digest(tag, "0xabc")
    order_tags.forget(digest="0xabc")
    assert order_tags.resolve_tag(tag) is None
    assert order_tags.resolve_digest("0xabc") is None


class _FakeClient:
    def __init__(self):
        self.calls = []

    def place_limit_order(self, product_id, size, price, is_buy=True, post_only=False,
                          reduce_only=False, client_id=None, **kwargs):
        self.calls.append(("limit", client_id))
        return {"digest": "lim-1", "status": "open"}

    def place_market_order(self, product_id, size, is_buy=True, reduce_only=False,
                           client_id=None, **kwargs):
        self.calls.append(("market", client_id))
        return {"digest": "mkt-1", "status": "filled", "price": 100}

    async def get_matches(self, *, product_ids=None, limit=200, idx=None, max_time=None):
        return [{"digest": "mkt-1", "amount": 1, "price": 100, "fee": "0"}]


def test_adapter_autotags_and_binds_digest():
    async def body():
        client = _FakeClient()
        a = NadoAdapter(client, META)
        order = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, Decimal(1), Decimal(100))
        # The client received a non-None 20-bit client_id...
        kind, cid = client.calls[-1]
        assert kind == "limit" and cid is not None and 1 <= cid < (1 << 20)
        # ...and the digest resolves back to that tag with the right metadata.
        resolved = order_tags.resolve_digest(order.id)
        assert resolved is not None
        assert resolved["tag"] == cid
        assert resolved["trading_pair"] == PAIR
        assert resolved["side"] == "BUY"
        assert resolved["order_type"] == "LIMIT_MAKER"

    asyncio.run(body())


def test_adapter_forgets_tag_on_rejection():
    class _RejectClient(_FakeClient):
        def place_limit_order(self, *a, client_id=None, **k):
            self.calls.append(("limit", client_id))
            return {"success": False, "error": "rejected"}

    async def body():
        client = _RejectClient()
        a = NadoAdapter(client, META)
        before = order_tags.stats()["tags"]
        with pytest.raises(Exception):
            await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, Decimal(1), Decimal(100))
        # The tag allocated for the rejected order must not leak.
        assert order_tags.stats()["tags"] == before

    asyncio.run(body())
