"""Phase C: WS-driven order lifecycle store + adapter read-through."""
from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.nadobro.engine import order_lifecycle, order_tags
from src.nadobro.engine.adapter.base import OrderState
from src.nadobro.engine.adapter.nado import NadoAdapter, ProductMeta
from src.nadobro.engine.types import OrderType, TradeType

PAIR = "KBTC-USDC"
META = {PAIR: ProductMeta(product_id=2, tick_size=Decimal("0.01"),
                          lot_size=Decimal("0.001"), min_notional=Decimal(1))}


@pytest.fixture(autouse=True)
def _reset():
    order_lifecycle.clear()
    order_tags.clear()
    yield
    order_lifecycle.clear()
    order_tags.clear()


# -- lifecycle store --------------------------------------------------------

def test_order_update_sets_state_and_bumps_seq():
    order_lifecycle.apply_order_update(digest="0xd", reason="placed")
    e = order_lifecycle.get("0xd")
    assert e.state is OrderState.OPEN and e.seq == 1
    order_lifecycle.apply_order_update(digest="0xd", reason="filled")
    e = order_lifecycle.get("0xd")
    assert e.state is OrderState.FILLED and e.seq == 2


def test_terminal_state_never_regresses():
    order_lifecycle.apply_order_update(digest="0xd", reason="cancelled")
    # A late fill event must not un-cancel the order.
    order_lifecycle.apply_fill(digest="0xd")
    assert order_lifecycle.get("0xd").state is OrderState.CANCELLED


def test_fill_resolves_digest_via_tag():
    tag = order_tags.allocate_tag()
    order_tags.register(tag, trading_pair=PAIR)
    order_tags.bind_digest(tag, "0xfill")
    order_lifecycle.apply_fill(tag=tag)
    e = order_lifecycle.get("0xfill")
    assert e is not None and e.state is OrderState.PARTIALLY_FILLED


def test_is_fresh_respects_ttl():
    order_lifecycle.apply_order_update(digest="0xd", reason="placed")
    e = order_lifecycle.get("0xd")
    assert order_lifecycle.is_fresh("0xd", now=e.last_ws_event_ts + 1)
    assert not order_lifecycle.is_fresh("0xd", ttl=8, now=e.last_ws_event_ts + 9)


def test_seeded_entry_is_not_fresh_until_ws_event():
    # Placement seed must NOT be considered fresh — keeps the no-WS path on REST.
    order_lifecycle.seed("0xseed", state=OrderState.OPEN)
    assert not order_lifecycle.is_fresh("0xseed")


# -- adapter read-through ---------------------------------------------------

class _CountingClient:
    def __init__(self):
        self.open_orders_calls = 0
        self.resting = [{"digest": "lim-1", "filled_base": 0, "price": 100}]

    def place_limit_order(self, product_id, size, price, is_buy=True, post_only=False,
                          reduce_only=False, client_id=None, **kwargs):
        return {"digest": "lim-1", "status": "open"}

    def get_open_orders(self, product_id, refresh=False, sender=None):
        self.open_orders_calls += 1
        return list(self.resting)

    async def get_matches(self, *, product_ids=None, limit=200, idx=None, max_time=None):
        return []


def test_no_ws_event_coalesces_within_tick_then_repolls():
    async def body():
        client = _CountingClient()
        a = NadoAdapter(client, META)
        order = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, Decimal(1), Decimal(100))
        # No WS events. Back-to-back status polls within the coalescing TTL now
        # share ONE get_open_orders fetch (the 429 fix: an N-level ladder made N
        # identical query_orders calls per tick; now it makes one per product).
        await a.order_status(order.id)
        await a.order_status(order.id)
        assert client.open_orders_calls == 1
        # Next tick (snapshot expired) re-polls fresh.
        a._open_orders_snap.clear()
        await a.order_status(order.id)
        assert client.open_orders_calls == 2
    asyncio.run(body())


def test_fresh_unchanged_lifecycle_skips_gateway():
    async def body():
        client = _CountingClient()
        a = NadoAdapter(client, META)
        order = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, Decimal(1), Decimal(100))
        # Simulate the WS "placed" event for this digest → makes it fresh.
        order_lifecycle.apply_order_update(digest=order.id, reason="placed")
        first = await a.order_status(order.id)   # seq advanced → 1 REST poll, caches seq
        assert client.open_orders_calls == 1 and first.state is OrderState.OPEN
        # No further WS events; entry still fresh and seq unchanged → no gateway.
        await a.order_status(order.id)
        await a.order_status(order.id)
        assert client.open_orders_calls == 1
    asyncio.run(body())


def test_multi_level_ladder_coalesces_open_orders_poll():
    """The 429 fix: a multi-level ladder polls order_status once per level per
    tick, each fetching the WHOLE product open-orders list. Those must coalesce to
    ONE get_open_orders call per product per tick (not N)."""
    async def body():
        client = _CountingClient()
        a = NadoAdapter(client, META)
        o1 = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, Decimal(1), Decimal(99))
        o2 = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, Decimal(1), Decimal(98))
        o3 = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, Decimal(1), Decimal(97))
        client.open_orders_calls = 0  # count only the status-poll phase
        # One tick's worth of per-level status polls (same product) -> 1 fetch.
        for oid in (o1.id, o2.id, o3.id):
            await a.order_status(oid)
        assert client.open_orders_calls == 1
    asyncio.run(body())


def test_new_ws_event_forces_repoll():
    async def body():
        client = _CountingClient()
        a = NadoAdapter(client, META)
        order = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, Decimal(1), Decimal(100))
        order_lifecycle.apply_order_update(digest=order.id, reason="placed")
        await a.order_status(order.id)            # poll #1
        assert client.open_orders_calls == 1
        # A fill event bumps the seq → next status must re-poll for true amounts.
        order_lifecycle.apply_fill(digest=order.id)
        await a.order_status(order.id)            # poll #2
        assert client.open_orders_calls == 2
    asyncio.run(body())


def test_terminal_snapshot_never_polls_again():
    async def body():
        client = _CountingClient()
        a = NadoAdapter(client, META)
        order = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, Decimal(1), Decimal(100))
        # Order leaves the book and fully fills (no longer resting + matched).
        client.resting = []
        client.get_matches = _match_full  # type: ignore[assignment]
        order_lifecycle.apply_order_update(digest=order.id, reason="filled")
        filled = await a.order_status(order.id)   # poll #1 → terminal, cached
        assert filled.state is OrderState.FILLED
        calls_after_terminal = client.open_orders_calls
        await a.order_status(order.id)
        await a.order_status(order.id)
        assert client.open_orders_calls == calls_after_terminal  # no more polls
    asyncio.run(body())


async def _match_full(*, product_ids=None, limit=200, idx=None, max_time=None):
    return [{"digest": "lim-1", "amount": 1, "price": 100, "fee": "0.01"}]
