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


def test_no_ws_event_polls_rest_every_call():
    async def body():
        client = _CountingClient()
        a = NadoAdapter(client, META)
        order = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, Decimal(1), Decimal(100))
        # No WS events ⇒ seeded entry isn't fresh ⇒ every status hits the gateway.
        await a.order_status(order.id)
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


# -- cross-process (Redis mirror) -------------------------------------------

class _FakeRedis:
    enabled = True

    def __init__(self):
        self.store: dict = {}

    def set_json(self, key, value, ttl_seconds=None):
        self.store[key] = value
        return True

    def get_json(self, key):
        return self.store.get(key)

    def delete(self, key):
        return self.store.pop(key, None) is not None


def test_worker_reads_main_process_ws_state_via_redis(monkeypatch):
    """Main process WS event mirrors to Redis; a worker (empty local store)
    reads the fresh state and can skip the gateway."""
    fake = _FakeRedis()
    monkeypatch.setattr("src.nadobro.services.upstash_redis.get_redis", lambda: fake)
    monkeypatch.setattr(order_lifecycle, "_REDIS_ENABLED", True)

    # --- "main process": a WS fill+terminal arrives and mirrors to Redis.
    order_lifecycle.apply_order_update(digest="0xX", reason="placed")
    order_lifecycle.apply_order_update(digest="0xX", reason="filled")
    assert "olc:0xX" in fake.store and fake.store["olc:0xX"]["state"] == "FILLED"

    # --- "worker process": no local knowledge of this order.
    order_lifecycle.clear()
    assert order_lifecycle._store.get("0xX") is None  # local empty
    e = order_lifecycle.get("0xX")
    assert e is not None and e.state is OrderState.FILLED and e.fresh is True


def test_redis_seed_is_not_fresh_but_ws_event_is(monkeypatch):
    fake = _FakeRedis()
    monkeypatch.setattr("src.nadobro.services.upstash_redis.get_redis", lambda: fake)
    monkeypatch.setattr(order_lifecycle, "_REDIS_ENABLED", True)

    order_lifecycle.seed("0xY", state=OrderState.OPEN)
    order_lifecycle.clear()  # worker: only Redis remains
    e = order_lifecycle.get("0xY")
    assert e is not None and e.state is OrderState.OPEN and e.fresh is False  # baseline only

    # A real WS event flips it to fresh for the worker.
    order_lifecycle.apply_order_update(digest="0xY", reason="placed")
    order_lifecycle.clear()
    e = order_lifecycle.get("0xY")
    assert e is not None and e.fresh is True
