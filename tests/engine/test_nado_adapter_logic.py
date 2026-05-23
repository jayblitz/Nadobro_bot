"""Unit tests for NadoAdapter order-state reconstruction logic.

These exercise the adapter's parsing/state machine against a lightweight
NadoClient stub that returns the *assumed* venue shapes (the field maps in
adapter/nado.py). They validate the logic; the exact field names are confirmed
on testnet via scripts/capture_nado_shapes.py. No live venue is used.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

from src.nadobro.engine.adapter.base import OrderState
from src.nadobro.engine.adapter.nado import NadoAdapter, ProductMeta
from src.nadobro.engine.types import OrderType, TradeType

PAIR = "KBTC-USDC"
META = {PAIR: ProductMeta(product_id=2, tick_size=Decimal("0.01"),
                          lot_size=Decimal("0.001"), min_notional=Decimal(1))}


class _FakeClient:
    def __init__(self):
        self.open_orders = []
        self.matches = []
        self.cancelled = []

    def place_limit_order(self, product_id, size, price, is_buy=True, post_only=False, reduce_only=False):
        return {"digest": "d1", "status": "open"}

    def place_market_order(self, product_id, size, is_buy=True, reduce_only=False):
        return {"digest": "m1", "status": "filled", "price": 100}

    def get_open_orders(self, product_id, refresh=False, sender=None):
        return list(self.open_orders)

    async def get_matches(self, *, product_ids=None, limit=200, idx=None, max_time=None):
        return list(self.matches)

    async def cancel_orders(self, *, product_id, digests):
        self.cancelled.extend(digests)
        return {"status": "ok"}

    def get_market_price(self, product_id):
        return {"bid": 99.0, "ask": 101.0}


def _adapter():
    return NadoAdapter(_FakeClient(), META)


def test_place_registers_order_and_market_fills():
    async def body():
        a = _adapter()
        o = await a.place_order(PAIR, TradeType.BUY, OrderType.MARKET, Decimal(1))
        assert o.id == "m1" and o.state is OrderState.FILLED
        assert o.id in a._orders

    asyncio.run(body())


def test_order_status_open_when_resting():
    async def body():
        a = _adapter()
        o = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, Decimal(1), Decimal(99))
        a._client.open_orders = [{"digest": "d1", "filled": 0}]
        st = await a.order_status(o.id)
        assert st.id == o.id
        assert st.state is OrderState.OPEN

    asyncio.run(body())


def test_order_status_filled_when_gone_and_matched():
    async def body():
        a = _adapter()
        o = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, Decimal(1), Decimal(99))
        a._client.open_orders = []  # no longer resting
        a._client.matches = [{"digest": "d1", "amount": 1, "price": 99, "fee": "0.1"}]
        st = await a.order_status(o.id)
        assert st.id == o.id
        assert st.state is OrderState.FILLED
        assert st.filled_base == Decimal(1) and st.filled_quote == Decimal(99)
        assert st.fee_quote == Decimal("0.1")

    asyncio.run(body())


def test_order_status_cancelled_when_gone_and_no_fills():
    async def body():
        a = _adapter()
        o = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, Decimal(1), Decimal(99))
        a._client.open_orders = []
        a._client.matches = []
        st = await a.order_status(o.id)
        assert st.id == o.id
        assert st.state is OrderState.CANCELLED

    asyncio.run(body())


def test_cancel_uses_registry_product_and_is_idempotent():
    async def body():
        a = _adapter()
        o = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT_MAKER, Decimal(1), Decimal(99))
        assert await a.cancel_order(o.id) is True
        assert "d1" in a._client.cancelled
        assert await a.cancel_order("unknown") is False  # idempotent

    asyncio.run(body())


def test_mid_price_from_market_price():
    async def body():
        a = _adapter()
        assert await a.mid_price(PAIR) == Decimal(100)

    asyncio.run(body())
