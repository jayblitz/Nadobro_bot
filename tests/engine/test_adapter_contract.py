"""Adapter contract suite: MockNadoAdapter must satisfy the behavioral
contract, and NadoAdapter must conform to NadoAdapterBase (interface)."""
from __future__ import annotations

import asyncio
import inspect
from decimal import Decimal

from src.nadobro.engine.adapter.base import NadoAdapterBase, NadoOrder
from src.nadobro.engine.adapter.nado import NadoAdapter, ProductMeta
from src.nadobro.engine.types import OrderType, TradeType
from tests.engine._mock_nado import MockNadoAdapter

PAIR = "SOL-USDC"


def test_mock_place_returns_id():
    async def body():
        a = MockNadoAdapter(mid=Decimal(100))
        order = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT, Decimal(1), Decimal(99))
        assert isinstance(order, NadoOrder)
        assert order.id

    asyncio.run(body())


def test_mock_cancel_is_idempotent():
    async def body():
        a = MockNadoAdapter(mid=Decimal(100))
        order = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT, Decimal(1), Decimal(99))
        assert await a.cancel_order(order.id) is True
        assert await a.cancel_order(order.id) is False  # already cancelled
        assert await a.cancel_order("does-not-exist") is False

    asyncio.run(body())


def test_mock_fill_stream_yields_in_order():
    async def body():
        a = MockNadoAdapter(mid=Decimal(100))
        o1 = await a.place_order(PAIR, TradeType.BUY, OrderType.LIMIT, Decimal(1), Decimal(100))
        a.fill_order(o1.id, price=Decimal(100))
        o2 = await a.place_order(PAIR, TradeType.SELL, OrderType.LIMIT, Decimal(1), Decimal(101))
        a.fill_order(o2.id, price=Decimal(101))
        fills = [f async for f in a.fill_stream(PAIR)]
        assert [f.price for f in fills] == [Decimal(100), Decimal(101)]

    asyncio.run(body())


def test_mock_meta_returns_decimals():
    a = MockNadoAdapter()
    assert isinstance(a.tick_size(PAIR), Decimal)
    assert isinstance(a.lot_size(PAIR), Decimal)
    assert isinstance(a.min_notional(PAIR), Decimal)


def test_mock_mid_price_tape_advances():
    async def body():
        a = MockNadoAdapter(mids=[Decimal(100), Decimal(101), Decimal(102)])
        assert await a.mid_price(PAIR) == Decimal(100)
        assert await a.mid_price(PAIR) == Decimal(101)
        assert await a.mid_price(PAIR) == Decimal(102)
        assert await a.mid_price(PAIR) == Decimal(102)  # clamps at last

    asyncio.run(body())


def test_nado_adapter_is_concrete_and_conforms():
    assert issubclass(NadoAdapter, NadoAdapterBase)
    assert not inspect.isabstract(NadoAdapter)
    for name in NadoAdapterBase.__abstractmethods__:
        assert getattr(NadoAdapter, name) is not getattr(NadoAdapterBase, name)


def test_nado_adapter_meta_without_live_client():
    a = NadoAdapter(
        client=object(),
        products={PAIR: ProductMeta(2, Decimal("0.01"), Decimal("0.001"), Decimal(5))},
    )
    assert a.tick_size(PAIR) == Decimal("0.01")
    assert a.lot_size(PAIR) == Decimal("0.001")
    assert a.min_notional(PAIR) == Decimal(5)
