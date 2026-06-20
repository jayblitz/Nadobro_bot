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

    def place_limit_order(self, product_id, size, price, is_buy=True, post_only=False, reduce_only=False, **kwargs):
        return {"digest": "d1", "status": "open"}

    def place_market_order(self, product_id, size, is_buy=True, reduce_only=False, **kwargs):
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


class _CapturingClient(_FakeClient):
    """Captures the isolated-margin kwargs the adapter forwards."""

    def __init__(self):
        super().__init__()
        self.market_calls = []

    def place_market_order(self, product_id, size, is_buy=True, **kwargs):
        self.market_calls.append({"product_id": product_id, "size": size,
                                  "is_buy": is_buy, **kwargs})
        return {"digest": "m1", "status": "filled", "price": 100,
                "filled_base": str(size), "filled_quote": str(size * 100)}


def test_isolated_perp_order_posts_isolated_margin():
    """An order on an isolated-only perp must carry isolated_only=True and a
    safety-buffered isolated_margin (= notional * 1.20 at 1x). RWA perps on Nado
    testnet reject otherwise (error_code 2006). This is the fix that unblocks the
    Delta Neutral short leg."""
    perp_pair = "QQQ-PERP"
    meta = {perp_pair: ProductMeta(product_id=7, tick_size=Decimal("0.01"),
                                   lot_size=Decimal("0.001"), min_notional=Decimal(1),
                                   is_perp=True, isolated_only=True)}

    async def body():
        client = _CapturingClient()
        a = NadoAdapter(client, meta)
        # SELL 0.5 base; ref price falls back to mid = (99+101)/2 = 100.
        await a.place_order(perp_pair, TradeType.SELL, OrderType.MARKET, Decimal("0.5"))
        assert len(client.market_calls) == 1
        call = client.market_calls[0]
        assert call["isolated_only"] is True
        # 0.5 * 100 / 1 * 1.20 = 60.0
        assert abs(float(call["isolated_margin"]) - 60.0) < 1e-6

    asyncio.run(body())


def test_funding_since_sums_received_positive():
    """funding_since returns net funding RECEIVED (positive), filtered to the
    product and timestamp window. The indexer signs funding positive = paid, so
    a row of -2.0 paid (i.e. received) becomes +2.0 received."""
    perp_pair = "QQQ-PERP"
    meta = {perp_pair: ProductMeta(product_id=7, tick_size=Decimal("0.01"),
                                   lot_size=Decimal("0.001"), min_notional=Decimal(1),
                                   is_perp=True, isolated_only=True)}

    class _FundingClient(_FakeClient):
        async def get_interest_and_funding_payments(self, *, product_ids=None, limit=200, idx=None):
            return [
                # received 2.0 (paid = -2.0), in-window, this product
                {"type": "funding", "product_id": 7, "amount": "-2.0", "timestamp": 2000},
                # paid 0.5 (cost), in-window
                {"type": "funding", "product_id": 7, "amount": "0.5", "timestamp": 2500},
                # before since_ts → excluded
                {"type": "funding", "product_id": 7, "amount": "-9.0", "timestamp": 500},
                # different product → excluded
                {"type": "funding", "product_id": 99, "amount": "-9.0", "timestamp": 3000},
                # interest, not funding → excluded
                {"type": "interest", "product_id": 7, "amount": "-9.0", "timestamp": 3000},
            ]

    async def body():
        a = NadoAdapter(_FundingClient(), meta)
        net = await a.funding_since(perp_pair, since_ts=1000)
        # received 2.0 - paid 0.5 = net received 1.5
        assert net == Decimal("1.5")

    asyncio.run(body())


def test_funding_since_excludes_undated_rows():
    """DN-FUNDING-WINDOW fix: a funding row with no parseable timestamp must NOT
    be summed (it used to leak in regardless of the run window, overstating
    funding earned). Only the in-window, dated row counts here."""
    perp_pair = "QQQ-PERP"
    meta = {perp_pair: ProductMeta(product_id=7, tick_size=Decimal("0.01"),
                                   lot_size=Decimal("0.001"), min_notional=Decimal(1),
                                   is_perp=True, isolated_only=True)}

    class _FundingClient(_FakeClient):
        async def get_interest_and_funding_payments(self, *, product_ids=None, limit=200, idx=None):
            return [
                {"type": "funding", "product_id": 7, "amount": "-2.0", "timestamp": 2000},
                # no timestamp / paid_at → undated → must be excluded
                {"type": "funding", "product_id": 7, "amount": "-50.0"},
            ]

    async def body():
        a = NadoAdapter(_FundingClient(), meta)
        net = await a.funding_since(perp_pair, since_ts=1000)
        assert net == Decimal("2.0")  # the undated -50 is NOT leaked in

    asyncio.run(body())


def test_reduce_only_stripped_on_spot_orders():
    """reduce_only is a perp concept; Nado rejects a reduce-only SPOT order with
    error_code 5000 'Invalid value' (this broke the DN spot leg's close). The
    adapter must strip it for non-perp products and keep it for perps."""
    spot = {"S": ProductMeta(2, Decimal("0.01"), Decimal("0.001"), Decimal(1),
                             is_perp=False, isolated_only=False)}
    perp = {"P": ProductMeta(7, Decimal("0.01"), Decimal("0.001"), Decimal(1),
                             is_perp=True, isolated_only=True)}

    async def body():
        c1 = _CapturingClient()
        await NadoAdapter(c1, spot).place_order(
            "S", TradeType.SELL, OrderType.MARKET, Decimal("0.5"), reduce_only=True)
        assert c1.market_calls[0]["reduce_only"] is False  # stripped for spot

        c2 = _CapturingClient()
        await NadoAdapter(c2, perp).place_order(
            "P", TradeType.SELL, OrderType.MARKET, Decimal("0.5"), reduce_only=True)
        assert c2.market_calls[0]["reduce_only"] is True   # kept for perp

    asyncio.run(body())


def test_spot_order_is_not_isolated():
    """A spot (or cross) product must NOT post isolated margin — that path is
    unchanged."""
    async def body():
        client = _CapturingClient()
        a = NadoAdapter(client, META)  # META[PAIR] is not isolated
        await a.place_order(PAIR, TradeType.BUY, OrderType.MARKET, Decimal("0.5"))
        call = client.market_calls[0]
        assert call["isolated_only"] is False
        assert call["isolated_margin"] is None

    asyncio.run(body())


def test_place_registers_order_and_market_fills():
    async def body():
        a = _adapter()
        # BUG-NA-2: venue may report "filled" without inline fill data.
        # The adapter must follow up with a matches query, NOT synthesize.
        a._client.matches = [{"digest": "m1", "amount": 1, "price": 100, "fee": "0.05"}]
        o = await a.place_order(PAIR, TradeType.BUY, OrderType.MARKET, Decimal(1))
        assert o.id == "m1" and o.state is OrderState.FILLED
        assert o.filled_base == Decimal(1)
        assert o.filled_quote == Decimal(100)
        assert o.fee_quote == Decimal("0.05")
        assert o.id in a._orders

    asyncio.run(body())


def test_market_fill_converts_x18_match_amounts_to_human():
    """The Nado indexer returns match fills x18-scaled (base_filled / quote_filled).
    The adapter MUST convert to human units — reading them raw recorded a
    0.128-SPY fill as 128000000000000000, which made the DN short un-placeable
    (base-matched off an x18 fill) and the long un-closeable (sold 1e18× the
    held size → venue 'Invalid value'). Regression for that root cause.
    """
    async def body():
        a = _adapter()
        # 0.128 base, 98.13 quote, 0.05 fee — all x18-scaled as the indexer sends.
        a._client.matches = [{
            "digest": "m1",
            "base_filled": "128000000000000000",       # 0.128
            "quote_filled": "98130000000000000000",     # 98.13
            "fee": "50000000000000000",                 # 0.05
        }]
        o = await a.place_order(PAIR, TradeType.BUY, OrderType.MARKET, Decimal("0.128"))
        assert o.state is OrderState.FILLED
        assert o.filled_base == Decimal("0.128")        # NOT 1.28e17
        assert o.filled_quote == Decimal("98.13")
        assert o.fee_quote == Decimal("0.05")

    asyncio.run(body())


def test_place_market_with_no_inline_or_match_data_downgrades_to_partial():
    """BUG-NA-2 fix: when venue reports FILLED but no match data exists yet,
    the adapter must NOT mark FILLED with synthesized base/quote. It must
    downgrade to PARTIALLY_FILLED with zeros so the executor keeps polling.
    """
    async def body():
        a = _adapter()
        a._client.matches = []  # archive hasn't indexed the fill yet
        o = await a.place_order(PAIR, TradeType.BUY, OrderType.MARKET, Decimal(1))
        assert o.state is OrderState.PARTIALLY_FILLED
        assert o.filled_base == Decimal(0)
        assert o.filled_quote == Decimal(0)

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
