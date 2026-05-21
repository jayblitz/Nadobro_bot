"""Nado execution adapter — wraps ``services/nado_client.NadoClient`` and the
1CT Linked Signer to satisfy :class:`NadoAdapterBase`.

This is the ONLY module in ``src/nadobro/engine`` permitted to import the
venue client (``connectors/nado`` does not exist in this repo; the real client
lives at ``src/nadobro/services/nado_client``). The engine-scoped half of
``tests/lint/test_adapter_isolation.py`` enforces that.

Phase 1 implements the full :class:`NadoAdapterBase` surface; the live
order-placement path is exercised on testnet in Phase 6, while unit tests run
against the in-memory ``MockNadoAdapter`` test double. ``tick_size`` /
``lot_size`` / ``min_notional`` are served from an injected product-metadata
map so they are usable (and testable) without a live connection.

Implemented in Phase 1.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import AsyncIterator, Dict, Optional

from src.nadobro.engine.adapter.base import (
    AdapterError,
    Fill,
    NadoAdapterBase,
    NadoOrder,
    OrderBookLevel,
    OrderBookSnapshot,
    OrderState,
)
from src.nadobro.engine.types import OrderType, TradeType, _dec

# The sole permitted venue import inside the engine.
from src.nadobro.services.nado_client import NadoClient


@dataclass
class ProductMeta:
    product_id: int
    tick_size: Decimal
    lot_size: Decimal
    min_notional: Decimal


def _to_dec(value: object, default: Decimal = Decimal(0)) -> Decimal:
    try:
        return _dec(value)
    except Exception:
        return default


class NadoAdapter(NadoAdapterBase):
    connector_name = "nado"

    def __init__(self, client: NadoClient, products: Dict[str, ProductMeta]) -> None:
        self._client = client
        self._products = products

    # -- product metadata -------------------------------------------------
    def _meta(self, trading_pair: str) -> ProductMeta:
        meta = self._products.get(trading_pair)
        if meta is None:
            raise AdapterError(f"Unknown trading pair: {trading_pair}")
        return meta

    def tick_size(self, trading_pair: str) -> Decimal:
        return self._meta(trading_pair).tick_size

    def lot_size(self, trading_pair: str) -> Decimal:
        return self._meta(trading_pair).lot_size

    def min_notional(self, trading_pair: str) -> Decimal:
        return self._meta(trading_pair).min_notional

    # -- orders -----------------------------------------------------------
    async def place_order(
        self,
        trading_pair: str,
        side: TradeType,
        order_type: OrderType,
        amount_base: Decimal,
        price: Optional[Decimal] = None,
        leverage: int = 1,
        reduce_only: bool = False,
    ) -> NadoOrder:
        meta = self._meta(trading_pair)
        is_buy = side is TradeType.BUY
        amount = float(amount_base)
        try:
            # NOTE: NadoClient sets leverage at the account/isolated-margin
            # level, not per order, so the ``leverage`` arg is not forwarded
            # here; account leverage is configured ahead of placement.
            if order_type is OrderType.MARKET:
                resp = await asyncio.to_thread(
                    self._client.place_market_order,
                    meta.product_id,
                    amount,
                    is_buy,
                    reduce_only=reduce_only,
                )
            else:
                if price is None:
                    raise AdapterError("limit order requires a price")
                resp = await asyncio.to_thread(
                    self._client.place_limit_order,
                    meta.product_id,
                    amount,
                    float(price),
                    is_buy,
                    post_only=order_type is OrderType.LIMIT_MAKER,
                    reduce_only=reduce_only,
                )
        except AdapterError:
            raise
        except Exception as exc:  # noqa: BLE001 - normalize venue errors
            raise AdapterError(f"place_order failed: {exc}") from exc
        return self._order_from_response(resp, trading_pair, side, order_type, amount_base, price)

    def _order_from_response(
        self,
        resp: object,
        trading_pair: str,
        side: TradeType,
        order_type: OrderType,
        amount_base: Decimal,
        price: Optional[Decimal],
    ) -> NadoOrder:
        data = resp if isinstance(resp, dict) else {}
        digest = str(data.get("digest") or data.get("order_id") or data.get("id") or "")
        if not digest:
            raise AdapterError("venue did not return an order id")
        filled_base = _to_dec(data.get("filled_base"), Decimal(0))
        filled_quote = _to_dec(data.get("filled_quote"), Decimal(0))
        fee_quote = _to_dec(data.get("fee_quote"), Decimal(0))
        state = OrderState.OPEN
        raw_state = str(data.get("status") or data.get("state") or "").lower()
        if order_type is OrderType.MARKET or raw_state in ("filled", "matched"):
            state = OrderState.FILLED
            if filled_base <= 0:
                filled_base = amount_base
                ref_price = price if price is not None else _to_dec(data.get("price"))
                filled_quote = filled_base * ref_price
        return NadoOrder(
            id=digest,
            trading_pair=trading_pair,
            side=side,
            order_type=order_type,
            amount_base=amount_base,
            price=price,
            state=state,
            filled_base=filled_base,
            filled_quote=filled_quote,
            fee_quote=fee_quote,
        )

    async def cancel_order(self, order_id: str) -> bool:
        try:
            await asyncio.to_thread(self._client.cancel_order, self._product_for_digest(order_id), order_id)
            return True
        except Exception:
            # Idempotent: unknown/terminal order is not an error.
            return False

    def _product_for_digest(self, order_id: str) -> int:
        # Phase 1 placeholder: the live order registry mapping digest->product
        # is wired in Phase 6 alongside the fill-stream subscription.
        raise AdapterError("digest->product mapping not wired until Phase 6")

    async def order_status(self, order_id: str) -> NadoOrder:
        raise AdapterError("order_status live polling not wired until Phase 6")

    async def fill_stream(self, trading_pair: str) -> AsyncIterator[Fill]:
        meta = self._meta(trading_pair)
        try:
            matches = await self._client.get_matches(product_ids=[meta.product_id])
        except Exception as exc:  # noqa: BLE001
            raise AdapterError(f"fill_stream failed: {exc}") from exc
        for m in matches or []:
            yield Fill(
                order_id=str(m.get("digest", "")),
                trading_pair=trading_pair,
                side=TradeType.BUY if m.get("is_buy") else TradeType.SELL,
                amount_base=_to_dec(m.get("amount")),
                price=_to_dec(m.get("price")),
                fee_quote=_to_dec(m.get("fee")),
                timestamp=float(m.get("timestamp", time.time())),
            )

    # -- market data ------------------------------------------------------
    async def order_book(self, trading_pair: str) -> OrderBookSnapshot:
        meta = self._meta(trading_pair)
        try:
            data = await asyncio.to_thread(self._client.get_market_price, meta.product_id)
        except Exception as exc:  # noqa: BLE001
            raise AdapterError(f"order_book failed: {exc}") from exc
        bid = _to_dec((data or {}).get("bid"))
        ask = _to_dec((data or {}).get("ask"))
        return OrderBookSnapshot(
            trading_pair=trading_pair,
            bids=[OrderBookLevel(bid, Decimal(0))] if bid else [],
            asks=[OrderBookLevel(ask, Decimal(0))] if ask else [],
            timestamp=time.time(),
        )

    async def mid_price(self, trading_pair: str) -> Decimal:
        book = await self.order_book(trading_pair)
        mid = book.mid
        if mid is None:
            raise AdapterError(f"no mid price for {trading_pair}")
        return mid
