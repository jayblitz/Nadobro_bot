"""Nado execution adapter — wraps ``services/nado_client.NadoClient`` and the
1CT Linked Signer to satisfy :class:`NadoAdapterBase`.

This is the ONLY module in ``src/nadobro/engine`` permitted to import the
venue client (``connectors/nado`` does not exist in this repo; the real client
lives at ``src/nadobro/services/nado_client``). The engine-scoped half of
``tests/lint/test_adapter_isolation.py`` enforces that.

Order state is reconstructed from the real client surface:
``get_open_orders`` (still resting?) + ``get_matches`` (fills) for
``order_status``; ``cancel_orders`` for cancellation; an internal digest->order
registry (filled at placement) resolves the product for a given order id.

Response field names are read defensively via the ``_*_KEYS`` maps below — run
``scripts/capture_nado_shapes.py`` on testnet and adjust those maps to the
exact venue shapes, then validate end-to-end (Phase 3/6 runbooks).

Implemented in Phase 1; live mapping completed in production hardening (B).
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, AsyncIterator, Dict, Optional, Sequence

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

# --- venue response field maps (confirm via scripts/capture_nado_shapes.py) --
_DIGEST_KEYS = ("digest", "order_digest", "order_id", "id")
_OPEN_FILLED_KEYS = ("filled", "filled_size", "cum_filled_size", "executed_size", "filled_base")
_PRICE_KEYS = ("price", "limit_price", "fill_price", "exec_price")
_MATCH_AMOUNT_KEYS = ("amount", "size", "base_filled", "filled_size", "filled_base")
_MATCH_FEE_KEYS = ("fee", "fee_quote", "fee_usd", "fee_amount")
_BID_KEYS = ("bid", "best_bid", "bid_price")
_ASK_KEYS = ("ask", "best_ask", "ask_price")
_MID_KEYS = ("mid", "mid_price", "mark", "mark_price", "price")
_OPEN_LIST_KEYS = ("orders", "open_orders", "data", "result")


@dataclass
class ProductMeta:
    product_id: int
    tick_size: Decimal
    lot_size: Decimal
    min_notional: Decimal


@dataclass
class _OrderRef:
    trading_pair: str
    product_id: int
    side: TradeType
    order_type: OrderType
    amount_base: Decimal
    price: Optional[Decimal]


def _to_dec(value: object, default: Decimal = Decimal(0)) -> Decimal:
    try:
        return _dec(value)
    except Exception:
        return default


def _first(d: Dict[str, Any], keys: Sequence[str], default: object = None) -> object:
    for k in keys:
        if isinstance(d, dict) and d.get(k) is not None:
            return d[k]
    return default


def _as_list(resp: object) -> list:
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        for k in _OPEN_LIST_KEYS:
            v = resp.get(k)
            if isinstance(v, list):
                return v
    return []


class NadoAdapter(NadoAdapterBase):
    connector_name = "nado"

    def __init__(self, client: NadoClient, products: Dict[str, ProductMeta]) -> None:
        self._client = client
        self._products = products
        self._orders: Dict[str, _OrderRef] = {}

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
            # NadoClient sets leverage at the account/isolated-margin level,
            # not per order, so ``leverage`` is configured ahead of placement.
            if order_type is OrderType.MARKET:
                resp = await asyncio.to_thread(
                    self._client.place_market_order, meta.product_id, amount, is_buy,
                    reduce_only=reduce_only,
                )
            else:
                if price is None:
                    raise AdapterError("limit order requires a price")
                resp = await asyncio.to_thread(
                    self._client.place_limit_order, meta.product_id, amount, float(price), is_buy,
                    post_only=order_type is OrderType.LIMIT_MAKER, reduce_only=reduce_only,
                )
        except AdapterError:
            raise
        except Exception as exc:  # noqa: BLE001 - normalize venue errors
            raise AdapterError(f"place_order failed: {exc}") from exc
        order = self._order_from_response(resp, trading_pair, side, order_type, amount_base, price)
        self._orders[order.id] = _OrderRef(
            trading_pair, meta.product_id, side, order_type, amount_base, price
        )
        return order

    def _order_from_response(
        self, resp: object, trading_pair: str, side: TradeType, order_type: OrderType,
        amount_base: Decimal, price: Optional[Decimal],
    ) -> NadoOrder:
        data = resp if isinstance(resp, dict) else {}
        digest = str(_first(data, _DIGEST_KEYS, "") or "")
        if not digest:
            raise AdapterError("venue did not return an order id")
        filled_base = _to_dec(data.get("filled_base"))
        filled_quote = _to_dec(data.get("filled_quote"))
        fee_quote = _to_dec(_first(data, _MATCH_FEE_KEYS))
        state = OrderState.OPEN
        raw_state = str(_first(data, ("status", "state"), "") or "").lower()
        if order_type is OrderType.MARKET or raw_state in ("filled", "matched"):
            state = OrderState.FILLED
            if filled_base <= 0:
                filled_base = amount_base
                ref_price = price if price is not None else _to_dec(_first(data, _PRICE_KEYS))
                filled_quote = filled_base * ref_price
        return NadoOrder(
            id=digest, trading_pair=trading_pair, side=side, order_type=order_type,
            amount_base=amount_base, price=price, state=state,
            filled_base=filled_base, filled_quote=filled_quote, fee_quote=fee_quote,
        )

    async def cancel_order(self, order_id: str) -> bool:
        ref = self._orders.get(order_id)
        if ref is None:
            return False
        try:
            await self._client.cancel_orders(product_id=ref.product_id, digests=[order_id])
            return True
        except Exception:
            # Idempotent: unknown/terminal order is not an error.
            return False

    async def order_status(self, order_id: str) -> NadoOrder:
        ref = self._orders.get(order_id)
        if ref is None:
            raise AdapterError(f"unknown order id: {order_id}")
        try:
            open_orders = await asyncio.to_thread(self._client.get_open_orders, ref.product_id, True)
        except Exception as exc:  # noqa: BLE001
            raise AdapterError(f"order_status failed: {exc}") from exc

        resting = self._find_open(open_orders, order_id)
        if resting is not None:
            filled_base = _to_dec(_first(resting, _OPEN_FILLED_KEYS))
            px = ref.price if ref.price is not None else _to_dec(_first(resting, _PRICE_KEYS))
            state = OrderState.PARTIALLY_FILLED if filled_base > 0 else OrderState.OPEN
            return self._mk_order(ref, state, filled_base, filled_base * px, Decimal(0))

        # No longer resting -> aggregate fills for this digest.
        filled_base, filled_quote, fee = await self._fills_for(ref.product_id, order_id)
        if filled_base >= ref.amount_base * Decimal("0.999"):
            state = OrderState.FILLED
        elif filled_base > 0:
            state = OrderState.PARTIALLY_FILLED  # gone from book but partially done
        else:
            state = OrderState.CANCELLED
        return self._mk_order(ref, state, filled_base, filled_quote, fee)

    def _mk_order(
        self, ref: _OrderRef, state: OrderState, filled_base: Decimal,
        filled_quote: Decimal, fee: Decimal,
    ) -> NadoOrder:
        return NadoOrder(
            id="", trading_pair=ref.trading_pair, side=ref.side, order_type=ref.order_type,
            amount_base=ref.amount_base, price=ref.price, state=state,
            filled_base=filled_base, filled_quote=filled_quote, fee_quote=fee,
        )

    @staticmethod
    def _find_open(open_orders: object, digest: str) -> Optional[Dict[str, Any]]:
        for o in _as_list(open_orders):
            if isinstance(o, dict) and str(_first(o, _DIGEST_KEYS, "")) == digest:
                return o
        return None

    async def _fills_for(self, product_id: int, digest: str) -> tuple[Decimal, Decimal, Decimal]:
        try:
            matches = await self._client.get_matches(product_ids=[product_id])
        except Exception as exc:  # noqa: BLE001
            raise AdapterError(f"order_status fills failed: {exc}") from exc
        fb = fq = fee = Decimal(0)
        for m in matches or []:
            if str(_first(m, _DIGEST_KEYS, "")) != digest:
                continue
            amt = _to_dec(_first(m, _MATCH_AMOUNT_KEYS))
            px = _to_dec(_first(m, _PRICE_KEYS))
            fb += amt
            fq += amt * px
            fee += _to_dec(_first(m, _MATCH_FEE_KEYS))
        return fb, fq, fee

    async def fill_stream(self, trading_pair: str) -> AsyncIterator[Fill]:
        meta = self._meta(trading_pair)
        try:
            matches = await self._client.get_matches(product_ids=[meta.product_id])
        except Exception as exc:  # noqa: BLE001
            raise AdapterError(f"fill_stream failed: {exc}") from exc
        for m in matches or []:
            amt = _to_dec(_first(m, _MATCH_AMOUNT_KEYS))
            yield Fill(
                order_id=str(_first(m, _DIGEST_KEYS, "") or ""),
                trading_pair=trading_pair,
                side=TradeType.BUY if m.get("is_buy") else TradeType.SELL,
                amount_base=amt,
                price=_to_dec(_first(m, _PRICE_KEYS)),
                fee_quote=_to_dec(_first(m, _MATCH_FEE_KEYS)),
                timestamp=float(m.get("timestamp", time.time())),
            )

    # -- market data ------------------------------------------------------
    async def order_book(self, trading_pair: str) -> OrderBookSnapshot:
        meta = self._meta(trading_pair)
        try:
            data = await asyncio.to_thread(self._client.get_market_price, meta.product_id)
        except Exception as exc:  # noqa: BLE001
            raise AdapterError(f"order_book failed: {exc}") from exc
        data = data or {}
        bid = _to_dec(_first(data, _BID_KEYS))
        ask = _to_dec(_first(data, _ASK_KEYS))
        if bid <= 0 and ask <= 0:
            # single mid/mark price endpoint -> synthesize a tight book
            mid = _to_dec(_first(data, _MID_KEYS))
            bid = ask = mid
        return OrderBookSnapshot(
            trading_pair=trading_pair,
            bids=[OrderBookLevel(bid, Decimal(0))] if bid > 0 else [],
            asks=[OrderBookLevel(ask, Decimal(0))] if ask > 0 else [],
            timestamp=time.time(),
        )

    async def mid_price(self, trading_pair: str) -> Decimal:
        book = await self.order_book(trading_pair)
        mid = book.mid
        if mid is None:
            raise AdapterError(f"no mid price for {trading_pair}")
        return mid

    async def candles(
        self, trading_pair: str, timeframe: str = "1h", limit: int = 200
    ) -> list:
        meta = self._meta(trading_pair)
        try:
            data = await asyncio.to_thread(
                self._client.get_candlesticks, meta.product_id, timeframe, limit
            )
        except Exception as exc:  # noqa: BLE001
            raise AdapterError(f"candles failed: {exc}") from exc
        return list(data or [])

    async def funding_rate(self, trading_pair: str) -> Optional[Decimal]:
        meta = self._meta(trading_pair)
        try:
            data = await asyncio.to_thread(self._client.get_funding_rate, meta.product_id)
        except Exception as exc:  # noqa: BLE001
            raise AdapterError(f"funding_rate failed: {exc}") from exc
        if data is None:
            return None
        raw = _first(data, ("rate", "funding_rate", "funding", "hourly_funding")) if isinstance(data, dict) else data
        return _to_dec(raw) if raw is not None else None
