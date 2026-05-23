"""Nado execution adapter — wraps ``services/nado_client.NadoClient`` and the
1CT Linked Signer to satisfy :class:`NadoAdapterBase`.

This is the ONLY module in ``src/nadobro/engine`` permitted to import the
venue client (``connectors/nado`` does not exist in this repo; the real client
lives at ``src/nadobro/services/nado_client``). The engine-scoped half of
``tests/lint/test_adapter_isolation.py`` enforces that.

Order state is reconstructed from the real client surface:
``get_open_orders`` (still resting?) + ``get_matches`` (fills) for
``order_status``; ``cancel_orders`` for cancellation; a hybrid digest->order
registry (in-memory + optional persistence hook + lazy venue-reconcile)
resolves the product for a given order id. The persistence hook means orders
survive a process restart; the lazy reconcile means even an un-persisted
order can be re-acquired by scanning ``get_open_orders`` for each configured
product (used as last resort and after a cold start).

Response field names are read defensively via the ``_*_KEYS`` maps below — run
``scripts/capture_nado_shapes.py`` on testnet and adjust those maps to the
exact venue shapes, then validate end-to-end (Phase 3/6 runbooks).

Implemented in Phase 1; live mapping completed in production hardening (B).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, AsyncIterator, Dict, Iterable, Optional, Sequence

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

logger = logging.getLogger(__name__)

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
_REJECTED_STATES = ("rejected", "expired", "failed", "error")
_CANCELLED_STATES = ("cancelled", "canceled", "voided")
_FILLED_STATES = ("filled", "matched", "complete", "completed")


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

    def to_record(self) -> Dict[str, Any]:
        return {
            "trading_pair": self.trading_pair,
            "product_id": int(self.product_id),
            "side": self.side.value,
            "order_type": self.order_type.value,
            "amount_base": str(self.amount_base),
            "price": str(self.price) if self.price is not None else None,
        }

    @classmethod
    def from_record(cls, rec: Dict[str, Any]) -> "_OrderRef":
        return cls(
            trading_pair=str(rec["trading_pair"]),
            product_id=int(rec["product_id"]),
            side=TradeType(rec["side"]),
            order_type=OrderType(rec["order_type"]),
            amount_base=_dec(rec["amount_base"]),
            price=_dec(rec["price"]) if rec.get("price") is not None else None,
        )


class OrderRegistry:
    """Persistence hook for the adapter's digest->ref registry.

    The default no-op implementation keeps the legacy in-memory-only behavior.
    Production wiring should inject a DB-backed implementation that survives
    restart (see BUG-NA-5). The adapter falls back to lazy venue-reconcile
    when both the in-memory cache and this registry are empty.
    """

    def record(self, order_id: str, ref: _OrderRef) -> None:  # noqa: ARG002
        return None

    def forget(self, order_id: str) -> None:  # noqa: ARG002
        return None

    def lookup(self, order_id: str) -> Optional[_OrderRef]:  # noqa: ARG002
        return None

    def all_ids(self) -> Iterable[str]:
        return ()


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

    def __init__(
        self,
        client: NadoClient,
        products: Dict[str, ProductMeta],
        registry: Optional[OrderRegistry] = None,
    ) -> None:
        self._client = client
        self._products = products
        self._orders: Dict[str, _OrderRef] = {}
        self._registry: OrderRegistry = registry or OrderRegistry()

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
        ref = _OrderRef(
            trading_pair, meta.product_id, side, order_type, amount_base, price
        )
        self._orders[order.id] = ref
        try:
            self._registry.record(order.id, ref)
        except Exception:  # noqa: BLE001 - persistence must not break order placement
            logger.warning("order registry record failed for %s", order.id, exc_info=True)

        # BUG-NA-2 fix: if the venue response did not include explicit fill
        # data but reported FILLED, follow up with an authoritative fills query
        # so we don't ship synthesized base/quote to inventory.
        if order.state is OrderState.FILLED and order.filled_base <= 0:
            try:
                fb, fq, fee = await self._fills_for(meta.product_id, order.id)
                if fb > 0:
                    order = NadoOrder(
                        id=order.id, trading_pair=trading_pair, side=side,
                        order_type=order_type, amount_base=amount_base, price=price,
                        state=order.state, filled_base=fb, filled_quote=fq, fee_quote=fee,
                    )
                else:
                    # Venue claims FILLED but archive shows no fills yet -> downgrade to
                    # PARTIALLY_FILLED so the executor keeps polling rather than
                    # marking the order terminal with zeros.
                    order = NadoOrder(
                        id=order.id, trading_pair=trading_pair, side=side,
                        order_type=order_type, amount_base=amount_base, price=price,
                        state=OrderState.PARTIALLY_FILLED,
                        filled_base=Decimal(0), filled_quote=Decimal(0), fee_quote=Decimal(0),
                    )
            except Exception:  # noqa: BLE001
                logger.warning(
                    "place_order: fills follow-up failed for %s; leaving state=PARTIAL until next status poll",
                    order.id, exc_info=True,
                )
                order = NadoOrder(
                    id=order.id, trading_pair=trading_pair, side=side,
                    order_type=order_type, amount_base=amount_base, price=price,
                    state=OrderState.PARTIALLY_FILLED,
                    filled_base=Decimal(0), filled_quote=Decimal(0), fee_quote=Decimal(0),
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
        raw_state = str(_first(data, ("status", "state"), "") or "").lower()

        # BUG-NA-3 fix: surface REJECTED + CANCELLED explicitly; previously
        # both collapsed into OPEN and executors stayed stuck.
        if raw_state in _REJECTED_STATES:
            state = OrderState.REJECTED
        elif raw_state in _CANCELLED_STATES:
            state = OrderState.CANCELLED
        elif raw_state in _FILLED_STATES or order_type is OrderType.MARKET:
            state = OrderState.FILLED
        else:
            state = OrderState.OPEN

        # BUG-NA-2 fix: never synthesize fills. If we don't have real
        # filled_base/quote from the venue, leave them at zero — place_order
        # will follow up with a fills query and the executor will resolve via
        # order_status on subsequent ticks.
        return NadoOrder(
            id=digest, trading_pair=trading_pair, side=side, order_type=order_type,
            amount_base=amount_base, price=price, state=state,
            filled_base=filled_base, filled_quote=filled_quote, fee_quote=fee_quote,
        )

    async def cancel_order(self, order_id: str) -> bool:
        ref = self._orders.get(order_id) or self._registry.lookup(order_id)
        if ref is None:
            # BUG-NA-5 fix: lazy reconcile against every known product so a
            # cold-start adapter can still cancel orders placed before the
            # restart.
            ref = await self._reconcile_order(order_id)
        if ref is None:
            return False
        self._orders[order_id] = ref
        try:
            await self._client.cancel_orders(product_id=ref.product_id, digests=[order_id])
        except Exception as exc:  # noqa: BLE001
            # Distinguish "already terminal" from transient venue error.
            # Try one verification probe; if the order is gone from the book
            # AND has no further activity, treat as success. Otherwise raise
            # so the executor's cancel-on-stop loop can confirm via polling.
            verified = await self._verify_no_longer_open(ref.product_id, order_id)
            if verified:
                self._registry.forget(order_id)
                self._orders.pop(order_id, None)
                return True
            raise AdapterError(f"cancel_order failed for {order_id}: {exc}") from exc
        self._registry.forget(order_id)
        return True

    async def _verify_no_longer_open(self, product_id: int, order_id: str) -> bool:
        try:
            open_orders = await asyncio.to_thread(self._client.get_open_orders, product_id, True)
        except Exception:  # noqa: BLE001
            return False
        return self._find_open(open_orders, order_id) is None

    async def order_status(self, order_id: str) -> NadoOrder:
        ref = self._orders.get(order_id) or self._registry.lookup(order_id)
        if ref is None:
            ref = await self._reconcile_order(order_id)
        if ref is None:
            raise AdapterError(f"unknown order id: {order_id}")
        self._orders[order_id] = ref
        try:
            open_orders = await asyncio.to_thread(self._client.get_open_orders, ref.product_id, True)
        except Exception as exc:  # noqa: BLE001
            raise AdapterError(f"order_status failed: {exc}") from exc

        resting = self._find_open(open_orders, order_id)
        if resting is not None:
            filled_base = _to_dec(_first(resting, _OPEN_FILLED_KEYS))
            px = ref.price if ref.price is not None else _to_dec(_first(resting, _PRICE_KEYS))
            state = OrderState.PARTIALLY_FILLED if filled_base > 0 else OrderState.OPEN
            # BUG-NA-6 (partial): also pull realised fees for a resting order
            # when there are partial matches. Cheap because we already have the
            # digest -> matches scan.
            fee = Decimal(0)
            if filled_base > 0:
                _, _, fee = await self._fills_for(ref.product_id, order_id)
            return self._mk_order(order_id, ref, state, filled_base, filled_base * px, fee)

        # No longer resting -> aggregate fills for this digest.
        filled_base, filled_quote, fee = await self._fills_for(ref.product_id, order_id)
        # BUG-NA-4 fix: derive the FILLED threshold from the venue lot size so
        # a single unfilled lot (smaller than 0.1% on tiny orders) is not
        # misclassified as a complete fill.
        lot = self._meta(ref.trading_pair).lot_size
        unfilled = ref.amount_base - filled_base
        if unfilled <= lot:
            state = OrderState.FILLED
        elif filled_base > 0:
            state = OrderState.PARTIALLY_FILLED  # gone from book but partially done
        else:
            state = OrderState.CANCELLED
        return self._mk_order(order_id, ref, state, filled_base, filled_quote, fee)

    async def _reconcile_order(self, order_id: str) -> Optional[_OrderRef]:
        """Scan every known product's open-orders for ``order_id`` and rebuild
        the ref. Last-resort recovery after a process restart (BUG-NA-5).
        Returns None if the order is not currently resting anywhere — in that
        case the caller cannot manage it without external persistence.
        """
        for pair, meta in self._products.items():
            try:
                open_orders = await asyncio.to_thread(
                    self._client.get_open_orders, meta.product_id, True,
                )
            except Exception:  # noqa: BLE001
                continue
            resting = self._find_open(open_orders, order_id)
            if resting is None:
                continue
            try:
                is_buy = bool(resting.get("is_buy") if isinstance(resting, dict) else False)
            except Exception:  # noqa: BLE001
                is_buy = False
            side = TradeType.BUY if is_buy else TradeType.SELL
            price = _to_dec(_first(resting, _PRICE_KEYS)) if isinstance(resting, dict) else Decimal(0)
            amount = _to_dec(_first(resting, ("amount", "size", "amount_base"))) if isinstance(resting, dict) else Decimal(0)
            ref = _OrderRef(
                trading_pair=pair, product_id=meta.product_id, side=side,
                # Unknown post-reconciliation; assume LIMIT (post-only is venue-side).
                order_type=OrderType.LIMIT,
                amount_base=amount if amount > 0 else Decimal(1),
                price=price if price > 0 else None,
            )
            try:
                self._registry.record(order_id, ref)
            except Exception:  # noqa: BLE001
                logger.warning("order registry re-record failed for %s", order_id, exc_info=True)
            return ref
        return None

    def _mk_order(
        self, order_id: str, ref: _OrderRef, state: OrderState, filled_base: Decimal,
        filled_quote: Decimal, fee: Decimal,
    ) -> NadoOrder:
        # BUG-NA-7 fix: thread the real digest through so subsequent
        # cancel_order/order_status calls can find the order.
        return NadoOrder(
            id=order_id, trading_pair=ref.trading_pair, side=ref.side, order_type=ref.order_type,
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
