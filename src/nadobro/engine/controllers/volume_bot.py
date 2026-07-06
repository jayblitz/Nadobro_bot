"""Volume Bot controller — spot-only single-order volume cycle.

Each cycle places exactly one post-only buy, waits until that buy is fully
filled, then places exactly one post-only sell for the filled base amount. The
sell quote is anchored to the actual entry fill and raised enough to cover
positive maker fees plus a small configured edge. Multiple cycles repeat until
the target cumulative volume or max-cycle safety cap is reached.
"""
from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Optional

from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.executors.order_executor import OrderExecutor, OrderExecutorConfig
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.types import ExecutionStrategy, PositionAction, TradeType, _dec

# Quote-like symbols that must never be selected as a base for Volume.
# Kept in sync with ``product_catalog._QUOTE_LIKE_SYMBOLS``.
_QUOTE_LIKE_BASES = frozenset({"USDC", "USDC0", "USDT", "USDT0", "USD"})

logger = logging.getLogger(__name__)


def _non_negative_decimal(value: object, default: str = "0") -> Decimal:
    try:
        parsed = _dec(value)
    except Exception:  # noqa: BLE001
        parsed = _dec(default)
    return parsed if parsed > 0 else Decimal(0)


class VolumeBotController(Controller):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(name="volume_bot", **kwargs)  # type: ignore[arg-type]
        raw_pair = str(self.cfg("trading_pair") or "").strip().upper()
        market = str(self.cfg("market", "spot")).lower()
        leverage = int(self.cfg("leverage", 1))
        if market != "spot":
            raise ValueError("VolumeBotController is spot-only (market must be 'spot')")
        if leverage != 1:
            raise ValueError("VolumeBotController is spot-only (leverage must be 1)")
        if not raw_pair:
            raise ValueError("VolumeBotController requires a non-empty trading_pair")
        if raw_pair.endswith("-PERP") or raw_pair.endswith("PERP"):
            raise ValueError(f"{raw_pair} is a perp; VolumeBotController is spot-only")
        base = raw_pair.split("-", 1)[0]
        if base in _QUOTE_LIKE_BASES:
            raise ValueError(
                f"{raw_pair} is a quote-like asset and not a valid Volume spot base"
            )

        self.trading_pair = raw_pair
        self.total_amount_quote = _dec(self.cfg("total_amount_quote", "100"))
        self.target_volume_usd = _dec(self.cfg("target_volume_usd", "0"))
        self.max_cycles = max(1, int(self.cfg("max_cycles", 100) or 100))
        self.buy_offset_bp = _non_negative_decimal(
            self.cfg("vol_buy_offset_bp", self.cfg("vol_maker_offset_bp", 5.0))
        )
        self.sell_edge_bp = _non_negative_decimal(
            self.cfg(
                "vol_sell_edge_bp",
                self.cfg("vol_min_edge_bp", self.cfg("vol_maker_offset_bp", 5.0)),
            )
        )
        self.maker_fee_rate = self._maker_fee_rate()

        # Option 5 (maker-first, cross-on-timeout): if a post-only leg is still
        # UNFILLED after this many seconds, cancel it and re-place the full size
        # as a taker MARKET order to guarantee the fill. 0 disables (pure maker).
        # NB: do NOT use ``... or 60`` — an explicit 0 is falsy and must survive.
        _cross_raw = self.cfg("vol_cross_after_seconds", 60)
        try:
            self.cross_after_seconds = max(0, int(float(_cross_raw if _cross_raw is not None else 60)))
        except (TypeError, ValueError):
            self.cross_after_seconds = 60

        self.session_volume_usd: Decimal = Decimal(0)
        self.session_realized_pnl_usd: Decimal = Decimal(0)
        self.cycles_completed = 0
        self.completed = False
        self.stop_reason = ""
        self.phase = "idle"

        self.buy_id: Optional[str] = None
        self.sell_id: Optional[str] = None
        self.entry_base = Decimal(0)
        self.entry_quote = Decimal(0)
        self.entry_fee_quote = Decimal(0)
        self.entry_price = Decimal(0)
        self.entry_fill_ts = 0.0
        self.close_base_remaining = Decimal(0)
        self.last_order_digest = ""
        self.last_order_kind = ""
        # Option 5 cross-on-timeout bookkeeping (per leg, reset each cycle).
        self.buy_target_base = Decimal(0)
        self.sell_target_base = Decimal(0)
        self.buy_placed_ts = 0.0
        self.sell_placed_ts = 0.0
        self.buy_crossed = False
        self.sell_crossed = False

    def _maker_fee_rate(self) -> Decimal:
        """Return positive maker fee cost as a fraction; rebates count as 0 cost."""
        raw = self.cfg("spot_maker_fee_rate", self.cfg("vol_maker_fee_rate"))
        if raw is None and self.cfg("vol_maker_fee_bp") is not None:
            raw = _dec(self.cfg("vol_maker_fee_bp")) / Decimal(10000)
        rate = _non_negative_decimal(raw, "0")
        # A malformed rate >= 100% would make the breakeven denominator invalid.
        return min(rate, Decimal("0.99"))

    def _target_reached(self) -> bool:
        return self.target_volume_usd > 0 and self.session_volume_usd >= self.target_volume_usd

    def _complete(self, reason: str) -> None:
        self.phase = "done"
        self.completed = True
        self.stop_reason = reason
        self._set_stopped()

    def _effectively_full(self, filled: Decimal, target: Decimal) -> bool:
        """True when ``filled`` covers ``target`` within one venue lot.

        A taker MARKET fill (the Option-5 cross path) — and a post-only maker
        whose requested size was not lot-aligned — is rounded DOWN to the lot by
        the venue, so an exact ``filled >= target`` test would misread a
        venue-completed fill as partial and either strand the just-bought base
        (buy) or drop the round-trip's volume/PnL and leave dust (sell). This
        mirrors the adapter's own FILLED rule (nado.py: ``unfilled <= lot``)."""
        if filled >= target:
            return True
        try:
            lot = _dec(self.adapter.lot_size(self.trading_pair))
        except Exception:  # noqa: BLE001 - degrade to exact compare if lot unknown
            lot = Decimal(0)
        return lot > 0 and (target - filled) <= lot

    async def _maker_buy_price(self) -> Decimal:
        try:
            book = await self.adapter.order_book(self.trading_pair)
            bid, ask = book.best_bid, book.best_ask
            if bid is not None and ask is not None and bid > 0 and ask > 0:
                px = (bid + ask) / Decimal(2) if bid < ask else bid
            elif bid is not None and bid > 0:
                px = bid
            else:
                px = await self.adapter.mid_price(self.trading_pair)
        except Exception:  # noqa: BLE001
            px = await self.adapter.mid_price(self.trading_pair)
        offset = self.buy_offset_bp / Decimal(10000)
        if offset > 0:
            px = px * (Decimal(1) - offset)
        return px

    async def _maker_sell_price(self) -> Decimal:
        if self.entry_base <= 0 or self.entry_quote <= 0:
            return await self.adapter.mid_price(self.trading_pair)

        estimated_buy_fee = self.entry_quote * self.maker_fee_rate
        buy_fee = self.entry_fee_quote if self.entry_fee_quote != 0 else estimated_buy_fee
        gross_cost = self.entry_quote + buy_fee
        fee_denominator = Decimal(1) - self.maker_fee_rate
        fee_floor = gross_cost / (self.entry_base * fee_denominator)
        edge_floor = self.entry_price * (Decimal(1) + (self.sell_edge_bp / Decimal(10000)))
        px = max(fee_floor, edge_floor)

        try:
            book = await self.adapter.order_book(self.trading_pair)
            bid, ask = book.best_bid, book.best_ask
            if ask is not None and ask > 0:
                px = max(px, ask)
            if bid is not None and bid > 0 and px <= bid:
                px = bid * (Decimal(1) + Decimal("0.0001"))
        except Exception:  # noqa: BLE001
            logger.warning(
                "volume_bot: sell price using fee/entry floor without live book guard; "
                "post-only sell may reject or rest away from book pair=%s controller=%s",
                self.trading_pair,
                self.id,
                exc_info=True,
            )
        return px

    async def _spawn_order(
        self,
        side: TradeType,
        amount_base: Decimal,
        price: Optional[Decimal],
        *,
        kind: str,
        position_action: PositionAction = PositionAction.OPEN,
        execution_strategy: ExecutionStrategy = ExecutionStrategy.LIMIT_MAKER,
    ) -> tuple[bool, Optional[OrderExecutor]]:
        cfg = OrderExecutorConfig(
            self.trading_pair,
            side,
            amount_base,
            execution_strategy,
            price=price,
            leverage=1,
            position_action=position_action,
        )
        ex = OrderExecutor(
            cfg,
            user_id=self.user_id,
            controller_id=self.id,
            adapter=self.adapter,
            inventory=self.inventory,
        )
        # MARKET crosses carry no price; size the risk request off live mid.
        if price is not None and price > 0:
            notional = amount_base * price
        else:
            notional = amount_base * await self.adapter.mid_price(self.trading_pair)
        ok = await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=notional)
        )
        if ok and ex.order is not None:
            self.last_order_digest = ex.order.id
            self.last_order_kind = kind
        return ok, ex if ok else None

    async def _cross_to_taker(
        self,
        ex: OrderExecutor,
        side: TradeType,
        target_base: Decimal,
        position_action: PositionAction,
    ) -> Optional[OrderExecutor]:
        """Cancel a timed-out post-only maker and, IF it is still unfilled,
        re-place the full size as a taker MARKET order to guarantee the fill.

        Returns the new market executor, or ``None`` when a fill landed at/before
        the cancel (the caller then falls through to the normal terminal handling
        instead of crossing — so a maker that filled right as we cancel can never
        be double-placed as a second market order)."""
        await self.orchestrator.stop(ex.id)
        order = getattr(ex, "order", None)
        filled = _dec(getattr(order, "filled_base", 0) or 0) if order is not None else Decimal(0)
        if filled > 0:
            return None
        if target_base <= 0:
            return None
        ok, new_ex = await self._spawn_order(
            side,
            target_base,
            None,
            kind="cross",
            position_action=position_action,
            execution_strategy=ExecutionStrategy.MARKET,
        )
        return new_ex if ok else None

    async def _start_buy_cycle(self) -> bool:
        buy_price = await self._maker_buy_price()
        if buy_price <= 0:
            self._complete("invalid_buy_price")
            return False
        amount_base = self.total_amount_quote / buy_price
        ok, ex = await self._spawn_order(
            TradeType.BUY, amount_base, buy_price, kind="buy"
        )
        if ok and ex is not None:
            self.buy_id = ex.id
            self.sell_id = None
            self.phase = "pending_fill"
            self.entry_base = Decimal(0)
            self.entry_quote = Decimal(0)
            self.entry_fee_quote = Decimal(0)
            self.entry_price = Decimal(0)
            self.entry_fill_ts = 0.0
            self.close_base_remaining = Decimal(0)
            self.buy_target_base = amount_base
            self.buy_placed_ts = time.time()
            self.buy_crossed = False
            return True
        self._complete("buy_spawn_failed")
        return False

    async def _start_sell_cycle(self) -> bool:
        sell_price = await self._maker_sell_price()
        amount_base = self.entry_base
        ok, ex = await self._spawn_order(
            TradeType.SELL,
            amount_base,
            sell_price,
            kind="sell",
            position_action=PositionAction.CLOSE,
        )
        if ok and ex is not None:
            self.sell_id = ex.id
            self.close_base_remaining = amount_base
            self.phase = "pending_close_fill"
            self.sell_target_base = amount_base
            self.sell_placed_ts = time.time()
            self.sell_crossed = False
            return True
        self._complete("sell_spawn_failed")
        return False

    def _sync_buy_progress(self, buy_ex: object) -> None:
        order = getattr(buy_ex, "order", None)
        if order is None:
            return
        filled_base = _dec(getattr(order, "filled_base", 0) or 0)
        if filled_base <= 0:
            return
        self.entry_base = filled_base
        self.entry_quote = _dec(getattr(order, "filled_quote", 0) or 0)
        self.entry_fee_quote = _dec(getattr(order, "fee_quote", 0) or 0)
        self.entry_price = self.entry_quote / self.entry_base if self.entry_base > 0 else Decimal(0)
        if self.entry_fill_ts <= 0:
            self.entry_fill_ts = time.time()

    def _sync_sell_progress(self, sell_ex: object) -> None:
        order = getattr(sell_ex, "order", None)
        if order is None:
            return
        sold_base = _dec(getattr(order, "filled_base", 0) or 0)
        self.close_base_remaining = max(Decimal(0), self.entry_base - sold_base)

    async def on_start(self) -> None:
        await self._start_buy_cycle()

    async def on_tick(self) -> None:
        for ex in self.my_executors(active_only=True):
            await self.orchestrator.tick(ex.id)

        if self.phase == "pending_fill" and self.buy_id is not None:
            buy_ex = self.orchestrator.get(self.buy_id)
            if buy_ex is None:
                return
            self._sync_buy_progress(buy_ex)
            if not buy_ex.is_terminated:
                # Option 5: the post-only buy is still resting. If it has been
                # UNFILLED past the timeout, cross to a taker MARKET buy for the
                # full size to guarantee the fill (once per cycle).
                if (
                    self.cross_after_seconds > 0
                    and not self.buy_crossed
                    and self.entry_base <= 0
                    and self.buy_placed_ts > 0
                    and (time.time() - self.buy_placed_ts) >= self.cross_after_seconds
                ):
                    self.buy_crossed = True
                    new_ex = await self._cross_to_taker(
                        buy_ex, TradeType.BUY, self.buy_target_base, PositionAction.OPEN
                    )
                    if new_ex is not None:
                        self.buy_id = new_ex.id
                    # Either way, let the next tick evaluate the outcome (the new
                    # market order, or the maker fill that landed at cancel time).
                return
            order = getattr(buy_ex, "order", None)
            if order is not None:
                self._sync_buy_progress(buy_ex)
            if self.entry_base <= 0:
                self._complete("no_fill")
                return
            filled_base = _dec(getattr(order, "filled_base", 0) or 0) if order is not None else Decimal(0)
            amount_base = _dec(getattr(order, "amount_base", 0) or 0) if order is not None else Decimal(0)
            if order is None or not self._effectively_full(filled_base, amount_base):
                self._complete("buy_not_fully_filled")
                return
            self.session_volume_usd += self.entry_quote
            self.phase = "filled_wait_close"
            if not await self._start_sell_cycle():
                self._complete("sell_spawn_failed")
            return

        if self.phase == "pending_close_fill" and self.sell_id is not None:
            sell_ex = self.orchestrator.get(self.sell_id)
            if sell_ex is None:
                return
            self._sync_sell_progress(sell_ex)
            if not sell_ex.is_terminated:
                # Option 5: the post-only sell (close) is still resting. If it has
                # been UNFILLED past the timeout, cross to a taker MARKET sell for
                # the full size — guarantees the position is flattened (once/cycle).
                if (
                    self.cross_after_seconds > 0
                    and not self.sell_crossed
                    and self.close_base_remaining >= self.sell_target_base
                    and self.sell_placed_ts > 0
                    and (time.time() - self.sell_placed_ts) >= self.cross_after_seconds
                ):
                    self.sell_crossed = True
                    new_ex = await self._cross_to_taker(
                        sell_ex, TradeType.SELL, self.sell_target_base, PositionAction.CLOSE
                    )
                    if new_ex is not None:
                        self.sell_id = new_ex.id
                return
            order = getattr(sell_ex, "order", None)
            if order is None:
                self._complete("sell_missing_order")
                return
            self._sync_sell_progress(sell_ex)
            sold_base = _dec(getattr(order, "filled_base", 0) or 0)
            if not self._effectively_full(sold_base, self.entry_base):
                self._complete("sell_not_fully_filled")
                return
            sell_quote = _dec(getattr(order, "filled_quote", 0) or 0)
            sell_fee = _dec(getattr(order, "fee_quote", 0) or 0)
            self.session_volume_usd += sell_quote
            self.session_realized_pnl_usd += (
                sell_quote - self.entry_quote - self.entry_fee_quote - sell_fee
            )
            self.cycles_completed += 1
            self.close_base_remaining = Decimal(0)
            if self.target_volume_usd <= 0:
                self._complete("round_trip_complete")
            elif self._target_reached():
                self._complete("target_volume_hit")
            elif self.cycles_completed >= self.max_cycles:
                self._complete("max_cycles")
            else:
                await self._start_buy_cycle()

    def volume_metrics(self) -> dict:
        volume_done = self.session_volume_usd
        remaining = max(Decimal(0), self.target_volume_usd - volume_done)
        return {
            "vol_phase": self.phase,
            "volume_done_usd": float(volume_done),
            "volume_remaining_usd": float(remaining),
            "session_volume_usd": float(volume_done),
            "session_realized_pnl_usd": float(self.session_realized_pnl_usd),
            "vol_cycles_completed": int(self.cycles_completed),
            "vol_entry_size": float(self.entry_base),
            "vol_entry_quote": float(self.entry_quote),
            "vol_entry_price": float(self.entry_price),
            "vol_entry_fill_ts": float(self.entry_fill_ts or 0.0),
            "vol_close_size": float(self.close_base_remaining),
            "vol_last_order_digest": self.last_order_digest,
            "vol_last_order_kind": self.last_order_kind,
        }
