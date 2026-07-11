"""Volume Bot controller — fast spot maker ping-pong (v3).

Objective: maximum executed spot volume per unit time at minimum cost, using
limit orders on BOTH legs. Each cycle is one buy → sell round trip of
``total_amount_quote``; cycles repeat until ``target_volume_usd`` (or the
``max_cycles`` safety cap) is reached.

The v2 controller produced almost no volume in production (max one fill per
session): the buy rested 5bp below mid with a ~25bp requote dead band (56 min
to fill $101 on KBTC), the sell was priced max(breakeven+edge, ask) with NO
requote path at all (8.5 h stall on WNVDAX), and the whole machine was gated
on per-cycle PROFIT — impossible in a flat/falling market. v3 replaces the
pricing and recovery mechanics while keeping the proven cycle accounting:

* Quotes are glued to the TOUCH: the buy joins the best bid (improving it by
  one tick when the spread leaves room), the sell joins the best ask the same
  way. Post-only, so every resting fill pays the maker fee (~1.8bp).
* BOTH legs requote on a timer (``vol_requote_seconds``): unfilled after N
  seconds → cancel and re-place at the fresh touch. No drift dead band.
* Per-cycle PROFIT is no longer required. The sell floor is the cycle's
  breakeven MINUS ``vol_max_cycle_loss_bp`` — a volume bot buys turnover with
  a bounded, configurable cost per cycle. The session SL rail remains the
  hard backstop.
* Maker-first, cross-on-deadline: a leg still unfilled after
  ``vol_cross_after_seconds`` (0 disables) is finished with a marketable
  LIMIT priced ``vol_cross_slippage_bp`` through the touch — still a limit
  order (bounded price), fills as taker. Restores the 8bf08d0 feature lost in
  the d10e6f1 merge.
* Market-hours aware: RWA spots (WNVDAX, WQQQX, …) have no live book when the
  underlying market is closed. A missing best bid/ask puts the controller in
  a ``market_closed`` wait state instead of quoting into a dead book (or
  failing the spawn) — it resumes automatically when the book comes back.
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
        # Passive distance BELOW the touch for the buy (0 = join/improve the
        # best bid). The v2 default of 5bp-under-mid is retired: on a tight
        # book that rests below the best bid and needs a down-move to fill.
        self.buy_offset_bp = _non_negative_decimal(self.cfg("vol_buy_offset_bp", 0))
        # Bounded cost of one round trip, in bp of the cycle notional,
        # measured against the fee-inclusive breakeven. Replaces the v2
        # forced-profit floor (breakeven + edge) that made cycles impossible
        # to complete unless price rose past entry + 2×fees + edge.
        self.max_cycle_loss_bp = _non_negative_decimal(
            self.cfg("vol_max_cycle_loss_bp", 20.0)
        )
        # Requote cadence: a resting leg older than this is cancelled and
        # re-placed at the fresh touch. Applies to BOTH legs (v2 chased only
        # the buy, and only after a ~25bp adverse run).
        self.requote_seconds = float(self.cfg("vol_requote_seconds", 20.0) or 0.0)
        # Maker-first deadline: a leg unfilled this long is finished with a
        # marketable LIMIT priced ``cross_slippage_bp`` through the touch.
        # 0 disables crossing (pure maker mode).
        self.cross_after_seconds = float(self.cfg("vol_cross_after_seconds", 75.0) or 0.0)
        self.cross_slippage_bp = _non_negative_decimal(
            self.cfg("vol_cross_slippage_bp", 15.0)
        )
        self.maker_fee_rate = self._maker_fee_rate()

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
        # Per-cycle accumulators across (possibly several) sell orders. Each
        # executor is booked exactly once (a tick can re-enter the terminated
        # branch if a follow-up spawn raised mid-transition).
        self.sold_base = Decimal(0)
        self.sold_quote = Decimal(0)
        self.sold_fee_quote = Decimal(0)
        self._accounted_sells: set = set()
        # Leg timers. ``leg_started_ts`` is set once per leg per cycle (the
        # cross deadline measures total leg age across requotes);
        # ``leg_quoted_ts`` resets on every placement (the requote timer).
        self.leg_started_ts = 0.0
        self.leg_quoted_ts = 0.0
        self.leg_crossed = False
        # Bounded recovery counters (reset each completed cycle).
        self.buy_retries = 0
        self.sell_attempts = 0
        self.requotes = 0
        self.crosses = 0
        self.market_closed = False
        self._market_closed_logged = False
        self.last_order_digest = ""
        self.last_order_kind = ""

    _MAX_BUY_RETRIES = 3
    _MAX_SELL_ATTEMPTS = 5
    _MAX_REQUOTES_PER_CYCLE = 120

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

    # -- book helpers -------------------------------------------------------

    async def _touch(self) -> Optional[tuple[Decimal, Decimal]]:
        """(best_bid, best_ask) when the book is LIVE, else None.

        A missing side is the market-closed signal for RWA spots (the venue
        keeps the product listed but the book empties outside market hours).
        v2 fell back to mid_price here and quoted into the dead book all
        night; v3 waits instead.
        """
        try:
            book = await self.adapter.order_book(self.trading_pair)
            bid, ask = book.best_bid, book.best_ask
        except Exception:  # noqa: BLE001 - a dead feed is handled as closed
            return None
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            return None
        return bid, ask

    def _tick(self) -> Decimal:
        try:
            tick = self.adapter.tick_size(self.trading_pair)
        except Exception:  # noqa: BLE001
            tick = Decimal(0)
        return tick if tick > 0 else Decimal(0)

    @staticmethod
    def _snap(price: Decimal, tick: Decimal, *, up: bool) -> Decimal:
        """Quantize to the venue tick, away from the aggressive side."""
        if tick <= 0 or price <= 0:
            return price
        steps = price / tick
        snapped = steps.to_integral_value(rounding="ROUND_CEILING" if up else "ROUND_FLOOR")
        return snapped * tick

    def _buy_price(self, bid: Decimal, ask: Decimal) -> Decimal:
        """Join the best bid; improve it by one tick when the spread leaves
        room (price-time priority: an improving quote is first in line)."""
        tick = self._tick()
        px = bid
        if tick > 0 and (ask - bid) >= tick * 2:
            px = bid + tick
        offset = self.buy_offset_bp / Decimal(10000)
        if offset > 0:
            px = px * (Decimal(1) - offset)
        return self._snap(px, tick, up=False)

    def _cycle_breakeven(self) -> Decimal:
        """Sell price at which the round trip nets exactly zero after both
        maker fees. Estimated from the entry when the buy fee is unknown."""
        if self.entry_base <= 0 or self.entry_quote <= 0:
            return Decimal(0)
        buy_fee = self.entry_fee_quote if self.entry_fee_quote != 0 else (
            self.entry_quote * self.maker_fee_rate
        )
        gross_cost = self.entry_quote + buy_fee
        denominator = Decimal(1) - self.maker_fee_rate
        return gross_cost / (self.entry_base * denominator)

    def _sell_floor(self) -> Decimal:
        """Lowest acceptable sell price: breakeven minus the loss budget."""
        breakeven = self._cycle_breakeven()
        if breakeven <= 0:
            return Decimal(0)
        return breakeven * (Decimal(1) - self.max_cycle_loss_bp / Decimal(10000))

    def _sell_price(self, bid: Decimal, ask: Decimal) -> Decimal:
        """Join the best ask (improving by one tick when the spread leaves
        room), clamped to the loss floor. The v2 rule px >= max(breakeven +
        edge, ask) forced per-cycle profit and never quoted inside the
        spread; v3 sells AT the market and bounds the downside instead."""
        tick = self._tick()
        px = ask
        if tick > 0 and (ask - bid) >= tick * 2:
            px = ask - tick
        floor = self._sell_floor()
        if floor > 0 and px < floor:
            px = floor
        return self._snap(px, tick, up=True)

    # -- order plumbing ------------------------------------------------------

    async def _spawn_order(
        self,
        side: TradeType,
        amount_base: Decimal,
        price: Decimal,
        *,
        kind: str,
        execution: ExecutionStrategy = ExecutionStrategy.LIMIT_MAKER,
        position_action: PositionAction = PositionAction.OPEN,
    ) -> tuple[bool, Optional[OrderExecutor]]:
        cfg = OrderExecutorConfig(
            self.trading_pair,
            side,
            amount_base,
            execution,
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
        ok = await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=amount_base * price)
        )
        if ok and ex.order is not None:
            self.last_order_digest = ex.order.id
            self.last_order_kind = kind
        return ok, ex if ok else None

    def _mark_quoted(self, *, new_leg: bool) -> None:
        now = time.time()
        if new_leg:
            self.leg_started_ts = now
            self.leg_crossed = False
        self.leg_quoted_ts = now

    async def _start_buy_cycle(self) -> bool:
        touch = await self._touch()
        if touch is None:
            self._enter_market_closed()
            return False
        self._exit_market_closed()
        buy_price = self._buy_price(*touch)
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
            self._mark_quoted(new_leg=True)
            self.entry_base = Decimal(0)
            self.entry_quote = Decimal(0)
            self.entry_fee_quote = Decimal(0)
            self.entry_price = Decimal(0)
            self.entry_fill_ts = 0.0
            self.close_base_remaining = Decimal(0)
            self.sold_base = Decimal(0)
            self.sold_quote = Decimal(0)
            self.sold_fee_quote = Decimal(0)
            self._accounted_sells.clear()
            return True
        self._complete("buy_spawn_failed")
        return False

    async def _requote_buy(self, buy_ex: OrderExecutor) -> bool:
        """Cancel the resting buy and re-place at the fresh touch. A partial
        fill flips the cycle to the sell leg for what we actually hold."""
        self.requotes += 1
        await self.orchestrator.stop(buy_ex.id)  # type: ignore[attr-defined]
        self._sync_buy_progress(buy_ex)
        if self.entry_base > 0:
            self.session_volume_usd += self.entry_quote
            self.phase = "filled_wait_close"
            await self._start_sell_cycle()
            return True
        touch = await self._touch()
        if touch is None:
            self._enter_market_closed()
            return False
        buy_price = self._buy_price(*touch)
        if buy_price <= 0:
            self._complete("invalid_buy_price")
            return False
        amount_base = self.total_amount_quote / buy_price
        ok, ex = await self._spawn_order(TradeType.BUY, amount_base, buy_price, kind="buy")
        if ok and ex is not None:
            self.buy_id = ex.id
            self._mark_quoted(new_leg=False)
            return True
        self._complete("buy_spawn_failed")
        return False

    async def _start_sell_cycle(self, amount_base: Optional[Decimal] = None) -> bool:
        amount = amount_base if amount_base is not None else (self.entry_base - self.sold_base)
        if amount <= 0:
            self._complete("sell_nothing_to_close")
            return False
        touch = await self._touch()
        if touch is None:
            # Holding inventory with a dead book: wait for the market to
            # reopen rather than failing the leg (the RWA overnight case).
            self._enter_market_closed()
            self.close_base_remaining = amount
            return False
        self._exit_market_closed()
        sell_price = self._sell_price(*touch)
        if sell_price <= 0:
            self._complete("invalid_sell_price")
            return False
        ok, ex = await self._spawn_order(
            TradeType.SELL,
            amount,
            sell_price,
            kind="sell",
            position_action=PositionAction.CLOSE,
        )
        if ok and ex is not None:
            self.sell_id = ex.id
            self.close_base_remaining = amount
            new_leg = self.phase != "pending_close_fill"
            self.phase = "pending_close_fill"
            self._mark_quoted(new_leg=new_leg)
            return True
        self._complete("sell_spawn_failed")
        return False

    async def _requote_sell(self, sell_ex: OrderExecutor) -> bool:
        """Cancel the resting sell and re-place the remainder at the fresh
        touch. v2 had no sell requote at all — one resting sell above a
        falling market stalled the session forever (8.5 h in prod)."""
        self.requotes += 1
        await self.orchestrator.stop(sell_ex.id)  # type: ignore[attr-defined]
        self._book_sell_fill(sell_ex)
        remaining = self.entry_base - self.sold_base
        if remaining <= 0:
            self._finish_cycle_and_continue_marker = True
            return True
        return await self._start_sell_cycle(remaining)

    async def _cross_leg(self, ex: OrderExecutor, side: TradeType) -> bool:
        """Finish a stalled leg with a marketable LIMIT through the touch —
        still a limit order (price-bounded), fills immediately as taker."""
        touch = await self._touch()
        if touch is None:
            self._enter_market_closed()
            return False
        bid, ask = touch
        self.crosses += 1
        self.leg_crossed = True
        await self.orchestrator.stop(ex.id)  # type: ignore[attr-defined]
        slip = self.cross_slippage_bp / Decimal(10000)
        tick = self._tick()
        if side is TradeType.BUY:
            self._sync_buy_progress(ex)
            remaining_quote = self.total_amount_quote - self.entry_quote
            if remaining_quote <= 0:
                return True
            px = self._snap(ask * (Decimal(1) + slip), tick, up=True)
            amount = remaining_quote / px
            ok, new_ex = await self._spawn_order(
                TradeType.BUY, amount, px, kind="buy_cross",
                execution=ExecutionStrategy.LIMIT,
            )
            if ok and new_ex is not None:
                self.buy_id = new_ex.id
                self.leg_quoted_ts = time.time()
                return True
            self._complete("buy_spawn_failed")
            return False
        self._book_sell_fill(ex)
        remaining = self.entry_base - self.sold_base
        if remaining <= 0:
            self._finish_cycle_and_continue_marker = True
            return True
        px = bid * (Decimal(1) - slip)
        floor = self._sell_floor()
        if floor > 0 and px < floor:
            # Crossing would exceed the per-cycle loss budget: keep resting at
            # the floor instead (bounded loss beats unbounded, but never blow
            # through the user's cost cap silently).
            logger.warning(
                "volume_bot: cross skipped — bid %s below loss floor %s; "
                "sell keeps resting pair=%s controller=%s",
                px, floor, self.trading_pair, self.id,
            )
            return await self._start_sell_cycle(remaining)
        px = self._snap(px, tick, up=False)
        ok, new_ex = await self._spawn_order(
            TradeType.SELL, remaining, px, kind="sell_cross",
            execution=ExecutionStrategy.LIMIT,
            position_action=PositionAction.CLOSE,
        )
        if ok and new_ex is not None:
            self.sell_id = new_ex.id
            self.close_base_remaining = remaining
            self.leg_quoted_ts = time.time()
            return True
        self._complete("sell_spawn_failed")
        return False

    # -- market-hours wait ----------------------------------------------------

    def _enter_market_closed(self) -> None:
        self.market_closed = True
        if self.phase not in ("done",):
            self.phase = "market_closed"
        if not self._market_closed_logged:
            self._market_closed_logged = True
            logger.warning(
                "volume_bot: no live book for %s (market closed?) — waiting, "
                "no orders placed controller=%s",
                self.trading_pair, self.id,
            )

    def _exit_market_closed(self) -> None:
        if self.market_closed:
            logger.info(
                "volume_bot: book is live again for %s — resuming controller=%s",
                self.trading_pair, self.id,
            )
        self.market_closed = False
        self._market_closed_logged = False

    # -- fill accounting -------------------------------------------------------

    def _sell_remainder_placeable(self, remaining: Decimal) -> bool:
        """A residue below the venue lot / min-notional cannot be re-sold."""
        try:
            lot = self.adapter.lot_size(self.trading_pair)
            min_notional = self.adapter.min_notional(self.trading_pair)
        except Exception:  # policy: degrade-ok(assume placeable; the spawn itself is the arbiter)
            return True
        if lot > 0 and remaining < lot:
            return False
        if min_notional > 0 and self.entry_price > 0 and remaining * self.entry_price < min_notional:
            return False
        return True

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

    def _merge_buy_fill(self, buy_ex: object) -> None:
        """Accumulate a cross-order buy fill ON TOP of the maker portion (the
        maker executor was already folded into entry_* before the cross)."""
        order = getattr(buy_ex, "order", None)
        if order is None:
            return
        filled_base = _dec(getattr(order, "filled_base", 0) or 0)
        if filled_base <= 0:
            return
        self.entry_base += filled_base
        self.entry_quote += _dec(getattr(order, "filled_quote", 0) or 0)
        self.entry_fee_quote += _dec(getattr(order, "fee_quote", 0) or 0)
        self.entry_price = self.entry_quote / self.entry_base if self.entry_base > 0 else Decimal(0)
        if self.entry_fill_ts <= 0:
            self.entry_fill_ts = time.time()

    def _book_sell_fill(self, sell_ex: object) -> None:
        """Book a sell executor's fill exactly once."""
        order = getattr(sell_ex, "order", None)
        sid = str(getattr(sell_ex, "id", "") or "")
        if order is not None and sid and sid not in self._accounted_sells:
            self._accounted_sells.add(sid)
            self.sold_base += _dec(getattr(order, "filled_base", 0) or 0)
            self.sold_quote += _dec(getattr(order, "filled_quote", 0) or 0)
            self.sold_fee_quote += _dec(getattr(order, "fee_quote", 0) or 0)
        self.close_base_remaining = max(Decimal(0), self.entry_base - self.sold_base)

    def _finish_cycle(self) -> None:
        """Book the completed round trip and reset per-cycle state."""
        self.session_volume_usd += self.sold_quote
        self.session_realized_pnl_usd += (
            self.sold_quote - self.entry_quote - self.entry_fee_quote - self.sold_fee_quote
        )
        self.cycles_completed += 1
        self.close_base_remaining = Decimal(0)
        self.buy_retries = 0
        self.sell_attempts = 0

    # Set by requote/cross paths when the remainder went to zero mid-transition
    # (the fill landed between our cancel and the re-place).
    _finish_cycle_and_continue_marker = False

    async def _after_cycle(self) -> None:
        self._finish_cycle()
        self.sell_id = None
        self.phase = "cycle_gap"
        if self.target_volume_usd <= 0:
            self._complete("round_trip_complete")
        elif self._target_reached():
            self._complete("target_volume_hit")
        elif self.cycles_completed >= self.max_cycles:
            self._complete("max_cycles")
        else:
            await self._start_buy_cycle()

    # -- lifecycle --------------------------------------------------------------

    async def on_start(self) -> None:
        await self._start_buy_cycle()

    async def on_tick(self) -> None:
        for ex in self.my_executors(active_only=True):
            await self.orchestrator.tick(ex.id)

        if self._finish_cycle_and_continue_marker:
            self._finish_cycle_and_continue_marker = False
            await self._after_cycle()
            return

        if self.phase == "market_closed":
            # No inventory → try to start a buy; inventory held → resume the
            # sell. Both paths re-check the book and fall back to waiting.
            if self.entry_base - self.sold_base > 0:
                await self._start_sell_cycle(self.entry_base - self.sold_base)
            else:
                await self._start_buy_cycle()
            return

        now = time.time()

        if self.phase == "pending_fill" and self.buy_id is not None:
            buy_ex = self.orchestrator.get(self.buy_id)
            if buy_ex is None:
                return
            was_cross = self.last_order_kind == "buy_cross" and buy_ex.id == self.buy_id
            if not buy_ex.is_terminated:
                if not was_cross:
                    # Live partial visibility (metrics + market-closed sell
                    # sizing). Cross orders merge at termination instead —
                    # syncing them here would overwrite the maker portion.
                    self._sync_buy_progress(buy_ex)
                leg_age = now - self.leg_started_ts if self.leg_started_ts else 0.0
                quote_age = now - self.leg_quoted_ts if self.leg_quoted_ts else 0.0
                if (
                    self.cross_after_seconds > 0
                    and not self.leg_crossed
                    and leg_age >= self.cross_after_seconds
                ):
                    await self._cross_leg(buy_ex, TradeType.BUY)
                    return
                if (
                    self.requote_seconds > 0
                    and not was_cross
                    and quote_age >= self.requote_seconds
                    and self.requotes < self._MAX_REQUOTES_PER_CYCLE
                ):
                    await self._requote_buy(buy_ex)
                return
            if was_cross:
                self._merge_buy_fill(buy_ex)
            else:
                self._sync_buy_progress(buy_ex)
            if self.entry_base > 0:
                # Fully or PARTIALLY filled then terminated: round-trip what we
                # actually hold. Completing here without selling would strand
                # the bought base in the user's wallet.
                self.session_volume_usd += self.entry_quote
                self.phase = "filled_wait_close"
                await self._start_sell_cycle()
                return
            # Terminated with zero fill (post-only reject / venue cancel):
            # requote a bounded number of times before giving up.
            if self.buy_retries < self._MAX_BUY_RETRIES:
                self.buy_retries += 1
                logger.warning(
                    "volume_bot: buy terminated unfilled; requoting (%s/%s) "
                    "pair=%s controller=%s",
                    self.buy_retries, self._MAX_BUY_RETRIES, self.trading_pair, self.id,
                )
                await self._start_buy_cycle()
                return
            self._complete("no_fill")
            return

        if self.phase == "filled_wait_close":
            # A sell spawn raised mid-transition on a previous tick. Retry
            # rather than strand the held base in a phase no branch serviced.
            if self.sell_attempts < self._MAX_SELL_ATTEMPTS:
                self.sell_attempts += 1
                await self._start_sell_cycle()
            else:
                self._complete("sell_spawn_failed")
            return

        if self.phase == "cycle_gap":
            # The next cycle's buy spawn raised mid-transition. Retry bounded.
            if self.buy_retries < self._MAX_BUY_RETRIES:
                self.buy_retries += 1
                await self._start_buy_cycle()
            else:
                self._complete("buy_respawn_failed")
            return

        if self.phase == "pending_close_fill" and self.sell_id is not None:
            sell_ex = self.orchestrator.get(self.sell_id)
            if sell_ex is None:
                return
            if not sell_ex.is_terminated:
                order = getattr(sell_ex, "order", None)
                if order is not None:
                    live_filled = _dec(getattr(order, "filled_base", 0) or 0)
                    self.close_base_remaining = max(
                        Decimal(0), self.entry_base - self.sold_base - live_filled
                    )
                leg_age = now - self.leg_started_ts if self.leg_started_ts else 0.0
                quote_age = now - self.leg_quoted_ts if self.leg_quoted_ts else 0.0
                was_cross = self.last_order_kind == "sell_cross"
                if (
                    self.cross_after_seconds > 0
                    and not self.leg_crossed
                    and leg_age >= self.cross_after_seconds
                ):
                    await self._cross_leg(sell_ex, TradeType.SELL)
                    return
                if (
                    self.requote_seconds > 0
                    and not was_cross
                    and quote_age >= self.requote_seconds
                    and self.requotes < self._MAX_REQUOTES_PER_CYCLE
                ):
                    await self._requote_sell(sell_ex)
                    return
                return
            self._book_sell_fill(sell_ex)
            remaining = self.entry_base - self.sold_base
            if remaining > 0:
                # Partial close: re-place the remainder unless it is venue dust
                # or we are out of attempts — never quietly strand inventory.
                if (
                    self.sell_attempts < self._MAX_SELL_ATTEMPTS
                    and self._sell_remainder_placeable(remaining)
                ):
                    self.sell_attempts += 1
                    await self._start_sell_cycle(remaining)
                    return
                logger.warning(
                    "volume_bot: %s base unsold after %s sell attempts "
                    "(dust or exhausted) — finishing cycle with the residue held "
                    "pair=%s controller=%s",
                    remaining, self.sell_attempts, self.trading_pair, self.id,
                )
            await self._after_cycle()

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
            "vol_requotes": int(self.requotes),
            "vol_crosses": int(self.crosses),
            "vol_market_closed": bool(self.market_closed),
            # Legacy metric names kept for the strategy card / status readers.
            "vol_buy_reprices": int(self.requotes),
            "vol_last_order_digest": self.last_order_digest,
            "vol_last_order_kind": self.last_order_kind,
        }
