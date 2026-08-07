"""Reverse Grid (R-Grid) — its own market-making strategy.

R-Grid is NOT a grid variant and NOT a phase switcher (that is D-Grid). Grid says
"don't buy above where I sold, don't sell below where I bought", which works in a
range and gets stuck when price trends out of it. R-Grid inverts that so a trend
pays:

    anchor = (buy exposure price + sell exposure price) / 2

    buy  trigger = anchor x (1 + spread)   → acts as price RISES above it
    sell trigger = anchor x (1 - spread)   → acts as price FALLS below it

Both legs reference the MIDPOINT between where the book has bought and where it
has sold — unlike grid, which mirrors the opposite leg's price directly. Each
leg's *exposure price* is a rolling VWAP over the most recent portion of that
leg's filled volume (``vwap_volume_fraction``, driven by the user's discretion
knob), which is a steadier reference than a single last fill.

Fills are TAKER on both legs (:class:`RGridTakerExecutor`; passive-only off) —
a buy trigger sits ABOVE the market, so a resting post-only order there would be
rejected, not filled. Crossing the spread is the point: the break is the signal.

Safety, in layers:

* **Session SL / TP** — % of margin on live PnL including uPnL, judged net of
  fees. Owned by ``strategy/bot_runtime._evaluate_session_pnl_rail``; this
  controller never second-guesses it, and never applies the same user number as a
  price barrier too (the units invariant).
* **Soft reset** — once price has moved favourably by ``reset_threshold_pct`` AND
  the position is in profit, the opposite (exit) leg starts FOLLOWING the trend:
  it trails the best price by one spread instead of sitting at the entry anchor.
  The run keeps going; the give-back is capped at one spread. Reduce-only.
* **Reversal recalibration** — when price breaks the trigger AGAINST the open
  book, the thesis that opened it is dead: close the WHOLE position reduce-only in
  one order, re-anchor to mid, and let the next break choose the side. Nibbling a
  step per tick left most of the position on the wrong side of a running move and
  paid a taker fee for each nibble.
* **Net-exposure cap** — inherited from MarketMakingController, and never
  disabled. The regime gate ships OFF (it pauses on TRENDS, which is when R-Grid
  must act); the cap, the soft reset and the session rails are the backstops.

R-Grid is never STOOD DOWN by the financial overlay
(``overlay_actuator.NEVER_SUPPRESSED``). Pausing a trend follower in a trend is
backwards, and pausing it mid-position is worse — nobody is left managing the
exit. The overlay protects it two other ways instead: it shades size and trigger
width through the mapped config, and a regime read that CONTRADICTS the open
position arms the trailing exit as soon as the position is in profit, rather than
waiting for the full threshold. Both reduce risk without abandoning the book.
"""
from __future__ import annotations

import logging
from collections import deque
from decimal import ROUND_DOWN, Decimal
from typing import Deque, Dict, Optional, Tuple

from src.nadobro.engine.controllers.market_making import MarketMakingController
from src.nadobro.engine.executors.rgrid_taker_executor import (
    INTENT_ENTRY,
    INTENT_REVERSAL,
    INTENT_TRAIL_EXIT,
    RGridTakerExecutor,
    build_entry_taker,
    build_exit_taker,
)
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.types import PositionAction, TradeType, _dec

logger = logging.getLogger(__name__)

# Retained fills per leg. Large enough that a volume-fraction window has history.
_FILL_HISTORY = 200
# Absolute floor on the soft-reset arm: a "profit" smaller than the round-trip
# taker cost is not profit. 8.6bp = 2 x the 4.3bp all-in taker rate.
_MIN_ARM_PCT = Decimal("0.00086")
# After this many consecutive refused exits, stop pretending a retry-only posture
# is safe and say so loudly (the rail still owns the hard stop).
_MAX_CONSECUTIVE_EXIT_FAILURES = 5

BUY, SELL = "buy", "sell"


class RGridController(MarketMakingController):
    def __init__(self, **kwargs: object) -> None:
        kwargs.setdefault("name", "rgrid")
        super().__init__(**kwargs)  # type: ignore[arg-type]
        # Per-leg exposure windows. The anchor is the average of the two legs, so
        # they must be tracked separately: one blended VWAP over both sides weights
        # whichever leg traded more volume, and a book that bought 3 units at 100
        # and sold 1 at 110 would anchor at 102.5 instead of the 105 midpoint.
        self._leg_fills: Dict[str, Deque[Tuple[Decimal, Decimal]]] = {
            BUY: deque(maxlen=_FILL_HISTORY),
            SELL: deque(maxlen=_FILL_HISTORY),
        }
        self._seen_filled: set[str] = set()
        # Seeded to mid on the first tick so the FIRST trigger is a real ±band move
        # rather than an instant one-sided entry.
        self._anchor: Optional[Decimal] = None
        self._last_anchor: Optional[Decimal] = None
        # Last mid seen, so grid_metrics() can report drift from the anchor without
        # an extra venue read (the /status card renders it every refresh).
        self._last_mid: Optional[Decimal] = None
        self._pending_taker_id: Optional[str] = None
        # Exposure-price window as a fraction of each leg's recent fill VOLUME.
        # 0 ⇒ VWAP over the whole retained window.
        self.vwap_volume_fraction = _dec(self.cfg("vwap_volume_fraction", "0") or "0")
        # Soft reset: arm threshold (favourable move, fraction of price).
        self.reset_threshold_pct = _dec(self.cfg("reset_threshold_pct", "0.002"))
        self.trail_enabled = bool(self.cfg("trail_enabled", True))
        self._trail_peak: Optional[Decimal] = None
        self._trail_armed = False
        # Consecutive refused exits. Returning early forever on "failed" (the safe
        # instinct) wedges the strategy: it stops entering AND stops exiting, and
        # because on_tick returns normally the orchestrator never sees a failure.
        self._exit_failures = 0
        # Overlay telemetry only — the actuator has already applied its effect to
        # size/spread/exposure in the mapped config before this controller sees it.
        self.signal_regime = str(self.cfg("signal_regime", "") or "")
        self.signal_confidence = float(self.cfg("signal_confidence", 0.0) or 0.0)
        # Below this the overlay's read is not confident enough to change anything.
        self.signal_min_confidence = float(self.cfg("rgrid_signal_min_confidence", 0.45) or 0.45)
        # SESSION ISOLATION: the anchor must reflect THIS run's fills. In-memory
        # absorption already guarantees that (my_executors is scoped to this
        # controller_id), but a rebuild (worker handoff / restart) would start
        # blank. The runtime injects ``seed_fills`` — this session's OWN recorded
        # trades — so the anchor survives a rebuild and never sees another
        # user/session/product.
        self._seed_from_history(self.cfg("seed_fills", None))

    # -- exposure window -----------------------------------------------------
    def _seed_from_history(self, rows: object) -> None:
        if not rows or not isinstance(rows, (list, tuple)):
            return
        parsed: list[Tuple[Decimal, Decimal, str]] = []
        for row in rows:
            if isinstance(row, dict):
                px_raw, base_raw, side = row.get("price"), row.get("size"), row.get("side")
            elif isinstance(row, (list, tuple)) and len(row) >= 3:
                px_raw, base_raw, side = row[0], row[1], row[2]
            else:
                continue
            try:
                px = _dec(px_raw)
                base = abs(_dec(base_raw))
            except Exception:  # policy: degrade-ok(skip a malformed seed; live fills re-anchor)
                continue
            if px <= 0 or base <= 0:
                continue
            leg = BUY if str(side or "").lower() in ("long", "buy") else SELL
            parsed.append((px, base, leg))
        # rows arrive newest-first; append oldest-first so window order matches live.
        for px, base, leg in reversed(parsed):
            self._leg_fills[leg].append((px, base))

    def _absorb_fills(self) -> None:
        """Fold newly FILLED entry takers into their leg's exposure window.

        EXIT fills are excluded on purpose. The window is the price at which the
        book took risk ON; a reduce-only exit prints at whatever the market
        offered, and folding it back in drags the anchor toward the exit price —
        which then re-triggers an entry within a tick of the close, paying two
        taker legs for no directional edge.
        """
        for ex in self.my_executors(active_only=False):
            if ex.id in self._seen_filled:
                continue
            order = getattr(ex, "order", None)
            # NOT gated on state is FILLED. The adapter reports a partially filled
            # order that is no longer resting as CANCELLED with the fill amounts
            # preserved (adapter/nado.py). Inventory and the reporting bridge both
            # record that fill, so gating on FILLED left a REAL position the
            # exposure window had never seen: the anchor stayed at the stale
            # reference and the same-direction break could re-fire immediately.
            # Terminal + any fill is the correct condition. (Audit 2026-08-06.)
            if order is None or not ex.is_terminated:
                continue
            if abs(_dec(order.filled_base)) <= 0:
                continue
            self._seen_filled.add(ex.id)
            if getattr(ex, "is_exit", False) or (
                getattr(getattr(ex, "config", None), "position_action", None)
                is PositionAction.CLOSE
            ):
                continue
            base = abs(_dec(order.filled_base))
            quote = abs(_dec(order.filled_quote))
            if base <= 0:
                continue
            px = quote / base
            side = getattr(getattr(ex, "config", None), "side", None)
            if side is TradeType.BUY:
                self._leg_fills[BUY].append((px, base))
            elif side is TradeType.SELL:
                self._leg_fills[SELL].append((px, base))
        if len(self._seen_filled) > 200:
            live = {e.id for e in self.my_executors(active_only=False)}
            self._seen_filled &= live

    def _windowed_vwap(self, fills: "Deque[Tuple[Decimal, Decimal]]") -> Optional[Decimal]:
        if not fills:
            return None
        total_base = sum((b for _, b in fills), Decimal(0))
        if total_base <= 0:
            return None
        if self.vwap_volume_fraction > 0:
            want = total_base * self.vwap_volume_fraction
            num = den = Decimal(0)
            for px, base in reversed(fills):  # most recent first
                num += px * base
                den += base
                if den >= want:
                    break
            return num / den if den > 0 else None
        return sum((px * base for px, base in fills), Decimal(0)) / total_base

    def leg_exposure_price(self, side: str) -> Optional[Decimal]:
        """One leg's exposure price: the windowed VWAP of its own fills."""
        return self._windowed_vwap(self._leg_fills.get(str(side).lower()) or deque())

    def exposure_anchor(self, mid: Optional[Decimal] = None) -> Optional[Decimal]:
        """The R-Grid anchor — the average of the two legs' exposure prices.

        Degenerate cases, in order: one leg only ⇒ that leg IS the average of what
        exists (a one-sided book has no midpoint to split); no fills at all ⇒ the
        seeded anchor (mid at session start), so the first trigger is a real break.
        """
        buy_px = self.leg_exposure_price(BUY)
        sell_px = self.leg_exposure_price(SELL)
        if buy_px is not None and sell_px is not None:
            return (buy_px + sell_px) / Decimal(2)
        if buy_px is not None:
            return buy_px
        if sell_px is not None:
            return sell_px
        return self._anchor or mid

    def _reset_exposure_window(self, mid: Decimal) -> None:
        """Flat book: forget the closed position's entries and re-anchor to mid, so
        a genuine fresh break is required instead of re-entering against a stale
        anchor. Also disarms the trail — it belonged to that position."""
        for leg in self._leg_fills.values():
            leg.clear()
        self._anchor = mid
        self._trail_peak = None
        self._trail_armed = False

    def _has_fills(self) -> bool:
        return any(self._leg_fills[leg] for leg in (BUY, SELL))

    # -- taker plumbing ------------------------------------------------------
    def _taker_in_flight(self) -> bool:
        if self._pending_taker_id is None:
            return False
        ex = self.orchestrator.get(self._pending_taker_id)
        if ex is None or ex.is_terminated:
            self._pending_taker_id = None
            return False
        return True

    def _net_base(self) -> Decimal:
        if self.inventory is None:
            return Decimal(0)
        return self.inventory.get(self.user_id, self.trading_pair, self.id).net_amount_base

    def _position_entry_price(self) -> Optional[Decimal]:
        """Cost basis of the CURRENTLY OPEN position — the full VWAP of the leg
        that opened it.

        NOT ``inventory.breakeven``: that is the session-LIFETIME avg buy / avg
        sell, and every leg accumulates into it, including the trail exits. A long
        closed by a SELL leaves that exit inside ``avg_sell_price``, so the next
        short read a large favourable excursion the moment it opened and armed the
        trail instantly — then exited on the first band-width move against it,
        paying ~10bp adverse + 8.6bp taker every cycle. (Audit 2026-08-06.)

        The per-leg windows are the right basis by construction: exits are excluded
        from them (:meth:`_absorb_fills`) and they are cleared whenever the book
        goes flat (:meth:`_reset_exposure_window`), so the leg matching the position
        side holds exactly this position's entries. Unwindowed on purpose — the
        discretion slice is for the *trigger* anchor, whereas a profit measurement
        must span everything actually paid for the position.
        """
        net = self._net_base()
        if net == 0:
            return None
        fills = self._leg_fills[BUY if net > 0 else SELL]
        total_base = sum((b for _, b in fills), Decimal(0))
        if total_base > 0:
            return sum((px * b for px, b in fills), Decimal(0)) / total_base
        # No window (a rebuild that seeded nothing): inventory is the last resort.
        if self.inventory is None:
            return None
        try:
            breakeven = self.inventory.get(self.user_id, self.trading_pair, self.id).breakeven
        except Exception:  # noqa: BLE001  # policy: degrade-ok(fall back to the anchor)
            return None
        return breakeven if (breakeven is not None and breakeven > 0) else None

    def _band(self) -> Decimal:
        """Trigger offset from the anchor — the user's spread, fee-floored."""
        return max(self.spread_ask_pct, self.spread_floor_half_pct)

    def _quantize_entry(self, amount_base: Decimal, mid: Decimal) -> Optional[Decimal]:
        """Round an ENTRY down to the venue's lot size, and refuse it outright if
        the result is under the venue's minimum notional.

        Why refusing beats shipping it: ``NadoClient.place_order`` GROWS a
        non-reducing order that lands under the minimum up to the venue floor. For
        an entry that means the size actually traded can exceed the size the risk
        engine just approved — the step cap, the net-exposure projection and the
        user's stop budget are all sized against the smaller number. Rounding DOWN
        to a lot and declining below the floor keeps the venue from ever having to
        bump us. Exits are not quantized here: a reduce-only close must be able to
        flatten the exact residual, and the adapter already crosses the spread
        rather than stranding a sub-minimum exit.
        """
        try:
            lot = Decimal(str(self.adapter.lot_size(self.trading_pair) or 0))
            floor_quote = Decimal(str(self.adapter.min_notional(self.trading_pair) or 0))
        except Exception:  # noqa: BLE001  # policy: degrade-ok(no metadata ⇒ send as-is)
            return amount_base
        if lot > 0:
            amount_base = (amount_base / lot).to_integral_value(rounding=ROUND_DOWN) * lot
        if amount_base <= 0:
            logger.warning(
                "rgrid: entry of %s rounds to zero at lot %s — skipping the break "
                "(user=%s pair=%s)", self.order_amount_quote, lot,
                self.user_id, self.trading_pair,
            )
            return None
        if floor_quote > 0 and amount_base * mid < floor_quote:
            logger.warning(
                "rgrid: entry notional %.2f is below the venue minimum %.2f — "
                "skipping the break rather than letting the venue grow it past the "
                "risk-approved size (user=%s pair=%s)",
                float(amount_base * mid), float(floor_quote),
                self.user_id, self.trading_pair,
            )
            return None
        return amount_base

    async def _fire_taker(self, side: TradeType, mid: Decimal, *, intent: str) -> bool:
        if mid <= 0 or self.order_amount_quote <= 0:
            return False
        amount_base = self._quantize_entry(self.order_amount_quote / mid, mid)
        if amount_base is None:
            return False
        cfg = build_entry_taker(
            self.trading_pair, side, amount_base,
            leverage=int(self.cfg("leverage", 1) or 1),
        )
        ex = RGridTakerExecutor(
            cfg, user_id=self.user_id, controller_id=self.id,
            adapter=self.adapter, inventory=self.inventory, intent=intent,
        )
        # Ask risk to approve the QUANTIZED notional, not the pre-rounding figure:
        # the approval must describe the order that is actually sent.
        if not await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=amount_base * mid)
        ):
            return False
        self._pending_taker_id = ex.id
        return True

    async def _fire_exit(self, net_base: Decimal, mid: Decimal) -> bool:
        side = TradeType.SELL if net_base > 0 else TradeType.BUY
        cfg = build_exit_taker(
            self.trading_pair, side, abs(net_base),
            leverage=int(self.cfg("leverage", 1) or 1),
        )
        ex = RGridTakerExecutor(
            cfg, user_id=self.user_id, controller_id=self.id,
            adapter=self.adapter, inventory=self.inventory, intent=INTENT_TRAIL_EXIT,
        )
        if not await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=abs(net_base) * mid)
        ):
            return False
        self._pending_taker_id = ex.id
        # Deliberately do NOT disarm the trail here. A market close can fill
        # PARTIALLY, and disarming on the request would leave the remainder
        # unprotected — it would have to earn a fresh favourable excursion before
        # the exit re-armed, even though the trend has already turned. Leaving the
        # trail armed means the next tick keeps reducing until flat; the flat paths
        # (_maybe_soft_reset's net==0 branch and _reset_exposure_window) own the
        # disarm, and _taker_in_flight throttles it to one order at a time.
        return True

    async def _fire_reversal_flatten(self, net_base: Decimal, mid: Decimal) -> bool:
        """Close the WHOLE position reduce-only because the break went against it.

        R-Grid adds INTO a move. When price breaks the opposite trigger the thesis
        that opened the position is invalidated, so the right response is to get
        flat, re-anchor, and let the NEXT break decide the direction — not to nibble
        one step off a losing position each tick while the move runs.

        This is the "recalibrate and switch to quoting in that direction" path, and
        it is why R-Grid does not need to be paused in a regime switch: it exits the
        stale side itself instead of being stood down mid-position.

        Reduce-only, so it can never open or flip in one order, and never gated on
        the exposure cap — reducing is always permitted.
        """
        side = TradeType.SELL if net_base > 0 else TradeType.BUY
        cfg = build_exit_taker(
            self.trading_pair, side, abs(net_base),
            leverage=int(self.cfg("leverage", 1) or 1),
        )
        ex = RGridTakerExecutor(
            cfg, user_id=self.user_id, controller_id=self.id,
            adapter=self.adapter, inventory=self.inventory, intent=INTENT_REVERSAL,
        )
        if not await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=abs(net_base) * mid)
        ):
            return False
        self._pending_taker_id = ex.id
        logger.info(
            "rgrid reversal: price broke the opposite trigger at %s against a %s of "
            "%s — flattening and re-anchoring so the next break sets the side "
            "(user=%s pair=%s)",
            mid, "long" if net_base > 0 else "short", abs(net_base),
            self.user_id, self.trading_pair,
        )
        return True

    def _overlay_opposes(self, long_side: bool) -> bool:
        """Does the financial overlay read the market against this position?

        Used to TIGHTEN protection, never to pause: R-Grid keeps trading, but a
        position the overlay disagrees with gets its exit leg armed as soon as it is
        in profit instead of waiting for the full arm threshold.
        """
        if self.signal_confidence < self.signal_min_confidence:
            return False
        regime = str(self.signal_regime or "").lower()
        return (regime == "trend_down" and long_side) or (regime == "trend_up" and not long_side)

    # -- soft reset ----------------------------------------------------------
    async def _maybe_soft_reset(self, mid: Decimal) -> str:
        """The documented soft reset: "when price drifts favorably and you're in
        profit, the system adjusts the opposite leg to follow the trend and lock in
        profits".

        Arms once the favourable excursion from the position's own entry reaches
        ``reset_threshold_pct``; from then on the exit leg trails the best price by
        one spread. Returns ``"none"`` (caller may add), ``"fired"`` (exit placed)
        or ``"failed"`` (an exit was WANTED and could not be placed — the caller
        must NOT add risk in the same tick).
        """
        net = self._net_base()
        if net == 0:
            self._trail_peak = None
            self._trail_armed = False
            return "none"
        if not self.trail_enabled or self.reset_threshold_pct <= 0 or mid <= 0:
            return "none"
        # An arm threshold inside the entry band is not a threshold — the exit
        # would arm before the break that opened the position is established. But
        # REFUSING in that case silently removed R-Grid's only non-rail exit: the
        # overlay scales spread_ask_pct live (spread_mult > 1 for any non-zero
        # ATR) while reset_threshold_pct is not scaled, and the shipped defaults
        # sit exactly on the 2 x band boundary — so an ordinary widening disarmed
        # the trail entirely. Widen the ARM instead of disabling the mechanism.
        # (Audit 2026-08-06.)
        band = self._band()
        arm_pct = max(self.reset_threshold_pct, band * Decimal(2), _MIN_ARM_PCT)
        entry = self._position_entry_price() or self.exposure_anchor(mid)
        if entry is None or entry <= 0:
            return "none"
        long_side = net > 0
        excursion = ((mid - entry) / entry) if long_side else ((entry - mid) / entry)
        # Track the extreme every tick (even pre-arm) so the trail starts from the
        # true peak, not from wherever price sat when the threshold was crossed.
        if self._trail_peak is None:
            self._trail_peak = mid
        elif long_side:
            self._trail_peak = max(self._trail_peak, mid)
        else:
            self._trail_peak = min(self._trail_peak, mid)
        if not self._trail_armed:
            # The overlay reading the market AGAINST this position does not pause
            # R-Grid — it arms the exit early, so a position the signal disagrees
            # with starts protecting its profit immediately instead of waiting for
            # the full threshold. Still requires an actual profit: arming underwater
            # would turn the trail into a stop that front-runs the SL rail.
            opposed = self._overlay_opposes(long_side)
            if excursion < arm_pct and not (opposed and excursion > 0):
                return "none"
            self._trail_armed = True
            logger.info(
                "rgrid soft reset armed%s: %s%% favourable from %s — the exit leg now "
                "follows the trend (user=%s pair=%s)",
                " EARLY (overlay reads the market against this position)" if opposed else "",
                round(float(excursion) * 100, 3), entry, self.user_id, self.trading_pair,
            )
        trigger = (
            self._trail_peak * (Decimal(1) - band) if long_side
            else self._trail_peak * (Decimal(1) + band)
        )
        if (mid > trigger) if long_side else (mid < trigger):
            return "none"   # trend intact — keep following it
        logger.info(
            "rgrid soft reset: trend stalled at %s (peak %s, trail %s) — banking the "
            "%s (user=%s pair=%s)",
            mid, self._trail_peak, trigger, "long" if long_side else "short",
            self.user_id, self.trading_pair,
        )
        return "fired" if await self._fire_exit(net, mid) else "failed"

    # -- tick ----------------------------------------------------------------
    async def on_tick(self) -> None:
        # Absorb fills BEFORE pricing: the anchor is defined by them.
        for ex in self.my_executors(active_only=True):
            await self.orchestrator.tick(ex.id)
        self._absorb_fills()

        mid = await self.adapter.mid_price(self.trading_pair)
        if mid <= 0:
            return
        self._last_mid = mid
        if self._anchor is None:
            self._anchor = mid

        base_value = self._base_value(mid)
        exposure = self.exposure_allowed_sides(self.trading_pair, mid)
        allow_buy, allow_sell = exposure["buy"], exposure["sell"]
        # The gate ships OFF for R-Grid; when an operator or the overlay arms it,
        # honour it as reduce-only rather than a full stop.
        await self.evaluate_quote_gate(self.trading_pair)
        if self.gate_paused:
            allow_buy = allow_buy and base_value < 0     # only reduce a short
            allow_sell = allow_sell and base_value > 0   # only reduce a long

        # One taker at a time: let the previous order settle (and be absorbed into
        # the anchor) before considering the next step.
        if self._taker_in_flight():
            return

        outcome = await self._maybe_soft_reset(mid)
        if outcome == "fired":
            self._exit_failures = 0
            return
        if outcome == "failed":
            # A close we could not place must NOT fall through into the add branch:
            # piling risk onto a position we just failed to reduce is the wrong
            # default for a safety action. But returning early FOREVER wedges the
            # strategy — it stops exiting AND stops entering, and on_tick returning
            # normally means the orchestrator never sees a failure. Bound it, shout,
            # and let the session SL/TP rail be the backstop past that.
            self._exit_failures += 1
            if self._exit_failures <= _MAX_CONSECUTIVE_EXIT_FAILURES:
                logger.warning(
                    "rgrid: soft-reset exit was refused (%s/%s) — skipping the add "
                    "branch this tick (user=%s pair=%s)",
                    self._exit_failures, _MAX_CONSECUTIVE_EXIT_FAILURES,
                    self.user_id, self.trading_pair,
                )
                return
            logger.error(
                "rgrid: soft-reset exit refused %s ticks running (user=%s pair=%s) — "
                "the position cannot be reduced by this controller; the session "
                "SL/TP rail is now the only exit",
                self._exit_failures, self.user_id, self.trading_pair,
            )
            return
        self._exit_failures = 0

        # Flat again after banking: the window still holds the OLD entries, whose
        # average sits far from the new mid — enough on its own to breach the band
        # and re-enter immediately. Require a genuine fresh break instead.
        if self._net_base() == 0 and self._has_fills():
            self._reset_exposure_window(mid)

        anchor = self.exposure_anchor(mid)
        if anchor is None or anchor <= 0:
            return
        self._last_anchor = anchor
        band = self._band()
        buy_trigger = anchor * (Decimal(1) + band)
        sell_trigger = anchor * (Decimal(1) - band)
        net = self._net_base()
        if mid >= buy_trigger:
            if net < 0:
                # Short book, price broke UP: the move that opened it has turned.
                # Get flat in ONE order and re-anchor — nibbling a step per tick
                # leaves most of the position on the wrong side of a running move.
                await self._fire_reversal_flatten(net, mid)
            elif allow_buy:
                await self._fire_taker(TradeType.BUY, mid, intent=INTENT_ENTRY)
        elif mid <= sell_trigger:
            if net > 0:
                await self._fire_reversal_flatten(net, mid)   # long book, break DOWN
            elif allow_sell:
                await self._fire_taker(TradeType.SELL, mid, intent=INTENT_ENTRY)

    # -- introspection -------------------------------------------------------
    def anchor_state(self) -> dict:
        return {
            "mode": "rgrid",
            "anchor": self._last_anchor or self._anchor,
            "buy_exposure_px": self.leg_exposure_price(BUY),
            "sell_exposure_px": self.leg_exposure_price(SELL),
            "trail_armed": self._trail_armed,
            "trail_peak": self._trail_peak,
        }

    def grid_metrics(self) -> dict:
        """Telemetry for /status and the order-monitor card. Deliberately reuses the
        shared ``grid_*`` keys the runtime already persists, plus rgrid-only ones."""
        anchor = self._last_anchor or self._anchor
        band = self._band()
        up_price = down_price = 0.0
        if anchor and anchor > 0:
            up_price = float(anchor * (Decimal(1) + band))
            down_price = float(anchor * (Decimal(1) - band))
        net_base = 0.0
        if self.inventory is not None:
            net_base = float(self.inventory.get(self.user_id, self.trading_pair, self.id).net_amount_base)
        # SHARED grid telemetry block. These exact key names are what the runtime
        # persists into bot_state and the /status card renders as Anchor / Drift /
        # Side (bot_runtime maps grid_* -> rgrid_* for the card). Emitting fewer of
        # them than the previous controller did leaves the card showing 0.000% /
        # NONE — or worse, the STALE value from the old controller, because the
        # runtime only overwrites a key it is actually given.
        drift_pct = 0.0
        if anchor and anchor > 0 and self._last_mid:
            drift_pct = float((self._last_mid - anchor) / anchor * Decimal(100))
        # Which leg the soft reset is protecting: the EXIT of the open position.
        if not self._trail_armed or net_base == 0:
            reset_side = "NONE"
        else:
            reset_side = "SELL" if net_base > 0 else "BUY"
        return {
            "grid_mode": "rgrid",
            "grid_anchor_price": float(anchor) if anchor else 0.0,
            "grid_drift_from_anchor_pct": drift_pct,
            "grid_reset_side": reset_side,
            "grid_reset_threshold_bp": float(self.reset_threshold_pct * Decimal(10000)),
            "grid_reset_active": bool(self.trail_enabled and self.reset_threshold_pct > 0),
            "grid_soft_reset_engaged": bool(self._trail_armed),
            "grid_net_base": net_base,
            # The two legs the anchor averages — the R-Grid card has always had a
            # "Buy VWAP / Sell VWAP" row and nothing ever populated it.
            "grid_buy_exposure_price": float(self.leg_exposure_price(BUY) or 0),
            "grid_sell_exposure_price": float(self.leg_exposure_price(SELL) or 0),
            # Where the next break fires: buy above the green level, sell below red.
            "grid_reset_up_price": up_price,
            "grid_reset_down_price": down_price,
            "rgrid_buy_trigger": up_price,
            "rgrid_sell_trigger": down_price,
            "rgrid_trail_armed": bool(self._trail_armed),
            "rgrid_trail_peak": float(self._trail_peak) if self._trail_peak else 0.0,
            "rgrid_signal_regime": self.signal_regime,
            "rgrid_signal_confidence": self.signal_confidence,
        }
