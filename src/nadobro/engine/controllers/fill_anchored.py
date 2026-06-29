"""Fill-anchored quoting — TreadFi-style Grid / RGrid (Phase 4, opt-in).

Where the classic ladder anchors a fixed band at spawn time, this controller
quotes ONE bid + ONE ask around a *reference price* that moves with fills:

- **grid** mode: reference = the LAST EXECUTED FILL, with the no-cross
  invariant — never place a buy above the last sell, never a sell below the
  last buy. Captures spread in chop; each round trip re-anchors the pair.
- **rgrid** mode: reference = rolling VWAP of recent fills (the "exposure
  price"). No cross-invariant — the quotes follow the trend, locking profit
  on the way.

**Soft reset** (grid): when mid drifts further than ``reset_threshold_pct``
from the reference, quotes temporarily re-anchor to mid (the ref-anchored
pair is either stranded or un-postable post-only) — the next fill snaps the
reference back to reality and grid pricing resumes. No flatten, no restart.

Inherits MarketMakingController's executor plumbing, regime gate
(pause = reduce-only, never flatten), inventory cap, and ATR auto-spread.
Selected via the ``fill_anchored`` strategy setting; the classic ladder
remains the default.
"""
from __future__ import annotations

from collections import deque
from decimal import Decimal
from typing import Deque, Optional, Tuple

from src.nadobro.engine.controllers.market_making import MarketMakingController
from src.nadobro.engine.executors.order_executor import OrderExecutor, OrderExecutorConfig
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.types import ExecutionStrategy, TradeType, _dec

# How many recent fills to retain for the exposure-price VWAP. Large enough that
# a volume-fraction window (rgrid_discretion) has history to work with.
_FILL_HISTORY = 200


class FillAnchoredQuotingController(MarketMakingController):
    def __init__(self, **kwargs: object) -> None:
        kwargs.setdefault("name", "fill_anchored")
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.mode = str(self.cfg("anchor_mode", "grid")).lower()
        self.reset_threshold_pct = _dec(self.cfg(
            "reset_threshold_pct", "0.0025" if self.mode == "grid" else "0.00125"
        ))
        self._reference: Optional[Decimal] = None
        self._last_buy_px: Optional[Decimal] = None
        self._last_sell_px: Optional[Decimal] = None
        self._fills: Deque[Tuple[Decimal, Decimal]] = deque(maxlen=_FILL_HISTORY)
        self._seen_filled: set[str] = set()
        # rgrid taker-momentum: instead of resting maker quotes, fire a TAKER
        # market order when price breaks the exposure band — buy as price rises
        # above ref·(1+spread), sell as it falls below ref·(1-spread). The
        # exposure VWAP re-anchors after each fill, so it steps WITH the trend
        # (adds longs into a pump / shorts into a dump) and self-throttles. Off
        # ⇒ classic fill-anchored maker quoting (unchanged).
        self.momentum = bool(self.cfg("momentum", False))
        # Exposure-price VWAP window as a fraction of recent fill VOLUME
        # (rgrid_discretion). 0 ⇒ VWAP over the whole retained window.
        self.vwap_volume_fraction = _dec(self.cfg("vwap_volume_fraction", "0") or "0")
        self._pending_taker_id: Optional[str] = None

    # -- fill anchoring -----------------------------------------------------
    def _absorb_fills(self) -> None:
        """Re-anchor on every child quote that terminated FILLED."""
        from src.nadobro.engine.adapter.base import OrderState

        for ex in self.my_executors(active_only=False):
            if ex.id in self._seen_filled:
                continue
            order = getattr(ex, "order", None)
            if order is None or order.state is not OrderState.FILLED:
                continue
            self._seen_filled.add(ex.id)
            base = abs(_dec(order.filled_base))
            quote = abs(_dec(order.filled_quote))
            if base <= 0:
                continue
            px = quote / base
            self._reference = px
            self._fills.append((px, base))
            side = getattr(getattr(ex, "config", None), "side", None)
            if side is TradeType.BUY:
                self._last_buy_px = px
            elif side is TradeType.SELL:
                self._last_sell_px = px
        # Bounded: forget terminated executors the orchestrator has dropped.
        if len(self._seen_filled) > 200:
            live = {e.id for e in self.my_executors(active_only=False)}
            self._seen_filled &= live

    def _exposure_vwap(self) -> Optional[Decimal]:
        if not self._fills:
            return None
        # Windowed by the most-recent fraction of fill volume when discretion is
        # set (a tighter, more reactive exposure price); else VWAP over all
        # retained fills.
        if self.vwap_volume_fraction > 0:
            total_base = sum((b for _, b in self._fills), Decimal(0))
            if total_base <= 0:
                return None
            want = total_base * self.vwap_volume_fraction
            num = Decimal(0)
            den = Decimal(0)
            for p, b in reversed(self._fills):  # most recent first
                num += p * b
                den += b
                if den >= want:
                    break
            return num / den if den > 0 else None
        total_base = sum((b for _, b in self._fills), Decimal(0))
        if total_base <= 0:
            return None
        return sum((p * b for p, b in self._fills), Decimal(0)) / total_base

    # -- taker momentum (rgrid) ---------------------------------------------
    def _taker_in_flight(self) -> bool:
        if self._pending_taker_id is None:
            return False
        ex = self.orchestrator.get(self._pending_taker_id)
        if ex is None or ex.is_terminated:
            self._pending_taker_id = None
            return False
        return True

    async def _fire_taker(self, side: TradeType, mid: Decimal) -> None:
        """Fire ONE reduce-or-add TAKER market order in ``side`` for the per-step
        size. The fill re-anchors the exposure VWAP, moving the next trigger with
        the trend."""
        if mid <= 0 or self.order_amount_quote <= 0:
            return
        amount_base = self.order_amount_quote / mid
        cfg = OrderExecutorConfig(
            self.trading_pair, side, amount_base, ExecutionStrategy.MARKET
        )
        ex = OrderExecutor(
            cfg, user_id=self.user_id, controller_id=self.id,
            adapter=self.adapter, inventory=self.inventory,
        )
        ok = await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=self.order_amount_quote)
        )
        if ok:
            self._pending_taker_id = ex.id

    async def _tick_momentum(self, mid: Decimal) -> None:
        base_value = self._base_value(mid)
        exposure = self.exposure_allowed_sides(self.trading_pair, mid)
        allow_buy = exposure["buy"]
        allow_sell = exposure["sell"]
        await self.evaluate_quote_gate(self.trading_pair)
        if self.gate_paused:
            allow_buy = allow_buy and base_value < 0    # only reduce a short
            allow_sell = allow_sell and base_value > 0  # only reduce a long
        # One taker at a time: wait for the prior market order to settle (and be
        # absorbed into the exposure VWAP) before considering the next step.
        if self._taker_in_flight():
            return
        band = max(self.spread_ask_pct, self.spread_floor_half_pct)
        ref = self._exposure_vwap() or self._reference or mid
        if ref <= 0:
            return
        buy_trigger = ref * (Decimal(1) + band)
        sell_trigger = ref * (Decimal(1) - band)
        if mid >= buy_trigger and allow_buy:
            await self._fire_taker(TradeType.BUY, mid)
        elif mid <= sell_trigger and allow_sell:
            await self._fire_taker(TradeType.SELL, mid)

    def _current_reference(self, mid: Decimal) -> Decimal:
        if self.mode == "rgrid":
            return self._exposure_vwap() or self._reference or mid
        return self._reference or mid

    # -- quoting --------------------------------------------------------------
    async def on_tick(self) -> None:
        # Absorb fills BEFORE pricing (the whole point of fill anchoring).
        for ex in self.my_executors(active_only=True):
            await self.orchestrator.tick(ex.id)
        self._absorb_fills()

        mid = await self.adapter.mid_price(self.trading_pair)
        if mid <= 0:
            return

        # rgrid taker-momentum: wait for a directional break from the anchor, then
        # follow it. Seed the anchor to the start mid so the FIRST trigger is a
        # real ±band move (no immediate one-sided short like the classic ladder).
        if self.mode == "rgrid" and self.momentum:
            if self._reference is None:
                self._reference = mid
            await self._tick_momentum(mid)
            return

        base_value = self._base_value(mid)

        exposure = self.exposure_allowed_sides(self.trading_pair, mid)
        allow_buy = exposure["buy"]
        allow_sell = exposure["sell"]

        await self.evaluate_quote_gate(self.trading_pair)
        if self.gate_paused:
            allow_buy = allow_buy and base_value < 0
            allow_sell = allow_sell and base_value > 0

        if self.auto_spread and self.gate_atr_pct > 0:
            half = _dec(str(self.gate_atr_pct)) * self.auto_spread_k / Decimal(2)
            half = max(self.spread_floor_half_pct, min(half, self.spread_cap_half_pct))
            self.spread_bid_pct = half
            self.spread_ask_pct = half

        ref = self._current_reference(mid)
        # Soft reset: mid escaped the reference band — ref-anchored quotes are
        # stranded (behind) or un-postable post-only (front). Re-anchor to mid
        # until the next fill restores a live reference.
        drift = abs(mid - ref) / ref if ref > 0 else Decimal(0)
        anchor = mid if drift > self.reset_threshold_pct else ref

        target_bid = anchor * (Decimal(1) - self.spread_bid_pct)
        target_ask = anchor * (Decimal(1) + self.spread_ask_pct)
        if self.mode == "grid":
            # No-cross invariant: never buy above the last sell, never sell
            # below the last buy — each round trip must capture spread.
            if self._last_sell_px is not None:
                target_bid = min(target_bid, self._last_sell_px * (Decimal(1) - self.spread_bid_pct))
            if self._last_buy_px is not None:
                target_ask = max(target_ask, self._last_buy_px * (Decimal(1) + self.spread_ask_pct))

        await self._reconcile(TradeType.BUY, target_bid, allow_buy)
        await self._reconcile(TradeType.SELL, target_ask, allow_sell)

    # Introspection for dashboards/tests.
    def anchor_state(self) -> dict:
        return {
            "mode": self.mode,
            "reference": self._reference,
            "last_buy_px": self._last_buy_px,
            "last_sell_px": self._last_sell_px,
            "exposure_vwap": self._exposure_vwap(),
        }
