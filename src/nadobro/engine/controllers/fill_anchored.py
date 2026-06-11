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
from src.nadobro.engine.types import TradeType, _dec

_VWAP_WINDOW = 20  # fills in the exposure-price window (rgrid)


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
        self._fills: Deque[Tuple[Decimal, Decimal]] = deque(maxlen=_VWAP_WINDOW)
        self._seen_filled: set[str] = set()

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
        total_base = sum((b for _, b in self._fills), Decimal(0))
        if total_base <= 0:
            return None
        return sum((p * b for p, b in self._fills), Decimal(0)) / total_base

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
