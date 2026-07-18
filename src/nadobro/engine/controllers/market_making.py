"""Market Making controller — symmetric maker quoting around mid with
inventory skew and profit protection. Replaces the legacy MM / Mid Mode.

Each tick: read mid, compute target bid/ask. If a resting quote is within
``price_distance_tolerance`` of the new target it is left alone; otherwise it
is cancelled and a fresh ``OrderExecutor(LIMIT_MAKER)`` is placed. Inventory
gating: above ``max_base_quote`` stop buying; with no base stop selling.
``profit_protection``: at max inventory with negative unrealized PnL, suspend.

Per-product daily-loss / drawdown / cost gating is enforced upstream by the
Risk Engine via the orchestrator's pre-tick check.

Implemented in Phase 4.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional

from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.executors.order_executor import OrderExecutor, OrderExecutorConfig
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.types import ExecutionStrategy, TradeType, _dec

# Directional bias maps linearly to the documented alpha-tilt: ±1 bias → ±0.2.
# As a per-side spread skew this means a full long bias quotes the bid at 0.8×
# the spread (closer to mid → front-loads buys) and the ask at 1.2× (further →
# back-loads sells); short bias is the mirror. Bounded so neither factor goes
# non-positive for bias in [-1, 1].
_BIAS_SKEW_STRENGTH = Decimal("0.2")


def _safe_bias(value: object) -> Decimal:
    """Parse directional_bias, clamped to [-1, 1]. Tolerates the legacy text
    default ("neutral") and any unparseable value by treating it as 0 (neutral)."""
    try:
        b = _dec(value)
    except Exception:  # noqa: BLE001 - "neutral"/None/garbage → neutral
        return Decimal(0)
    return max(Decimal(-1), min(Decimal(1), b))


class MarketMakingController(Controller):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(name=kwargs.pop("name", "market_making"), **kwargs)  # type: ignore[arg-type]
        self.trading_pair = str(self.cfg("trading_pair"))
        self.spread_bid_pct = _dec(self.cfg("spread_bid_pct", "0.001"))
        self.spread_ask_pct = _dec(self.cfg("spread_ask_pct", "0.001"))
        self.order_amount_quote = _dec(self.cfg("order_amount_quote", "10"))
        self.price_distance_tolerance = _dec(self.cfg("price_distance_tolerance", "0.0005"))
        _mb = self.cfg("max_base_quote")
        self.max_base_quote = _dec(_mb) if _mb is not None else None
        _nb = self.cfg("min_base_quote")
        self.min_base_quote = _dec(_nb) if _nb is not None else None
        self.profit_protection = bool(self.cfg("profit_protection", False))
        # ATR auto-spread (Phase 3): when enabled, the per-side spread tracks
        # k x ATR / 2, clamped to [floor, cap]. The floor must clear fees +
        # adverse selection — quoting below it pays to trade.
        self.auto_spread = bool(self.cfg("auto_spread", False))
        self.auto_spread_k = _dec(self.cfg("auto_spread_k", "1.5"))
        self.spread_floor_half_pct = _dec(self.cfg("spread_floor_half_pct", "0.00015"))
        self.spread_cap_half_pct = _dec(self.cfg("spread_cap_half_pct", "0.005"))
        # MM-SPREAD-FLOOR fix: the auto-spread path clamps each side to
        # spread_floor_half_pct (the fee-clearing minimum), but a MANUAL spread
        # was applied verbatim — a user could quote a sub-fee book that loses on
        # every fill. Floor the manual per-side spread at the same minimum.
        self.spread_bid_pct = max(self.spread_bid_pct, self.spread_floor_half_pct)
        self.spread_ask_pct = max(self.spread_ask_pct, self.spread_floor_half_pct)
        # Directional bias (Mid Mode): lean the book long (>0) / short (<0) by
        # skewing the per-side spreads. 0 = symmetric (default). Previously the
        # user's directional_bias setting only changed the preview math and was
        # never applied to live quoting — this wires it into the controller.
        self.directional_bias = _safe_bias(self.cfg("directional_bias", "0"))
        # Quote mode (Turbo Volume): "mid" (default) prices mid ± spread as
        # always; "touch" joins the best bid/ask (improving by one tick when
        # the spread leaves room) — the same maker geometry as volume_bot v3.
        # POLICY (2026-07-15): this controller is MAKER-ONLY — every order is
        # post-only; no taker leg exists (fees + Nado wash-trading policy; the
        # full rationale lives in docs/mm_volume_tuning.md).
        self.quote_mode = str(self.cfg("quote_mode", "mid") or "mid").lower()
        self._bid_id: Optional[str] = None
        self._bid_price: Optional[Decimal] = None
        self._ask_id: Optional[str] = None
        self._ask_price: Optional[Decimal] = None

    async def on_start(self) -> None:
        return None

    def _base_value(self, mid: Decimal) -> Decimal:
        if self.inventory is None:
            return Decimal(0)
        hold = self.inventory.get(self.user_id, self.trading_pair, self.id)
        return hold.net_amount_base * mid

    async def _touch_targets(self) -> Optional[tuple[Decimal, Decimal, Decimal]]:
        """(target_bid, target_ask, book_mid) glued to the touch, or None when
        the book has no live two-sided touch (dead/one-sided book -> caller
        falls back to mid ± spread pricing). Same join/improve geometry as
        volume_bot v3: join the best bid/ask, improve by one tick when the
        spread leaves at least two ticks of room (price-time priority puts the
        improver first). ``book_mid`` rides along so touch mode needs ONE
        market-data call per tick — mid_price() is itself an order_book fetch,
        and fetching both doubled the per-tick hit on the shared IP budget."""
        try:
            book = await self.adapter.order_book(self.trading_pair)
            bid, ask = book.best_bid, book.best_ask
        except Exception:  # noqa: BLE001 - a dead feed falls back to mid pricing
            return None
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            return None
        # AUDIT-MM-2026-07-14 #7: a degraded feed substitutes bid = ask = mid
        # (and a crossed venue book inverts them) — neither is a real touch to
        # join. Fall back to mid ± spread pricing instead of quoting AT mid.
        if bid >= ask:
            return None
        try:
            tick = self.adapter.tick_size(self.trading_pair)
        except Exception:  # noqa: BLE001
            tick = Decimal(0)
        target_bid, target_ask = bid, ask
        if tick and tick > 0 and (ask - bid) >= tick * 2:
            target_bid = bid + tick
            target_ask = ask - tick
        if target_bid >= target_ask:  # one-tick book after improve — join only
            target_bid, target_ask = bid, ask
        return target_bid, target_ask, (bid + ask) / Decimal(2)

    def _unrealized(self, mid: Decimal) -> Decimal:
        if self.inventory is None:
            return Decimal(0)
        return self.inventory.get(self.user_id, self.trading_pair, self.id).unrealized_pnl(mid)

    async def on_tick(self) -> None:
        # BUG-MM-1 fix: tick all child OrderExecutors FIRST so fills are
        # absorbed into inventory before we read base_value for the
        # inventory-skew decision. Without this, the MM controller is blind
        # to its own fills and keeps stale quotes indefinitely.
        for ex in self.my_executors(active_only=True):
            await self.orchestrator.tick(ex.id)

        # ONE market-data call per tick: in touch mode the order-book snapshot
        # provides both the touch targets and the mid (mid_price() is itself an
        # order_book fetch — calling both doubled the per-tick hit on the
        # shared per-IP query budget). Dead/degraded book -> classic mid fetch.
        touch = await self._touch_targets() if self.quote_mode == "touch" else None
        mid = touch[2] if touch is not None else await self.adapter.mid_price(self.trading_pair)
        base_value = self._base_value(mid)
        at_max = self.max_base_quote is not None and base_value >= self.max_base_quote
        at_min = self.min_base_quote is not None and base_value <= self.min_base_quote
        allow_buy = not at_max          # stop buying above the inventory ceiling
        allow_sell = not at_min         # stop selling below the inventory floor
        if self.profit_protection and at_max and self._unrealized(mid) < 0:
            allow_buy = allow_sell = False

        # Inventory cap (Phase 1): margin-relative net-exposure backstop with
        # hysteresis — suppress the side that worsens exposure, keep the
        # reducing side quoting so the book can trim back toward neutral.
        exposure = self.exposure_allowed_sides(self.trading_pair, mid)
        allow_buy = allow_buy and exposure["buy"]
        allow_sell = allow_sell and exposure["sell"]

        # Regime gate (Phase 2): in PAUSE, place no NEW exposure — quote only
        # the side that reduces the current net position (the exit path); a
        # flat book quotes nothing until the regime reads ranging again.
        await self.evaluate_quote_gate(self.trading_pair)
        if self.gate_paused:
            net = base_value
            allow_buy = allow_buy and net < 0    # buying only reduces a short
            allow_sell = allow_sell and net > 0  # selling only reduces a long

        # ATR auto-spread (Phase 3): scale the quoted spread with realized
        # volatility so captured edge stays ahead of fees as conditions move.
        if self.auto_spread and self.gate_atr_pct > 0:
            half = _dec(str(self.gate_atr_pct)) * self.auto_spread_k / Decimal(2)
            half = max(self.spread_floor_half_pct, min(half, self.spread_cap_half_pct))
            self.spread_bid_pct = half
            self.spread_ask_pct = half

        # Directional bias: skew the per-side spreads so the favored side quotes
        # closer to mid (fills more) and the other side further (fills less),
        # accumulating the desired inventory lean. Floored so neither side quotes
        # through the fee-clearing minimum; bias=0 leaves the quotes symmetric.
        eff_bid_pct = self.spread_bid_pct
        eff_ask_pct = self.spread_ask_pct
        if self.directional_bias != 0:
            skew = self.directional_bias * _BIAS_SKEW_STRENGTH
            eff_bid_pct = max(self.spread_floor_half_pct, self.spread_bid_pct * (Decimal(1) - skew))
            eff_ask_pct = max(self.spread_floor_half_pct, self.spread_ask_pct * (Decimal(1) + skew))

        target_bid = mid * (Decimal(1) - eff_bid_pct)
        target_ask = mid * (Decimal(1) + eff_ask_pct)
        # Touch mode (Turbo Volume): glue quotes to the live touch instead of
        # mid ± spread (targets computed once at the top of the tick).
        # Bias/auto-spread math above still ran — it provides the fallback
        # targets when the book has no two-sided touch. The fee-floor spread
        # does NOT apply here by design: volume mode deliberately trades
        # per-fill edge for fill rate, bounded by the session SL rail.
        if touch is not None:
            target_bid, target_ask = touch[0], touch[1]
        await self._reconcile(TradeType.BUY, target_bid, allow_buy, mid)
        await self._reconcile(TradeType.SELL, target_ask, allow_sell, mid)

    def _projected_order_within_exposure(
        self, side: TradeType, mid: Decimal
    ) -> bool:
        """Whether another full quote fits the margin-relative exposure cap.

        The existing gate observes filled inventory only. Turbo Mid quotes one
        full deployed notional per side, so inventory just below the cap after
        an adverse mark could otherwise admit a second full-size order and jump
        to almost 2x the promised limit. Reducing orders are always allowed,
        even when they cannot bring an already-oversized position below the cap
        in one fill.
        """
        if self.inventory is None or mid <= 0:
            return True
        cap_pct = self.cfg("max_net_exposure_pct")
        margin = self.cfg("margin_quote")
        if cap_pct is None or margin is None:
            return True
        try:
            cap_quote = _dec(margin) * _dec(cap_pct) / Decimal(100)
        except Exception:  # noqa: BLE001 - malformed/unset cap keeps legacy behavior
            return True
        if cap_quote <= 0:
            return True
        current_quote = self._base_value(mid)
        delta_quote = self.order_amount_quote if side is TradeType.BUY else -self.order_amount_quote
        projected_quote = current_quote + delta_quote
        # Never block an order that reduces absolute exposure. For a worsening
        # order, equality is allowed so a flat Turbo session can place its first
        # full-size quote exactly at the configured cap.
        worsens = abs(projected_quote) > abs(current_quote)
        return not (worsens and abs(projected_quote) > cap_quote)

    async def _reconcile(
        self, side: TradeType, target: Decimal, allowed: bool, mid: Decimal
    ) -> None:
        is_bid = side is TradeType.BUY
        cur_id = self._bid_id if is_bid else self._ask_id
        cur_price = self._bid_price if is_bid else self._ask_price

        # Include the next order in the exposure decision, not only inventory
        # that has already filled. This also cancels a partially filled resting
        # quote once its remaining direction would breach the cap.
        allowed = allowed and self._projected_order_within_exposure(side, mid)

        # BUG-MM-2 fix: if the recorded quote already terminated (filled or
        # cancelled by the venue), forget it so we don't skip re-spawning a
        # fresh one just because the stale price was "close enough".
        if cur_id is not None:
            ex_existing = self.orchestrator.get(cur_id)
            if ex_existing is None or ex_existing.is_terminated:
                self._set_quote(is_bid, None, None)
                cur_id, cur_price = None, None

        if not allowed:
            if cur_id is not None:
                await self.orchestrator.stop(cur_id)
                self._set_quote(is_bid, None, None)
            return

        if cur_id is not None and cur_price is not None:
            ex = self.orchestrator.get(cur_id)
            if ex is not None and not ex.is_terminated:
                if self._within_tolerance(target, cur_price):
                    return  # within tolerance — leave the resting quote
                await self.orchestrator.stop(cur_id)
                self._set_quote(is_bid, None, None)

        # BUG-MM-3 fix: guard against ZeroDivisionError when target collapses
        # to 0 (e.g. mid feed returned 0 and spread_*_pct is 1).
        if target <= 0:
            return
        amount_base = self.order_amount_quote / target
        cfg = OrderExecutorConfig(
            self.trading_pair, side, amount_base, ExecutionStrategy.LIMIT_MAKER, price=target
        )
        ex = OrderExecutor(
            cfg, user_id=self.user_id, controller_id=self.id, adapter=self.adapter,
            inventory=self.inventory,
        )
        ok = await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=self.order_amount_quote)
        )
        if ok:
            self._set_quote(is_bid, ex.id, target)

    def _within_tolerance(self, target: Decimal, cur_price: Decimal) -> bool:
        """Is the resting quote close enough to the new target to leave alone?

        Mid mode keeps the relative ``price_distance_tolerance`` (half-spread).
        Touch mode must TRACK the touch: half a spread behind the best bid is
        no longer at the touch at all, so staleness there is ONE venue tick —
        inclusive. AUDIT-MM-2026-07-14 #6: the venue BBO includes our own
        resting quote; once we ARE the touch, the improve rule computes
        target = our_price + tick every tick. A strict (< tick) tolerance
        cancelled and re-improved on our own reflection endlessly, walking
        both quotes inward until they sat 1-2 ticks apart. Inclusive (<= tick)
        parks the quote once it is at-or-one-tick-inside the touch."""
        if cur_price <= 0:
            return False
        if self.quote_mode == "touch":
            try:
                tick = self.adapter.tick_size(self.trading_pair)
            except Exception:  # noqa: BLE001
                tick = Decimal(0)
            if tick and tick > 0:
                return abs(target - cur_price) <= tick
        return abs(target - cur_price) / cur_price <= self.price_distance_tolerance

    def _set_quote(self, is_bid: bool, ex_id: Optional[str], price: Optional[Decimal]) -> None:
        if is_bid:
            self._bid_id, self._bid_price = ex_id, price
        else:
            self._ask_id, self._ask_price = ex_id, price
