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


class MarketMakingController(Controller):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(name="market_making", **kwargs)  # type: ignore[arg-type]
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

        mid = await self.adapter.mid_price(self.trading_pair)
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

        target_bid = mid * (Decimal(1) - self.spread_bid_pct)
        target_ask = mid * (Decimal(1) + self.spread_ask_pct)
        await self._reconcile(TradeType.BUY, target_bid, allow_buy)
        await self._reconcile(TradeType.SELL, target_ask, allow_sell)

    async def _reconcile(self, side: TradeType, target: Decimal, allowed: bool) -> None:
        is_bid = side is TradeType.BUY
        cur_id = self._bid_id if is_bid else self._ask_id
        cur_price = self._bid_price if is_bid else self._ask_price

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
                if cur_price > 0 and abs(target - cur_price) / cur_price <= self.price_distance_tolerance:
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

    def _set_quote(self, is_bid: bool, ex_id: Optional[str], price: Optional[Decimal]) -> None:
        if is_bid:
            self._bid_id, self._bid_price = ex_id, price
        else:
            self._ask_id, self._ask_price = ex_id, price
