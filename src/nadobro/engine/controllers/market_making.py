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
        mid = await self.adapter.mid_price(self.trading_pair)
        base_value = self._base_value(mid)
        at_max = self.max_base_quote is not None and base_value >= self.max_base_quote
        at_min = self.min_base_quote is not None and base_value <= self.min_base_quote
        allow_buy = not at_max          # stop buying above the inventory ceiling
        allow_sell = not at_min         # stop selling below the inventory floor
        if self.profit_protection and at_max and self._unrealized(mid) < 0:
            allow_buy = allow_sell = False

        target_bid = mid * (Decimal(1) - self.spread_bid_pct)
        target_ask = mid * (Decimal(1) + self.spread_ask_pct)
        await self._reconcile(TradeType.BUY, target_bid, allow_buy)
        await self._reconcile(TradeType.SELL, target_ask, allow_sell)

    async def _reconcile(self, side: TradeType, target: Decimal, allowed: bool) -> None:
        is_bid = side is TradeType.BUY
        cur_id = self._bid_id if is_bid else self._ask_id
        cur_price = self._bid_price if is_bid else self._ask_price

        if not allowed:
            if cur_id is not None:
                await self.orchestrator.stop(cur_id)
                self._set_quote(is_bid, None, None)
            return

        if cur_id is not None and cur_price is not None:
            ex = self.orchestrator.get(cur_id)
            if ex is not None and not ex.is_terminated:
                if abs(target - cur_price) / cur_price <= self.price_distance_tolerance:
                    return  # within tolerance — leave the resting quote
                await self.orchestrator.stop(cur_id)

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
