"""Volume Bot controller — spot-only wash-cycle, preserving the PR #72/#73
spot-only cutover. Hard-coded to spot (leverage 1) on the supported pairs;
perp configs are rejected at construction. Runs a MAKER TWAP buy half then a
MAKER TWAP sell-cleanup half. The daily-volume cap is enforced upstream by the
Risk Engine.

Implemented in Phase 4.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional

from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.executors.twap_executor import TWAPExecutor, TWAPExecutorConfig
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.types import TradeType, _dec

SUPPORTED_SPOT_PAIRS = {"KBTC-USDC", "WETH-USDC"}


class VolumeBotController(Controller):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(name="volume_bot", **kwargs)  # type: ignore[arg-type]
        self.trading_pair = str(self.cfg("trading_pair"))
        market = str(self.cfg("market", "spot")).lower()
        leverage = int(self.cfg("leverage", 1))
        if market != "spot":
            raise ValueError("VolumeBotController is spot-only (market must be 'spot')")
        if leverage != 1:
            raise ValueError("VolumeBotController is spot-only (leverage must be 1)")
        if self.trading_pair not in SUPPORTED_SPOT_PAIRS:
            raise ValueError(f"{self.trading_pair} is not a supported volume-bot spot pair")
        self.total_amount_quote = _dec(self.cfg("total_amount_quote", "50"))
        self.total_duration = float(self.cfg("total_duration", 600))
        self.order_interval = float(self.cfg("order_interval", 60))
        self.phase = "buying"
        self.buy_id: Optional[str] = None
        self.sell_id: Optional[str] = None

    def _twap(self, side: TradeType, amount_quote: Decimal) -> TWAPExecutor:
        cfg = TWAPExecutorConfig(
            self.trading_pair, side, amount_quote, self.total_duration,
            self.order_interval, mode="MAKER", leverage=1,
        )
        return TWAPExecutor(cfg, user_id=self.user_id, controller_id=self.id,
                            adapter=self.adapter, inventory=self.inventory)

    async def on_start(self) -> None:
        ex = self._twap(TradeType.BUY, self.total_amount_quote)
        ok = await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=self.total_amount_quote)
        )
        if ok:
            self.buy_id = ex.id

    async def on_tick(self) -> None:
        for ex in self.my_executors(active_only=True):
            await self.orchestrator.tick(ex.id)

        if self.phase == "buying" and self.buy_id is not None:
            buy_ex = self.orchestrator.get(self.buy_id)
            if buy_ex is not None and buy_ex.is_terminated:
                sell_notional = getattr(buy_ex, "filled_quote", Decimal(0)) or Decimal(0)
                if sell_notional > 0:
                    sell = self._twap(TradeType.SELL, sell_notional)
                    ok = await self.spawn_executor(
                        sell, ExecutorRequest(order_amount_quote=sell_notional)
                    )
                    if ok:
                        self.sell_id = sell.id
                        self.phase = "selling"
                    else:
                        self.phase = "done"
                else:
                    self.phase = "done"
        elif self.phase == "selling" and self.sell_id is not None:
            sell_ex = self.orchestrator.get(self.sell_id)
            if sell_ex is not None and sell_ex.is_terminated:
                self.phase = "done"
