"""Copy Trading controller — mirrors a leader's signals.

Each tick: poll the injected signal source. For every new signal, apply the
slippage gate (skip if mid has moved more than ``max_slippage_pct`` since the
signal price), then spawn a PositionExecutor that opens the entry (MARKET) and
manages the exit with TP/SL inherited from the signal (or a user override).

Implemented in Phase 4.
"""
from __future__ import annotations

import inspect
from decimal import Decimal
from typing import List, Optional, Set

from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.executors.order_executor import OrderExecutorConfig
from src.nadobro.engine.executors.position_executor import PositionExecutor, PositionExecutorConfig
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.types import ExecutionStrategy, TradeType, TripleBarrierConfig, _dec


class CopyController(Controller):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(name="copy_trading", **kwargs)  # type: ignore[arg-type]
        self.max_slippage_pct = _dec(self.cfg("max_slippage_pct", "0.01"))
        self.processed: Set[str] = set()
        self.skipped: List[str] = []

    async def on_start(self) -> None:
        return None

    async def _signals(self) -> List[dict]:
        source = self.cfg("signal_source")
        if source is None:
            return []
        result = source()  # type: ignore[operator]
        if inspect.isawaitable(result):
            result = await result
        return list(result or [])

    async def on_tick(self) -> None:
        for ex in self.my_executors(active_only=True):
            await self.orchestrator.tick(ex.id)

        for sig in await self._signals():
            sig_id = str(sig.get("id"))
            if not sig_id or sig_id in self.processed:
                continue
            self.processed.add(sig_id)
            await self._handle_signal(sig)

    async def _handle_signal(self, sig: dict) -> None:
        pair = str(sig["pair"])
        side = sig["side"]
        if not isinstance(side, TradeType):
            side = TradeType.BUY if str(side).upper() in ("BUY", "LONG") else TradeType.SELL
        signal_price = _dec(sig["price"])
        mid = await self.adapter.mid_price(pair)
        if signal_price > 0 and abs(mid - signal_price) / signal_price > self.max_slippage_pct:
            self.skipped.append(str(sig.get("id")))
            return
        amount_quote = _dec(sig.get("amount_quote", "10"))
        amount_base = amount_quote / mid
        barriers = TripleBarrierConfig(
            take_profit=(_dec(sig["take_profit"]) if sig.get("take_profit") is not None else None),
            stop_loss=(_dec(sig["stop_loss"]) if sig.get("stop_loss") is not None else None),
        )
        oc = OrderExecutorConfig(pair, side, amount_base, ExecutionStrategy.MARKET)
        ex = PositionExecutor(
            PositionExecutorConfig(order_config=oc, barriers=barriers),
            user_id=self.user_id, controller_id=self.id, adapter=self.adapter,
            inventory=self.inventory,
        )
        await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=amount_quote, position_size_quote=amount_quote)
        )
