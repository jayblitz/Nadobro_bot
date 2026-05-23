"""Copy Trading controller — mirrors a leader's signals.

Each tick: poll the injected signal source. For every new signal, apply the
slippage gate (skip if mid has moved more than ``max_slippage_pct`` since the
signal price), then spawn a PositionExecutor that opens the entry (MARKET) and
manages the exit with TP/SL inherited from the signal (or a user override).

Implemented in Phase 4.
"""
from __future__ import annotations

import inspect
import logging
from decimal import Decimal
from typing import List, Optional, Set

from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.executors.order_executor import OrderExecutorConfig
from src.nadobro.engine.executors.position_executor import PositionExecutor, PositionExecutorConfig
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.types import (
    ExecutionStrategy,
    TradeType,
    TrailingStop,
    TripleBarrierConfig,
    _dec,
)

logger = logging.getLogger(__name__)


class SignalDedupeStore:
    """Persistence hook for already-processed signals. Default no-op keeps
    in-memory-only behavior. Production wiring should inject a DB-backed
    impl so signals are not replayed across restarts (BUG-CT-1).
    """

    def is_processed(self, controller_id: str, signal_id: str) -> bool:  # noqa: ARG002
        return False

    def mark_processed(self, controller_id: str, signal_id: str) -> None:  # noqa: ARG002
        return None


class CopyController(Controller):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(name="copy_trading", **kwargs)  # type: ignore[arg-type]
        self.max_slippage_pct = _dec(self.cfg("max_slippage_pct", "0.01"))
        self.processed: Set[str] = set()
        self.skipped: List[str] = []
        store = self.cfg("dedupe_store")
        self._dedupe: SignalDedupeStore = (
            store if isinstance(store, SignalDedupeStore) else SignalDedupeStore()
        )

    def _seen(self, sig_id: str) -> bool:
        if sig_id in self.processed:
            return True
        try:
            return bool(self._dedupe.is_processed(self.id, sig_id))
        except Exception:  # noqa: BLE001
            return False

    def _mark(self, sig_id: str) -> None:
        self.processed.add(sig_id)
        try:
            self._dedupe.mark_processed(self.id, sig_id)
        except Exception:  # noqa: BLE001
            logger.warning("copy_trading: dedupe persistence failed for %s", sig_id, exc_info=True)

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
            if not sig_id or self._seen(sig_id):
                continue
            # Mark *before* handling so a partial-failure that retries the
            # signal source won't double-spawn the same trade.
            self._mark(sig_id)
            try:
                await self._handle_signal(sig)
            except (KeyError, ValueError, TypeError) as exc:
                # BUG-CT-3 fix: malformed signals (missing pair/side/price)
                # must skip rather than kill the controller.
                logger.warning("copy_trading: skipping malformed signal %s: %s", sig_id, exc)
                self.skipped.append(sig_id)
            except Exception:  # noqa: BLE001
                logger.exception("copy_trading: unexpected error handling signal %s", sig_id)
                self.skipped.append(sig_id)

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
        # BUG-CT-2 fix: thread time_limit and trailing_stop through from the
        # signal so leader-defined exit conditions are honored.
        time_limit = sig.get("time_limit")
        trailing_stop = sig.get("trailing_stop")
        if trailing_stop is not None and not isinstance(trailing_stop, TrailingStop):
            try:
                trailing_stop = TrailingStop(
                    activation_price=_dec(trailing_stop["activation_price"]),
                    trailing_delta=_dec(trailing_stop["trailing_delta"]),
                )
            except (KeyError, TypeError, ValueError):
                trailing_stop = None
        barriers = TripleBarrierConfig(
            take_profit=(_dec(sig["take_profit"]) if sig.get("take_profit") is not None else None),
            stop_loss=(_dec(sig["stop_loss"]) if sig.get("stop_loss") is not None else None),
            time_limit=int(time_limit) if time_limit is not None else None,
            trailing_stop=trailing_stop,
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
