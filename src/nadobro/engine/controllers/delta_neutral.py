"""Delta Neutral controller — buy spot (long leg) + leverage-short the perp
(short leg) on the same underlying, hold the hedged pair for a configured
duration, then exit BOTH legs together so one side is never left exposed. The
strategy farms spot+perp volume and collects funding while staying delta
neutral, and can repeat for ``cycles`` rounds.

Lifecycle (driven by on_start + on_tick):

  OPENING  on_start opens both legs atomically (long first; short sized off the
           long's filled base × hedge_ratio so the legs match in BASE terms
           regardless of the spot/perp mid gap). If the short fails to spawn the
           long is rolled back — never carry an unhedged leg.
  HOLDING  hold for ``hold_seconds`` (default 1h, up to 24h). Each tick a drift
           gate flattens both legs early if the hedge breaks beyond
           ``max_drift_pct`` (safety). At hold expiry both legs are closed.
  CLOSING  reduce-only MARKET closes fired on BOTH legs concurrently (same tick
           ⇒ same minute). Wait until both terminate.
  WAITING  pause ``cycle_gap_seconds`` between cycles.
  DONE     all cycles complete (or a drift break aborted further cycles).

Margin: the short leg trades the perp; Nado RWA perps are isolated-only, so the
adapter posts isolated margin for it automatically (see engine/adapter/nado.py
+ services/margin.py). The short is strictly 1x by design (margin = full
notional), so liquidation risk on the hedge is minimal.

Implemented in Phase 4; hardened for production in engine-v2.
"""
from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from enum import Enum
from typing import Optional

from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.executor_base import Executor
from src.nadobro.engine.executors.order_executor import OrderExecutorConfig
from src.nadobro.engine.executors.position_executor import PositionExecutor, PositionExecutorConfig
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.types import CloseType, ExecutionStrategy, TradeType, TripleBarrierConfig, _dec

logger = logging.getLogger(__name__)


def _int_cfg(value: object, default: int) -> int:
    """Coerce a config value to int, defaulting only when it's missing (None) —
    NOT when it's a legitimate 0. Keeps hold_seconds=0 ("close immediately")
    distinct from "unset"."""
    if value is None:
        return default
    try:
        return int(value)  # type: ignore[call-overload]  # cfg values are int/float/str
    except (TypeError, ValueError):
        return default


class DNPhase(Enum):
    OPENING = "OPENING"
    HOLDING = "HOLDING"
    CLOSING = "CLOSING"
    WAITING = "WAITING"
    DONE = "DONE"


class DeltaNeutralController(Controller):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(name="delta_neutral", **kwargs)  # type: ignore[arg-type]
        self.long_pair = str(self.cfg("trading_pair_long"))
        self.short_pair = str(self.cfg("trading_pair_short"))
        self.hedge_ratio = _dec(self.cfg("hedge_ratio", "1"))
        self.leg_amount_quote = _dec(self.cfg("leg_amount_quote", "50"))
        self.max_drift_pct = _dec(self.cfg("max_drift_pct", "0.05"))
        # Hold the hedged pair this long before closing both legs. Default 1h;
        # the engine_runtime mapping clamps the user setting to [60s, 24h]. Note
        # we only default on None — hold_seconds=0 is a valid "close immediately"
        # value (used in tests), so a falsy-zero `or` fallback would be wrong.
        self.hold_seconds = max(0, _int_cfg(self.cfg("hold_seconds"), 3600))
        # Volume farming: repeat open→hold→close this many times.
        self.total_cycles = max(1, _int_cfg(self.cfg("cycles"), 1))
        self.cycle_gap_seconds = max(0, _int_cfg(self.cfg("cycle_gap_seconds"), 30))
        # Strictly 1x short by design; surfaced so the perp leg's executor and
        # the adapter size isolated margin = full notional.
        self.leverage = max(1, _int_cfg(self.cfg("leverage"), 1))

        self.long_id: Optional[str] = None
        self.short_id: Optional[str] = None
        self.hedge_broken = False

        self.phase = DNPhase.OPENING
        self.cycles_completed = 0
        self.opened_at: Optional[float] = None      # hold-clock start (both legs open)
        self.wait_until: Optional[float] = None      # WAITING → OPENING gate
        self._abort_cycles = False                   # set on a drift break
        self.entry_funding_rate: Optional[Decimal] = None
        self.last_close_type: Optional[CloseType] = None
        # Funding accounting over the whole run (received-positive quote). The
        # short collects funding while the hedge is open; we poll the indexer
        # funding feed via the adapter and accumulate it for the PnL card.
        self.first_cycle_open_ts: Optional[float] = None
        self.cumulative_funding: Decimal = Decimal(0)
        self._funding_reported: Decimal = Decimal(0)

    # -- helpers ----------------------------------------------------------
    def _barriers(self) -> TripleBarrierConfig:
        b = self.cfg("barriers")
        return b if isinstance(b, TripleBarrierConfig) else TripleBarrierConfig()

    def _ex(self, executor_id: Optional[str]) -> Optional[Executor]:
        return self.orchestrator.get(executor_id) if executor_id else None

    def _entry_base(self, executor_id: Optional[str]) -> Decimal:
        ex = self._ex(executor_id)
        return _dec(getattr(ex, "entry_base", Decimal(0)) or 0)

    async def _spawn_leg(self, pair: str, side: TradeType, amount_base: Decimal) -> Optional[str]:
        """Spawn one leg as a MARKET PositionExecutor for ``amount_base``. The
        risk request is sized from the leg's own mid so the Risk Engine sees the
        true per-leg notional."""
        if amount_base <= 0:
            return None
        mid = await self.adapter.mid_price(pair)
        amount_quote = amount_base * mid
        oc = OrderExecutorConfig(
            pair, side, amount_base, ExecutionStrategy.MARKET, leverage=self.leverage
        )
        ex = PositionExecutor(
            PositionExecutorConfig(order_config=oc, barriers=self._barriers()),
            user_id=self.user_id, controller_id=self.id, adapter=self.adapter,
            inventory=self.inventory,
        )
        ok = await self.spawn_executor(
            ex, ExecutorRequest(order_amount_quote=amount_quote, position_size_quote=amount_quote)
        )
        return ex.id if ok else None

    async def _open_cycle(self) -> None:
        """Open both legs atomically. Long first; short sized off the long's
        filled base so the legs match in BASE terms (BUG-DN-3 fix — the prior
        per-leg quote/mid sizing let the spot and perp legs diverge enough to
        trip the drift gate on the first tick). Rolls the long back if the short
        can't spawn."""
        self.hedge_broken = False
        self.opened_at = None

        long_base = self.leg_amount_quote / await self.adapter.mid_price(self.long_pair)
        self.long_id = await self._spawn_leg(self.long_pair, TradeType.BUY, long_base)
        if self.long_id is None:
            raise RuntimeError("delta_neutral: long leg failed to spawn")

        # Base-match the short to what the long actually filled. Fall back to a
        # quote-matched estimate only if the long fill isn't known yet (async
        # fill) — the drift gate then reconciles any residual.
        filled_long = self._entry_base(self.long_id)
        if filled_long > 0:
            short_base = filled_long * self.hedge_ratio
        else:
            short_mid = await self.adapter.mid_price(self.short_pair)
            short_base = (self.leg_amount_quote * self.hedge_ratio) / short_mid

        self.short_id = await self._spawn_leg(self.short_pair, TradeType.SELL, short_base)
        if self.short_id is None:
            # Roll back the long leg so we don't carry unhedged exposure.
            await self.orchestrator.stop(self.long_id, CloseType.EARLY_STOP)
            self.long_id = None
            raise RuntimeError("delta_neutral: short leg failed to spawn; long leg rolled back")

        # If the short underfilled, trim the long so the hedge is balanced from
        # the start rather than waiting for the drift gate to react.
        await self._rebalance_after_open()

        now = time.time()
        self.opened_at = now
        if self.first_cycle_open_ts is None:
            self.first_cycle_open_ts = now
        self.entry_funding_rate = await self._read_funding_rate()
        self.phase = DNPhase.HOLDING

    async def _rebalance_after_open(self) -> None:
        """Trim the over-hedged long leg when the short underfilled, so the legs
        match in base terms. Only acts on a material imbalance (>0.5% of the
        long); smaller residuals are left to the drift gate. Best-effort — a
        failed trim never aborts the cycle (the drift gate remains the backstop)."""
        if self.hedge_ratio <= 0:
            return
        short_filled = self._entry_base(self.short_id)
        long_filled = self._entry_base(self.long_id)
        if short_filled <= 0 or long_filled <= 0:
            return
        desired_long = short_filled / self.hedge_ratio
        excess = long_filled - desired_long
        if excess <= long_filled * Decimal("0.005"):
            return
        long_ex = self._ex(self.long_id)
        reduce = getattr(long_ex, "reduce_position", None)
        if reduce is None:
            return
        try:
            reduced = await reduce(excess)
        except Exception:  # noqa: BLE001 - trim is best-effort
            logger.warning("delta_neutral: long-trim failed", exc_info=True)
            return
        if reduced and reduced > 0:
            logger.info(
                "delta_neutral: short underfilled — trimmed long by %s "
                "(long=%s short=%s ratio=%s)",
                reduced, long_filled, short_filled, self.hedge_ratio,
            )

    async def refresh_funding(self) -> Decimal:
        """Poll the indexer funding feed (via the adapter) for net funding
        received since the run's first leg opened, update the cumulative total,
        and return the delta since the previous poll. Best-effort: funding is
        indexed with a lag, so a poll may return 0 until the venue settles it."""
        if self.first_cycle_open_ts is None:
            return Decimal(0)
        try:
            total = await self.adapter.funding_since(self.short_pair, self.first_cycle_open_ts)
        except Exception:  # noqa: BLE001 - funding read is non-critical
            return Decimal(0)
        delta = total - self._funding_reported
        self._funding_reported = total
        self.cumulative_funding = total
        return delta

    @property
    def funding_quote(self) -> Decimal:
        """Net funding received so far this run (positive = earned)."""
        return self.cumulative_funding

    async def _read_funding_rate(self) -> Optional[Decimal]:
        """Best-effort funding-rate snapshot for the perp leg (informational —
        the short earns funding when this is positive). Never breaks a cycle."""
        try:
            return await self.adapter.funding_rate(self.short_pair)
        except Exception:  # noqa: BLE001 - funding read is non-critical
            return None

    async def _leg_value(self, pair: str) -> Decimal:
        if self.inventory is None:
            return Decimal(0)
        mid = await self.adapter.mid_price(pair)
        hold = self.inventory.get(self.user_id, pair, self.id)
        return abs(hold.net_amount_base) * mid

    async def _close_both_now(self, close_type: CloseType) -> None:
        """Fire reduce-only MARKET closes on BOTH legs concurrently so they exit
        within the same tick (⇒ same minute) — one side is never left exposed.
        Closes may not fill synchronously on a live venue; the CLOSING phase
        keeps ticking the legs until both terminate."""
        self.last_close_type = close_type
        ids = [i for i in (self.long_id, self.short_id) if i]
        if ids:
            await asyncio.gather(
                *(self.orchestrator.stop(i, close_type) for i in ids),
                return_exceptions=True,
            )
        self.phase = DNPhase.CLOSING

    def _both_legs_terminated(self) -> bool:
        for eid in (self.long_id, self.short_id):
            ex = self._ex(eid)
            if ex is not None and not ex.is_terminated:
                return False
        return True

    # -- lifecycle --------------------------------------------------------
    async def on_start(self) -> None:
        await self._open_cycle()

    async def on_tick(self) -> None:
        # Always progress child executors first (poll opening / barriers /
        # closing) before evaluating controller-level phase transitions.
        for ex in self.my_executors(active_only=True):
            await self.orchestrator.tick(ex.id)

        if self.phase is DNPhase.HOLDING:
            await self._tick_holding()
        elif self.phase is DNPhase.CLOSING:
            await self._tick_closing()
        elif self.phase is DNPhase.WAITING:
            await self._tick_waiting()
        # OPENING is only transient inside _open_cycle; DONE is terminal.

    async def _tick_holding(self) -> None:
        # Drift gate (safety): close both immediately if the hedge breaks, and
        # do NOT start further cycles — a broken hedge signals something wrong.
        if self.hedge_ratio > 0:
            long_value = await self._leg_value(self.long_pair)
            short_value = await self._leg_value(self.short_pair)
            if short_value > 0 and long_value > 0:
                target_long = short_value / self.hedge_ratio
                drift = abs(long_value - target_long) / max(long_value, target_long)
                if drift > self.max_drift_pct:
                    self.hedge_broken = True
                    self._abort_cycles = True
                    await self._close_both_now(CloseType.EARLY_STOP)
                    return

        # Hold expiry → planned synchronized exit.
        if self.opened_at is not None and (time.time() - self.opened_at) >= self.hold_seconds:
            await self._close_both_now(CloseType.TIME_LIMIT)

    async def _tick_closing(self) -> None:
        if not self._both_legs_terminated():
            return  # keep ticking the legs until both flat
        self.cycles_completed += 1
        # Settle funding earned so far before clearing the cycle. Funding is
        # indexed with a lag, so this may lag the true total until the venue
        # settles; the PnL card also reads the synced funding feed independently.
        await self.refresh_funding()
        self.long_id = None
        self.short_id = None
        self.opened_at = None
        if self._abort_cycles or self.cycles_completed >= self.total_cycles:
            self.phase = DNPhase.DONE
        else:
            self.wait_until = time.time() + self.cycle_gap_seconds
            self.phase = DNPhase.WAITING

    async def _tick_waiting(self) -> None:
        if self.wait_until is not None and time.time() >= self.wait_until:
            self.wait_until = None
            self.phase = DNPhase.OPENING
            try:
                await self._open_cycle()
            except Exception as exc:  # noqa: BLE001 - a failed re-open ends the run
                logger.warning("delta_neutral: cycle re-open failed: %s", exc, exc_info=True)
                self.phase = DNPhase.DONE
