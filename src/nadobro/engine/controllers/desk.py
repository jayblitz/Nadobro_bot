"""Desk controller — runs a user's text-to-trade execution plans.

One controller per (user, network) owns ALL of that user's active plans:
``awaiting_trigger`` plans get a dumb absolute price/time check each tick;
fired plans run their entry leg (market / limit / TWAP via the existing
executors); completed entries optionally arm an exit watcher (TP / SL /
trailing) that closes the ACTUAL filled amount — never the requested one.

Layering: the DB is owned by the service-side desk runner, which injects
async callables (``plans_provider`` / ``claim_trigger`` / ``finish`` /
``checkpoint`` / ``spot_open``). The controller never imports services and
only sees the venue through the adapter, like every other controller.

Restart contract (a 24h TWAP must survive a deploy):
- nothing is placed before ``claim_trigger`` wins the guarded DB update;
- progress (fills, phase) is checkpointed after every tick;
- on re-attach, a TWAP resumes a *remainder* schedule from the checkpoint
  (worst case one slice of overlap — documented, alerted);
- an ambiguous one-shot entry (market/limit spawned, outcome unknown) is
  FAILED loudly with a "check your portfolio" alert, never re-fired blind.

Spot market hours (tokenized stocks): when ``spot_open`` reports the market
closed, waiting triggers hold their fire and a running TWAP is suspended
(resting slice cancelled, fills kept) then resumed with the remaining size
when the market reopens — slices are never burned into a closed book.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field as dc_field
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, List, Optional

from src.nadobro.engine.controllers.controller_base import Controller
from src.nadobro.engine.desk_plan import (
    ExecutionPlan,
    trigger_satisfied,
)
from src.nadobro.engine.executors.order_executor import OrderExecutor, OrderExecutorConfig
from src.nadobro.engine.executors.twap_executor import TWAPExecutor, TWAPExecutorConfig
from src.nadobro.engine.risk import ExecutorRequest
from src.nadobro.engine.types import (
    CloseType,
    ExecutionStrategy,
    PositionAction,
    TradeType,
    _dec,
)

logger = logging.getLogger(__name__)

# Plan phases (in-memory + checkpointed; orthogonal to the DB status, which
# only knows awaiting_trigger/running/terminal).
PH_WAITING = "waiting"
PH_ENTRY = "entry"
PH_SUSPENDED = "suspended"      # spot market closed mid-entry
PH_EXIT_WATCH = "exit_watch"
PH_EXITING = "exiting"

_PROGRESS_MARKS = (25, 50, 75)


@dataclass
class _PlanRun:
    plan: ExecutionPlan
    phase: str = PH_WAITING
    entry_exec_id: Optional[str] = None
    exit_exec_id: Optional[str] = None
    # fills accumulated across entry executors (suspend/resume spawns several)
    prior_base: Decimal = Decimal(0)
    prior_quote: Decimal = Decimal(0)
    prior_fees: Decimal = Decimal(0)
    target_quote: Decimal = Decimal(0)   # total entry notional, set at fire time
    deadline_ts: float = 0.0             # limit-entry expiry
    entry_started: bool = False
    entry_done: bool = False
    exit_reason: str = ""
    pending_failure: str = ""            # set by sync recovery; failed on next advance
    hwm: Optional[Decimal] = None        # trailing high-water (long)
    lwm: Optional[Decimal] = None        # trailing low-water (short)
    notified: set = dc_field(default_factory=set)
    dirty: bool = True                   # needs checkpoint

    def executor_totals(self, orch) -> tuple[Decimal, Decimal, Decimal]:
        """prior + current entry executor fills = (base, quote, fees)."""
        base, quote, fees = self.prior_base, self.prior_quote, self.prior_fees
        if self.entry_exec_id:
            ex = orch.get(self.entry_exec_id)
            if ex is not None:
                base += _exec_filled_base(ex)
                quote += _exec_filled_quote(ex)
                fees += _exec_fees(ex)
        return base, quote, fees


def _exec_filled_base(ex) -> Decimal:
    return _dec(getattr(ex, "filled_base", None) or getattr(ex, "_recorded_base", 0) or 0)


def _exec_filled_quote(ex) -> Decimal:
    return _dec(getattr(ex, "filled_quote", None) or getattr(ex, "_recorded_quote", 0) or 0)


def _exec_fees(ex) -> Decimal:
    return _dec(getattr(ex, "_fees_paid_quote", 0) or 0)


class DeskController(Controller):
    """``configs`` (injected by services/desk_runtime):

    - ``plans_provider``: async () -> list of {plan_id, status, plan, state}
    - ``claim_trigger``:  async (plan_id) -> bool (guarded DB transition)
    - ``finish``:         async (plan_id, status, error|None) -> None
    - ``checkpoint``:     async (plan_id, dict) -> None
    - ``spot_open``:      async (product) -> bool (market-hours; perps always True)
    """

    LIMIT_ENTRY_TTL_SECONDS = 7 * 24 * 3600.0

    def __init__(self, **kwargs: object) -> None:
        kwargs.setdefault("name", "desk")
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.trading_pair = str(self.cfg("trading_pair", "DESK") or "DESK")
        self._plans_provider = self.cfg("plans_provider")
        self._claim_trigger = self.cfg("claim_trigger")
        self._finish = self.cfg("finish")
        self._checkpoint = self.cfg("checkpoint")
        self._spot_open = self.cfg("spot_open")
        if not all(callable(c) for c in (
            self._plans_provider, self._claim_trigger, self._finish, self._checkpoint,
        )):
            raise ValueError("DeskController requires plans_provider/claim_trigger/finish/checkpoint")
        self._runs: Dict[str, _PlanRun] = {}
        self._events: List[Dict[str, Any]] = []
        self._mid_cache: Dict[str, Decimal] = {}

    # -- events --------------------------------------------------------------
    def _emit(self, etype: str, plan: ExecutionPlan, **extra: Any) -> None:
        evt = {"type": etype, "plan_id": plan.plan_id, "summary": plan.describe(),
               "product": plan.product, "market": plan.market, **extra}
        self._events.append(evt)
        logger.info("desk[%s] %s plan=%s %s", self.user_id, etype, plan.plan_id, extra)

    def consume_desk_events(self) -> List[Dict[str, Any]]:
        out, self._events = self._events, []
        return out

    # -- helpers ---------------------------------------------------------------
    async def _mid(self, product: str) -> Optional[Decimal]:
        if product in self._mid_cache:
            return self._mid_cache[product]
        try:
            mid = await self.adapter.mid_price(product)
        except Exception:  # noqa: BLE001 - one bad feed must not stall other plans
            logger.warning("desk[%s]: mid_price failed for %s", self.user_id, product,
                           exc_info=True)
            return None
        if mid and mid > 0:
            self._mid_cache[product] = mid
            return mid
        return None

    async def _market_open(self, plan: ExecutionPlan) -> bool:
        if plan.market != "spot" or not callable(self._spot_open):
            return True
        try:
            return bool(await self._spot_open(plan.product))
        except Exception:  # noqa: BLE001 - fail open: hours feed down != halt trading
            return True

    def _entry_side(self, plan: ExecutionPlan) -> TradeType:
        return TradeType.BUY if plan.side == "buy" else TradeType.SELL

    def _state_dict(self, run: _PlanRun) -> Dict[str, Any]:
        base, quote, fees = run.executor_totals(self.orchestrator)
        return {
            "phase": run.phase,
            "entry_started": run.entry_started,
            "entry_done": run.entry_done,
            "filled_base": str(base),
            "filled_quote": str(quote),
            "fees": str(fees),
            "target_quote": str(run.target_quote),
            "deadline_ts": run.deadline_ts,
            "hwm": str(run.hwm) if run.hwm is not None else None,
            "lwm": str(run.lwm) if run.lwm is not None else None,
            "exit_reason": run.exit_reason,
            "notified": sorted(run.notified),
        }

    # -- lifecycle ---------------------------------------------------------------
    async def on_start(self) -> None:
        await self._reconcile()

    async def on_tick(self) -> None:
        self._mid_cache.clear()
        await self._reconcile()

        # Drive every owned executor first (slices, closes), then read states.
        for ex in self.my_executors(active_only=True):
            await self.orchestrator.tick(ex.id)

        for run in list(self._runs.values()):
            try:
                await self._advance(run)
            except Exception:  # noqa: BLE001 - one plan must not stall the rest
                logger.exception("desk[%s]: advance failed for plan %s",
                                 self.user_id, run.plan.plan_id)

        # Checkpoint after advancing — the restart contract.
        for run in self._runs.values():
            if run.dirty:
                try:
                    await self._checkpoint(run.plan.plan_id, self._state_dict(run))
                    run.dirty = False
                except Exception:  # noqa: BLE001 - checkpoint failure is survivable
                    logger.warning("desk[%s]: checkpoint failed for %s",
                                   self.user_id, run.plan.plan_id, exc_info=True)

    async def on_stop(self, reason: str = "stopped") -> None:
        for ex in self.my_executors(active_only=True):
            try:
                await ex.on_stop(CloseType.EARLY_STOP)
            except Exception:  # noqa: BLE001
                logger.warning("desk[%s]: executor stop failed", self.user_id, exc_info=True)

    # -- reconcile against the store ------------------------------------------------
    async def _reconcile(self) -> None:
        try:
            records = await self._plans_provider() or []
        except Exception:  # noqa: BLE001 - provider down: keep driving what we have
            logger.warning("desk[%s]: plans_provider failed", self.user_id, exc_info=True)
            return

        seen: set[str] = set()
        for rec in records:
            plan = rec.get("plan")
            status = str(rec.get("status") or "")
            if not isinstance(plan, ExecutionPlan):
                continue
            seen.add(plan.plan_id)
            if plan.plan_id in self._runs:
                continue
            if status == "awaiting_trigger":
                self._runs[plan.plan_id] = _PlanRun(plan=plan)
            elif status == "running":
                self._recover(plan, rec.get("state") or {})

        # Plans that vanished from the active set were cancelled (or finished
        # by us — already dropped). Stop their executors; keep their fills.
        for plan_id in [p for p in self._runs if p not in seen]:
            run = self._runs.pop(plan_id)
            for exec_id in (run.entry_exec_id, run.exit_exec_id):
                ex = self.orchestrator.get(exec_id) if exec_id else None
                if ex is not None and not ex.is_terminated:
                    try:
                        await ex.on_stop(CloseType.EARLY_STOP)
                    except Exception:  # noqa: BLE001
                        logger.warning("desk[%s]: cancel-stop failed for %s",
                                       self.user_id, plan_id, exc_info=True)
            self._emit("plan_cancelled", run.plan)

    def _recover(self, plan: ExecutionPlan, state: Dict[str, Any]) -> None:
        """Re-attach a plan that was RUNNING before a restart."""
        run = _PlanRun(plan=plan)
        run.entry_started = bool(state.get("entry_started"))
        run.entry_done = bool(state.get("entry_done"))
        run.prior_base = _dec(state.get("filled_base") or 0)
        run.prior_quote = _dec(state.get("filled_quote") or 0)
        run.prior_fees = _dec(state.get("fees") or 0)
        run.target_quote = _dec(state.get("target_quote") or 0)
        run.deadline_ts = float(state.get("deadline_ts") or 0)
        run.notified = set(state.get("notified") or [])
        run.exit_reason = str(state.get("exit_reason") or "")
        hwm, lwm = state.get("hwm"), state.get("lwm")
        run.hwm = _dec(hwm) if hwm else None
        run.lwm = _dec(lwm) if lwm else None

        if run.entry_done:
            # Entry leg already finished before the restart. Route through
            # _tick_entry -> _entry_finished, which re-arms the exit watch
            # (fills permitting), re-attempts a lost finish_plan, or fails a
            # zero-fill plan — never re-fires the entry.
            run.phase = PH_ENTRY
            run.entry_exec_id = None
            self._runs[plan.plan_id] = run
            if plan.exits and not plan.exits.is_empty() and run.prior_base > 0:
                self._emit("plan_recovered", plan, detail="exit watch re-armed")
            return

        if run.entry_started and plan.algo in ("market", "limit"):
            # One-shot order with unknown outcome (a lost resting limit is
            # just as unknowable): never re-fire blind — fail loudly instead.
            run.pending_failure = (
                "a restart interrupted this order and its outcome is unknown"
            )
            run.entry_exec_id = None
            self._runs[plan.plan_id] = run
            self._emit("plan_ambiguous", plan)
            return

        # TWAP / limit remainder (or nothing placed yet): respawn on next advance.
        run.phase = PH_ENTRY if run.entry_started else PH_WAITING
        # claim already won before the restart — skip straight to entry
        if not run.entry_started:
            run.phase = PH_ENTRY
        run.entry_exec_id = None
        self._runs[plan.plan_id] = run
        if run.entry_started:
            self._emit("plan_recovered", plan,
                       detail="resuming remainder (≤1 slice may overlap)")

    # -- phase machine ------------------------------------------------------------
    async def _advance(self, run: _PlanRun) -> None:
        plan = run.plan
        if run.pending_failure:
            await self._fail(run, run.pending_failure)
            return
        if run.phase == PH_WAITING:
            await self._tick_waiting(run)
        elif run.phase == PH_ENTRY:
            await self._tick_entry(run)
        elif run.phase == PH_SUSPENDED:
            await self._tick_suspended(run)
        elif run.phase == PH_EXIT_WATCH:
            await self._tick_exit_watch(run)
        elif run.phase == PH_EXITING:
            await self._tick_exiting(run)

    async def _tick_waiting(self, run: _PlanRun) -> None:
        plan = run.plan
        if not await self._market_open(plan):
            return  # hold fire while the (stock-token) market is closed
        mid = await self._mid(plan.product or "")
        if mid is None:
            return
        if not trigger_satisfied(plan.entry_trigger, mid=float(mid)):
            return
        won = False
        try:
            won = bool(await self._claim_trigger(plan.plan_id))
        except Exception:  # noqa: BLE001 - claim failure: retry next tick
            logger.warning("desk[%s]: claim_trigger failed for %s",
                           self.user_id, plan.plan_id, exc_info=True)
            return
        if not won:
            # Another worker owns it — drop; reconcile will re-add if needed.
            self._runs.pop(plan.plan_id, None)
            return
        self._emit("trigger_fired", plan, mid=str(mid))
        run.phase = PH_ENTRY
        run.dirty = True
        await self._spawn_entry(run, mid)

    async def _spawn_entry(self, run: _PlanRun, mid: Decimal) -> None:
        plan = run.plan
        product = plan.product or ""
        if not product:
            await self._fail(run, "plan has no product")
            return
        leverage = int(plan.leverage or 1) if plan.market == "perp" else 1
        side = self._entry_side(plan)

        if run.target_quote <= 0:
            run.target_quote = (
                _dec(plan.size_quote) if plan.size_quote
                else _dec(plan.size_base or 0) * mid
            )
        remaining_quote = run.target_quote - run.prior_quote
        if remaining_quote <= 0:
            run.entry_done = True
            await self._entry_finished(run)
            return

        run.entry_started = True
        run.dirty = True
        # Persist entry_started BEFORE anything touches the venue: a crash
        # between placement and an end-of-tick checkpoint would otherwise
        # recover as "nothing placed" and re-fire a one-shot order blind.
        try:
            await self._checkpoint(plan.plan_id, self._state_dict(run))
        except Exception:  # noqa: BLE001 - placement still guarded by recovery rules
            logger.warning("desk[%s]: pre-spawn checkpoint failed for %s",
                           self.user_id, plan.plan_id, exc_info=True)

        if plan.algo == "twap":
            total_secs = float(plan.duration_minutes or 60) * 60.0
            interval = float(plan.interval_seconds or 30)
            # Resume case: scale duration to the remaining fraction, floor at
            # one interval so the config stays valid.
            if run.prior_quote > 0 and run.target_quote > 0:
                frac = float(remaining_quote / run.target_quote)
                total_secs = max(interval, total_secs * frac)
            twap_cfg = TWAPExecutorConfig(
                product, side, remaining_quote, total_secs, interval,
                mode="MAKER" if plan.exec_mode == "maker" else "TAKER",
                leverage=leverage,
            )
            ex: Any = TWAPExecutor(twap_cfg, user_id=self.user_id, controller_id=self.id,
                                   adapter=self.adapter, inventory=self.inventory)
        else:
            amount_base = (
                _dec(plan.size_base) if plan.size_base
                else remaining_quote / (_dec(plan.limit_price) if (plan.algo == "limit" and plan.limit_price) else mid)
            )
            order_cfg = OrderExecutorConfig(
                product, side, amount_base,
                ExecutionStrategy.LIMIT if plan.algo == "limit" else ExecutionStrategy.MARKET,
                price=_dec(plan.limit_price) if plan.algo == "limit" else None,
                leverage=leverage,
                position_action=PositionAction.OPEN,
            )
            ex = OrderExecutor(order_cfg, user_id=self.user_id, controller_id=self.id,
                               adapter=self.adapter, inventory=self.inventory)
            if plan.algo == "limit" and run.deadline_ts <= 0:
                run.deadline_ts = time.time() + self.LIMIT_ENTRY_TTL_SECONDS

        ok = await self.spawn_executor(ex, ExecutorRequest(order_amount_quote=remaining_quote))
        if not ok:
            await self._fail(run, "risk engine refused the order (budget/limits)")
            return
        run.entry_exec_id = ex.id

    async def _tick_entry(self, run: _PlanRun) -> None:
        plan = run.plan
        if run.entry_done and run.entry_exec_id is None:
            await self._entry_finished(run)
            return
        if run.entry_exec_id is None:
            # recovered remainder (or spawn pending): need a mid to size it
            if not await self._market_open(plan):
                run.phase = PH_SUSPENDED
                run.dirty = True
                return
            mid = await self._mid(plan.product or "")
            if mid is None:
                return
            await self._spawn_entry(run, mid)
            return

        ex = self.orchestrator.get(run.entry_exec_id)
        if ex is None:
            run.entry_exec_id = None
            return

        # Spot market closed mid-run: suspend (cancel resting, keep fills).
        if not ex.is_terminated and plan.algo == "twap" and not await self._market_open(plan):
            await ex.on_stop(CloseType.EARLY_STOP)
            run.prior_base += _exec_filled_base(ex)
            run.prior_quote += _exec_filled_quote(ex)
            run.prior_fees += _exec_fees(ex)
            run.entry_exec_id = None
            run.phase = PH_SUSPENDED
            run.dirty = True
            self._emit("entry_suspended", plan, reason="market closed")
            return

        # Limit-entry expiry.
        if not ex.is_terminated and run.deadline_ts and time.time() > run.deadline_ts:
            await ex.on_stop(CloseType.EARLY_STOP)

        if not ex.is_terminated:
            self._notify_progress(run)
            run.dirty = True
            return

        # Entry executor finished — absorb and move on.
        run.prior_base += _exec_filled_base(ex)
        run.prior_quote += _exec_filled_quote(ex)
        run.prior_fees += _exec_fees(ex)
        run.entry_exec_id = None
        run.entry_done = True
        run.dirty = True
        await self._entry_finished(run)

    def _notify_progress(self, run: _PlanRun) -> None:
        if run.plan.algo != "twap" or run.target_quote <= 0:
            return
        base, quote, _ = run.executor_totals(self.orchestrator)
        pct = int(float(quote / run.target_quote) * 100)
        for mark in _PROGRESS_MARKS:
            if pct >= mark and mark not in run.notified:
                run.notified.add(mark)
                self._emit("entry_progress", run.plan, pct=mark,
                           filled_base=str(base), filled_quote=str(quote))

    async def _tick_suspended(self, run: _PlanRun) -> None:
        if await self._market_open(run.plan):
            run.phase = PH_ENTRY
            run.dirty = True
            self._emit("entry_resumed", run.plan)

    async def _entry_finished(self, run: _PlanRun) -> None:
        plan = run.plan
        base, quote, fees = run.prior_base, run.prior_quote, run.prior_fees
        vwap = (quote / base) if base > 0 else Decimal(0)

        if base <= 0:
            await self._fail(run, "entry leg filled nothing")
            return

        if plan.exits and not plan.exits.is_empty():
            run.phase = PH_EXIT_WATCH
            run.dirty = True
            self._emit("entry_filled", plan, filled_base=str(base), vwap=str(vwap),
                       fees=str(fees), exits_armed=True)
            return

        await self._complete(run)

    # -- exit watcher (uniform for spot sell-leg and perp reduce-only) -------------
    def _exit_levels(self, run: _PlanRun) -> Dict[str, Optional[Decimal]]:
        """Absolute TP/SL levels vs ACTUAL entry VWAP. For a buy entry TP is
        above and SL below; for a sell (short) entry, mirrored."""
        plan, ex = run.plan, run.plan.exits
        base, quote, _ = run.prior_base, run.prior_quote, run.prior_fees
        vwap = (quote / base) if base > 0 else Decimal(0)
        if ex is None or vwap <= 0:
            return {"tp": None, "sl": None, "trail_pct": None}
        is_long = plan.side == "buy"
        tp = sl = None
        if ex.tp_price is not None:
            tp = _dec(ex.tp_price)
        elif ex.tp_pct is not None:
            k = _dec(ex.tp_pct) / 100
            tp = vwap * (1 + k) if is_long else vwap * (1 - k)
        if ex.sl_price is not None:
            sl = _dec(ex.sl_price)
        elif ex.sl_pct is not None:
            k = _dec(ex.sl_pct) / 100
            sl = vwap * (1 - k) if is_long else vwap * (1 + k)
        trail = _dec(ex.trailing_pct) / 100 if ex.trailing_pct is not None else None
        return {"tp": tp, "sl": sl, "trail_pct": trail}

    async def _tick_exit_watch(self, run: _PlanRun) -> None:
        plan = run.plan
        mid = await self._mid(plan.product or "")
        if mid is None:
            return
        levels = self._exit_levels(run)
        is_long = plan.side == "buy"
        reason = ""

        # stop-loss first: capital protection beats profit taking on a gap
        sl = levels["sl"]
        if sl is not None and ((is_long and mid <= sl) or (not is_long and mid >= sl)):
            reason = "stop_loss"

        trail = levels["trail_pct"]
        if not reason and trail is not None and plan.market == "perp":
            if is_long:
                run.hwm = mid if run.hwm is None or mid > run.hwm else run.hwm
                if mid <= run.hwm * (1 - trail):
                    reason = "trailing_stop"
            else:
                run.lwm = mid if run.lwm is None or mid < run.lwm else run.lwm
                if mid >= run.lwm * (1 + trail):
                    reason = "trailing_stop"
            run.dirty = True

        tp = levels["tp"]
        if not reason and tp is not None and ((is_long and mid >= tp) or (not is_long and mid <= tp)):
            reason = "take_profit"

        if not reason:
            return

        run.exit_reason = reason
        run.dirty = True
        self._emit("exit_triggered", plan, reason=reason, mid=str(mid))
        await self._spawn_exit(run)

    async def _spawn_exit(self, run: _PlanRun) -> None:
        plan = run.plan
        product = plan.product or ""
        amount = run.prior_base
        if amount <= 0 or not product:
            await self._complete(run)
            return
        close_side = TradeType.SELL if plan.side == "buy" else TradeType.BUY
        cfg = OrderExecutorConfig(
            product, close_side, amount, ExecutionStrategy.MARKET,
            leverage=int(plan.leverage or 1) if plan.market == "perp" else 1,
            # perp: reduce-only close of the position; spot: a plain sell of
            # the tokens we actually bought.
            position_action=PositionAction.CLOSE if plan.market == "perp" else PositionAction.OPEN,
        )
        ex = OrderExecutor(cfg, user_id=self.user_id, controller_id=self.id,
                           adapter=self.adapter, inventory=self.inventory)
        notional = amount * (self._mid_cache.get(product) or Decimal(1))
        ok = await self.spawn_executor(ex, ExecutorRequest(order_amount_quote=notional))
        if not ok:
            await self._fail(run, f"exit ({run.exit_reason}) refused by risk engine — "
                                  "close manually from Portfolio")
            return
        run.exit_exec_id = ex.id
        run.phase = PH_EXITING
        run.dirty = True

    async def _tick_exiting(self, run: _PlanRun) -> None:
        ex = self.orchestrator.get(run.exit_exec_id) if run.exit_exec_id else None
        if ex is None:
            # restart lost the executor: re-arm the watch; it re-fires
            run.phase = PH_EXIT_WATCH
            run.exit_exec_id = None
            run.dirty = True
            return
        if not ex.is_terminated:
            return
        exit_base = _exec_filled_base(ex)
        exit_quote = _exec_filled_quote(ex)
        run.prior_fees += _exec_fees(ex)
        if exit_base < run.prior_base * _dec("0.999"):
            # Partial close — retry the remainder next tick via the watch.
            # The remainder keeps the ENTRY vwap (scale cost by remaining
            # base); subtracting close PROCEEDS from entry COST would warp
            # the TP/SL levels for what's left.
            old_base = run.prior_base
            vwap = (run.prior_quote / old_base) if old_base > 0 else Decimal(0)
            run.prior_base = max(Decimal(0), old_base - exit_base)
            run.prior_quote = vwap * run.prior_base
            run.phase = PH_EXIT_WATCH
            run.exit_exec_id = None
            run.dirty = True
            self._emit("exit_partial", run.plan, closed_base=str(exit_base))
            return
        await self._complete(run, exit_quote=exit_quote)

    # -- terminal -------------------------------------------------------------------
    async def _complete(self, run: _PlanRun, exit_quote: Optional[Decimal] = None) -> None:
        plan = run.plan
        base, quote, fees = run.prior_base, run.prior_quote, run.prior_fees
        vwap = (quote / base) if base > 0 else Decimal(0)
        summary: Dict[str, Any] = {
            "filled_base": str(base), "filled_quote": str(quote),
            "vwap": str(vwap), "fees": str(fees),
        }
        if run.exit_reason:
            summary["exit_reason"] = run.exit_reason
        if exit_quote is not None:
            summary["exit_quote"] = str(exit_quote)
        try:
            await self._finish(plan.plan_id, "completed", None)
        except Exception:  # noqa: BLE001 - retried via _recover on next reconcile
            logger.warning("desk[%s]: finish(completed) failed for %s",
                           self.user_id, plan.plan_id, exc_info=True)
        self._runs.pop(plan.plan_id, None)
        self._emit("plan_completed", plan, **summary)

    async def _fail(self, run: _PlanRun, error: str) -> None:
        plan = run.plan
        try:
            await self._finish(plan.plan_id, "failed", error)
        except Exception:  # noqa: BLE001
            logger.warning("desk[%s]: finish(failed) failed for %s",
                           self.user_id, plan.plan_id, exc_info=True)
        self._runs.pop(plan.plan_id, None)
        self._emit("plan_failed", plan, error=error)
