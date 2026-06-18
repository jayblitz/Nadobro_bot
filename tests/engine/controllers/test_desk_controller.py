"""DeskController fault-injection suite.

The failure families pinned here:
1. Trigger discipline — nothing is placed before the guarded claim wins; a
   lost claim places nothing.
2. Entry/exit chaining — exits size from ACTUAL fills (vwap), spot exits are
   plain sells, SL beats TP on a gap, partial closes retry.
3. Restart recovery — TWAP resumes a remainder; an ambiguous one-shot entry
   is failed loudly, never re-fired blind.
4. Spot market hours — waiting triggers hold fire; a running TWAP suspends
   (fills kept) and resumes with the remainder.
5. Cancel — executors stopped, fills kept, no finish() overwrite.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.desk import (
    PH_ENTRY,
    PH_EXIT_WATCH,
    PH_SUSPENDED,
    PH_WAITING,
    DeskController,
)
from src.nadobro.engine.desk_plan import EntryTrigger, ExecutionPlan, ExitPlan
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.types import CloseType, TradeType


def record(plan, status="awaiting_trigger", state=None):
    return {"plan_id": plan.plan_id, "status": status, "plan": plan,
            "state": state or {}}


def make_desk(adapter, records, *, spot_open=None, claim_wins=True):
    """Controller + in-memory store double. ``records`` mutates like the DB:
    claim flips awaiting_trigger->running, finish removes the row."""
    orch = ExecutorOrchestrator()
    store = {"records": records, "finished": [], "checkpoints": {}, "claims": []}

    async def plans_provider():
        return [dict(r) for r in store["records"]]

    async def claim_trigger(plan_id):
        store["claims"].append(plan_id)
        if not claim_wins:
            return False
        for r in store["records"]:
            if r["plan_id"] == plan_id and r["status"] == "awaiting_trigger":
                r["status"] = "running"
                return True
        return False

    async def finish(plan_id, status, error):
        store["finished"].append((plan_id, status, error))
        store["records"] = [r for r in store["records"] if r["plan_id"] != plan_id]

    async def checkpoint(plan_id, state):
        store["checkpoints"][plan_id] = state
        # ordering probe: how many venue orders existed when this state
        # was persisted (pins the checkpoint-before-placement contract)
        store.setdefault("checkpoint_log", []).append(
            (dict(state), len(adapter.placed))
        )

    async def spot_is_open(product):
        if spot_open is None:
            return True
        return spot_open() if callable(spot_open) else bool(spot_open)

    c = DeskController(
        user_id=1, orchestrator=orch, adapter=adapter, inventory=InventoryRepository(),
        configs={
            "trading_pair": "DESK",
            "plans_provider": plans_provider,
            "claim_trigger": claim_trigger,
            "finish": finish,
            "checkpoint": checkpoint,
            "spot_open": spot_is_open,
        },
        controller_id="DESK",
    )
    return orch, c, store


def kinds(c):
    return [e["type"] for e in c.consume_desk_events()]


async def ticks(orch, c, n):
    for _ in range(n):
        await orch.tick_controller(c.id)


def spot_market_plan(**kw):
    base = dict(algo="market", market="spot", product="ETH", side="buy", size_base=1.0)
    base.update(kw)
    return ExecutionPlan(**base)


# ---------------------------------------------------------------------------
# 1. trigger discipline
# ---------------------------------------------------------------------------

def test_no_order_before_trigger_fires():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = spot_market_plan(entry_trigger=EntryTrigger(kind="price_below", price=95.0))
        orch, c, store = make_desk(adapter, [record(plan)])
        await orch.spawn_controller(c)
        await ticks(orch, c, 3)
        assert adapter.placed == []          # nothing on the venue
        assert store["claims"] == []          # claim not even attempted
        adapter.set_mid(94)
        await ticks(orch, c, 3)
        assert store["claims"] == [plan.plan_id]
        assert len(adapter.placed) == 1
        assert adapter.placed[0].side is TradeType.BUY
        assert store["finished"] and store["finished"][0][1] == "completed"

    asyncio.run(body())


def test_lost_claim_places_nothing():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = spot_market_plan()  # no trigger: fires immediately
        orch, c, store = make_desk(adapter, [record(plan)], claim_wins=False)
        await orch.spawn_controller(c)
        await ticks(orch, c, 3)
        assert store["claims"]                # it tried
        assert adapter.placed == []           # but never placed
        assert store["finished"] == []

    asyncio.run(body())


def test_spot_plan_addresses_the_spot_market_not_the_perp():
    """The dual-listed-asset bug: 'buy 2 ETH' (spot) must place against the
    SPOT product, never the perp that shares the base. The adapter routes by
    the trading-pair string, so the controller must use the -SPOT alias."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = spot_market_plan(product="ETH", market="spot", size_base=2.0)
        orch, c, store = make_desk(adapter, [record(plan)])
        await orch.spawn_controller(c)
        await ticks(orch, c, 2)
        assert adapter.placed
        assert adapter.placed[0].trading_pair == "ETH-SPOT"

    asyncio.run(body())


def test_perp_plan_addresses_the_perp_market():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = ExecutionPlan(algo="market", market="perp", product="ETH", side="buy",
                             size_base=2.0, leverage=3)
        orch, c, store = make_desk(adapter, [record(plan)])
        await orch.spawn_controller(c)
        await ticks(orch, c, 2)
        assert adapter.placed and adapter.placed[0].trading_pair == "ETH-PERP"

    asyncio.run(body())


def test_immediate_plan_executes_and_reports():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = spot_market_plan(size_quote=200.0, size_base=None)
        orch, c, store = make_desk(adapter, [record(plan)])
        await orch.spawn_controller(c)
        await ticks(orch, c, 1)
        evts = kinds(c)
        assert "trigger_fired" in evts
        await ticks(orch, c, 2)
        assert "plan_completed" in kinds(c)
        assert store["finished"] == [(plan.plan_id, "completed", None)]
        # $200 at mid 100 -> 2 base
        assert adapter.placed[0].amount_base == Decimal(2)

    asyncio.run(body())


# ---------------------------------------------------------------------------
# 2. exits
# ---------------------------------------------------------------------------

def test_spot_tp_exit_sells_actual_fill():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = spot_market_plan(exits=ExitPlan(tp_pct=5.0))
        orch, c, store = make_desk(adapter, [record(plan)])
        await orch.spawn_controller(c)
        await ticks(orch, c, 3)
        evts = kinds(c)
        assert "entry_filled" in evts
        run = c._runs[plan.plan_id]  # noqa: SLF001
        assert run.phase == PH_EXIT_WATCH
        assert store["finished"] == []        # still active while watching

        adapter.set_mid(104)                  # below TP (105) — no exit
        await ticks(orch, c, 1)
        assert all(o.side is TradeType.BUY for o in adapter.placed)

        adapter.set_mid(106)                  # TP hit
        await ticks(orch, c, 3)
        evts = kinds(c)
        assert "exit_triggered" in evts and "plan_completed" in evts
        sells = [o for o in adapter.placed if o.side is TradeType.SELL]
        assert len(sells) == 1
        assert sells[0].amount_base == Decimal(1)  # the ACTUAL filled base
        assert store["finished"][-1][1] == "completed"

    asyncio.run(body())


def test_sl_beats_tp_on_a_gap():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        # Degenerate on purpose: both levels at 100 — a gap that satisfies
        # both must resolve to stop_loss (capital protection first).
        plan = spot_market_plan(exits=ExitPlan(tp_price=100.0, sl_price=100.0))
        orch, c, store = make_desk(adapter, [record(plan)])
        await orch.spawn_controller(c)
        await ticks(orch, c, 5)
        evts = [e for e in c.consume_desk_events() if e["type"] == "exit_triggered"]
        assert evts and evts[0]["reason"] == "stop_loss"

    asyncio.run(body())


def test_perp_short_exit_levels_are_mirrored():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = ExecutionPlan(algo="market", market="perp", product="BTC", side="sell",
                             size_base=1.0, leverage=2,
                             exits=ExitPlan(tp_pct=5.0, sl_pct=3.0))
        orch, c, store = make_desk(adapter, [record(plan)])
        await orch.spawn_controller(c)
        await ticks(orch, c, 3)
        adapter.set_mid(102)                  # short: SL is at 103, TP at 95
        await ticks(orch, c, 1)
        assert not any(e["type"] == "exit_triggered" for e in c.consume_desk_events())
        adapter.set_mid(94)                   # TP for the short
        await ticks(orch, c, 3)
        evts = [e for e in c.consume_desk_events() if e["type"] == "exit_triggered"]
        assert evts and evts[0]["reason"] == "take_profit"
        closes = [o for o in adapter.placed if o.side is TradeType.BUY]
        assert closes and closes[0].amount_base == Decimal(1)

    asyncio.run(body())


# ---------------------------------------------------------------------------
# 3. restart recovery
# ---------------------------------------------------------------------------

def test_ambiguous_market_entry_fails_loudly_never_refires():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = spot_market_plan()
        rec = record(plan, status="running",
                     state={"entry_started": True, "entry_done": False})
        orch, c, store = make_desk(adapter, [rec])
        await orch.spawn_controller(c)
        await ticks(orch, c, 2)
        assert adapter.placed == []           # NEVER re-fired blind
        assert "plan_ambiguous" in kinds(c)
        assert store["finished"] and store["finished"][0][1] == "failed"

    asyncio.run(body())


def test_twap_recovery_resumes_remainder_only():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = ExecutionPlan(algo="twap", market="spot", product="ETH", side="buy",
                             size_quote=100.0, duration_minutes=10, interval_seconds=60)
        rec = record(plan, status="running", state={
            "entry_started": True, "entry_done": False,
            "filled_base": "0.5", "filled_quote": "50", "fees": "0",
            "target_quote": "100",
        })
        orch, c, store = make_desk(adapter, [rec])
        await orch.spawn_controller(c)
        await ticks(orch, c, 2)
        assert "plan_recovered" in kinds(c)
        run = c._runs[plan.plan_id]  # noqa: SLF001
        ex = orch.get(run.entry_exec_id)
        assert ex is not None
        # remainder only: $50 of the original $100
        assert ex.config.total_amount_quote == Decimal(50)
        # and roughly half the schedule (scaled by remaining fraction)
        assert ex.config.total_duration == 300.0

    asyncio.run(body())


def test_entry_done_recovery_rearms_exit_watch_without_reordering():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = spot_market_plan(exits=ExitPlan(tp_pct=5.0))
        rec = record(plan, status="running", state={
            "entry_started": True, "entry_done": True,
            "filled_base": "1", "filled_quote": "100", "fees": "0",
            "target_quote": "100",
        })
        orch, c, store = make_desk(adapter, [rec])
        await orch.spawn_controller(c)
        await ticks(orch, c, 2)
        assert adapter.placed == []           # no re-entry
        run = c._runs[plan.plan_id]  # noqa: SLF001
        assert run.phase == PH_EXIT_WATCH
        adapter.set_mid(106)
        await ticks(orch, c, 3)
        sells = [o for o in adapter.placed if o.side is TradeType.SELL]
        assert sells and sells[0].amount_base == Decimal(1)

    asyncio.run(body())


# ---------------------------------------------------------------------------
# 4. spot market hours (tokenized stocks)
# ---------------------------------------------------------------------------

def test_closed_market_holds_trigger_fire():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = spot_market_plan(product="QQQX")
        gate = {"open": False}
        orch, c, store = make_desk(adapter, [record(plan)],
                                   spot_open=lambda: gate["open"])
        await orch.spawn_controller(c)
        await ticks(orch, c, 3)
        assert store["claims"] == [] and adapter.placed == []
        gate["open"] = True
        await ticks(orch, c, 2)
        assert adapter.placed                 # fired at the open

    asyncio.run(body())


def test_twap_suspends_when_market_closes_and_resumes_remainder():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = ExecutionPlan(algo="twap", market="spot", product="QQQX", side="buy",
                             size_quote=100.0, duration_minutes=10, interval_seconds=60)
        gate = {"open": True}
        orch, c, store = make_desk(adapter, [record(plan)],
                                   spot_open=lambda: gate["open"])
        await orch.spawn_controller(c)
        await ticks(orch, c, 2)               # fire + first slice (TAKER fills)
        run = c._runs[plan.plan_id]  # noqa: SLF001
        first_ex = orch.get(run.entry_exec_id)
        assert first_ex is not None and first_ex.filled_quote > 0

        gate["open"] = False                  # the stock market closes
        await ticks(orch, c, 1)
        assert run.phase == PH_SUSPENDED
        assert "entry_suspended" in kinds(c)
        filled_before = run.prior_quote
        assert filled_before > 0              # fills were KEPT

        await ticks(orch, c, 2)               # closed: nothing new
        n_orders = len(adapter.placed)

        gate["open"] = True                   # reopen
        await ticks(orch, c, 2)
        assert "entry_resumed" in kinds(c)
        ex2 = orch.get(run.entry_exec_id)
        assert ex2 is not None and ex2 is not first_ex
        # remainder only
        assert ex2.config.total_amount_quote == plan_quote_remaining(filled_before)
        assert len(adapter.placed) > n_orders

    def plan_quote_remaining(filled):
        return Decimal(100) - filled

    asyncio.run(body())


# ---------------------------------------------------------------------------
# 5. cancel
# ---------------------------------------------------------------------------

def test_cancel_stops_executors_keeps_fills_no_finish_overwrite():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = ExecutionPlan(algo="twap", market="spot", product="ETH", side="buy",
                             size_quote=100.0, duration_minutes=10, interval_seconds=60)
        orch, c, store = make_desk(adapter, [record(plan)])
        await orch.spawn_controller(c)
        await ticks(orch, c, 2)
        run = c._runs[plan.plan_id]  # noqa: SLF001
        ex = orch.get(run.entry_exec_id)
        assert ex is not None and not ex.is_terminated

        # User cancels: the store flips status; the active list no longer has it.
        store["records"] = []
        await ticks(orch, c, 1)
        assert "plan_cancelled" in kinds(c)
        assert plan.plan_id not in c._runs  # noqa: SLF001
        assert ex.is_terminated               # resting work pulled
        assert store["finished"] == []        # store row stays 'cancelled'

    asyncio.run(body())


# ---------------------------------------------------------------------------
# misc: checkpoints happen, mid failures don't stall others
# ---------------------------------------------------------------------------

def test_checkpoints_are_written_and_carry_fills():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = ExecutionPlan(algo="twap", market="spot", product="ETH", side="buy",
                             size_quote=100.0, duration_minutes=10, interval_seconds=60)
        orch, c, store = make_desk(adapter, [record(plan)])
        await orch.spawn_controller(c)
        await ticks(orch, c, 3)
        ck = store["checkpoints"].get(plan.plan_id)
        assert ck and ck["entry_started"] is True
        assert Decimal(ck["filled_quote"]) > 0

    asyncio.run(body())


def test_entry_started_is_checkpointed_before_any_order_is_placed():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = spot_market_plan()
        orch, c, store = make_desk(adapter, [record(plan)])
        await orch.spawn_controller(c)
        await ticks(orch, c, 3)
        started = [(s, n) for s, n in store["checkpoint_log"] if s["entry_started"]]
        assert started, "entry_started must be checkpointed"
        # the FIRST entry_started checkpoint precedes the first venue order —
        # this closes the crash window that would re-fire a one-shot blind
        assert started[0][1] == 0

    asyncio.run(body())


def test_failed_pre_spawn_checkpoint_places_nothing_until_retry():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        plan = spot_market_plan()
        orch, c, store = make_desk(adapter, [record(plan)])
        calls = {"n": 0}

        async def flaky_checkpoint(plan_id, state):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("db temporarily unavailable")
            store["checkpoints"][plan_id] = state
            store.setdefault("checkpoint_log", []).append(
                (dict(state), len(adapter.placed))
            )

        c._checkpoint = flaky_checkpoint  # noqa: SLF001 - fault injection
        await orch.spawn_controller(c)

        await ticks(orch, c, 1)
        assert adapter.placed == []
        assert store["checkpoints"][plan.plan_id]["entry_started"] is False

        await ticks(orch, c, 1)
        assert len(adapter.placed) == 1
        started = [(s, n) for s, n in store["checkpoint_log"] if s["entry_started"]]
        assert started and started[0][1] == 0

    asyncio.run(body())


def test_partial_exit_keeps_entry_vwap_for_remainder():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100), auto_fill_market=True)
        plan = spot_market_plan(size_base=10.0, exits=ExitPlan(tp_pct=5.0))
        orch, c, store = make_desk(adapter, [record(plan)])
        await orch.spawn_controller(c)
        await ticks(orch, c, 3)               # entry fills 10 @ 100
        run = c._runs[plan.plan_id]  # noqa: SLF001
        assert run.phase == PH_EXIT_WATCH
        # simulate a partial close already absorbed: 5 base sold at 110
        adapter.auto_fill_market = False
        adapter.set_mid(110)                  # TP (105) hit -> close spawns, rests
        await ticks(orch, c, 1)
        adapter.fill_order(adapter.placed[-1].id, amount=Decimal(5),
                           price=Decimal(110), partial=True)
        # the close terminates early with only half done (venue IOC/cancel)
        await orch.stop(run.exit_exec_id, CloseType.EARLY_STOP)
        await ticks(orch, c, 2)
        # remainder: 5 base, and its vwap must still be the ENTRY price (100)
        assert run.prior_base == Decimal(5)
        assert run.prior_quote / run.prior_base == Decimal(100)

    asyncio.run(body())


def test_one_bad_feed_does_not_stall_other_plans():
    async def body():
        class FlakyMid(MockNadoAdapter):
            async def mid_price(self, pair):  # type: ignore[override]
                if pair.startswith("BAD"):  # BAD-SPOT (market-qualified)
                    raise RuntimeError("feed down")
                return await super().mid_price(pair)

        adapter = FlakyMid(mid=Decimal(100))
        bad = spot_market_plan(product="BAD",
                               entry_trigger=EntryTrigger(kind="price_below", price=95.0))
        good = spot_market_plan(product="ETH")
        orch, c, store = make_desk(adapter, [record(bad), record(good)])
        await orch.spawn_controller(c)
        await ticks(orch, c, 3)
        assert any(s for s in store["finished"] if s[0] == good.plan_id)

    asyncio.run(body())
