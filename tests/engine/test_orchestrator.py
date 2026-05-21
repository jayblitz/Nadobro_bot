"""Executor Orchestrator tests: spawn, list-by-controller, batched cancel,
risk gate, kill switch, event emission."""
from __future__ import annotations

import asyncio
from decimal import Decimal

from src.nadobro.engine.executor_base import Executor, ExecutorFailed
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.risk import ExecutorRequest, RiskEngine
from src.nadobro.engine.types import CloseType, RiskLimits, RiskState


class _Dummy(Executor):
    async def on_create(self) -> None:
        self._activate()

    async def on_tick(self) -> None:
        pass


def _mk(cid: str = "c1", uid: int = 1) -> _Dummy:
    return _Dummy(user_id=uid, controller_id=cid, trading_pair="SOL-USDC", adapter=None)


def test_spawn_and_list_by_controller():
    async def body():
        orch = ExecutorOrchestrator()
        e1, e2, e3 = _mk("c1"), _mk("c2"), _mk("c1")
        assert await orch.spawn(e1)
        assert await orch.spawn(e2)
        assert await orch.spawn(e3)
        assert len(orch.list()) == 3
        assert len(orch.list("c1")) == 2
        assert e1.is_active

    asyncio.run(body())


def test_batched_cancel_stops_controller_children():
    async def body():
        orch = ExecutorOrchestrator()
        children = [_mk("c1") for _ in range(5)]
        for e in children:
            await orch.spawn(e)
        await orch.spawn(_mk("c2"))  # unrelated controller
        n = await orch.stop_controller("c1", CloseType.EARLY_STOP)
        assert n == 5
        assert all(
            e.is_terminated and e.close_type is CloseType.EARLY_STOP for e in children
        )
        assert len(orch.list("c2", active_only=True)) == 1

    asyncio.run(body())


def test_risk_gate_blocks_spawn():
    async def body():
        eng = RiskEngine(RiskLimits(max_open_executors=1))
        orch = ExecutorOrchestrator(
            risk_engine=eng, risk_state_provider=lambda cid: RiskState()
        )
        assert await orch.spawn(_mk("c1"), ExecutorRequest(Decimal(10)))
        blocked = await orch.spawn(_mk("c1"), ExecutorRequest(Decimal(10)))
        assert blocked is False
        rejects = [e for e in orch.event_log if e.kind == "spawn_rejected"]
        assert rejects and rejects[0].reason == "max_open_executors"

    asyncio.run(body())


def test_kill_switch_blocks_spawn():
    async def body():
        orch = ExecutorOrchestrator()
        orch.kill_switch_on("halt")
        assert orch.is_killed
        assert await orch.spawn(_mk()) is False

    asyncio.run(body())


def test_event_emission_and_drain():
    async def body():
        orch = ExecutorOrchestrator()
        e = _mk()
        await orch.spawn(e)
        await orch.stop(e.id)
        kinds = [ev.kind for ev in orch.event_log]
        assert "spawned" in kinds and "stopped" in kinds
        drained = orch.drain_events()
        assert len(drained) >= 2

    asyncio.run(body())


class _Counter(Executor):
    async def on_create(self) -> None:
        self._activate()
        self.ticks = 0

    async def on_tick(self) -> None:
        self.ticks += 1


class _FailOnCreate(Executor):
    async def on_create(self) -> None:
        self._activate()
        raise ExecutorFailed("boom on create")

    async def on_tick(self) -> None:
        pass


class _FailOnTick(Executor):
    async def on_create(self) -> None:
        self._activate()

    async def on_tick(self) -> None:
        raise ExecutorFailed("boom on tick")


def test_tick_drives_executor_and_emits():
    async def body():
        orch = ExecutorOrchestrator()
        e = _Counter(user_id=1, controller_id="c", trading_pair="P", adapter=None)
        await orch.spawn(e)
        await orch.tick(e.id)
        assert e.ticks == 1
        assert any(ev.kind == "tick" for ev in orch.event_log)
        await orch.tick("unknown")  # no-op

    asyncio.run(body())


def test_tick_failure_emits_failed():
    async def body():
        orch = ExecutorOrchestrator()
        e = _FailOnTick(user_id=1, controller_id="c", trading_pair="P", adapter=None)
        await orch.spawn(e)
        await orch.tick(e.id)
        assert any(ev.kind == "failed" for ev in orch.event_log)

    asyncio.run(body())


def test_spawn_on_create_failure_returns_false():
    async def body():
        orch = ExecutorOrchestrator()
        e = _FailOnCreate(user_id=1, controller_id="c", trading_pair="P", adapter=None)
        assert await orch.spawn(e) is False
        assert any(ev.kind == "failed" for ev in orch.event_log)

    asyncio.run(body())


def test_stop_unknown_returns_false():
    async def body():
        orch = ExecutorOrchestrator()
        assert await orch.stop("nope") is False

    asyncio.run(body())


def test_kill_switch_off_reenables_spawn():
    async def body():
        orch = ExecutorOrchestrator()
        orch.kill_switch_on("x")
        assert orch.is_killed
        orch.kill_switch_off()
        assert not orch.is_killed
        assert await orch.spawn(_mk())

    asyncio.run(body())


def test_events_async_generator_yields():
    async def body():
        orch = ExecutorOrchestrator()
        await orch.spawn(_mk())
        gen = orch.events()
        ev = await asyncio.wait_for(gen.__anext__(), timeout=1.0)
        assert ev.kind == "spawned"

    asyncio.run(body())
