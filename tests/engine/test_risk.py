"""Risk Engine gate + kill-switch tests."""
from __future__ import annotations

from decimal import Decimal

from src.nadobro.engine.risk import (
    ExecutorRequest,
    InMemoryKillSwitchStore,
    RiskEngine,
)
from src.nadobro.engine.types import RiskLimits, RiskState


def test_pretick_daily_loss_floor():
    eng = RiskEngine(RiskLimits(daily_pnl_floor_quote=Decimal(-100)))
    ok, reason = eng.pre_tick_check("c", RiskState(daily_pnl_quote=Decimal(-100)))
    assert not ok and reason == "daily_pnl_floor"
    ok, reason = eng.pre_tick_check("c", RiskState(daily_pnl_quote=Decimal(-50)))
    assert ok and reason is None


def test_pretick_drawdown_cap():
    eng = RiskEngine(RiskLimits(max_drawdown_pct=Decimal("0.2")))
    ok, reason = eng.pre_tick_check("c", RiskState(drawdown_pct=Decimal("0.25")))
    assert not ok and reason == "max_drawdown"


def test_pretick_daily_cost_cap():
    eng = RiskEngine(RiskLimits(daily_cost_cap_usd=Decimal(10)))
    ok, reason = eng.pre_tick_check("c", RiskState(daily_cost_usd=Decimal(10)))
    assert not ok and reason == "daily_cost_cap"


def test_perexec_max_open():
    eng = RiskEngine(RiskLimits(max_open_executors=2))
    ok, reason = eng.pre_executor_check(
        "c", ExecutorRequest(Decimal(10)), RiskState(executor_count=2)
    )
    assert not ok and reason == "max_open_executors"


def test_perexec_max_single_order():
    eng = RiskEngine(RiskLimits(max_single_order_quote=Decimal(100)))
    ok, reason = eng.pre_executor_check("c", ExecutorRequest(Decimal(101)), RiskState())
    assert not ok and reason == "max_single_order_quote"
    ok, _ = eng.pre_executor_check("c", ExecutorRequest(Decimal(100)), RiskState())
    assert ok


def test_perexec_max_position_size():
    eng = RiskEngine(RiskLimits(max_position_size_quote=Decimal(500)))
    ok, reason = eng.pre_executor_check(
        "c", ExecutorRequest(Decimal(10), Decimal(501)), RiskState()
    )
    assert not ok and reason == "max_position_size_quote"


def test_kill_switch_blocks_both_gates_and_persists():
    store = InMemoryKillSwitchStore()
    eng = RiskEngine(RiskLimits(), kill_switch=store)
    eng.kill_switch_on("manual halt")
    ok, reason = eng.pre_tick_check("c", RiskState())
    assert not ok and reason.startswith("kill_switch")
    ok, reason = eng.pre_executor_check("c", ExecutorRequest(Decimal(1)), RiskState())
    assert not ok and reason.startswith("kill_switch")

    # Persistence: a fresh engine over the same store is still killed.
    eng2 = RiskEngine(RiskLimits(), kill_switch=store)
    assert eng2.is_killed()
    ok, _ = eng2.pre_tick_check("c", RiskState())
    assert not ok

    eng2.kill_switch_off()
    assert not store.is_engaged()
    ok, _ = eng2.pre_tick_check("c", RiskState())
    assert ok
