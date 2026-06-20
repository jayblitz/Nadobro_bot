import asyncio
from decimal import Decimal

from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.market_making import MarketMakingController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator
from src.nadobro.engine.risk import RiskEngine
from src.nadobro.engine.types import RiskLimits, RiskState, TradeType


def _mm(adapter, inv, configs, *, orch=None, controller_id=None):
    orch = orch or ExecutorOrchestrator()
    c = MarketMakingController(
        user_id=1, orchestrator=orch, adapter=adapter, inventory=inv,
        configs=configs, controller_id=controller_id,
    )
    return orch, c


BASE = {"trading_pair": "P", "spread_bid_pct": "0.01", "spread_ask_pct": "0.01",
        "order_amount_quote": "10", "price_distance_tolerance": "0.001", "max_base_quote": "1000"}


def test_manual_spread_is_floored_at_fee_clearing_minimum():
    """MM-SPREAD-FLOOR fix: a manual sub-floor spread is raised to
    spread_floor_half_pct so the book can't quote below the fee-clearing
    minimum (which would lose on every fill)."""
    cfg = dict(BASE)
    cfg.update(spread_bid_pct="0.00001", spread_ask_pct="0.00001",  # 0.1 bp, sub-floor
               spread_floor_half_pct="0.00015")                      # 1.5 bp floor
    _, c = _mm(MockNadoAdapter(mid=Decimal(100)), InventoryRepository(), cfg)
    assert c.spread_bid_pct == Decimal("0.00015")
    assert c.spread_ask_pct == Decimal("0.00015")


def test_manual_spread_above_floor_is_unchanged():
    cfg = dict(BASE)
    cfg.update(spread_bid_pct="0.01", spread_ask_pct="0.01", spread_floor_half_pct="0.00015")
    _, c = _mm(MockNadoAdapter(mid=Decimal(100)), InventoryRepository(), cfg)
    assert c.spread_bid_pct == Decimal("0.01")
    assert c.spread_ask_pct == Decimal("0.01")


def test_quotes_around_mid():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _mm(adapter, InventoryRepository(), dict(BASE))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert len(orch.list(c.id, active_only=True)) == 2
        prices = sorted(o.price for o in adapter.placed)
        assert prices == [Decimal(99), Decimal(101)]

    asyncio.run(body())


def test_refresh_on_drift():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch, c = _mm(adapter, InventoryRepository(), dict(BASE))
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        bid1 = c._bid_id
        cancels_before = len(adapter.cancelled)
        adapter.set_mid(Decimal(110))
        await orch.tick_controller(c.id)
        assert c._bid_id != bid1 and len(adapter.cancelled) > cancels_before

    asyncio.run(body())


def test_inventory_suspends_buys_at_max_base():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        inv = InventoryRepository()
        inv.apply_fill(1, "P", "CID", TradeType.BUY, Decimal(20), Decimal(2000))
        orch, c = _mm(adapter, inv, dict(BASE), controller_id="CID")
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert c._bid_id is None and c._ask_id is not None

    asyncio.run(body())


def test_profit_protection_suspends_both():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(90))
        inv = InventoryRepository()
        inv.apply_fill(1, "P", "CID", TradeType.BUY, Decimal(20), Decimal(2000))
        cfg = dict(BASE, profit_protection=True)
        orch, c = _mm(adapter, inv, cfg, controller_id="CID")
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert c._bid_id is None and c._ask_id is None

    asyncio.run(body())


def test_risk_pretick_block_skips_quoting():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        risk = RiskEngine(RiskLimits(daily_pnl_floor_quote=Decimal(0)))

        def provider(_cid):
            s = RiskState()
            s.daily_pnl_quote = Decimal(-100)
            return s

        orch = ExecutorOrchestrator(risk_engine=risk, risk_state_provider=provider)
        _, c = _mm(adapter, InventoryRepository(), dict(BASE), orch=orch)
        await orch.spawn_controller(c)
        await orch.tick_controller(c.id)
        assert any(ev.kind == "controller_skipped" for ev in orch.event_log)
        assert orch.list(c.id, active_only=True) == []

    asyncio.run(body())
