import asyncio
from decimal import Decimal

import pytest
from tests.engine._mock_nado import MockNadoAdapter

from src.nadobro.engine.controllers.volume_bot import VolumeBotController
from src.nadobro.engine.inventory import InventoryRepository
from src.nadobro.engine.orchestrator import ExecutorOrchestrator


def _vb_kwargs(configs):
    return dict(user_id=1, orchestrator=ExecutorOrchestrator(),
                adapter=MockNadoAdapter(mid=Decimal(100)),
                inventory=InventoryRepository(), configs=configs, controller_id="VB")


def test_rejects_perp_leverage_quote_and_empty_pair():
    # market=perp must be rejected (volume bot is spot-only)
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": "KBTC-USDC", "market": "perp"}))
    # leverage != 1 must be rejected
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": "KBTC-USDC", "leverage": 5}))
    # An explicit perp pair must be rejected even though market is spot.
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": "BTC-PERP"}))
    # Empty / missing pair must be rejected.
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": ""}))
    # Quote-only "pair" must be rejected (USDC against itself is not tradeable).
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": "USDC"}))
    with pytest.raises(ValueError):
        VolumeBotController(**_vb_kwargs({"trading_pair": "USDT0"}))


def test_accepts_catalog_listed_pairs_per_network():
    """Per-network bases like ``QQQX`` / ``SPYX`` (testnet) and ``BTC`` /
    ``ETH`` (mainnet) must construct without raising, regardless of whether
    the symbol is dashed or bare. The previous hardcoded
    ``SUPPORTED_SPOT_PAIRS = {KBTC-USDC, WETH-USDC}`` blocked every new
    listing; this test guards against a regression to that behavior.
    """
    for pair in ("KBTC", "WETH", "QQQX", "SPYX", "BTC", "ETH", "kBTC-USDC0", "QQQX-USDT0"):
        controller = VolumeBotController(**_vb_kwargs({"trading_pair": pair}))
        assert controller.trading_pair == pair.upper()


def test_buy_then_sell_cleanup():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1, orchestrator=orch, adapter=adapter, inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC-USDC", "total_amount_quote": "40",
                     "total_duration": 40, "order_interval": 20}, controller_id="VB",
        )
        await orch.spawn_controller(c)
        assert c.phase == "buying"
        buy = orch.get(c.buy_id)
        adapter.fill_order(buy.current_order.id, price=Decimal(100))
        buy.start_ts -= 20
        await orch.tick_controller(c.id)        # ingest slice0, post slice1
        adapter.fill_order(buy.current_order.id, price=Decimal(100))
        buy.start_ts -= 60
        await orch.tick_controller(c.id)        # finalize buy -> spawn sell
        assert buy.is_terminated
        assert c.phase == "selling" and c.sell_id is not None

    asyncio.run(body())


def test_buy_with_no_fills_completes_to_done():
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1, orchestrator=orch, adapter=adapter, inventory=InventoryRepository(),
            configs={"trading_pair": "WETH-USDC", "total_amount_quote": "40",
                     "total_duration": 40, "order_interval": 20}, controller_id="VB",
        )
        await orch.spawn_controller(c)
        buy = orch.get(c.buy_id)
        buy.start_ts -= 200  # all slices due + past duration, nothing filled
        await orch.tick_controller(c.id)
        assert buy.is_terminated
        assert c.phase == "done" and c.sell_id is None

    asyncio.run(body())


async def _pump(adapter, orch, c, n=40):
    """Fill every active TWAP slice and force time forward each tick until the
    controller signals completion (or n ticks elapse)."""
    for _ in range(n):
        for ex in list(orch.list(c.id, active_only=True)):
            co = getattr(ex, "current_order", None)
            if co is not None:
                try:
                    adapter.fill_order(co.id, price=Decimal(100))
                except Exception:
                    pass
            ex.start_ts -= 1000  # all slices due + past duration
        await orch.tick_controller(c.id)
        if getattr(c, "completed", False):
            break


def test_single_round_trip_completes_when_no_target():
    """VOL-LOOP: with no target_volume_usd the bot does ONE round-trip then
    signals completion (was: idled in 'done' forever)."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1, orchestrator=orch, adapter=adapter, inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC-USDC", "total_amount_quote": "40",
                     "total_duration": 40, "order_interval": 20}, controller_id="VB",
        )
        await orch.spawn_controller(c)
        await _pump(adapter, orch, c)
        assert c.completed is True
        assert c.stop_reason == "round_trip_complete"
        assert c.cycles_completed == 1
        assert not c.is_active  # stopped itself so the engine tears it down

    asyncio.run(body())


def test_loops_until_target_volume_then_completes():
    """VOL-LOOP: with a target, the bot re-arms buy->sell cycles until the
    cumulative volume target is met, then completes with target_volume_hit."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        # Each round-trip ≈ 40 buy + 40 sell = ~80 volume; target 200 needs ≥3.
        c = VolumeBotController(
            user_id=1, orchestrator=orch, adapter=adapter, inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC-USDC", "total_amount_quote": "40",
                     "total_duration": 40, "order_interval": 20,
                     "target_volume_usd": "200", "max_cycles": 50}, controller_id="VB",
        )
        await orch.spawn_controller(c)
        await _pump(adapter, orch, c, n=80)
        assert c.completed is True
        assert c.stop_reason == "target_volume_hit"
        assert c.cycles_completed >= 2          # it actually LOOPED
        assert c.session_volume_usd >= Decimal("200")

    asyncio.run(body())


def test_max_cycles_caps_runaway_loop():
    """VOL-NO-CAP: an unreachable target can't loop forever — max_cycles stops
    it (here target is huge but the cap is 2)."""
    async def body():
        adapter = MockNadoAdapter(mid=Decimal(100))
        orch = ExecutorOrchestrator()
        c = VolumeBotController(
            user_id=1, orchestrator=orch, adapter=adapter, inventory=InventoryRepository(),
            configs={"trading_pair": "KBTC-USDC", "total_amount_quote": "40",
                     "total_duration": 40, "order_interval": 20,
                     "target_volume_usd": "1000000", "max_cycles": 2}, controller_id="VB",
        )
        await orch.spawn_controller(c)
        await _pump(adapter, orch, c, n=80)
        assert c.completed is True
        assert c.stop_reason == "max_cycles"
        assert c.cycles_completed == 2

    asyncio.run(body())
