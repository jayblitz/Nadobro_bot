import asyncio
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.config import get_product_max_leverage
from src.nadobro.handlers.intent_handlers import _enrich_trade_payload
from src.nadobro.handlers.intent_parser import parse_interaction_intent, parse_position_management_intent
from src.nadobro.handlers import callbacks, home_card, formatters
from src.nadobro.i18n import get_active_language, language_context
from src.nadobro.services import bot_runtime
from src.nadobro.services import execution_queue
from src.nadobro.services import runtime_supervisor
from src.nadobro.services import trade_service
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.stop_loss_service import _should_trigger_stop_loss
from src.nadobro.services.trade_service import _place_take_profit_order
from src.nadobro.strategies import delta_neutral


class RuntimeAndLeverageTests(unittest.TestCase):
    def test_run_blocking_preserves_language_contextvars(self):
        async def _run():
            with language_context("fr"):
                self.assertEqual(get_active_language(), "fr")
                self.assertEqual(await run_blocking(get_active_language), "fr")

        asyncio.run(_run())

    def test_build_portfolio_view_handles_position_failures(self):
        class FailingClient:
            def get_all_positions(self):
                raise RuntimeError("boom")

        fake_user = SimpleNamespace(network_mode=SimpleNamespace(value="mainnet"))
        with patch.object(home_card, "get_user", return_value=fake_user), patch.object(
            home_card, "get_user_readonly_client", return_value=FailingClient()
        ):
            text, reply_markup = home_card.build_portfolio_view(telegram_id=7)

        self.assertIn("Portfolio refresh is temporarily unavailable", text)
        self.assertIsNotNone(reply_markup)

    def test_resolve_home_view_uses_blocking_wrapper_for_portfolio(self):
        run_blocking_mock = AsyncMock(return_value=("portfolio", "keyboard"))
        with patch.object(home_card, "run_blocking", run_blocking_mock):
            result = asyncio.run(home_card.resolve_home_view("portfolio:view", telegram_id=5))

        self.assertEqual(result, ("portfolio", "keyboard"))
        run_blocking_mock.assert_awaited_once_with(home_card._view_portfolio_text, 5)

    def test_handle_portfolio_history_invalid_page_does_not_raise(self):
        query = SimpleNamespace()
        edit_mock = AsyncMock()

        async def _run_blocking_stub(func, *args, **kwargs):
            if func is callbacks.get_trade_history:
                return [
                    {
                        "product": "BTC-PERP",
                        "side": "LONG",
                        "status": "closed",
                        "price": 60000,
                        "close_price": 60500,
                        "pnl": 25,
                        "created_at": "2026-03-27T12:00:00Z",
                    }
                ]
            return func(*args, **kwargs)

        with patch.object(callbacks, "run_blocking", side_effect=_run_blocking_stub), patch.object(
            callbacks, "_edit_loc", edit_mock
        ):
            asyncio.run(callbacks._handle_portfolio(query, "portfolio:history:not-a-number", telegram_id=77))

        edit_mock.assert_awaited()

    def test_stop_workers_cancels_and_awaits_worker_tasks(self):
        async def _worker():
            await asyncio.sleep(60)

        async def _run():
            old_workers = list(execution_queue._workers)
            try:
                task1 = asyncio.create_task(_worker())
                task2 = asyncio.create_task(_worker())
                execution_queue._workers = [task1, task2]
                await execution_queue.stop_workers()
                self.assertEqual(execution_queue._workers, [])
                self.assertTrue(task1.cancelled())
                self.assertTrue(task2.cancelled())
            finally:
                execution_queue._workers = old_workers

        asyncio.run(_run())

    def test_enrich_trade_payload_clamps_leverage_by_product_cap(self):
        cap = get_product_max_leverage("LINK", network="mainnet")
        payload = {
            "direction": "long",
            "order_type": "market",
            "product": "LINK",
            "size": 1.0,
            "leverage": 40,
        }
        settings = {"default_leverage": 3, "slippage": 1}
        enriched = _enrich_trade_payload(telegram_id=1, payload=payload, settings=settings)
        self.assertEqual(enriched["leverage"], min(40, cap))

    def test_enrich_trade_payload_enforces_minimum_leverage(self):
        payload = {
            "direction": "long",
            "order_type": "market",
            "product": "BTC",
            "size": 1.0,
            "leverage": 0,
        }
        settings = {"default_leverage": 0, "slippage": 1}
        enriched = _enrich_trade_payload(telegram_id=1, payload=payload, settings=settings)
        self.assertEqual(enriched["leverage"], 1)

    def test_start_user_bot_rejects_product_leverage_over_cap(self):
        cap = get_product_max_leverage("LINK", network="mainnet")
        ok, msg = bot_runtime.start_user_bot(
            telegram_id=9_999_990_001,
            strategy="grid",
            product="LINK",
            leverage=float(cap) + 1.0,
            slippage_pct=1,
        )
        self.assertFalse(ok)
        self.assertIn(f"Max leverage for LINK is {cap}x", msg)

    @unittest.skipUnless(
        os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DATABASE_URL"),
        "PostgreSQL required once preflight runs (DATABASE_URL)",
    )
    def test_start_user_bot_skips_preflight_when_leverage_ok_but_no_wallet(self):
        cap = get_product_max_leverage("BTC", network="mainnet")
        lev = min(40.0, float(cap))
        ok, msg = bot_runtime.start_user_bot(
            telegram_id=9_999_990_002,
            strategy="grid",
            product="BTC",
            leverage=lev,
            slippage_pct=1,
        )
        self.assertFalse(ok)
        self.assertNotIn("Max leverage", msg)

    def test_start_user_bot_blocks_when_preflight_fails(self):
        fake_user = SimpleNamespace(network_mode=SimpleNamespace(value="mainnet"))
        with patch.object(bot_runtime, "get_user", return_value=fake_user), patch.object(
            bot_runtime, "run_strategy_start_preflight", return_value=(False, "Wallet not linked")
        ):
            ok, msg = bot_runtime.start_user_bot(
                telegram_id=1,
                strategy="vol",
                product="BTC",
                leverage=1,
                slippage_pct=1,
            )
        self.assertFalse(ok)
        self.assertEqual(msg, "Wallet not linked")

    def test_stop_all_user_bots_closes_each_running_network(self):
        telegram_id = 42
        rows = [
            {
                "key": f"{bot_runtime.STATE_PREFIX}{telegram_id}:mainnet",
                "value": json.dumps({"running": True}),
            },
            {
                "key": f"{bot_runtime.STATE_PREFIX}{telegram_id}:testnet",
                "value": json.dumps({"running": True}),
            },
        ]

        class DummyTask:
            def cancel(self):
                return None

        close_calls = []

        def _close_stub(user_id, network=None, **kwargs):
            close_calls.append((user_id, network))
            return {"success": True}

        main_tk = f"{telegram_id}:mainnet"
        test_tk = f"{telegram_id}:testnet"
        old_tasks = dict(bot_runtime._tasks)
        try:
            bot_runtime._tasks = {
                main_tk: DummyTask(),
                test_tk: DummyTask(),
            }
            with patch.object(bot_runtime, "query_all", return_value=rows), patch.object(
                bot_runtime, "set_bot_state"
            ), patch.object(bot_runtime, "close_all_positions", side_effect=_close_stub):
                ok, msg = bot_runtime.stop_all_user_bots(telegram_id, cancel_orders=True)
        finally:
            bot_runtime._tasks = old_tasks

        self.assertTrue(ok)
        self.assertIn("Stopped 2 running strategy loop(s).", msg)
        self.assertCountEqual(
            close_calls,
            [(telegram_id, "mainnet"), (telegram_id, "testnet")],
        )

    def test_run_cycle_sl_path_returns_tuple(self):
        telegram_id = 7
        network = "mainnet"
        state = {
            "running": True,
            "strategy": "grid",
            "product": "BTC",
            "reference_price": 100.0,
            "sl_pct": 5.0,
            "tp_pct": 0.0,
            "interval_seconds": 1,
            "last_run_ts": 0.0,
        }

        class FakeClient:
            def get_market_price(self, _product_id):
                return {"mid": 90.0}

        fake_user = SimpleNamespace(network_mode=SimpleNamespace(value=network))

        async def _run_blocking_stub(func, *args, **kwargs):
            return func(*args, **kwargs)

        with patch.object(bot_runtime, "is_trading_paused", return_value=False), patch.object(
            bot_runtime, "run_blocking", side_effect=_run_blocking_stub
        ), patch.object(bot_runtime, "get_user", return_value=fake_user), patch.object(
            bot_runtime, "get_user_readonly_client", return_value=FakeClient()
        ), patch.object(
            bot_runtime, "get_user_nado_client", return_value=FakeClient()
        ), patch.object(
            bot_runtime, "_save_state"
        ), patch.object(
            bot_runtime, "close_all_positions", return_value={"success": True}
        ), patch.object(
            bot_runtime, "_notify"
        ):
            result = asyncio.run(bot_runtime._run_cycle(telegram_id, network, state))

        self.assertEqual(result, (True, None))

    def test_run_cycle_grid_uses_strategy_pnl_stop_action(self):
        telegram_id = 8
        network = "mainnet"
        state = {
            "running": True,
            "strategy": "rgrid",
            "product": "BTC",
            "reference_price": 100.0,
            "sl_pct": 0.1,  # Should not be used as primary guard for GRID now.
            "interval_seconds": 1,
            "last_run_ts": 0.0,
        }

        class FakeClient:
            def get_market_price(self, _product_id):
                return {"mid": 90.0}

            def get_open_orders(self, _product_id):
                return []

        fake_user = SimpleNamespace(network_mode=SimpleNamespace(value=network))

        async def _run_blocking_stub(func, *args, **kwargs):
            return func(*args, **kwargs)

        with patch.object(bot_runtime, "is_trading_paused", return_value=False), patch.object(
            bot_runtime, "run_blocking", side_effect=_run_blocking_stub
        ), patch.object(bot_runtime, "get_user", return_value=fake_user), patch.object(
            bot_runtime, "get_user_readonly_client", return_value=FakeClient()
        ), patch.object(
            bot_runtime, "get_user_nado_client", return_value=FakeClient()
        ), patch.object(
            bot_runtime, "_dispatch_strategy", return_value={"success": True, "action": "grid_stop_loss_hit", "detail": "stop"}
        ), patch.object(
            bot_runtime, "_save_state"
        ), patch.object(
            bot_runtime, "close_all_positions", return_value={"success": True}
        ) as close_mock, patch.object(
            bot_runtime, "_notify"
        ):
            result = asyncio.run(bot_runtime._run_cycle(telegram_id, network, state))

        self.assertEqual(result, (True, None))
        self.assertTrue(close_mock.called)

    def test_ensure_task_uses_cached_loop_when_called_off_loop(self):
        calls = []

        class FakeLoop:
            def is_running(self):
                return True

            def call_soon_threadsafe(self, fn, *args):
                calls.append((fn, args))
                fn(*args)

        old_loop = bot_runtime._runtime_loop
        old_tasks = dict(bot_runtime._tasks)
        try:
            bot_runtime._runtime_loop = FakeLoop()
            bot_runtime._tasks = {}
            with patch.object(bot_runtime, "_schedule_task_on_loop") as schedule_mock:
                bot_runtime._ensure_task(telegram_id=123, network="testnet")
                schedule_mock.assert_called_once_with(123, "testnet")
            self.assertEqual(len(calls), 1)
        finally:
            bot_runtime._runtime_loop = old_loop
            bot_runtime._tasks = old_tasks

    def test_parse_interaction_intent_routes_current_pnl_question(self):
        intent = parse_interaction_intent("What is my current pnl?")
        self.assertIsNotNone(intent)
        self.assertEqual(intent.get("action"), "open_view")
        self.assertEqual(intent.get("target"), "portfolio:view")

    def test_parse_interaction_intent_does_not_hijack_generic_pnl_question(self):
        intent = parse_interaction_intent("What is pnl?")
        self.assertIsNone(intent)

    def test_parse_interaction_intent_routes_market_radar_to_points(self):
        intent = parse_interaction_intent("market radar")
        self.assertIsNotNone(intent)
        self.assertEqual(intent.get("action"), "open_view")
        self.assertEqual(intent.get("target"), "points:view")

    def test_parse_position_management_close_all(self):
        intent = parse_position_management_intent("close all positions")
        self.assertIsNotNone(intent)
        self.assertEqual(intent.get("action"), "close_all")
        intent2 = parse_position_management_intent("market close everything")
        self.assertIsNotNone(intent2)
        self.assertEqual(intent2.get("action"), "close_all")

    def test_parse_position_management_market_close(self):
        intent = parse_position_management_intent("close my BTC position")
        self.assertIsNotNone(intent)
        self.assertEqual(intent.get("action"), "close_market")
        self.assertEqual(intent.get("product"), "BTC")
        self.assertIsNone(intent.get("size"))

    def test_parse_position_management_limit_close(self):
        intent = parse_position_management_intent("close BTC at 69500")
        self.assertIsNotNone(intent)
        self.assertEqual(intent.get("action"), "limit_close")
        self.assertEqual(intent.get("product"), "BTC")
        self.assertEqual(intent.get("limit_price"), 69500.0)

    def test_parse_position_management_tp_sl(self):
        intent = parse_position_management_intent("set TP order for my BTC open position at 69500")
        self.assertIsNotNone(intent)
        self.assertEqual(intent.get("action"), "set_tp_sl")
        self.assertEqual(intent.get("product"), "BTC")
        self.assertEqual(intent.get("tp_price"), 69500.0)

    def test_parse_position_management_defers_to_full_trade_intent(self):
        intent = parse_position_management_intent("long 5 BTC with 20x leverage")
        self.assertIsNone(intent)

    def test_place_take_profit_order_places_opposite_side_limit(self):
        calls = []

        class FakeClient:
            def place_limit_order(self, product_id, size, price, is_buy=True):
                calls.append((product_id, size, price, is_buy))
                return {"success": True, "digest": "tp-digest"}

        result = _place_take_profit_order(
            client=FakeClient(),
            product_id=1,
            size=2.5,
            is_long=True,
            tp_price=70000.0,
        )
        self.assertTrue(result.get("tp_set"))
        self.assertEqual(calls, [(1, 2.5, 70000.0, False)])

    def test_should_trigger_stop_loss_for_long_and_short(self):
        self.assertTrue(_should_trigger_stop_loss("LONG", 68000.0, 68500.0))
        self.assertFalse(_should_trigger_stop_loss("LONG", 69000.0, 68500.0))
        self.assertTrue(_should_trigger_stop_loss("SHORT", 69000.0, 68500.0))
        self.assertFalse(_should_trigger_stop_loss("SHORT", 68000.0, 68500.0))

    def test_runtime_supervisor_group_mapping(self):
        self.assertEqual(runtime_supervisor.strategy_worker_group("grid"), "mm_grid")
        self.assertEqual(runtime_supervisor.strategy_worker_group("rgrid"), "mm_grid")
        self.assertEqual(runtime_supervisor.strategy_worker_group("dn"), "dn")
        self.assertEqual(runtime_supervisor.strategy_worker_group("vol"), "vol")
        self.assertEqual(runtime_supervisor.strategy_worker_group("bro"), "bro")

    def test_cycle_result_label_marks_skip(self):
        self.assertEqual(bot_runtime._cycle_result_label(True, "skipped_interval"), "skipped")
        self.assertEqual(bot_runtime._cycle_result_label(True, "maintenance_pause"), "skipped")
        self.assertEqual(bot_runtime._cycle_result_label(True, None), "ok")
        self.assertEqual(bot_runtime._cycle_result_label(False, "boom"), "error")

    def test_submit_with_timeout_returns_timeout_error(self):
        def _slow():
            import time as _t
            _t.sleep(0.2)
            return {"success": True}

        ok, result = trade_service._submit_with_timeout(_slow, timeout_s=0.01)
        self.assertFalse(ok)
        self.assertIn("timed out", str(result).lower())

    def test_dn_wait_mode_stays_idle_on_unfavorable_funding(self):
        class FakeClient:
            def get_funding_rate(self, _product_id):
                return {"funding_rate": -0.0002}

            def get_all_positions(self):
                return []

            def get_balance(self):
                return {"balances": {1001: 0.0}}

        state = {
            "product": "BTC",
            "notional_usd": 100.0,
            "leverage": 2.0,
            "funding_entry_mode": "wait",
            "slippage_pct": 1.0,
        }
        with patch(
            "src.nadobro.config.get_dn_pair",
            return_value={
                "product": "BTC",
                "perp_product_id": 1,
                "perp_symbol": "BTC-PERP",
                "spot_product_id": 1001,
                "spot_symbol": "KBTC",
                "entry_allowed": True,
                "entry_block_reason": "",
            },
        ):
            result = delta_neutral.run_cycle(
                telegram_id=1,
                network="testnet",
                state=state,
                client=FakeClient(),
                mid=50000.0,
                product_id=1,
                product="BTC",
                open_orders=[],
            )
        self.assertTrue(result.get("success"))
        self.assertEqual(result.get("action"), "wait_unfavorable")

    def test_dn_xstock_spot_buy_uses_wrapped_target_size(self):
        """xStocks: perp size is share-equivalent; spot book is wrapped tokens (amount / exchange_rate)."""

        class FakeClient:
            def get_funding_rate(self, _product_id):
                return {"funding_rate": 0.0002}

            def get_all_positions(self):
                return [{"product_id": 200, "amount": -1.0, "side": "SHORT"}]

            def get_balance(self):
                return {"balances": {50: 0.0}}

        state = {
            "product": "NVDA",
            "notional_usd": 105.0,
            "leverage": 2.0,
            "funding_entry_mode": "enter_anyway",
            "slippage_pct": 1.0,
        }
        captured = {}

        def _spot_exec(*args, **kwargs):
            captured["size"] = args[2]
            return {"success": True}

        with patch(
            "src.nadobro.config.get_dn_pair",
            return_value={
                "product": "NVDA",
                "perp_product_id": 200,
                "perp_symbol": "NVDA-PERP",
                "spot_product_id": 50,
                "spot_symbol": "WBNVDA",
                "exchange_rate_x18": "1050000000000000000",
                "entry_allowed": True,
                "entry_block_reason": "",
            },
        ), patch(
            "src.nadobro.services.trade_service.execute_spot_market_order",
            side_effect=_spot_exec,
        ), patch(
            "src.nadobro.services.trade_service.execute_market_order",
            return_value={"success": True},
        ):
            delta_neutral.run_cycle(
                telegram_id=1,
                network="testnet",
                state=state,
                client=FakeClient(),
                mid=100.0,
                product_id=200,
                product="NVDA",
                open_orders=[],
            )
        # target_perp = 105/100 = 1.05 shares; wrapped = 1.05 / 1.05 = 1.0 token
        self.assertAlmostEqual(captured.get("size", 0.0), 1.0, places=6)

    def test_dn_entry_blocked_returns_wait_when_spot_leg_needed(self):
        """If spot must increase but entry is not allowed, do not fall through to perp orders."""

        class FakeClient:
            def get_funding_rate(self, _product_id):
                return {"funding_rate": 0.0002}

            def get_all_positions(self):
                return []

            def get_balance(self):
                return {"balances": {50: 0.0}}

        state = {
            "product": "NVDA",
            "notional_usd": 100.0,
            "leverage": 2.0,
            "funding_entry_mode": "enter_anyway",
            "slippage_pct": 1.0,
        }

        with patch(
            "src.nadobro.config.get_dn_pair",
            return_value={
                "product": "NVDA",
                "perp_product_id": 200,
                "perp_symbol": "NVDA-PERP",
                "spot_product_id": 50,
                "spot_symbol": "WBNVDA",
                "exchange_rate_x18": "1000000000000000000",
                "entry_allowed": False,
                "entry_block_reason": "Spot market is currently closed.",
            },
        ), patch(
            "src.nadobro.services.trade_service.execute_spot_market_order",
        ) as spot_mock, patch(
            "src.nadobro.services.trade_service.execute_market_order",
        ) as perp_mock:
            result = delta_neutral.run_cycle(
                telegram_id=1,
                network="testnet",
                state=state,
                client=FakeClient(),
                mid=100.0,
                product_id=200,
                product="NVDA",
                open_orders=[],
            )
        self.assertTrue(result.get("success"))
        self.assertEqual(result.get("action"), "wait_market_hours")
        self.assertIn("closed", (result.get("detail") or "").lower())
        spot_mock.assert_not_called()
        perp_mock.assert_not_called()

    def test_dn_enter_anyway_opens_short_on_unfavorable_funding(self):
        class FakeClient:
            def get_funding_rate(self, _product_id):
                return {"funding_rate": -0.0002}

            def get_all_positions(self):
                return []

            def get_balance(self):
                return {"balances": {1001: 0.0}}

        state = {
            "product": "WBSPYX",
            "notional_usd": 100.0,
            "leverage": 2.0,
            "funding_entry_mode": "enter_anyway",
            "slippage_pct": 1.0,
        }
        with patch(
            "src.nadobro.config.get_dn_pair",
            return_value={
                "product": "WBSPYX",
                "perp_product_id": 117,
                "perp_symbol": "WBSPYX-PERP",
                "spot_product_id": 1001,
                "spot_symbol": "WBSPYX",
                "entry_allowed": True,
                "entry_block_reason": "",
            },
        ), patch(
            "src.nadobro.services.trade_service.execute_spot_market_order",
            return_value={"success": True},
        ), patch(
            "src.nadobro.services.trade_service.execute_market_order",
            return_value={"success": True},
        ):
            result = delta_neutral.run_cycle(
                telegram_id=1,
                network="testnet",
                state=state,
                client=FakeClient(),
                mid=50000.0,
                product_id=117,
                product="WBSPYX",
                open_orders=[],
            )
        self.assertTrue(result.get("success"))
        self.assertEqual(result.get("action"), "enter_short")

    def test_start_user_bot_rejects_dn_pair_when_entry_blocked(self):
        fake_user = SimpleNamespace(network_mode=SimpleNamespace(value="testnet"))
        with patch.object(bot_runtime, "get_user", return_value=fake_user), patch.object(
            bot_runtime,
            "get_dn_pair",
            return_value={
                "product": "WBSPYX",
                "perp_product_id": 117,
                "spot_product_id": 118,
                "entry_allowed": False,
                "entry_block_reason": "Spot market is currently closed.",
            },
        ):
            ok, msg = bot_runtime.start_user_bot(
                telegram_id=1,
                strategy="dn",
                product="WBSPYX",
                leverage=2,
                slippage_pct=1,
            )
        self.assertFalse(ok)
        self.assertIn("currently closed", msg)

    def test_dn_strategy_defaults_include_worker_group_and_funding_mode(self):
        from src.nadobro.services.runtime_supervisor import strategy_worker_group

        dn = bot_runtime._strategy_defaults("dn")
        self.assertEqual(dn.get("funding_entry_mode"), "enter_anyway")
        self.assertEqual(strategy_worker_group("dn"), "dn")

    def test_handle_strategy_job_vol_overlap_drops_extra_pending_tick(self):
        telegram_id = 77
        network = "mainnet"
        key = f"{telegram_id}:{network}"
        old_locks = dict(bot_runtime._job_locks)
        old_pending = dict(bot_runtime._job_pending_payloads)
        old_coalesced = dict(bot_runtime._job_coalesce_counts)
        old_stats = dict(bot_runtime._job_stats)
        lock = asyncio.Lock()

        async def _run():
            await lock.acquire()
            bot_runtime._job_locks[key] = lock
            bot_runtime._job_pending_payloads[key] = {"telegram_id": telegram_id, "network": network, "strategy": "vol"}
            await bot_runtime.handle_strategy_job({"telegram_id": telegram_id, "network": network, "strategy": "vol"})

        try:
            asyncio.run(_run())
            self.assertEqual(bot_runtime._job_pending_payloads[key]["strategy"], "vol")
            self.assertEqual(int(bot_runtime._job_coalesce_counts.get(key, 0)), 0)
            self.assertGreaterEqual(int(bot_runtime._job_stats.get("vol_overlap_skips", 0)), 1)
        finally:
            bot_runtime._job_locks = old_locks
            bot_runtime._job_pending_payloads = old_pending
            bot_runtime._job_coalesce_counts = old_coalesced
            bot_runtime._job_stats = old_stats

    def test_enqueue_strategy_tracks_vol_stats(self):
        old_stats = dict(execution_queue._stats)
        old_workers = list(execution_queue._workers)
        old_seen = dict(execution_queue._dedupe_seen)

        class _Worker:
            def done(self):
                return False

            def get_name(self):
                return "strategy-0"

        async def _run():
            execution_queue._stats.update({k: 0 for k in execution_queue._stats.keys()})
            execution_queue._workers = [_Worker()]
            execution_queue._dedupe_seen.clear()
            enqueued_first = await execution_queue.enqueue_strategy(
                {"telegram_id": 1, "network": "mainnet", "strategy": "vol"},
                dedupe_key="dup-key",
            )
            enqueued_second = await execution_queue.enqueue_strategy(
                {"telegram_id": 1, "network": "mainnet", "strategy": "vol"},
                dedupe_key="dup-key",
            )
            return enqueued_first, enqueued_second

        try:
            first, second = asyncio.run(_run())
            self.assertTrue(first)
            self.assertFalse(second)
            self.assertEqual(int(execution_queue._stats.get("vol_strategy_enqueued") or 0), 1)
            self.assertEqual(int(execution_queue._stats.get("vol_strategy_deduped") or 0), 1)
        finally:
            execution_queue._stats = old_stats
            execution_queue._workers = old_workers
            execution_queue._dedupe_seen = old_seen

    def test_fmt_status_overview_surfaces_cycle_and_order_reason(self):
        status = {
            "running": True,
            "strategy": "vol",
            "product": "BTC",
            "notional_usd": 100,
            "runs": 3,
            "interval_seconds": 10,
            "started_at": "2026-01-01T00:00:00",
            "next_cycle_in": 0,
            "last_cycle_result": "error",
            "last_error": "VOL[opened_market_wait_close/execution]: order rejected",
            "last_error_category": "execution",
            "error_streak": 4,
            "runtime_diagnostics": {"queue": {"strategy_qsize": 0, "strategy_qmax": 500}, "pending_coalesced_ticks": 0},
            "order_observability": {"orders_placed": 0, "orders_filled": 0, "orders_cancelled": 0, "cycles": 3, "zero_order_cycles": 3, "last_reason": "order rejected"},
            "vol_order_attempts": 2,
            "vol_order_failures": 2,
            "last_order_error": "builder unavailable",
        }
        onboarding = {"onboarding_complete": True, "network": "mainnet", "has_key": True, "funded": True}
        text = formatters.fmt_status_overview(status, onboarding)
        self.assertIn("Last cycle", text)
        self.assertIn("Error class", text)
        self.assertIn("Last order error", text)


if __name__ == "__main__":
    unittest.main()
