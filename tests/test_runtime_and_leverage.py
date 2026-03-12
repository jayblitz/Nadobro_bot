import asyncio
import json
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

# Lightweight telegram stubs for test environments without python-telegram-bot.
if "telegram" not in sys.modules:
    telegram_mod = types.ModuleType("telegram")
    telegram_constants = types.ModuleType("telegram.constants")
    telegram_ext = types.ModuleType("telegram.ext")

    class _ParseMode:
        MARKDOWN_V2 = "MARKDOWN_V2"
        MARKDOWN = "MARKDOWN"

    class _CallbackContext:
        user_data = {}

    telegram_constants.ParseMode = _ParseMode
    telegram_ext.CallbackContext = _CallbackContext
    sys.modules["telegram"] = telegram_mod
    sys.modules["telegram.constants"] = telegram_constants
    sys.modules["telegram.ext"] = telegram_ext

# Lightweight psycopg2 stubs for import-only test environments.
if "psycopg2" not in sys.modules:
    psycopg2_mod = types.ModuleType("psycopg2")
    psycopg2_pool = types.ModuleType("psycopg2.pool")
    psycopg2_extras = types.ModuleType("psycopg2.extras")
    psycopg2_sql = types.ModuleType("psycopg2.sql")

    class _ThreadedConnectionPool:
        def __init__(self, *args, **kwargs):
            pass

    class _RealDictCursor:
        pass

    class _SqlFragment:
        def __init__(self, value=""):
            self.value = value

        def format(self, *args, **kwargs):
            return self

        def join(self, iterable):
            return self

        def __mul__(self, _):
            return self

    def _sql_factory(value=""):
        return _SqlFragment(value)

    def _identifier_factory(_value=""):
        return _SqlFragment("")

    def _placeholder_factory():
        return _SqlFragment("%s")

    psycopg2_pool.ThreadedConnectionPool = _ThreadedConnectionPool
    psycopg2_extras.RealDictCursor = _RealDictCursor
    psycopg2_sql.SQL = _sql_factory
    psycopg2_sql.Identifier = _identifier_factory
    psycopg2_sql.Placeholder = _placeholder_factory

    psycopg2_mod.pool = psycopg2_pool
    psycopg2_mod.extras = psycopg2_extras
    psycopg2_mod.sql = psycopg2_sql

    sys.modules["psycopg2"] = psycopg2_mod
    sys.modules["psycopg2.pool"] = psycopg2_pool
    sys.modules["psycopg2.extras"] = psycopg2_extras
    sys.modules["psycopg2.sql"] = psycopg2_sql

# Lightweight requests stub for import-only environments.
if "requests" not in sys.modules:
    requests_mod = types.ModuleType("requests")

    class _DummyResponse:
        def json(self):
            return {}

    class _DummySession:
        def get(self, *args, **kwargs):
            return _DummyResponse()

    requests_mod.Session = _DummySession
    sys.modules["requests"] = requests_mod

from src.nadobro.handlers.intent_handlers import _enrich_trade_payload
from src.nadobro.handlers.intent_parser import parse_interaction_intent
from src.nadobro.services import bot_runtime
from src.nadobro.services.stop_loss_service import _should_trigger_stop_loss
from src.nadobro.services.trade_service import _place_take_profit_order


class RuntimeAndLeverageTests(unittest.TestCase):
    def test_enrich_trade_payload_clamps_leverage_by_product_cap(self):
        payload = {
            "direction": "long",
            "order_type": "market",
            "product": "LINK",
            "size": 1.0,
            "leverage": 40,
        }
        settings = {"default_leverage": 3, "slippage": 1}
        enriched = _enrich_trade_payload(telegram_id=1, payload=payload, settings=settings)
        self.assertEqual(enriched["leverage"], 20)

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
        ok, msg = bot_runtime.start_user_bot(
            telegram_id=1,
            strategy="mm",
            product="LINK",
            leverage=40,
            slippage_pct=1,
            passphrase="pw",
        )
        self.assertFalse(ok)
        self.assertIn("Max leverage for LINK is 20x", msg)

    def test_start_user_bot_accepts_valid_cap_for_btc(self):
        fake_user = SimpleNamespace(network_mode=SimpleNamespace(value="mainnet"))
        with patch.object(bot_runtime, "get_user", return_value=fake_user), patch.object(
            bot_runtime, "get_strategy_settings", return_value=("mainnet", {})
        ), patch.object(bot_runtime, "_save_state"), patch.object(bot_runtime, "_ensure_task"):
            ok, msg = bot_runtime.start_user_bot(
                telegram_id=1,
                strategy="mm",
                product="BTC",
                leverage=40,
                slippage_pct=1,
                passphrase="pw",
            )
        self.assertTrue(ok)
        self.assertIn("MM bot started on BTC-PERP", msg)

    def test_stop_all_user_bots_uses_network_passphrase_for_close_all(self):
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

        def _close_stub(user_id, passphrase=None):
            close_calls.append((user_id, passphrase))
            return {"success": True}

        main_tk = f"{telegram_id}:mainnet"
        test_tk = f"{telegram_id}:testnet"
        old_tasks = dict(bot_runtime._tasks)
        old_pass = dict(bot_runtime._session_passphrases)
        try:
            bot_runtime._tasks = {
                main_tk: DummyTask(),
                test_tk: DummyTask(),
            }
            bot_runtime._session_passphrases = {
                main_tk: "main-pass",
                # Intentionally omit testnet passphrase to verify guarded behavior.
            }
            with patch.object(bot_runtime, "query_all", return_value=rows), patch.object(
                bot_runtime, "set_bot_state"
            ), patch.object(bot_runtime, "close_all_positions", side_effect=_close_stub):
                ok, msg = bot_runtime.stop_all_user_bots(telegram_id, cancel_orders=True)
        finally:
            bot_runtime._tasks = old_tasks
            bot_runtime._session_passphrases = old_pass

        self.assertTrue(ok)
        self.assertIn("Stopped 2 running strategy loop(s).", msg)
        self.assertIn("Some close-all actions failed", msg)
        self.assertIn("testnet: no active strategy session passphrase", msg)
        self.assertEqual(close_calls, [(telegram_id, "main-pass")])

    def test_run_cycle_sl_path_returns_tuple(self):
        telegram_id = 7
        network = "mainnet"
        tk = f"{telegram_id}:{network}"
        state = {
            "running": True,
            "strategy": "mm",
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

        old_pass = dict(bot_runtime._session_passphrases)
        try:
            bot_runtime._session_passphrases = {tk: "ok-pass"}
            with patch.object(bot_runtime, "is_trading_paused", return_value=False), patch.object(
                bot_runtime, "run_blocking", side_effect=_run_blocking_stub
            ), patch.object(bot_runtime, "get_user", return_value=fake_user), patch.object(
                bot_runtime, "get_user_readonly_client", return_value=FakeClient()
            ), patch.object(
                bot_runtime, "_save_state"
            ), patch.object(
                bot_runtime, "close_all_positions", return_value={"success": True}
            ), patch.object(
                bot_runtime, "_notify"
            ):
                result = asyncio.run(bot_runtime._run_cycle(telegram_id, network, state))
        finally:
            bot_runtime._session_passphrases = old_pass

        self.assertEqual(result, (True, None))

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


if __name__ == "__main__":
    unittest.main()
