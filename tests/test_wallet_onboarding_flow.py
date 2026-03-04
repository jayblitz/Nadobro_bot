import asyncio
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


# Lightweight telegram stubs for import-only test environments.
if "telegram" not in sys.modules:
    telegram_mod = types.ModuleType("telegram")
    telegram_constants = types.ModuleType("telegram.constants")
    telegram_ext = types.ModuleType("telegram.ext")
    telegram_error = types.ModuleType("telegram.error")

    class _ParseMode:
        MARKDOWN_V2 = "MARKDOWN_V2"
        MARKDOWN = "MARKDOWN"

    class _ChatAction:
        TYPING = "typing"

    class _CallbackContext:
        user_data = {}

    class _Update:
        pass

    class _InlineKeyboardButton:
        def __init__(self, text, callback_data=None):
            self.text = text
            self.callback_data = callback_data

    class _InlineKeyboardMarkup:
        def __init__(self, inline_keyboard):
            self.inline_keyboard = inline_keyboard

    class _BadRequest(Exception):
        pass

    telegram_constants.ParseMode = _ParseMode
    telegram_constants.ChatAction = _ChatAction
    telegram_ext.CallbackContext = _CallbackContext
    telegram_mod.Update = _Update
    telegram_mod.InlineKeyboardButton = _InlineKeyboardButton
    telegram_mod.InlineKeyboardMarkup = _InlineKeyboardMarkup
    telegram_error.BadRequest = _BadRequest

    sys.modules["telegram"] = telegram_mod
    sys.modules["telegram.constants"] = telegram_constants
    sys.modules["telegram.ext"] = telegram_ext
    sys.modules["telegram.error"] = telegram_error

# Lightweight psycopg2 stubs for import-only environments.
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

from src.nadobro.handlers import callbacks, commands


class WalletOnboardingFlowTests(unittest.TestCase):
    def test_seed_wallet_setup_flow_sets_context_and_returns_message(self):
        context = SimpleNamespace(user_data={})
        fake_key = SimpleNamespace(hex=lambda: "0xabc123")
        fake_account = SimpleNamespace(key=fake_key, address="0x1111111111111111111111111111111111111111")

        with patch.object(callbacks.Account, "create", return_value=fake_account):
            msg = callbacks.seed_wallet_setup_flow(context)

        self.assertEqual(context.user_data.get("wallet_flow"), "awaiting_main_address")
        self.assertEqual(context.user_data.get("wallet_linked_signer_pk"), "0xabc123")
        self.assertEqual(
            context.user_data.get("wallet_linked_signer_address"),
            "0x1111111111111111111111111111111111111111",
        )
        self.assertIn("Wallet Connect Guide", msg)
        self.assertIn("0xabc123", msg)

    def test_cmd_start_shows_wallet_setup_cta_when_wallet_missing(self):
        calls = []

        async def _reply_text(text, **kwargs):
            calls.append((text, kwargs))

        message = SimpleNamespace(reply_text=_reply_text)
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=1, username="u"),
            message=message,
        )
        context = SimpleNamespace(user_data={})

        with patch.object(commands, "_send_start_image", new=AsyncMock()), patch.object(
            commands, "get_or_create_user", return_value=(SimpleNamespace(), True, None)
        ), patch.object(
            commands, "is_new_onboarding_complete", return_value=True
        ), patch.object(
            commands, "ensure_active_wallet_ready", return_value=(False, "Wallet not linked")
        ):
            asyncio.run(commands.cmd_start(update, context))

        self.assertTrue(calls)
        self.assertIn("connect your wallet first", calls[0][0].lower())

    def test_trade_callback_redirects_to_wallet_setup_when_missing(self):
        query = SimpleNamespace(edit_message_text=AsyncMock())
        context = SimpleNamespace(user_data={})

        with patch.object(callbacks, "is_new_onboarding_complete", return_value=True), patch.object(
            callbacks, "ensure_active_wallet_ready", return_value=(False, "Wallet not linked")
        ), patch.object(callbacks, "prompt_wallet_setup", new=AsyncMock()) as prompt_mock:
            asyncio.run(callbacks._handle_trade(query, "trade:long", 1, context))

        prompt_mock.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
