import asyncio
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.handlers import callbacks, commands


class WalletOnboardingFlowTests(unittest.TestCase):
    def test_wallet_view_seeds_wallet_flow_when_not_linked(self):
        query = SimpleNamespace(edit_message_text=AsyncMock())
        context = SimpleNamespace(user_data={})
        fake_key = SimpleNamespace(hex=lambda: "0xabc123")
        fake_account = SimpleNamespace(key=fake_key, address="0x1111111111111111111111111111111111111111")

        with patch.object(callbacks, "get_user_wallet_info", return_value={"linked_signer_address": None}), patch(
            "eth_account.Account.create", return_value=fake_account
        ):
            asyncio.run(callbacks._handle_wallet(query, "wallet:view", 1, context))

        self.assertEqual(context.user_data.get("wallet_flow"), "awaiting_main_address")
        self.assertEqual(context.user_data.get("wallet_linked_signer_pk"), "0xabc123")
        self.assertEqual(
            context.user_data.get("wallet_linked_signer_address"),
            "0x1111111111111111111111111111111111111111",
        )
        query.edit_message_text.assert_awaited()

    def test_cmd_start_shows_tos_screen_when_language_already_set(self):
        calls = []

        async def _reply_text(text, **kwargs):
            calls.append((text, kwargs))

        message = SimpleNamespace(reply_text=_reply_text)
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=1, username="u"),
            message=message,
        )
        context = SimpleNamespace(user_data={})

        with patch.object(commands, "get_or_create_user", return_value=(SimpleNamespace(), True, None)), patch.object(
            commands, "has_private_access", return_value=True
        ), patch.object(
            commands, "is_new_onboarding_complete", return_value=False
        ), patch.object(
            commands, "get_new_onboarding_state", return_value={"language": "en"}
        ), patch.object(
            commands, "get_user_language", return_value="en"
        ):
            asyncio.run(commands.cmd_start(update, context))

        self.assertTrue(calls)
        self.assertIn("let's get it", calls[0][0].lower())

    @unittest.skipUnless(
        os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DATABASE_URL"),
        "PostgreSQL required (set DATABASE_URL or SUPABASE_DATABASE_URL)",
    )
    def test_trade_callback_shows_wallet_error_when_wallet_missing(self):
        tid = 9_888_776_655
        from src.nadobro.db import execute
        from src.nadobro.services.onboarding_service import set_new_onboarding_tos_accepted
        from src.nadobro.services.user_service import get_or_create_user

        query = SimpleNamespace(edit_message_text=AsyncMock())
        context = SimpleNamespace(user_data={})
        try:
            get_or_create_user(tid, username="pytest_wallet_flow")
            set_new_onboarding_tos_accepted(tid)
            asyncio.run(callbacks._handle_trade(query, "trade:long", tid, context))
        finally:
            execute("DELETE FROM bot_state WHERE key = %s", (f"onboarding_v2:{tid}",))
            execute("DELETE FROM users WHERE telegram_id = %s", (tid,))

        query.edit_message_text.assert_awaited()
        text = (query.edit_message_text.call_args[0][0] if query.edit_message_text.call_args else "") or ""
        self.assertIn("Wallet", text)

    def test_nav_main_clears_pending_multi_step_state(self):
        query = SimpleNamespace(edit_message_text=AsyncMock())
        context = SimpleNamespace(
            user_data={
                "wallet_flow": "awaiting_main_address",
                "pending_alert": {"product": "BTC"},
                "pending_trade": {"step": "custom_size"},
                "pending_strategy_input": {"strategy": "grid", "field": "spread_bp"},
                "pending_bro_input": {"field": "tp_sl"},
                "trade_flow": {"state": "size"},
                "pending_copy_wallet": True,
                "pending_admin_copy_wallet": True,
            }
        )

        with patch.object(callbacks, "_show_dashboard", AsyncMock()):
            asyncio.run(callbacks._handle_nav(query, "nav:main", 1, context))

        self.assertEqual(context.user_data, {})


if __name__ == "__main__":
    unittest.main()
