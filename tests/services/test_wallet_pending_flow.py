import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.handlers import messages
from src.nadobro.services.wallet_pending_flow import (
    clear_wallet_pending_flow,
    load_wallet_pending_flow,
    persist_wallet_pending_flow,
)


class WalletPendingFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # The round-trip test does REAL Fernet encryption of the pending pk.
        # Tests must not depend on a developer's .env key — generate one.
        if not os.environ.get("ENCRYPTION_KEY") and not os.environ.get("ENCRYPTION_KEYS"):
            from cryptography.fernet import Fernet

            os.environ["ENCRYPTION_KEY"] = Fernet.generate_key().decode()
            from src.nadobro.services import crypto

            crypto._fernet_instance = None  # drop any cached (key-less) instance

    @unittest.skipUnless(
        os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DATABASE_URL"),
        "PostgreSQL required (set DATABASE_URL or SUPABASE_DATABASE_URL)",
    )
    def test_persist_and_load_round_trip(self):
        uid = 9_888_776_601
        try:
            persist_wallet_pending_flow(
                uid,
                flow="awaiting_main_address",
                pk_hex="0xabc123",
                linked_signer_address="0x1111111111111111111111111111111111111111",
            )
            loaded = load_wallet_pending_flow(uid)
            self.assertIsNotNone(loaded)
            self.assertEqual(loaded["flow"], "awaiting_main_address")
            self.assertEqual(loaded["pk_hex"], "0xabc123")
            self.assertEqual(
                loaded["linked_signer_address"],
                "0x1111111111111111111111111111111111111111",
            )
        finally:
            clear_wallet_pending_flow(uid)

    def test_address_paste_hydrates_from_bot_state(self):
        telegram_id = 42
        main_addr = "0x2222222222222222222222222222222222222222"
        update = SimpleNamespace(
            message=SimpleNamespace(reply_text=AsyncMock(), delete=AsyncMock()),
            effective_user=SimpleNamespace(id=telegram_id),
        )
        context = SimpleNamespace(
            user_data={},
            bot=SimpleNamespace(delete_message=AsyncMock()),
        )
        pending = {
            "flow": "awaiting_main_address",
            "pk_hex": "0xabc123",
            "linked_signer_address": "0x1111111111111111111111111111111111111111",
        }

        async def _run():
            with patch.object(messages, "hydrate_wallet_flow_context", side_effect=lambda ctx, uid: (
                ctx.user_data.update(
                    {
                        "wallet_flow": pending["flow"],
                        "wallet_linked_signer_pk": pending["pk_hex"],
                        "wallet_linked_signer_address": pending["linked_signer_address"],
                    }
                )
                or True
            )), patch.object(messages, "encrypt_with_server_key", return_value=b"cipher"), patch.object(
                messages, "save_linked_signer"
            ), patch.object(messages, "run_blocking", side_effect=lambda fn, *a, **k: fn(*a, **k)), patch.object(
                messages, "clear_wallet_pending_flow"
            ), patch.object(messages, "_delete_user_message", AsyncMock()), patch.object(
                messages, "_reply_loc", AsyncMock()
            ) as reply_loc:
                handled = await messages._handle_wallet_flow(update, context, telegram_id, main_addr)
                self.assertTrue(handled)
                reply_loc.assert_awaited()
                reply_text = reply_loc.call_args[0][1]
                self.assertIn("1CT linked", reply_text)

        import asyncio

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
