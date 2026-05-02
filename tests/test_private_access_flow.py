from __future__ import annotations

import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.handlers import admin_invites, commands


class PrivateAccessFlowTests(unittest.TestCase):
    def test_cmd_start_existing_user_sends_non_empty_dashboard(self):
        calls = []

        async def _reply_text(text, **kwargs):
            calls.append((text, kwargs))

        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=1, username="u"),
            message=SimpleNamespace(reply_text=_reply_text),
        )
        context = SimpleNamespace(user_data={}, args=[])

        with patch.object(commands, "get_or_create_user", return_value=(SimpleNamespace(), False, None)), patch.object(
            commands, "has_private_access", return_value=True
        ), patch.object(
            commands, "is_new_onboarding_complete", return_value=True
        ), patch.object(
            commands, "get_user_language", return_value="en"
        ):
            asyncio.run(commands.cmd_start(update, context))

        self.assertTrue(calls)
        self.assertIsInstance(calls[0][0], str)
        self.assertGreater(len(calls[0][0].strip()), 0)

    def test_cmd_start_without_access_shows_private_card(self):
        calls = []

        async def _reply_text(text, **kwargs):
            calls.append((text, kwargs))

        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=1, username="u"),
            message=SimpleNamespace(reply_text=_reply_text),
        )
        context = SimpleNamespace(user_data={}, args=[])

        with patch.object(commands, "get_or_create_user", return_value=(SimpleNamespace(), True, None)), patch.object(
            commands, "has_private_access", return_value=False
        ):
            asyncio.run(commands.cmd_start(update, context))

        self.assertIn("Private Alpha Access", calls[0][0])
        self.assertIsNotNone(calls[0][1].get("reply_markup"))

    def test_admin_invite_generate_command(self):
        calls = []

        async def _reply_text(text, **kwargs):
            calls.append(text)

        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123),
            message=SimpleNamespace(reply_text=_reply_text),
        )
        context = SimpleNamespace(args=["2", "launch-alpha"])

        with patch.object(admin_invites, "is_admin", return_value=True), patch.object(
            admin_invites,
            "generate_invite_codes",
            return_value=[{"code": "ABCDEFGH"}, {"code": "JKLMNPQR"}],
        ):
            asyncio.run(admin_invites.cmd_invite_generate(update, context))

        self.assertIn("ABCDEFGH", calls[0])
        self.assertIn("launch-alpha", calls[0])

    def test_invite_commands_are_registered(self):
        main_py = Path(__file__).resolve().parent.parent / "main.py"
        content = main_py.read_text()
        for command in ("invite_generate", "invite_status", "invite_revoke", "invite_grant"):
            self.assertIn(f'CommandHandler("{command}"', content)


if __name__ == "__main__":
    unittest.main()
