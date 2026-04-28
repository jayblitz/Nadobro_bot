from __future__ import annotations

import asyncio
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

fastapi_mod = sys.modules.get("fastapi") or types.ModuleType("fastapi")


class HTTPException(Exception):
    def __init__(self, status_code: int, detail: str = ""):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def _depends(_dependency=None):
    return None


fastapi_mod.HTTPException = getattr(fastapi_mod, "HTTPException", HTTPException)
fastapi_mod.Depends = getattr(fastapi_mod, "Depends", _depends)
fastapi_mod.Request = getattr(fastapi_mod, "Request", object)
sys.modules["fastapi"] = fastapi_mod
HTTPException = fastapi_mod.HTTPException

openai_mod = sys.modules.get("openai") or types.ModuleType("openai")


class _OpenAI:
    def __init__(self, *args, **kwargs):
        pass


openai_mod.OpenAI = getattr(openai_mod, "OpenAI", _OpenAI)
sys.modules["openai"] = openai_mod

from miniapp_api import dependencies
from src.nadobro.handlers import admin_invites, commands


class PrivateAccessFlowTests(unittest.TestCase):
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

    def test_miniapp_rejects_user_without_private_access(self):
        request = SimpleNamespace(headers={"Authorization": "tma init-data"})
        tg_user = SimpleNamespace(id=456, username="u")

        with patch.object(dependencies, "validate_init_data", return_value=tg_user), patch.object(
            dependencies, "get_or_create_user", return_value=(SimpleNamespace(_data={"telegram_id": 456}), False, None)
        ), patch.object(dependencies, "has_private_access", return_value=False):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(dependencies.get_current_user(request))

        self.assertEqual(raised.exception.status_code, 403)

    def test_invite_commands_are_registered(self):
        main_py = Path(__file__).resolve().parent.parent / "main.py"
        content = main_py.read_text()
        for command in ("invite_generate", "invite_status", "invite_revoke", "invite_grant"):
            self.assertIn(f'CommandHandler("{command}"', content)


if __name__ == "__main__":
    unittest.main()
