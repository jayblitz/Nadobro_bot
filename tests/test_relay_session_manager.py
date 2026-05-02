import unittest
import sys
import types
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

if "asyncpg" not in sys.modules:
    asyncpg_mod = types.ModuleType("asyncpg")

    class _Pool:
        pass

    class _UniqueViolationError(Exception):
        pass

    asyncpg_mod.Pool = _Pool
    asyncpg_mod.UniqueViolationError = _UniqueViolationError
    sys.modules["asyncpg"] = asyncpg_mod

if "telethon" not in sys.modules:
    telethon_mod = types.ModuleType("telethon")
    telethon_events = types.ModuleType("telethon.events")
    telethon_tl_types = types.ModuleType("telethon.tl.types")

    class _TelegramClient:
        pass

    class _User:
        pass

    class _NewMessage:
        class Event:
            pass

    telethon_mod.TelegramClient = _TelegramClient
    telethon_mod.events = telethon_events
    telethon_events.NewMessage = _NewMessage
    telethon_tl_types.User = _User
    sys.modules["telethon"] = telethon_mod
    sys.modules["telethon.events"] = telethon_events
    sys.modules["telethon.tl.types"] = telethon_tl_types

from relay import session_manager


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Conn:
    def __init__(self, fetch_values, execute_error=None):
        self.fetch_values = fetch_values
        self.execute_error = execute_error

    async def fetchval(self, *_args):
        return self.fetch_values.pop(0)

    async def execute(self, *_args):
        if self.execute_error:
            raise self.execute_error

    def transaction(self):
        return _Transaction()


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _Acquire(self.conn)


class RelaySessionManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_session_returns_existing_request_after_active_session_race(self):
        class _UniqueViolation(Exception):
            pass

        conn = _Conn([None, 0, None, "sess_existing"], execute_error=_UniqueViolation())
        pool = _Pool(conn)

        with patch.object(session_manager, "get_pool", return_value=pool), patch.object(
            session_manager.asyncpg, "UniqueViolationError", _UniqueViolation
        ), patch.object(
            session_manager, "get_lowiqpts_entity", new=AsyncMock(return_value=SimpleNamespace(id=123))
        ), patch.object(
            session_manager, "send_message", new=AsyncMock()
        ) as send_message:
            result = await session_manager.create_session(1, 2, "0xabc", "req-1")

        self.assertEqual(result, {"ok": True, "session_id": "sess_existing"})
        send_message.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
