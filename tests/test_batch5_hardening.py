import asyncio
import sys
import types
from datetime import datetime, timezone

from _stubs import install_test_stubs

install_test_stubs()

if "asyncpg" not in sys.modules:
    asyncpg_mod = types.ModuleType("asyncpg")

    class _AsyncpgPool:
        pass

    class _UniqueViolationError(Exception):
        pass

    asyncpg_mod.Pool = _AsyncpgPool
    asyncpg_mod.UniqueViolationError = _UniqueViolationError
    sys.modules["asyncpg"] = asyncpg_mod


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _Acquire(self.conn)


def test_relay_poll_events_is_scoped_to_session(monkeypatch):
    from relay import event_store

    captured = {}

    class _Conn:
        async def fetch(self, sql, *params):
            captured["sql"] = sql
            captured["params"] = params
            return [
                {
                    "cursor_id": 11,
                    "session_id": "sess_a",
                    "text": "hello",
                    "options_json": None,
                    "source_message_id": None,
                    "created_at": datetime.now(timezone.utc),
                }
            ]

    monkeypatch.setattr(event_store, "get_pool", lambda: _Pool(_Conn()))

    result = asyncio.run(event_store.poll_events(session_id="sess_a", cursor="10", limit=5))

    assert result["events"][0]["session_id"] == "sess_a"
    assert "e.session_id = $1" in captured["sql"]
    assert captured["params"] == ("sess_a", 10, 5)


def test_points_relay_poll_uses_session_scoped_cursor(monkeypatch):
    from src.nadobro.services import points_service

    captured = {}

    async def _poll_events(*, session_id, cursor):
        captured["session_id"] = session_id
        captured["cursor"] = cursor
        return {"ok": True, "events": [], "next_cursor": "12"}

    monkeypatch.setattr(points_service, "relay_is_configured", lambda: True)
    monkeypatch.setattr(points_service, "relay_poll_events", _poll_events)
    bot_app = type("BotApp", (), {"bot_data": {
        "lowiqpts_pending_queue": [{"relay_session_id": "sess_a"}],
        "lowiqpts_relay_cursor:sess_a": "11",
    }})()

    asyncio.run(points_service.poll_lowiqpts_relay_events(bot_app))

    assert captured == {"session_id": "sess_a", "cursor": "11"}
    assert bot_app.bot_data["lowiqpts_relay_cursor:sess_a"] == "12"
