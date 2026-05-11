import asyncio
import sys
import time
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
                    "photo_bytes": None,
                    "photo_mime": None,
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


def test_points_relay_polls_all_pending_sessions(monkeypatch):
    from src.nadobro.services import points_service

    calls: list[tuple[str, object]] = []

    async def _poll_events(*, session_id, cursor):
        calls.append((session_id, cursor))
        return {"ok": True, "events": [], "next_cursor": None}

    monkeypatch.setattr(points_service, "relay_is_configured", lambda: True)
    monkeypatch.setattr(points_service, "relay_poll_events", _poll_events)
    bot_app = type("BotApp", (), {"bot_data": {
        "lowiqpts_pending_queue": [
            {"relay_session_id": "sess_a"},
            {"relay_session_id": "sess_b"},
        ],
    }})()

    asyncio.run(points_service.poll_lowiqpts_relay_events(bot_app))

    assert calls == [("sess_a", None), ("sess_b", None)]


def test_claim_pending_never_fallback_to_foreign_queue():
    """Dropped queue[0] fallback: unattributed relay text must not bind another user's pending row."""
    from src.nadobro.services import points_service

    w = "0x" + "ab" * 20
    row = {"req_id": "a", "chat_id": 1, "telegram_id": 1, "wallet": w, "ts": time.time(), "relay_session_id": ""}
    bd = {
        points_service._PENDING_QUEUE_KEY: [row],
        points_service._PENDING_BY_WALLET_KEY: {},
    }

    assert points_service._claim_pending_for_event(bd, "", "LOWIQPTS said something with no wallet and no session") is None


def test_claim_pending_matches_wallet_in_reply_text():
    from src.nadobro.services import points_service

    w = "0x" + "cd" * 20
    row = {"req_id": "b", "chat_id": 2, "telegram_id": 99, "wallet": w, "ts": time.time(), "relay_session_id": ""}
    wl = str(w).lower()
    bd = {
        points_service._PENDING_QUEUE_KEY: [row],
        points_service._PENDING_BY_WALLET_KEY: {wl: [row]},
    }

    claimed = points_service._claim_pending_for_event(bd, "", f"Points overview for {w}")
    assert claimed is row


def test_parse_lowiq_points_reply_handles_markdown_volume_lines():
    from src.nadobro.services import points_service

    blob = """📊 NADO REPORT
Period: 1 May - 8 May Epoch 14
**Core:**
**Volume:** 1 134.62
**Points:** 502.14
**Cost/Point:** -0.03
"""
    parsed = points_service.parse_lowiq_points_reply(blob)
    assert parsed is not None
    assert abs(parsed["volume_usd"] - 1134.62) < 0.01
    assert abs(parsed["points"] - 502.14) < 0.01
    assert abs(parsed["cost_per_point"] - (-0.03)) < 0.0001
    assert points_service._looks_like_nado_report_summary_text(blob)
