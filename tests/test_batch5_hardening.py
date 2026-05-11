import asyncio
import sys
import time
import types
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

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


def test_claim_pending_prefers_newest_wallet_row_when_stacked():
    """Multiple pending rows for one wallet: wallet fallback must bind to latest refresh, not queue[0]."""
    from src.nadobro.services import points_service

    w = "0x" + "ee" * 20
    wl = str(w).lower()
    now = time.time()
    old = {"req_id": "old", "chat_id": 1, "telegram_id": 1, "wallet": w, "ts": now - 30.0, "relay_session_id": ""}
    new = {"req_id": "new", "chat_id": 2, "telegram_id": 2, "wallet": w, "ts": now, "relay_session_id": ""}
    bd = {
        points_service._PENDING_QUEUE_KEY: [old, new],
        points_service._PENDING_BY_WALLET_KEY: {wl: [old, new]},
    }

    claimed = points_service._claim_pending_for_event(bd, "", f"Status for {w}")
    assert claimed is new


def test_lowiqpts_option_prompt_does_not_complete_pending_when_parse_matches(monkeypatch):
    """Regression: metrics embedded alongside Yes/No options must not drop pending before the user taps."""
    from src.nadobro.services import points_service

    completed: list[dict] = []
    monkeypatch.setattr(points_service, "_schedule_timeout", lambda *a, **k: None)
    monkeypatch.setattr(
        points_service,
        "complete_pending_request",
        lambda bd, req: completed.append(req),
    )

    wallet = "0x" + "de" * 20
    req_row = {
        "req_id": "req1",
        "chat_id": 42,
        "telegram_id": 7,
        "wallet": wallet.lower(),
        "ts": time.time(),
        "relay_session_id": "sess_z",
    }
    bot_data = {
        points_service._PENDING_QUEUE_KEY: [req_row],
        points_service._ACTIVE_BY_CHAT_KEY: {42: "req1"},
    }
    send_mock = AsyncMock()
    bot_app = SimpleNamespace(bot=SimpleNamespace(send_message=send_mock))
    event = {
        "session_id": "sess_z",
        "text": "Include extras?\n\nPoints: 100\nVolume: $50",
        "options": ["Yes", "No"],
    }

    asyncio.run(points_service._process_relay_event(bot_app, bot_data, event))

    assert completed == []
    assert send_mock.await_count == 1


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
