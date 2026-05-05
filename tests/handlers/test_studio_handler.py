import asyncio
from types import SimpleNamespace

from src.nadobro.handlers.studio_handler import handle_studio_callback, handle_studio_text
from src.nadobro.studio.intent import Quantity, TradingIntent


class Msg:
    text = "long btc 1 at 2x"

    def __init__(self):
        self.replies = []

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


class Query:
    def __init__(self, data: str, user_id: int):
        self.data = data
        self.from_user = SimpleNamespace(id=user_id)
        self.edits = []
        self.answered = False

    async def answer(self):
        self.answered = True

    async def edit_message_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


def test_studio_text_disabled(monkeypatch):
    monkeypatch.setattr("src.nadobro.handlers.studio_handler.studio_enabled", lambda: False)
    update = SimpleNamespace(message=Msg(), effective_user=SimpleNamespace(id=1))
    assert asyncio.run(handle_studio_text(update, SimpleNamespace())) is False


def test_studio_text_confirmation(monkeypatch):
    monkeypatch.setattr("src.nadobro.handlers.studio_handler.studio_enabled", lambda: True)
    monkeypatch.setattr("src.nadobro.handlers.studio_handler._network", lambda uid: "mainnet")
    monkeypatch.setattr("src.nadobro.handlers.studio_handler.conversation.active_session", lambda *a: None)
    monkeypatch.setattr("src.nadobro.handlers.studio_handler.conversation.start_session", lambda *a: {"id": 1, "history_json": []})
    monkeypatch.setattr("src.nadobro.handlers.studio_handler.conversation.save_turn", lambda *a, **k: None)

    async def fake_extract(*args, **kwargs):
        return TradingIntent(
            action="buy",
            symbol="BTC",
            order_type="market",
            quantity=Quantity(type="contracts", value=1),
            leverage=2,
            take_profit={"type": "percent", "value": 2},
            stop_loss={"type": "percent", "value": -1},
            raw_input="long",
        )

    monkeypatch.setattr("src.nadobro.handlers.studio_handler.extract", fake_extract)
    msg = Msg()
    update = SimpleNamespace(message=msg, effective_user=SimpleNamespace(id=1))
    context = SimpleNamespace(user_data={"studio_live_mode": True})
    assert asyncio.run(handle_studio_text(update, context)) is True
    assert "Strategy Summary" in msg.replies[0][0]


def test_studio_text_requires_live_mode_without_active_session(monkeypatch):
    monkeypatch.setattr("src.nadobro.handlers.studio_handler.studio_enabled", lambda: True)
    monkeypatch.setattr("src.nadobro.handlers.studio_handler._network", lambda uid: "mainnet")
    monkeypatch.setattr("src.nadobro.handlers.studio_handler.conversation.active_session", lambda *a: None)

    extracted = {"called": False}

    async def fake_extract(*args, **kwargs):
        extracted["called"] = True
        return None

    monkeypatch.setattr("src.nadobro.handlers.studio_handler.extract", fake_extract)
    msg = Msg()
    update = SimpleNamespace(message=msg, effective_user=SimpleNamespace(id=1))
    context = SimpleNamespace(user_data={})
    assert asyncio.run(handle_studio_text(update, context)) is False
    assert extracted["called"] is False
    assert msg.replies == []


def test_studio_callback_rejects_session_owned_by_another_user(monkeypatch):
    intent = TradingIntent(
        action="buy",
        symbol="BTC",
        order_type="market",
        quantity=Quantity(type="contracts", value=1),
        leverage=2,
        raw_input="long",
    )
    monkeypatch.setattr(
        "src.nadobro.handlers.studio_handler.conversation.get_studio_session_row",
        lambda session_id: {"id": session_id, "telegram_id": 111, "intent_json": {}, "history_json": []},
    )
    monkeypatch.setattr("src.nadobro.handlers.studio_handler.conversation.load_intent", lambda row: intent)
    monkeypatch.setattr("src.nadobro.handlers.studio_handler.conversation.load_history", lambda row: [])
    executed = {}
    monkeypatch.setattr(
        "src.nadobro.handlers.studio_handler.execute_intent",
        lambda *args, **kwargs: executed.setdefault("called", True),
    )

    query = Query("studio:confirm:7", user_id=222)

    assert asyncio.run(handle_studio_callback(query, SimpleNamespace())) is True
    assert query.answered is True
    assert query.edits[-1][0] == "Studio session not found."
    assert executed == {}
