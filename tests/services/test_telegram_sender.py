"""Tests for the rate-shaped, priority-laned Telegram sender."""
from __future__ import annotations

import asyncio
import importlib
import time

import pytest


@pytest.fixture()
def fresh_sender(monkeypatch):
    """Reload the module so per-test env tweaks land cleanly."""
    monkeypatch.setenv("TELEGRAM_GLOBAL_RPS", "1000")
    monkeypatch.setenv("TELEGRAM_GLOBAL_BURST", "1000")
    monkeypatch.setenv("TELEGRAM_CHAT_RPS", "1000")
    monkeypatch.setenv("TELEGRAM_CHAT_BURST", "1000")
    from src.nadobro.notify import telegram_sender

    importlib.reload(telegram_sender)
    return telegram_sender


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


def test_send_is_rate_shaped(monkeypatch):
    """With a 10rps global budget and burst 5, a burst of 20 sends takes ≥1s."""
    monkeypatch.setenv("TELEGRAM_GLOBAL_RPS", "10")
    monkeypatch.setenv("TELEGRAM_GLOBAL_BURST", "5")
    monkeypatch.setenv("TELEGRAM_CHAT_RPS", "1000")
    monkeypatch.setenv("TELEGRAM_CHAT_BURST", "1000")
    from src.nadobro.notify import telegram_sender

    importlib.reload(telegram_sender)

    sent: list[int] = []

    async def fake_send(*, chat_id, text, **_):
        sent.append(chat_id)

    async def body() -> None:
        sender = telegram_sender.get_sender()
        sender.bind(fake_send)
        await sender.start()
        try:
            started = time.monotonic()
            await asyncio.gather(
                *[
                    sender.send_text(i, "hi", lane=telegram_sender.Lane.INFO, await_result=True)
                    for i in range(20)
                ]
            )
            elapsed = time.monotonic() - started
            assert len(sent) == 20
            assert elapsed >= 1.0, f"expected ≥1s under 10rps/burst5 budget, got {elapsed:.2f}s"
        finally:
            await sender.stop()

    _run(body())


def test_priority_lane_jumps_queue(monkeypatch):
    """A USER_REPLY enqueued after 50 INFOs should still go out earlier."""
    monkeypatch.setenv("TELEGRAM_GLOBAL_RPS", "5")
    monkeypatch.setenv("TELEGRAM_GLOBAL_BURST", "1")
    monkeypatch.setenv("TELEGRAM_CHAT_RPS", "1000")
    monkeypatch.setenv("TELEGRAM_CHAT_BURST", "1000")
    from src.nadobro.notify import telegram_sender

    importlib.reload(telegram_sender)

    sent: list[str] = []

    async def fake_send(*, chat_id, text, **_):
        sent.append(text)

    async def body() -> None:
        sender = telegram_sender.get_sender()
        sender.bind(fake_send)
        await sender.start()
        try:
            for i in range(50):
                await sender.send_text(1, f"info-{i}", lane=telegram_sender.Lane.INFO)
            await asyncio.sleep(0.05)  # let the worker consume the first item
            await sender.send_text(1, "URGENT", lane=telegram_sender.Lane.USER_REPLY)
            while "URGENT" not in sent and len(sent) < 15:
                await asyncio.sleep(0.1)
            urgent_pos = sent.index("URGENT") if "URGENT" in sent else len(sent)
            assert "URGENT" in sent, "USER_REPLY was never delivered"
            assert urgent_pos < 10, f"USER_REPLY arrived after position {urgent_pos}; lane priority broken"
        finally:
            await sender.stop()

    _run(body())


def test_dedupe_drops_repeats(fresh_sender):
    sent: list[str] = []

    async def fake_send(*, chat_id, text, **_):
        sent.append(text)

    async def body() -> None:
        sender = fresh_sender.get_sender()
        sender.bind(fake_send)
        await sender.start()
        try:
            await sender.send_text(1, "hi", dedupe_key="k", await_result=True)
            accepted = await sender.send_text(1, "hi-again", dedupe_key="k")
            assert accepted is False
            assert sent == ["hi"]
        finally:
            await sender.stop()

    _run(body())


def test_lane_cap_drops_oldest_info(monkeypatch):
    monkeypatch.setenv("TELEGRAM_GLOBAL_RPS", "0.1")
    monkeypatch.setenv("TELEGRAM_GLOBAL_BURST", "0")  # nothing leaves while we fill
    monkeypatch.setenv("TELEGRAM_CHAT_RPS", "1000")
    monkeypatch.setenv("TELEGRAM_CHAT_BURST", "1000")
    monkeypatch.setenv("TELEGRAM_LANE_CAPACITY_INFO", "3")
    from src.nadobro.notify import telegram_sender

    importlib.reload(telegram_sender)

    async def fake_send(*, chat_id, text, **_):
        pass

    async def body() -> None:
        sender = telegram_sender.get_sender()
        sender.bind(fake_send)
        await sender.start()
        try:
            for i in range(5):
                await sender.send_text(1, f"m{i}", lane=telegram_sender.Lane.INFO)
            stats = sender.stats()
            assert stats["lanes"]["info"] <= 3
            assert stats["dropped"] >= 2
        finally:
            await sender.stop()

    _run(body())


def test_retry_after_reschedules(fresh_sender):
    """A 429-like exception with retry_after should re-queue, not lose, the message."""
    attempts: list[int] = []

    class _RetryExc(Exception):
        retry_after = 0.05

    async def fake_send(*, chat_id, text, **_):
        attempts.append(time.monotonic())
        if len(attempts) == 1:
            raise _RetryExc("429")
        # second attempt succeeds

    async def body() -> None:
        sender = fresh_sender.get_sender()
        sender.bind(fake_send)
        await sender.start()
        try:
            ok = await sender.send_text(1, "hi", await_result=True)
            assert ok is True
            assert len(attempts) == 2
            assert attempts[1] - attempts[0] >= 0.04
        finally:
            await sender.stop()

    _run(body())
