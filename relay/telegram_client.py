import asyncio
import logging
import os
from typing import Callable, Coroutine, Optional

from telethon import TelegramClient, events
from telethon.tl.types import User

logger = logging.getLogger("relay.telegram")

_client: Optional[TelegramClient] = None
_lowiqpts_entity: Optional[User] = None
_on_message_callback: Optional[Callable[..., Coroutine]] = None


def _api_id() -> int:
    raw = os.environ.get("TELEGRAM_API_ID", "")
    if not raw:
        raise RuntimeError("TELEGRAM_API_ID is not set")
    return int(raw)


def _api_hash() -> str:
    val = os.environ.get("TELEGRAM_API_HASH", "")
    if not val:
        raise RuntimeError("TELEGRAM_API_HASH is not set")
    return val


def _session_path() -> str:
    return os.environ.get("SESSION_PATH", "/data/relay.session")


def _lowiqpts_username() -> str:
    return os.environ.get("LOWIQPTS_USERNAME", "lowiqpts")


def set_message_callback(cb: Callable[..., Coroutine]) -> None:
    global _on_message_callback
    _on_message_callback = cb


async def start_client() -> TelegramClient:
    global _client, _lowiqpts_entity
    session = _session_path()
    _client = TelegramClient(session, _api_id(), _api_hash())
    await _client.start()
    me = await _client.get_me()
    logger.info("Telegram user logged in as %s (id=%s)", me.username or me.first_name, me.id)

    username = _lowiqpts_username()
    try:
        _lowiqpts_entity = await _client.get_entity(username)
        logger.info("Resolved @%s -> id=%s", username, _lowiqpts_entity.id)
    except Exception as e:
        logger.warning("Could not resolve @%s at startup: %s (will retry per-session)", username, e)

    _client.add_event_handler(_handle_incoming, events.NewMessage(incoming=True))
    logger.info("Telethon event handler registered")
    return _client


async def stop_client() -> None:
    global _client
    if _client:
        await _client.disconnect()
        _client = None
        logger.info("Telegram client disconnected")


async def get_lowiqpts_entity():
    global _lowiqpts_entity
    if _lowiqpts_entity is not None:
        return _lowiqpts_entity
    if _client is None:
        raise RuntimeError("Telegram client not started")
    username = _lowiqpts_username()
    _lowiqpts_entity = await _client.get_entity(username)
    return _lowiqpts_entity


async def send_message(entity, text: str):
    if _client is None:
        raise RuntimeError("Telegram client not started")
    return await _client.send_message(entity, text)


async def _handle_incoming(event: events.NewMessage.Event) -> None:
    if _on_message_callback is None:
        return
    sender = await event.get_sender()
    if sender is None:
        return
    lowiq = _lowiqpts_entity
    if lowiq is None:
        return
    if sender.id != lowiq.id:
        return
    text = event.message.message or ""
    options: list[str] = []
    try:
        rows = event.message.buttons or []
        for row in rows:
            for btn in row:
                label = str(getattr(btn, "text", "") or "").strip()
                if label:
                    options.append(label)
    except Exception:
        options = []

    chat_id = event.chat_id
    logger.debug("Incoming from @%s (chat=%s): %s", _lowiqpts_username(), chat_id, text[:80])
    try:
        await _on_message_callback(chat_id=chat_id, sender_id=sender.id, text=text, options=options)
    except Exception:
        logger.warning("Error in message callback", exc_info=True)
