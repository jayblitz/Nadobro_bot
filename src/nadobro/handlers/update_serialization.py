"""Serialize Telegram updates per ``telegram_id`` to reduce ``user_data`` races."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from telegram import Update
from telegram.ext import CallbackContext, ContextTypes

_registry_guard = asyncio.Lock()
_user_locks: dict[int, asyncio.Lock] = {}


async def _lock_for_user(telegram_id: int) -> asyncio.Lock:
    async with _registry_guard:
        lock = _user_locks.get(telegram_id)
        if lock is None:
            lock = asyncio.Lock()
            _user_locks[telegram_id] = lock
        return lock


def with_user_serialized(
    handler: Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[Any]],
) -> Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[Any]]:
    """Wrap an async handler so concurrent updates for the same user run sequentially."""

    async def _wrapped(update: Update, context: CallbackContext):
        user = update.effective_user
        if user is None:
            return await handler(update, context)
        ul = await _lock_for_user(int(user.id))
        async with ul:
            return await handler(update, context)

    return _wrapped

