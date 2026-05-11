"""Serialize access to the shared @lowiqpts DM — one multi-turn flow at a time."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

logger = logging.getLogger("relay.lowiq_turn")

_SEM = asyncio.Semaphore(1)
_holder_sid: Optional[str] = None
_holder_guard = asyncio.Lock()

_WAIT_SECONDS = float(os.environ.get("RELAY_LOWIQ_CHANNEL_WAIT_SECONDS", "180") or "180")


async def acquire_turn() -> bool:
    """Block up to RELAY_LOWIQ_CHANNEL_WAIT_SECONDS for exclusive LOWIQPTS channel access."""
    try:
        await asyncio.wait_for(_SEM.acquire(), timeout=max(1.0, _WAIT_SECONDS))
        return True
    except asyncio.TimeoutError:
        logger.info("LOWIQPTS channel busy (wait %.1fs timed out)", _WAIT_SECONDS)
        return False


async def bind_turn(session_id: str) -> None:
    global _holder_sid
    async with _holder_guard:
        _holder_sid = session_id


async def release_unbound_turn() -> None:
    """Release semaphore when turn was acquired but no session owns it yet (early-return paths)."""
    _SEM.release()


async def release_turn(session_id: str) -> None:
    """Release LOWIQPTS channel for sessions that finished normally."""
    global _holder_sid
    release_sem = False
    async with _holder_guard:
        if _holder_sid == session_id:
            _holder_sid = None
            release_sem = True
    if release_sem:
        try:
            _SEM.release()
        except ValueError:
            logger.warning("LOWIQPTS channel semaphore over-release (ignored)", exc_info=True)
