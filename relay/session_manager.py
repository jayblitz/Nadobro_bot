import logging
import secrets
import time
from typing import Optional

from relay.db import get_pool
from relay.telegram_client import get_lowiqpts_entity, send_message

logger = logging.getLogger("relay.sessions")

_IDLE_TIMEOUT_SECONDS = 300


def _generate_session_id() -> str:
    return f"sess_{secrets.token_urlsafe(16)}"


async def create_session(
    telegram_user_id: int,
    chat_id: int,
    wallet: str,
    request_id: str,
) -> dict:
    pool = get_pool()
    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT id FROM relay_sessions WHERE request_id = $1 AND status = 'active'",
            request_id,
        )
        if existing:
            return {"ok": True, "session_id": existing}

        active_count = await conn.fetchval(
            "SELECT count(*) FROM relay_sessions WHERE status = 'active'"
        )
        if active_count and active_count > 0:
            await conn.execute(
                """
                UPDATE relay_sessions
                SET status = 'preempted', updated_at = now()
                WHERE status = 'active'
                """
            )
            logger.info("Preempted %d active session(s) for new request", active_count)

    session_id = _generate_session_id()
    entity = await get_lowiqpts_entity()
    await send_message(entity, f"/nado {wallet}")
    lowiqpts_chat_id = entity.id

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO relay_sessions
                (id, telegram_user_id, chat_id, wallet, request_id, lowiqpts_chat_id, status)
            VALUES ($1, $2, $3, $4, $5, $6, 'active')
            """,
            session_id, telegram_user_id, chat_id, wallet, request_id, lowiqpts_chat_id,
        )

    logger.info("Session %s created for wallet=%s req=%s", session_id, wallet[:10], request_id)
    return {"ok": True, "session_id": session_id}


async def reply_to_session(session_id: str, text: str) -> dict:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, status FROM relay_sessions WHERE id = $1",
            session_id,
        )
    if not row:
        return {"ok": False, "error": "session_not_found"}
    if row["status"] != "active":
        return {"ok": False, "error": "session_closed"}

    entity = await get_lowiqpts_entity()
    await send_message(entity, text)

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE relay_sessions SET updated_at = now() WHERE id = $1",
            session_id,
        )

    logger.info("Reply sent in session %s: %s", session_id, text[:40])
    return {"ok": True}


async def close_session(session_id: str, reason: Optional[str] = None) -> dict:
    pool = get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE relay_sessions SET status = 'closed', updated_at = now() WHERE id = $1 AND status = 'active'",
            session_id,
        )
    if result == "UPDATE 0":
        return {"ok": False, "error": "session_not_found_or_already_closed"}

    logger.info("Session %s closed (reason=%s)", session_id, reason or "none")
    return {"ok": True}


async def get_active_session_ids() -> list[str]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id FROM relay_sessions WHERE status = 'active'"
        )
    return [row["id"] for row in rows]


async def cleanup_idle_sessions() -> int:
    pool = get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE relay_sessions
            SET status = 'expired', updated_at = now()
            WHERE status = 'active'
              AND updated_at < now() - interval '5 minutes'
            """
        )
    count = int(result.split()[-1]) if result else 0
    if count > 0:
        logger.info("Cleaned up %d idle sessions", count)
    return count
