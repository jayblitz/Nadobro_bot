import logging
import secrets
from typing import Optional

import asyncpg

from relay.db import get_pool
from relay import lowiq_turn
from relay.telegram_client import click_message_button, get_lowiqpts_entity, send_message

logger = logging.getLogger("relay.sessions")


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

        busy_user = await conn.fetchval(
            """
            SELECT id FROM relay_sessions
            WHERE telegram_user_id = $1 AND status = 'active'
            LIMIT 1
            """,
            telegram_user_id,
        )
        if busy_user:
            await conn.execute(
                """
                UPDATE relay_sessions
                SET request_id = $2, chat_id = $3, wallet = $4, updated_at = now()
                WHERE id = $1 AND status = 'active'
                """,
                busy_user,
                request_id,
                chat_id,
                wallet,
            )
            return {"ok": True, "session_id": busy_user}

    if not await lowiq_turn.acquire_turn():
        return {
            "ok": False,
            "error": "channel_busy",
            "detail": "LOWIQPTS relay channel is busy with another refresh. Try again in a minute.",
        }

    session_id = _generate_session_id()
    entity = await get_lowiqpts_entity()
    lowiqpts_chat_id = entity.id

    try:
        async with pool.acquire() as conn:
            existing = await conn.fetchval(
                "SELECT id FROM relay_sessions WHERE request_id = $1 AND status = 'active'",
                request_id,
            )
            if existing:
                await lowiq_turn.release_unbound_turn()
                return {"ok": True, "session_id": existing}
            try:
                await conn.execute(
                    """
                    INSERT INTO relay_sessions
                        (id, telegram_user_id, chat_id, wallet, request_id, lowiqpts_chat_id, status)
                    VALUES ($1, $2, $3, $4, $5, $6, 'active')
                    """,
                    session_id,
                    telegram_user_id,
                    chat_id,
                    wallet,
                    request_id,
                    lowiqpts_chat_id,
                )
            except asyncpg.UniqueViolationError:
                existing_uid = await conn.fetchval(
                    """
                    SELECT id FROM relay_sessions
                    WHERE telegram_user_id = $1 AND status = 'active'
                    LIMIT 1
                    """,
                    telegram_user_id,
                )
                await lowiq_turn.release_unbound_turn()
                if existing_uid:
                    await conn.execute(
                        """
                        UPDATE relay_sessions
                        SET request_id = $2, chat_id = $3, wallet = $4, updated_at = now()
                        WHERE id = $1 AND status = 'active'
                        """,
                        existing_uid,
                        request_id,
                        chat_id,
                        wallet,
                    )
                    return {"ok": True, "session_id": existing_uid}
                return {
                    "ok": False,
                    "error": "session_race",
                    "detail": "Could not claim LOWIQPTS session; retry shortly.",
                }
    except Exception:
        await lowiq_turn.release_unbound_turn()
        raise

    await lowiq_turn.bind_turn(session_id)

    try:
        await send_message(entity, f"/nado {wallet}")
    except Exception as e:
        logger.error("Telegram send failed for session %s, cleaning up: %s", session_id, e)
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE relay_sessions SET status = 'failed', updated_at = now() WHERE id = $1",
                session_id,
            )
        await lowiq_turn.release_turn(session_id)
        raise

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

    await lowiq_turn.release_turn(session_id)
    logger.info("Session %s closed (reason=%s)", session_id, reason or "none")
    return {"ok": True}


async def reply_option_to_session(session_id: str, option_text: str, source_message_id: int) -> dict:
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
    await click_message_button(entity, int(source_message_id), str(option_text))

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE relay_sessions SET updated_at = now() WHERE id = $1",
            session_id,
        )

    logger.info("Option clicked in session %s: %s", session_id, option_text[:40])
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
        rows = await conn.fetch(
            """
            UPDATE relay_sessions
            SET status = 'expired', updated_at = now()
            WHERE status = 'active'
              AND updated_at < now() - interval '5 minutes'
            RETURNING id
            """
        )
    for row in rows or []:
        sid = str(row.get("id") or "").strip()
        if sid:
            await lowiq_turn.release_turn(sid)
    count = len(rows or [])
    if count > 0:
        logger.info("Cleaned up %d idle sessions", count)
    return count
