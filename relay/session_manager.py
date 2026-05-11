import logging
import os
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

    session_id = _generate_session_id()
    entity = await get_lowiqpts_entity()
    lowiqpts_chat_id = entity.id
    wait_sec = float(os.environ.get("RELAY_LOWIQ_CHANNEL_WAIT_SECONDS", "180") or "180")

    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT id FROM relay_sessions WHERE request_id = $1 AND status = 'active'",
            request_id,
        )
        if existing:
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

    async def _send_nado():
        await send_message(entity, f"/nado {wallet}")

    try:
        ok = await lowiq_turn.run_exclusive_lowiq_channel(
            pool,
            timeout_seconds=wait_sec,
            coro=_send_nado,
        )
        if not ok:
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE relay_sessions SET status = 'failed', updated_at = now() WHERE id = $1",
                    session_id,
                )
            return {
                "ok": False,
                "error": "channel_busy",
                "detail": "LOWIQPTS relay channel is busy with another refresh. Try again in a minute.",
            }
    except Exception as e:
        logger.error(
            "Telegram send failed for session %s, cleaning up: %s",
            session_id,
            e,
            exc_info=True,
        )
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE relay_sessions SET status = 'failed', updated_at = now() WHERE id = $1",
                session_id,
            )
        raise

    logger.info("Session %s created for wallet=%s req=%s", session_id, wallet[:10], request_id)
    return {"ok": True, "session_id": session_id}


async def reply_to_session(session_id: str, text: str) -> dict:
    pool = get_pool()
    wait_sec = float(os.environ.get("RELAY_LOWIQ_CHANNEL_WAIT_SECONDS", "180") or "180")
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

    async def _send_reply():
        await send_message(entity, text)

    ok = await lowiq_turn.run_exclusive_lowiq_channel(
        pool,
        timeout_seconds=wait_sec,
        coro=_send_reply,
    )
    if not ok:
        return {
            "ok": False,
            "error": "channel_busy",
            "detail": "LOWIQPTS relay channel is busy; retry shortly.",
        }

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


async def reply_option_to_session(session_id: str, option_text: str, source_message_id: int) -> dict:
    pool = get_pool()
    wait_sec = float(os.environ.get("RELAY_LOWIQ_CHANNEL_WAIT_SECONDS", "180") or "180")
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

    async def _click_option():
        await click_message_button(entity, int(source_message_id), str(option_text))

    ok = await lowiq_turn.run_exclusive_lowiq_channel(
        pool,
        timeout_seconds=wait_sec,
        coro=_click_option,
    )
    if not ok:
        return {
            "ok": False,
            "error": "channel_busy",
            "detail": "LOWIQPTS relay channel is busy; retry shortly.",
        }

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
    count = len(rows or [])
    if count > 0:
        logger.info("Cleaned up %d idle sessions", count)
    return count
