import logging
from typing import Optional

from relay.db import get_pool

logger = logging.getLogger("relay.events")


async def store_event(session_id: str, text: str) -> Optional[int]:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, status FROM relay_sessions WHERE id = $1",
            session_id,
        )
        if not row or row["status"] != "active":
            logger.debug("Dropping event for inactive/unknown session %s", session_id)
            return None

        event_id = await conn.fetchval(
            """
            INSERT INTO relay_events (session_id, text)
            VALUES ($1, $2)
            RETURNING id
            """,
            session_id, text,
        )

        await conn.execute(
            "UPDATE relay_sessions SET updated_at = now() WHERE id = $1",
            session_id,
        )

    logger.debug("Stored event %s for session %s", event_id, session_id)
    return event_id


async def poll_events(cursor: Optional[str] = None, limit: int = 25) -> dict:
    pool = get_pool()
    cursor_id = 0
    if cursor:
        try:
            cursor_id = int(cursor)
        except (TypeError, ValueError):
            cursor_id = 0

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT e.id, e.session_id, e.text, e.created_at
            FROM relay_events e
            JOIN relay_sessions s ON s.id = e.session_id
            WHERE e.id > $1
            ORDER BY e.id ASC
            LIMIT $2
            """,
            cursor_id, limit,
        )

    events = []
    next_cursor = None
    for row in rows:
        events.append({
            "id": row["id"],
            "session_id": row["session_id"],
            "text": row["text"],
            "created_at": row["created_at"].isoformat(),
        })
        next_cursor = str(row["id"])

    return {
        "ok": True,
        "events": events,
        "next_cursor": next_cursor,
    }


async def find_session_for_incoming(sender_id: int) -> Optional[str]:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id FROM relay_sessions
            WHERE lowiqpts_chat_id = $1
              AND status = 'active'
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            sender_id,
        )
    return row["id"] if row else None
