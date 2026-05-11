import base64
import logging
import json
from typing import Optional

from relay.db import get_pool

logger = logging.getLogger("relay.events")


async def store_event(
    session_id: str,
    text: str,
    options: Optional[list[str]] = None,
    source_message_id: Optional[int] = None,
    photo_bytes: Optional[bytes] = None,
    photo_mime: Optional[str] = None,
) -> Optional[int]:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, status FROM relay_sessions WHERE id = $1",
            session_id,
        )
        if not row or row["status"] != "active":
            logger.debug("Dropping event for inactive/unknown session %s", session_id)
            return None

        options_json = None
        if options:
            try:
                options_json = json.dumps([str(x) for x in options if str(x).strip()])
            except (TypeError, ValueError):
                options_json = None

        cursor_id = await conn.fetchval(
            """
            INSERT INTO relay_events (session_id, text, options_json, source_message_id, photo_bytes, photo_mime)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING cursor_id
            """,
            session_id,
            text or "",
            options_json,
            int(source_message_id) if source_message_id else None,
            photo_bytes,
            photo_mime,
        )

        await conn.execute(
            "UPDATE relay_sessions SET updated_at = now() WHERE id = $1",
            session_id,
        )

    logger.debug("Stored event cursor_id=%s for session %s", cursor_id, session_id)
    return cursor_id


async def poll_events(session_id: str, cursor: Optional[str] = None, limit: int = 25) -> dict:
    pool = get_pool()
    after_cursor = 0
    if cursor:
        try:
            after_cursor = int(cursor)
        except (TypeError, ValueError):
            after_cursor = 0

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT e.cursor_id, e.session_id, e.text, e.options_json, e.source_message_id,
                   e.photo_bytes, e.photo_mime, e.created_at
            FROM relay_events e
            JOIN relay_sessions s ON s.id = e.session_id
            WHERE e.session_id = $1
              AND e.cursor_id > $2
            ORDER BY e.cursor_id ASC
            LIMIT $3
            """,
            str(session_id), after_cursor, limit,
        )

    events = []
    next_cursor = None
    for row in rows:
        options = None
        raw_options = row.get("options_json")
        if raw_options:
            try:
                parsed = json.loads(raw_options)
                if isinstance(parsed, list):
                    options = [str(x) for x in parsed if str(x).strip()]
            except (json.JSONDecodeError, TypeError, ValueError):
                options = None

        event_item = {
            "id": row["cursor_id"],
            "session_id": row["session_id"],
            "text": row["text"],
            "created_at": row["created_at"].isoformat(),
        }
        pb = row.get("photo_bytes")
        if pb:
            try:
                event_item["photo_base64"] = base64.b64encode(bytes(pb)).decode("ascii")
                pm = row.get("photo_mime") or "image/jpeg"
                event_item["photo_mime"] = str(pm)
            except Exception:
                logger.warning("relay event cursor=%s: failed to encode photo", row["cursor_id"], exc_info=True)
        if options:
            event_item["options"] = options
        if row.get("source_message_id"):
            event_item["source_message_id"] = int(row["source_message_id"])
        events.append(event_item)
        next_cursor = str(row["cursor_id"])

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


async def purge_old_relay_events(retention_days: int = 30) -> int:
    """Delete relay_events older than retention window (does not touch relay_sessions)."""
    from datetime import datetime, timedelta, timezone

    days = max(1, int(retention_days or 30))
    pool = get_pool()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM relay_events WHERE created_at < $1",
            cutoff,
        )
    try:
        return int(str(result).split()[-1])
    except (TypeError, ValueError, IndexError):
        return 0
