import logging
import os
from typing import Optional

import asyncpg

logger = logging.getLogger("relay.db")

_pool: Optional[asyncpg.Pool] = None


def _dsn() -> str:
    return os.environ.get("DATABASE_URL", "")


async def init_db() -> None:
    global _pool
    dsn = _dsn()
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")
    _pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
    await _ensure_tables()
    logger.info("Database pool initialised")


async def close_db() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed")


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database not initialised – call init_db() first")
    return _pool


async def _ensure_tables() -> None:
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS relay_sessions (
                id              TEXT PRIMARY KEY,
                telegram_user_id BIGINT NOT NULL,
                chat_id         BIGINT NOT NULL,
                wallet          TEXT NOT NULL,
                request_id      TEXT NOT NULL,
                lowiqpts_chat_id BIGINT,
                status          TEXT NOT NULL DEFAULT 'active',
                created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS relay_events (
                id          BIGSERIAL PRIMARY KEY,
                session_id  TEXT NOT NULL REFERENCES relay_sessions(id) ON DELETE CASCADE,
                text        TEXT NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_relay_events_session
            ON relay_events (session_id, id);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_relay_sessions_status
            ON relay_sessions (status);
        """)
    logger.info("Relay tables verified/created")
