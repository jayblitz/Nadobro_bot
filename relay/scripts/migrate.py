"""
Run this script to create relay tables in your Supabase PostgreSQL database.

Usage:
    DATABASE_URL="postgresql://..." python -m relay.scripts.migrate
"""

import asyncio
import os
import sys


async def main():
    dsn = os.environ.get("DATABASE_URL", "")
    if not dsn:
        print("ERROR: DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)

    import asyncpg
    conn = await asyncpg.connect(dsn)

    print("Creating relay_sessions table...")
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

    print("Creating relay_events table...")
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS relay_events (
            cursor_id   BIGSERIAL PRIMARY KEY,
            session_id  TEXT NOT NULL REFERENCES relay_sessions(id) ON DELETE CASCADE,
            text        TEXT NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    print("Creating indexes...")
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_relay_events_session
        ON relay_events (session_id, cursor_id);
    """)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_relay_sessions_status
        ON relay_sessions (status);
    """)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_relay_sessions_request_id
        ON relay_sessions (request_id);
    """)

    await conn.close()
    print("Done! Relay tables created successfully.")


if __name__ == "__main__":
    asyncio.run(main())
