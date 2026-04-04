"""Per-IP daily budget for mutating Mini App API traffic (PostgreSQL-backed).

HTTP: only POST/PUT/PATCH/DELETE to /api/* are counted (see RateLimitMiddleware).
Voice: /ws/voice increments the same counter once per connection attempt (voice.py).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from src.nadobro.db import execute, execute_returning

logger = logging.getLogger(__name__)

_DAILY_LIMIT = int(os.environ.get("MINIAPP_RATE_LIMIT_PER_IP_PER_DAY", "50"))


def ensure_rate_limit_table() -> None:
    execute(
        """
        CREATE TABLE IF NOT EXISTS miniapp_rate_limit (
            client_ip TEXT NOT NULL,
            day_utc DATE NOT NULL,
            request_count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (client_ip, day_utc)
        )
        """
    )


def _today_utc():
    return datetime.now(timezone.utc).date()


def check_rate_limit(client_ip: str, limit: int | None = None) -> bool:
    """Increment counter for (ip, today UTC). Return True if allowed, False if over limit."""
    lim = limit if limit is not None else _DAILY_LIMIT
    if lim <= 0:
        return True
    ip = (client_ip or "unknown").strip()[:128] if client_ip else "unknown"
    day = _today_utc()
    try:
        row = execute_returning(
            """
            INSERT INTO miniapp_rate_limit (client_ip, day_utc, request_count)
            VALUES (%s, %s, 1)
            ON CONFLICT (client_ip, day_utc) DO UPDATE
            SET request_count = miniapp_rate_limit.request_count + 1
            RETURNING request_count
            """,
            (ip, day),
        )
        count = int((row or {}).get("request_count") or 0)
    except Exception:
        logger.exception("miniapp rate limit DB error; allowing request ip=%s", ip)
        return True
    if count > lim:
        logger.warning("miniapp rate limit exceeded ip=%s count=%s limit=%s", ip, count, lim)
        return False
    return True
