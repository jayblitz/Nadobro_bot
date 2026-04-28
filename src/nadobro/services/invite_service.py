import hashlib
import logging
import os
import secrets
import string
import time
from datetime import datetime, timezone
from typing import Optional

import psycopg2.extras

from src.nadobro.config import ADMIN_USER_IDS, ENCRYPTION_KEY
from src.nadobro.db import execute, execute_returning, get_db, put_db, query_one
from src.nadobro.models.database import insert_admin_log
from src.nadobro.services.user_service import get_or_create_user, invalidate_user_cache

logger = logging.getLogger(__name__)

INVITE_CODE_LENGTH = 8
INVITE_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
_ACCESS_CACHE_TTL = 10
_ACCESS_CACHE_MAX_ENTRIES = 1024
_access_cache: dict[int, dict] = {}


def normalize_code(code: str) -> str:
    return "".join(ch for ch in str(code or "").upper() if ch in string.ascii_uppercase + string.digits)


def _invite_pepper() -> str:
    pepper = os.environ.get("INVITE_CODE_PEPPER") or ENCRYPTION_KEY
    if pepper:
        return pepper
    logger.warning("INVITE_CODE_PEPPER and ENCRYPTION_KEY are unset; using development invite-code pepper")
    return "nadobro-dev-invite-pepper"


def _hash_code(code: str) -> str:
    normalized = normalize_code(code)
    return hashlib.sha256(f"{normalized}:{_invite_pepper()}".encode("utf-8")).hexdigest()


def _generate_plain_code() -> str:
    return "".join(secrets.choice(INVITE_CODE_ALPHABET) for _ in range(INVITE_CODE_LENGTH))


def _is_expired(row: dict) -> bool:
    expires_at = row.get("expires_at")
    if not expires_at:
        return False
    if isinstance(expires_at, str):
        try:
            expires_at = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        except ValueError:
            return False
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return expires_at <= datetime.now(timezone.utc)


def _prune_access_cache() -> None:
    now = time.time()
    stale = [k for k, v in _access_cache.items() if now - float(v.get("ts") or 0) > _ACCESS_CACHE_TTL]
    for key in stale:
        _access_cache.pop(key, None)
    while len(_access_cache) > _ACCESS_CACHE_MAX_ENTRIES:
        oldest = min(_access_cache, key=lambda k: float(_access_cache[k].get("ts") or 0))
        _access_cache.pop(oldest, None)


def invalidate_private_access_cache(telegram_id: Optional[int] = None) -> None:
    if telegram_id is None:
        _access_cache.clear()
        return
    _access_cache.pop(int(telegram_id), None)


def _cache_access(telegram_id: int, granted: bool) -> None:
    _prune_access_cache()
    _access_cache[int(telegram_id)] = {"granted": bool(granted), "ts": time.time()}


def has_private_access(telegram_id: int) -> bool:
    telegram_id = int(telegram_id)
    if telegram_id in ADMIN_USER_IDS:
        return True
    entry = _access_cache.get(telegram_id)
    if entry and time.time() - float(entry.get("ts") or 0) < _ACCESS_CACHE_TTL:
        return bool(entry.get("granted"))
    if entry:
        _access_cache.pop(telegram_id, None)
    row = query_one(
        "SELECT private_access_granted FROM users WHERE telegram_id = %s",
        (telegram_id,),
    )
    granted = bool(row and row.get("private_access_granted"))
    _cache_access(telegram_id, granted)
    return granted


def generate_invite_codes(
    admin_id: int,
    count: int,
    *,
    note: str | None = None,
    expires_at: datetime | None = None,
    created_for_telegram_id: int | None = None,
) -> list[dict]:
    if int(admin_id) not in ADMIN_USER_IDS:
        raise PermissionError("Unauthorized.")
    count = max(1, min(int(count or 1), 100))
    generated: list[dict] = []
    attempts = 0
    while len(generated) < count and attempts < count * 5:
        attempts += 1
        code = _generate_plain_code()
        row = execute_returning(
            """
            INSERT INTO invite_codes
                (code_hash, code_prefix, created_by, created_for_telegram_id, note, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (code_hash) DO NOTHING
            RETURNING id, code_prefix, created_at, expires_at, note
            """,
            (_hash_code(code), code[:3], int(admin_id), created_for_telegram_id, note, expires_at),
        )
        if row:
            row["code"] = code
            generated.append(row)
    if len(generated) != count:
        raise RuntimeError("Failed to generate enough unique invite codes.")
    insert_admin_log(
        {
            "admin_id": int(admin_id),
            "action": "invite_generate",
            "details": f"count={count} note={note or ''} created_for={created_for_telegram_id or ''}",
        }
    )
    return generated


def redeem_invite_code(telegram_id: int, username: str | None, code: str) -> tuple[bool, str]:
    telegram_id = int(telegram_id)
    normalized = normalize_code(code)
    if len(normalized) != INVITE_CODE_LENGTH:
        return False, "Invalid access code. Please enter the 8-character code from @jaynadobro."

    get_or_create_user(telegram_id, username)
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM invite_codes WHERE code_hash = %s FOR UPDATE", (_hash_code(normalized),))
            invite = dict(cur.fetchone() or {})
            if not invite:
                conn.rollback()
                return False, "Invalid access code. Please check the code and try again."
            if invite.get("revoked_at") or _is_expired(invite):
                conn.rollback()
                return False, "This access code is no longer available."
            if invite.get("created_for_telegram_id") and int(invite["created_for_telegram_id"]) != telegram_id:
                conn.rollback()
                return False, "This access code was issued for another Telegram account."
            if int(invite.get("redemption_count") or 0) >= int(invite.get("max_redemptions") or 1):
                if invite.get("redeemed_by") == telegram_id:
                    cur.execute(
                        """
                        UPDATE users
                        SET private_access_granted = true,
                            private_access_code_id = %s,
                            private_access_granted_at = COALESCE(private_access_granted_at, now()),
                            private_access_granted_by = %s
                        WHERE telegram_id = %s
                        """,
                        (invite["id"], invite["created_by"], telegram_id),
                    )
                    conn.commit()
                    invalidate_private_access_cache(telegram_id)
                    invalidate_user_cache(telegram_id)
                    return True, "Access already granted. Welcome back to Nadobro."
                conn.rollback()
                return False, "This access code has already been used."

            cur.execute(
                """
                UPDATE invite_codes
                SET redeemed_by = %s,
                    redeemed_username = %s,
                    redeemed_at = now(),
                    redemption_count = redemption_count + 1,
                    updated_at = now()
                WHERE id = %s
                """,
                (telegram_id, username, invite["id"]),
            )
            cur.execute(
                """
                UPDATE users
                SET private_access_granted = true,
                    private_access_code_id = %s,
                    private_access_granted_at = now(),
                    private_access_granted_by = %s
                WHERE telegram_id = %s
                """,
                (invite["id"], invite["created_by"], telegram_id),
            )
        conn.commit()
        invalidate_private_access_cache(telegram_id)
        invalidate_user_cache(telegram_id)
        return True, "Access granted. Welcome to Nadobro."
    except Exception:
        conn.rollback()
        raise
    finally:
        put_db(conn)


def grant_private_access(admin_id: int, telegram_id: int, note: str | None = None) -> tuple[bool, str]:
    if int(admin_id) not in ADMIN_USER_IDS:
        return False, "Unauthorized."
    get_or_create_user(int(telegram_id))
    execute(
        """
        UPDATE users
        SET private_access_granted = true,
            private_access_granted_at = COALESCE(private_access_granted_at, now()),
            private_access_granted_by = %s
        WHERE telegram_id = %s
        """,
        (int(admin_id), int(telegram_id)),
    )
    invalidate_private_access_cache(int(telegram_id))
    invalidate_user_cache(int(telegram_id))
    insert_admin_log(
        {
            "admin_id": int(admin_id),
            "action": "invite_grant",
            "details": f"telegram_id={telegram_id} note={note or ''}",
        }
    )
    return True, "Private access granted."


def revoke_invite_code(admin_id: int, code: str) -> tuple[bool, str]:
    if int(admin_id) not in ADMIN_USER_IDS:
        return False, "Unauthorized."
    normalized = normalize_code(code)
    if len(normalized) != INVITE_CODE_LENGTH:
        return False, "Invalid access code."
    row = execute_returning(
        """
        UPDATE invite_codes
        SET revoked_at = now(), revoked_by = %s, updated_at = now()
        WHERE code_hash = %s AND revoked_at IS NULL
        RETURNING id, redeemed_by
        """,
        (int(admin_id), _hash_code(normalized)),
    )
    if not row:
        return False, "Invite code not found or already revoked."
    if row.get("redeemed_by"):
        invalidate_private_access_cache(int(row["redeemed_by"]))
    insert_admin_log({"admin_id": int(admin_id), "action": "invite_revoke", "details": f"prefix={normalized[:3]}"})
    return True, "Invite code revoked."


def get_user_invite_status(telegram_id: int) -> dict:
    row = query_one(
        """
        SELECT u.telegram_id, u.telegram_username, u.private_access_granted,
               u.private_access_granted_at, u.private_access_granted_by,
               c.code_prefix, c.redeemed_at, c.revoked_at, c.expires_at, c.note
        FROM users u
        LEFT JOIN invite_codes c ON c.id = u.private_access_code_id
        WHERE u.telegram_id = %s
        """,
        (int(telegram_id),),
    )
    return row or {"telegram_id": int(telegram_id), "private_access_granted": False}


def get_invite_code_status(code: str) -> Optional[dict]:
    normalized = normalize_code(code)
    if len(normalized) != INVITE_CODE_LENGTH:
        return None
    return query_one(
        """
        SELECT id, code_prefix, created_by, created_for_telegram_id, note,
               max_redemptions, redemption_count, redeemed_by, redeemed_username,
               redeemed_at, expires_at, revoked_at, revoked_by, created_at, updated_at
        FROM invite_codes
        WHERE code_hash = %s
        """,
        (_hash_code(normalized),),
    )
