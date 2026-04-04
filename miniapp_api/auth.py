"""Telegram Mini App initData HMAC-SHA256 authentication.

The Telegram WebApp passes ``initData`` — a URL-encoded query string that
includes ``hash`` plus user/chat/start params.  We validate it using the bot
token as specified in the Telegram docs:

  1. Build ``data_check_string`` from all params *except* ``hash``, sorted
     alphabetically and joined with newlines.
  2. ``secret_key  = HMAC-SHA256("WebAppData", bot_token)``
  3. ``computed    = HMAC-SHA256(secret_key, data_check_string)``
  4. Valid iff ``computed == hash``.

We also enforce an expiry window (``auth_date`` must be within the last 24 h).
"""

import hashlib
import hmac
import json
import time
from typing import Optional
from urllib.parse import parse_qs, unquote

from miniapp_api.config import TELEGRAM_TOKEN

# Maximum age (seconds) of a valid initData payload.
_MAX_AUTH_AGE = 86_400  # 24 hours


class AuthError(Exception):
    pass


class TelegramUser:
    """Lightweight container for the authenticated Telegram user."""

    __slots__ = ("id", "first_name", "last_name", "username", "language_code", "photo_url")

    def __init__(self, data: dict):
        self.id: int = int(data["id"])
        self.first_name: str = data.get("first_name", "")
        self.last_name: str = data.get("last_name", "")
        self.username: str = data.get("username", "")
        self.language_code: str = data.get("language_code", "en")
        self.photo_url: str = data.get("photo_url", "")


def validate_init_data(init_data: str, *, max_age: int = _MAX_AUTH_AGE) -> TelegramUser:
    """Validate Telegram Mini App ``initData`` and return the authenticated user.

    Raises :class:`AuthError` on any validation failure.
    """
    if not TELEGRAM_TOKEN:
        raise AuthError("TELEGRAM_TOKEN is not configured")

    parsed = parse_qs(init_data, keep_blank_values=True)

    received_hash = parsed.pop("hash", [None])[0]
    if not received_hash:
        raise AuthError("Missing hash in initData")

    # Flatten single-value lists produced by parse_qs.
    flat: dict[str, str] = {}
    for k, v_list in parsed.items():
        flat[k] = v_list[0]

    # 1. Build the data-check string (sorted, newline-separated).
    data_check_string = "\n".join(f"{k}={flat[k]}" for k in sorted(flat))

    # 2. HMAC secret derived from bot token.
    secret_key = hmac.new(b"WebAppData", TELEGRAM_TOKEN.encode(), hashlib.sha256).digest()

    # 3. Compute and compare.
    computed = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(computed, received_hash):
        raise AuthError("initData HMAC verification failed")

    # 4. Check auth_date freshness.
    auth_date_str = flat.get("auth_date")
    if not auth_date_str:
        raise AuthError("Missing auth_date")
    try:
        auth_date = int(auth_date_str)
    except ValueError:
        raise AuthError("Invalid auth_date")

    if time.time() - auth_date > max_age:
        raise AuthError("initData has expired")

    # 5. Extract user object.
    user_json = flat.get("user")
    if not user_json:
        raise AuthError("Missing user in initData")

    try:
        user_data = json.loads(unquote(user_json))
    except (json.JSONDecodeError, TypeError):
        raise AuthError("Invalid user JSON in initData")

    return TelegramUser(user_data)


def parse_init_data_unsafe(init_data: str) -> Optional[TelegramUser]:
    """Best-effort extraction of the user without HMAC checks.

    **Do not use in production routes** — any client could forge ``user`` JSON.
    For local tooling or unit tests only. Returns ``None`` on any failure.
    """
    try:
        parsed = parse_qs(init_data, keep_blank_values=True)
        user_json = parsed.get("user", [None])[0]
        if not user_json:
            return None
        return TelegramUser(json.loads(unquote(user_json)))
    except Exception:
        return None
