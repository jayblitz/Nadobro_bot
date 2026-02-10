import hashlib
import hmac
import json
import time
from urllib.parse import parse_qs, unquote
from src.nadobro.config import TELEGRAM_TOKEN


def validate_init_data(init_data: str, max_age_seconds: int = 86400) -> dict | None:
    parsed = parse_qs(init_data, keep_blank_values=True)

    if "hash" not in parsed:
        return None

    received_hash = parsed.pop("hash")[0]

    data_check_pairs = []
    for key in sorted(parsed.keys()):
        val = parsed[key][0]
        data_check_pairs.append(f"{key}={val}")

    data_check_string = "\n".join(data_check_pairs)

    secret_key = hmac.new(
        b"WebAppData", TELEGRAM_TOKEN.encode("utf-8"), hashlib.sha256
    ).digest()

    computed_hash = hmac.new(
        secret_key, data_check_string.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(computed_hash, received_hash):
        return None

    auth_date_str = parsed.get("auth_date", [None])[0]
    if auth_date_str:
        try:
            auth_date = int(auth_date_str)
            if time.time() - auth_date > max_age_seconds:
                return None
        except (ValueError, TypeError):
            return None

    user_str = parsed.get("user", [None])[0]
    if user_str:
        try:
            user_data = json.loads(unquote(user_str))
            return user_data
        except (json.JSONDecodeError, TypeError):
            return None

    return None


def get_telegram_user_from_init_data(init_data: str) -> dict | None:
    return validate_init_data(init_data)
