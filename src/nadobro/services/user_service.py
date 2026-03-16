import logging
import time
from datetime import datetime
from typing import Optional

from src.nadobro.models.database import UserRow, NetworkMode
from src.nadobro.db import query_one, query_all, execute, query_count
from src.nadobro.services.nado_client import get_nado_client, NadoClient, clear_client_cache
from src.nadobro.i18n import get_active_language, localize_text

logger = logging.getLogger(__name__)


def _loc(text):
    return localize_text(text, get_active_language())


_user_cache = {}
_USER_CACHE_TTL = 10


def _cache_user(user: UserRow):
    _user_cache[user.telegram_id] = {"user": user, "ts": time.time()}


def _get_cached_user(telegram_id: int) -> Optional[UserRow]:
    entry = _user_cache.get(telegram_id)
    if entry and (time.time() - entry["ts"] < _USER_CACHE_TTL):
        return entry["user"]
    return None


def invalidate_user_cache(telegram_id: Optional[int] = None):
    if telegram_id:
        _user_cache.pop(telegram_id, None)
    else:
        _user_cache.clear()


def get_or_create_user(telegram_id: int, username: str = None) -> tuple[UserRow, bool, Optional[str]]:
    row = query_one("SELECT * FROM users WHERE telegram_id = %s", (telegram_id,))
    if row:
        if username:
            execute(
                "UPDATE users SET last_active = %s, telegram_username = %s WHERE telegram_id = %s",
                (datetime.utcnow().isoformat(), username, telegram_id),
            )
        else:
            execute(
                "UPDATE users SET last_active = %s WHERE telegram_id = %s",
                (datetime.utcnow().isoformat(), telegram_id),
            )
        user = UserRow(row)
        _cache_user(user)
        return user, False, None

    execute(
        "INSERT INTO users (telegram_id, telegram_username, language, network_mode) VALUES (%s, %s, %s, %s)",
        (telegram_id, username, "en", "mainnet"),
    )
    row = query_one("SELECT * FROM users WHERE telegram_id = %s", (telegram_id,))
    user = UserRow(row) if row else UserRow({"telegram_id": telegram_id, "network_mode": "mainnet"})
    _cache_user(user)
    return user, True, None


def get_user(telegram_id: int) -> Optional[UserRow]:
    cached = _get_cached_user(telegram_id)
    if cached:
        return cached
    row = query_one("SELECT * FROM users WHERE telegram_id = %s", (telegram_id,))
    if not row:
        return None
    user = UserRow(row)
    _cache_user(user)
    return user


def switch_network(telegram_id: int, network: str) -> tuple[bool, str]:
    user = get_user(telegram_id)
    if not user:
        return False, _loc("User not found. Use /start first.")

    execute("UPDATE users SET network_mode = %s WHERE telegram_id = %s", (network, telegram_id))
    clear_client_cache()
    _readonly_cache.clear()
    invalidate_user_cache(telegram_id)

    addr = user.main_address
    if addr:
        msg = f"{_loc('Switched to')} {network} {_loc('mode.')}\n{_loc('Active wallet:')} `{addr}`"
    else:
        msg = (
            f"{_loc('Switched to')} {network} {_loc('mode.')}\n"
            f"{_loc('Link your wallet via the Wallet button to trade.')}"
        )
    return True, msg


def get_user_nado_client(telegram_id: int, passphrase: Optional[str] = None) -> Optional[NadoClient]:
    user = get_user(telegram_id)
    if not user or not user.linked_signer_address or not user.main_address:
        return None
    enc_pk = user.encrypted_linked_signer_pk
    salt = user.salt
    if not enc_pk or not salt or not passphrase:
        return None
    try:
        import base64
        from src.nadobro.services.crypto import decrypt_with_passphrase
        ciphertext = base64.b64decode(enc_pk) if isinstance(enc_pk, str) else enc_pk
        salt_b = base64.b64decode(salt) if isinstance(salt, str) else salt
        pk_bytes = decrypt_with_passphrase(ciphertext, salt_b, passphrase)
        pk = pk_bytes.decode("utf-8") if isinstance(pk_bytes, bytes) else pk_bytes
        network = user.network_mode.value
        return get_nado_client(pk, network, main_address=user.main_address)
    except Exception as e:
        logger.warning("Failed to get Nado client for user %s: %s", telegram_id, e)
        return None


_readonly_cache: dict[str, NadoClient] = {}
_READONLY_CACHE_TTL = 60


def get_user_readonly_client(telegram_id: int) -> Optional[NadoClient]:
    user = get_user(telegram_id)
    if not user or not user.main_address:
        return None
    network = user.network_mode.value
    cache_key = f"ro:{user.main_address}:{network}"
    cached = _readonly_cache.get(cache_key)
    if cached:
        return cached
    client = NadoClient.from_address(user.main_address, network)
    _readonly_cache[cache_key] = client
    return client


def get_user_wallet_info(telegram_id: int, verify_signer: bool = False) -> Optional[dict]:
    user = get_user(telegram_id)
    if not user:
        return None
    network = user.network_mode.value
    linked = bool(user.linked_signer_address and user.encrypted_linked_signer_pk)
    info = {
        "testnet_address": user.main_address,
        "mainnet_address": user.main_address,
        "network": network,
        "active_address": user.main_address,
        "linked_signer_address": user.linked_signer_address,
        "testnet_ready": linked,
        "mainnet_ready": linked,
        "signer_verification": None,
    }

    if verify_signer and linked and user.main_address:
        try:
            readonly = get_user_readonly_client(telegram_id)
            if readonly:
                check = readonly.verify_linked_signer(user.linked_signer_address)
                info["signer_verification"] = check
        except Exception as e:
            logger.warning("Signer verification failed for user %s: %s", telegram_id, e)
            info["signer_verification"] = {"verified": False, "error": str(e), "current_signer": None, "expected_signer": user.linked_signer_address}

    return info


def save_linked_signer(
    telegram_id: int,
    main_address: str,
    linked_signer_address: str,
    encrypted_pk: bytes,
    salt: bytes,
) -> None:
    import base64
    execute(
        "UPDATE users SET main_address = %s, linked_signer_address = %s, encrypted_linked_signer_pk = %s, salt = %s WHERE telegram_id = %s",
        (
            main_address,
            linked_signer_address,
            base64.b64encode(encrypted_pk).decode("ascii"),
            base64.b64encode(salt).decode("ascii"),
            telegram_id,
        ),
    )
    invalidate_user_cache(telegram_id)
    clear_client_cache()
    _readonly_cache.clear()


def has_mode_private_key(telegram_id: int, network: str) -> bool:
    user = get_user(telegram_id)
    if not user:
        return False
    return bool(user.linked_signer_address and user.encrypted_linked_signer_pk)


def ensure_active_wallet_ready(telegram_id: int) -> tuple[bool, str]:
    user = get_user(telegram_id)
    if not user:
        return False, _loc("User not found. Use /start first.")
    if not has_mode_private_key(telegram_id, user.network_mode.value):
        return (
            False,
            _loc("Wallet not linked. Use the Wallet button to connect your wallet (Linked Signer)."),
        )
    return True, ""


def update_trade_stats(telegram_id: int, volume_usd: float):
    row = query_one(
        "SELECT total_trades, total_volume_usd FROM users WHERE telegram_id = %s",
        (telegram_id,),
    )
    if not row:
        return
    execute(
        "UPDATE users SET total_trades = %s, total_volume_usd = %s, last_trade_at = %s WHERE telegram_id = %s",
        (
            int(row.get("total_trades") or 0) + 1,
            float(row.get("total_volume_usd") or 0) + volume_usd,
            datetime.utcnow().isoformat(),
            telegram_id,
        ),
    )
    invalidate_user_cache(telegram_id)


def get_all_users_count() -> int:
    return query_count("SELECT COUNT(*) FROM users")


def get_active_users_count() -> int:
    from datetime import timedelta
    cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat()
    return query_count("SELECT COUNT(*) FROM users WHERE last_active >= %s", (cutoff,))


def remove_user_private_key(telegram_id: int, network: str = "testnet") -> tuple[bool, str]:
    execute(
        "UPDATE users SET main_address = NULL, linked_signer_address = NULL, encrypted_linked_signer_pk = NULL, salt = NULL WHERE telegram_id = %s",
        (telegram_id,),
    )
    invalidate_user_cache(telegram_id)
    clear_client_cache()
    _readonly_cache.clear()
    return True, _loc(f"{network} wallet unlinked. You can link again via Wallet button.")


def update_user_language(telegram_id: int, lang: str):
    execute("UPDATE users SET language = %s WHERE telegram_id = %s", (lang, telegram_id))
    invalidate_user_cache(telegram_id)
