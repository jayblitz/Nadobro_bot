import logging
import time
from datetime import datetime
from typing import Optional

from src.nadobro.models.database import UserRow, NetworkMode, get_supabase
from src.nadobro.services.nado_client import get_nado_client, NadoClient, clear_client_cache

logger = logging.getLogger(__name__)

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
    sb = get_supabase()
    r = sb.table("users").select("*").eq("telegram_id", telegram_id).execute()
    if r.data and len(r.data) > 0:
        row = r.data[0]
        sb.table("users").update({
            "last_active": datetime.utcnow().isoformat(),
            **({"telegram_username": username} if username else {}),
        }).eq("telegram_id", telegram_id).execute()
        user = UserRow(row)
        _cache_user(user)
        return user, False, None

    sb.table("users").insert({
        "telegram_id": telegram_id,
        "telegram_username": username,
        "language": "en",
        "network_mode": "mainnet",
    }).execute()
    r = sb.table("users").select("*").eq("telegram_id", telegram_id).execute()
    user = UserRow(r.data[0]) if r.data else UserRow({"telegram_id": telegram_id, "network_mode": "mainnet"})
    _cache_user(user)
    return user, True, None


def get_user(telegram_id: int) -> Optional[UserRow]:
    cached = _get_cached_user(telegram_id)
    if cached:
        return cached
    sb = get_supabase()
    r = sb.table("users").select("*").eq("telegram_id", telegram_id).execute()
    if not r.data or len(r.data) == 0:
        return None
    user = UserRow(r.data[0])
    _cache_user(user)
    return user


def switch_network(telegram_id: int, network: str) -> tuple[bool, str]:
    user = get_user(telegram_id)
    if not user:
        return False, "User not found. Use /start first."

    new_mode = NetworkMode.MAINNET if network == "mainnet" else NetworkMode.TESTNET
    get_supabase().table("users").update({"network_mode": network}).eq("telegram_id", telegram_id).execute()
    clear_client_cache()
    invalidate_user_cache(telegram_id)

    addr = user.main_address
    if addr:
        msg = f"Switched to {network} mode.\nActive wallet: `{addr}`"
    else:
        msg = (
            f"Switched to {network} mode.\n"
            "Link your wallet via the Wallet button to trade."
        )
    return True, msg


def get_user_nado_client(telegram_id: int, passphrase: Optional[str] = None) -> Optional[NadoClient]:
    """Return NadoClient for user's linked signer. Requires passphrase to decrypt (or cached in session)."""
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
        return get_nado_client(pk, network)
    except Exception as e:
        logger.warning("Failed to get Nado client for user %s: %s", telegram_id, e)
        return None


def get_user_wallet_info(telegram_id: int) -> Optional[dict]:
    user = get_user(telegram_id)
    if not user:
        return None
    network = user.network_mode.value
    linked = bool(user.linked_signer_address and user.encrypted_linked_signer_pk)
    return {
        "testnet_address": user.main_address,
        "mainnet_address": user.main_address,
        "network": network,
        "active_address": user.main_address,
        "linked_signer_address": user.linked_signer_address,
        "testnet_ready": linked,
        "mainnet_ready": linked,
    }


def save_linked_signer(
    telegram_id: int,
    main_address: str,
    linked_signer_address: str,
    encrypted_pk: bytes,
    salt: bytes,
) -> None:
    """Store linked signer after wallet link flow. encrypted_pk/salt stored as base64 for portability."""
    import base64
    sb = get_supabase()
    sb.table("users").update({
        "main_address": main_address,
        "linked_signer_address": linked_signer_address,
        "encrypted_linked_signer_pk": base64.b64encode(encrypted_pk).decode("ascii"),
        "salt": base64.b64encode(salt).decode("ascii"),
    }).eq("telegram_id", telegram_id).execute()
    invalidate_user_cache(telegram_id)
    clear_client_cache()


def has_mode_private_key(telegram_id: int, network: str) -> bool:
    user = get_user(telegram_id)
    if not user:
        return False
    return bool(user.linked_signer_address and user.encrypted_linked_signer_pk)


def ensure_active_wallet_ready(telegram_id: int) -> tuple[bool, str]:
    user = get_user(telegram_id)
    if not user:
        return False, "User not found. Use /start first."
    if not has_mode_private_key(telegram_id, user.network_mode.value):
        return (
            False,
            "Wallet not linked. Use the Wallet button to connect your wallet (Linked Signer).",
        )
    return True, ""



def update_trade_stats(telegram_id: int, volume_usd: float):
    sb = get_supabase()
    r = sb.table("users").select("total_trades, total_volume_usd").eq("telegram_id", telegram_id).execute()
    if not r.data or len(r.data) == 0:
        return
    row = r.data[0]
    sb.table("users").update({
        "total_trades": int(row.get("total_trades") or 0) + 1,
        "total_volume_usd": float(row.get("total_volume_usd") or 0) + volume_usd,
        "last_trade_at": datetime.utcnow().isoformat(),
    }).eq("telegram_id", telegram_id).execute()
    invalidate_user_cache(telegram_id)


def get_all_users_count() -> int:
    r = get_supabase().table("users").select("telegram_id", count="exact").execute()
    return r.count or 0


def get_active_users_count() -> int:
    from datetime import timedelta
    cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat()
    r = get_supabase().table("users").select("telegram_id", count="exact").gte("last_active", cutoff).execute()
    return r.count or 0



def remove_user_private_key(telegram_id: int, network: str = "testnet") -> tuple[bool, str]:
    # Could clear linked_signer_address/encrypted_linked_signer_pk/main_address
    get_supabase().table("users").update({
        "main_address": None,
        "linked_signer_address": None,
        "encrypted_linked_signer_pk": None,
        "salt": None,
    }).eq("telegram_id", telegram_id).execute()
    invalidate_user_cache(telegram_id)
    clear_client_cache()
    return True, f"{network} wallet unlinked. You can link again via Wallet button."