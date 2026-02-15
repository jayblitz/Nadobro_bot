import logging
import time
from datetime import datetime
from typing import Optional
from src.nadobro.models.database import User, NetworkMode, get_session
from src.nadobro.services.crypto import (
    encrypt_private_key, decrypt_private_key,
    derive_address_from_private_key, normalize_private_key,
)
from src.nadobro.services.nado_client import get_nado_client, NadoClient, clear_client_cache

logger = logging.getLogger(__name__)

_user_cache = {}
_USER_CACHE_TTL = 10


def _cache_user(user):
    _user_cache[user.telegram_id] = {"user": user, "ts": time.time()}


def _get_cached_user(telegram_id):
    entry = _user_cache.get(telegram_id)
    if entry and (time.time() - entry["ts"] < _USER_CACHE_TTL):
        return entry["user"]
    return None


def invalidate_user_cache(telegram_id=None):
    if telegram_id:
        _user_cache.pop(telegram_id, None)
    else:
        _user_cache.clear()


def get_or_create_user(telegram_id: int, username: str = None) -> tuple[User, bool, Optional[str]]:
    with get_session() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if user:
            user.last_active = datetime.utcnow()
            if username:
                user.telegram_username = username
            session.commit()
            session.refresh(user)
            session.expunge(user)
            _cache_user(user)
            return user, False, None

        user = User(
            telegram_id=telegram_id,
            telegram_username=username,
            network_mode=NetworkMode.TESTNET,
        )
        session.add(user)
        session.commit()
        session.refresh(user)
        session.expunge(user)
        _cache_user(user)

        return user, True, None


def get_user(telegram_id: int) -> Optional[User]:
    cached = _get_cached_user(telegram_id)
    if cached:
        return cached
    with get_session() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if user:
            session.refresh(user)
            session.expunge(user)
            _cache_user(user)
        return user


def switch_network(telegram_id: int, network: str) -> tuple[bool, str]:
    with get_session() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if not user:
            return False, "User not found. Use /start first."

        new_mode = NetworkMode.MAINNET if network == "mainnet" else NetworkMode.TESTNET

        user.network_mode = new_mode
        clear_client_cache()
        session.commit()
        invalidate_user_cache(telegram_id)

        addr = user.wallet_address_mainnet if new_mode == NetworkMode.MAINNET else user.wallet_address_testnet
        if addr:
            msg = f"Switched to {network} mode.\nActive wallet: `{addr}`"
        else:
            msg = (
                f"Switched to {network} mode.\n"
                "No key imported for this mode yet.\n"
                "Use /import_key to add a dedicated trading private key."
            )

        return True, msg


def get_user_nado_client(telegram_id: int) -> Optional[NadoClient]:
    with get_session() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if not user:
            return None

        if user.network_mode == NetworkMode.MAINNET:
            if not user.encrypted_private_key_mainnet:
                return None
            pk = decrypt_private_key(user.encrypted_private_key_mainnet)
            network = "mainnet"
        else:
            if not user.encrypted_private_key_testnet:
                return None
            pk = decrypt_private_key(user.encrypted_private_key_testnet)
            network = "testnet"

        return get_nado_client(pk, network)


def get_user_wallet_info(telegram_id: int) -> Optional[dict]:
    with get_session() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if not user:
            return None
        return {
            "testnet_address": user.wallet_address_testnet,
            "mainnet_address": user.wallet_address_mainnet,
            "network": user.network_mode.value,
            "active_address": user.wallet_address_mainnet if user.network_mode == NetworkMode.MAINNET else user.wallet_address_testnet,
            "testnet_ready": bool(user.encrypted_private_key_testnet and user.wallet_address_testnet),
            "mainnet_ready": bool(user.encrypted_private_key_mainnet and user.wallet_address_mainnet),
        }


def import_user_private_key(telegram_id: int, private_key: str, network: str = "testnet") -> tuple[bool, str]:
    with get_session() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if not user:
            return False, "User not found."

        try:
            normalized = normalize_private_key(private_key)
            address = derive_address_from_private_key(normalized)
        except Exception as e:
            return False, f"Invalid private key: {str(e)}"

        encrypted_key = encrypt_private_key(normalized)

        if network == "mainnet":
            user.encrypted_private_key_mainnet = encrypted_key
            user.wallet_address_mainnet = address
            user.mnemonic_hash_mainnet = None
        else:
            user.encrypted_private_key_testnet = encrypted_key
            user.wallet_address_testnet = address
            user.mnemonic_hash_testnet = None

        clear_client_cache()
        session.commit()
        invalidate_user_cache(telegram_id)

        return True, f"Trading key imported successfully.\nAddress: `{address}`"


def remove_user_private_key(telegram_id: int, network: str = "testnet") -> tuple[bool, str]:
    with get_session() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if not user:
            return False, "User not found."

        if network == "mainnet":
            user.encrypted_private_key_mainnet = None
            user.wallet_address_mainnet = None
            user.mnemonic_hash_mainnet = None
        else:
            user.encrypted_private_key_testnet = None
            user.wallet_address_testnet = None
            user.mnemonic_hash_testnet = None

        clear_client_cache()
        session.commit()
        invalidate_user_cache(telegram_id)
        return True, f"{network} key removed."


def has_mode_private_key(telegram_id: int, network: str) -> bool:
    user = get_user(telegram_id)
    if not user:
        return False
    if network == "mainnet":
        return bool(user.encrypted_private_key_mainnet and user.wallet_address_mainnet)
    return bool(user.encrypted_private_key_testnet and user.wallet_address_testnet)


def ensure_active_wallet_ready(telegram_id: int) -> tuple[bool, str]:
    user = get_user(telegram_id)
    if not user:
        return False, "User not found. Use /start first."
    network = user.network_mode.value
    if not has_mode_private_key(telegram_id, network):
        return (
            False,
            f"No trading key set for {network} mode. "
            "Use /import_key and paste a dedicated private key for this mode.",
        )
    client = get_user_nado_client(telegram_id)
    if not client:
        return False, "Wallet client not initialized. Try /import_key again."
    balance = client.get_balance()
    if not balance.get("exists"):
        return (
            False,
            "Subaccount not found on Nado yet. Fund your imported wallet first, then retry.",
        )
    return True, ""


def get_user_private_key(telegram_id: int, network: Optional[str] = None) -> tuple[bool, str]:
    user = get_user(telegram_id)
    if not user:
        return False, "User not found."

    target_network = network or user.network_mode.value
    if target_network == "mainnet":
        encrypted = user.encrypted_private_key_mainnet
    else:
        encrypted = user.encrypted_private_key_testnet

    if not encrypted:
        return False, f"No private key set for {target_network} mode."

    try:
        return True, decrypt_private_key(encrypted)
    except Exception:
        return False, "Failed to decrypt private key."


def update_trade_stats(telegram_id: int, volume_usd: float):
    with get_session() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if user:
            user.total_trades = (user.total_trades or 0) + 1
            user.total_volume_usd = (user.total_volume_usd or 0) + volume_usd
            user.last_trade_at = datetime.utcnow()
            session.commit()


def get_all_users_count() -> int:
    with get_session() as session:
        return session.query(User).count()


def get_active_users_count() -> int:
    with get_session() as session:
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(days=7)
        return session.query(User).filter(User.last_active >= cutoff).count()
