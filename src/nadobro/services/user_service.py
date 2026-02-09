import logging
from datetime import datetime
from src.nadobro.models.database import User, NetworkMode, get_session
from src.nadobro.services.crypto import (
    generate_wallet, encrypt_private_key, decrypt_private_key,
    hash_mnemonic, recover_wallet_from_mnemonic,
)
from src.nadobro.services.nado_client import get_nado_client, NadoClient, clear_client_cache

logger = logging.getLogger(__name__)


def get_or_create_user(telegram_id: int, username: str = None) -> tuple[User, bool, str | None]:
    with get_session() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if user:
            user.last_active = datetime.utcnow()
            if username:
                user.telegram_username = username
            session.commit()
            merged = session.merge(user)
            session.expunge(merged)
            return merged, False, None

        wallet = generate_wallet()
        encrypted_key = encrypt_private_key(wallet["private_key"])
        mnemonic_h = hash_mnemonic(wallet["mnemonic"])

        user = User(
            telegram_id=telegram_id,
            telegram_username=username,
            encrypted_private_key_testnet=encrypted_key,
            wallet_address_testnet=wallet["address"],
            mnemonic_hash_testnet=mnemonic_h,
            network_mode=NetworkMode.TESTNET,
        )
        session.add(user)
        session.commit()
        session.refresh(user)
        session.expunge(user)

        return user, True, wallet["mnemonic"]


def get_user(telegram_id: int) -> User | None:
    with get_session() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if user:
            session.expunge(user)
        return user


def switch_network(telegram_id: int, network: str) -> tuple[bool, str]:
    with get_session() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if not user:
            return False, "User not found. Use /start first."

        new_mode = NetworkMode.MAINNET if network == "mainnet" else NetworkMode.TESTNET

        if new_mode == NetworkMode.MAINNET and not user.encrypted_private_key_mainnet:
            wallet = generate_wallet()
            encrypted_key = encrypt_private_key(wallet["private_key"])
            user.encrypted_private_key_mainnet = encrypted_key
            user.wallet_address_mainnet = wallet["address"]
            user.mnemonic_hash_mainnet = hash_mnemonic(wallet["mnemonic"])
            mnemonic_msg = (
                f"New mainnet wallet created!\n"
                f"Address: {wallet['address']}\n\n"
                f"SAVE YOUR RECOVERY PHRASE (shown once):\n"
                f"`{wallet['mnemonic']}`\n\n"
                f"Store this safely - it's the ONLY way to recover your mainnet wallet."
            )
        else:
            mnemonic_msg = None

        user.network_mode = new_mode
        clear_client_cache()
        session.commit()

        addr = user.wallet_address_mainnet if new_mode == NetworkMode.MAINNET else user.wallet_address_testnet
        msg = f"Switched to {network} mode.\nActive wallet: `{addr}`"
        if mnemonic_msg:
            msg = mnemonic_msg + "\n\n" + msg

        return True, msg


def get_user_nado_client(telegram_id: int) -> NadoClient | None:
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


def get_user_wallet_info(telegram_id: int) -> dict | None:
    with get_session() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if not user:
            return None
        return {
            "testnet_address": user.wallet_address_testnet,
            "mainnet_address": user.wallet_address_mainnet,
            "network": user.network_mode.value,
            "active_address": user.wallet_address_mainnet if user.network_mode == NetworkMode.MAINNET else user.wallet_address_testnet,
        }


def recover_user_wallet(telegram_id: int, mnemonic: str, network: str = "testnet") -> tuple[bool, str]:
    with get_session() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if not user:
            return False, "User not found."

        try:
            wallet = recover_wallet_from_mnemonic(mnemonic)
        except Exception as e:
            return False, f"Invalid recovery phrase: {str(e)}"

        encrypted_key = encrypt_private_key(wallet["private_key"])

        if network == "mainnet":
            user.encrypted_private_key_mainnet = encrypted_key
            user.wallet_address_mainnet = wallet["address"]
            user.mnemonic_hash_mainnet = hash_mnemonic(mnemonic)
        else:
            user.encrypted_private_key_testnet = encrypted_key
            user.wallet_address_testnet = wallet["address"]
            user.mnemonic_hash_testnet = hash_mnemonic(mnemonic)

        clear_client_cache()
        session.commit()

        return True, f"Wallet recovered successfully!\nAddress: `{wallet['address']}`"


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
