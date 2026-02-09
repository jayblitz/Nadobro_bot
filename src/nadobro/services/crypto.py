import os
import hashlib
import secrets
from cryptography.fernet import Fernet
from eth_account import Account

Account.enable_unaudited_hdwallet_features()

def _get_fernet() -> Fernet:
    key = os.environ.get("ENCRYPTION_KEY")
    if not key:
        raise RuntimeError("ENCRYPTION_KEY environment variable not set")
    if len(key) < 32:
        key = hashlib.sha256(key.encode()).digest()
        key = __import__("base64").urlsafe_b64encode(key)
    elif isinstance(key, str):
        try:
            Fernet(key.encode() if isinstance(key, str) else key)
            key = key.encode() if isinstance(key, str) else key
        except Exception:
            key = hashlib.sha256(key.encode()).digest()
            key = __import__("base64").urlsafe_b64encode(key)
    return Fernet(key if isinstance(key, bytes) else key.encode())


def generate_wallet() -> dict:
    acct, mnemonic = Account.create_with_mnemonic()
    return {
        "address": acct.address,
        "private_key": acct.key.hex(),
        "mnemonic": mnemonic,
    }


def encrypt_private_key(private_key: str) -> str:
    f = _get_fernet()
    return f.encrypt(private_key.encode()).decode()


def decrypt_private_key(encrypted_key: str) -> str:
    f = _get_fernet()
    return f.decrypt(encrypted_key.encode()).decode()


def hash_mnemonic(mnemonic: str) -> str:
    return hashlib.sha256(mnemonic.encode()).hexdigest()


def recover_wallet_from_mnemonic(mnemonic: str) -> dict:
    acct = Account.from_mnemonic(mnemonic)
    return {
        "address": acct.address,
        "private_key": acct.key.hex(),
    }


def generate_webhook_secret() -> str:
    return secrets.token_hex(32)
