import os
import hashlib
import base64
import secrets
import re
from cryptography.fernet import Fernet, InvalidToken
from eth_account import Account

Account.enable_unaudited_hdwallet_features()

_fernet_instance = None

def _get_fernet() -> Fernet:
    global _fernet_instance
    if _fernet_instance is not None:
        return _fernet_instance

    key = os.environ.get("ENCRYPTION_KEY")
    if not key:
        raise RuntimeError("ENCRYPTION_KEY environment variable not set")

    key_bytes = key.encode("utf-8") if isinstance(key, str) else key

    try:
        f = Fernet(key_bytes)
        _fernet_instance = f
        return f
    except Exception:
        pass

    derived = hashlib.sha256(key_bytes).digest()
    fernet_key = base64.urlsafe_b64encode(derived)
    _fernet_instance = Fernet(fernet_key)
    return _fernet_instance


def validate_encryption_key():
    try:
        f = _get_fernet()
        test_data = b"nadobro_encryption_test"
        encrypted = f.encrypt(test_data)
        decrypted = f.decrypt(encrypted)
        if decrypted != test_data:
            raise RuntimeError("Encryption round-trip failed")
        return True
    except Exception as e:
        raise RuntimeError(f"ENCRYPTION_KEY validation failed: {e}")


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


def derive_address_from_private_key(private_key: str) -> str:
    acct = Account.from_key(private_key)
    return acct.address


def normalize_private_key(private_key: str) -> str:
    raw = (private_key or "").strip()
    if raw.startswith("0x"):
        raw = raw[2:]
    if not re.fullmatch(r"[0-9a-fA-F]{64}", raw):
        raise ValueError("Invalid private key format. Expected 64 hex chars.")
    return "0x" + raw.lower()


def is_probable_mnemonic(text: str) -> bool:
    if not text:
        return False
    words = [w for w in text.strip().split() if w]
    if len(words) < 12:
        return False
    return all(re.fullmatch(r"[a-zA-Z]+", w) for w in words[:12])


def private_key_fingerprint(private_key: str) -> str:
    normalized = normalize_private_key(private_key)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return digest[-8:]
