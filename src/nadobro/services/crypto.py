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
    except Exception as e:
        raise RuntimeError(
            "ENCRYPTION_KEY is not a valid Fernet key. "
            "Generate a proper key with: "
            "python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
        ) from e


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



def private_key_fingerprint(private_key: str) -> str:
    normalized = normalize_private_key(private_key)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return digest[-8:]


def encrypt_with_server_key(data: bytes) -> bytes:
    f = _get_fernet()
    return f.encrypt(data)


def decrypt_with_server_key(ciphertext: bytes) -> bytes:
    f = _get_fernet()
    return f.decrypt(ciphertext)



