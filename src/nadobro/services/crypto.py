import os
import hashlib
import base64
import secrets
import re
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
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

    import logging
    logging.getLogger(__name__).warning(
        "ENCRYPTION_KEY is not a valid Fernet key — deriving one via SHA-256. "
        "For better security, generate a proper key with: "
        "python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
    )
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


PBKDF2_ITERATIONS = 600_000


def decrypt_with_passphrase(ciphertext: bytes, salt: bytes, passphrase: str) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=PBKDF2_ITERATIONS,
        backend=default_backend(),
    )
    key = base64.urlsafe_b64encode(kdf.derive(passphrase.encode("utf-8")))
    f = Fernet(key)
    return f.decrypt(ciphertext)
