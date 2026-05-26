import os
import hashlib
import base64
import secrets
import re
from cryptography.fernet import Fernet, MultiFernet, InvalidToken
from eth_account import Account

Account.enable_unaudited_hdwallet_features()

_fernet_instance = None


def _load_encryption_keys() -> list[str]:
    """Return the ordered list of Fernet keys for encrypt/decrypt.

    Supports zero-downtime key rotation: set ``ENCRYPTION_KEYS`` to a
    comma-separated list (newest first), or keep using the single
    ``ENCRYPTION_KEY``. The FIRST key always performs encryption; every key is
    tried for decryption, so previously-stored ciphertext keeps working after
    you prepend a new key. ``ENCRYPTION_KEYS`` takes precedence when both are
    set, and a bare ``ENCRYPTION_KEY`` is appended so the old primary can still
    decrypt during the migration window.
    """
    keys: list[str] = []
    multi = os.environ.get("ENCRYPTION_KEYS")
    if multi:
        keys.extend(k.strip() for k in multi.split(",") if k.strip())
    single = os.environ.get("ENCRYPTION_KEY")
    if single and single.strip() and single.strip() not in keys:
        keys.append(single.strip())
    return keys


def _get_fernet() -> MultiFernet:
    global _fernet_instance
    if _fernet_instance is not None:
        return _fernet_instance

    raw_keys = _load_encryption_keys()
    if not raw_keys:
        raise RuntimeError("ENCRYPTION_KEY (or ENCRYPTION_KEYS) environment variable not set")

    fernets: list[Fernet] = []
    for key in raw_keys:
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        try:
            fernets.append(Fernet(key_bytes))
        except Exception as e:
            raise RuntimeError(
                "ENCRYPTION_KEY/ENCRYPTION_KEYS contains an invalid Fernet key. "
                "Generate a proper key with: "
                "python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
            ) from e

    # MultiFernet encrypts with fernets[0] and decrypts against all of them.
    _fernet_instance = MultiFernet(fernets)
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


def reencrypt_with_primary_key(ciphertext: bytes) -> bytes:
    """Re-encrypt existing ciphertext under the newest key (rotation helper).

    After prepending a new key to ``ENCRYPTION_KEYS``, run a one-off migration
    that reads each stored ``encrypted_linked_signer_pk``, calls this, and
    writes it back. Once every row is rotated, the retired key can be dropped
    from ``ENCRYPTION_KEYS``.
    """
    return _get_fernet().rotate(ciphertext)



