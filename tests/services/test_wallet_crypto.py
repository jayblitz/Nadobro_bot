"""Wallet crypto — the 1CT linked-signer key path.

``src/nadobro/core/crypto.py`` had NO dedicated test file. That gap surfaced
during the cryptography 48.0.1 -> 50.0.0 bump (PR #217, CVE-2026-69247/8/9):
the whole suite passed and told us nothing about the only question that
mattered — can a key encrypted by the OLD library still be decrypted by the
NEW one? A same-run round-trip passes even if the token format changes
completely, because it encrypts and decrypts with the same code.

The load-bearing test here is ``test_token_encrypted_by_an_older_library_still_decrypts``:
it pins a REAL Fernet token produced under cryptography 48.0.1 as a literal, so
any future upgrade that breaks the stored format fails CI instead of locking
users out of their wallets.

Everything is exercised through the module's own public helpers rather than
through Fernet directly, so key loading, rotation and validation are covered as
they are actually used.
"""
from __future__ import annotations

import importlib

import pytest
from cryptography.fernet import Fernet, InvalidToken

# Deterministic Fernet keys (valid url-safe base64 of 32 bytes). Test-only.
KEY_A = "0123456789abcdef0123456789abcdef0123456789a="
KEY_B = "ZZZZ456789abcdef0123456789abcdef0123456789a="
KEY_C = "QQQQ456789abcdef0123456789abcdef0123456789a="


@pytest.fixture
def crypto(monkeypatch):
    """Fresh crypto module bound to KEY_A, with its MultiFernet cache cleared.

    ``_fernet_instance`` is a module-level singleton, so every test that changes
    the key environment must reset it or it silently keeps the previous keys.
    """
    def _load(*keys: str):
        monkeypatch.delenv("ENCRYPTION_KEYS", raising=False)
        monkeypatch.delenv("ENCRYPTION_KEY", raising=False)
        if len(keys) == 1:
            monkeypatch.setenv("ENCRYPTION_KEY", keys[0])
        elif keys:
            monkeypatch.setenv("ENCRYPTION_KEYS", ",".join(keys))
        mod = importlib.import_module("src.nadobro.core.crypto")
        mod._fernet_instance = None          # noqa: SLF001 - reset the singleton
        return mod

    mod = _load(KEY_A)
    mod._reload_with = _load                 # let tests re-key mid-test
    yield mod
    mod._fernet_instance = None              # noqa: SLF001 - don't leak into other tests


# ==========================================================================
# The regression guard the cryptography bump needed
# ==========================================================================
def test_token_encrypted_by_an_older_library_still_decrypts(crypto):
    """THE test. This ciphertext was produced by cryptography 48.0.1 under
    KEY_A; the suite now runs on 50.0.0. Fernet is a versioned, spec-stable
    format, and this literal is what proves it stays that way — a stored
    linked-signer key must survive every future library upgrade.

    If this ever fails after a dependency bump: DO NOT regenerate the literal.
    It means the on-disk format changed and every user's stored key is
    unreadable. Roll the bump back and plan a migration.
    """
    legacy = (
        b"gAAAAABqcXxryr9-ajYpdXUcvZ_Yx6BtXZarQ9mWp2w9g6Ti-sukp5jn7Ufnpqd9XO_"
        b"ndj3L1FGoHup8nbiblHFFdyfzaKNkXmYRPU8m28C6GNnCIC6oKKdtOgMmxt_l-8LD1KlH8sGJ"
    )
    assert crypto.decrypt_with_server_key(legacy) == b"wallet-private-key-SECRET-PAYLOAD"


def test_a_legacy_token_under_a_RETIRED_key_still_decrypts(crypto):
    """Rotation half of the same guard: this token was encrypted under KEY_B by
    cryptography 48.0.1. With KEY_C primary and KEY_B retained, MultiFernet must
    still read it — that is the whole point of the ENCRYPTION_KEYS list."""
    mod = crypto._reload_with(KEY_C, KEY_B)
    legacy_b = (
        b"gAAAAABqcXxrr5EPv39U4lHOpQzav7JSOorfenvqOGwAB08qZDt9oSnq4SkCZPFpIgp15L4"
        b"Cz9sk00JBh3CQKQxDVPTQzKi1gz343XWzWQQEBlx-ksUEDYY="
    )
    assert mod.decrypt_with_server_key(legacy_b) == b"rotated-key-payload"


# ==========================================================================
# Key loading, precedence, rotation
# ==========================================================================
def test_round_trip(crypto):
    secret = b"0x" + b"a" * 64
    assert crypto.decrypt_with_server_key(crypto.encrypt_with_server_key(secret)) == secret


def test_ciphertext_is_not_the_plaintext(crypto):
    """A misconfiguration that stored the key in the clear must be caught."""
    secret = b"0xdeadbeef"
    blob = crypto.encrypt_with_server_key(secret)
    assert secret not in blob
    assert b"deadbeef" not in blob


def test_encryption_uses_the_FIRST_key_and_decryption_tries_all(crypto):
    """Zero-downtime rotation: prepend a key, and old ciphertext keeps working
    while new ciphertext uses the new primary."""
    old = crypto._reload_with(KEY_B)
    old_blob = old.encrypt_with_server_key(b"written-before-rotation")

    rotated = crypto._reload_with(KEY_C, KEY_B)          # new primary, old retained
    assert rotated.decrypt_with_server_key(old_blob) == b"written-before-rotation"
    new_blob = rotated.encrypt_with_server_key(b"written-after-rotation")

    # The new ciphertext must be readable by the NEW primary alone — proving it
    # was encrypted with KEY_C, not still with KEY_B.
    only_new = crypto._reload_with(KEY_C)
    assert only_new.decrypt_with_server_key(new_blob) == b"written-after-rotation"


def test_a_dropped_key_can_no_longer_read_its_ciphertext(crypto):
    """The safety property behind rotation: retiring a key really does retire
    it, so a leaked old key is worthless once removed AND its rows re-encrypted."""
    old = crypto._reload_with(KEY_B)
    blob = old.encrypt_with_server_key(b"secret")
    only_other = crypto._reload_with(KEY_C)
    with pytest.raises(InvalidToken):
        only_other.decrypt_with_server_key(blob)


def test_reencrypt_moves_ciphertext_onto_the_primary_key(crypto):
    """``reencrypt_with_primary_key`` is the migration helper that makes the
    retired key droppable. After it runs, the old key must be unnecessary."""
    old = crypto._reload_with(KEY_B)
    blob = old.encrypt_with_server_key(b"needs-migrating")

    rotated = crypto._reload_with(KEY_C, KEY_B)
    migrated = rotated.reencrypt_with_primary_key(blob)

    only_new = crypto._reload_with(KEY_C)
    assert only_new.decrypt_with_server_key(migrated) == b"needs-migrating"


def test_bare_ENCRYPTION_KEY_is_appended_so_the_old_primary_still_decrypts(crypto, monkeypatch):
    """Documented migration window: ENCRYPTION_KEYS takes precedence, but a
    lingering ENCRYPTION_KEY must still be tried for DEcryption."""
    monkeypatch.setenv("ENCRYPTION_KEY", KEY_A)
    monkeypatch.delenv("ENCRYPTION_KEYS", raising=False)
    crypto._fernet_instance = None
    blob = crypto.encrypt_with_server_key(b"legacy-single-key")

    monkeypatch.setenv("ENCRYPTION_KEYS", KEY_C)      # new list, old single kept
    crypto._fernet_instance = None
    assert crypto._load_encryption_keys() == [KEY_C, KEY_A]
    assert crypto.decrypt_with_server_key(blob) == b"legacy-single-key"


def test_missing_key_fails_loudly(crypto, monkeypatch):
    monkeypatch.delenv("ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("ENCRYPTION_KEYS", raising=False)
    crypto._fernet_instance = None
    with pytest.raises(RuntimeError, match="ENCRYPTION_KEY"):
        crypto.encrypt_with_server_key(b"x")


def test_malformed_key_error_tells_the_operator_how_to_fix_it(crypto, monkeypatch):
    monkeypatch.setenv("ENCRYPTION_KEY", "not-a-valid-fernet-key")
    monkeypatch.delenv("ENCRYPTION_KEYS", raising=False)
    crypto._fernet_instance = None
    with pytest.raises(RuntimeError) as exc:
        crypto.encrypt_with_server_key(b"x")
    assert "Fernet.generate_key()" in str(exc.value)


def test_validate_encryption_key_round_trips(crypto):
    assert crypto.validate_encryption_key() is True


def test_tampered_ciphertext_is_rejected(crypto):
    """Fernet is authenticated: a flipped byte must raise, never return garbage
    that we would then hand to the signer as a 'private key'."""
    blob = bytearray(crypto.encrypt_with_server_key(b"0x" + b"c" * 64))
    blob[-1] ^= 0x01
    with pytest.raises(InvalidToken):
        crypto.decrypt_with_server_key(bytes(blob))


# ==========================================================================
# Key handling helpers
# ==========================================================================
def test_generate_wallet_produces_a_self_consistent_account(crypto):
    w = crypto.generate_wallet()
    assert set(w) == {"address", "private_key", "mnemonic"}
    assert crypto.derive_address_from_private_key(w["private_key"]) == w["address"]
    assert len(w["mnemonic"].split()) >= 12
    # And it survives the storage path.
    blob = crypto.encrypt_with_server_key(w["private_key"].encode())
    assert crypto.decrypt_with_server_key(blob).decode() == w["private_key"]


def test_generated_keys_are_unique(crypto):
    keys = {crypto.generate_wallet()["private_key"] for _ in range(5)}
    assert len(keys) == 5, "wallet generation is not producing distinct keys"


@pytest.mark.parametrize("raw,expected_suffix", [
    ("0x" + "AB" * 32, "ab" * 32),      # uppercase is lowercased
    ("AB" * 32, "ab" * 32),             # missing 0x prefix is added
    ("  0x" + "cd" * 32 + "  ", "cd" * 32),   # surrounding whitespace
])
def test_normalize_private_key_canonicalises(crypto, raw, expected_suffix):
    out = crypto.normalize_private_key(raw)
    assert out == "0x" + expected_suffix
    assert crypto.normalize_private_key(out) == out, "not idempotent"


@pytest.mark.parametrize("bad", [
    "", "   ", None,
    "0x" + "ab" * 31,          # 62 chars — too short
    "0x" + "ab" * 33,          # 66 chars — too long
    "0x" + "zz" * 32,          # non-hex
    "0x",
])
def test_normalize_private_key_rejects_malformed_input(crypto, bad):
    """A silently-accepted bad key would be stored and then fail at signing
    time, long after the user could connect the two events."""
    with pytest.raises(ValueError, match="Invalid private key"):
        crypto.normalize_private_key(bad)


def test_fingerprint_is_stable_and_does_not_leak_the_key(crypto):
    """Stable across every spelling of the same key, and HASH-derived rather
    than a slice of the key — a truncation would publish real key material."""
    import hashlib

    pk = "0x" + "ab" * 32
    fp = crypto.private_key_fingerprint(pk)
    assert len(fp) == 8
    # Same key, different spellings (upper case, "0X" prefix, no prefix).
    assert fp == crypto.private_key_fingerprint(pk.upper())
    assert fp == crypto.private_key_fingerprint("0X" + "AB" * 32)
    assert fp == crypto.private_key_fingerprint("ab" * 32)
    # Not a slice of the key: it is the tail of sha256(normalised).
    assert fp != pk[-8:]
    expected = hashlib.sha256(("0x" + "ab" * 32).encode()).hexdigest()[-8:]
    assert fp == expected


def test_different_keys_get_different_fingerprints(crypto):
    a = crypto.private_key_fingerprint("0x" + "ab" * 32)
    b = crypto.private_key_fingerprint("0x" + "cd" * 32)
    assert a != b


def test_webhook_secret_is_long_and_random(crypto):
    s1, s2 = crypto.generate_webhook_secret(), crypto.generate_webhook_secret()
    assert s1 != s2
    assert len(s1) == 64 and all(c in "0123456789abcdef" for c in s1)
