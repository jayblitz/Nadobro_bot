"""Pepper-hashing utilities for referral codes.

The private-alpha invite gate was removed in May 2026 (the bot is now open to
everyone). Only the cryptographic helpers used by ``referral_service`` to hash
public referral codes survive in this module so that one stored
``code_hash`` row cannot be forged without the deployment-time secret.
"""
from __future__ import annotations

import hashlib
import logging
import os
import secrets
import string

from src.nadobro.config import ENCRYPTION_KEY

logger = logging.getLogger(__name__)

INVITE_CODE_LENGTH = 8
INVITE_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


def normalize_code(code: str) -> str:
    return "".join(
        ch for ch in str(code or "").upper() if ch in string.ascii_uppercase + string.digits
    )


def _invite_pepper() -> str:
    pepper = os.environ.get("INVITE_CODE_PEPPER") or ENCRYPTION_KEY
    if pepper:
        return pepper
    if os.environ.get("NADOBRO_ALLOW_DEV_INVITE_PEPPER", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        logger.warning(
            "INVITE_CODE_PEPPER and ENCRYPTION_KEY are unset; using development "
            "invite-code pepper because NADOBRO_ALLOW_DEV_INVITE_PEPPER is set "
            "(do NOT enable this in production)."
        )
        return "nadobro-dev-invite-pepper"
    raise RuntimeError(
        "INVITE_CODE_PEPPER and ENCRYPTION_KEY are unset. Refusing to hash "
        "referral codes with a hardcoded pepper. Set ENCRYPTION_KEY (or "
        "INVITE_CODE_PEPPER) before continuing, or set "
        "NADOBRO_ALLOW_DEV_INVITE_PEPPER=true for local dev only."
    )


def _hash_code(code: str) -> str:
    normalized = normalize_code(code)
    return hashlib.sha256(f"{normalized}:{_invite_pepper()}".encode("utf-8")).hexdigest()


def _generate_plain_code() -> str:
    return "".join(secrets.choice(INVITE_CODE_ALPHABET) for _ in range(INVITE_CODE_LENGTH))
