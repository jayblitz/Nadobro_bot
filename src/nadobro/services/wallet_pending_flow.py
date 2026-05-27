"""Persist in-progress wallet linking across webhook workers and restarts."""

from __future__ import annotations

import base64
import logging
import time

from src.nadobro.db import execute
from src.nadobro.models.database import get_bot_state, set_bot_state
from src.nadobro.services.crypto import decrypt_with_server_key, encrypt_with_server_key

logger = logging.getLogger(__name__)

WALLET_PENDING_TTL_SECONDS = 3600


def _key(telegram_user_id: int) -> str:
    return f"wallet_pending_flow:{int(telegram_user_id)}"


def persist_wallet_pending_flow(
    telegram_user_id: int,
    *,
    flow: str,
    pk_hex: str,
    linked_signer_address: str,
) -> None:
    pk = str(pk_hex or "").strip()
    addr = str(linked_signer_address or "").strip()
    if not flow or not pk or not addr:
        logger.warning("Refusing to persist empty wallet_pending_flow uid=%s", telegram_user_id)
        return
    ciphertext = encrypt_with_server_key(pk.encode("utf-8"))
    set_bot_state(
        _key(telegram_user_id),
        {
            "flow": str(flow),
            "encrypted_pk_b64": base64.b64encode(ciphertext).decode("ascii"),
            "linked_signer_address": addr,
            "_ts": time.time(),
        },
    )


def load_wallet_pending_flow(telegram_user_id: int) -> dict | None:
    row = get_bot_state(_key(telegram_user_id))
    if not row or not isinstance(row, dict):
        return None
    ts = float(row.get("_ts") or row.get("ts") or 0)
    if ts and time.time() - ts > WALLET_PENDING_TTL_SECONDS:
        clear_wallet_pending_flow(telegram_user_id)
        return None
    enc_b64 = row.get("encrypted_pk_b64")
    linked_addr = str(row.get("linked_signer_address") or "").strip()
    flow = str(row.get("flow") or "").strip()
    if not enc_b64 or not linked_addr or not flow:
        return None
    try:
        pk_hex = decrypt_with_server_key(base64.b64decode(str(enc_b64))).decode("utf-8")
    except Exception:
        logger.warning("Failed to decrypt wallet_pending_flow uid=%s", telegram_user_id)
        clear_wallet_pending_flow(telegram_user_id)
        return None
    return {
        "flow": flow,
        "pk_hex": pk_hex,
        "linked_signer_address": linked_addr,
    }


def clear_wallet_pending_flow(telegram_user_id: int) -> None:
    execute("DELETE FROM bot_state WHERE key = %s", (_key(telegram_user_id),))
