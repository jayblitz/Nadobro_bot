"""Nado-style referral system for Nadobro.

Referral invite codes are earned from the user's own trading volume:
1 code per $10,000 in all-time volume, capped at 1000 codes. These codes grant
private access and link the new user to the direct referrer.
"""
from __future__ import annotations

import logging
import os
from math import floor

import psycopg2.extras

from src.nadobro.db import execute_returning, get_db, put_db, query_all, query_one
from src.nadobro.config import BOT_USERNAME
from src.nadobro.services.invite_service import _generate_plain_code, _hash_code, normalize_code

logger = logging.getLogger(__name__)

REFERRAL_VOLUME_PER_INVITE_USD = float(os.environ.get("REFERRAL_VOLUME_PER_INVITE_USD", "10000"))
REFERRAL_MAX_INVITE_CODES = int(os.environ.get("REFERRAL_MAX_INVITE_CODES", "1000"))
REFERRAL_LINK_PREFIX = "ref_"
VALID_REFERRAL_NETWORKS = frozenset({"mainnet", "testnet"})


def normalize_network(network: str | None) -> str:
    value = str(network or "mainnet").strip().lower()
    return value if value in VALID_REFERRAL_NETWORKS else "mainnet"


def earned_invite_allowance(total_volume_usd: float) -> int:
    volume = max(0.0, float(total_volume_usd or 0.0))
    if REFERRAL_VOLUME_PER_INVITE_USD <= 0:
        return 0
    return min(REFERRAL_MAX_INVITE_CODES, int(floor(volume / REFERRAL_VOLUME_PER_INVITE_USD)))


def normalize_referral_payload(payload: str) -> str:
    raw = str(payload or "").strip()
    if raw.lower().startswith(REFERRAL_LINK_PREFIX):
        raw = raw[len(REFERRAL_LINK_PREFIX):]
    return normalize_code(raw)


def referral_start_payload(code: str) -> str:
    return f"{REFERRAL_LINK_PREFIX}{normalize_referral_payload(code)}"


def bot_deep_link(code: str) -> str:
    bot_username = (BOT_USERNAME or "Nadbro_bot").lstrip("@")
    return f"https://t.me/{bot_username}?start={referral_start_payload(code)}"


def get_user_trade_volume_for_network(telegram_id: int, network: str) -> float:
    network = normalize_network(network)
    table = f"trades_{network}"
    row = query_one(
        f"""
        SELECT COALESCE(SUM(ABS(size) * COALESCE(NULLIF(price, 0), fill_price, 0)), 0) AS total
        FROM {table}
        WHERE user_id = %s
          AND status IN ('filled', 'closed')
        """,
        (int(telegram_id),),
    )
    return float((row or {}).get("total") or 0.0)


def _generated_code_count(telegram_id: int, network: str) -> int:
    network = normalize_network(network)
    row = query_one(
        """
        SELECT COUNT(*) AS count
        FROM invite_codes
        WHERE code_type = 'referral'
          AND referrer_user_id = %s
          AND (network = %s OR (network IS NULL AND %s = 'testnet'))
        """,
        (int(telegram_id), network, network),
    )
    return int((row or {}).get("count") or 0)


def _active_share_code(telegram_id: int, network: str) -> dict | None:
    network = normalize_network(network)
    return query_one(
        """
        SELECT id, public_code, code_prefix, network, redemption_count, max_redemptions, created_at
        FROM invite_codes
        WHERE code_type = 'referral'
          AND referrer_user_id = %s
          AND (network = %s OR (network IS NULL AND %s = 'testnet'))
          AND active = true
          AND revoked_at IS NULL
          AND redemption_count < max_redemptions
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (int(telegram_id), network, network),
    )


def generate_referral_invite_code(telegram_id: int, network: str = "mainnet") -> tuple[bool, str, dict | None]:
    network = normalize_network(network)
    user_row = query_one(
        "SELECT telegram_id FROM users WHERE telegram_id = %s",
        (int(telegram_id),),
    )
    if not user_row:
        return False, "User not found. Use /start first.", None

    total_volume = get_user_trade_volume_for_network(int(telegram_id), network)
    earned = earned_invite_allowance(total_volume)
    generated = _generated_code_count(int(telegram_id), network)
    if generated >= earned:
        needed = ((generated + 1) * REFERRAL_VOLUME_PER_INVITE_USD) - total_volume
        return (
            False,
            f"Referral codes unlock every ${REFERRAL_VOLUME_PER_INVITE_USD:,.0f} of your own {network} trading volume. "
            f"Trade about ${max(0.0, needed):,.2f} more to unlock the next one.",
            None,
        )

    sequence = generated + 1
    threshold = sequence * REFERRAL_VOLUME_PER_INVITE_USD
    attempts = 0
    while attempts < 10:
        attempts += 1
        code = _generate_plain_code()
        row = execute_returning(
            """
            INSERT INTO invite_codes
                (code_hash, public_code, code_type, code_prefix, created_by, referrer_user_id,
                 network, note, max_redemptions, earned_volume_threshold_usd, sequence_number)
            VALUES (%s, %s, 'referral', %s, %s, %s, %s, %s, 1, %s, %s)
            ON CONFLICT (code_hash) DO NOTHING
            RETURNING id, public_code, code_prefix, network, created_at, redemption_count, max_redemptions
            """,
            (
                _hash_code(code),
                code,
                code[:3],
                int(telegram_id),
                int(telegram_id),
                network,
                f"{network}_earned_volume_threshold=${threshold:,.0f}",
                threshold,
                sequence,
            ),
        )
        if row:
            row["link"] = bot_deep_link(row["public_code"])
            return True, "Referral invite code generated.", row

    return False, "Could not generate a unique referral code. Try again.", None


def ensure_share_code_for_user(telegram_id: int, network: str = "mainnet") -> tuple[dict | None, str | None]:
    network = normalize_network(network)
    row = _active_share_code(int(telegram_id), network)
    if row:
        row["link"] = bot_deep_link(row["public_code"])
        return row, None

    ok, msg, generated = generate_referral_invite_code(int(telegram_id), network=network)
    if ok:
        return generated, None
    return None, msg


def create_referral_from_invite(referrer_user_id: int, referred_user_id: int, invite_code_id: int, username: str | None) -> tuple[bool, str]:
    referrer = int(referrer_user_id)
    referred = int(referred_user_id)
    if referrer == referred:
        return False, "You cannot use your own referral invite code."

    row = execute_returning(
        """
        INSERT INTO referrals (referrer_user_id, referred_user_id, invite_code_id, referred_username)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (referred_user_id) DO NOTHING
        RETURNING id
        """,
        (referrer, referred, int(invite_code_id), username),
    )
    if not row:
        existing = query_one(
            "SELECT referrer_user_id FROM referrals WHERE referred_user_id = %s",
            (referred,),
        )
        if existing and int(existing.get("referrer_user_id")) == referrer:
            return True, "Referral already linked."
        return False, "This account is already linked to a referrer."
    return True, "Referral linked."


def record_referred_volume(
    referred_user_id: int,
    volume_usd: float,
    *,
    network: str = "mainnet",
    source: str = "trade_stats",
    increment_trade_count: bool = True,
) -> None:
    network = normalize_network(network)
    volume = max(0.0, float(volume_usd or 0.0))
    if volume <= 0:
        return
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM referrals WHERE referred_user_id = %s FOR UPDATE",
                (int(referred_user_id),),
            )
            referral = dict(cur.fetchone() or {})
            if not referral:
                conn.rollback()
                return

            cur.execute(
                """
                UPDATE referrals
                SET referred_volume_usd = referred_volume_usd + %s,
                    referred_trade_count = referred_trade_count + %s,
                    first_trade_at = COALESCE(first_trade_at, now()),
                    last_trade_at = now(),
                    updated_at = now()
                WHERE id = %s
                """,
                (volume, 1 if increment_trade_count else 0, referral["id"]),
            )
            cur.execute(
                """
                INSERT INTO referral_volume_events
                    (referral_id, referrer_user_id, referred_user_id, network, volume_usd, trade_count_delta, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    referral["id"],
                    referral["referrer_user_id"],
                    int(referred_user_id),
                    network,
                    volume,
                    1 if increment_trade_count else 0,
                    source,
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_db(conn)


def get_referral_dashboard(telegram_id: int, network: str = "mainnet") -> dict:
    network = normalize_network(network)
    user = query_one(
        "SELECT telegram_id FROM users WHERE telegram_id = %s",
        (int(telegram_id),),
    ) or {"telegram_id": int(telegram_id)}
    total_volume = get_user_trade_volume_for_network(int(telegram_id), network)
    earned = earned_invite_allowance(total_volume)
    generated = _generated_code_count(int(telegram_id), network)
    code = _active_share_code(int(telegram_id), network)
    if code:
        code["link"] = bot_deep_link(code["public_code"])
    warning = None

    stats = query_one(
        """
        SELECT COUNT(DISTINCT referrals.id) AS total_referrals,
               COALESCE(SUM(e.volume_usd), 0) AS total_referred_volume,
               COALESCE(SUM(e.trade_count_delta), 0) AS total_referred_trades
        FROM referrals
        LEFT JOIN referral_volume_events e
          ON e.referral_id = referrals.id
         AND e.network = %s
        WHERE referrals.referrer_user_id = %s
        """,
        (network, int(telegram_id)),
    ) or {}
    referred_users = query_all(
        """
        SELECT r.referred_user_id,
               COALESCE(u.telegram_username, r.referred_username, '') AS username,
               COALESCE(SUM(e.volume_usd), 0) AS referred_volume_usd,
               COALESCE(SUM(e.trade_count_delta), 0) AS referred_trade_count,
               MAX(e.created_at) AS last_trade_at,
               r.created_at
        FROM referrals r
        LEFT JOIN users u ON u.telegram_id = r.referred_user_id
        LEFT JOIN referral_volume_events e
          ON e.referral_id = r.id
         AND e.network = %s
        WHERE r.referrer_user_id = %s
        GROUP BY r.id, r.referred_user_id, u.telegram_username, r.referred_username, r.created_at
        ORDER BY COALESCE(SUM(e.volume_usd), 0) DESC, r.created_at DESC
        LIMIT 10
        """,
        (network, int(telegram_id)),
    )
    remaining = max(0, earned - generated)
    next_needed = 0.0 if earned >= REFERRAL_MAX_INVITE_CODES else max(
        0.0,
        ((generated + 1) * REFERRAL_VOLUME_PER_INVITE_USD) - total_volume,
    )
    if not code and remaining <= 0:
        warning = (
            f"Referral codes unlock every ${REFERRAL_VOLUME_PER_INVITE_USD:,.0f} of your own {network} trading volume. "
            f"Trade about ${next_needed:,.2f} more to unlock the next one."
        )
    elif not code and remaining > 0:
        warning = "You have an earned invite available. Tap Generate Invite Code to create it."
    return {
        "telegram_id": int(telegram_id),
        "network": network,
        "own_volume_usd": total_volume,
        "earned_codes": earned,
        "generated_codes": generated,
        "remaining_codes": remaining,
        "max_codes": REFERRAL_MAX_INVITE_CODES,
        "volume_per_code": REFERRAL_VOLUME_PER_INVITE_USD,
        "next_code_volume_needed": next_needed,
        "share_code": code,
        "warning": warning,
        "total_referrals": int(stats.get("total_referrals") or 0),
        "total_referred_volume": float(stats.get("total_referred_volume") or 0.0),
        "total_referred_trades": int(stats.get("total_referred_trades") or 0),
        "referred_users": referred_users or [],
    }
