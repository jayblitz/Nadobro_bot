"""Builds the per-user data dict consumed by ``services.pnl_card.generate_pnl_card``.

This module is the **only** place that reads strategy/referral data from the
DB for the share card. Keeping it separate from the pure renderer means:

  * ``services.pnl_card`` stays a pure ``dict -> PNG`` function (easy to test,
    no DB dependency)
  * Per-user scoping lives in one auditable place — every query filters by
    ``telegram_id`` so users can never see another user's stats

Inputs come from:
  * ``strategy_sessions`` (filtered by ``user_id`` + ``network``) — volume,
    realized PnL, fees, strategy, product
  * ``invite_codes`` (filtered by ``referrer_user_id`` + ``network``) — the
    user's most recent active referral ``public_code``
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Optional

from src.nadobro.db import query_one

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Number formatters — output strings the renderer can paste directly.
# ---------------------------------------------------------------------------
def _to_decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (ValueError, ArithmeticError):
        return Decimal("0")


def _fmt_dollar(amount: Decimal) -> str:
    """Unsigned dollar amount with M/K shorthand above $1M / $1K.

    Matches the master card: e.g. ``$1.23M``, ``$1,234.50``, ``$0.00``.
    """
    abs_amt = abs(amount)
    if abs_amt >= Decimal("1000000"):
        return f"${(amount / Decimal('1000000')):,.2f}M"
    if abs_amt >= Decimal("1000"):
        return f"${amount:,.2f}"
    return f"${amount:,.2f}"


def _fmt_signed_dollar(amount: Decimal) -> str:
    """Always-signed dollar amount, e.g. ``+$234.56`` / ``-$12.34`` / ``+$0.00``.

    The renderer color-codes the value based on the leading sign char, so the
    sign must always be present.
    """
    if amount < 0:
        return f"-${abs(amount):,.2f}"
    return f"+${amount:,.2f}"


def _fmt_negative_dollar(amount: Decimal) -> str:
    """Fees are always shown as a negative cost on the card, even when the
    underlying value is stored as a positive number in the DB.
    """
    abs_amt = abs(amount)
    if abs_amt == 0:
        return "$0.00"
    return f"-${abs_amt:,.2f}"


# ---------------------------------------------------------------------------
# Symbol normalisation
# ---------------------------------------------------------------------------
def _format_symbol(product_name: Optional[str], strategy: Optional[str]) -> str:
    """``BTC`` / ``ETH`` → ``BTC-PERP``. ``MULTI`` → ``MULTI``. Falls through
    to ``BTC-PERP`` when nothing usable is set."""
    raw = (product_name or "").strip().upper()
    if not raw:
        return "BTC-PERP"
    if raw == "MULTI":
        return "MULTI"
    if "-" in raw:
        return raw  # already symbolised (e.g. BTC-PERP, ETH-USDC)
    # Volume Spot ships as KBTC/WETH/USDC — render as-is.
    if raw in {"KBTC", "WETH", "USDC", "USDT"}:
        return raw
    return f"{raw}-PERP"


# ---------------------------------------------------------------------------
# DB helpers — every query is scoped by user_id + network.
# ---------------------------------------------------------------------------
def _fetch_session(telegram_id: int, network: str, session_id: Optional[int]) -> dict:
    if session_id is not None:
        # Specific session — must belong to this user on this network.
        row = query_one(
            """
            SELECT *
            FROM strategy_sessions
            WHERE id = %s AND user_id = %s AND network = %s
            """,
            (int(session_id), int(telegram_id), network),
        )
        return dict(row or {})
    # Latest session for this user on this network. Prefer most-recently
    # completed session; fall back to any session if none have ended yet.
    row = query_one(
        """
        SELECT *
        FROM strategy_sessions
        WHERE user_id = %s AND network = %s
        ORDER BY COALESCE(stopped_at, started_at) DESC
        LIMIT 1
        """,
        (int(telegram_id), network),
    )
    return dict(row or {})


def _net_funding_usd(session: dict, network: str) -> Decimal:
    """Net funding RECEIVED (positive) on the session's perp over its lifetime,
    summed from the synced ``funding_payments_<network>`` feed (the data the
    adapter's funding endpoint produces). The indexer signs funding positive =
    *paid* by the user, so we negate to report received-positive. Scoped to the
    session's product_id and ``[started_at, stopped_at]`` window."""
    product_id = session.get("product_id")
    started_at = session.get("started_at")
    user_id = session.get("user_id")
    if product_id is None or started_at is None or user_id is None:
        return Decimal(0)
    table = "funding_payments_testnet" if str(network).lower() == "testnet" else "funding_payments_mainnet"
    try:
        row = query_one(
            f"""
            SELECT COALESCE(SUM(amount_x18), 0) AS paid_x18
            FROM {table}
            WHERE user_id = %s AND product_id = %s
              AND paid_at >= %s
              AND paid_at <= COALESCE(%s, now())
            """,
            (int(user_id), int(product_id), started_at, session.get("stopped_at")),
        )
    except Exception:  # noqa: BLE001 - funding is additive context, never fatal
        return Decimal(0)
    if not row:
        return Decimal(0)
    paid_x18 = _to_decimal(dict(row).get("paid_x18"))
    return -(paid_x18 / Decimal(10 ** 18))


def _fetch_active_referral_code(telegram_id: int, network: str) -> Optional[str]:
    """The user's most recent active referral ``public_code`` for this network.

    Returns ``None`` when the user hasn't unlocked / created a referral yet —
    the renderer omits the bottom-right line in that case.
    """
    row = query_one(
        """
        SELECT public_code
        FROM invite_codes
        WHERE code_type = 'referral'
          AND referrer_user_id = %s
          AND (network = %s OR (network IS NULL AND %s = 'testnet'))
          AND active = true
          AND revoked_at IS NULL
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (int(telegram_id), network, network),
    )
    if not row:
        return None
    code = (dict(row).get("public_code") or "").strip()
    return code or None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def build_pnl_card_data(
    telegram_id: int,
    network: str,
    session_id: Optional[int] = None,
) -> dict:
    """Return a dict ready for ``generate_pnl_card``.

    All numeric fields are derived from ``strategy_sessions`` rows owned by
    ``telegram_id`` on ``network``. The referral code comes from the user's
    own ``invite_codes`` row. If no session is found, the dict still renders
    a valid card with $0 values so the user gets a clear empty state instead
    of an error.

    Args:
        telegram_id: Owner of the card. **Must** be the requesting user —
            callers are responsible for not accepting this from untrusted
            input (e.g. URL params).
        network: ``"mainnet"`` / ``"testnet"`` — scopes the session lookup.
        session_id: Optional specific session. When omitted, uses the user's
            latest session.
    """
    session = _fetch_session(telegram_id, network, session_id)

    volume = _to_decimal(session.get("total_volume_usd"))
    pnl = _to_decimal(session.get("realized_pnl"))
    fees = _to_decimal(session.get("total_fees_paid"))
    product_name = session.get("product_name")
    strategy = session.get("strategy")

    # Delta Neutral profit IS the funding: price PnL nets ~0 across the hedged
    # legs, so fold net funding received into the displayed PnL. Scoped to DN so
    # other strategies (whose realized_pnl already reflects their economics)
    # aren't double-counted.
    if str(strategy or "").lower() in ("dn", "delta_neutral"):
        # DN-PNL-FEES fix: DN profit IS the funding captured, but DN fires four
        # taker MARKET legs per cycle, so fees can equal or exceed the funding.
        # The headline must be NET of fees — ``realized + funding - fees``.
        # Previously fees were shown only on a separate line, overstating DN
        # profit and sometimes flipping a true net loss into a displayed gain.
        pnl = pnl + _net_funding_usd(session, network) - fees

    referral_code = _fetch_active_referral_code(telegram_id, network)

    return {
        "symbol": _format_symbol(product_name, strategy),
        "strategy": str(strategy or "bro"),
        "volume": _fmt_dollar(volume),
        "net_fees": _fmt_negative_dollar(fees),
        "pnl": _fmt_signed_dollar(pnl),
        "referral_code": referral_code or "",
    }


def build_round_trip_card_data(
    telegram_id: int,
    network: str,
    round_trip_key: str,
) -> dict:
    """PnL card data for a History round-trip (manual / non-strategy).

    ``round_trip_key`` is the stable id minted by
    :func:`trade_service.compute_round_trips` (typically the close trade
    id). The card uses ``strategy="manual"`` so the renderer picks the
    neutral history skin.
    """
    from src.nadobro.trading.trade_service import find_round_trip

    rt = find_round_trip(int(telegram_id), network, str(round_trip_key))
    if not rt:
        return {
            "symbol": "MANUAL",
            "strategy": "manual",
            "volume": _fmt_dollar(Decimal("0")),
            "net_fees": _fmt_negative_dollar(Decimal("0")),
            "pnl": _fmt_signed_dollar(Decimal("0")),
            "referral_code": _fetch_active_referral_code(telegram_id, network) or "",
        }

    volume = _to_decimal(rt.get("volume_usd"))
    pnl = _to_decimal(rt.get("realized_pnl"))
    fees = _to_decimal(rt.get("fees"))
    product_name = rt.get("product_name") or rt.get("pair")
    referral_code = _fetch_active_referral_code(telegram_id, network)
    return {
        "symbol": _format_symbol(product_name, None),
        "strategy": "manual",
        "volume": _fmt_dollar(volume),
        "net_fees": _fmt_negative_dollar(fees),
        "pnl": _fmt_signed_dollar(pnl),
        "referral_code": referral_code or "",
    }
