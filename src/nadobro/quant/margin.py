"""Isolated-margin sizing — the single source of truth for how much margin to
post on an isolated-only market.

Both the manual trade path (``services/trade_service``) and the engine adapter
(``engine/adapter/nado``) must agree on this number, so the math lives here and
both import it. Keeping one implementation avoids the failure mode where the
two paths drift and the engine under-margins an order the manual path would
have sized correctly (venue error_code 2006, "account health below
threshold").

Dependency-free on purpose: only the stdlib, so the engine adapter can import
it without pulling in the venue client.
"""
from __future__ import annotations

import os
from typing import Optional


def isolated_margin_safety_multiplier() -> float:
    """Headroom multiplier applied on top of the bare initial-margin requirement.

    ``isolated_margin = notional / leverage`` is the venue's exact initial-margin
    floor. Signing exactly that leaves zero room for fees, the next price tick,
    or the cross-account's simultaneous reservations across other resting
    quotes, so isolated-only markets (equities, commodities, RWAs) reject with
    error_code 2006 ("account health below threshold") almost immediately.
    Default 1.20 covers ~1.5 bp taker fee + 1 tick + a small maintenance
    cushion; override via ``NADO_ISOLATED_MARGIN_SAFETY`` for tuning.
    """
    raw = (os.environ.get("NADO_ISOLATED_MARGIN_SAFETY") or "1.20").strip()
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return 1.20
    return max(1.0, val)


def compute_isolated_margin(size: float, price: float, leverage: float) -> Optional[float]:
    """Initial-margin requirement plus the safety buffer, or ``None`` on bad
    inputs. At ``leverage == 1`` this is ``notional * safety`` — i.e. the leg is
    (over-)fully collateralized, which is what the Delta Neutral short leg wants.
    """
    try:
        size_f = abs(float(size))
        price_f = float(price)
        lev_f = float(leverage)
    except (TypeError, ValueError):
        return None
    if size_f <= 0 or price_f <= 0 or lev_f <= 0:
        return None
    return (size_f * price_f) / max(1.0, lev_f) * isolated_margin_safety_multiplier()
