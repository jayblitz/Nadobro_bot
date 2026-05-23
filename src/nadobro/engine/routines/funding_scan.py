"""Funding-scan routine — surveys perp funding rates across pairs.

Live funding comes from the Nado venue, but the routine stays venue-agnostic:
callers pass a ``rate_provider`` (async or sync callable ``pair -> rate``);
the Phase 4 controllers wire the engine adapter in. Deterministic given the
provider.

Implemented in Phase 4.
"""
from __future__ import annotations

import inspect
from decimal import Decimal
from typing import Awaitable, Callable, Dict, Optional, Sequence, Union

RateProvider = Callable[[str], Union[Optional[Decimal], Awaitable[Optional[Decimal]]]]


async def run(pairs: Sequence[str], rate_provider: RateProvider) -> Dict[str, Decimal]:
    out: Dict[str, Decimal] = {}
    for pair in pairs:
        rate = rate_provider(pair)
        if inspect.isawaitable(rate):
            rate = await rate
        if rate is not None:
            out[pair] = Decimal(str(rate))
    return out


def most_negative(rates: Dict[str, Decimal]) -> Optional[str]:
    """Pair paying longs the most (most negative funding) — best long-carry."""
    return min(rates, key=lambda p: rates[p]) if rates else None


def most_positive(rates: Dict[str, Decimal]) -> Optional[str]:
    """Pair paying shorts the most (most positive funding) — best short-carry."""
    return max(rates, key=lambda p: rates[p]) if rates else None
