"""Phase 4 reliability: bounded retry-with-backoff for transient venue errors.

Used by ``mm_bot.run_cycle`` around ``client.get_market_price`` and
``client.get_open_orders`` so a transient 429 / network blip from the Nado
gateway doesn't blow up an entire cycle.

Kept intentionally tiny — execution_queue.py already owns queue/worker pooling;
this module owns request-level retry semantics.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Optional, Tuple

logger = logging.getLogger(__name__)


# Substrings that mark an error as transient and worth retrying. Keep generic so
# both REST 429s and SDK-level rate-limit messages get caught.
_RATE_LIMIT_HINTS: tuple[str, ...] = (
    "429",
    "rate limit",
    "rate-limit",
    "ratelimit",
    "too many requests",
    "throttle",
)
# Generic transient network failures we also retry (one extra attempt). Keep
# narrow so we don't paper over real config / auth failures.
_TRANSIENT_HINTS: tuple[str, ...] = (
    "timeout",
    "timed out",
    "temporarily unavailable",
    "connection reset",
    "connection aborted",
    "service unavailable",
    "503",
    "504",
    "bad gateway",
    "502",
)


def is_rate_limit_error(exc: BaseException | str) -> bool:
    msg = str(exc).lower()
    return any(hint in msg for hint in _RATE_LIMIT_HINTS)


def is_transient_error(exc: BaseException | str) -> bool:
    msg = str(exc).lower()
    if is_rate_limit_error(msg):
        return True
    return any(hint in msg for hint in _TRANSIENT_HINTS)


def call_with_retry(
    fn: Callable[..., Any],
    *args,
    max_retries: int = 2,
    base_delay: float = 0.3,
    max_delay: float = 2.0,
    is_empty_result: Optional[Callable[[Any], bool]] = None,
    label: str = "",
    **kwargs,
) -> Tuple[Any, list[str]]:
    """Call ``fn(*args, **kwargs)``, retrying on transient errors / empty results.

    ``is_empty_result`` — optional predicate that flags a successful return as
    "empty enough to retry" (e.g. ``mid <= 0`` for ``get_market_price``).
    Treats an empty result as a soft transient failure for one extra attempt
    rather than re-raising.

    Returns ``(value, errors)`` where ``errors`` is a list of stringified
    failures observed during retries — caller can stash for the dashboard.
    """
    errors: list[str] = []
    last_value: Any = None
    attempts = max(1, int(max_retries) + 1)
    fn_label = label or getattr(fn, "__name__", "callable")
    for attempt in range(attempts):
        try:
            value = fn(*args, **kwargs)
        except Exception as exc:  # pragma: no cover - hardened against any client surface
            text = f"{fn_label}: {exc}"
            errors.append(text)
            if not is_transient_error(exc) or attempt == attempts - 1:
                logger.warning("call_with_retry exhausted on %s: %s", fn_label, exc)
                raise
            sleep_for = min(max_delay, base_delay * (2 ** attempt))
            logger.info(
                "call_with_retry transient error on %s (attempt %d/%d), sleeping %.2fs: %s",
                fn_label,
                attempt + 1,
                attempts,
                sleep_for,
                exc,
            )
            time.sleep(sleep_for)
            continue
        last_value = value
        if is_empty_result is not None and is_empty_result(value):
            errors.append(f"{fn_label}: empty result on attempt {attempt + 1}")
            if attempt == attempts - 1:
                break
            sleep_for = min(max_delay, base_delay * (2 ** attempt))
            time.sleep(sleep_for)
            continue
        return value, errors
    return last_value, errors


def market_price_is_empty(value: Any) -> bool:
    """``get_market_price`` returns ``{"bid":0,"ask":0,"mid":0}`` on its own
    swallowed exceptions. Treat that as an empty/transient response so the
    retry helper can take another swing."""
    if not isinstance(value, dict):
        return True
    try:
        return float(value.get("mid") or 0.0) <= 0.0
    except (TypeError, ValueError):
        return True


def open_orders_is_empty(value: Any) -> bool:
    """``get_open_orders`` returns a list (possibly empty). Empty is a *valid*
    response — the bot has no open orders. We do NOT retry on that (an empty
    list is a fact, not a transient error). Helper kept for symmetry / future
    use; currently always returns False.

    .. caution::

       The ``call_with_retry`` wrapper around ``client.get_open_orders``
       provides limited protection in practice because
       ``NadoClient.get_open_orders`` swallows every internal exception (SDK
       and REST paths both ``except Exception``) and *caches the empty list*
       for the cache TTL on failure. A 429 mid-cycle therefore produces an
       empty list that bypasses our retry wrapper entirely, and subsequent
       calls within the cache TTL see the cached empty result.

       Mitigation in mm_bot: the post-cancel refresh fetch passes
       ``refresh=True`` to bypass the open-orders cache after we just
       cancelled stale orders. A deeper fix (raise on transient errors instead
       of caching empties) is tracked as a follow-up against
       ``services/nado_client.py:get_open_orders``.
    """
    return False
