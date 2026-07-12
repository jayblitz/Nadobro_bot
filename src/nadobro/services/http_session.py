"""Hardened HTTP session for Nado REST + archive calls.

Why this module exists
======================

The bot talks to several Nado endpoints through ``requests`` (``nado_client``,
``product_catalog``, ``nado_archive``). Recently Nado's edge has been routing
unauthenticated requests through a Cloudflare challenge that returns:

    HTTP/1.1 403 Forbidden
    Content-Type: text/html; charset=UTF-8
    <body: Cloudflare "Just a moment..." interstitial>

A naive client just sees a non-JSON 403, fails the call, and the caller
retries instantly. The result is a *log/throughput storm* that worsens the
challenge instead of resolving it.

This module centralizes three defenses:

1. **Browser-like default headers** on the shared ``Session`` so simple
   anti-bot heuristics let our (legitimate) traffic through.
2. **Backoff-with-jitter retries** that look at the *response body* — not
   just the status code — so we can specifically re-try on Cloudflare
   challenges without retrying on real 4xx errors from the venue.
3. **Per-host circuit breaker** that opens after N consecutive challenges
   within W seconds and short-circuits subsequent calls for a cooldown
   window. Callers see ``None`` (treated as "data unavailable, use cache")
   instead of hammering the edge.

A dedup'd warning log fires once per minute per host so operators see the
state without log spam.
"""
from __future__ import annotations

import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter

from src.nadobro.utils.env import env_float, env_int
from src.nadobro.services.ipv4_egress import install_ipv4_only_resolver

logger = logging.getLogger(__name__)

# Before building the shared Session, pin urllib3 to AF_INET when configured.
# Covers REST, archive, product_catalog, and nado_protocol SDK sessions.
install_ipv4_only_resolver()


# ---------------------------------------------------------------------------
# Tunables (all overridable via env so prod can react without a redeploy).
# ---------------------------------------------------------------------------
_HTTP_POOL_CONNECTIONS = env_int("NADO_HTTP_POOL_CONNECTIONS", 64)
_HTTP_POOL_MAXSIZE = env_int("NADO_HTTP_POOL_MAXSIZE", 64)
_CF_RETRY_MAX = env_int("NADO_CF_RETRY_MAX", 2)
_CF_RETRY_BASE_SECONDS = env_float("NADO_CF_RETRY_BASE_SECONDS", 0.5)
_CF_RETRY_JITTER_SECONDS = env_float("NADO_CF_RETRY_JITTER_SECONDS", 0.4)

# Circuit breaker: if we see this many CF challenges in this window, freeze
# outbound traffic to that host for the cooldown.
_CF_BREAKER_THRESHOLD = env_int("NADO_CF_BREAKER_THRESHOLD", 8)
_CF_BREAKER_WINDOW_SECONDS = env_float("NADO_CF_BREAKER_WINDOW_SECONDS", 10.0)
_CF_BREAKER_COOLDOWN_SECONDS = env_float("NADO_CF_BREAKER_COOLDOWN_SECONDS", 30.0)

# Log dedup: only emit one Cloudflare-challenge warning per host per window.
_CF_LOG_THROTTLE_SECONDS = env_float("NADO_CF_LOG_THROTTLE_SECONDS", 60.0)

# Per-host **weight** token bucket: models Nado's documented per-IP query
# budget of 2400 weight/min == 400 weight/10s == **40 weight/s, burst 400**
# (https://docs.nado.xyz/developer-resources/api/rate-limits). Tokens are
# *weight*, not requests — callers pass the documented weight of each call as
# ``cost`` (see ``nado_weights``). Gateway and archive are distinct netlocs, so
# each gets its own bucket and its own 2400/min IP budget automatically.
# Defaults carry a ~10% safety margin (36/360) to absorb clock drift and the
# per-minute-vs-per-10s nuance; raise toward 40/400 or lower via env in prod.
_HTTP_RPS_PER_HOST = env_float("NADO_HTTP_RPS_PER_HOST", 36.0)
_HTTP_BURST_PER_HOST = env_float("NADO_HTTP_BURST_PER_HOST", 360.0)
_HTTP_BUCKET_MAX_WAIT_SECONDS = env_float("NADO_HTTP_BUCKET_MAX_WAIT_SECONDS", 2.5)


# ---------------------------------------------------------------------------
# Shared session.
# ---------------------------------------------------------------------------
def _build_session() -> requests.Session:
    sess = requests.Session()
    # Look like a real desktop Chrome request. Cloudflare's lightweight bot
    # check ignores well-formed User-Agent + Accept-Language combos.
    sess.headers.update(
        {
            "User-Agent": os.environ.get(
                "NADO_HTTP_USER_AGENT",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36",
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Origin": "https://app.nado.xyz",
            "Referer": "https://app.nado.xyz/",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    adapter = HTTPAdapter(
        pool_connections=max(8, _HTTP_POOL_CONNECTIONS),
        pool_maxsize=max(8, _HTTP_POOL_MAXSIZE),
    )
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


SESSION: requests.Session = _build_session()


# ---------------------------------------------------------------------------
# Cloudflare-challenge detection + circuit breaker.
# ---------------------------------------------------------------------------
@dataclass
class _BreakerState:
    failures: list[float]
    open_until: float


_breaker_lock = threading.RLock()
_breaker: dict[str, _BreakerState] = {}
_log_last_emit: dict[str, float] = {}


# ---------------------------------------------------------------------------
# Per-host token bucket (one bucket per netloc).
# ---------------------------------------------------------------------------
@dataclass
class _BucketState:
    tokens: float
    last_refill: float


_bucket_lock = threading.Lock()
_buckets: dict[str, _BucketState] = {}


def _acquire_token(host: str, *, cost: float = 1.0, max_wait: float | None = None) -> bool:
    """Reserve ``cost`` weight tokens for ``host`` or time out after ``max_wait``.

    ``cost`` is the documented Nado *weight* of the call (default 1). Returns
    True if the weight was reserved, False if we timed out. Callers that receive
    False should treat the request as throttled (skip rather than pile on).
    Pure-Python sleep loop; safe to invoke from threads inside
    ``ThreadPoolExecutor``.
    """
    if not host or _HTTP_RPS_PER_HOST <= 0:
        return True
    # A single call can never cost more than the burst ceiling, otherwise the
    # bucket could never accumulate enough tokens and we'd deadlock until the
    # deadline. Clamp (burst 360 >> any documented single-call weight ~50).
    need = max(1.0, min(float(cost), _HTTP_BURST_PER_HOST))
    budget = float(max_wait if max_wait is not None else _HTTP_BUCKET_MAX_WAIT_SECONDS)
    deadline = time.monotonic() + budget
    while True:
        with _bucket_lock:
            now = time.monotonic()
            state = _buckets.get(host)
            if state is None:
                state = _BucketState(tokens=_HTTP_BURST_PER_HOST, last_refill=now)
                _buckets[host] = state
            elapsed = max(0.0, now - state.last_refill)
            state.tokens = min(_HTTP_BURST_PER_HOST, state.tokens + elapsed * _HTTP_RPS_PER_HOST)
            state.last_refill = now
            if state.tokens >= need:
                state.tokens -= need
                return True
            wait = (need - state.tokens) / _HTTP_RPS_PER_HOST
        if time.monotonic() + wait > deadline:
            return False
        time.sleep(min(wait, 0.25))


def bucket_snapshot() -> dict[str, dict[str, float]]:
    """Diagnostic snapshot of per-host token buckets."""
    with _bucket_lock:
        return {
            host: {"tokens": state.tokens, "last_refill": state.last_refill}
            for host, state in _buckets.items()
        }


def throttle_host(url: str, *, cost: float = 1.0, max_wait: float | None = None) -> bool:
    """Public weight token-bucket gate. Callers (NadoClient SDK + REST paths)
    invoke this before each call so SDK and REST share one shaping bucket per
    host. ``cost`` is the documented Nado weight of the call."""
    return _acquire_token(_host(url), cost=cost, max_wait=max_wait)


def _host(url: str) -> str:
    try:
        return urlsplit(url).netloc.lower()
    except Exception:
        return ""


def _is_cloudflare_challenge(resp: Optional[requests.Response]) -> bool:
    """Heuristic: 403 + HTML body containing the Cloudflare challenge marker."""
    if resp is None:
        return False
    if resp.status_code != 403:
        return False
    ctype = (resp.headers.get("content-type") or "").lower()
    if "text/html" not in ctype:
        return False
    try:
        body = (resp.text or "")[:512].lower()
    except Exception:
        body = ""
    return "just a moment" in body or "cf-chl-" in body or "cloudflare" in body


def is_circuit_open(url: str) -> bool:
    host = _host(url)
    if not host:
        return False
    with _breaker_lock:
        state = _breaker.get(host)
        if not state:
            return False
        return time.time() < state.open_until


def _record_challenge(host: str) -> None:
    if not host:
        return
    now = time.time()
    with _breaker_lock:
        state = _breaker.get(host)
        if state is None:
            state = _BreakerState(failures=[], open_until=0.0)
            _breaker[host] = state
        cutoff = now - _CF_BREAKER_WINDOW_SECONDS
        state.failures = [t for t in state.failures if t >= cutoff]
        state.failures.append(now)
        if len(state.failures) >= _CF_BREAKER_THRESHOLD and now >= state.open_until:
            state.open_until = now + _CF_BREAKER_COOLDOWN_SECONDS
            logger.warning(
                "Cloudflare circuit OPEN for host=%s for %.0fs after %d challenges in %.0fs",
                host, _CF_BREAKER_COOLDOWN_SECONDS, len(state.failures), _CF_BREAKER_WINDOW_SECONDS,
            )


def _record_success(host: str) -> None:
    if not host:
        return
    with _breaker_lock:
        state = _breaker.get(host)
        if state is not None and state.failures:
            state.failures.clear()


def _log_cf_warning(url: str, status: int, snippet: str) -> None:
    host = _host(url) or "?"
    now = time.time()
    last = _log_last_emit.get(host, 0.0)
    if now - last < _CF_LOG_THROTTLE_SECONDS:
        return
    _log_last_emit[host] = now
    logger.warning(
        "Cloudflare challenge from %s status=%s (suppressing repeats for %.0fs); body=%r",
        host, status, _CF_LOG_THROTTLE_SECONDS, snippet[:160],
    )


def _gateway_blocked(url: str) -> bool:
    if is_circuit_open(url):
        return True
    try:
        from src.nadobro.services.gateway_budget import is_gateway_rate_limited
        return is_gateway_rate_limited(url)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Public request helper.
# ---------------------------------------------------------------------------
def cf_request(
    method: str,
    url: str,
    *,
    timeout: float,
    params: dict | None = None,
    json_body: dict | None = None,
    headers: dict | None = None,
    cost: float = 1.0,
) -> Optional[requests.Response]:
    """Perform an HTTP request through the shared session with Cloudflare-aware
    retries and a per-host circuit breaker.

    ``cost`` is the documented Nado weight of the call (default 1) charged
    against the per-host weight bucket. Returns the final ``Response`` (which
    the caller can ``.json()``) or ``None`` when the circuit is open or every
    retry was challenged.
    """
    if _gateway_blocked(url):
        return None
    host = _host(url)
    if not _acquire_token(host, cost=cost):
        logger.debug("cf_request throttled host=%s (token bucket starved, cost=%s)", host, cost)
        return None
    attempts = max(1, _CF_RETRY_MAX + 1)
    last_resp: Optional[requests.Response] = None
    for attempt in range(attempts):
        try:
            if method.upper() == "POST":
                resp = SESSION.post(url, params=params, json=json_body, headers=headers, timeout=timeout)
            else:
                resp = SESSION.get(url, params=params, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            logger.debug("cf_request transport error host=%s attempt=%d: %s", host, attempt, exc)
            return None
        last_resp = resp
        if not _is_cloudflare_challenge(resp):
            _record_success(host)
            return resp
        _record_challenge(host)
        try:
            snippet = (resp.text or "").strip().replace("\n", " ")[:160]
        except Exception:
            snippet = ""
        _log_cf_warning(url, resp.status_code, snippet)
        if attempt + 1 >= attempts:
            break
        delay = _CF_RETRY_BASE_SECONDS * (2 ** attempt) + random.uniform(0, _CF_RETRY_JITTER_SECONDS)
        time.sleep(delay)
    return last_resp


def cf_get(url: str, *, timeout: float, params: dict | None = None, headers: dict | None = None, cost: float = 1.0) -> Optional[requests.Response]:
    return cf_request("GET", url, timeout=timeout, params=params, headers=headers, cost=cost)


def cf_post(url: str, *, timeout: float, json_body: dict | None = None, headers: dict | None = None, cost: float = 1.0) -> Optional[requests.Response]:
    return cf_request("POST", url, timeout=timeout, json_body=json_body, headers=headers, cost=cost)


def breaker_snapshot() -> dict[str, dict[str, float | int]]:
    """Diagnostic snapshot; safe to call from monitoring endpoints."""
    with _breaker_lock:
        now = time.time()
        out: dict[str, dict[str, float | int]] = {}
        for host, state in _breaker.items():
            out[host] = {
                "open": 1 if now < state.open_until else 0,
                "open_until": state.open_until,
                "recent_failures": len(state.failures),
            }
        return out
