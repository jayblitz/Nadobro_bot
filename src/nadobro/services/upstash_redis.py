"""Upstash Redis REST client (thin wrapper).

Why this module exists
======================

The bot previously relied on **process-local** dicts for caches like
``_linked_signer_cache`` and ``_balance_cache``. That has two failure modes
which bit us hard on 2026-05-27:

1. A transient ``Too Many Requests`` from Nado on ``query_subaccount_info``
   flips the in-memory state to ``{"exists": False, "balances": {}}`` and the
   bot tells the user "1CT not linked" — even though the on-chain signer is
   verified. With a Redis layer we keep the last *successful* read for ~30s
   and serve it on transient query failures.

2. The ``portfolio_sync`` apscheduler job ran past its 30s interval and
   APScheduler skipped subsequent ticks because ``max_instances=1`` is
   already enforced in-process. With a Redis-backed lock we can additionally
   guarantee that a slow tick on one machine doesn't pile up work on another
   (forward-looking — Fly currently runs a single machine, but we plan to
   scale this).

Design notes
============

* **No new pip deps.** Uses ``requests`` (already pinned in
  requirements.txt). Upstash REST is just JSON over HTTPS with a bearer
  token.
* **Fail-safe.** If env vars are missing or the network is unreachable, all
  ops return ``None`` / ``False`` and callers fall through to their previous
  behaviour (in-memory cache + live query). Redis is a strict cache, never
  a source of truth.
* **Short timeouts** (``UPSTASH_REDIS_TIMEOUT_SECONDS``, default 1.5s) so
  Redis hiccups never delay an order placement.

Env vars consumed:
  - ``UPSTASH_REDIS_REST_URL``     (e.g. https://your-db.upstash.io)
  - ``UPSTASH_REDIS_REST_TOKEN``   (Bearer token from Upstash dashboard)
  - ``UPSTASH_REDIS_TIMEOUT_SECONDS`` (optional, default 1.5)
  - ``UPSTASH_REDIS_KEY_PREFIX``   (optional, default "nadobro:")
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from typing import Any, Iterable, Optional

import requests

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_SECONDS = float(
    os.environ.get("UPSTASH_REDIS_TIMEOUT_SECONDS", "1.5") or "1.5"
)
_KEY_PREFIX = os.environ.get("UPSTASH_REDIS_KEY_PREFIX", "nadobro:") or "nadobro:"

# Throttle "Upstash unreachable" warnings so a real outage doesn't fill the
# log; we only need to know it's degraded, not log every miss.
_warn_lock = threading.Lock()
_last_warn_ts: float = 0.0
_WARN_INTERVAL_SECONDS = 60.0


def _warn_once(msg: str) -> None:
    global _last_warn_ts
    now = time.time()
    with _warn_lock:
        if now - _last_warn_ts < _WARN_INTERVAL_SECONDS:
            return
        _last_warn_ts = now
    logger.warning(msg)


class UpstashRedis:
    """Minimal Upstash REST client. Methods return ``None``/``False`` on any
    error so callers can safely treat a Redis outage as a cache miss."""

    def __init__(
        self,
        url: Optional[str] = None,
        token: Optional[str] = None,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        key_prefix: str = _KEY_PREFIX,
    ) -> None:
        self.url = (url or os.environ.get("UPSTASH_REDIS_REST_URL") or "").rstrip("/")
        self.token = token or os.environ.get("UPSTASH_REDIS_REST_TOKEN") or ""
        self.timeout = timeout_seconds
        self.key_prefix = key_prefix
        self._session: Optional[requests.Session] = None

    @property
    def enabled(self) -> bool:
        return bool(self.url and self.token)

    def _get_session(self) -> requests.Session:
        if self._session is None:
            s = requests.Session()
            s.headers.update(
                {
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
            )
            self._session = s
        return self._session

    def _full_key(self, key: str) -> str:
        return f"{self.key_prefix}{key}"

    def _post(self, command: Iterable[Any]) -> Optional[Any]:
        """Send a single Redis command via Upstash REST.

        Returns the ``result`` field on success, or ``None`` on any error
        (including network failure, non-2xx, or Redis-level error). All
        errors are logged at most once per minute to avoid log spam.
        """
        if not self.enabled:
            return None
        try:
            # Upstash REST expects commands as an array of args.
            payload = [str(part) for part in command]
            resp = self._get_session().post(self.url, data=json.dumps(payload), timeout=self.timeout)
            if resp.status_code != 200:
                _warn_once(
                    f"Upstash REST status={resp.status_code} cmd={payload[0]} body={resp.text[:120]!r}"
                )
                return None
            data = resp.json()
            if isinstance(data, dict) and "error" in data:
                _warn_once(f"Upstash REST error cmd={payload[0]}: {data['error']}")
                return None
            if isinstance(data, dict):
                return data.get("result")
            return data
        except (requests.RequestException, ValueError) as e:
            _warn_once(f"Upstash REST request failed cmd={list(command)[:1]}: {e}")
            return None
        except Exception as e:  # pragma: no cover - last-resort safety net
            _warn_once(f"Upstash REST unexpected failure: {e}")
            return None

    # ---- Primitive ops ----------------------------------------------------

    def get(self, key: str) -> Optional[str]:
        return self._post(["GET", self._full_key(key)])

    def set(self, key: str, value: str, ttl_seconds: Optional[int] = None) -> bool:
        cmd = ["SET", self._full_key(key), value]
        if ttl_seconds is not None and ttl_seconds > 0:
            cmd.extend(["EX", str(int(ttl_seconds))])
        return self._post(cmd) == "OK"

    def delete(self, key: str) -> bool:
        result = self._post(["DEL", self._full_key(key)])
        try:
            return int(result or 0) > 0
        except (TypeError, ValueError):
            return False

    def setnx(self, key: str, value: str, ttl_seconds: Optional[int] = None) -> bool:
        """SET key value NX [EX ttl] — returns True if we acquired the key."""
        cmd = ["SET", self._full_key(key), value, "NX"]
        if ttl_seconds is not None and ttl_seconds > 0:
            cmd.extend(["EX", str(int(ttl_seconds))])
        return self._post(cmd) == "OK"

    # ---- JSON helpers -----------------------------------------------------

    def get_json(self, key: str) -> Optional[Any]:
        raw = self.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return None

    def set_json(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> bool:
        try:
            encoded = json.dumps(value, default=str, separators=(",", ":"))
        except (TypeError, ValueError):
            return False
        return self.set(key, encoded, ttl_seconds=ttl_seconds)


# Module-level singleton — cheap to construct, but callers shouldn't pay
# import overhead per invocation.
_default: Optional[UpstashRedis] = None
_default_lock = threading.Lock()


def get_redis() -> UpstashRedis:
    global _default
    if _default is None:
        with _default_lock:
            if _default is None:
                _default = UpstashRedis()
                if _default.enabled:
                    logger.info(
                        "Upstash Redis enabled url=%s prefix=%s timeout=%.2fs",
                        _default.url.split("//", 1)[-1].split(".", 1)[0] + ".upstash.io",
                        _default.key_prefix,
                        _default.timeout,
                    )
                else:
                    logger.info(
                        "Upstash Redis not configured "
                        "(UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN); "
                        "caches will use in-memory fallback only."
                    )
    return _default


class RedisLock:
    """Tiny distributed lock built on SETNX + EX.

    Usage::

        with RedisLock("portfolio_sync", ttl_seconds=55) as got:
            if not got:
                return
            ...

    The lock auto-expires after ``ttl_seconds``; we deliberately don't
    auto-extend because the caller knows its own expected duration. If the
    holder crashes, the next tick simply acquires after the TTL.
    """

    def __init__(self, name: str, ttl_seconds: int = 30, redis: Optional[UpstashRedis] = None) -> None:
        self.name = f"lock:{name}"
        self.ttl_seconds = ttl_seconds
        self.redis = redis or get_redis()
        self._token = uuid.uuid4().hex
        self._held = False

    def acquire(self) -> bool:
        if not self.redis.enabled:
            # No Redis configured — treat as "always acquired" so single-machine
            # callers keep working. APScheduler max_instances=1 already prevents
            # in-process overlap.
            self._held = True
            return True
        self._held = self.redis.setnx(self.name, self._token, ttl_seconds=self.ttl_seconds)
        return self._held

    def release(self) -> None:
        if not self._held or not self.redis.enabled:
            self._held = False
            return
        try:
            # Only delete if we still own it (avoid releasing a key acquired by
            # another worker after our TTL expired). Cheap GET-then-DEL is good
            # enough; the small race window is acceptable for our throughput.
            current = self.redis.get(self.name)
            if current == self._token:
                self.redis.delete(self.name)
        finally:
            self._held = False

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
