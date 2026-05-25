r"""Bounded LRU+TTL caches for module-level state.

Why this module exists
======================

The audit identified several module-level ``dict``\s that grow without
bound at 1000 users (``_user_locks``, ``_client_cache``, ``_inflight``,
read-only client cache, etc.). Each entry holds onto Python objects
indefinitely; in the worst case it pins entire SDK sessions to memory
for users who haven't interacted in weeks.

This module exposes two thread-safe, lock-free-readers-where-possible
primitives:

* :class:`LRUCache` — capped size + per-entry TTL. ``get`` returns
  ``None`` for expired entries and evicts them lazily; ``set`` enforces
  the cap by dropping the oldest entry.
* :class:`KeyedLockMap` — lazily-created per-key locks (e.g. per-user
  ``asyncio.Lock``) with size cap and last-touch tracking. Locks that
  have been idle longer than ``idle_seconds`` are eligible for eviction
  when the cap is exceeded.

Both are designed for the dominant access pattern in this codebase
(many concurrent reads, occasional writes) and the dominant operational
constraint (one Python process per shard, 1000 distinct keys).
"""
from __future__ import annotations

import asyncio
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Generic, Hashable, Iterator, Optional, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


@dataclass
class _Entry(Generic[V]):
    value: V
    expires_at: float  # 0 means never expires


class LRUCache(Generic[K, V]):
    """Thread-safe LRU with optional per-entry TTL.

    ``max_size`` ≤ 0 disables the cap (use with care). ``ttl_seconds``
    of 0 means "no expiry"; positive values cause stale entries to be
    discarded on read.
    """

    def __init__(self, *, max_size: int, ttl_seconds: float = 0.0) -> None:
        self._max_size = int(max_size)
        self._ttl = float(ttl_seconds)
        self._store: "OrderedDict[K, _Entry[V]]" = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        now = time.time()
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return default
            if entry.expires_at and entry.expires_at <= now:
                self._store.pop(key, None)
                return default
            self._store.move_to_end(key)
            return entry.value

    def set(self, key: K, value: V, *, ttl_seconds: Optional[float] = None) -> None:
        ttl = self._ttl if ttl_seconds is None else float(ttl_seconds)
        expires_at = (time.time() + ttl) if ttl > 0 else 0.0
        with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
                self._store[key] = _Entry(value=value, expires_at=expires_at)
                return
            self._store[key] = _Entry(value=value, expires_at=expires_at)
            self._enforce_cap_locked()

    def pop(self, key: K) -> Optional[V]:
        with self._lock:
            entry = self._store.pop(key, None)
            return entry.value if entry else None

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def _enforce_cap_locked(self) -> None:
        if self._max_size <= 0:
            return
        while len(self._store) > self._max_size:
            self._store.popitem(last=False)

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {"size": len(self._store), "max_size": self._max_size}

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)

    def __contains__(self, key: object) -> bool:
        with self._lock:
            return key in self._store

    def __iter__(self) -> Iterator[K]:
        with self._lock:
            return iter(list(self._store.keys()))


@dataclass
class _LockHandle:
    lock: Any  # asyncio.Lock or threading.Lock
    last_touched: float


class KeyedLockMap:
    """Lazy per-key locks with LRU eviction.

    Designed for the ``_user_locks`` / ``_inflight`` pattern: each
    distinct ``key`` needs a serialization primitive, but at 1000+ users
    the dict cannot grow unbounded. Idle locks (not touched within
    ``idle_seconds``) are evicted when the cap is exceeded.

    The factory passed in determines the lock type: ``asyncio.Lock`` for
    coroutine code, ``threading.Lock`` for blocking-pool work.
    """

    def __init__(
        self,
        *,
        max_size: int,
        idle_seconds: float = 600.0,
        factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        self._max_size = int(max_size)
        self._idle = float(idle_seconds)
        self._factory = factory or asyncio.Lock
        self._store: "OrderedDict[Hashable, _LockHandle]" = OrderedDict()
        self._guard = threading.Lock()

    def get(self, key: Hashable) -> Any:
        now = time.time()
        with self._guard:
            handle = self._store.get(key)
            if handle is None:
                handle = _LockHandle(lock=self._factory(), last_touched=now)
                self._store[key] = handle
                self._enforce_cap_locked(now)
            else:
                handle.last_touched = now
                self._store.move_to_end(key)
            return handle.lock

    def discard(self, key: Hashable) -> None:
        with self._guard:
            self._store.pop(key, None)

    def _enforce_cap_locked(self, now: float) -> None:
        if self._max_size <= 0:
            return
        if len(self._store) <= self._max_size:
            return
        # Prefer to drop idle entries first.
        idle_cutoff = now - self._idle
        for key, handle in list(self._store.items()):
            if len(self._store) <= self._max_size:
                break
            if handle.last_touched <= idle_cutoff:
                self._store.pop(key, None)
        while len(self._store) > self._max_size:
            self._store.popitem(last=False)

    def stats(self) -> dict[str, int]:
        with self._guard:
            return {"size": len(self._store), "max_size": self._max_size}

    def __len__(self) -> int:
        with self._guard:
            return len(self._store)


__all__ = ("LRUCache", "KeyedLockMap")
