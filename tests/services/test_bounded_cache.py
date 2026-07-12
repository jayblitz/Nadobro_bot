"""Unit tests for bounded LRU+TTL cache and keyed lock map."""
from __future__ import annotations

import asyncio
import threading
import time

import pytest

from src.nadobro.core.bounded_cache import KeyedLockMap, LRUCache


class TestLRUCache:
    def test_set_get_basic(self):
        cache: LRUCache[str, int] = LRUCache(max_size=3)
        cache.set("a", 1)
        cache.set("b", 2)
        assert cache.get("a") == 1
        assert cache.get("b") == 2
        assert cache.get("missing") is None

    def test_evicts_oldest_when_over_cap(self):
        cache: LRUCache[str, int] = LRUCache(max_size=2)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)  # should evict "a"
        assert cache.get("a") is None
        assert cache.get("b") == 2
        assert cache.get("c") == 3

    def test_get_promotes_to_mru(self):
        cache: LRUCache[str, int] = LRUCache(max_size=2)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.get("a")  # promotes a
        cache.set("c", 3)  # should now evict b
        assert cache.get("a") == 1
        assert cache.get("b") is None
        assert cache.get("c") == 3

    def test_ttl_expiry(self):
        cache: LRUCache[str, int] = LRUCache(max_size=10, ttl_seconds=0.05)
        cache.set("a", 1)
        assert cache.get("a") == 1
        time.sleep(0.06)
        assert cache.get("a") is None

    def test_pop(self):
        cache: LRUCache[str, int] = LRUCache(max_size=5)
        cache.set("a", 1)
        assert cache.pop("a") == 1
        assert cache.get("a") is None
        assert cache.pop("missing") is None

    def test_thread_safety_under_contention(self):
        cache: LRUCache[int, int] = LRUCache(max_size=200)
        errors: list[BaseException] = []

        def worker(start: int) -> None:
            try:
                for i in range(start, start + 500):
                    cache.set(i % 100, i)
                    cache.get(i % 100)
            except BaseException as exc:  # pragma: no cover - safety
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i * 1000,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
        assert cache.stats()["size"] <= 200


class TestKeyedLockMap:
    def test_get_creates_lock_per_key(self):
        m = KeyedLockMap(max_size=4, factory=threading.Lock)
        lock_a = m.get("a")
        lock_b = m.get("b")
        assert lock_a is not lock_b
        assert m.get("a") is lock_a

    def test_evicts_idle_when_over_cap(self):
        m = KeyedLockMap(max_size=2, idle_seconds=0.01, factory=threading.Lock)
        m.get("a")
        time.sleep(0.02)
        m.get("b")
        m.get("c")  # cap exceeded — "a" should be evicted because idle
        assert m.stats()["size"] == 2

    def test_idle_eviction_does_not_drop_recent_keys(self):
        m = KeyedLockMap(max_size=2, idle_seconds=10, factory=threading.Lock)
        m.get("a")
        m.get("b")
        m.get("c")  # nothing is idle → must still drop the LRU
        assert m.stats()["size"] == 2

    def test_async_lock_factory(self):
        async def body() -> None:
            m = KeyedLockMap(max_size=4)  # default factory: asyncio.Lock
            lock = m.get(42)
            async with lock:
                pass
            assert m.get(42) is lock

        asyncio.run(body())
