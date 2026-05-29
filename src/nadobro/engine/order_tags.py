"""Unique-ID tagging + correlation registry for engine (MM) orders.

Phase B of the WS v2 work. Nado's place-order accepts an optional ``client_id``
that is echoed back as ``id`` in the ``order_update`` / ``fill`` subscription
streams. Critically, the docs note that ``client_id`` is **not** part of the
order digest, so the authoritative way to distinguish otherwise-identical
orders (e.g. the same grid level re-posted repeatedly) is the **last 20 bits of
the order nonce**. ``NadoClient.place_order`` embeds the same tag in both places.

This module:
  * hands out unique 20-bit tags (``allocate_tag``), and
  * keeps a bounded registry mapping ``tag <-> digest <-> metadata`` so that a
    stream event carrying either an ``id`` (tag) or a ``digest`` can be resolved
    back to the controller / executor / grid level that placed the order.

Process-local and thread-safe. The 20-bit space (1,048,575 values) is far larger
than the number of live orders for a wallet, and tags are recycled by wrapping,
so collisions among *live* orders are effectively impossible.
"""
from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any, Optional

_TAG_MODULO = 1 << 20  # 20 bits — matches the order-nonce low bits.
_MAX_ENTRIES = 8192     # bound memory; oldest tags evicted first.

_lock = threading.RLock()
_counter = 0
_by_tag: "OrderedDict[int, dict[str, Any]]" = OrderedDict()
_digest_to_tag: "OrderedDict[str, int]" = OrderedDict()


def allocate_tag() -> int:
    """Return a fresh 20-bit tag in ``[1, 2**20 - 1]`` (0 is reserved as
    "untagged"). Monotonic with wraparound."""
    global _counter
    with _lock:
        _counter = (_counter + 1) % _TAG_MODULO
        if _counter == 0:
            _counter = 1
        return _counter


def _evict_if_needed() -> None:
    while len(_by_tag) > _MAX_ENTRIES:
        old_tag, old_meta = _by_tag.popitem(last=False)
        old_digest = old_meta.get("digest")
        if old_digest:
            _digest_to_tag.pop(old_digest, None)
    while len(_digest_to_tag) > _MAX_ENTRIES:
        _digest_to_tag.popitem(last=False)


def register(tag: int, **meta: Any) -> None:
    """Record metadata for a freshly allocated tag (before the digest is known)."""
    if tag is None:
        return
    with _lock:
        entry = _by_tag.get(int(tag)) or {}
        entry.update(meta)
        entry["tag"] = int(tag)
        _by_tag[int(tag)] = entry
        _by_tag.move_to_end(int(tag))
        _evict_if_needed()


def bind_digest(tag: Optional[int], digest: Optional[str]) -> None:
    """Link a venue order ``digest`` to a previously registered ``tag``."""
    if tag is None or not digest:
        return
    with _lock:
        entry = _by_tag.get(int(tag))
        if entry is None:
            entry = {"tag": int(tag)}
            _by_tag[int(tag)] = entry
        entry["digest"] = str(digest)
        _digest_to_tag[str(digest)] = int(tag)
        _by_tag.move_to_end(int(tag))
        _digest_to_tag.move_to_end(str(digest))
        _evict_if_needed()


def resolve_tag(tag: Optional[int]) -> Optional[dict[str, Any]]:
    if tag is None:
        return None
    with _lock:
        entry = _by_tag.get(int(tag))
        return dict(entry) if entry is not None else None


def resolve_digest(digest: Optional[str]) -> Optional[dict[str, Any]]:
    if not digest:
        return None
    with _lock:
        tag = _digest_to_tag.get(str(digest))
        if tag is None:
            return None
        entry = _by_tag.get(int(tag))
        return dict(entry) if entry is not None else None


def forget(*, tag: Optional[int] = None, digest: Optional[str] = None) -> None:
    with _lock:
        if digest:
            t = _digest_to_tag.pop(str(digest), None)
            if t is not None:
                _by_tag.pop(int(t), None)
        if tag is not None:
            entry = _by_tag.pop(int(tag), None)
            if entry and entry.get("digest"):
                _digest_to_tag.pop(entry["digest"], None)


def clear() -> None:
    """Test/operator helper — wipe all state."""
    global _counter
    with _lock:
        _counter = 0
        _by_tag.clear()
        _digest_to_tag.clear()


def stats() -> dict[str, int]:
    with _lock:
        return {"tags": len(_by_tag), "digests": len(_digest_to_tag), "counter": _counter}
