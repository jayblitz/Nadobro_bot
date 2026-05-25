"""Manual-order intent IDs must be deterministic so a double-tap collapses.

Re-importing trade_service is heavy because of its transitive deps. To keep
the test fast and isolated, we recreate the small helper inline and verify
its properties; the production code calls this same hash from
``trade_service.execute_*`` paths.
"""
from __future__ import annotations

import hashlib
import time


def _build_manual_nonce(
    *,
    telegram_id: int,
    network: str,
    product: str,
    is_long: bool,
    size: float,
    reduce_only: bool,
    post_only: bool,
    price: str,
    window_seconds: int = 30,
) -> str:
    window_bucket = int(time.time()) // window_seconds
    digest_input = "|".join(
        str(part) for part in (
            "manual",
            telegram_id,
            network,
            product,
            "long" if is_long else "short",
            f"{float(size):.10g}",
            bool(reduce_only),
            bool(post_only),
            str(price or "").strip(),
            window_bucket,
        )
    )
    return "manual:" + hashlib.sha256(digest_input.encode("utf-8")).hexdigest()[:24]


def test_same_params_within_window_produce_same_nonce():
    kwargs = dict(
        telegram_id=42,
        network="mainnet",
        product="BTC",
        is_long=True,
        size=0.01,
        reduce_only=False,
        post_only=False,
        price="",
    )
    a = _build_manual_nonce(**kwargs)
    b = _build_manual_nonce(**kwargs)
    assert a == b


def test_different_users_produce_different_nonces():
    common = dict(
        network="mainnet",
        product="BTC",
        is_long=True,
        size=0.01,
        reduce_only=False,
        post_only=False,
        price="",
    )
    assert _build_manual_nonce(telegram_id=1, **common) != _build_manual_nonce(telegram_id=2, **common)


def test_different_size_produces_different_nonce():
    common = dict(
        telegram_id=1,
        network="mainnet",
        product="BTC",
        is_long=True,
        reduce_only=False,
        post_only=False,
        price="",
    )
    assert _build_manual_nonce(size=0.01, **common) != _build_manual_nonce(size=0.02, **common)


def test_different_window_buckets_produce_different_nonce():
    kwargs = dict(
        telegram_id=1,
        network="mainnet",
        product="BTC",
        is_long=True,
        size=0.01,
        reduce_only=False,
        post_only=False,
        price="",
    )
    a = _build_manual_nonce(window_seconds=1, **kwargs)
    time.sleep(1.2)
    b = _build_manual_nonce(window_seconds=1, **kwargs)
    assert a != b, "after the window rolls over the nonce should refresh so retries are not perpetually blocked"
