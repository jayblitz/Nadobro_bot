"""Per-user cache invalidation guard.

Regression test for the scaling bug where ``switch_network`` and
``remove_user_private_key`` called ``clear_client_cache()`` and
``_readonly_cache.clear()`` with no arguments, evicting **every** user's cached
SDK session whenever any single user changed network or unlinked. The fix
introduces ``_invalidate_user_caches`` which targets only the active user's
entries; the test verifies a sibling cache entry survives the operation.
"""
from __future__ import annotations

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import nado_client, user_service


def _seed_two_users():
    """Plant cache rows for two different wallets so we can prove the second
    survives when the first is invalidated.
    """
    addr_a = "0xaaaa000000000000000000000000000000000000"
    addr_b = "0xbbbb000000000000000000000000000000000000"
    # NadoClient cache keys are ``f"{addr.lower()}_{network}"``.
    nado_client._client_cache[f"{addr_a}_mainnet"] = object()
    nado_client._client_cache[f"{addr_a}_testnet"] = object()
    nado_client._client_cache[f"{addr_b}_mainnet"] = object()
    # Readonly cache keys live in user_service: ``"ro:<addr>:<network>"``.
    user_service._readonly_cache[f"ro:{addr_a}:mainnet"] = {"client": object(), "ts": 1.0}
    user_service._readonly_cache[f"ro:{addr_b}:mainnet"] = {"client": object(), "ts": 1.0}
    return addr_a, addr_b


def test_invalidate_user_caches_only_clears_target_address(monkeypatch):
    addr_a, addr_b = _seed_two_users()
    monkeypatch.setattr(
        "src.nadobro.services.nado_sync.clear_cache", lambda *_a, **_k: None, raising=False,
    )
    user_service._invalidate_user_caches(addr_a, telegram_id=1)
    assert f"{addr_a}_mainnet" not in nado_client._client_cache
    assert f"{addr_a}_testnet" not in nado_client._client_cache
    assert f"ro:{addr_a}:mainnet" not in user_service._readonly_cache
    # User B's entries MUST survive — the scaling bug was that they did not.
    assert f"{addr_b}_mainnet" in nado_client._client_cache
    assert f"ro:{addr_b}:mainnet" in user_service._readonly_cache


def test_invalidate_user_caches_handles_missing_address(monkeypatch):
    """If the user was already unlinked (address is None), the helper must
    not touch any cache entries.
    """
    monkeypatch.setattr(
        "src.nadobro.services.nado_sync.clear_cache", lambda *_a, **_k: None, raising=False,
    )
    nado_client._client_cache["0xdead_mainnet"] = object()
    user_service._invalidate_user_caches(None, telegram_id=2)
    assert "0xdead_mainnet" in nado_client._client_cache
