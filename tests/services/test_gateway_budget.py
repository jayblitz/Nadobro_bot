"""Tests for the unified Nado gateway budget layer."""
from __future__ import annotations

import importlib
import sys
import time

import pytest


@pytest.fixture(autouse=True)
def _reset_gateway_state():
    """Clear the gateway module's process-global state after each test.

    ``record_gateway_failure`` opens a rate-limit circuit keyed by host that
    otherwise leaks into other test files (which share the same host name) and
    makes ``cf_request`` short-circuit to None. Reset on the *current* module in
    sys.modules so reload-based tests are covered too.
    """
    yield
    mod = sys.modules.get("src.nadobro.services.gateway_budget")
    if mod is None:
        return
    with mod._lock:
        mod._gateway_rl.clear()
        mod._user_buckets.clear()
        mod._user_inflight.clear()
        mod._wallet_buckets.clear()


def _fresh_module(monkeypatch):
    monkeypatch.setenv("NADO_USER_GATEWAY_RPS", "100")
    monkeypatch.setenv("NADO_USER_GATEWAY_BURST", "100")
    monkeypatch.setenv("NADO_USER_MAX_INFLIGHT", "4")
    monkeypatch.setenv("NADO_GATEWAY_RL_THRESHOLD", "2")
    monkeypatch.setenv("NADO_GATEWAY_RL_WINDOW_SECONDS", "30")
    monkeypatch.setenv("NADO_GATEWAY_RL_COOLDOWN_SECONDS", "60")
    from src.nadobro.services import gateway_budget

    importlib.reload(gateway_budget)
    return gateway_budget


def test_try_acquire_releases_on_host_throttle(monkeypatch):
    mod = _fresh_module(monkeypatch)
    url = "https://gateway.mainnet.nado.xyz/query"

    def _deny(_url, *, cost=1.0, max_wait=None):
        return False

    monkeypatch.setattr("src.nadobro.services.http_session.throttle_host", _deny)
    assert mod.try_acquire(url, user_id=42) is False
    snap = mod.snapshot()
    assert 42 not in snap["user_inflight"]


def test_query_weight_forwarded_to_host_bucket(monkeypatch):
    """A query's documented weight is charged against the host bucket as cost."""
    mod = _fresh_module(monkeypatch)
    url = "https://gateway.mainnet.nado.xyz/query"
    seen: dict = {}

    def _record(_url, *, cost=1.0, max_wait=None):
        seen["cost"] = cost
        return True

    monkeypatch.setattr("src.nadobro.services.http_session.throttle_host", _record)
    assert mod.try_acquire(url, user_id=1, weight=10) is True
    assert seen["cost"] == 10
    mod.release(1)


def test_per_user_bucket_drains_by_weight(monkeypatch):
    """The per-user fair-share bucket consumes weight, not a flat 1/call."""
    monkeypatch.setenv("NADO_USER_GATEWAY_RPS", "0.0001")  # negligible refill
    monkeypatch.setenv("NADO_USER_GATEWAY_BURST", "10")
    monkeypatch.setenv("NADO_USER_MAX_INFLIGHT", "100")
    import importlib
    from src.nadobro.services import gateway_budget as mod
    importlib.reload(mod)
    monkeypatch.setattr("src.nadobro.services.http_session.throttle_host", lambda *_a, **_k: True)
    url = "https://gateway.mainnet.nado.xyz/query"
    # First weight-8 call fits in the burst-10 bucket; the next weight-8 call
    # cannot (only ~2 tokens left) and is denied with a tiny wait budget.
    assert mod.try_acquire(url, user_id=99, weight=8, max_wait=0.01) is True
    assert mod.try_acquire(url, user_id=99, weight=8, max_wait=0.01) is False


def test_execute_lane_uses_wallet_budget(monkeypatch):
    """kind='execute' charges a per-wallet bucket, independent of host/IP."""
    monkeypatch.setenv("NADO_WALLET_EXECUTE_RPS", "0.0001")
    monkeypatch.setenv("NADO_WALLET_EXECUTE_BURST", "5")
    import importlib
    from src.nadobro.services import gateway_budget as mod
    importlib.reload(mod)
    # Host throttle must NOT be consulted for executes.
    def _boom(*_a, **_k):
        raise AssertionError("execute lane must not touch the host bucket")
    monkeypatch.setattr("src.nadobro.services.http_session.throttle_host", _boom)
    url = "https://gateway.mainnet.nado.xyz/execute"
    wallet = "0xabc"
    assert mod.try_acquire(url, kind="execute", wallet=wallet, weight=5, max_wait=0.01) is True
    assert mod.try_acquire(url, kind="execute", wallet=wallet, weight=5, max_wait=0.01) is False
    # A different wallet has its own budget.
    assert mod.try_acquire(url, kind="execute", wallet="0xdef", weight=5, max_wait=0.01) is True


def test_execute_lane_needs_no_release(monkeypatch):
    """Executes take no in-flight slot, so they never need release()."""
    mod = _fresh_module(monkeypatch)
    url = "https://gateway.mainnet.nado.xyz/execute"
    assert mod.try_acquire(url, kind="execute", wallet="0x1", weight=1) is True
    snap = mod.snapshot()
    assert snap["user_inflight"] == {}


def test_per_user_inflight_cap(monkeypatch):
    mod = _fresh_module(monkeypatch)
    url = "https://gateway.mainnet.nado.xyz/query"
    monkeypatch.setattr("src.nadobro.services.http_session.throttle_host", lambda *_a, **_k: True)
    assert mod.try_acquire(url, user_id=7) is True
    assert mod.try_acquire(url, user_id=7) is True
    assert mod.try_acquire(url, user_id=7) is True
    assert mod.try_acquire(url, user_id=7) is True
    assert mod.try_acquire(url, user_id=7) is False
    mod.release(7)
    assert mod.try_acquire(url, user_id=7) is True
    mod.release(7)
    mod.release(7)
    mod.release(7)
    mod.release(7)


def test_rate_limit_circuit_opens(monkeypatch):
    mod = _fresh_module(monkeypatch)
    url = "https://gateway.mainnet.nado.xyz/query"
    mod.record_gateway_failure(url, 'Too many requests "error_code":1000')
    mod.record_gateway_failure(url, "error_code=1000")
    assert mod.is_gateway_rate_limited(url) is True
    assert mod.is_gateway_blocked(url) is True


def test_is_rate_limit_error():
    from src.nadobro.services.gateway_budget import is_rate_limit_error

    assert is_rate_limit_error('Too many requests "error_code":1000')
    assert is_rate_limit_error(Exception("error_code=1000"))
    assert not is_rate_limit_error("connection reset")
