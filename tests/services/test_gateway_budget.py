"""Tests for the unified Nado gateway budget layer."""
from __future__ import annotations

import importlib
import time


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

    def _deny(_url, *, max_wait=None):
        return False

    monkeypatch.setattr("src.nadobro.services.http_session.throttle_host", _deny)
    assert mod.try_acquire(url, user_id=42) is False
    snap = mod.snapshot()
    assert 42 not in snap["user_inflight"]


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
