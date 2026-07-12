"""Tests for WS health registry used to skip REST portfolio polls."""
from __future__ import annotations

import importlib
import time


def _fresh_module(monkeypatch):
    monkeypatch.setenv("NADO_WS_HEALTH_SECONDS", "45")
    monkeypatch.setenv("NADO_WS_RECONCILE_SECONDS", "300")
    from src.nadobro.venue import ws_health

    importlib.reload(ws_health)
    return ws_health


def test_healthy_after_touch(monkeypatch):
    mod = _fresh_module(monkeypatch)
    mod.mark_connected(1, "mainnet")
    mod.touch(1, "mainnet")
    assert mod.is_healthy(1, "mainnet") is True
    assert mod.should_skip_poll(1, "mainnet", reason="poll") is True
    assert mod.should_skip_poll(1, "mainnet", reason="refresh") is False


def test_stale_socket_not_healthy(monkeypatch):
    mod = _fresh_module(monkeypatch)
    mod.mark_connected(2, "testnet")
    mod._last_event[(2, "testnet")] = time.time() - 120
    assert mod.is_healthy(2, "testnet") is False


def test_reconcile_due(monkeypatch):
    mod = _fresh_module(monkeypatch)
    assert mod.reconcile_due(1, "mainnet", time.monotonic() - 400) is True
    assert mod.reconcile_due(1, "mainnet", time.monotonic() - 10) is False
