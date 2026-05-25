"""Tests for per-user strategy circuit breaker."""
from __future__ import annotations

import importlib
import time


def _fresh_module(monkeypatch):
    monkeypatch.setenv("NADO_USER_CIRCUIT_THRESHOLD", "3")
    monkeypatch.setenv("NADO_USER_CIRCUIT_WINDOW_SECONDS", "60")
    monkeypatch.setenv("NADO_USER_CIRCUIT_COOLDOWN_SECONDS", "120")
    from src.nadobro.services import user_circuit

    importlib.reload(user_circuit)
    return user_circuit


def test_circuit_opens_after_threshold(monkeypatch):
    mod = _fresh_module(monkeypatch)
    for i in range(3):
        mod.record_failure(1001, "mainnet", f"err-{i}")
    assert mod.is_open(1001, "mainnet") is True
    assert "err-2" in mod.last_error(1001, "mainnet")


def test_success_clears_circuit(monkeypatch):
    mod = _fresh_module(monkeypatch)
    mod.record_failure(1002, "testnet", "boom")
    mod.record_failure(1002, "testnet", "boom")
    mod.record_success(1002, "testnet")
    assert mod.is_open(1002, "testnet") is False


def test_snapshot_reports_open_circuits(monkeypatch):
    mod = _fresh_module(monkeypatch)
    for _ in range(3):
        mod.record_failure(9, "mainnet", "rate limit")
    snap = mod.snapshot()
    assert snap["tracked"] >= 1
    assert any(k.startswith("9:") for k in snap["open"])
