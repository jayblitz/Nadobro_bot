"""Edge-case coverage for the vault deposit-opening watcher."""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def watch_module(monkeypatch):
    monkeypatch.setenv("VAULT_DEPOSIT_CLOSED_EPSILON_USDT0", "1.0")
    monkeypatch.setenv("VAULT_DEPOSIT_OPEN_MIN_USDT0", "100.0")
    import src.nadobro.vault.vault_deposit_watch_service as mod
    importlib.reload(mod)
    yield mod


def test_closed_to_open_triggers(watch_module):
    assert watch_module.should_notify_deposit_opening(0.0, 500.0) is True
    assert watch_module.should_notify_deposit_opening(1.0, 100.0) is True


def test_open_to_more_open_does_not_trigger(watch_module):
    assert watch_module.should_notify_deposit_opening(500.0, 1000.0) is False
    assert watch_module.should_notify_deposit_opening(101.0, 5000.0) is False


def test_closed_to_dust_does_not_trigger(watch_module):
    assert watch_module.should_notify_deposit_opening(0.0, 50.0) is False


def test_open_to_closed_does_not_trigger(watch_module):
    assert watch_module.should_notify_deposit_opening(500.0, 0.0) is False


def test_eligibility_below_cap(watch_module):
    assert watch_module.user_eligible_for_deposit_watch(0.0) is True
    assert watch_module.user_eligible_for_deposit_watch(19_999.99) is True


def test_eligibility_at_or_above_cap(watch_module):
    assert watch_module.user_eligible_for_deposit_watch(20_000.0) is False
    assert watch_module.user_eligible_for_deposit_watch(25_000.0) is False


def test_capacity_open_threshold(watch_module):
    assert watch_module.is_deposit_capacity_open(100.0) is True
    assert watch_module.is_deposit_capacity_open(99.99) is False


def test_capacity_closed_threshold(watch_module):
    assert watch_module.is_deposit_capacity_closed(0.0) is True
    assert watch_module.is_deposit_capacity_closed(1.0) is True
    assert watch_module.is_deposit_capacity_closed(1.01) is False
