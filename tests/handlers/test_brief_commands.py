"""Regression tests for brief_commands helpers."""

from __future__ import annotations

import enum
from types import SimpleNamespace

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.handlers import brief_commands


class _FakeMode(enum.Enum):
    MAINNET = "mainnet"
    TESTNET = "testnet"


def test_network_for_coerces_enum_to_str(monkeypatch):
    user = SimpleNamespace(network_mode=_FakeMode.MAINNET)
    monkeypatch.setattr(
        brief_commands,
        "get_or_create_user",
        lambda telegram_id, _username: (user, False, None),
    )
    result = brief_commands._network_for(42)
    assert result == "mainnet"
    assert isinstance(result, str)


def test_network_for_passes_string_through(monkeypatch):
    user = SimpleNamespace(network_mode="testnet")
    monkeypatch.setattr(
        brief_commands,
        "get_or_create_user",
        lambda telegram_id, _username: (user, False, None),
    )
    assert brief_commands._network_for(7) == "testnet"


def test_network_for_falls_back_on_missing_user(monkeypatch):
    def _raise(*_args, **_kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(brief_commands, "get_or_create_user", _raise)
    assert brief_commands._network_for(99) == "mainnet"
