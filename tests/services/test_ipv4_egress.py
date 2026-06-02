"""Tests for IPv4 egress pinning (NADO_FORCE_IPV4)."""
from __future__ import annotations

import importlib
import socket

import pytest


@pytest.fixture()
def fresh_ipv4():
    from src.nadobro.services import ipv4_egress

    importlib.reload(ipv4_egress)
    return ipv4_egress


def test_force_ipv4_enabled_defaults_on(fresh_ipv4, monkeypatch):
    monkeypatch.delenv("NADO_FORCE_IPV4", raising=False)
    assert fresh_ipv4.force_ipv4_enabled() is True


@pytest.mark.parametrize("value", ("0", "false", "no", "off"))
def test_force_ipv4_can_be_disabled(fresh_ipv4, monkeypatch, value):
    monkeypatch.setenv("NADO_FORCE_IPV4", value)
    assert fresh_ipv4.force_ipv4_enabled() is False


def test_install_patches_urllib3_to_af_inet(fresh_ipv4, monkeypatch):
    monkeypatch.setenv("NADO_FORCE_IPV4", "1")
    import urllib3.util.connection as urllib3_connection

    fresh_ipv4._INSTALLED = False
    assert fresh_ipv4.install_ipv4_only_resolver() is True
    assert urllib3_connection.allowed_gai_family() == socket.AF_INET


def test_websocket_connect_kwargs_when_enabled(fresh_ipv4, monkeypatch):
    monkeypatch.setenv("NADO_FORCE_IPV4", "1")
    assert fresh_ipv4.websocket_connect_kwargs() == {"family": socket.AF_INET}


def test_websocket_connect_kwargs_when_disabled(fresh_ipv4, monkeypatch):
    monkeypatch.setenv("NADO_FORCE_IPV4", "0")
    assert fresh_ipv4.websocket_connect_kwargs() == {}
