"""Pin outbound Nado traffic to IPv4 for stable Fly egress identity.

Fly static egress is allocated as an IPv4+IPv6 pair, but Python ``requests``
and ``websockets`` may prefer IPv6 when the destination publishes AAAA
records. For Nado rate-limit whitelisting we want the static IPv4 egress
address consistently.

Set ``NADO_FORCE_IPV4=0`` to disable (e.g. local dev without static egress).
"""
from __future__ import annotations

import logging
import os
import socket

logger = logging.getLogger(__name__)

_INSTALLED = False


def force_ipv4_enabled() -> bool:
    raw = os.environ.get("NADO_FORCE_IPV4", "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def install_ipv4_only_resolver() -> bool:
    """Patch urllib3 so ``requests`` connections resolve to AF_INET only."""
    global _INSTALLED
    if _INSTALLED or not force_ipv4_enabled():
        return _INSTALLED
    try:
        import urllib3.util.connection as urllib3_connection

        urllib3_connection.allowed_gai_family = lambda: socket.AF_INET
        _INSTALLED = True
        logger.info("NADO_FORCE_IPV4 enabled: outbound HTTP(S) uses IPv4 only")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to install IPv4-only HTTP resolver: %s", exc)
    return _INSTALLED


def websocket_connect_kwargs() -> dict:
    """Extra kwargs for ``websockets.connect`` when IPv4 egress is forced."""
    if force_ipv4_enabled():
        return {"family": socket.AF_INET}
    return {}
