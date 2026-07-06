"""Leaf helpers for reading environment variables consistently.

This module has no intra-package imports (stdlib only) so it can be used from
any layer — config, services, handlers, engine, main — without risking an
import cycle.

The project historically inlined the truthy check
``os.environ.get(NAME, ...).strip().lower() in ("1", "true", "yes", "on")``
in ~20 places (config.py, main.py, and a dozen service modules), each with its
own default handling. ``env_bool`` is the single source of truth for that
parse. An unset **or blank** variable resolves to ``default``.
"""

from __future__ import annotations

import os

_TRUTHY = frozenset({"1", "true", "yes", "on"})
_FALSY = frozenset({"0", "false", "no", "off"})


def env_bool(name: str, default: bool = False) -> bool:
    """Return the boolean value of env var ``name``.

    Unset or blank (whitespace-only) resolves to ``default``. Otherwise the
    value is truthy when it is one of ``1/true/yes/on`` (case-insensitive).
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    raw = raw.strip().lower()
    if raw == "":
        return default
    return raw in _TRUTHY


def env_tristate(name: str) -> bool | None:
    """Three-valued read: ``True`` for truthy, ``False`` for explicit falsy
    (``0/false/no/off``), ``None`` when unset/blank/unrecognized.

    Use when "not set" must be distinguished from "set to false" (e.g. a
    per-strategy override that should fall through to a global default).
    """
    raw = os.environ.get(name)
    if raw is None:
        return None
    raw = raw.strip().lower()
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return None
