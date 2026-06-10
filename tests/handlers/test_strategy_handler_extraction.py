"""Guards for the strategy-domain extraction from callbacks.py (2026-06).

The strategy handlers live in handlers/strategy_handler.py; callbacks.py
keeps a lazy shim so its three dispatch sites (and any external patcher)
keep working. These tests pin the delegation contract and the module
boundary so a refactor can't silently fork the two copies again.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.handlers import callbacks, strategy_handler


def test_callbacks_shim_delegates_to_strategy_handler():
    impl = AsyncMock(return_value=None)
    with patch.object(strategy_handler, "_handle_strategy", impl):
        asyncio.run(callbacks._handle_strategy("query", "strategy:menu", "ctx", 7))
    impl.assert_awaited_once_with("query", "strategy:menu", "ctx", 7)


def test_strategy_domain_lives_only_in_strategy_handler():
    # The moved helpers must exist on the new module...
    for name in (
        "_strategy_available_products",
        "_strategy_config_section_kb",
        "_strategy_config_section_text",
        "_build_strategy_preview_text",
        "_mm_cycle_budget_preflight",
        "_fmt_strategy_config_text",
    ):
        assert callable(getattr(strategy_handler, name)), name
    # ...and not as separate copies in callbacks (the shim is the only
    # strategy-named symbol callbacks defines itself).
    src = open("src/nadobro/handlers/callbacks.py").read()
    assert "def _strategy_config_section_kb" not in src
    assert "def _build_strategy_preview_text" not in src
    assert "def _fmt_strategy_config_text" not in src


def test_shared_utils_are_the_same_objects():
    # strategy_handler reuses callbacks' shared UI utilities rather than
    # forking them — identity, not equality.
    assert strategy_handler._edit_loc is callbacks._edit_loc
    assert strategy_handler._get_user_settings is callbacks._get_user_settings
    assert strategy_handler._handle_nav is callbacks._handle_nav
