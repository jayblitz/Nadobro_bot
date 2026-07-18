"""Safe lifecycle tests for clearing saved Copy Trading selections."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src.nadobro.handlers import copy_handler
from src.nadobro.handlers.keyboards import copy_hub_kb
from src.nadobro.models import database as db_mod
from src.nadobro.trading import copy_service


def test_clear_selections_is_owner_scoped_exposure_safe_and_locks_the_start_race():
    class Cursor:
        def __init__(self):
            self.calls = []
            self.rowcount = 0
            self._protection = iter((False, True))

        def execute(self, sql, params=None):
            self.calls.append((sql, params))
            self.rowcount = 1 if "UPDATE copy_traders" in sql else 0

        def fetchall(self):
            return [{"id": 10}, {"id": 11}]

        def fetchone(self):
            return {"protected": next(self._protection)}

    cursor = Cursor()
    with patch.object(db_mod, "run_transaction", side_effect=lambda work: work(cursor)):
        result = db_mod.clear_saved_copy_trader_selections(4242)

    assert result == (1, 1)
    owner_sql, owner_params = cursor.calls[0]
    assert owner_params == (4242,)
    assert "t.owner_user_id = %s" in owner_sql
    assert "t.is_curated = false" in owner_sql
    assert "FOR UPDATE" in owner_sql
    protection_sql = cursor.calls[1][0]
    assert "m.active = true" in protection_sql
    assert "p.status = 'open'" in protection_sql
    assert any("SET active = false" in sql for sql, _ in cursor.calls)


def test_clear_service_only_delegates_to_safe_selection_query():
    with patch.object(copy_service, "clear_saved_copy_trader_selections", return_value=(3, 2)) as clear:
        assert copy_service.clear_saved_copy_traders(99) == (3, 2)
    clear.assert_called_once_with(99)


def test_hub_keyboard_exposes_clear_only_for_personal_selections():
    personal = [{"id": 1, "wallet": "0xabc", "label": "mine", "owner_user_id": 7, "is_curated": False}]
    curated = [{"id": 2, "wallet": "0xdef", "label": "public", "owner_user_id": None, "is_curated": True}]

    personal_callbacks = [button.callback_data for row in copy_hub_kb(personal).inline_keyboard for button in row]
    curated_callbacks = [button.callback_data for row in copy_hub_kb(curated).inline_keyboard for button in row]

    assert "copy:clear" in personal_callbacks
    assert "copy:clear" not in curated_callbacks


def test_clear_callback_calls_backend_and_preserves_protected_selections():
    async def _case():
        context = SimpleNamespace(user_data={
            "copy_setup": {"step": "budget"},
            "pending_copy_wallet": True,
            "pending_admin_copy_wallet": True,
        })
        edit = AsyncMock()

        async def _run(fn, *args, **kwargs):
            if fn.__name__ == "clear_saved_copy_traders":
                assert args == (4242,)
                return 2, 1
            if fn.__name__ == "get_available_traders":
                return []
            raise AssertionError(f"Unexpected blocking function: {fn.__name__}")

        with patch.object(copy_handler, "run_blocking", side_effect=_run), \
             patch.object(copy_handler, "_edit_loc", edit), \
             patch.object(copy_handler, "is_admin", return_value=False):
            await copy_handler._handle_copy(SimpleNamespace(), "copy:clear:confirm", context, 4242)

        assert "copy_setup" not in context.user_data
        assert "pending_copy_wallet" not in context.user_data
        assert "pending_admin_copy_wallet" not in context.user_data
        rendered = edit.await_args.args[1]
        assert "Cleared" in rendered
        assert "Kept" in rendered
        assert "selection\\(s\\)" in rendered

    asyncio.run(_case())


def test_hub_cancels_stale_copy_wizard_and_pending_wallet():
    async def _case():
        context = SimpleNamespace(user_data={
            "copy_setup": {"step": "risk"},
            "pending_copy_wallet": True,
            "pending_admin_copy_wallet": True,
        })
        edit = AsyncMock()

        with patch.object(copy_handler, "run_blocking", new=AsyncMock(return_value=[])), \
             patch.object(copy_handler, "_edit_loc", edit), \
             patch.object(copy_handler, "is_admin", return_value=False):
            await copy_handler._handle_copy(SimpleNamespace(), "copy:hub", context, 4242)

        assert "copy_setup" not in context.user_data
        assert "pending_copy_wallet" not in context.user_data
        assert "pending_admin_copy_wallet" not in context.user_data

    asyncio.run(_case())


def test_admin_menu_cancels_stale_admin_wallet_input():
    async def _case():
        context = SimpleNamespace(user_data={"pending_admin_copy_wallet": True})
        edit = AsyncMock()

        with patch.object(copy_handler, "run_blocking", new=AsyncMock(return_value=[])), \
             patch.object(copy_handler, "_edit_loc", edit), \
             patch.object(copy_handler, "is_admin", return_value=True):
            await copy_handler._handle_copy(SimpleNamespace(), "copy:admin:menu", context, 4242)

        assert "pending_admin_copy_wallet" not in context.user_data
        edit.assert_awaited_once()

    asyncio.run(_case())
