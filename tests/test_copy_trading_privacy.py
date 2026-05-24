"""Privacy guarantees for the Copy Trading feature.

Production logs and direct user reports showed User B could see custom
copy-trade wallets added by User A. The root cause was that
``copy_traders`` was a globally-shared table — every UI query loaded the
full table for everyone. After the fix, custom traders carry an
``owner_user_id`` and the data-access functions filter on it.

These tests pin the SQL contracts so the regression cannot return.
"""
import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.models import database as db_mod
from src.nadobro.services import copy_service


def _stash_sql(captured: list):
    """Spy on (query/exec, sql, params) tuples for assertions."""

    def _capture(kind):
        def _inner(sql, params=None):
            captured.append((kind, sql, params))
            return [] if kind in {"query_all"} else None

        return _inner

    return _capture


class CopyTradingPrivacyTests(unittest.TestCase):
    """Validate scoping in ``models.database`` for copy traders."""

    def test_get_active_copy_traders_filters_by_owner_user_id(self):
        """A caller passing user_id MUST receive only curated + own rows.

        Without the WHERE clause one user could pull another's private
        trader rows — the original privacy bug. We verify the SQL emits
        both ``owner_user_id IS NULL`` (curated/public) and a parameter
        bound to the caller's id."""
        captured: list = []
        with patch.object(db_mod, "query_all", side_effect=_stash_sql(captured)("query_all")):
            db_mod.get_active_copy_traders(user_id=4242)

        self.assertEqual(len(captured), 1)
        _, sql, params = captured[0]
        self.assertIn("owner_user_id IS NULL", sql)
        self.assertIn("owner_user_id = %s", sql)
        self.assertEqual(params, (4242,))

    def test_get_active_copy_traders_without_user_returns_only_curated(self):
        """Background callers (pollers / health checks) that don't have a
        viewer context must still see ONLY curated entries — never user
        privates. This prevents accidental leakage from cron jobs."""
        captured: list = []
        with patch.object(db_mod, "query_all", side_effect=_stash_sql(captured)("query_all")):
            db_mod.get_active_copy_traders()

        _, sql, params = captured[0]
        self.assertIn("owner_user_id IS NULL", sql)
        self.assertNotIn("owner_user_id = %s", sql)
        self.assertIsNone(params)

    def test_upsert_personal_trader_includes_owner_id(self):
        """Personal entries must persist the owner so subsequent reads can
        filter on it. The conflict target is the partial owner-scoped
        unique index, never the curated one."""
        captured: list = []

        def _exec_returning(sql, params=None):
            captured.append((sql, params))
            return {"id": 99}

        with patch.object(db_mod, "execute_returning", side_effect=_exec_returning):
            tid = db_mod.upsert_copy_trader(
                "0xABCD",
                label="alpha",
                is_curated=False,
                owner_user_id=4242,
            )

        self.assertEqual(tid, 99)
        sql, params = captured[0]
        self.assertIn("owner_user_id", sql)
        self.assertIn("ON CONFLICT (owner_user_id, wallet_address)", sql)
        self.assertEqual(params, ("0xABCD", "alpha", 4242))

    def test_upsert_personal_trader_without_owner_returns_none(self):
        """We must refuse personal upserts that lack an owner — otherwise
        a buggy caller could create an orphaned row visible globally."""
        with patch.object(db_mod, "execute_returning") as mock_exec:
            tid = db_mod.upsert_copy_trader(
                "0xABCD",
                label="alpha",
                is_curated=False,
                owner_user_id=None,
            )

        self.assertIsNone(tid)
        mock_exec.assert_not_called()

    def test_upsert_curated_trader_forces_owner_null(self):
        """Curated entries are admin-managed; the SQL must hit the
        owner-IS-NULL partial unique index, regardless of any owner_id
        accidentally passed by the caller."""
        captured: list = []

        def _exec_returning(sql, params=None):
            captured.append((sql, params))
            return {"id": 1}

        with patch.object(db_mod, "execute_returning", side_effect=_exec_returning):
            db_mod.upsert_copy_trader(
                "0xCURATED",
                label="hero",
                is_curated=True,
                owner_user_id=4242,
            )

        sql, params = captured[0]
        self.assertIn("ON CONFLICT (wallet_address) WHERE owner_user_id IS NULL", sql)
        # Curated payload must NOT include the owner_id value.
        self.assertEqual(params, ("0xCURATED", "hero"))


class CopyServicePrivacyTests(unittest.TestCase):
    """End-to-end privacy at the service layer."""

    def test_get_available_traders_forwards_user_id_filter(self):
        """The view-layer helper must pass the user_id down to the SQL
        layer. Without this, the privacy migration is useless because the
        Telegram hub will still load globally."""
        with patch.object(
            copy_service,
            "get_active_copy_traders",
            return_value=[{
                "id": 1,
                "wallet_address": "0xself",
                "label": "self",
                "is_curated": False,
                "owner_user_id": 4242,
            }],
        ) as spy:
            traders = copy_service.get_available_traders(user_id=4242)

        spy.assert_called_once_with(user_id=4242)
        self.assertEqual(len(traders), 1)
        self.assertEqual(traders[0]["owner_user_id"], 4242)

    def test_add_trader_personal_requires_owner_id(self):
        """A frontend bug that forgets to pass the owner must result in a
        hard failure, not a silently-shared row."""
        ok, msg, tid = copy_service.add_trader(
            "0xAAA00000000000000000000000000000000000bb",
            label="alpha",
            is_curated=False,
            owner_user_id=None,
        )
        self.assertFalse(ok)
        self.assertIsNone(tid)
        self.assertIn("Owner", msg)

    def test_remove_trader_refuses_other_users_private(self):
        """User B must not be able to deactivate User A's custom wallet.
        Without this guard a curious user with a copy-trader id from the
        URL or a previous session could nuke another user's setup."""
        owned_by_a = {
            "id": 7,
            "wallet_address": "0xtarget",
            "label": "stranger",
            "active": True,
            "owner_user_id": 1111,
        }
        with patch.object(copy_service, "get_copy_trader", return_value=owned_by_a), \
             patch.object(copy_service, "get_mirrors_for_trader") as mirrors_spy, \
             patch.object(copy_service, "deactivate_copy_trader") as deactivate_spy:
            ok, msg = copy_service.remove_trader(7, requester_user_id=2222, is_admin=False)

        self.assertFalse(ok)
        mirrors_spy.assert_not_called()
        deactivate_spy.assert_not_called()

    def test_remove_trader_allows_owner(self):
        """The owner of a private trader CAN remove it. Verifies the
        guard does not over-restrict the legitimate user."""
        owned_by_user = {
            "id": 7,
            "wallet_address": "0xmine",
            "label": "myself",
            "active": True,
            "owner_user_id": 4242,
        }
        with patch.object(copy_service, "get_copy_trader", return_value=owned_by_user), \
             patch.object(copy_service, "get_mirrors_for_trader", return_value=[]), \
             patch.object(copy_service, "deactivate_copy_trader") as deactivate_spy:
            ok, msg = copy_service.remove_trader(7, requester_user_id=4242, is_admin=False)

        self.assertTrue(ok, msg=msg)
        deactivate_spy.assert_called_once_with(7)

    def test_start_copy_refuses_other_users_private_trader(self):
        """Even if a user crafts a callback like ``copy:start:<other_id>``
        the service layer must refuse — the trader belongs to someone
        else's session."""
        owned_by_a = {
            "id": 9,
            "wallet_address": "0xtarget",
            "label": "stranger",
            "active": True,
            "owner_user_id": 1111,
        }
        fake_user = type("U", (), {"linked_signer_address": "0xsigner", "network_mode": type("N", (), {"value": "mainnet"})()})()
        with patch.object(copy_service, "get_user", return_value=fake_user), \
             patch.object(copy_service, "get_copy_trader", return_value=owned_by_a), \
             patch.object(copy_service, "count_user_active_mirrors", return_value=0), \
             patch.object(copy_service, "create_copy_mirror_v2", return_value=42) as create_spy:
            ok, _ = copy_service.start_copy(telegram_id=2222, trader_id=9, margin_per_trade=50.0)

        self.assertFalse(ok)
        create_spy.assert_not_called()


if __name__ == "__main__":
    unittest.main()
