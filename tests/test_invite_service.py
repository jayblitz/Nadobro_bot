from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import invite_service


class _FakeCursor:
    def __init__(self, row=None):
        self.row = row
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.statements.append((sql, params))

    def fetchone(self):
        return self.row


class _FakeConn:
    def __init__(self, row=None):
        self.cursor_obj = _FakeCursor(row)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, *args, **kwargs):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class InviteServiceTests(unittest.TestCase):
    def tearDown(self):
        invite_service.invalidate_private_access_cache()

    def test_normalize_code(self):
        self.assertEqual(invite_service.normalize_code(" abcd-2345 "), "ABCD2345")

    def test_generate_invite_codes_hashes_plaintext(self):
        captured = []

        def _execute_returning(_sql, params):
            captured.append(params)
            return {"id": 1, "code_prefix": "ABC", "created_at": "now", "expires_at": None, "note": None}

        with patch.object(invite_service, "ADMIN_USER_IDS", [123]), patch.object(
            invite_service, "_generate_plain_code", return_value="ABCDEFGH"
        ), patch.object(invite_service, "execute_returning", side_effect=_execute_returning), patch.object(
            invite_service, "insert_admin_log"
        ):
            codes = invite_service.generate_invite_codes(123, 1)

        self.assertEqual(codes[0]["code"], "ABCDEFGH")
        self.assertNotEqual(captured[0][0], "ABCDEFGH")
        self.assertEqual(len(captured[0][0]), 64)

    def test_redeem_invite_code_success(self):
        invite = {
            "id": 7,
            "created_by": 123,
            "redemption_count": 0,
            "max_redemptions": 1,
            "revoked_at": None,
            "expires_at": None,
            "created_for_telegram_id": None,
            "redeemed_by": None,
        }
        conn = _FakeConn(invite)
        with patch.object(invite_service, "get_db", return_value=conn), patch.object(
            invite_service, "put_db"
        ), patch.object(invite_service, "get_or_create_user"), patch.object(
            invite_service, "invalidate_user_cache"
        ):
            ok, msg = invite_service.redeem_invite_code(456, "tester", "ABCDEFGH")

        self.assertTrue(ok)
        self.assertIn("Access granted", msg)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(conn.rollbacks, 0)

    def test_redeem_invite_code_rejects_reuse(self):
        invite = {
            "id": 7,
            "created_by": 123,
            "redemption_count": 1,
            "max_redemptions": 1,
            "revoked_at": None,
            "expires_at": None,
            "created_for_telegram_id": None,
            "redeemed_by": 999,
        }
        conn = _FakeConn(invite)
        with patch.object(invite_service, "get_db", return_value=conn), patch.object(
            invite_service, "put_db"
        ), patch.object(invite_service, "get_or_create_user"):
            ok, msg = invite_service.redeem_invite_code(456, "tester", "ABCDEFGH")

        self.assertFalse(ok)
        self.assertIn("already been used", msg)
        self.assertEqual(conn.rollbacks, 1)

    def test_has_private_access_uses_short_cache(self):
        with patch.object(invite_service, "ADMIN_USER_IDS", []), patch.object(
            invite_service, "query_one", return_value={"private_access_granted": True}
        ) as query_one:
            self.assertTrue(invite_service.has_private_access(456))
            self.assertTrue(invite_service.has_private_access(456))

        query_one.assert_called_once()


if __name__ == "__main__":
    unittest.main()
