import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import referral_service


class _FakeCursor:
    def __init__(self, rows, capture=None):
        self.rows = list(rows)
        self.capture = capture if capture is not None else {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.capture.setdefault("executed", []).append((sql, params))
        self.capture["params"] = params

    def fetchone(self):
        if not self.rows:
            return None
        return self.rows.pop(0)


class _FakeConn:
    def __init__(self, rows, capture=None):
        self.rows = rows
        self.capture = capture if capture is not None else {}
        self.committed = False
        self.rolled_back = False

    def cursor(self, *args, **kwargs):
        return _FakeCursor(self.rows, self.capture)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class ReferralServiceTests(unittest.TestCase):
    def test_earned_invite_allowance_caps_at_max(self):
        self.assertEqual(referral_service.earned_invite_allowance(0), 0)
        self.assertEqual(referral_service.earned_invite_allowance(9999), 0)
        self.assertEqual(referral_service.earned_invite_allowance(10000), 1)
        self.assertEqual(referral_service.earned_invite_allowance(10_000_000_000), referral_service.REFERRAL_MAX_INVITE_CODES)

    def test_referral_payload_normalization(self):
        self.assertEqual(referral_service.normalize_referral_payload("ref_abcd-2345"), "ABCD2345")
        self.assertEqual(referral_service.referral_start_payload("abcd2345"), "ref_ABCD2345")

    def test_generate_referral_invite_requires_earned_volume(self):
        conn = _FakeConn([
            {"telegram_id": 1},
            {"total": 9999.0},
            {"count": 0},
        ])
        with patch.object(referral_service, "get_db", return_value=conn), patch.object(
            referral_service, "put_db", return_value=None
        ):
            ok, msg, row = referral_service.generate_referral_invite_code(1, network="mainnet")

        self.assertFalse(ok)
        self.assertIsNone(row)
        self.assertIn("unlock", msg.lower())
        self.assertIn("mainnet", msg.lower())
        self.assertTrue(conn.rolled_back)

    def test_generate_referral_invite_inserts_public_referral_code(self):
        captured = {}
        conn = _FakeConn([
            {"telegram_id": 1},
            {"total": 10000.0},
            {"count": 0},
            {"id": 9, "public_code": "ABCDEFGH", "code_prefix": "ABC"},
        ], captured)

        with patch.object(referral_service, "get_db", return_value=conn), patch.object(
            referral_service, "put_db", return_value=None
        ), patch.object(
            referral_service, "_generate_plain_code", return_value="ABCDEFGH"
        ), patch.object(
            referral_service, "BOT_USERNAME", "Nadbro_bot"
        ):
            ok, _msg, row = referral_service.generate_referral_invite_code(1, network="testnet")

        self.assertTrue(ok)
        self.assertTrue(conn.committed)
        self.assertEqual(row["public_code"], "ABCDEFGH")
        self.assertIn("https://t.me/Nadbro_bot", row["link"])
        self.assertIn("start=ref_ABCDEFGH", row["link"])
        self.assertEqual(captured["params"][1], "ABCDEFGH")
        self.assertEqual(captured["params"][4], 1)
        self.assertEqual(captured["params"][5], "testnet")

    def test_generated_count_keeps_legacy_codes_testnet_only(self):
        captured = {}

        def _query_one(sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return {"count": 2}

        with patch.object(referral_service, "query_one", side_effect=_query_one):
            self.assertEqual(referral_service._generated_code_count(7, "testnet"), 2)

        self.assertIn("network IS NULL", captured["sql"])
        self.assertEqual(captured["params"], (7, "testnet", "testnet"))


if __name__ == "__main__":
    unittest.main()
