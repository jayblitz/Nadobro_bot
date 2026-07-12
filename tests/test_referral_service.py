"""Tests for the vanity-code referral service."""
from __future__ import annotations

import unittest
from typing import Any
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.users import referral_service


class _FakeCursor:
    """Minimal psycopg2-like cursor that returns a queued list of rows.

    Tests pre-populate ``rows`` in the order the production code will
    ``fetchone()`` them and assert on ``executed`` after the call.
    """

    def __init__(self, rows: list[Any], capture: dict | None = None) -> None:
        self.rows = list(rows)
        self.capture = capture if capture is not None else {}
        self.last_row: Any | None = None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params: Any | None = None) -> None:
        self.capture.setdefault("executed", []).append((sql, params))
        self.capture["last_sql"] = sql
        self.capture["last_params"] = params
        # Pop the next queued row so it shows up in fetchone().
        self.last_row = self.rows.pop(0) if self.rows else None

    def fetchone(self) -> Any | None:
        return self.last_row


class _FakeConn:
    def __init__(self, rows: list[Any], capture: dict | None = None) -> None:
        self.cursor_obj = _FakeCursor(rows, capture)
        self.committed = False
        self.rolled_back = False

    def cursor(self, *args, **kwargs) -> _FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


class ValidateCustomCodeTests(unittest.TestCase):
    def test_normalizes_lowercase_and_strips_separators(self) -> None:
        normalized, error = referral_service.validate_custom_code(" abcd-12_34 ")
        self.assertIsNone(error)
        self.assertEqual(normalized, "ABCD1234")

    def test_rejects_too_short(self) -> None:
        _normalized, error = referral_service.validate_custom_code("ab")
        self.assertIsNotNone(error)
        self.assertIn("at least", (error or "").lower())

    def test_rejects_too_long(self) -> None:
        _normalized, error = referral_service.validate_custom_code("A" * 21)
        self.assertIsNotNone(error)
        self.assertIn("at most", (error or "").lower())

    def test_rejects_empty_input(self) -> None:
        _normalized, error = referral_service.validate_custom_code("")
        self.assertIsNotNone(error)

    def test_rejects_punctuation_only(self) -> None:
        _normalized, error = referral_service.validate_custom_code("!@#")
        self.assertIsNotNone(error)

    def test_rejects_reserved(self) -> None:
        for word in ("ADMIN", "nadobro", "Support"):
            with self.subTest(word=word):
                _normalized, error = referral_service.validate_custom_code(word)
                self.assertIsNotNone(error)
                self.assertIn("reserved", (error or "").lower())


class ClaimReferralCodeTests(unittest.TestCase):
    def test_rejects_invalid_code_before_db_access(self) -> None:
        with patch.object(referral_service, "get_db") as get_db:
            ok, msg, row = referral_service.claim_referral_code(1, "ab")
        self.assertFalse(ok)
        self.assertIsNone(row)
        self.assertIn("at least", msg.lower())
        get_db.assert_not_called()

    def test_happy_path_inserts_row_and_commits(self) -> None:
        captured: dict = {}
        # Cursor returns: user lookup row, no-existing-code row (None), insert RETURNING row.
        conn = _FakeConn(
            [
                {"telegram_id": 1},
                None,
                {
                    "id": 9,
                    "public_code": "ALICE",
                    "code_prefix": "ALI",
                    "network": "mainnet",
                    "redemption_count": 0,
                    "max_redemptions": 2_147_483_647,
                    "created_at": "now",
                },
            ],
            captured,
        )
        with patch.object(referral_service, "get_db", return_value=conn), patch.object(
            referral_service, "put_db"
        ), patch.object(referral_service, "BOT_USERNAME", "Nadbro_bot"):
            ok, msg, row = referral_service.claim_referral_code(1, "alice")

        self.assertTrue(ok)
        self.assertEqual(msg, "Referral code claimed.")
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["public_code"], "ALICE")
        self.assertIn("https://t.me/Nadbro_bot", row["link"])
        self.assertIn("start=ref_ALICE", row["link"])
        self.assertTrue(conn.committed)
        # The INSERT params: (hash, public_code, prefix, created_by, referrer, network, note, max_redemptions)
        insert_params = captured["executed"][-1][1]
        self.assertEqual(insert_params[1], "ALICE")  # public_code
        self.assertEqual(insert_params[2], "ALI")  # code_prefix
        self.assertEqual(insert_params[3], 1)  # created_by
        self.assertEqual(insert_params[4], 1)  # referrer_user_id

    def test_existing_same_code_is_returned_idempotently(self) -> None:
        existing = {
            "id": 9,
            "public_code": "ALICE",
            "code_prefix": "ALI",
            "network": "mainnet",
            "redemption_count": 3,
            "max_redemptions": 2_147_483_647,
            "created_at": "earlier",
        }
        conn = _FakeConn([{"telegram_id": 1}, existing])
        with patch.object(referral_service, "get_db", return_value=conn), patch.object(
            referral_service, "put_db"
        ), patch.object(referral_service, "BOT_USERNAME", "Nadbro_bot"):
            ok, msg, row = referral_service.claim_referral_code(1, "alice")
        self.assertTrue(ok)
        self.assertEqual(msg, "You already own this code.")
        assert row is not None
        self.assertEqual(row["public_code"], "ALICE")
        self.assertTrue(conn.committed)

    def test_existing_different_code_is_immutable(self) -> None:
        existing = {
            "id": 9,
            "public_code": "ALICE",
            "code_prefix": "ALI",
            "network": "mainnet",
            "redemption_count": 0,
            "max_redemptions": 2_147_483_647,
            "created_at": "earlier",
        }
        conn = _FakeConn([{"telegram_id": 1}, existing])
        with patch.object(referral_service, "get_db", return_value=conn), patch.object(
            referral_service, "put_db"
        ), patch.object(referral_service, "BOT_USERNAME", "Nadbro_bot"):
            ok, msg, row = referral_service.claim_referral_code(1, "bobby")
        self.assertFalse(ok)
        self.assertIn("permanent", msg.lower())
        assert row is not None
        self.assertEqual(row["public_code"], "ALICE")

    def test_conflict_returns_taken_message(self) -> None:
        # Insert hits a unique conflict (another user owns the string).
        conn = _FakeConn(
            [
                {"telegram_id": 1},
                None,
                None,  # RETURNING yields no row on ON CONFLICT DO NOTHING.
            ]
        )
        with patch.object(referral_service, "get_db", return_value=conn), patch.object(
            referral_service, "put_db"
        ), patch.object(referral_service, "BOT_USERNAME", "Nadbro_bot"):
            ok, msg, row = referral_service.claim_referral_code(1, "TAKEN")
        self.assertFalse(ok)
        self.assertIsNone(row)
        self.assertIn("taken", msg.lower())
        self.assertTrue(conn.rolled_back)

    def test_missing_user_returns_error(self) -> None:
        conn = _FakeConn([None])
        with patch.object(referral_service, "get_db", return_value=conn), patch.object(
            referral_service, "put_db"
        ):
            ok, msg, row = referral_service.claim_referral_code(1, "alice")
        self.assertFalse(ok)
        self.assertIsNone(row)
        self.assertIn("user not found", msg.lower())


class AutoGenerateReferralCodeTests(unittest.TestCase):
    def test_returns_existing_code_without_minting(self) -> None:
        existing = {
            "id": 1,
            "public_code": "ALICE",
            "code_prefix": "ALI",
            "network": "mainnet",
            "redemption_count": 0,
            "max_redemptions": 2_147_483_647,
            "created_at": "now",
        }
        with patch.object(
            referral_service, "get_user_referral_code", return_value=existing
        ), patch.object(referral_service, "claim_referral_code") as claim:
            ok, msg, row = referral_service.auto_generate_referral_code(7)
        self.assertFalse(ok)
        assert row is not None
        self.assertEqual(row["public_code"], "ALICE")
        self.assertIn("permanent", msg.lower())
        claim.assert_not_called()

    def test_retries_until_unique(self) -> None:
        # First two attempts collide (conflict, no row); third succeeds.
        outcomes = [
            (False, "This code is already taken. Please pick a different one.", None),
            (False, "This code is already taken. Please pick a different one.", None),
            (
                True,
                "Referral code claimed.",
                {
                    "id": 2,
                    "public_code": "ZZZZ1234",
                    "code_prefix": "ZZZ",
                    "network": "mainnet",
                    "redemption_count": 0,
                    "max_redemptions": 2_147_483_647,
                    "created_at": "now",
                    "link": "https://t.me/Nadbro_bot?start=ref_ZZZZ1234",
                },
            ),
        ]
        with patch.object(referral_service, "get_user_referral_code", return_value=None), patch.object(
            referral_service, "claim_referral_code", side_effect=outcomes
        ), patch.object(referral_service, "_generate_plain_code", return_value="ZZZZ1234"):
            ok, msg, row = referral_service.auto_generate_referral_code(7)
        self.assertTrue(ok)
        assert row is not None
        self.assertEqual(row["public_code"], "ZZZZ1234")
        self.assertEqual(msg, "Referral code generated.")

    def test_gives_up_after_max_attempts(self) -> None:
        outcomes = [(False, "This code is already taken. Please pick a different one.", None)] * 5
        with patch.object(referral_service, "get_user_referral_code", return_value=None), patch.object(
            referral_service, "claim_referral_code", side_effect=outcomes
        ):
            ok, msg, row = referral_service.auto_generate_referral_code(7, max_attempts=5)
        self.assertFalse(ok)
        self.assertIsNone(row)
        self.assertIn("taken", msg.lower())


class RedeemReferralCodeTests(unittest.TestCase):
    def _invite_row(self, **overrides: Any) -> dict:
        row = {
            "id": 99,
            "referrer_user_id": 100,
            "code_type": "referral",
            "active": True,
            "revoked_at": None,
            "public_code": "ALICE",
        }
        row.update(overrides)
        return row

    def test_rejects_unknown_code(self) -> None:
        conn = _FakeConn([None])
        with patch.object(referral_service, "get_db", return_value=conn), patch.object(
            referral_service, "put_db"
        ):
            ok, msg = referral_service.redeem_referral_code(7, "tester", "UNKNOWN")
        self.assertFalse(ok)
        self.assertIn("not found", msg.lower())
        self.assertTrue(conn.rolled_back)

    def test_rejects_self_referral(self) -> None:
        invite = self._invite_row(referrer_user_id=7)
        conn = _FakeConn([invite])
        with patch.object(referral_service, "get_db", return_value=conn), patch.object(
            referral_service, "put_db"
        ):
            ok, msg = referral_service.redeem_referral_code(7, "tester", "ALICE")
        self.assertFalse(ok)
        self.assertIn("your own", msg.lower())
        self.assertTrue(conn.rolled_back)

    def test_rejects_already_linked_to_different_referrer(self) -> None:
        invite = self._invite_row(referrer_user_id=100)
        existing = {"referrer_user_id": 200}
        conn = _FakeConn([invite, existing])
        with patch.object(referral_service, "get_db", return_value=conn), patch.object(
            referral_service, "put_db"
        ):
            ok, msg = referral_service.redeem_referral_code(7, "tester", "ALICE")
        self.assertFalse(ok)
        self.assertIn("already linked", msg.lower())
        self.assertTrue(conn.rolled_back)

    def test_idempotent_when_same_referrer_already_linked(self) -> None:
        invite = self._invite_row(referrer_user_id=100)
        existing = {"referrer_user_id": 100}
        conn = _FakeConn([invite, existing])
        with patch.object(referral_service, "get_db", return_value=conn), patch.object(
            referral_service, "put_db"
        ):
            ok, msg = referral_service.redeem_referral_code(7, "tester", "ALICE")
        self.assertTrue(ok)
        self.assertIn("already linked", msg.lower())
        self.assertTrue(conn.committed)

    def test_happy_path_inserts_referral_and_bumps_count(self) -> None:
        invite = self._invite_row(referrer_user_id=100)
        conn = _FakeConn(
            [
                invite,  # SELECT invite
                None,    # SELECT referrals (no existing)
                {"id": 555},  # INSERT referrals RETURNING
                None,    # UPDATE invite_codes
            ]
        )
        with patch.object(referral_service, "get_db", return_value=conn), patch.object(
            referral_service, "put_db"
        ):
            ok, msg = referral_service.redeem_referral_code(7, "tester", "ALICE")
        self.assertTrue(ok)
        self.assertEqual(msg, "Referral linked.")
        self.assertTrue(conn.committed)
        executed_sql = [pair[0] for pair in conn.cursor_obj.capture["executed"]]
        self.assertTrue(any("UPDATE invite_codes" in s for s in executed_sql))
        self.assertTrue(any("INSERT INTO referrals" in s for s in executed_sql))

    def test_rejects_revoked_code(self) -> None:
        invite = self._invite_row(active=False)
        conn = _FakeConn([invite])
        with patch.object(referral_service, "get_db", return_value=conn), patch.object(
            referral_service, "put_db"
        ):
            ok, msg = referral_service.redeem_referral_code(7, "tester", "ALICE")
        self.assertFalse(ok)
        self.assertIn("no longer active", msg.lower())

    def test_rejects_non_referral_code_type(self) -> None:
        invite = self._invite_row(code_type="private_access")
        conn = _FakeConn([invite])
        with patch.object(referral_service, "get_db", return_value=conn), patch.object(
            referral_service, "put_db"
        ):
            ok, msg = referral_service.redeem_referral_code(7, "tester", "ALICE")
        self.assertFalse(ok)
        self.assertIn("not a referral", msg.lower())


class HelpersTests(unittest.TestCase):
    def test_referral_payload_normalization_strips_prefix(self) -> None:
        self.assertEqual(
            referral_service.normalize_referral_payload("ref_alice"), "ALICE"
        )
        self.assertEqual(
            referral_service.referral_start_payload("alice"), "ref_ALICE"
        )

    def test_bot_deep_link_uses_configured_bot_username(self) -> None:
        with patch.object(referral_service, "BOT_USERNAME", "MyBot"):
            link = referral_service.bot_deep_link("alice")
        self.assertEqual(link, "https://t.me/MyBot?start=ref_ALICE")


if __name__ == "__main__":
    unittest.main()
