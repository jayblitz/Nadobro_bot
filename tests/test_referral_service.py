import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services import referral_service


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
        with patch.object(
            referral_service,
            "query_one",
            return_value={"telegram_id": 1, "total_volume_usd": 9999.0},
        ), patch.object(referral_service, "_generated_code_count", return_value=0):
            ok, msg, row = referral_service.generate_referral_invite_code(1)

        self.assertFalse(ok)
        self.assertIsNone(row)
        self.assertIn("unlock", msg.lower())

    def test_generate_referral_invite_inserts_public_referral_code(self):
        captured = {}

        def _execute_returning(_sql, params):
            captured["params"] = params
            return {"id": 9, "public_code": "ABCDEFGH", "code_prefix": "ABC"}

        with patch.object(
            referral_service,
            "query_one",
            return_value={"telegram_id": 1, "total_volume_usd": 10000.0},
        ), patch.object(referral_service, "_generated_code_count", return_value=0), patch.object(
            referral_service, "_generate_plain_code", return_value="ABCDEFGH"
        ), patch.object(referral_service, "execute_returning", side_effect=_execute_returning), patch.dict(
            "os.environ", {"TELEGRAM_BOT_USERNAME": "Nadobro_bot"}
        ):
            ok, _msg, row = referral_service.generate_referral_invite_code(1)

        self.assertTrue(ok)
        self.assertEqual(row["public_code"], "ABCDEFGH")
        self.assertIn("start=ref_ABCDEFGH", row["link"])
        self.assertEqual(captured["params"][1], "ABCDEFGH")
        self.assertEqual(captured["params"][4], 1)


if __name__ == "__main__":
    unittest.main()
