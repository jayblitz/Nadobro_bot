import io
import logging
import unittest

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.services.log_redaction import (
    RedactingFormatter,
    SensitiveDataRedactFilter,
    redact_sensitive_text,
)


class LogRedactionTests(unittest.TestCase):
    def test_redacts_bot_tokens_addresses_subaccounts_and_account_ids(self):
        text = (
            "POST https://api.telegram.org/bot123456:ABC_def-GHI/getMe "
            "addr=0xac63eaedbbbb85afb7a42b1312b4982c23f14288 "
            "subaccount=0xac63eaedbbbb85afb7a42b1312b4982c23f1428864656661756c740000000000 "
            '{"account_id":1982353571057176576} '
            "host=aws-1-eu-north-1.pooler.supabase.com ip=51.21.189.77 "
            "machine=7849300be75398 listen_address=[fdaa:4b:a29c:a7b:4d6:fafa:718b:2]:22 "
            "digest=sha256:f5f2a2a50e6e9226fcede55cf72a0f5fd9ff898bba6d80a25b86e84805a76219"
        )

        redacted = redact_sensitive_text(text)

        self.assertIn("/bot<REDACTED>/getMe", redacted)
        self.assertNotIn("123456:ABC_def-GHI", redacted)
        self.assertNotIn("0xac63eaedbbbb85afb7a42b1312b4982c23f14288", redacted)
        self.assertNotIn("64656661756c740000000000", redacted)
        self.assertNotIn("1982353571057176576", redacted)
        self.assertNotIn("aws-1-eu-north-1.pooler.supabase.com", redacted)
        self.assertNotIn("51.21.189.77", redacted)
        self.assertNotIn("7849300be75398", redacted)
        self.assertNotIn("fdaa:4b:a29c:a7b:4d6:fafa:718b:2", redacted)
        self.assertNotIn("f5f2a2a50e6e9226fcede55cf72a0f5fd9ff898bba6d80a25b86e84805a76219", redacted)

    def test_redacting_formatter_sanitizes_exception_text(self):
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.addFilter(SensitiveDataRedactFilter())
        handler.setFormatter(RedactingFormatter("%(levelname)s:%(message)s"))
        logger = logging.getLogger("test.redaction")
        logger.handlers = [handler]
        logger.propagate = False
        logger.setLevel(logging.INFO)

        try:
            raise RuntimeError(
                "bad subaccount 0xac63eaedbbbb85afb7a42b1312b4982c23f1428864656661756c740000000000"
            )
        except RuntimeError:
            logger.exception("request failed for %s", "0xac63eaedbbbb85afb7a42b1312b4982c23f14288")

        output = stream.getvalue()
        self.assertIn("0x<REDACTED>", output)
        self.assertNotIn("0xac63eaedbbbb85afb7a42b1312b4982c23f14288", output)
        self.assertNotIn("64656661756c740000000000", output)

    def test_redaction_preserves_numeric_log_args(self):
        self.assertEqual(redact_sensitive_text(7.123456789012345), 7.123456789012345)

        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.addFilter(SensitiveDataRedactFilter())
        handler.setFormatter(RedactingFormatter("%(message)s"))
        logger = logging.getLogger("test.redaction.numeric")
        logger.handlers = [handler]
        logger.propagate = False
        logger.setLevel(logging.INFO)

        logger.info("Support answer generated in %.1fs", 7.123456789012345)

        self.assertIn("7.1s", stream.getvalue())


if __name__ == "__main__":
    unittest.main()
