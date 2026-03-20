import asyncio
import unittest

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.handlers.formatters import build_trade_preview_text
from src.nadobro.handlers.intent_handlers import _reply_md_safe


class _FakeMessage:
    def __init__(self):
        self.calls = []
        self._failed_once = False

    async def reply_text(self, text, parse_mode=None):
        self.calls.append({"text": text, "parse_mode": parse_mode})
        if parse_mode and not self._failed_once:
            self._failed_once = True
            from telegram.error import BadRequest

            raise BadRequest("Can't parse entities: character '.' is reserved and must be escaped")
        return True


class TextTradeParityTests(unittest.TestCase):
    def test_trade_preview_escapes_localized_est_margin_label(self):
        preview = build_trade_preview_text(
            action="short",
            product="XAG",
            size=2.0,
            price=31.25,
            leverage=10,
            est_margin=6.25,
            tp=29.0,
            sl=33.5,
        )
        self.assertIn("*Est\\. Margin:*", preview)
        self.assertIn("Take Profit", preview)
        self.assertIn("Stop Loss", preview)

    def test_reply_md_safe_falls_back_to_plain_text(self):
        msg = _FakeMessage()
        asyncio.run(_reply_md_safe(msg, "Test *Est\\. Margin:* value"))
        self.assertEqual(len(msg.calls), 2)
        self.assertEqual(msg.calls[0]["parse_mode"], "MARKDOWN_V2")
        self.assertIsNone(msg.calls[1]["parse_mode"])
        self.assertNotIn("\\", msg.calls[1]["text"])


if __name__ == "__main__":
    unittest.main()
