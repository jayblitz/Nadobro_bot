import sys
import types
import unittest

from _stubs import install_test_stubs

install_test_stubs()

if "openai" not in sys.modules:
    openai_mod = types.ModuleType("openai")

    class _OpenAI:
        def __init__(self, *args, **kwargs):
            pass

    openai_mod.OpenAI = _OpenAI
    sys.modules["openai"] = openai_mod

if "cryptography.fernet" not in sys.modules:
    crypto_mod = types.ModuleType("cryptography")
    fernet_mod = types.ModuleType("cryptography.fernet")

    class _Fernet:
        def __init__(self, *args, **kwargs):
            pass

        @staticmethod
        def generate_key():
            return b"0" * 44

        def encrypt(self, value):
            return value

        def decrypt(self, value):
            return value

    class _InvalidToken(Exception):
        pass

    fernet_mod.Fernet = _Fernet
    fernet_mod.InvalidToken = _InvalidToken
    crypto_mod.fernet = fernet_mod
    sys.modules["cryptography"] = crypto_mod
    sys.modules["cryptography.fernet"] = fernet_mod


class TradingBroUpgradeTests(unittest.TestCase):
    def test_classifier_treats_mm_build_question_as_learning(self):
        from src.nadobro.services.conversation_intent import classify_conversation_intent

        intent = classify_conversation_intent(
            "How can I build a working MM bot which runs on telegram and has GRID, RGRID, Dynamic GRID and Volume bot?"
        )

        self.assertEqual(intent.name, "learn")
        self.assertGreaterEqual(intent.confidence, 0.8)

    def test_managed_agent_does_not_launch_for_educational_strategy_question(self):
        from src.nadobro.services.managed_agent_service import _is_strategy_start_request

        self.assertFalse(
            _is_strategy_start_request(
                "How can I build a working MM bot which runs on telegram and has GRID and Dynamic GRID?"
            )
        )
        self.assertTrue(_is_strategy_start_request("start dgrid BTC 3x"))

    def test_trading_bro_question_preserves_user_message_for_router(self):
        from src.nadobro.services.knowledge_service import _question_for_routing
        from src.nadobro.services.trading_bro_service import build_trading_bro_question

        framed = build_trading_bro_question("How do I build an MM bot?", mode="strategy_design")

        self.assertIn("Mode: strategy_design", framed)
        self.assertEqual(_question_for_routing(framed), "How do I build an MM bot?")

    def test_source_policy_omits_generic_sources_and_keeps_live_provenance(self):
        from src.nadobro.services.knowledge_service import (
            _format_provenance_line,
            _should_include_provenance,
        )

        self.assertFalse(
            _should_include_provenance(
                "How do I build an MM bot?",
                "",
                ["https://docs.nado.xyz"],
            )
        )
        self.assertTrue(
            _should_include_provenance(
                "latest news on BTC",
                "[X/TWITTER RESULTS]\nSome result",
                ["https://x.com"],
            )
        )
        self.assertEqual(_format_provenance_line(["https://x.com"]), "Based on: X")

    def test_bro_answer_card_and_markdown_links_render_safely(self):
        from src.nadobro.handlers.formatters import fmt_bro_answer_card, format_ai_response

        rendered = format_ai_response("Read [Nado docs](https://docs.nado.xyz) and use **risk controls**.")
        self.assertIn("[Nado docs](https://docs.nado.xyz)", rendered)
        self.assertIn("*risk controls*", rendered)

        card = fmt_bro_answer_card("## Build Plan\n1. Add router\n- Test it", mode="strategy_design")
        self.assertIn("*Strategy Builder*", card)
        self.assertIn("1\\. Add router", card)
        self.assertIn("\\- Test it", card)

    def test_trading_bro_prompt_uses_cool_buddy_tone(self):
        from src.nadobro.services.trading_bro_service import build_trading_bro_question

        framed = build_trading_bro_question("Explain D-GRID", mode="educational_guide")

        self.assertIn("cool buddy", framed)
        self.assertIn("relaxed, helpful, confident", framed)

    def test_streaming_draft_updates_at_word_like_boundaries(self):
        from src.nadobro.handlers.messages import _should_update_streaming_draft

        self.assertTrue(_should_update_streaming_draft("This is a longer streamed chunk ", 0, 1.0, 2.0))
        self.assertFalse(_should_update_streaming_draft("This is a longer streamed chu", 0, 1.0, 2.0))


if __name__ == "__main__":
    unittest.main()
