import unittest

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.handlers import formatters
from src.nadobro.handlers import keyboards


class TelegramUiFormatterTests(unittest.TestCase):
    def test_home_command_center_card_uses_sectioned_layout(self):
        text = formatters.fmt_home_command_center_card("mainnet", "$119.39")
        self.assertIn("🎯 *Nadobro Command Center*", text)
        self.assertIn("*Quick snapshot*", text)
        self.assertIn("*Your toolkit*", text)
        self.assertIn("└ 🌐 *Execution Mode*", text)
        self.assertIn("Q\\&A", text)

    def test_strategy_hub_intro_uses_tree_sections(self):
        text = formatters.fmt_strategy_hub_intro()
        self.assertIn("🧠 *Nadobro Strategy Lab*", text)
        self.assertIn("*Your toolkit*", text)
        self.assertIn("├ Open any strategy cockpit dashboard", text)
        self.assertIn("pre\\-trade analytics", text)

    def test_wallet_connect_card_escapes_private_key(self):
        text = formatters.fmt_wallet_connect_card("0xabc_def")
        self.assertIn("*Setup steps*", text)
        self.assertIn("`0xabc\\_def`", text)
        self.assertIn("1\\-Click Trading", text)

    def test_alert_menu_intro_uses_new_toolkit_copy(self):
        text = formatters.fmt_alert_menu_intro()
        self.assertIn("*Your toolkit*", text)
        self.assertIn("├ Create price alerts", text)
        self.assertIn("└ Track PnL triggers", text)

    def test_home_keyboard_emoji_semantics_match(self):
        self.assertEqual(keyboards.HOME_MODULE_EMOJIS["trade"], "🤖")
        self.assertTrue(keyboards.HOME_BTN_TRADE.startswith("🤖"))
        self.assertTrue(keyboards.HOME_BTN_ALERTS.startswith("🔔"))
        self.assertTrue(keyboards.HOME_BTN_MODE.startswith("🌐"))


if __name__ == "__main__":
    unittest.main()
