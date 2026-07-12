"""Guardrails for manual close ↔ open pairing (2026-07-09 prod audit).

``find_open_trade`` used to take the most recent filled row for the product
regardless of source or side, so a manual close could grab a live strategy
session's fill, mark it closed, and stamp the venue close price/PnL onto it —
observed on prod 2026-07-09 where four manual closes each corrupted a
different session's fill (sessions 114/116/117/118).
"""

import unittest
from unittest.mock import patch

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.models import database as db
from src.nadobro.trading import trade_service


class FindOpenTradeGuardrails(unittest.TestCase):
    def _capture(self):
        captured = {}

        def _q(sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return None

        return captured, _q

    def test_excludes_session_tagged_fills(self):
        captured, _q = self._capture()
        with patch.object(db, "query_one", side_effect=_q):
            db.find_open_trade(42, 2, network="mainnet")
        assert "strategy_session_id IS NULL" in captured["sql"]

    def test_side_filter_applied_when_given(self):
        captured, _q = self._capture()
        with patch.object(db, "query_one", side_effect=_q):
            db.find_open_trade(42, 2, network="mainnet", side="short")
        assert "side = %s" in captured["sql"]
        assert captured["params"][-1] == "short"


class RecordCloseSideMatching(unittest.TestCase):
    def test_close_pairs_with_opposite_side_manual_open(self):
        """A close ORDER on side X closes a position on the opposite side —
        the open lookup must ask for that position side, source-restricted."""
        calls = {}

        def _fake_find(telegram_id, product_id, network="mainnet", side=None):
            calls["side"] = side
            return None

        with patch.object(trade_service, "get_user", return_value=None), \
             patch.object(trade_service, "find_open_trade", side_effect=_fake_find), \
             patch.object(trade_service, "insert_trade", return_value=1), \
             patch.object(trade_service, "update_trade_stats", return_value=None):
            trade_service._record_close_in_db(
                42, 2, 0.035, 0.035, "long", client=None,
                fill_price=62984.0, network="mainnet",
                order_digest="0xclose",
            )
        # Buying (side='long') closes a SHORT position.
        assert calls["side"] == "short"


if __name__ == "__main__":
    unittest.main()
