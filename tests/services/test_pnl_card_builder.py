"""Tests for ``services.pnl_card_builder``: per-user data builder.

The builder is the single source of truth for the data dict the renderer
consumes. These tests lock in two contracts:

1. **Per-user scoping** — every DB query MUST filter by the requesting
   ``telegram_id``. The user must never see another user's stats.
2. **Number formatting** — the dict produced is consumable verbatim by
   ``services.pnl_card.generate_pnl_card`` (signed PnL, M/K shorthand,
   negative-cost fees).
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest

from src.nadobro.services import pnl_card_builder as builder


# ---------------------------------------------------------------------------
# Number formatting
# ---------------------------------------------------------------------------
class TestFormatters:
    def test_unsigned_dollar_under_million(self):
        assert builder._fmt_dollar(Decimal("0")) == "$0.00"
        assert builder._fmt_dollar(Decimal("1234.5")) == "$1,234.50"
        assert builder._fmt_dollar(Decimal("999999.99")) == "$999,999.99"

    def test_unsigned_dollar_million_shorthand(self):
        assert builder._fmt_dollar(Decimal("1000000")) == "$1.00M"
        assert builder._fmt_dollar(Decimal("1234567.89")) == "$1.23M"
        assert builder._fmt_dollar(Decimal("2_500_000")) == "$2.50M"

    def test_signed_dollar_always_has_sign(self):
        assert builder._fmt_signed_dollar(Decimal("0")) == "+$0.00"
        assert builder._fmt_signed_dollar(Decimal("234.56")) == "+$234.56"
        assert builder._fmt_signed_dollar(Decimal("-12.34")) == "-$12.34"
        # The renderer keys color off the leading sign char, so even tiny
        # losses must be rendered with the minus.
        assert builder._fmt_signed_dollar(Decimal("-0.01")) == "-$0.01"

    def test_negative_dollar_always_negative_for_costs(self):
        assert builder._fmt_negative_dollar(Decimal("0")) == "$0.00"
        assert builder._fmt_negative_dollar(Decimal("12.34")) == "-$12.34"
        # Even if the DB stores fees as a negative value, we still render
        # the absolute amount with a leading minus.
        assert builder._fmt_negative_dollar(Decimal("-12.34")) == "-$12.34"

    def test_to_decimal_handles_messy_inputs(self):
        assert builder._to_decimal(None) == Decimal("0")
        assert builder._to_decimal("") == Decimal("0")
        assert builder._to_decimal("1234.56") == Decimal("1234.56")
        assert builder._to_decimal(Decimal("3.14")) == Decimal("3.14")
        # Garbage in must produce $0 (not raise) so a flaky DB column
        # never breaks the share-card flow.
        assert builder._to_decimal("not-a-number") == Decimal("0")


# ---------------------------------------------------------------------------
# Symbol normalisation
# ---------------------------------------------------------------------------
class TestSymbolFormatting:
    def test_perp_default(self):
        assert builder._format_symbol("BTC", "mm") == "BTC-PERP"
        assert builder._format_symbol("ETH", "grid") == "ETH-PERP"
        assert builder._format_symbol("SOL", "vol") == "SOL-PERP"

    def test_already_qualified(self):
        assert builder._format_symbol("BTC-PERP", "mm") == "BTC-PERP"
        assert builder._format_symbol("ETH-USDC", "vol") == "ETH-USDC"

    def test_multi_passes_through(self):
        assert builder._format_symbol("MULTI", "bro") == "MULTI"

    def test_spot_symbols_pass_through(self):
        assert builder._format_symbol("KBTC", "vol") == "KBTC"
        assert builder._format_symbol("WETH", "vol") == "WETH"

    def test_empty_falls_back_to_btc_perp(self):
        assert builder._format_symbol(None, None) == "BTC-PERP"
        assert builder._format_symbol("", None) == "BTC-PERP"
        assert builder._format_symbol("   ", None) == "BTC-PERP"


# ---------------------------------------------------------------------------
# Per-user scoping — the most important contract.
# ---------------------------------------------------------------------------
class TestPerUserScoping:
    def test_latest_session_query_filters_by_user_and_network(self):
        """``build_pnl_card_data`` MUST filter every DB query by telegram_id
        + network so one user cannot see another user's stats.
        """
        captured_queries: list[tuple] = []

        def fake_query_one(sql: str, params=None):
            captured_queries.append((sql, params))
            return None

        with patch.object(builder, "query_one", side_effect=fake_query_one):
            builder.build_pnl_card_data(telegram_id=42, network="mainnet")

        # Two queries: latest session + active referral code.
        assert len(captured_queries) == 2

        session_sql, session_params = captured_queries[0]
        assert "strategy_sessions" in session_sql
        assert "user_id = %s" in session_sql
        assert "network = %s" in session_sql
        # First two params must be (user_id, network) — never reversed.
        assert session_params[0] == 42
        assert session_params[1] == "mainnet"

        referral_sql, referral_params = captured_queries[1]
        assert "invite_codes" in referral_sql
        assert "referrer_user_id = %s" in referral_sql
        assert referral_params[0] == 42

    def test_specific_session_query_also_scoped_to_user(self):
        captured: list[tuple] = []

        def fake_query_one(sql: str, params=None):
            captured.append((sql, params))
            return None

        with patch.object(builder, "query_one", side_effect=fake_query_one):
            builder.build_pnl_card_data(
                telegram_id=42, network="mainnet", session_id=999
            )

        # Specific session lookup must require BOTH session_id AND user_id —
        # this prevents a forged session_id from leaking another user's row.
        session_sql, params = captured[0]
        assert "id = %s" in session_sql
        assert "user_id = %s" in session_sql
        assert "network = %s" in session_sql
        assert 999 in params
        assert 42 in params
        assert "mainnet" in params


# ---------------------------------------------------------------------------
# End-to-end: dict shape consumable by the renderer.
# ---------------------------------------------------------------------------
class TestBuildPnLCardDataShape:
    def _stub_queries(self, session_row, referral_row):
        calls = {"n": 0}

        def fake(sql: str, params=None):
            calls["n"] += 1
            if calls["n"] == 1:
                return session_row
            return referral_row

        return fake

    def test_running_session_with_referral_code(self):
        session = {
            "id": 1,
            "user_id": 42,
            "network": "mainnet",
            "strategy": "mm",
            "product_name": "BTC",
            "total_volume_usd": Decimal("1234567.89"),
            "realized_pnl": Decimal("234.56"),
            "total_fees_paid": Decimal("12.34"),
            "status": "running",
        }
        referral = {"public_code": "NADO123"}

        with patch.object(
            builder, "query_one", side_effect=self._stub_queries(session, referral)
        ):
            data = builder.build_pnl_card_data(telegram_id=42, network="mainnet")

        assert data == {
            "symbol": "BTC-PERP",
            "strategy": "mm",
            "volume": "$1.23M",
            "net_fees": "-$12.34",
            "pnl": "+$234.56",
            "referral_code": "NADO123",
        }

    def test_negative_pnl_signed_correctly(self):
        session = {
            "user_id": 42,
            "network": "mainnet",
            "strategy": "vol",
            "product_name": "ETH",
            "total_volume_usd": Decimal("500"),
            "realized_pnl": Decimal("-99.99"),
            "total_fees_paid": Decimal("3.21"),
        }
        with patch.object(
            builder, "query_one", side_effect=self._stub_queries(session, None)
        ):
            data = builder.build_pnl_card_data(telegram_id=42, network="mainnet")
        assert data["pnl"] == "-$99.99"
        assert data["net_fees"] == "-$3.21"
        assert data["volume"] == "$500.00"

    def test_no_referral_omits_code_field_as_empty_string(self):
        # The renderer only draws the referral line when truthy, so empty
        # string must produce a clean card with no bottom-right text.
        session = {
            "user_id": 42,
            "network": "mainnet",
            "strategy": "bro",
            "product_name": "BTC",
            "total_volume_usd": Decimal("0"),
            "realized_pnl": Decimal("0"),
            "total_fees_paid": Decimal("0"),
        }
        with patch.object(
            builder, "query_one", side_effect=self._stub_queries(session, None)
        ):
            data = builder.build_pnl_card_data(telegram_id=42, network="mainnet")
        assert data["referral_code"] == ""

    def test_no_session_returns_empty_state_dict(self):
        # New user with no sessions: builder returns a valid all-zero dict so
        # the renderer still produces a card (with $0 values) instead of
        # crashing.
        with patch.object(
            builder, "query_one", side_effect=self._stub_queries(None, None)
        ):
            data = builder.build_pnl_card_data(telegram_id=42, network="mainnet")
        assert data["symbol"] == "BTC-PERP"  # default fallback
        assert data["volume"] == "$0.00"
        assert data["pnl"] == "+$0.00"
        assert data["net_fees"] == "$0.00"
        assert data["referral_code"] == ""


# ---------------------------------------------------------------------------
# End-to-end: builder output is consumable by the renderer without errors.
# ---------------------------------------------------------------------------
class TestBuilderFeedsRenderer:
    def test_renderer_accepts_builder_output(self):
        from src.nadobro.services import pnl_card

        session = {
            "user_id": 42,
            "network": "mainnet",
            "strategy": "mm",
            "product_name": "BTC",
            "total_volume_usd": Decimal("1234567.89"),
            "realized_pnl": Decimal("234.56"),
            "total_fees_paid": Decimal("12.34"),
        }
        referral = {"public_code": "NADO123"}

        calls = {"n": 0}

        def stub(sql, params=None):
            calls["n"] += 1
            return session if calls["n"] == 1 else referral

        with patch.object(builder, "query_one", side_effect=stub):
            data = builder.build_pnl_card_data(telegram_id=42, network="mainnet")

        out = pnl_card.generate_pnl_card(data)
        assert out.startswith(b"\x89PNG\r\n\x1a\n")
