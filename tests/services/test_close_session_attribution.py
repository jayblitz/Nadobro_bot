"""Unit tests for write-side close→session attribution (Layer 2 hardening).

`close_all_positions` flattens the WHOLE venue position and matches an arbitrary
open trade via `find_open_trade`, so the synthetic close must NOT inherit that
open's `strategy_session_id` when it is oversized — otherwise a small run books a
giant account-wide "close" (the session 47 / 51 corruption). See
`_resolve_close_session_id`.
"""
from __future__ import annotations

from src.nadobro.trading.trade_service import _resolve_close_session_id


def _open(session_id, size):
    return {"id": 1, "strategy_session_id": session_id, "fill_size": size}


def test_explicit_session_always_wins_even_if_oversized():
    # Caller knows the run and sized its own close — trust it.
    assert _resolve_close_session_id(77, _open(99, 0.0016), close_size=5.0) == 77


def test_inherits_when_close_matches_open_size():
    assert _resolve_close_session_id(None, _open(42, 0.01), close_size=0.01) == 42


def test_does_not_inherit_oversized_account_wide_flatten():
    # session 47 shape: opened 0.0016, account-wide close 0.02785 -> drop the tag.
    assert _resolve_close_session_id(None, _open(47, 0.0016), close_size=0.02785) is None


def test_inherits_within_one_percent_tolerance():
    # Close marginally larger than the open (fee/rounding drift) still inherits.
    assert _resolve_close_session_id(None, _open(5, 1.0), close_size=1.005) == 5


def test_no_open_trade_returns_none():
    assert _resolve_close_session_id(None, None, close_size=1.0) is None


def test_open_without_session_returns_none():
    assert _resolve_close_session_id(None, _open(None, 0.01), close_size=0.01) is None


def test_unknown_open_size_keeps_legacy_inherit():
    # Can't measure the open's size -> least-surprise: inherit (legacy behavior).
    assert _resolve_close_session_id(None, _open(8, 0), close_size=0.5) == 8


def test_size_falls_back_to_size_field():
    # open trade carries `size` rather than `fill_size`.
    open_trade = {"id": 1, "strategy_session_id": 13, "size": 0.02}
    assert _resolve_close_session_id(None, open_trade, close_size=0.02) == 13
    assert _resolve_close_session_id(None, open_trade, close_size=2.0) is None
