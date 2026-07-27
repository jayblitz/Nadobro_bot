"""COPY-NO-SCALE-UP: mirror a leader ADDING to a position we already copy.

Reported 2026-07-27: a follower copied a leader's HYPE long at 16.6, the leader
later added 21.6 (position -> 36.6), and the bot placed nothing. It mirrored the
FIRST entry and then went deaf to the trade.

TWO compounding causes:

1. ``_partial_close_fraction`` returns 0.0 whenever ``new >= baseline``
   (explicitly, by its old design) and nothing else in ``_sync_mirror_positions``
   reacted to a growing leader position — there was no scale-up path at all.
2. The deeper one: the conviction weight is
   ``position_notional / leader_max_notional`` = 1.0 for a single-position
   leader, so the follower committed 100% of ``margin_per_trade`` on the
   leader's FIRST fill. Even with a scale-up path there was no headroom —
   the target size was CONSTANT no matter how large the leader grew.

Fix: the open commits only ``COPY_INITIAL_ENTRY_FRACTION`` (default 50%) of the
budget and the add mirrors the leader's growth RATIO, capped at the full
per-trade budget. ``margin_per_trade`` stays a HARD cap.

Worked example (the reported case, $100 budget @ 10x):
    open  leader 16.6 -> copy  8.338  ($50 of $100)
    add   leader 36.6 (x2.20) -> copy 16.675 ($100 of $100, capped)
"""
from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from src.nadobro.trading import copy_service as cs


def _drive(coro):
    return asyncio.run(coro)


def _cp(**over):
    """A tracked copy position: the follower's mirror of the leader's 16.6 HYPE."""
    base = {
        "id": 1, "product_id": 42, "product_name": "HYPE-PERP", "side": "LONG",
        "size": 8.338, "entry_price": 60.07, "leader_size": 16.6, "leverage": 10.0,
    }
    base.update(over)
    return base


def _leader(size, entry=59.97, leverage=10.0):
    return {"size": size, "entry_price": entry, "side": "LONG", "leverage": leverage}


class _Recorder:
    """Captures the order the scale-up places."""

    def __init__(self, ok=True):
        self.orders: list[dict] = []
        self.ok = ok

    def __call__(self, **kw):
        self.orders.append(kw)
        return {
            "success": self.ok,
            "fill_price": 59.97,
            "fill_size": kw.get("size"),
            "error": None if self.ok else "rejected",
        }


def _run_add(existing, leader, *, margin=100.0, max_lev=20.0, leader_max_notional=None,
             recorder=None, mirror_live=None):
    recorder = recorder or _Recorder()
    grown: list[tuple] = []
    notes: list[str] = []
    leader_max_notional = (
        leader_max_notional
        if leader_max_notional is not None
        else leader["size"] * leader["entry_price"]
    )

    async def _note(_uid, text, **_k):
        notes.append(text)

    with patch.object(cs, "execute_market_order", recorder), \
         patch.object(cs, "grow_copy_position", lambda *a: grown.append(a)), \
         patch.object(cs, "get_product_max_leverage", lambda *a, **k: 20.0), \
         patch.object(cs, "get_product_name", lambda *a, **k: "HYPE-PERP"), \
         patch.object(cs, "get_copy_mirror",
                      lambda *_a, **_k: (mirror_live if mirror_live is not None
                                         else {"active": True})), \
         patch.object(cs, "_notify_user", _note):
        outcome = _drive(cs._mirror_leader_add_if_needed(
            existing, leader, 7, 1, "mainnet",
            margin_per_trade=margin, max_leverage=max_lev,
            leader_max_notional=leader_max_notional,
        ))
    return outcome, recorder.orders, grown, notes


# ── the reported bug ────────────────────────────────────────────

def test_leader_add_is_mirrored_the_reported_hype_case():
    """16.6 -> 36.6 must place an order. Previously: nothing."""
    outcome, orders, grown, notes = _run_add(_cp(), _leader(36.6))
    assert outcome == "added"
    assert len(orders) == 1, "the leader's add must be mirrored with an order"
    order = orders[0]
    assert order["product"] == "HYPE"
    assert order["is_long"] is True
    assert order["size"] > 0
    # An ADD must NOT be reduce-only — that was the reduce path's contract.
    assert order.get("reduce_only") in (None, False)
    assert "Increased" in notes[0]


def test_add_moves_the_baseline_so_a_later_trim_is_measured_correctly():
    existing = _cp()
    _run_add(existing, _leader(36.6))
    assert existing["leader_size"] == 36.6
    # A subsequent trim back to 16.6 is now a real reduction, not a no-op.
    assert cs._partial_close_fraction(existing["leader_size"], 16.6) > 0


def test_copy_row_gets_size_weighted_average_entry():
    existing = _cp()
    _run_add(existing, _leader(36.6))
    # size grew, and entry is between the original and the add price
    assert existing["size"] > 8.338
    assert 59.97 <= existing["entry_price"] <= 60.07


# ── budget + noise guards ───────────────────────────────────────

def test_add_never_exceeds_the_per_trade_budget():
    """A leader who doubles down cannot push the follower past margin_per_trade.

    The conviction weight is capped at 1.0, so the target notional is at most
    margin_per_trade * leverage.
    """
    existing = _cp()
    outcome, orders, _, _ = _run_add(
        existing, _leader(500.0), margin=100.0, max_lev=10.0,
        # leader's largest position == this one -> weight pinned at 1.0
        leader_max_notional=500.0 * 59.97,
    )
    if outcome == "added":
        total_notional = existing["size"] * existing["entry_price"]
        assert total_notional <= 100.0 * 10.0 * 1.02   # budget * lev (+rounding)


def test_dust_adds_are_ignored_as_noise():
    """An add under SCALE_UP_MIN_DELTA_PCT must not churn an order."""
    tiny = 16.6 * (1 + (cs.SCALE_UP_MIN_DELTA_PCT - 1.0) / 100.0)
    outcome, orders, _, _ = _run_add(_cp(), _leader(tiny))
    assert outcome == ""
    assert orders == []


def test_leader_trim_is_not_handled_here():
    outcome, orders, _, _ = _run_add(_cp(), _leader(8.0))
    assert outcome == "" and orders == []


def test_unknown_baseline_is_inert():
    outcome, orders, _, _ = _run_add(_cp(leader_size=0.0), _leader(36.6))
    assert outcome == "" and orders == []


# ── safety ──────────────────────────────────────────────────────

def test_stopped_mirror_never_places_an_add():
    """A stop/pause landing mid-sync must win over a new order."""
    for live in ({"active": False}, {"active": True, "stop_requested": True},
                 {"active": True, "paused": True}):
        outcome, orders, _, _ = _run_add(_cp(), _leader(36.6), mirror_live=live)
        assert outcome == "" and orders == [], f"placed an order while {live}"


def test_pending_maker_open_blocks_the_add():
    """Never stack an add on top of an unresolved maker open (double exposure)."""
    cs._PENDING_MAKER_OPENS[(1, 42)] = {"digest": "0xabc"}
    try:
        outcome, orders, _, _ = _run_add(_cp(), _leader(36.6))
        assert outcome == "" and orders == []
    finally:
        cs._PENDING_MAKER_OPENS.pop((1, 42), None)


def test_failed_order_leaves_the_row_untouched():
    existing = _cp()
    outcome, _orders, grown, _ = _run_add(
        existing, _leader(36.6), recorder=_Recorder(ok=False)
    )
    assert outcome == ""
    assert grown == []
    assert existing["size"] == 8.338 and existing["leader_size"] == 16.6


def test_scale_up_can_be_disabled_by_flag():
    with patch.object(cs, "SCALE_UPS_ENABLED", False):
        outcome, orders, _, _ = _run_add(_cp(), _leader(36.6))
    assert outcome == "" and orders == []


# ── idempotency under the 30s poll loop ─────────────────────────

def test_repeated_polls_do_not_churn_orders():
    """The poller re-runs every 30s with the SAME leader snapshot. After one
    add, further polls must place nothing — otherwise the follower would stack
    orders every cycle and blow through the budget."""
    existing = _cp()
    first, orders1, _, _ = _run_add(existing, _leader(36.6))
    assert first == "added" and len(orders1) == 1

    for _ in range(10):
        outcome, orders, _, _ = _run_add(existing, _leader(36.6))
        assert outcome == "" and orders == [], "re-polled the same leader and re-ordered"


def test_incremental_adds_converge_to_the_budget_cap_without_exceeding_it():
    """A leader scaling in over many polls must walk the follower UP to the cap
    and stop there — never past it."""
    existing = _cp()
    ceiling = 16.675          # $100 budget @ 10x / 59.97
    for leader_size in (20.0, 25.0, 30.0, 36.6, 50.0, 80.0):
        _run_add(existing, _leader(leader_size))
        assert existing["size"] <= ceiling * 1.001, (
            f"exceeded budget cap at leader {leader_size}: {existing['size']}"
        )
    assert existing["size"] == pytest.approx(ceiling, rel=1e-3), "should reach the cap"
