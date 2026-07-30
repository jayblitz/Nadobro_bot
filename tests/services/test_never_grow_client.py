"""never_grow at the REAL NadoClient.place_order boundary (audit round 3).

The existing coverage only greps the source or drives a fake client, so it missed
that round 2's pre-submission gate ALSO refused marketable orders. That mattered
because the adapter converts a sub-min-notional spot exit to MARKET precisely
because market orders are NOT subject to the venue's resting minimum — so the
gate cancelled round 1's escape hatch and the spot leg could never be sold
(DN then pinned in CLOSING forever, the exact naked-leg outcome this branch
exists to prevent).

Contract:
  RESTING     + reducing  -> refuse, never signed (it cannot fill at that size)
  MARKETABLE  + reducing  -> submit  (this is how a sub-minimum leg gets out)
  anything    + opening   -> unchanged (the bump is how a small entry reaches the floor)
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from src.nadobro.venue.nado_client import NadoClient, _min_size_x18_cache

KBTC_PID = 1
MIN_NOTIONAL_X18 = 100 * 10 ** 18       # kBTC's real $100 minimum
SUB_MIN_SIZE = 0.00155                  # x 63890 = $99.03, just under it
PRICE = 63890.0


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("NADO_BUILDER_ID", "1")
    monkeypatch.setenv("NADO_BUILDER_FEE_RATE", "10")
    c = NadoClient.__new__(NadoClient)
    c._initialized = True
    c.client = object()
    c.network = "mainnet"
    c.subaccount_hex = "0xabc"
    _min_size_x18_cache[("mainnet", KBTC_PID)] = MIN_NOTIONAL_X18
    return c


def _place(client, *, order_type, never_grow, is_buy=False, size=SUB_MIN_SIZE):
    """Returns (result, reached_venue)."""
    reached = []

    def _fake_dispatch(*a, **k):
        reached.append(True)
        return {"digest": "0xok", "status": "filled"}

    with patch.object(NadoClient, "_gateway_allowed", lambda *a, **k: True), \
         patch.object(NadoClient, "_gateway_release", lambda *a, **k: None), \
         patch.object(NadoClient, "_dispatch_execute", _fake_dispatch, create=True), \
         patch.object(NadoClient, "_friendly_error", lambda self, e: str(e)):
        res = client.place_order(product_id=KBTC_PID, size=size, price=PRICE,
                                order_type=order_type, is_buy=is_buy,
                                never_grow=never_grow)
    return res, bool(reached)


def test_a_marketable_reducing_exit_IS_submitted(client):
    """THE round-3 regression. place_market_order calls in with order_type="ioc"
    and a real slippage-bounded price, so a never_grow-only gate refused it."""
    res, reached = _place(client, order_type="ioc", never_grow=True)
    assert not res.get("min_notional_block"), (
        "the marketable exit was refused client-side — the spot leg can never be "
        "sold and DN parks in CLOSING forever"
    )
    assert reached, "the order never reached the venue"


def test_a_resting_reducing_order_is_refused_not_grown(client):
    """It cannot fill below the minimum at any size, and growing it above the
    held balance is what stranded the leg in the first place."""
    res, reached = _place(client, order_type="default", never_grow=True)
    assert res.get("min_notional_block") is True
    assert res.get("success") is False
    assert not reached, "a doomed sub-minimum resting close was signed anyway"


def test_an_opening_order_is_still_bumped_to_the_floor(client):
    """Opens must keep the bump — that is how a small entry reaches the minimum."""
    res, reached = _place(client, order_type="default", never_grow=False, is_buy=True)
    assert not res.get("min_notional_block")
    assert reached


def test_a_reducing_order_ABOVE_the_minimum_is_never_blocked(client):
    res, reached = _place(client, order_type="default", never_grow=True, size=0.01)
    assert not res.get("min_notional_block")
    assert reached


@pytest.mark.parametrize("order_type", ["ioc", "fok", "market"])
def test_every_marketable_type_is_exempt(client, order_type):
    res, _ = _place(client, order_type=order_type, never_grow=True)
    assert not res.get("min_notional_block"), order_type
