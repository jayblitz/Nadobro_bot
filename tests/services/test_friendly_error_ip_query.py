"""Regression tests for ``NadoClient._friendly_error`` ip_query_only branching.

Background
==========

On 2026-05-27 a user's close-trade attempt failed with
``{"reason": "ip_query_only", "blocked": true}``. The previous user-facing
message led with "Your 1CT signer key is not linked on Nado" even though
``verify_linked_signer`` returned ``verified=True`` — the diagnostic suffix
was buried after the misleading lead.

These tests pin the new branching:

* verified=True   → IP-throttle copy, no link-your-key blame
* mismatch        → signer-mismatch copy with both addresses
* missing         → standard "link your key" copy
* error/unknown   → fall-back 3-step list with possible-throttle first
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from src.nadobro.venue.nado_client import NadoClient


def _make_client(verify_result: dict) -> NadoClient:
    """Build a NadoClient bypassing ``__init__`` so we don't hit eth_account.

    We only need ``_friendly_error`` + ``verify_linked_signer`` to be callable;
    the rest of the SDK state is irrelevant for this assertion.
    """
    client = NadoClient.__new__(NadoClient)
    client.private_key = None
    client.network = "mainnet"
    client.client = None
    client.address = "0xabc0000000000000000000000000000000000def"
    client.subaccount_hex = client.address + "00" * 12
    client.main_address = client.address
    client.acting_user_id = None
    client._initialized = False
    # Monkey-patch verify_linked_signer to return whatever the test wants.
    client.verify_linked_signer = lambda *a, **kw: dict(verify_result)  # type: ignore[assignment]
    return client


def test_verified_signer_does_not_blame_user():
    client = _make_client(
        {
            "verified": True,
            "current_signer": "0xabc0000000000000000000000000000000000def",
            "error": None,
        }
    )
    msg = client._friendly_error('{"reason": "ip_query_only", "blocked": true}')
    # Must NOT lead with "your 1CT signer key is not linked"
    assert "not linked" not in msg.lower()
    assert "verified" in msg.lower() or "linked correctly" in msg.lower() or "is linked" in msg.lower()
    # Must indicate transient / throttle nature so the user waits instead of
    # going to fix a non-broken thing.
    assert "throttle" in msg.lower() or "temporarily" in msg.lower() or "retry" in msg.lower()


def test_signer_mismatch_shows_both_addresses():
    client = _make_client(
        {
            "verified": False,
            "current_signer": "0x1234567890abcdef1234567890abcdef12345678",
            "error": None,
        }
    )
    msg = client._friendly_error('{"reason": "ip_query_only", "blocked": true}')
    assert "mismatch" in msg.lower()
    assert "0x12345678" in msg  # exchange's signer prefix
    assert "0xabc00000" in msg  # bot's signer prefix


def test_no_linked_signer_shows_setup_instructions():
    client = _make_client({"verified": False, "current_signer": None, "error": None})
    msg = client._friendly_error('{"reason": "ip_query_only", "blocked": true}')
    assert "not linked" in msg.lower()
    assert "1-click trading" in msg.lower()
    # Surface the bot's address so the user knows what to paste.
    assert "0xabc0000000000000000000000000000000000def" in msg.lower() or "0xabc00000" in msg.lower()


def test_verify_error_falls_back_to_three_step_list_with_throttle_first():
    client = _make_client(
        {
            "verified": False,
            "current_signer": None,
            "error": "Too Many Requests",
        }
    )
    msg = client._friendly_error('{"reason": "ip_query_only", "blocked": true}')
    # We can't verify — show the fall-back list. Throttle case must appear
    # FIRST since it's the most common cause now allowlisted IPs are the norm.
    lower = msg.lower()
    throttle_idx = lower.find("throttle")
    not_linked_idx = lower.find("not be linked")
    assert throttle_idx >= 0, msg
    assert not_linked_idx >= 0, msg
    assert throttle_idx < not_linked_idx, (
        "throttle reason must precede the 'not linked' reason in the fallback list"
    )
    # And the verify error must be surfaced as diagnostic context.
    assert "too many requests" in lower


def test_diagnostic_failure_does_not_crash():
    client = _make_client({})
    # Force verify_linked_signer to raise
    def boom(*args, **kwargs):
        raise RuntimeError("upstream dead")
    client.verify_linked_signer = boom  # type: ignore[assignment]
    msg = client._friendly_error('{"reason": "ip_query_only", "blocked": true}')
    # Falls through to the error branch without raising
    assert isinstance(msg, str) and len(msg) > 0
    assert "upstream dead" in msg.lower()
