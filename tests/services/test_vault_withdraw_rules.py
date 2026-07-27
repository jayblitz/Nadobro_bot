"""Vault withdraw rules: burn only what the venue says is UNLOCKED.

Root cause pinned here (2026-07-25): a user with 2.777688 NLP and a card reading
"Lockup: Unlocked" got ``{"error_code":2096,"error":"Do not have enough unlocked
NLP."}`` on every withdraw.

Two defects, both fixed here:

1. **Failure booked as zero.** ``nlp_locked_balances`` costs gateway weight 20
   against the per-user budget (8 rps / burst 24). The vault snapshot fires
   several weight-20 NLP queries per refresh, and after the 2026-07-18 deposit
   fix moved ``max_nlp_mintable`` first to win that race, THIS query became the
   one that got denied. ``_query_rest`` returns None on throttle, which the old
   code turned into ``locked=0 / unlocked=0 / no lock entries`` — no exception.
   ``get_nlp_position`` then fell back to the raw spot NLP balance, and with no
   lock rows the lockup timer read 0 → the card claimed the WHOLE position was
   "Unlocked" and a 100% withdraw tried to burn locked tokens.
2. **Float round-trip on a max burn.** ``int(round(float(U)/1e18 * 1e18))`` can
   land a few wei ABOVE the exact on-chain integer, which the venue also rejects
   with 2096. Burning "everything unlocked" now sends the venue's own integer.
"""

from unittest.mock import patch

from src.nadobro.vault import nlp_vault_service as svc


def _snap(**over):
    base = {
        "ready": True,
        "lp_balance": 2.777688,
        "lp_value_usdt0": 2.96,
        "lockup_seconds_remaining": 0,
        "unlocked_known": True,
        "lp_unlocked": 2.777688,
        "lp_locked": 0.0,
        "error": None,
    }
    base.update(over)
    return base


class _FakeClient:
    _initialized = True

    def __init__(self, locked_payload):
        self._locked = locked_payload
        self.burn_calls: list = []

    def get_nlp_locked_balances(self):
        return self._locked

    def burn_nlp(self, nlp_amount, *, amount_x18=None):
        self.burn_calls.append({"nlp_amount": nlp_amount, "amount_x18": amount_x18})
        return {"success": True, "digest": "0xdead"}


class _FakeUser:
    linked_signer_address = "0x" + "1" * 40

    class network_mode:  # noqa: N801 - mirrors the enum attr shape
        value = "mainnet"


def _withdraw(amount, snap, locked_payload):
    client = _FakeClient(locked_payload)
    with patch.object(svc, "get_user", return_value=_FakeUser()), patch.object(
        svc, "get_user_nado_client", return_value=client
    ), patch.object(svc, "get_user_vault_snapshot", return_value=snap), patch.object(
        svc, "_log_vault_event", lambda *a, **k: None
    ):
        return svc.withdraw_from_vault(1, amount), client


_ALL_UNLOCKED = {
    "ok": True,
    "balance_locked": 0.0,
    "balance_unlocked": 2.777688,
    "balance_unlocked_x18": 2777688000000000000,
    "locked_entries": [],
}
_PARTLY_LOCKED = {
    "ok": True,
    "balance_locked": 2.0,
    "balance_unlocked": 0.777688,
    "balance_unlocked_x18": 777688000000000000,
    "locked_entries": [{"amount": 2.0, "unlocked_at": 4102444800}],  # far future
}
_UNKNOWN = {  # throttled query
    "ok": False,
    "balance_locked": 0.0,
    "balance_unlocked": 0.0,
    "balance_unlocked_x18": 0,
    "locked_entries": [],
}


def test_full_burn_sends_exact_onchain_integer_not_a_float_roundtrip():
    """The 2096 trigger: a max burn must use the venue's own wei value, never
    int(round(float * 1e18)), which can overshoot the real balance."""
    result, client = _withdraw(2.777688, _snap(), _ALL_UNLOCKED)
    assert result["success"] is True
    assert client.burn_calls[0]["amount_x18"] == 2777688000000000000


def test_partial_burn_below_unlocked_uses_the_float_amount():
    result, client = _withdraw(0.5, _snap(), _ALL_UNLOCKED)
    assert result["success"] is True
    assert client.burn_calls[0]["amount_x18"] is None
    assert client.burn_calls[0]["nlp_amount"] == 0.5


def test_burning_more_than_unlocked_is_refused_locally_with_the_real_number():
    """Old behaviour: burn the full 2.777688 -> venue 2096. Now: refused up
    front, telling the user exactly how much is actually withdrawable."""
    result, client = _withdraw(2.777688, _snap(lp_unlocked=0.777688, lp_locked=2.0), _PARTLY_LOCKED)
    assert result["success"] is False
    assert "0.777688" in result["error"]
    assert "unlocks in" in result["error"]
    assert client.burn_calls == []          # no doomed venue round-trip


def test_nothing_unlocked_is_refused_and_never_hits_the_venue():
    nothing = {**_PARTLY_LOCKED, "balance_unlocked": 0.0, "balance_unlocked_x18": 0}
    result, client = _withdraw(1.0, _snap(lp_unlocked=0.0, lp_locked=2.777688), nothing)
    assert result["success"] is False
    assert "unlocked" in result["error"].lower()
    assert client.burn_calls == []


def test_unknown_unlocked_balance_still_attempts_the_burn():
    """A throttled locked-balance query must not permanently block a legitimate
    withdraw — we just refuse to *assume* the whole balance is burnable."""
    result, client = _withdraw(0.5, _snap(unlocked_known=False), _UNKNOWN)
    assert result["success"] is True
    assert len(client.burn_calls) == 1
    assert client.burn_calls[0]["amount_x18"] is None


def test_venue_2096_is_translated_into_plain_language():
    class _Rejecting(_FakeClient):
        def burn_nlp(self, nlp_amount, *, amount_x18=None):
            self.burn_calls.append({"nlp_amount": nlp_amount, "amount_x18": amount_x18})
            return {
                "success": False,
                "error": '{"status":"failure","error_code":2096,'
                         '"error":"Do not have enough unlocked NLP."}',
            }

    client = _Rejecting(_UNKNOWN)
    with patch.object(svc, "get_user", return_value=_FakeUser()), patch.object(
        svc, "get_user_nado_client", return_value=client
    ), patch.object(svc, "get_user_vault_snapshot", return_value=_snap(unlocked_known=False)), patch.object(
        svc, "_log_vault_event", lambda *a, **k: None
    ):
        result = svc.withdraw_from_vault(1, 0.5)
    assert result["success"] is False
    assert "2096" not in result["error"]           # no raw venue JSON
    assert "unlocks 4 days" in result["error"]


def test_is_insufficient_unlocked_error_matches_the_venue_payload():
    assert svc._is_insufficient_unlocked_error(
        '{"error_code":2096,"error":"Do not have enough unlocked NLP."}'
    )
    assert svc._is_insufficient_unlocked_error("Do not have enough unlocked NLP.")
    assert not svc._is_insufficient_unlocked_error("insufficient margin")
    assert not svc._is_insufficient_unlocked_error(None)
