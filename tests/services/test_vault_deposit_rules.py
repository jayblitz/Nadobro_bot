"""Vault deposit rules: the 70%-of-balance ceiling and throttle-tolerant gating.

Root cause pinned here (2026-07-18): the gateway max_nlp_mintable query costs
weight 20 against the per-user budget (8 rps / burst 24); when the vault
snapshot's call burst throttled it, the failure was booked as "mintable = $0"
and both the card (🔒 Margin in use) and the deposit gate ("Deposit room is
$0.00") blocked a user whose venue UI happily offered the deposit. Failure and
zero are now distinct, and the venue's own spot_leverage=false mint check
("fails if the transaction causes a borrow" — official docs) stays the final
arbiter when capacity is unknown.
"""

from unittest.mock import patch

import pytest

from src.nadobro.vault import nlp_vault_service as svc


def _snap(**over):
    base = {
        "ready": True,
        "usdt0_balance": 144.93,
        "lp_value_usdt0": 0.0,
        "deposit_room_usdt0": 0.0,
        "max_mintable_usdt0": 0.0,
        "mintable_known": False,
        "error": None,
    }
    base.update(over)
    return base


class _FakeClient:
    _initialized = True

    def __init__(self):
        self.mint_calls = []

    def mint_nlp(self, amount, *, spot_leverage=False):
        self.mint_calls.append((amount, spot_leverage))
        return {"success": True, "digest": "0xabc", "quote_amount_usdt0": amount}


class _FakeUser:
    linked_signer_address = "0x" + "1" * 40

    class network_mode:  # noqa: N801 - mirrors the enum attr shape
        value = "mainnet"


def _deposit(amount, snap, client=None):
    client = client or _FakeClient()
    with patch.object(svc, "get_user", return_value=_FakeUser()), patch.object(
        svc, "get_user_nado_client", return_value=client
    ), patch.object(svc, "get_user_vault_snapshot", return_value=snap), patch.object(
        svc, "_log_vault_event", lambda *a, **k: None
    ):
        return svc.deposit_to_vault(1, amount), client


def test_deposit_over_70pct_of_balance_is_rejected():
    result, client = _deposit(120.0, _snap())  # 120 > 70% of 144.93 (=101.45)
    assert result["success"] is False
    assert "70%" in result["error"]
    assert client.mint_calls == []


def test_deposit_within_70pct_proceeds_when_capacity_unknown():
    """A throttled capacity query must not block the mint — the venue's
    no-borrow check guards the execute."""
    result, client = _deposit(100.0, _snap())
    assert result["success"] is True
    assert client.mint_calls == [(100.0, False)]  # always no-borrow


def test_deposit_room_enforced_only_when_known():
    known = _snap(mintable_known=True, deposit_room_usdt0=50.0, max_mintable_usdt0=50.0)
    result, client = _deposit(100.0, known)
    assert result["success"] is False
    assert "Deposit room" in result["error"]
    assert client.mint_calls == []


def test_deposit_over_balance_still_rejected():
    result, client = _deposit(200.0, _snap())
    assert result["success"] is False
    assert "Insufficient" in result["error"]
    assert client.mint_calls == []


def test_ceiling_env_is_clamped():
    assert 1.0 <= svc.VAULT_DEPOSIT_MAX_PCT <= 100.0
    assert svc.VAULT_DEPOSIT_MAX_PCT == pytest.approx(70.0)
