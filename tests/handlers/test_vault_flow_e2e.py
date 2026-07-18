"""End-to-end vault flows: every tap from the Vault card to a signed mint/burn.

Drives the REAL handler (handle_vault_callback / handle_vault_text), the REAL
snapshot/service logic (get_user_vault_snapshot, deposit_to_vault,
withdraw_from_vault), and the REAL cards — against a faithful fake venue
client. Only the process boundaries are stubbed (user lookup, pool metrics,
archive ledger, deposit-watch rows).

The fake client mirrors the verified gateway semantics (2026-07-18 probes):
max_nlp_mintable ~= min(per-wallet cap remainder, no-borrow spendable), with
ok=False representing a gateway-budget throttle; mint_nlp enforces the
documented spot_leverage=false contract ("fails if the transaction causes a
borrow").
"""

import asyncio
from unittest.mock import patch

import pytest

from src.nadobro.handlers import vault_handler as vh
from src.nadobro.vault import nlp_vault_service as svc


# ── stubs ────────────────────────────────────────────────────────────────

class FakeVaultClient:
    _initialized = True
    subaccount_hex = "0x" + "ab" * 32

    def __init__(
        self,
        *,
        usdt0=144.93,
        lp_balance=0.0,
        nav=1.04,
        mintable_no_borrow=144.93,
        mintable_ok=True,
        mintable_with_borrow=None,
        lockup_ms=None,
        reject_mint=None,
    ):
        self.usdt0 = usdt0
        self.lp_balance = lp_balance
        self.nav = nav
        self.mintable_no_borrow = mintable_no_borrow
        self.mintable_ok = mintable_ok
        self.mintable_with_borrow = (
            mintable_with_borrow if mintable_with_borrow is not None else mintable_no_borrow
        )
        self.lockup_ms = lockup_ms
        self.reject_mint = reject_mint
        self.mints = []
        self.burns = []

    def initialize(self):
        return True

    def get_balance(self, **_):
        return {"exists": True, "balances": {0: self.usdt0}}

    def resolve_nlp_product_id(self):
        return 11

    def get_max_nlp_mintable(self, *, spot_leverage=False, product_id=None):
        if not self.mintable_ok:
            return {"ok": False, "max_mintable_usdt0": 0.0, "raw": {}}
        amount = self.mintable_with_borrow if spot_leverage else self.mintable_no_borrow
        return {"ok": True, "max_mintable_usdt0": float(amount), "raw": {}}

    def get_nlp_position(self):
        return {
            "exists": self.lp_balance > 0,
            "lp_balance": self.lp_balance,
            "lp_value_usdt0": self.lp_balance * self.nav,
            "last_mint_ts_ms": self.lockup_ms,
            "nlp_product_id": 11,
            "nav_usdt0": self.nav,
        }

    def mint_nlp(self, amount, *, spot_leverage=False):
        if self.reject_mint:
            return {"success": False, "error": self.reject_mint}
        self.mints.append((float(amount), bool(spot_leverage)))
        return {"success": True, "digest": "0x" + "1" * 64, "quote_amount_usdt0": amount}

    def burn_nlp(self, amount):
        self.burns.append(float(amount))
        return {"success": True, "digest": "0x" + "2" * 64, "nlp_amount": amount}


class FakeUser:
    linked_signer_address = "0x" + "3" * 40

    class network_mode:  # noqa: N801 - mirrors the enum attr shape
        value = "mainnet"


class FakeQuery:
    def __init__(self, data, user_id=7):
        self.data = data
        self.from_user = type("U", (), {"id": user_id})()
        self.edits = []
        self.answers = []

    async def edit_message_text(self, text, parse_mode=None, reply_markup=None):
        self.edits.append({"text": text, "kb": reply_markup, "parse_mode": parse_mode})

    async def answer(self, text=None, **_):
        self.answers.append(text)


class FakeMessage:
    def __init__(self, text):
        self.text = text
        self.replies = []

    async def reply_text(self, text, parse_mode=None, reply_markup=None):
        self.replies.append({"text": text, "kb": reply_markup})


class FakeUpdate:
    def __init__(self, text, user_id=7):
        self.message = FakeMessage(text)
        self.effective_user = type("U", (), {"id": user_id})()


class FakeContext:
    def __init__(self):
        self.user_data = {}


def _drive(coro):
    return asyncio.run(coro)


@pytest.fixture
def env(monkeypatch):
    """Patch the process boundaries; everything between them runs for real."""
    client = FakeVaultClient()
    holder = {"client": client}
    for mod in (svc, vh):
        monkeypatch.setattr(mod, "get_user", lambda *_a, **_k: FakeUser(), raising=False)
    monkeypatch.setattr(svc, "get_user_nado_client", lambda *_a, **_k: holder["client"])
    monkeypatch.setattr(svc, "get_pool_metrics", lambda *_a, **_k: {
        "tvl_usdt0": 9_099_631.40, "apr_pct": 7.59, "apr_source": "snapshots",
    })
    monkeypatch.setattr(svc, "_sync_lp_events_with_archive", lambda *_a, **_k: None)
    monkeypatch.setattr(svc, "_ledger_rows_for_pnl", lambda *_a, **_k: [])
    monkeypatch.setattr(svc, "get_vault_deposit_watch", lambda *_a, **_k: None)
    monkeypatch.setattr(svc, "_log_vault_event", lambda *_a, **_k: None)
    return holder


def _buttons(edit):
    kb = edit["kb"]
    return [btn for row in kb.inline_keyboard for btn in row] if kb else []


def _callbacks(edit):
    return [b.callback_data for b in _buttons(edit)]


# ── deposit flow ─────────────────────────────────────────────────────────

def test_deposit_happy_path_home_to_mint(env):
    ctx = FakeContext()

    q = FakeQuery("vault:home")
    _drive(vh.handle_vault_callback(q, ctx))
    home = q.edits[-1]
    assert "USDT0 balance: `$144.93`" in home["text"]
    assert "vault:deposit" in _callbacks(home)

    q = FakeQuery("vault:deposit")
    _drive(vh.handle_vault_callback(q, ctx))
    picker = q.edits[-1]
    assert "Max you can deposit now" in picker["text"]
    # 70% ceiling: min(no-borrow 144.93, 70% of 144.93 = 101.451, room)
    assert "$101.45" in picker["text"]

    q = FakeQuery("vault:deposit:preset:100.0")
    _drive(vh.handle_vault_callback(q, ctx))
    confirm = q.edits[-1]
    assert "Confirm Deposit" in confirm["text"]
    assert "Gas fee: `$1.00`" in confirm["text"]
    # (100 - 1) / 1.04 NAV = 95.192307...
    assert "95.192308" in confirm["text"]
    assert "4 days" in confirm["text"]
    assert "vault:deposit:confirm:100.0" in _callbacks(confirm)

    q = FakeQuery("vault:deposit:confirm:100.0")
    _drive(vh.handle_vault_callback(q, ctx))
    assert env["client"].mints == [(100.0, False)]  # exactly one, never borrows
    done = q.edits[-1]
    assert "Deposited $100.00" in done["text"]
    assert "4-day post-mint lockup" in done["text"]


def test_deposit_margin_locked_bounces_with_reason(env):
    env["client"] = FakeVaultClient(mintable_no_borrow=0.0, mintable_with_borrow=120.0)
    ctx = FakeContext()
    q = FakeQuery("vault:deposit")
    _drive(vh.handle_vault_callback(q, ctx))
    flash = q.edits[-1]
    assert "backing open positions" in flash["text"]
    assert "Max you can deposit now" not in flash["text"]
    assert env["client"].mints == []


def test_deposit_cap_reached_bounces_with_reason(env):
    env["client"] = FakeVaultClient(
        lp_balance=19_500.0, nav=1.03, mintable_no_borrow=0.0, mintable_with_borrow=0.0,
    )
    ctx = FakeContext()
    q = FakeQuery("vault:deposit")
    _drive(vh.handle_vault_callback(q, ctx))
    assert "Private Alpha" in q.edits[-1]["text"]


def test_deposit_unknown_capacity_reaches_picker_and_mints(env):
    """A throttled capacity read must not bounce the user — the picker is
    balance-ceiling bounded and the venue's no-borrow check guards the mint."""
    env["client"] = FakeVaultClient(mintable_ok=False)
    ctx = FakeContext()

    q = FakeQuery("vault:deposit")
    _drive(vh.handle_vault_callback(q, ctx))
    assert "Max you can deposit now" in q.edits[-1]["text"]

    q = FakeQuery("vault:deposit:confirm:100.0")
    _drive(vh.handle_vault_callback(q, ctx))
    assert env["client"].mints == [(100.0, False)]


def test_deposit_over_70pct_rejected_end_to_end(env):
    ctx = FakeContext()
    q = FakeQuery("vault:deposit:confirm:120.0")  # > 101.45 ceiling
    _drive(vh.handle_vault_callback(q, ctx))
    assert env["client"].mints == []
    assert "70%" in q.edits[-1]["text"]


def test_deposit_double_tap_mints_once(env):
    ctx = FakeContext()
    ctx.user_data[vh._OP_INFLIGHT_KEY] = "deposit"  # first tap still in flight
    q = FakeQuery("vault:deposit:confirm:100.0")
    _drive(vh.handle_vault_callback(q, ctx))
    assert env["client"].mints == []
    assert any("processing" in (a or "") for a in q.answers)
    # guard clears with the first operation, later taps work again
    ctx.user_data.pop(vh._OP_INFLIGHT_KEY)
    q2 = FakeQuery("vault:deposit:confirm:100.0")
    _drive(vh.handle_vault_callback(q2, ctx))
    assert env["client"].mints == [(100.0, False)]


def test_deposit_venue_rejection_surfaces_error(env):
    env["client"] = FakeVaultClient(reject_mint="Mint would cause a borrow on the subaccount")
    ctx = FakeContext()
    q = FakeQuery("vault:deposit:confirm:100.0")
    _drive(vh.handle_vault_callback(q, ctx))
    assert "borrow" in q.edits[-1]["text"]
    assert env["client"].mints == []


def test_deposit_custom_amount_text_flow(env):
    ctx = FakeContext()
    q = FakeQuery("vault:deposit:custom")
    _drive(vh.handle_vault_callback(q, ctx))
    assert ctx.user_data[vh._DEPOSIT_PENDING_KEY] == "deposit"

    bad = FakeUpdate("not-a-number")
    assert _drive(vh.handle_vault_text(bad, ctx)) is True
    assert "positive number" in bad.message.replies[-1]["text"]

    good = FakeUpdate("$1,00")  # "$1,00" -> "100"
    assert _drive(vh.handle_vault_text(good, ctx)) is True
    assert "Confirm Deposit" in good.message.replies[-1]["text"]
    assert vh._DEPOSIT_PENDING_KEY not in ctx.user_data


def test_deposit_custom_cancel_clears_pending(env):
    ctx = FakeContext()
    _drive(vh.handle_vault_callback(FakeQuery("vault:deposit:custom"), ctx))
    # ❌ Cancel routes to vault:home — the prompt must be forgotten
    _drive(vh.handle_vault_callback(FakeQuery("vault:home"), ctx))
    assert vh._DEPOSIT_PENDING_KEY not in ctx.user_data
    # a number typed later in normal chat is NOT hijacked into a confirm card
    later = FakeUpdate("500")
    assert _drive(vh.handle_vault_text(later, ctx)) is False
    assert later.message.replies == []


# ── withdraw flow ────────────────────────────────────────────────────────

def test_withdraw_happy_path_pct_to_burn(env):
    env["client"] = FakeVaultClient(lp_balance=100.0, nav=1.04)
    ctx = FakeContext()

    q = FakeQuery("vault:withdraw")
    _drive(vh.handle_vault_callback(q, ctx))
    picker = q.edits[-1]
    assert "NLP balance: `100.000000`" in picker["text"]
    assert any(cb.startswith("vault:withdraw:pct:") for cb in _callbacks(picker))

    q = FakeQuery("vault:withdraw:pct:50")
    _drive(vh.handle_vault_callback(q, ctx))
    confirm = q.edits[-1]
    assert "Burn `50.000000` NLP" in confirm["text"]
    # est out = 52.00; fee = $1 sequencer + max($1, 10bps of 52) = $2.00
    assert "Estimated USDT0 out (pre-fees): `$52.00`" in confirm["text"]
    assert "Estimated fees: `$2.00`" in confirm["text"]

    q = FakeQuery("vault:withdraw:confirm:50.0")
    _drive(vh.handle_vault_callback(q, ctx))
    assert env["client"].burns == [50.0]
    assert "Burned 50.000000 NLP" in q.edits[-1]["text"]


def test_withdraw_lockup_blocks_even_from_stale_keyboard(env):
    import time as _time

    env["client"] = FakeVaultClient(
        lp_balance=100.0, lockup_ms=int(_time.time() * 1000)  # minted just now
    )
    ctx = FakeContext()

    q = FakeQuery("vault:withdraw")
    _drive(vh.handle_vault_callback(q, ctx))
    picker = q.edits[-1]
    assert "lockup" in picker["text"].lower()
    assert not any(cb.startswith("vault:withdraw:pct:") for cb in _callbacks(picker))

    # A stale confirm button (pre-lockup keyboard) must still be rejected
    q = FakeQuery("vault:withdraw:confirm:50.0")
    _drive(vh.handle_vault_callback(q, ctx))
    assert env["client"].burns == []
    assert "Lockup active" in q.edits[-1]["text"]


def test_withdraw_over_balance_rejected(env):
    env["client"] = FakeVaultClient(lp_balance=10.0)
    ctx = FakeContext()
    q = FakeQuery("vault:withdraw:confirm:11.0")
    _drive(vh.handle_vault_callback(q, ctx))
    assert env["client"].burns == []
    assert "only have" in q.edits[-1]["text"]


def test_withdraw_custom_with_zero_nlp_is_friendly(env):
    ctx = FakeContext()
    _drive(vh.handle_vault_callback(FakeQuery("vault:withdraw:custom"), ctx))
    reply = FakeUpdate("1.25")
    assert _drive(vh.handle_vault_text(reply, ctx)) is True
    assert "don't have any NLP" in reply.message.replies[-1]["text"]


# ── snapshot semantics under the fake venue ─────────────────────────────

def test_snapshot_throttle_is_unknown_not_locked(env):
    env["client"] = FakeVaultClient(mintable_ok=False)
    snap = svc.get_user_vault_snapshot(7)
    assert snap["mintable_known"] is False
    assert snap["deposit_blocked_reason"] is None
    assert snap["deposit_max_usdt0"] == pytest.approx(144.93 * 0.7)


def test_snapshot_margin_locked_requires_successful_probes(env):
    env["client"] = FakeVaultClient(mintable_no_borrow=0.0, mintable_with_borrow=120.0)
    snap = svc.get_user_vault_snapshot(7)
    assert snap["deposit_blocked_reason"] == "margin_locked"
    assert snap["mintable_with_borrow_usdt0"] == pytest.approx(120.0)


def test_snapshot_vault_full_when_borrow_probe_also_zero(env):
    env["client"] = FakeVaultClient(mintable_no_borrow=0.0, mintable_with_borrow=0.0)
    snap = svc.get_user_vault_snapshot(7)
    assert snap["deposit_blocked_reason"] == "vault_full"
