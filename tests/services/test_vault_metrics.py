import json
from pathlib import Path

import pytest

from src.nadobro.services.vault_metrics_service import (
    annualize_apr_from_snapshots,
    build_lp_ledger_from_archive,
    compute_pnl_from_ledger,
    deposit_room_usdt0,
)
from src.nadobro.services.vault_deposit_watch_service import (
    should_notify_deposit_opening,
    user_eligible_for_deposit_watch,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nado"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def test_annualize_apr_from_snapshots():
    data = _load("nlp_snapshots_mainnet.json")
    apr, source = annualize_apr_from_snapshots(data["snapshots"])
    assert source == "snapshots"
    assert apr is not None
    assert apr > 0


def test_build_lp_ledger_from_archive():
    payload = _load("nlp_lp_events_mainnet.json")
    ledger = build_lp_ledger_from_archive(payload, nlp_product_id=11)
    assert len(ledger) == 2
    assert ledger[0]["event_type"] == "burn"
    assert ledger[1]["event_type"] == "mint"
    assert ledger[1]["quote_usdt0"] == pytest.approx(8.0, rel=1e-6)


def test_compute_pnl_from_ledger_basic_mint_then_burn():
    ledger = [
        {"event_type": "mint", "timestamp": 1, "quote_usdt0": 100.0, "nlp_amount": 100.0, "submission_idx": "1"},
        {"event_type": "mint", "timestamp": 2, "quote_usdt0": 100.0, "nlp_amount": 90.0, "submission_idx": "2"},
        {"event_type": "burn", "timestamp": 3, "quote_usdt0": 60.0, "nlp_amount": 50.0, "submission_idx": "3"},
    ]
    pnl = compute_pnl_from_ledger(ledger, current_lp_value_usdt0=150.0)
    assert pnl["total_deposited_usdt0"] == pytest.approx(200.0)
    assert pnl["total_withdrawn_usdt0"] == pytest.approx(60.0)
    # Average-cost: mints add 200 cost basis, burn removes 50/190 of remaining basis.
    expected_basis_after_burn = 200.0 * (1.0 - 50.0 / 190.0)
    assert pnl["cost_basis_usdt0"] == pytest.approx(expected_basis_after_burn, rel=1e-6)
    assert pnl["all_time_earned_usdt0"] == pytest.approx((150.0 + 60.0) - 200.0)
    assert pnl["unrealized_pnl_usdt0"] == pytest.approx(150.0 - expected_basis_after_burn, rel=1e-6)


def test_compute_pnl_full_exit_zeroes_basis():
    ledger = [
        {"event_type": "mint", "timestamp": 1, "quote_usdt0": 50.0, "nlp_amount": 50.0, "submission_idx": "1"},
        {"event_type": "burn", "timestamp": 2, "quote_usdt0": 55.0, "nlp_amount": 50.0, "submission_idx": "2"},
    ]
    pnl = compute_pnl_from_ledger(ledger, current_lp_value_usdt0=0.0)
    assert pnl["cost_basis_usdt0"] == pytest.approx(0.0)
    assert pnl["all_time_earned_usdt0"] == pytest.approx(5.0)
    assert pnl["unrealized_pnl_usdt0"] == pytest.approx(0.0)


def test_deposit_room_usdt0():
    assert deposit_room_usdt0(1000.0, 5000.0, 20000.0) == 5000.0
    assert deposit_room_usdt0(19500.0, 5000.0, 20000.0) == 500.0
    assert deposit_room_usdt0(20000.0, 5000.0, 20000.0) == 0.0


def test_should_notify_deposit_opening():
    assert should_notify_deposit_opening(0.0, 500.0) is True
    assert should_notify_deposit_opening(1.0, 100.0) is True
    assert should_notify_deposit_opening(500.0, 600.0) is False
    assert should_notify_deposit_opening(0.0, 50.0) is False


def test_user_eligible_for_deposit_watch():
    assert user_eligible_for_deposit_watch(0.0) is True
    assert user_eligible_for_deposit_watch(19999.0) is True
    assert user_eligible_for_deposit_watch(20000.0) is False
