"""The alert context must be scoped to the evaluation network.

Regression for the cross-network PnL alert bug: positions for PnL alerts were
fetched via ``get_user_readonly_client(user_id)`` — the user's CURRENT active
network — while evaluation was scoped to the alert worker's network, so a user
browsing testnet had their mainnet PnL alert judged against testnet positions.
"""
from __future__ import annotations

import asyncio


def test_build_alert_context_pins_positions_to_evaluation_network(monkeypatch):
    from src.nadobro.models import database as db_models
    from src.nadobro.runtime import scheduler
    from src.nadobro.users import user_service

    pnl_alert = {
        "id": 1,
        "user_id": 42,
        "condition": db_models.AlertCondition.PNL_ABOVE.value,
        "product_name": "BTC-PERP",
        "target_value": 10.0,
    }

    seen_scan_networks: list = []
    seen_client_networks: list = []

    def fake_get_all_active_alerts(network=None):
        seen_scan_networks.append(network)
        return [pnl_alert]

    class _FakeClient:
        def get_all_positions(self):
            return [{"product_name": "BTC-PERP", "unrealized_pnl": 12.5}]

    def fake_get_user_readonly_client(telegram_id, network=None):
        seen_client_networks.append(network)
        return _FakeClient()

    monkeypatch.setattr(db_models, "get_all_active_alerts", fake_get_all_active_alerts)
    monkeypatch.setattr(user_service, "get_user_readonly_client", fake_get_user_readonly_client)
    # No funding alerts in play — the funding branch needs _check_client anyway.
    monkeypatch.setattr(scheduler, "_check_client", None)

    funding_rates, positions_by_user = asyncio.run(scheduler._build_alert_context("mainnet"))

    # The alert scan and the position client are BOTH pinned to the
    # evaluation network, never the user's current browsing mode.
    assert seen_scan_networks == ["mainnet"]
    assert seen_client_networks == ["mainnet"]
    assert positions_by_user[42][0]["unrealized_pnl"] == 12.5
    assert funding_rates == {}
