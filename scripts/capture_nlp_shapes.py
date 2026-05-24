"""Capture NLP vault API response shapes for fixtures and parser development.

Usage:
    NADO_NETWORK=mainnet NADO_ADDRESS=0x... python scripts/capture_nlp_shapes.py
    TELEGRAM_ID=<id> python scripts/capture_nlp_shapes.py
"""
from __future__ import annotations

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _client():
    tid = os.environ.get("TELEGRAM_ID")
    if tid:
        from src.nadobro.services.user_service import get_user_nado_client

        c = get_user_nado_client(int(tid))
        if c is None:
            raise SystemExit("get_user_nado_client returned None")
        return c
    from src.nadobro.services.nado_client import NadoClient

    addr = os.environ["NADO_ADDRESS"]
    c = NadoClient.from_address(addr, network=os.environ.get("NADO_NETWORK", "mainnet"))
    c.initialize()
    return c


def dump(label: str, value) -> None:
    print(f"\n===== {label} =====")
    print(json.dumps(value, indent=2, default=str)[:8000])


def main() -> None:
    from src.nadobro.services.nado_archive import query_nlp_lp_events, query_nlp_snapshots

    c = _client()
    network = c.network
    sub = c.subaccount_hex

    dump("network/subaccount", {"network": network, "subaccount": sub})
    dump("get_nlp_pool_info", c.get_nlp_pool_info())
    dump("get_nlp_pool_stats", c.get_nlp_pool_stats())
    dump("get_nlp_position", c.get_nlp_position())
    dump("get_max_nlp_mintable", c.get_max_nlp_mintable(spot_leverage=False))
    dump("get_nlp_locked_balances", c.get_nlp_locked_balances())
    dump("nlp_snapshots", query_nlp_snapshots(network, count=5, granularity=86400))
    dump("nlp_lp_events", query_nlp_lp_events(network, sub, limit=3))


if __name__ == "__main__":
    main()
