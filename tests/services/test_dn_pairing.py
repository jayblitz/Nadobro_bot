"""DN-CUSTOM-ASSETS guardrail: wrapped RWA spot symbols must pair with their
perps so Delta Neutral isn't stuck on BTC/ETH.

Mainnet RWA tickers are QQQX / SPYX (the X is part of the name) and the spot
legs are wrapped (wQQQX / wSPYX). The original ``_dn_underlying_key`` reduced the
spot to QQQ/SPY while the perp stayed QQQX/SPYX, so the pair was silently
dropped. The fix indexes spots under broader candidate keys and falls back to
them when the exact underlying_key match misses.
"""
from __future__ import annotations

import src.nadobro.services.product_catalog as pc


def _patch_catalog(monkeypatch):
    monkeypatch.setattr(pc, "get_catalog", lambda network=None, client=None, refresh=False: {
        "perps": {
            "BTC":  {"id": 2,  "symbol": "BTC-PERP",  "trading_status": "live"},
            "QQQX": {"id": 20, "symbol": "QQQX-PERP", "trading_status": "live"},
            "SPYX": {"id": 22, "symbol": "SPYX-PERP", "trading_status": "live"},
        }
    })
    monkeypatch.setattr(pc, "get_spot_catalog", lambda network=None, refresh=False: {
        "spots": {
            "KBTC":  {"id": 1,  "symbol": "KBTC",  "underlying_key": "BTC", "trading_status": "live", "market_hours": None},
            "wQQQX": {"id": 11, "symbol": "wQQQX", "underlying_key": "QQQ", "trading_status": "live", "market_hours": None},
            "wSPYX": {"id": 13, "symbol": "wSPYX", "underlying_key": "SPY", "trading_status": "live", "market_hours": None},
        }
    })
    monkeypatch.setattr(pc, "_fetch_v2_symbols_map", lambda network: {})
    monkeypatch.setattr(pc, "_market_is_open", lambda row: True)


def test_wrapped_rwa_spots_pair_with_perps(monkeypatch):
    _patch_catalog(monkeypatch)
    cat = pc._build_dn_pair_catalog("mainnet")
    assert cat is not None
    pairs = cat["pairs"]
    # The whole point: QQQX and SPYX are now selectable for DN, not just BTC.
    assert {"BTC", "QQQX", "SPYX"} <= set(pairs)
    assert pairs["QQQX"]["spot_product_id"] == 11
    assert pairs["QQQX"]["perp_product_id"] == 20
    assert pairs["SPYX"]["spot_product_id"] == 13
    assert pairs["SPYX"]["perp_product_id"] == 22


def test_btc_still_pairs_via_exact_underlying_key(monkeypatch):
    """The broader candidate matching must not regress the exact-match path."""
    _patch_catalog(monkeypatch)
    cat = pc._build_dn_pair_catalog("mainnet")
    assert cat["pairs"]["BTC"]["spot_product_id"] == 1
    assert cat["pairs"]["BTC"]["perp_product_id"] == 2


def test_candidates_keep_trailing_x_form():
    """wQQQX must offer QQQX as a candidate (strip only the leading wrapper),
    which is what lets it meet the QQQX perp."""
    cands = pc._dn_pair_candidates("wQQQX")
    assert "QQQX" in cands
    # And the legacy testnet reduction is still present for the xStocks form.
    assert "QQQ" in cands
