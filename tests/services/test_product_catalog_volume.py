"""Tests for the catalog-driven Volume-bot pair discovery.

These guard the per-execution-mode market parsing requested in the issue
"Volume bot doesn't pick all the Spot assets on Testnet". The previous
hardcoded ``VOLUME_SPOT_SYMBOLS = ("KBTC", "WETH", "USDC")`` made new spot
listings (QQQX, SPYX, kBTC, wETH on testnet) invisible to the bot. The new
implementation pulls from ``product_catalog.list_volume_spot_bases`` which
walks the live v2 spot catalog per network.
"""
from __future__ import annotations

import pytest

from src.nadobro.services import product_catalog as pc


def _spot_catalog(rows: list[dict]) -> dict:
    spots: dict[str, dict] = {}
    by_id: dict[int, str] = {}
    for row in rows:
        key = row["base"].upper()
        spots[key] = row
        by_id[int(row["id"])] = key
    return {"spots": spots, "by_id": by_id, "aliases": {}}


def test_list_volume_spot_bases_returns_live_testnet_listings(monkeypatch):
    """Testnet: kBTC, wETH, QQQx, SPYx (and USDC as a quote-like) -> only
    real tradeable bases come back, USDC/USDT0 are filtered out.
    """
    catalog = _spot_catalog(
        [
            {"id": 1, "symbol": "USDC", "base": "USDC", "trading_status": "live", "dynamic": True},
            {"id": 3, "symbol": "KBTC", "base": "KBTC", "trading_status": "live", "dynamic": True},
            {"id": 5, "symbol": "WETH", "base": "WETH", "trading_status": "live", "dynamic": True},
            {"id": 7, "symbol": "QQQX", "base": "QQQX", "trading_status": "live", "dynamic": True},
            {"id": 9, "symbol": "SPYX", "base": "SPYX", "trading_status": "live", "dynamic": True},
        ]
    )
    monkeypatch.setattr(pc, "get_spot_catalog", lambda network="testnet", refresh=False: catalog)
    bases = pc.list_volume_spot_bases("testnet")
    assert "USDC" not in bases  # quote-like, never tradeable as base
    assert bases == ["KBTC", "WETH", "QQQX", "SPYX"]  # stable order by product id


def test_list_volume_spot_bases_excludes_non_live(monkeypatch):
    catalog = _spot_catalog(
        [
            {"id": 1, "symbol": "KBTC", "base": "KBTC", "trading_status": "live"},
            {"id": 2, "symbol": "WETH", "base": "WETH", "trading_status": "delisted"},
            {"id": 3, "symbol": "QQQX", "base": "QQQX", "trading_status": "live",
             "market_hours": {"is_open": False}},
        ]
    )
    monkeypatch.setattr(pc, "get_spot_catalog", lambda network="testnet", refresh=False: catalog)
    bases = pc.list_volume_spot_bases("testnet")
    assert bases == ["KBTC"]  # WETH delisted, QQQX market closed


def test_list_volume_spot_bases_dedupes_symbols(monkeypatch):
    catalog = _spot_catalog(
        [
            {"id": 1, "symbol": "KBTC", "base": "KBTC", "trading_status": "live"},
            {"id": 2, "symbol": "KBTC", "base": "KBTC", "trading_status": "live"},
        ]
    )
    monkeypatch.setattr(pc, "get_spot_catalog", lambda network="testnet", refresh=False: catalog)
    assert pc.list_volume_spot_bases("testnet") == ["KBTC"]


def test_is_spot_catalog_dynamic_reflects_data_source(monkeypatch):
    live = _spot_catalog([{"id": 1, "symbol": "BTC", "base": "BTC", "dynamic": True, "trading_status": "live"}])
    static = _spot_catalog([{"id": 1, "symbol": "BTC", "base": "BTC", "dynamic": False, "trading_status": "live"}])
    monkeypatch.setattr(pc, "get_spot_catalog", lambda network="mainnet", refresh=False: live)
    assert pc.is_spot_catalog_dynamic("mainnet") is True
    monkeypatch.setattr(pc, "get_spot_catalog", lambda network="mainnet", refresh=False: static)
    assert pc.is_spot_catalog_dynamic("mainnet") is False


def test_get_spot_catalog_falls_back_to_stale_cache_when_live_unavailable(monkeypatch):
    """When the live v2 fetch returns nothing (Cloudflare 403 / outage), the
    previous cached catalog should be served for the stale-TTL window so the
    UI doesn't collapse to an empty list. Regression guard for the
    'Switch to mainnet' bug that fired whenever the catalog endpoint was
    blocked.
    """
    pc._spot_catalog_cache.clear()
    # Seed the cache with a live catalog.
    live_catalog = _spot_catalog(
        [{"id": 1, "symbol": "BTC", "base": "BTC", "trading_status": "live", "dynamic": True}]
    )
    monkeypatch.setattr(pc, "_build_dynamic_spot_catalog", lambda network: live_catalog)
    first = pc.get_spot_catalog("mainnet", refresh=True)
    assert "BTC" in first["spots"]

    # Now simulate the upstream going dark.
    monkeypatch.setattr(pc, "_build_dynamic_spot_catalog", lambda network: None)
    served = pc.get_spot_catalog("mainnet", refresh=True)
    # Should still get the previously-cached BTC, not the trimmed static fallback.
    assert "BTC" in served["spots"]
