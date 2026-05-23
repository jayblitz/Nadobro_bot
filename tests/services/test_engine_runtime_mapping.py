"""Unit tests for the engine_runtime settings->config mapping, risk-limit
mapping, product-metadata builder, and the feature gate. Pure logic — no venue."""
from __future__ import annotations

from decimal import Decimal

from src.nadobro.engine.types import TripleBarrierConfig
from src.nadobro.services import engine_runtime as er


def test_map_grid_config_centers_band_and_sets_barriers():
    cfg = er.map_strategy_config(
        "grid",
        {"notional_usd": 75.0, "cycle_notional_usd": 75.0, "spread_bp": 4.0,
         "levels": 2, "tp_pct": 0.6, "sl_pct": 0.5},
        Decimal(100), product="BTC-USDC", leverage=3,
    )
    assert cfg["trading_pair"] == "BTC-USDC"
    assert cfg["min_spread_between_orders"] == Decimal("0.0004")
    assert cfg["max_open_orders"] == 2
    assert cfg["total_amount_quote"] == Decimal("75")
    assert cfg["leverage"] == 3
    # band = 100 * 0.0004 * 2 = 0.08
    assert cfg["start_price"] == Decimal("99.92")
    assert cfg["end_price"] == Decimal("100.08")
    # long grid hard stop below: 100 * (1 - 0.005)
    assert cfg["limit_price"] == Decimal("99.5")
    tb = cfg["triple_barrier_config"]
    assert isinstance(tb, TripleBarrierConfig)
    assert tb.take_profit == Decimal("0.006") and tb.stop_loss == Decimal("0.005")


def test_map_rgrid_hard_stop_is_above_mid():
    cfg = er.map_strategy_config(
        "rgrid", {"notional_usd": 100.0, "spread_bp": 10.0, "levels": 4, "sl_pct": 0.8},
        Decimal(100), product="BTC-USDC",
    )
    assert cfg["limit_price"] == Decimal("100.8")  # above for a short grid


def test_map_mid_config():
    cfg = er.map_strategy_config(
        "mid", {"notional_usd": 100.0, "spread_bp": 5.0, "levels": 2,
                "inventory_soft_limit_usd": 60.0},
        Decimal(100), product="BTC-USDC",
    )
    assert cfg["spread_bid_pct"] == Decimal("0.0005")
    assert cfg["spread_ask_pct"] == Decimal("0.0005")
    assert cfg["order_amount_quote"] == Decimal("50")  # 100 / 2 levels
    assert cfg["max_base_quote"] == Decimal("60")


def test_map_vol_config_is_spot():
    cfg = er.map_strategy_config(
        "vol", {"notional_usd": 40.0, "interval_seconds": 60}, Decimal(100), product="KBTC-USDC",
    )
    assert cfg["market"] == "spot" and cfg["leverage"] == 1
    assert cfg["total_amount_quote"] == Decimal("40")
    assert cfg["total_duration"] == 240.0 and cfg["order_interval"] == 60.0


def test_map_risk_limits():
    lim = er.map_risk_limits({"notional_usd": 100.0, "levels": 3, "session_notional_cap_usd": 0.0})
    assert lim.max_open_executors == 5            # levels + 2
    assert lim.max_single_order_quote == Decimal("100")
    assert lim.max_position_size_quote == Decimal("300")  # notional * levels fallback


def test_build_product_meta_from_catalog():
    class _Client:
        def get_all_products_info(self):
            return [{"symbol": "BTC-USDC", "product_id": 2, "tick_size": "0.5",
                     "lot_size": "0.001", "min_notional": "5"}]

    meta = er.build_product_meta_from_catalog(_Client())
    assert "BTC-USDC" in meta
    pm = meta["BTC-USDC"]
    assert pm.product_id == 2 and pm.tick_size == Decimal("0.5")
    assert pm.min_notional == Decimal("5")


def test_build_product_meta_handles_bad_catalog():
    class _Bad:
        def get_all_products_info(self):
            raise RuntimeError("down")

    assert er.build_product_meta_from_catalog(_Bad()) == {}


def test_engine_v2_enabled_gate(monkeypatch):
    # BUG-SR-1 / BR-1 fix: default is now ON. The legacy dispatch path was
    # retired, so unset must mean enabled to avoid silently no-opping.
    monkeypatch.delenv("NADO_ENGINE_V2_RUNTIME", raising=False)
    assert er.engine_v2_enabled() is True
    monkeypatch.setenv("NADO_ENGINE_V2_RUNTIME", "true")
    assert er.engine_v2_enabled() is True
    monkeypatch.setenv("NADO_ENGINE_V2_RUNTIME", "0")
    assert er.engine_v2_enabled() is False
    monkeypatch.setenv("NADO_ENGINE_V2_RUNTIME", "false")
    assert er.engine_v2_enabled() is False
