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
    # NO_ORDERS_AUDIT-FIX-R4: spread_bp is the per-level STEP; a BUY grid's
    # band steps DOWN from mid so post-only buys never cross the book.
    # span = (levels - 1) * step = 1 * 0.0004 -> start = 100 * (1 - 0.0004).
    assert cfg["start_price"] == Decimal("99.96")
    assert cfg["end_price"] == Decimal("100")
    # Knobs for DynamicGridController side-correct band rebuilds.
    assert cfg["step_pct"] == Decimal("0.0004")
    assert cfg["levels_count"] == 2
    # long grid hard stop below: 100 * (1 - 0.005)
    assert cfg["limit_price"] == Decimal("99.5")
    tb = cfg["triple_barrier_config"]
    assert isinstance(tb, TripleBarrierConfig)
    assert tb.take_profit == Decimal("0.006") and tb.stop_loss == Decimal("0.005")


def test_map_dn_config_defaults():
    cfg = er.map_strategy_config(
        "dn",
        {"notional_usd": 500.0, "fixed_margin_usd": 500.0},
        Decimal(715), product="QQQ",
    )
    assert cfg["trading_pair_long"] == "QQQ-USDT0"   # spot leg (USDT0-quoted)
    assert cfg["trading_pair_short"] == "QQQ-PERP"    # perp leg
    assert cfg["leg_amount_quote"] == Decimal("500")
    assert cfg["hedge_ratio"] == Decimal("1")
    assert cfg["hold_seconds"] == 3600                # default 1h
    assert cfg["cycles"] == 1
    assert cfg["leverage"] == 1                       # strictly 1x short
    # Per-leg TP/SL are OFF by default — a one-sided TP would break the hedge.
    tb = cfg["barriers"]
    assert isinstance(tb, TripleBarrierConfig)
    assert tb.take_profit is None and tb.stop_loss is None


def test_map_dn_clamps_hold_and_sets_cycles():
    cfg = er.map_strategy_config(
        "dn",
        {"fixed_margin_usd": 250.0, "dn_hold_seconds": 999999.0,
         "dn_cycles": 5.0, "dn_cycle_gap_seconds": 45.0, "dn_hedge_ratio": 1.0},
        Decimal(100), product="AAPL",
    )
    assert cfg["hold_seconds"] == 86400               # clamped to 24h
    assert cfg["cycles"] == 5
    assert cfg["cycle_gap_seconds"] == 45


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


def test_map_vol_config_normalizes_dashed_and_bare_product():
    """``state.product`` may be stored as a bare base (current UI flow) or
    as a dashed pair (legacy + tests). Both must produce a canonical
    ``trading_pair`` the VolumeBotController accepts. Regression guard for
    the production crash where ``USDC`` / ``KBTC`` collided with the
    controller's hardcoded ``{"KBTC-USDC", "WETH-USDC"}`` set.
    """
    bare = er.map_strategy_config(
        "vol", {"notional_usd": 50.0, "interval_seconds": 30}, Decimal(100), product="KBTC",
    )
    assert bare["trading_pair"] == "KBTC"

    dashed = er.map_strategy_config(
        "vol", {"notional_usd": 50.0, "interval_seconds": 30}, Decimal(100), product="KBTC-USDC0",
    )
    # Suffix stripped via normalize_volume_spot_symbol.
    assert dashed["trading_pair"] == "KBTC"

    testnet_listing = er.map_strategy_config(
        "vol", {"notional_usd": 50.0, "interval_seconds": 30}, Decimal(100), product="QQQX",
    )
    assert testnet_listing["trading_pair"] == "QQQX"


def test_map_risk_limits():
    lim = er.map_risk_limits({"notional_usd": 100.0, "levels": 3, "session_notional_cap_usd": 0.0})
    assert lim.max_open_executors == 5            # levels + 2
    assert lim.max_single_order_quote == Decimal("100")
    assert lim.max_position_size_quote == Decimal("300")  # notional * levels fallback


def test_build_product_meta_from_catalog(monkeypatch):
    """Real catalog rows carry x18-scaled increments + isolated_only; the builder
    must convert them (not fall back to permissive 0.01/0.001/1 defaults) and
    flag perps vs spot. Regression for the bug where get_all_products_info()
    returned {"perp","spot"} (not a "products" list) so the builder yielded {}.
    """
    from src.nadobro.services import product_catalog as pc

    # x18-scaled: 0.5 tick, 0.001 lot, 5 min-notional.
    X18 = 10 ** 18
    perps = {
        "perps": {
            "QQQ": {
                "id": 7, "symbol": "QQQ-PERP", "isolated_only": True,
                "price_increment_x18": str(5 * X18 // 10),     # 0.5
                "size_increment_x18": str(X18 // 1000),        # 0.001
                "min_size_x18": str(5 * X18),                  # 5
            },
        }
    }
    spots = {
        "spots": {
            "QQQ-USDC0": {
                "id": 8, "symbol": "QQQ-USDC0",
                "price_increment_x18": str(X18 // 100),        # 0.01
                "size_increment_x18": str(X18 // 1000),        # 0.001
                "min_size_x18": str(X18),                      # 1
            },
        }
    }
    monkeypatch.setattr(pc, "get_catalog", lambda **kw: perps)
    monkeypatch.setattr(pc, "get_spot_catalog", lambda **kw: spots)

    class _Client:
        network = "testnet"

    meta = er.build_product_meta_from_catalog(_Client())

    # Perp resolves under base, canonical symbol, and BASE-PERP alias.
    for key in ("QQQ", "QQQ-PERP"):
        assert key in meta
    perp = meta["QQQ-PERP"]
    assert perp.product_id == 7
    assert perp.tick_size == Decimal("0.5")        # real increment, not 0.01 default
    assert perp.lot_size == Decimal("0.001")
    assert perp.min_notional == Decimal("5")
    assert perp.is_perp is True and perp.isolated_only is True

    spot = meta["QQQ-USDC0"]
    assert spot.product_id == 8 and spot.is_perp is False and spot.isolated_only is False


def test_dual_listed_base_keeps_spot_and_perp_addressable(monkeypatch):
    """Desk routing regression: a base with BOTH a perp and a spot listing
    (e.g. ETH) must keep the bare base on the perp (grid/MM expect that) AND
    expose a -SPOT alias on the spot product, or a spot 'buy 2 ETH' silently
    opens a perp."""
    from src.nadobro.services import product_catalog as pc

    X18 = 10 ** 18
    perps = {"perps": {"ETH": {
        "id": 100, "symbol": "ETH-PERP",
        "price_increment_x18": str(X18 // 100), "size_increment_x18": str(X18 // 1000),
        "min_size_x18": str(X18),
    }}}
    spots = {"spots": {"ETH": {
        "id": 200, "symbol": "ETH",
        "price_increment_x18": str(X18 // 100), "size_increment_x18": str(X18 // 1000),
        "min_size_x18": str(X18),
    }}}
    monkeypatch.setattr(pc, "get_catalog", lambda **kw: perps)
    monkeypatch.setattr(pc, "get_spot_catalog", lambda **kw: spots)

    class _Client:
        network = "testnet"

    meta = er.build_product_meta_from_catalog(_Client())

    # Bare base + -PERP -> the perp (unchanged behaviour for grid/MM).
    assert meta["ETH"].product_id == 100 and meta["ETH"].is_perp is True
    assert meta["ETH-PERP"].product_id == 100 and meta["ETH-PERP"].is_perp is True
    # -SPOT alias -> the SPOT product (the Desk routing fix).
    assert meta["ETH-SPOT"].product_id == 200 and meta["ETH-SPOT"].is_perp is False


def test_build_product_meta_handles_bad_catalog(monkeypatch):
    from src.nadobro.services import product_catalog as pc

    def _boom(**kw):
        raise RuntimeError("down")

    monkeypatch.setattr(pc, "get_catalog", _boom)
    monkeypatch.setattr(pc, "get_spot_catalog", _boom)

    class _Client:
        network = "testnet"

    assert er.build_product_meta_from_catalog(_Client()) == {}


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
