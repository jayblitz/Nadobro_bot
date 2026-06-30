"""Unit tests for the engine_runtime settings->config mapping, risk-limit
mapping, product-metadata builder, and the feature gate. Pure logic — no venue."""
from __future__ import annotations

from decimal import Decimal

from src.nadobro.engine.types import TripleBarrierConfig
from src.nadobro.services import engine_runtime as er


def test_map_grid_config_centers_band_and_sets_barriers():
    # Classic static-ladder mapping (grid now defaults to fill-anchored; the
    # ladder is the opt-out escape via fill_anchored=0).
    cfg = er.map_strategy_config(
        "grid",
        {"notional_usd": 75.0, "cycle_notional_usd": 75.0, "spread_bp": 4.0,
         "levels": 2, "tp_pct": 0.6, "sl_pct": 0.5, "fill_anchored": 0},
        Decimal(100), product="BTC-USDC", leverage=3,
    )
    assert cfg["trading_pair"] == "BTC-USDC"
    assert cfg["min_spread_between_orders"] == Decimal("0.0004")
    assert cfg["max_open_orders"] == 2
    # SIZING (2026-06-21): deployed notional = margin x effective leverage, so a
    # $75 margin at 3x quotes $225 across the ladder (was margin-only $75).
    assert cfg["total_amount_quote"] == Decimal("225")
    assert cfg["leverage"] == 3
    # POST-ONLY-CROSS fix: the near-mid boundary is offset onto the maker side by
    # max(step/2, 1.5bp). step=0.0004 -> maker_offset=0.0002; span=1*0.0004.
    # BUY band steps DOWN from (mid - maker_offset): start=100*(1-0.0002-0.0004),
    # end=100*(1-0.0002).
    assert cfg["start_price"] == Decimal("99.94")
    assert cfg["end_price"] == Decimal("99.98")
    # Knobs for DynamicGridController side-correct band rebuilds.
    assert cfg["step_pct"] == Decimal("0.0004")
    assert cfg["levels_count"] == 2
    # GRID-DUAL-UNIT fix (f391f3c): limit_price is NO LONGER auto-derived from
    # sl_pct. A mid-anchored hard stop fired on a brief wick to mid*(1-sl) even
    # when little had filled — a premature stop-out. SL now lives in the
    # fill-aware avg-entry barrier (triple_barrier_config.stop_loss) + the
    # fee-aware session rail. limit_price stays available as an explicit
    # catastrophic stop but defaults to 0 (disabled).
    assert cfg["limit_price"] == Decimal("0")
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


def test_map_rgrid_band_is_above_mid_and_stop_in_barrier():
    # rgrid now defaults to fill-anchored trend-follow; this pins the CLASSIC
    # one-sided ladder, which is opt-out via fill_anchored=0.
    cfg = er.map_strategy_config(
        "rgrid", {"notional_usd": 100.0, "spread_bp": 10.0, "levels": 4, "sl_pct": 0.8,
                  "fill_anchored": 0},
        Decimal(100), product="BTC-USDC",
    )
    # A short grid's sell band steps UP from mid. POST-ONLY-CROSS fix: the nearest
    # sell is offset STRICTLY above mid (was == mid, which crossed the book and
    # the venue rejected with error_code 2008).
    assert cfg["start_price"] > Decimal("100")
    assert cfg["end_price"] > cfg["start_price"]
    # GRID-DUAL-UNIT fix (f391f3c): no mid-anchored hard limit_price stop; the
    # short's SL lives in the fill-aware barrier (avg-entry + sl_pct).
    assert cfg["limit_price"] == Decimal("0")
    tb = cfg["triple_barrier_config"]
    assert isinstance(tb, TripleBarrierConfig)
    assert tb.stop_loss == Decimal("0.008")  # 0.8% from avg entry, above for a short


def test_map_mid_config():
    cfg = er.map_strategy_config(
        "mid", {"notional_usd": 100.0, "spread_bp": 5.0, "levels": 2,
                "inventory_soft_limit_usd": 60.0},
        Decimal(100), product="BTC-USDC",
    )
    assert cfg["spread_bid_pct"] == Decimal("0.0005")
    assert cfg["spread_ask_pct"] == Decimal("0.0005")
    # Mid is one bid + one ask: full deployed notional per side (levels do NOT
    # subdivide the quote). $100 margin × 1x = $100.
    assert cfg["order_amount_quote"] == Decimal("100")
    assert cfg["max_base_quote"] == Decimal("60")


def test_participation_chunk_overrides_per_order_size():
    """mm_cycle_notional_usd (set by bot_runtime at start when a participation
    preset is active) replaces the deployed-based per-order size across the MM
    family; 0/unset keeps the deployed sizing."""
    # chunk 30 is distinct from every default (mid 100, grid/rgrid 100/4=25, dgrid 100).
    mid = er.map_strategy_config(
        "mid", {"notional_usd": 100.0, "spread_bp": 5.0, "mm_cycle_notional_usd": 30.0},
        Decimal(100), product="BTC-USDC",
    )
    assert mid["order_amount_quote"] == Decimal("30")
    for strat in ("grid", "rgrid"):
        cfg = er.map_strategy_config(
            strat, {"notional_usd": 100.0, "levels": 4, "fill_anchored": 1, "mm_cycle_notional_usd": 30.0},
            Decimal(100), product="BTC-USDC",
        )
        assert cfg["order_amount_quote"] == Decimal("30"), strat
    dg = er.map_strategy_config(
        "dgrid", {"notional_usd": 100.0, "mm_cycle_notional_usd": 30.0},
        Decimal(100), product="BTC-USDC",
    )
    assert dg["total_amount_quote"] == Decimal("30")
    # Opt-out: no chunk → deployed-based sizing unchanged.
    mid0 = er.map_strategy_config(
        "mid", {"notional_usd": 100.0, "spread_bp": 5.0}, Decimal(100), product="BTC-USDC",
    )
    assert mid0["order_amount_quote"] == Decimal("100")


def test_fill_anchored_reset_threshold_reads_ui_keys():
    """The UI writes the reset threshold under grid_reset_threshold_pct /
    rgrid_reset_threshold_pct, but the default (fill-anchored) grid/rgrid
    controller reads reset_threshold_pct. The mapping must translate the UI keys
    or the "Reset Threshold" button is a silent no-op (the controller would
    always use its 0.25% / 0.125% default). Regression guard for that bug."""
    # Grid: button writes grid_reset_threshold_pct=0.8 (%). Expect 0.8/100.
    grid = er.map_strategy_config(
        "grid", {"notional_usd": 100.0, "levels": 4, "fill_anchored": 1,
                 "grid_reset_threshold_pct": 0.8},
        Decimal(100), product="BTC-USDC",
    )
    assert grid["reset_threshold_pct"] == Decimal("0.8") / Decimal(100)
    # R-Grid: button writes rgrid_reset_threshold_pct=1.5 (%). Expect 1.5/100.
    rgrid = er.map_strategy_config(
        "rgrid", {"notional_usd": 100.0, "levels": 4, "fill_anchored": 1,
                  "rgrid_reset_threshold_pct": 1.5},
        Decimal(100), product="BTC-USDC",
    )
    assert rgrid["reset_threshold_pct"] == Decimal("1.5") / Decimal(100)
    # Unset → per-mode default preserved (grid 0.25% / rgrid 0.125%).
    grid_def = er.map_strategy_config(
        "grid", {"notional_usd": 100.0, "levels": 4, "fill_anchored": 1},
        Decimal(100), product="BTC-USDC",
    )
    assert grid_def["reset_threshold_pct"] == Decimal("0.25") / Decimal(100)
    rgrid_def = er.map_strategy_config(
        "rgrid", {"notional_usd": 100.0, "levels": 4, "fill_anchored": 1},
        Decimal(100), product="BTC-USDC",
    )
    assert rgrid_def["reset_threshold_pct"] == Decimal("0.125") / Decimal(100)


def test_dgrid_continuous_quoting_wiring():
    """D-Grid must (a) recycle completed levels so it keeps working the band and
    (b) leave the re-center threshold at 0 when the user hasn't pinned one, so
    the controller picks its band-width auto-follow default (50bp was too coarse
    and the grid 'placed a few orders and stopped'). An explicit value passes
    through."""
    dg = er.map_strategy_config(
        "dgrid", {"notional_usd": 100.0, "levels": 3, "dgrid_spread_bp": 8.0},
        Decimal(58000), product="BTC-USDC",
    )
    assert dg["recycle_levels"] is True
    assert dg["dgrid_reset_threshold_bp"] == 0.0  # -> controller auto-follow default
    # Explicit user reset (grid_reset_threshold_pct=0.8% -> 80bp) is honored.
    dg2 = er.map_strategy_config(
        "dgrid", {"notional_usd": 100.0, "levels": 3, "grid_reset_threshold_pct": 0.8},
        Decimal(58000), product="BTC-USDC",
    )
    assert dg2["dgrid_reset_threshold_bp"] == 80.0


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


def test_deployed_notional_is_margin_times_leverage():
    """SIZING (2026-06-21): grid/MM deploy margin x effective leverage. With no
    leverage it stays margin-sized (back-compat); with leverage it scales."""
    # Classic ladder (fill_anchored=0) exposes deployed as total_amount_quote.
    base = er.map_strategy_config(
        "grid", {"notional_usd": 100.0, "levels": 2, "fill_anchored": 0}, Decimal(100), product="BTC-PERP",
    )
    assert base["total_amount_quote"] == Decimal("100")   # eff_lev defaults to 1
    lev5 = er.map_strategy_config(
        "grid", {"notional_usd": 100.0, "levels": 2, "fill_anchored": 0}, Decimal(100), product="BTC-PERP", leverage=5,
    )
    assert lev5["total_amount_quote"] == Decimal("500")   # 100 x 5
    assert lev5["leverage"] == 5
    # An explicit mm_leverage_override (Tiny preset / Lev button) wins over the
    # session leverage.
    over = er.map_strategy_config(
        "grid", {"notional_usd": 100.0, "mm_leverage_override": 3, "levels": 2, "fill_anchored": 0},
        Decimal(100), product="BTC-PERP", leverage=10,
    )
    assert over["total_amount_quote"] == Decimal("300")   # override 3x beats 10x


def test_map_risk_limits_scale_with_leverage():
    """The per-order and position caps must follow the DEPLOYED notional or the
    Risk Engine rejects every leveraged order (the documented 'LIVE but 0 orders'
    failure)."""
    lim = er.map_risk_limits(
        {"notional_usd": 100.0, "levels": 3, "session_notional_cap_usd": 0.0}, "grid", leverage=5,
    )
    assert lim.max_single_order_quote == Decimal("500")          # deployed = 100 x 5
    assert lim.max_position_size_quote == Decimal("1500")        # deployed * levels


def test_map_risk_limits_dn_follows_leg_size_not_notional():
    """Regression: DN sizes legs from fixed_margin_usd, so its risk caps must
    follow that — not notional_usd. The old behavior capped single orders at
    notional_usd ($50) and rejected every $250 leg (LIVE but 0 orders)."""
    lim = er.map_risk_limits(
        {"fixed_margin_usd": 250.0, "notional_usd": 50.0, "dn_hedge_ratio": 1.0}, "dn"
    )
    # cap = leg(250) * hedge(1) * 2.0 headroom = 500
    assert lim.max_single_order_quote == Decimal("500")
    assert lim.max_position_size_quote == Decimal("500")
    assert lim.max_open_executors == 6
    # A 2x hedge ratio scales the cap with the larger short leg.
    lim2 = er.map_risk_limits({"fixed_margin_usd": 100.0, "dn_hedge_ratio": 2.0}, "dn")
    assert lim2.max_single_order_quote == Decimal("400")  # 100 * 2 * 2


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


def test_spread_is_per_strategy_user_set():
    """The quoting step must come from the strategy's own user-set spread, not a
    single hardcoded spread_bp for everyone."""
    from decimal import Decimal
    from src.nadobro.services.engine_runtime import map_strategy_config

    mid = Decimal("100")

    # rgrid honors rgrid_spread_bp (20bp), not the generic spread_bp (5). It now
    # defaults to fill-anchored, so the spread flows to the per-side quote band.
    rg = map_strategy_config(
        "rgrid", {"notional_usd": 100.0, "spread_bp": 5.0, "rgrid_spread_bp": 20.0, "levels": 2},
        mid, product="BTC-PERP",
    )
    assert rg["controller_override"] == "fill_anchored"
    assert rg["spread_ask_pct"] == Decimal("20.0") / Decimal(10000)
    # Classic ladder (opt-out) still honors rgrid_spread_bp as the ladder step.
    rg_classic = map_strategy_config(
        "rgrid",
        {"notional_usd": 100.0, "spread_bp": 5.0, "rgrid_spread_bp": 20.0, "levels": 2,
         "fill_anchored": 0},
        mid, product="BTC-PERP",
    )
    assert rg_classic["min_spread_between_orders"] == Decimal("20.0") / Decimal(10000)

    # dgrid honors dgrid_spread_bp (15bp), not the default 8.
    dg = map_strategy_config(
        "dgrid", {"notional_usd": 100.0, "dgrid_spread_bp": 15.0, "levels": 4},
        mid, product="BTC-PERP",
    )
    assert dg["step_pct"] == Decimal("15.0") / Decimal(10000)

    # grid (classic ladder, fill_anchored=0) still uses spread_bp as the step.
    g = map_strategy_config(
        "grid", {"notional_usd": 100.0, "spread_bp": 3.0, "levels": 2, "fill_anchored": 0},
        mid, product="BTC-PERP",
    )
    assert g["min_spread_between_orders"] == Decimal("3.0") / Decimal(10000)


def test_min_max_spread_bp_drive_auto_spread_bounds():
    """min_spread_bp / max_spread_bp (and dgrid_*) now set the per-side spread
    floor/cap (bps → fraction). Unset preserves the legacy 1.5bp / 50bp band."""
    from decimal import Decimal
    from src.nadobro.services.engine_runtime import map_strategy_config

    mid = Decimal("100")
    g = map_strategy_config(
        "grid", {"notional_usd": 100.0, "levels": 2, "min_spread_bp": 3.0, "max_spread_bp": 40.0},
        mid, product="BTC-PERP",
    )
    assert g["spread_floor_half_pct"] == Decimal("3.0") / Decimal(10000)
    assert g["spread_cap_half_pct"] == Decimal("40.0") / Decimal(10000)

    g0 = map_strategy_config("grid", {"notional_usd": 100.0, "levels": 2}, mid, product="BTC-PERP")
    assert g0["spread_floor_half_pct"] == Decimal("1.5") / Decimal(10000)
    assert g0["spread_cap_half_pct"] == Decimal("50.0") / Decimal(10000)

    m = map_strategy_config(
        "mid", {"notional_usd": 100.0, "levels": 2, "min_spread_bp": 4.0, "max_spread_bp": 20.0},
        mid, product="BTC-PERP",
    )
    assert m["spread_floor_half_pct"] == Decimal("4.0") / Decimal(10000)
    assert m["spread_cap_half_pct"] == Decimal("20.0") / Decimal(10000)

    dg = map_strategy_config(
        "dgrid", {"notional_usd": 100.0, "levels": 4, "dgrid_min_spread_bp": 5.0,
                  "dgrid_max_spread_bp": 30.0},
        mid, product="BTC-PERP",
    )
    assert dg["spread_floor_half_pct"] == Decimal("5.0") / Decimal(10000)
    assert dg["spread_cap_half_pct"] == Decimal("30.0") / Decimal(10000)

    # min > max can't invert the band — cap is held at the floor.
    inv = map_strategy_config(
        "grid", {"notional_usd": 100.0, "levels": 2, "min_spread_bp": 30.0, "max_spread_bp": 10.0},
        mid, product="BTC-PERP",
    )
    assert inv["spread_cap_half_pct"] == inv["spread_floor_half_pct"] == Decimal("30.0") / Decimal(10000)


def test_mid_directional_bias_scales_net_exposure_cap_and_clamps():
    """Mid directional bias is allowed up to +20% net-exposure headroom at |bias|=1
    (the documented '20% additional margin'), scaling linearly, and bias is
    clamped to [-1, 1]."""
    from decimal import Decimal
    from src.nadobro.services.engine_runtime import map_strategy_config

    mid = Decimal("100")
    neutral = map_strategy_config("mid", {"notional_usd": 100.0, "levels": 1}, mid, product="BTC-PERP")
    assert neutral["max_net_exposure_pct"] == 30.0          # default, no bias
    assert neutral["directional_bias"] == 0.0

    long_full = map_strategy_config(
        "mid", {"notional_usd": 100.0, "levels": 1, "directional_bias": 1.0}, mid, product="BTC-PERP",
    )
    assert abs(long_full["max_net_exposure_pct"] - 36.0) < 1e-9   # +20%
    assert long_full["directional_bias"] == 1.0

    half = map_strategy_config(
        "mid", {"notional_usd": 100.0, "levels": 1, "directional_bias": -0.5}, mid, product="BTC-PERP",
    )
    assert abs(half["max_net_exposure_pct"] - 33.0) < 1e-9        # +10% at |0.5|
    assert half["directional_bias"] == -0.5

    clamped = map_strategy_config(
        "mid", {"notional_usd": 100.0, "levels": 1, "directional_bias": 5.0}, mid, product="BTC-PERP",
    )
    assert clamped["directional_bias"] == 1.0
    assert abs(clamped["max_net_exposure_pct"] - 36.0) < 1e-9


def test_sl_tp_is_per_strategy_user_set():
    """rgrid/dgrid SL/TP come from rgrid_stop_loss_pct / rgrid_take_profit_pct
    (the fields the UI writes), not the generic sl_pct/tp_pct default."""
    from decimal import Decimal
    from src.nadobro.services.engine_runtime import map_strategy_config
    from src.nadobro.services.strategy_registry import effective_sl_tp_pct

    # resolver
    assert effective_sl_tp_pct("dgrid", {"rgrid_stop_loss_pct": 10.0, "rgrid_take_profit_pct": 50.0,
                                         "sl_pct": 0.8, "tp_pct": 1.2}) == (10.0, 50.0)
    assert effective_sl_tp_pct("rgrid", {"sl_pct": 0.8, "tp_pct": 1.2}) == (0.8, 1.2)  # fallback
    assert effective_sl_tp_pct("grid", {"sl_pct": 3.0, "tp_pct": 9.0,
                                        "rgrid_stop_loss_pct": 99.0}) == (3.0, 9.0)  # grid ignores rgrid_*

    # barrier honors the dgrid SL/TP the user set (10% / 50%).
    cfg = map_strategy_config(
        "dgrid",
        {"notional_usd": 100.0, "levels": 4, "rgrid_stop_loss_pct": 10.0,
         "rgrid_take_profit_pct": 50.0, "sl_pct": 0.8, "tp_pct": 1.2},
        Decimal("100"), product="BTC-PERP",
    )
    tb = cfg["triple_barrier_config"]
    assert tb.stop_loss == Decimal("10.0") / Decimal(100)
    assert tb.take_profit == Decimal("50.0") / Decimal(100)


def test_should_build_controller_truth_table():
    """The build gate must let the cycle-running worker ADOPT a controller it
    lacks locally (even against a stale remote row), while the non-worker
    fallback defers to a live owner so it never double-builds."""
    from src.nadobro.services.engine_runtime import _should_build_controller as B

    # Local FAILED controller -> always rebuild.
    assert B(needs_recovery=True, has_local_active=True, worker_mode=True, is_running=True) is True
    # Live local controller -> just tick.
    assert B(needs_recovery=False, has_local_active=True, worker_mode=False, is_running=True) is False
    # Worker with NO local controller -> adopt, even if remote says running
    # (crashed/recycled worker, or stale row). THIS is the no-orders fix.
    assert B(needs_recovery=False, has_local_active=False, worker_mode=True, is_running=True) is True
    # Non-worker (main fallback) with no local and a live remote owner -> defer
    # (no double build).
    assert B(needs_recovery=False, has_local_active=False, worker_mode=False, is_running=True) is False
    # Non-worker, no local, nobody running -> build (single-process happy path).
    assert B(needs_recovery=False, has_local_active=False, worker_mode=False, is_running=False) is True


def test_live_config_signature_ignores_mid_anchor_but_detects_leverage():
    """A price-only remap must not churn the live controller, but a leverage /
    deployed-notional edit must be visible before the next tick."""
    cfg_100 = er.map_strategy_config(
        "grid",
        {"notional_usd": 100.0, "levels": 2, "mm_leverage_override": 5},
        Decimal("100"),
        product="BTC-PERP",
        leverage=5,
    )
    cfg_110 = er.map_strategy_config(
        "grid",
        {"notional_usd": 100.0, "levels": 2, "mm_leverage_override": 5},
        Decimal("110"),
        product="BTC-PERP",
        leverage=5,
    )
    limits_5x = er.map_risk_limits(
        {"notional_usd": 100.0, "levels": 2, "mm_leverage_override": 5},
        "grid",
        leverage=5,
    )
    assert er._live_config_signature(cfg_100, limits_5x) == er._live_config_signature(cfg_110, limits_5x)

    cfg_1x = er.map_strategy_config(
        "grid",
        {"notional_usd": 100.0, "levels": 2, "mm_leverage_override": 1},
        Decimal("110"),
        product="BTC-PERP",
        leverage=1,
    )
    limits_1x = er.map_risk_limits(
        {"notional_usd": 100.0, "levels": 2, "mm_leverage_override": 1},
        "grid",
        leverage=1,
    )
    assert er._live_config_signature(cfg_100, limits_5x) != er._live_config_signature(cfg_1x, limits_1x)


def test_apply_live_mid_config_updates_order_size_and_risk_limits():
    import asyncio
    from types import SimpleNamespace

    from src.nadobro.engine.controllers.market_making import MarketMakingController

    old_settings = {"notional_usd": 100.0, "levels": 2, "mm_leverage_override": 5}
    new_settings = {"notional_usd": 100.0, "levels": 2, "mm_leverage_override": 1}
    old_cfg = er.map_strategy_config("mid", old_settings, Decimal("100"), product="BTC-PERP", leverage=5)
    new_cfg = er.map_strategy_config("mid", new_settings, Decimal("100"), product="BTC-PERP", leverage=1)
    old_limits = er.map_risk_limits(old_settings, "mid", leverage=5)
    new_limits = er.map_risk_limits(new_settings, "mid", leverage=1)

    class _Orch:
        def __init__(self):
            # Production ExecutorOrchestrator exposes RiskEngine as ``risk``.
            self.risk = SimpleNamespace(limits=old_limits)
            self.stopped = []

        async def stop(self, ex_id):
            self.stopped.append(ex_id)

    orch = _Orch()
    controller = MarketMakingController(
        user_id=7,
        configs=old_cfg,
        orchestrator=orch,
        adapter=object(),
        inventory=None,
        limits=old_limits,
        controller_id="mid:7:mainnet",
    )
    controller._bid_id = "bid-order"
    controller._ask_id = "ask-order"
    controller._bid_price = Decimal("99")
    controller._ask_price = Decimal("101")

    asyncio.run(
        er._apply_live_controller_update(
            "mid", controller, orch, new_cfg, new_limits, Decimal("100")
        )
    )

    assert controller.order_amount_quote == Decimal("100")   # full deployed (one bid+ask)
    assert controller.configs["leverage"] == 1
    assert controller.limits.max_single_order_quote == Decimal("100.0")
    assert orch.risk.limits.max_single_order_quote == Decimal("100.0")
    assert orch.stopped == ["bid-order", "ask-order"]
    assert controller._bid_id is None and controller._ask_id is None


def test_apply_live_fill_anchored_grid_refreshes_mm_quotes_and_risk_limits():
    import asyncio
    from types import SimpleNamespace

    from src.nadobro.engine.controllers.fill_anchored import FillAnchoredQuotingController

    old_settings = {
        "notional_usd": 100.0,
        "levels": 2,
        "fill_anchored": 1,
        "spread_bp": 5.0,
        "mm_leverage_override": 5,
    }
    new_settings = {
        "notional_usd": 100.0,
        "levels": 2,
        "fill_anchored": 1,
        "spread_bp": 20.0,
        "mm_leverage_override": 1,
        "reset_threshold_pct": 0.5,
    }
    old_cfg = er.map_strategy_config("grid", old_settings, Decimal("100"), product="BTC-PERP", leverage=5)
    new_cfg = er.map_strategy_config("grid", new_settings, Decimal("100"), product="BTC-PERP", leverage=1)
    old_limits = er.map_risk_limits(old_settings, "grid", leverage=5)
    new_limits = er.map_risk_limits(new_settings, "grid", leverage=1)

    class _Orch:
        def __init__(self):
            self.risk = SimpleNamespace(limits=old_limits)
            self.stopped = []

        async def stop(self, ex_id):
            self.stopped.append(ex_id)

    orch = _Orch()
    controller = FillAnchoredQuotingController(
        user_id=7,
        configs=old_cfg,
        orchestrator=orch,
        adapter=object(),
        inventory=None,
        limits=old_limits,
        controller_id="grid:7:mainnet",
    )
    controller._bid_id = "bid-order"
    controller._ask_id = "ask-order"
    controller._bid_price = Decimal("99")
    controller._ask_price = Decimal("101")

    asyncio.run(
        er._apply_live_controller_update(
            "grid", controller, orch, new_cfg, new_limits, Decimal("100")
        )
    )

    assert controller.order_amount_quote == Decimal("50")
    assert controller.spread_bid_pct == Decimal("0.002")
    assert controller.reset_threshold_pct == Decimal("0.005")
    assert orch.risk.limits.max_single_order_quote == Decimal("100.0")
    assert orch.stopped == ["bid-order", "ask-order"]
    assert controller._bid_id is None and controller._ask_id is None


def test_apply_live_grid_config_requotes_without_controller_stop():
    import asyncio
    from types import SimpleNamespace

    from src.nadobro.engine.controllers.grid_trading import build_grid_config
    from src.nadobro.engine.types import TradeType

    old_settings = {"notional_usd": 100.0, "levels": 2, "mm_leverage_override": 5, "fill_anchored": 0}
    new_settings = {"notional_usd": 100.0, "levels": 2, "mm_leverage_override": 1, "fill_anchored": 0}
    old_cfg = er.map_strategy_config("grid", old_settings, Decimal("100"), product="BTC-PERP", leverage=5)
    new_cfg = er.map_strategy_config("grid", new_settings, Decimal("100"), product="BTC-PERP", leverage=1)
    old_limits = er.map_risk_limits(old_settings, "grid", leverage=5)
    new_limits = er.map_risk_limits(new_settings, "grid", leverage=1)

    class _Executor:
        id = "grid-ex"
        open_side = TradeType.BUY

        def __init__(self):
            self.config = build_grid_config(old_cfg, TradeType.BUY)
            self.recentered = []

        async def recenter(self, start_price, end_price):
            self.recentered.append((start_price, end_price))

    class _Orch:
        def __init__(self, executor):
            self.risk = SimpleNamespace(limits=old_limits)
            self.executor = executor
            self.stopped = []

        def list(self, controller_id, active_only=True):
            return [self.executor]

        async def stop(self, ex_id):
            self.stopped.append(ex_id)

    executor = _Executor()
    orch = _Orch(executor)
    controller = SimpleNamespace(
        id="grid:7:mainnet",
        configs=old_cfg,
        limits=old_limits,
        trading_pair="BTC-PERP",
    )

    asyncio.run(
        er._apply_live_controller_update(
            "grid", controller, orch, new_cfg, new_limits, Decimal("100")
        )
    )

    assert executor.config.total_amount_quote == Decimal("100")
    assert executor.config.leverage == 1
    assert executor.recentered
    assert orch.stopped == []
    assert orch.risk.limits.max_single_order_quote == Decimal("100.0")
