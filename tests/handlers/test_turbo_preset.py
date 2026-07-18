"""Turbo Volume preset — the coherent setting trio, per strategy.

The preset exists because leverage, the inventory allowance, and the session
SL interact: leverage scales uPnL as % of margin linearly (the rail's unit),
and the caps must fit at least one full-size fill or the book goes one-sided
after every fill. These tests pin the written values and the coherence rules.
"""

from _stubs import install_test_stubs

install_test_stubs()

from src.nadobro.handlers.strategy_handler import (  # noqa: E402
    _TURBO_LEVERAGE_DEFAULT,
    _TURBO_SESSION_SL_PCT,
    _replace_mm_preset,
    _turbo_preset_settings,
)


def test_mid_turbo_writes_the_coherent_trio():
    cfg = _turbo_preset_settings("mid", product_max_leverage=50.0)
    assert cfg["mm_leverage_override"] == 10          # min(10, 50)
    assert cfg["mm_quote_mode"] == "touch"
    assert cfg["inventory_soft_limit_usd"] == 0.0     # auto = deployed
    assert cfg["max_net_exposure_pct"] == 100.0       # one full-size fill fits
    assert cfg["sl_pct"] == _TURBO_SESSION_SL_PCT
    assert cfg["tp_pct"] == 0.0                       # no session profit-stop
    assert cfg["interval_seconds"] == 5
    assert cfg["spread_bp"] == 2.0
    assert cfg["mm_preset"] == "turbo"


def test_leverage_is_capped_by_the_product_max():
    assert _turbo_preset_settings("mid", 5.0)["mm_leverage_override"] == 5
    assert _turbo_preset_settings("mid", 1.0)["mm_leverage_override"] == 1
    assert _turbo_preset_settings("grid", 40.0)["mm_leverage_override"] == int(
        _TURBO_LEVERAGE_DEFAULT
    )


def test_rgrid_dgrid_write_their_own_sl_keys_and_keep_tp():
    """rgrid/dgrid rails read rgrid_stop_loss_pct; their TP and level
    mechanics must NOT be disturbed (position exits flow through barriers)."""
    for sid, spread_key in (("rgrid", "rgrid_spread_bp"), ("dgrid", "dgrid_spread_bp")):
        cfg = _turbo_preset_settings(sid, 50.0)
        assert cfg["rgrid_stop_loss_pct"] == _TURBO_SESSION_SL_PCT
        assert cfg[spread_key] == 3.0
        assert "rgrid_take_profit_pct" not in cfg
        assert "tp_pct" not in cfg
        assert "levels" not in cfg


def test_grid_keeps_tp_and_touch_mode_stays_mid_only():
    cfg = _turbo_preset_settings("grid", 50.0)
    assert cfg["sl_pct"] == _TURBO_SESSION_SL_PCT
    assert "tp_pct" not in cfg
    assert "mm_quote_mode" not in cfg
    assert "inventory_soft_limit_usd" not in cfg


def test_session_sl_survives_typical_noise_at_turbo_leverage():
    """Coherence invariant: at the turbo leverage, the session SL (% of
    margin) must tolerate at least a 0.5% adverse price move on full
    one-sided inventory — otherwise routine noise auto-stops the bot and
    volume collapses (a stopped bot prints zero fills)."""
    tolerated_price_move_pct = _TURBO_SESSION_SL_PCT / _TURBO_LEVERAGE_DEFAULT
    assert tolerated_price_move_pct >= 0.5


def test_tiny_after_turbo_removes_turbo_only_settings():
    cfg = {"notional_usd": 250.0}
    _replace_mm_preset(cfg, "mid", "turbo", _turbo_preset_settings("mid", 50.0))
    _replace_mm_preset(
        cfg,
        "mid",
        "tiny",
        {
            "mm_leverage_override": 3,
            "min_order_notional_usd": 20.0,
            "mm_collateral_safety_factor": 1.10,
        },
    )

    assert cfg["mm_preset"] == "tiny"
    assert cfg["mm_leverage_override"] == 3
    assert cfg["min_order_notional_usd"] == 20.0
    for key in (
        "mm_quote_mode",
        "inventory_soft_limit_usd",
        "max_net_exposure_pct",
        "tp_pct",
        "sl_pct",
        "spread_bp",
        "interval_seconds",
    ):
        assert key not in cfg
    assert cfg["notional_usd"] == 250.0


def test_turbo_after_tiny_removes_tiny_only_settings():
    cfg = {
        "mm_preset": "tiny",
        "mm_leverage_override": 3,
        "min_order_notional_usd": 20.0,
        "mm_collateral_safety_factor": 1.10,
        "notional_usd": 250.0,
    }
    _replace_mm_preset(cfg, "mid", "turbo", _turbo_preset_settings("mid", 50.0))

    assert cfg["mm_preset"] == "turbo"
    assert cfg["mm_leverage_override"] == 10
    assert "min_order_notional_usd" not in cfg
    assert "mm_collateral_safety_factor" not in cfg
    assert cfg["mm_quote_mode"] == "touch"
    assert cfg["notional_usd"] == 250.0


def test_standard_clears_active_preset_but_keeps_unrelated_settings():
    cfg = {"notional_usd": 250.0}
    _replace_mm_preset(cfg, "mid", "turbo", _turbo_preset_settings("mid", 50.0))
    _replace_mm_preset(cfg, "mid", "standard")

    assert cfg == {"notional_usd": 250.0, "mm_preset": "standard"}
