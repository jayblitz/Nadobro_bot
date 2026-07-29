"""Static guard: every whitelisted strategy field has a limits entry.

AUDIT-MM-2026-07-14 #1: ``mm_cross_after_seconds`` was added to
``allowed_numeric_fields`` without a ``limits`` entry — every Cross button
then crashed on ``limits[field]``. The handler now rejects such fields
defensively, but a silent reject is still a dead button, so this test pins
the invariant at the source level: the whitelist must be a subset of the
limits keys (and of the custom-input help so ✍️ paths stay reachable).
"""
import ast
import pathlib

SRC = pathlib.Path("src/nadobro/handlers/strategy_handler.py").read_text()
TREE = ast.parse(SRC)


def _string_elts(node) -> set[str]:
    return {e.value for e in getattr(node, "elts", []) if isinstance(e, ast.Constant)}


def _collect():
    allowed: set[str] = set()
    limit_keys: set[str] = set()
    input_allowed: set[str] = set()
    help_keys: set[str] = set()
    for node in ast.walk(TREE):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            name = getattr(target, "id", "")
            if name == "allowed_numeric_fields" and isinstance(node.value, ast.Set):
                allowed = _string_elts(node.value)
            elif name == "limits" and isinstance(node.value, ast.Dict):
                limit_keys = {
                    k.value for k in node.value.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)
                }
            elif name == "allowed_inputs" and isinstance(node.value, ast.Tuple):
                input_allowed = _string_elts(node.value)
            elif name == "help_text" and isinstance(node.value, ast.Dict):
                help_keys = {
                    k.value for k in node.value.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)
                }
    return allowed, limit_keys, input_allowed, help_keys


def test_every_whitelisted_numeric_field_has_a_limits_entry():
    allowed, limit_keys, _, _ = _collect()
    assert allowed, "failed to locate allowed_numeric_fields in source"
    assert limit_keys, "failed to locate limits dict in source"
    missing = allowed - limit_keys
    assert not missing, (
        f"whitelisted fields with no limits entry (buttons would be dead): {sorted(missing)}"
    )


def test_every_custom_input_field_is_numeric_whitelisted():
    allowed, _, input_allowed, _ = _collect()
    assert input_allowed, "failed to locate allowed_inputs in source"
    # A ✍️ input for a field the set-path rejects can never be saved.
    orphans = input_allowed - allowed
    assert not orphans, f"custom-input fields missing from allowed_numeric_fields: {sorted(orphans)}"



# ── VOL-DIRECTION-TAB (reported 2026-07-28) ─────────────────────
# "When users click on Direction in Volume bot, it just shows the TP/SL
# interface rather." _strategy_config_section_kb's vol branch IGNORED the
# `section` argument and returned the margin/TP/SL/target keyboard for BOTH
# tabs, so the Direction *heading* rendered over the TP/SL *controls*. And
# vol_direction had no button anywhere: the value was validated
# ({"long","short"}) and consumed by bot_runtime, but the UI could never set it.

def _vol_kb(section):
    from src.nadobro.handlers.strategy_handler import _strategy_config_section_kb
    return _strategy_config_section_kb("vol", section)


def _callbacks(markup):
    return [b.callback_data for row in markup.inline_keyboard for b in row]


def test_vol_direction_tab_offers_long_and_short():
    cbs = _callbacks(_vol_kb("direction"))
    assert "strategy:set_text:vol:vol_direction:long" in cbs
    assert "strategy:set_text:vol:vol_direction:short" in cbs


def test_vol_direction_tab_is_not_the_tp_sl_keyboard():
    cbs = _callbacks(_vol_kb("direction"))
    leaked = [c for c in cbs if any(f in c for f in
              ("tp_pct", "sl_pct", "session_margin_usd", "target_volume_usd"))]
    assert not leaked, f"the Direction tab is still showing TP/SL controls: {leaked}"


def test_vol_risk_tab_still_has_tp_sl_margin_and_target():
    cbs = " ".join(_callbacks(_vol_kb("risk")))
    for field in ("tp_pct", "sl_pct", "session_margin_usd", "target_volume_usd"):
        assert field in cbs, f"{field} disappeared from the TP/SL tab"


def test_vol_direction_uses_set_text_not_set():
    """`set` rejects every vol field outside the numeric four, so a `set:`
    callback here would be silently dropped."""
    for cb in _callbacks(_vol_kb("direction")):
        if "vol_direction" in cb:
            assert cb.startswith("strategy:set_text:"), cb


def test_vol_direction_value_is_in_the_allowed_text_set():
    """The button value must survive the handler's allow-list."""
    import re
    from pathlib import Path
    src = Path("src/nadobro/handlers/strategy_handler.py").read_text()
    allowed = re.search(r'"vol_direction":\s*\{([^}]*)\}', src).group(1)
    for cb in _callbacks(_vol_kb("direction")):
        if "vol_direction" in cb:
            assert f'"{cb.rsplit(":", 1)[1]}"' in allowed


# ── SPOT-EXIT-GUARANTEE on the UI ───────────────────────────────
# engine_runtime raises a DN/Vol spot leg to min_notional x 1.15 so its EXIT can
# still clear the venue floor (kBTC $100 vs a ~$99 leg, prod 2026-07-28). The
# cards must not keep claiming the smaller number the user typed — that is real
# capital they did not ask for.

_KBTC_ROW = {"id": 1, "base": "KBTC", "symbol": "KBTC", "underlying_key": "BTC",
             "min_size_x18": "100000000000000000000"}     # $100, the live value


def _warm_spot_cache(monkeypatch):
    from src.nadobro.venue import product_catalog as pc
    monkeypatch.setitem(pc._spot_catalog_cache, "mainnet",
                        {"ts": 9e18, "data": {"spots": {"KBTC": _KBTC_ROW}}})


def test_leg_size_shows_the_effective_size_when_the_floor_raises_it(monkeypatch):
    _warm_spot_cache(monkeypatch)
    from src.nadobro.handlers.strategy_handler import _leg_size_display
    out = _leg_size_display(100.0, "BTC", "mainnet")
    assert "$100" in out and "$115" in out, out
    assert "floor" in out


def test_leg_size_is_plain_when_the_user_is_already_above_the_floor(monkeypatch):
    _warm_spot_cache(monkeypatch)
    from src.nadobro.handlers.strategy_handler import _leg_size_display
    assert _leg_size_display(500.0, "BTC", "mainnet") == "$500"


def test_leg_size_never_blocks_the_tap_on_a_cache_miss(monkeypatch):
    """Click-path rule: serve cached or nothing. A cold catalog must NOT fetch."""
    from src.nadobro.venue import product_catalog as pc
    from src.nadobro.handlers.strategy_handler import _leg_size_display
    monkeypatch.setattr(pc, "_spot_catalog_cache", {})

    def _boom(*a, **k):
        raise AssertionError("the card fetched the catalog — this hangs the tap")

    monkeypatch.setattr(pc, "_build_dynamic_spot_catalog", _boom)
    monkeypatch.setattr(pc, "get_spot_catalog", _boom)
    assert _leg_size_display(100.0, "BTC", "mainnet") == "$100"


def test_leg_size_is_plain_without_a_product(monkeypatch):
    _warm_spot_cache(monkeypatch)
    from src.nadobro.handlers.strategy_handler import _leg_size_display
    assert _leg_size_display(100.0, "", "mainnet") == "$100"
    assert _leg_size_display(100.0, None, "mainnet") == "$100"


def test_leg_size_survives_a_broken_catalog_row(monkeypatch):
    from src.nadobro.venue import product_catalog as pc
    from src.nadobro.handlers.strategy_handler import _leg_size_display
    monkeypatch.setitem(pc._spot_catalog_cache, "mainnet",
                        {"ts": 9e18, "data": {"spots": {"KBTC": {
                            "base": "KBTC", "min_size_x18": "not-a-number"}}}})
    assert _leg_size_display(100.0, "BTC", "mainnet") == "$100"


def test_the_dn_core_card_shows_the_effective_leg(monkeypatch):
    _warm_spot_cache(monkeypatch)
    from src.nadobro.handlers.strategy_handler import _strategy_config_section_text
    line = [l for l in _strategy_config_section_text(
        "dn", {"product": "BTC", "fixed_margin_usd": 100.0}, "mainnet", "setup"
    ).split("\n") if "per leg" in l][0]
    assert "$115" in line, line


def test_the_vol_card_shows_the_effective_margin(monkeypatch):
    _warm_spot_cache(monkeypatch)
    from src.nadobro.handlers.strategy_handler import _strategy_config_section_text
    line = [l for l in _strategy_config_section_text(
        "vol", {"product": "BTC", "session_margin_usd": 100.0}, "mainnet", "risk"
    ).split("\n") if "Margin per cycle" in l][0]
    assert "$115" in line, line


def test_the_effective_size_string_is_markdownv2_safe(monkeypatch):
    """Unescaped ( ) or . in MarkdownV2 makes Telegram reject the whole message."""
    _warm_spot_cache(monkeypatch)
    from src.nadobro.handlers.strategy_handler import _leg_size_display
    out = _leg_size_display(100.0, "BTC", "mainnet")
    # every literal paren must be backslash-escaped
    for i, ch in enumerate(out):
        if ch in "()":
            assert i > 0 and out[i - 1] == "\\", f"unescaped {ch!r} in {out!r}"
