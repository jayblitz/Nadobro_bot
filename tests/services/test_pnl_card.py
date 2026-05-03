"""Renderer-only tests for ``services.pnl_card``.

PR1 of the PnL share-card feature ships a pure renderer; these tests cover the
data -> PNG bytes contract without any DB, network, or Telegram dependency.
The happy-path case compares against a committed golden PNG with a perceptual
hash so font-hinting differences across machines don't cause false failures.
"""
from __future__ import annotations

import asyncio
import io
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from PIL import Image

from src.nadobro.services import pnl_card as pc

imagehash = pytest.importorskip("imagehash")


PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLDEN = PROJECT_ROOT / "tests" / "fixtures" / "cards" / "golden_card_btc_long.png"

PNG_HEADER = b"\x89PNG\r\n\x1a\n"


def _make_data(**overrides) -> pc.PnLCardData:
    base = dict(
        user_id=42,
        network="mainnet",
        strategy="bro_mode",
        strategy_display="Bro Mode",
        mode_label="aggressive and BTC focused",
        symbol="BTC-PERP",
        coin_key="btc",
        volume_usd=Decimal("1234567.89"),
        realized_pnl=Decimal("234.56"),
        net_fees=Decimal("-12.34"),
        funding=Decimal("0"),
        started_at=datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc),
        ended_at=datetime(2026, 5, 1, 15, 25, 0, tzinfo=timezone.utc),
        duration_label="3h 25m",
        win_rate_pct=Decimal("60.0"),
        total_trades=42,
        referral_code="NADO123",
        is_running=False,
    )
    base.update(overrides)
    return pc.PnLCardData(**base)


def _open_png(data_bytes: bytes) -> Image.Image:
    return Image.open(io.BytesIO(data_bytes)).convert("RGB")


def test_render_happy_path_btc_long_matches_golden():
    out = pc._render(_make_data())

    assert out, "renderer returned empty bytes"
    assert out.startswith(PNG_HEADER), "output is not a valid PNG"
    img = _open_png(out)
    assert img.size == (pc.CANVAS_W, pc.CANVAS_H) == (1360, 768)

    assert GOLDEN.exists(), f"golden fixture missing: {GOLDEN}"
    golden_img = Image.open(GOLDEN).convert("RGB")
    distance = imagehash.phash(img) - imagehash.phash(golden_img)
    assert distance < 8, f"perceptual hash distance too large ({distance})"


def test_generate_runs_in_executor_and_returns_same_bytes():
    data = _make_data()
    sync_bytes = pc._render(data)
    async_bytes = asyncio.run(pc.pnl_card_generator.generate(data))
    assert async_bytes == sync_bytes


def test_missing_background_raises_card_asset_error(monkeypatch, tmp_path):
    monkeypatch.setattr(pc, "BG_PATH", tmp_path / "does_not_exist.png")
    with pytest.raises(pc.CardAssetError):
        pc._render(_make_data())


def test_missing_logo_uses_text_fallback(monkeypatch, tmp_path):
    monkeypatch.setattr(pc, "LOGO_NADOBRO_NB_PATH", tmp_path / "no_nb.png")
    monkeypatch.setattr(pc, "LOGO_NADOBRO_GLYPH_PATH", tmp_path / "no_glyph.png")
    monkeypatch.setattr(pc, "LOGO_NADOBRO_PATH", tmp_path / "no_logo.png")
    out = pc._render(_make_data())
    assert out.startswith(PNG_HEADER)
    assert _open_png(out).size == (1360, 768)


def test_missing_coin_icon_omits_gracefully(monkeypatch, tmp_path):
    monkeypatch.setattr(pc, "COIN_DIR", tmp_path / "missing_coin_dir")
    monkeypatch.setattr(pc, "COIN_FALLBACK", tmp_path / "missing_coin_dir" / "_generic.png")
    out = pc._render(_make_data(coin_key="totally_fake_ticker"))
    assert out.startswith(PNG_HEADER)
    assert _open_png(out).size == (1360, 768)


# Region over the right-column PnL value (shared baseline with net fees).
_PNL_VALUE_BOX = (760, 400, 1000, 500)


def _color_counts(img: Image.Image, box: tuple[int, int, int, int]) -> tuple[int, int]:
    """Return ``(red_pixels, green_pixels)`` in a crop, using loose thresholds.

    Robust to bitmap-font fallback when Inter TTF is absent: thresholds match
    both the brand SHORT_RED/LONG_GREEN palette and any anti-aliased neighbors.
    """
    region = img.crop(box)
    pixels = list(region.get_flattened_data())
    red = sum(1 for r, g, b in pixels if r > 150 and g < 130 and b < 130)
    green = sum(1 for r, g, b in pixels if g > 130 and r < 150 and b < 150)
    return red, green


def test_negative_pnl_dominantly_red_in_value_area():
    out = pc._render(_make_data(realized_pnl=Decimal("-321.99")))
    img = _open_png(out)
    red, green = _color_counts(img, _PNL_VALUE_BOX)
    assert red > green and red >= 5, f"expected red-dominant PnL value area: red={red}, green={green}"


def test_positive_pnl_dominantly_green_in_value_area():
    out = pc._render(_make_data(realized_pnl=Decimal("999.00")))
    img = _open_png(out)
    red, green = _color_counts(img, _PNL_VALUE_BOX)
    assert green > red and green >= 5, f"expected green-dominant PnL value area: red={red}, green={green}"


def test_pnl_color_swaps_with_sign():
    """Cross-check: positive PnL should be greener than negative; negative redder than positive."""
    pos = _open_png(pc._render(_make_data(realized_pnl=Decimal("999.00"))))
    neg = _open_png(pc._render(_make_data(realized_pnl=Decimal("-999.00"))))
    pos_red, pos_green = _color_counts(pos, _PNL_VALUE_BOX)
    neg_red, neg_green = _color_counts(neg, _PNL_VALUE_BOX)
    assert pos_green > neg_green
    assert neg_red > pos_red


def test_testnet_watermark_changes_output():
    mainnet = pc._render(_make_data(network="mainnet"))
    testnet = pc._render(_make_data(network="testnet"))
    assert mainnet != testnet, "testnet watermark should change the rendered output"


def test_running_state_changes_output():
    stopped = pc._render(_make_data(is_running=False))
    running = pc._render(_make_data(is_running=True))
    assert stopped != running, "LIVE badge should change the rendered output"


def test_multi_product_no_icon_renders():
    data = _make_data(symbol="MULTI", coin_key=None)
    out = pc._render(data)
    assert out.startswith(PNG_HEADER)
    assert _open_png(out).size == (1360, 768)


def test_format_currency_under_million():
    assert pc._format_currency(Decimal("1234.5")) == "$1,234.50"
    assert pc._format_currency(Decimal("0")) == "$0.00"


def test_format_currency_above_million_uses_shorthand():
    assert pc._format_currency(Decimal("2500000")) == "$2.50M"


def test_signed_currency_sign_handling():
    assert pc._signed_currency(Decimal("12.34")) == "+$12.34"
    assert pc._signed_currency(Decimal("-12.34")) == "-$12.34"
    assert pc._signed_currency(Decimal("0")) == "+$0.00"


def test_pnl_color_thresholds():
    assert pc._pnl_color(Decimal("0")) == pc.LONG_GREEN
    assert pc._pnl_color(Decimal("1")) == pc.LONG_GREEN
    assert pc._pnl_color(Decimal("-0.01")) == pc.SHORT_RED


def test_pnl_label_strategy_mapping():
    assert pc.PNL_LABEL_BY_STRATEGY["mm_bot"] == "MM PnL"
    assert pc.PNL_LABEL_BY_STRATEGY.get("bro_mode", "PnL") == "PnL"


def test_strategy_display_covers_all_known_strategies():
    expected = {"bro_mode", "delta_neutral", "mm_bot", "volume_bot", "grid", "r_grid", "d_grid"}
    assert set(pc.STRATEGY_DISPLAY.keys()) == expected
