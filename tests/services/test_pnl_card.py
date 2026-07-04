"""Tests for ``services.pnl_card``: v2 1600x900 dict -> PNG renderer."""
from __future__ import annotations

import io
from pathlib import Path

import pytest
from PIL import Image

from src.nadobro.services import pnl_card as pc

PNG_HEADER = b"\x89PNG\r\n\x1a\n"


def _sample_data(**overrides) -> dict:
    base = {
        "symbol": "BTC:PERP-USDC",
        "strategy": "grid",
        "volume": "$1.23M",
        "net_fees": "-$12.34",
        "pnl": "+$234.56",
        "referral_code": "NADO123",
    }
    base.update(overrides)
    return base


def _open_png(data: bytes) -> Image.Image:
    return Image.open(io.BytesIO(data)).convert("RGB")


def test_generate_pnl_card_returns_png_1600x900():
    out = pc.generate_pnl_card(_sample_data())
    assert out.startswith(PNG_HEADER)
    img = _open_png(out)
    assert img.size == (pc.CANVAS_W, pc.CANVAS_H) == (1600, 900)


def test_missing_background_raises(monkeypatch, tmp_path):
    missing = tmp_path / "missing.png"

    def _fake_resolve(_reaction: pc.Reaction) -> Path:
        return missing

    monkeypatch.setattr(pc, "_resolve_bg_path", _fake_resolve)
    with pytest.raises(FileNotFoundError, match="Background image not found"):
        pc.generate_pnl_card(_sample_data())


def test_referral_omitted_when_empty():
    out_with = pc.generate_pnl_card(_sample_data(referral_code="NADO123"))
    out_without = pc.generate_pnl_card(_sample_data(referral_code=""))
    assert out_with.startswith(PNG_HEADER)
    assert out_without.startswith(PNG_HEADER)
    assert _open_png(out_without).size == (1600, 900)
    assert out_with != out_without


def test_positive_vs_negative_pnl_changes_output():
    pos = pc.generate_pnl_card(_sample_data(pnl="+$999.00"))
    neg = pc.generate_pnl_card(_sample_data(pnl="-$999.00"))
    assert pos.startswith(PNG_HEADER) and neg.startswith(PNG_HEADER)
    assert pos != neg


def test_strategy_key_changes_output():
    a = pc.generate_pnl_card(_sample_data(strategy="grid"))
    b = pc.generate_pnl_card(_sample_data(strategy="volume_bot"))
    assert a.startswith(PNG_HEADER) and b.startswith(PNG_HEADER)
    assert a != b


def test_strategy_label_mapping_canonical_keys():
    assert pc._strategy_label("grid") == "GRID"
    assert pc._strategy_label("dgrid") == "DGRID"
    assert pc._strategy_label("d-grid") == "DGRID"
    assert pc._strategy_label("dynamic grid") == "DGRID"
    assert pc._strategy_label("rgrid") == "RGRID"
    assert pc._strategy_label("r-grid") == "RGRID"
    assert pc._strategy_label("reverse grid") == "RGRID"
    assert pc._strategy_label("mid") == "MID MODE"
    assert pc._strategy_label("mid mode") == "MID MODE"
    assert pc._strategy_label("mm") == "MID MODE"
    assert pc._strategy_label("market maker") == "MID MODE"
    assert pc._strategy_label("volume") == "VOLUME BOT"
    assert pc._strategy_label("volume_bot") == "VOLUME BOT"
    assert pc._strategy_label("volume bot") == "VOLUME BOT"
    assert pc._strategy_label("dn") == "DN"
    assert pc._strategy_label("delta_neutral") == "DN"
    assert pc._strategy_label("delta neutral") == "DN"


def test_strategy_label_unknown_falls_back_to_grid():
    assert pc._strategy_label(None) == "GRID"
    assert pc._strategy_label("") == "GRID"
    assert pc._strategy_label("   ") == "GRID"
    assert pc._strategy_label("totally-unknown") == "GRID"


def test_strategy_icons_match_strategy_lab_screenshot():
    assert pc.STRATEGY_EMOJIS == {
        "GRID": "🤖",
        "RGRID": "🧱",
        "DGRID": "⚡",
        "MID MODE": "🎯",
        "DN": "⚖️",
        "VOLUME BOT": "🔁",
    }


@pytest.mark.parametrize(
    ("pnl", "volume", "expected"),
    [
        (-1001.0, 10.0, "bearish"),
        (-1.0, 10.0, "negative"),
        (1000.0, 100000.0, "bullish"),
        (999.99, 1000000.0, "positive"),
        (1200.0, 99999.0, "positive"),
    ],
)
def test_choose_reaction(pnl, volume, expected):
    assert pc._choose_reaction(pnl, volume) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("+$1,234.50", 1234.5),
        ("-$1.2K", -1200.0),
        ("$1.23M", 1230000.0),
        ("($42.00)", -42.0),
        (500, 500.0),
    ],
)
def test_parse_float(raw, expected):
    assert pc._parse_float(raw) == expected


def test_canvas_dimensions_match_v2_backgrounds():
    for name in pc._V2_BG_NAMES.values():
        bg = pc.PNL_V2_DIR / name
        assert bg.exists(), f"missing v2 background {bg}"
        with Image.open(bg) as im:
            assert im.size == (pc.CANVAS_W, pc.CANVAS_H)


def test_v2_backgrounds_record_matching_master_sources():
    expected_sources = {
        "positive": "PnL positive master.png",
        "negative": "PnL negative master.png",
        "bullish": "PnL bullish master.png",
        "bearish": "PnL Bearish master.png",
    }
    for reaction, filename in pc._V2_BG_NAMES.items():
        bg = pc.PNL_V2_DIR / filename
        with Image.open(bg) as im:
            assert im.info.get("nadobro_reaction") == reaction
            assert im.info.get("nadobro_source_master") == expected_sources[reaction]
            assert im.info.get("nadobro_background_policy") == (
                "matching-master-only; dynamic-ui-zones-cleared"
            )


def test_brand_assets_used_by_renderer_exist():
    assert (pc.LOGOS_DIR / "Nadobro Logo v3.png").exists()
    assert (pc.LOGOS_DIR / "nado.png").exists()


def test_asset_key_normalizes_common_market_symbols():
    assert pc._asset_key("ETH:PERP-USDC") == "ETH"
    assert pc._asset_key("BTC-PERP") == "BTC"
    assert pc._asset_key("SOL/USDC") == "SOL"
    assert pc._asset_key("NLP:USDC") == "NADO"
    assert pc._asset_key("WETH_USDT0") == "ETH"


def test_market_icon_resolver_uses_image_assets():
    assert pc._resolve_market_icon_path("ETH:PERP-USDC") == pc.MARKET_ICONS_DIR / "ETH.png"
    assert pc._resolve_market_icon_path("BTC-PERP") == pc.MARKET_ICONS_DIR / "BTC.png"
    assert pc._resolve_market_icon_path("USDC") == pc.MARKET_ICONS_DIR / "USDC.png"


def test_known_nado_asset_symbols_include_preview_assets():
    assert "ETH-PERP" in pc.NADO_ASSET_SYMBOLS
    assert "BTC-PERP" in pc.NADO_ASSET_SYMBOLS
    assert "SOL-PERP" in pc.NADO_ASSET_SYMBOLS
    assert "USDC" in pc.NADO_ASSET_SYMBOLS


def test_resolve_bg_prefers_reaction_v2_background(monkeypatch, tmp_path):
    fake_cards = tmp_path / "cards"
    fake_v2 = fake_cards / "pnl_v2"
    fake_v2.mkdir(parents=True)
    (fake_v2 / "bullish.png").write_bytes(b"v2bytes")
    (fake_cards / "PnL background.jpg").write_bytes(b"legacybytes")
    monkeypatch.setattr(pc, "CARDS_DIR", fake_cards)
    monkeypatch.setattr(pc, "PNL_V2_DIR", fake_v2)
    assert pc._resolve_bg_path("bullish").name == "bullish.png"


def test_resolve_bg_falls_back_to_legacy_png(monkeypatch, tmp_path):
    fake_cards = tmp_path / "cards"
    fake_v2 = fake_cards / "pnl_v2"
    fake_cards.mkdir()
    (fake_cards / "pnl_card_bg.png").write_bytes(b"pngbytes")
    monkeypatch.setattr(pc, "CARDS_DIR", fake_cards)
    monkeypatch.setattr(pc, "PNL_V2_DIR", fake_v2)
    chosen = pc._resolve_bg_path("positive")
    assert chosen.name == "pnl_card_bg.png"


def test_loaded_font_is_truetype_when_system_fonts_available():
    font = pc._get_font(48, bold=True)
    assert getattr(font, "size", None) is not None or font.__class__.__name__ == "FreeTypeFont"


def test_generate_dev_preview_cards_writes_four_samples(tmp_path):
    written = pc.generate_dev_preview_cards(tmp_path)
    assert set(written) == {"positive_grid", "negative_dgrid", "bullish_volume_bot", "bearish_dn"}
    for path in written.values():
        assert path.exists()
        data = path.read_bytes()
        assert data.startswith(PNG_HEADER)
        assert _open_png(data).size == (1600, 900)
