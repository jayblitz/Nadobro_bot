"""Tests for ``services.pnl_card``: 1024×578 dict → PNG renderer."""
from __future__ import annotations

import io
from pathlib import Path

import pytest
from PIL import Image

from src.nadobro.services import pnl_card as pc

PNG_HEADER = b"\x89PNG\r\n\x1a\n"


def _sample_data(**overrides) -> dict:
    base = {
        "symbol": "BTC-PERP",
        "strategy": "bro",
        "volume": "$1.23M",
        "net_fees": "-$12.34",
        "pnl": "+$234.56",
        "referral_code": "NADO123",
    }
    base.update(overrides)
    return base


def _open_png(data: bytes) -> Image.Image:
    return Image.open(io.BytesIO(data)).convert("RGB")


def test_generate_pnl_card_returns_png_1024x578():
    out = pc.generate_pnl_card(_sample_data())
    assert out.startswith(PNG_HEADER)
    img = _open_png(out)
    assert img.size == (pc.CANVAS_W, pc.CANVAS_H) == (1024, 578)


def test_missing_background_raises(monkeypatch, tmp_path):
    missing = tmp_path / "missing.png"

    def _fake_resolve() -> Path:
        return missing

    monkeypatch.setattr(pc, "_resolve_bg_path", _fake_resolve)
    with pytest.raises(FileNotFoundError, match="Background image not found"):
        pc.generate_pnl_card(_sample_data())


def test_referral_omitted_when_empty():
    out_with = pc.generate_pnl_card(_sample_data(referral_code="NADO123"))
    out_without = pc.generate_pnl_card(_sample_data(referral_code=""))
    assert out_with.startswith(PNG_HEADER)
    assert out_without.startswith(PNG_HEADER)
    assert _open_png(out_without).size == (1024, 578)
    # The referral text changes the bottom-right region; bytes must differ.
    assert out_with != out_without


def test_positive_vs_negative_pnl_changes_output():
    pos = pc.generate_pnl_card(_sample_data(pnl="+$999.00"))
    neg = pc.generate_pnl_card(_sample_data(pnl="-$999.00"))
    assert pos.startswith(PNG_HEADER) and neg.startswith(PNG_HEADER)
    # Positive is green / negative is red — pixel bytes diverge.
    assert pos != neg


def test_strategy_key_changes_output():
    a = pc.generate_pnl_card(_sample_data(strategy="bro"))
    b = pc.generate_pnl_card(_sample_data(strategy="volume"))
    assert a.startswith(PNG_HEADER) and b.startswith(PNG_HEADER)
    assert a != b


def test_strategy_label_mapping_canonical_keys():
    # The renderer accepts both "_" and "-" delimiters as well as raw words.
    assert pc._strategy_label("bro") == "Bro Mode"
    assert pc._strategy_label("Bro Mode") == "Bro Mode"
    assert pc._strategy_label("grid") == "Grid Mode"
    assert pc._strategy_label("rgrid") == "R-Grid Mode"
    assert pc._strategy_label("r-grid") == "R-Grid Mode"
    assert pc._strategy_label("r_grid") == "R-Grid Mode"
    assert pc._strategy_label("dgrid") == "D-Grid Mode"
    assert pc._strategy_label("volume") == "Volume Mode"
    assert pc._strategy_label("volume_bot") == "Volume Mode"
    assert pc._strategy_label("mm") == "MM Mode"
    assert pc._strategy_label("delta_neutral") == "Delta Neutral"
    assert pc._strategy_label("copy_trading") == "Copy Mode"
    assert pc._strategy_label("studio") == "Strategy Studio"


def test_strategy_label_unknown_falls_back_to_bro_mode():
    assert pc._strategy_label(None) == "Bro Mode"
    assert pc._strategy_label("") == "Bro Mode"
    assert pc._strategy_label("   ") == "Bro Mode"
    assert pc._strategy_label("totally-unknown") == "Bro Mode"


def test_canvas_dimensions_match_master_image():
    """Output canvas matches assets/cards/pnl_card_master.png so the layout
    constants stay in master-image space.

    Skipped silently if the master is missing from the repo.
    """
    master = pc.CARDS_DIR / "pnl_card_master.png"
    if not master.exists():
        pytest.skip("pnl_card_master.png not present")
    with Image.open(master) as im:
        assert im.size == (pc.CANVAS_W, pc.CANVAS_H)


def test_resolve_bg_prefers_pnl_background_jpg(monkeypatch, tmp_path):
    """The clean designer plate "PnL background.jpg" wins over PNG fallbacks."""
    fake_dir = tmp_path
    (fake_dir / "PnL background.jpg").write_bytes(b"jpgbytes")
    (fake_dir / "pnl_card_bg.png").write_bytes(b"pngbytes")
    monkeypatch.setattr(pc, "CARDS_DIR", fake_dir)
    chosen = pc._resolve_bg_path()
    assert chosen.name == "PnL background.jpg"


def test_resolve_bg_falls_back_to_legacy_png(monkeypatch, tmp_path):
    fake_dir = tmp_path
    (fake_dir / "pnl_card_bg.png").write_bytes(b"pngbytes")
    monkeypatch.setattr(pc, "CARDS_DIR", fake_dir)
    chosen = pc._resolve_bg_path()
    assert chosen.name == "pnl_card_bg.png"


def test_loaded_font_is_truetype_when_system_fonts_available():
    """Guards against the silent regression where Pillow's bitmap fallback
    rendered every label at ~10px because no TTF could be loaded.
    """
    font = pc._get_font(48, bold=True)
    # If we got a TTF the size attribute is set; the bitmap default has no .size.
    assert getattr(font, "size", None) is not None or font.__class__.__name__ == "FreeTypeFont"


def test_qr_resolver_prefers_jpeg_then_png():
    """Telegram-bot QR is the bottom-left CTA; resolver tries the canonical
    designer-provided JPEG first, then PNG fallbacks."""
    chosen = pc._resolve_qr_path()
    # The repo ships "Nadobro tg QR code.jpeg" by default.
    assert chosen is not None
    assert chosen.name == "Nadobro tg QR code.jpeg"


def test_qr_paste_is_compositied_at_footer_box(monkeypatch, tmp_path):
    """Paste helper writes pixels inside the (_X_QR, _Y_QR) → (+_SZ_QR, +_SZ_QR)
    box and leaves the rest of the canvas untouched."""
    from PIL import Image as _Image

    canvas = _Image.new("RGBA", (pc.CANVAS_W, pc.CANVAS_H), (0, 0, 0, 255))
    pc._paste_telegram_qr(canvas)
    # Sample a pixel inside the QR box — should no longer be pure black.
    inside = canvas.getpixel((pc._X_QR + pc._SZ_QR // 2, pc._Y_QR + pc._SZ_QR // 2))
    # Sample a pixel far away (top-right corner) — should stay (0,0,0,255).
    outside = canvas.getpixel((pc.CANVAS_W - 4, 4))
    assert inside != (0, 0, 0, 255), "QR didn't write into the footer box"
    assert outside == (0, 0, 0, 255), "QR bled outside the footer box"


def test_qr_missing_does_not_crash(monkeypatch, tmp_path):
    """Renderer must keep working even if the QR asset disappears."""
    monkeypatch.setattr(pc, "_QR_CANDIDATES", (tmp_path / "no_qr.jpeg",))
    out = pc.generate_pnl_card(_sample_data())
    assert out.startswith(PNG_HEADER)
    assert _open_png(out).size == (1024, 578)


def test_legacy_footer_text_constants_are_removed():
    """Guard against accidental re-introduction of the old "Join now:" /
    URL two-line footer. The new footer is the QR code only."""
    # If these layout constants come back, the legacy text will render again.
    assert not hasattr(pc, "_Y_FOOTER1")
    assert not hasattr(pc, "_Y_FOOTER2")
    assert not hasattr(pc, "_SZ_FOOTER")
    # The QR layout constants must exist instead.
    assert hasattr(pc, "_X_QR")
    assert hasattr(pc, "_Y_QR")
    assert hasattr(pc, "_SZ_QR")
