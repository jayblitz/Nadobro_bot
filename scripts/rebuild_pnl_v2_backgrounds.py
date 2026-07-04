"""Rebuild Nadobro PnL v2 empty backgrounds from reaction masters.

This script intentionally does not swap mascots or remix scenes between
reactions. Each output background is derived only from its matching master:

- positive.png <- PnL positive master.png
- negative.png <- PnL negative master.png
- bullish.png  <- PnL bullish master.png
- bearish.png  <- PnL Bearish master.png

Only dynamic UI/text zones are cleared. The reaction mascot, chart direction,
stage, arrow, border, and cyber trading scene stay from the matching master.
"""
from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, PngImagePlugin

CANVAS_SIZE = (1600, 900)
ROOT = Path(__file__).resolve().parents[1]
CARDS_DIR = ROOT / "assets" / "cards"
PNL_V2_DIR = CARDS_DIR / "pnl_v2"

SOURCE_MASTERS = {
    "positive": CARDS_DIR / "PnL positive master.png",
    "negative": CARDS_DIR / "PnL negative master.png",
    "bullish": CARDS_DIR / "PnL bullish master.png",
    "bearish": CARDS_DIR / "PnL Bearish master.png",
}

# These regions contain user-specific data or selected controls in the master
# references. They are cleared so the renderer can draw real user data.
CLEAR_ZONES = (
    ((36, 30, 395, 142), 18),     # header logo/wordmark
    ((36, 150, 815, 322), 20),    # symbol + On Nado
    ((36, 316, 1025, 405), 12),   # strategy tabs
    ((36, 420, 1065, 692), 18),   # volume / fees / PnL
    ((36, 670, 386, 866), 14),    # reaction badge / referral
    ((970, 720, 1470, 872), 18),  # strategy callout
)

CLEAR_COLOR = (2, 5, 18)


def _clear_dynamic_zones(master: Image.Image) -> Image.Image:
    out = master.convert("RGBA")
    width, height = CANVAS_SIZE

    for box, radius in CLEAR_ZONES:
        mask = Image.new("L", CANVAS_SIZE, 0)
        draw = ImageDraw.Draw(mask)
        draw.rounded_rectangle(box, radius=radius, fill=255)

        # Feather the outer edge so the clear area does not look like a hard
        # pasted rectangle on top of the scene.
        feather = mask.filter(ImageFilter.GaussianBlur(4))
        layer = Image.new("RGBA", CANVAS_SIZE, CLEAR_COLOR + (0,))
        layer.putalpha(feather)
        out.alpha_composite(layer)

        x1, y1, x2, y2 = box
        inner = (
            max(0, x1 + 8),
            max(0, y1 + 8),
            min(width, x2 - 8),
            min(height, y2 - 8),
        )
        ImageDraw.Draw(out, "RGBA").rounded_rectangle(
            inner,
            radius=max(2, radius - 6),
            fill=CLEAR_COLOR + (252,),
        )

    return out


def rebuild_backgrounds() -> dict[str, Path]:
    PNL_V2_DIR.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}

    for reaction, source in SOURCE_MASTERS.items():
        if not source.exists():
            raise FileNotFoundError(f"Missing PnL v2 source master: {source}")

        master = Image.open(source).convert("RGB").resize(CANVAS_SIZE, Image.LANCZOS)
        cleaned = _clear_dynamic_zones(master)

        metadata = PngImagePlugin.PngInfo()
        metadata.add_text("nadobro_reaction", reaction)
        metadata.add_text("nadobro_source_master", source.name)
        metadata.add_text(
            "nadobro_background_policy",
            "matching-master-only; dynamic-ui-zones-cleared",
        )

        output = PNL_V2_DIR / f"{reaction}.png"
        cleaned.convert("RGB").save(
            output,
            pnginfo=metadata,
            optimize=True,
            compress_level=6,
        )
        written[reaction] = output

    return written


if __name__ == "__main__":
    for reaction_name, path in rebuild_backgrounds().items():
        print(f"{reaction_name}: {path}")
