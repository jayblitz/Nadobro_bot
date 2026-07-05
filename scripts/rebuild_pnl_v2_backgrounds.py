"""Rebuild Nadobro PnL v2 empty backgrounds from reaction masters.

This script intentionally does not swap mascots or remix scenes between
reactions. Each output background is derived only from its matching master:

- positive.png <- PnL positive master.png
- negative.png <- PnL negative master.png
- bullish.png  <- PnL bullish master.png
- bearish.png  <- PnL Bearish master.png

The left/user-data side is rebuilt as one continuous clean scene so dynamic
stats are never drawn on top of baked stats. The reaction mascot, chart
direction, stage, arrow, border, and cyber trading scene stay from the matching
master on the right side.
"""
from __future__ import annotations

from pathlib import Path

import random

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

CLEAN_LEFT_X = 1015
CLEAN_FEATHER = 140


def _draw_clean_data_field(reaction: str) -> Image.Image:
    """Create a continuous empty field for all dynamic user data."""
    width, height = CANVAS_SIZE
    img = Image.new("RGBA", CANVAS_SIZE, (2, 5, 18, 255))
    px = img.load()

    rng = random.Random(f"nadobro-pnl-v2-{reaction}")
    accent = (56, 242, 160) if reaction in {"positive", "bullish"} else (255, 76, 91)

    for y in range(height):
        for x in range(width):
            nx = x / width
            ny = y / height
            glow = max(0.0, 1.0 - (((nx - 0.46) / 0.42) ** 2 + ((ny - 0.70) / 0.50) ** 2))
            top = max(0.0, 1.0 - ny)
            noise = rng.randint(-3, 3)
            r = int(3 + 4 * top + accent[0] * glow * 0.020 + noise)
            g = int(8 + 9 * top + accent[1] * glow * 0.035 + noise)
            b = int(24 + 18 * top + accent[2] * glow * 0.040 + noise)
            px[x, y] = (max(0, r), max(0, g), max(0, b), 255)

    return img.filter(ImageFilter.GaussianBlur(0.25))


def _clear_dynamic_zones(master: Image.Image, reaction: str) -> Image.Image:
    original = master.convert("RGBA")
    out = original.copy()
    field = _draw_clean_data_field(reaction)

    mask = Image.new("L", CANVAS_SIZE, 0)
    mask_px = mask.load()
    width, height = CANVAS_SIZE
    clean_x = 1085
    feather = 100
    for y in range(height):
        for x in range(width):
            if x <= clean_x:
                alpha = 255
            elif x >= clean_x + feather:
                alpha = 0
            else:
                alpha = int(255 * (1 - (x - clean_x) / feather))
            mask_px[x, y] = alpha

    out.paste(field, (0, 0), mask)

    # Paste the original reaction art back on top. The mask is broad on the
    # mascot/chart/stage side, but intentionally avoids the old metric/PnL text
    # lanes from the masters.
    protect = Image.new("L", CANVAS_SIZE, 0)
    protect_draw = ImageDraw.Draw(protect)
    protect_draw.rectangle((1120, 0, width, height), fill=255)
    protect_draw.ellipse((955, 80, 1515, 545), fill=255)
    protect_draw.ellipse((1030, 390, 1515, 835), fill=255)
    protect_draw.polygon([(950, 700), (1530, 635), (1560, 860), (870, 890)], fill=255)
    out.paste(original, (0, 0), protect.filter(ImageFilter.GaussianBlur(10)))

    # The mascot protection mask is feathered, which can otherwise pull tiny
    # fragments of the master's old strategy row back into the dynamic tab lane.
    tab_lane_clear = Image.new("L", CANVAS_SIZE, 0)
    tab_lane_draw = ImageDraw.Draw(tab_lane_clear)
    tab_lane_draw.rectangle((895, 305, 1005, 410), fill=255)
    out.paste(field, (0, 0), tab_lane_clear.filter(ImageFilter.GaussianBlur(12)))

    # Preserve the original border exactly.
    border = Image.new("L", CANVAS_SIZE, 0)
    border_draw = ImageDraw.Draw(border)
    border_draw.rectangle((0, 0, width, 16), fill=255)
    border_draw.rectangle((0, height - 16, width, height), fill=255)
    border_draw.rectangle((0, 0, 16, height), fill=255)
    border_draw.rectangle((width - 16, 0, width, height), fill=255)
    out.paste(original, (0, 0), border)
    return out


def rebuild_backgrounds() -> dict[str, Path]:
    PNL_V2_DIR.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}

    for reaction, source in SOURCE_MASTERS.items():
        if not source.exists():
            raise FileNotFoundError(f"Missing PnL v2 source master: {source}")

        master = Image.open(source).convert("RGB").resize(CANVAS_SIZE, Image.LANCZOS)
        cleaned = _clear_dynamic_zones(master, reaction)

        metadata = PngImagePlugin.PngInfo()
        metadata.add_text("nadobro_reaction", reaction)
        metadata.add_text("nadobro_source_master", source.name)
        metadata.add_text(
            "nadobro_background_policy",
            "matching-master-only; continuous-data-field",
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
