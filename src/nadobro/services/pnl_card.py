"""Branded shareable PnL card renderer for Nadobro strategy sessions.

Pure renderer: data -> PNG bytes. No DB, no Telegram, no I/O beyond reading the
asset files. The handler/builder layers (separate modules) are the only callers
of this code; this module never talks to the network.

The **background** is always ``assets/cards/pnl_card_bg.png`` (artwork without
baked-in dynamic copy) plus a light left readability tint. Flattened mockups used
for layout tuning must never be pasted as the base layer, or every label would
draw twice.

Run image composition off the event loop via ``PnLCardGenerator.generate``,
which dispatches the synchronous ``_render`` to the default thread executor.
"""
from __future__ import annotations

import asyncio
import io
import logging
import math
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)


CANVAS_W: int = 1360
CANVAS_H: int = 768

NB_CYAN: tuple = (0x22, 0xd3, 0xee)
NB_GREEN: tuple = (0x4a, 0xde, 0x80)
NB_BG_DEEP: tuple = (0x0a, 0x0e, 0x12)
# Spec from design pass (card mockup): profit / loss accents.
LONG_GREEN: tuple = (0x00, 0xff, 0x88)
SHORT_RED: tuple = (0xff, 0x5f, 0x5f)
TG_BG: tuple = (0x0f, 0x19, 0x23)
GOLD: tuple = (0xd4, 0xaf, 0x37)
WHITE: tuple = (0xff, 0xff, 0xff)
WHITE_70: tuple = (0xff, 0xff, 0xff, 178)
GRAY_LIGHT: tuple = (0xb0, 0xb0, 0xb0)
DARK_PANEL_70: tuple = (0x05, 0x08, 0x14, 178)


STRATEGY_DISPLAY: dict[str, str] = {
    "bro_mode": "Bro Mode",
    "delta_neutral": "Delta Neutral",
    "mm_bot": "Market Maker",
    "volume_bot": "Volume Bot",
    "grid": "Grid",
    "r_grid": "Reverse Grid",
    "d_grid": "Dynamic Grid",
}

PNL_LABEL_BY_STRATEGY: dict[str, str] = {
    "mm_bot": "MM PnL",
}


ASSETS: Path = Path(__file__).resolve().parents[3] / "assets"
# Full-bleed background (no session text baked in). Replace this asset if the
# art should match a new designer plate; layout Y/X constants are scaled from a
# 1024×578 reference card.
BG_PATH: Path = ASSETS / "cards" / "pnl_card_bg.png"
# Primary NB monogram (user-provided); then legacy fallbacks.
LOGO_NADOBRO_NB_PATH: Path = ASSETS / "logos" / "nadobro_nb_logo.png"
LOGO_NADOBRO_GLYPH_PATH: Path = ASSETS / "logos" / "nadobro_glyph.png"
LOGO_NADOBRO_PATH: Path = ASSETS / "logos" / "nadobro.png"
LOGO_NADO_PATH: Path = ASSETS / "logos" / "nado.png"
COIN_DIR: Path = ASSETS / "coin_icons"
COIN_FALLBACK: Path = COIN_DIR / "_generic.png"
FONTS_DIR: Path = ASSETS / "fonts"

BRAND_WORDMARK: str = "NADOBRO"

# Bottom-left footer (replaces ``Mode:`` on the reference card; two lines so the URL stays legible).
JOIN_NOW_PREFIX: str = "Join now:"
JOIN_NOW_URL: str = "https://t.me/Nadbro_bot"
JOIN_NOW_LINE: str = f"{JOIN_NOW_PREFIX} {JOIN_NOW_URL}"

# Master card is 1024×578; output canvas 1360×768 (same aspect ratio within 1px).
_REF_H = 578
_REF_W = 1024


def _ys(y_ref: float) -> int:
    """Scale master-space Y into ``CANVAS_H`` space."""
    return int(round(y_ref * CANVAS_H / _REF_H))


def _xs(x_ref: float) -> int:
    """Scale master-space X into ``CANVAS_W`` space."""
    return int(round(x_ref * CANVAS_W / _REF_W))


# Pixel rhythm from ``pnl_card_master.png`` (1024×578), row-scanned; scaled to
# ``CANVAS_*``. Net Fees / PnL labels share one baseline; values share one below.
_REF_PAD_X = 42
_Y_HEADER = _ys(46)
_Y_SYMBOL = _ys(148)
_Y_VOLUME_LABEL = _ys(206)
_Y_VOLUME_VALUE = _ys(234)
_Y_ON_NADO = _ys(278)
_Y_FEES_LABEL = _ys(288)
_Y_FEES_VALUE = _ys(308)
_Y_FOOTER = _ys(526)
# PnL column: left edge of label/value aligns ~under background NB (master ~x575–592).
_REF_X_PNL_COL = 576
_X_PNL_COL = _xs(_REF_X_PNL_COL)
_PAD_X = max(48, _xs(_REF_PAD_X))

# System fonts used when Inter is absent so text stays legible (bitmap default is ~10px).
_FONT_SYSTEM_BOLD: tuple[str, ...] = (
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    "/System/Library/Fonts/Supplemental/Arial.ttf",
    "/System/Library/Fonts/Helvetica.ttc",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
)
_FONT_SYSTEM_REGULAR: tuple[str, ...] = (
    "/System/Library/Fonts/Supplemental/Arial.ttf",
    "/System/Library/Fonts/Helvetica.ttc",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
)


@dataclass(frozen=True)
class PnLCardData:
    """Immutable input to the renderer.

    All numeric values come from ``strategy_sessions``. Sign conventions:
    ``net_fees`` and ``funding`` are signed (negative = paid by user). The
    builder is the only place that touches the DB; this dataclass is the
    boundary between DB land and the pure renderer.

    ``referral_code`` must be the user's primary referral ``public_code`` from
    ``invite_codes`` when they have one; ``None`` only when no code exists —
    never a hardcoded placeholder.
    """

    user_id: int
    network: str
    strategy: str
    strategy_display: str
    mode_label: Optional[str]
    symbol: str
    coin_key: Optional[str]
    volume_usd: Decimal
    realized_pnl: Decimal
    net_fees: Decimal
    funding: Decimal
    started_at: datetime
    ended_at: Optional[datetime]
    duration_label: str
    win_rate_pct: Optional[Decimal]
    total_trades: int
    referral_code: Optional[str]
    is_running: bool


class CardAssetError(RuntimeError):
    """Raised when a required asset (the background) is missing."""


def _try_truetype(path: str, size: int) -> Optional[ImageFont.FreeTypeFont]:
    try:
        return ImageFont.truetype(path, size)
    except OSError:
        return None


def _load_font(name: str, size: int, *, system_fallbacks: tuple[str, ...]) -> ImageFont.ImageFont:
    """Prefer Inter from ``assets/fonts``; fall back to OS fonts; last resort bitmap (tiny)."""
    path = FONTS_DIR / name
    if path.exists():
        ft = _try_truetype(str(path), size)
        if ft is not None:
            return ft
        logger.warning("pnl_card.font_missing", extra={"path": str(path), "size": size})
    for sys_path in system_fallbacks:
        ft = _try_truetype(sys_path, size)
        if ft is not None:
            return ft
    logger.warning("pnl_card.font_fallback_bitmap", extra={"requested": name, "size": size})
    return ImageFont.load_default()


# Inter Regular + Bold only (same family; bold selected weights only).
_FONT_REG_32 = _load_font("Inter-Regular.ttf", 32, system_fallbacks=_FONT_SYSTEM_REGULAR)
_FONT_REG_30 = _load_font("Inter-Regular.ttf", 30, system_fallbacks=_FONT_SYSTEM_REGULAR)
_FONT_BOLD_96 = _load_font("Inter-Bold.ttf", 96, system_fallbacks=_FONT_SYSTEM_BOLD)
_FONT_BOLD_64 = _load_font("Inter-Bold.ttf", 64, system_fallbacks=_FONT_SYSTEM_BOLD)
_FONT_BOLD_52 = _load_font("Inter-Bold.ttf", 52, system_fallbacks=_FONT_SYSTEM_BOLD)
_FONT_BOLD_44 = _load_font("Inter-Bold.ttf", 44, system_fallbacks=_FONT_SYSTEM_BOLD)
_FONT_BOLD_30 = _load_font("Inter-Bold.ttf", 30, system_fallbacks=_FONT_SYSTEM_BOLD)
_FONT_BOLD_28 = _load_font("Inter-Bold.ttf", 28, system_fallbacks=_FONT_SYSTEM_BOLD)


def _format_currency(amount: Decimal) -> str:
    """Comma-separated dollar amount; M/K shorthand only above $1M."""
    abs_amt = abs(amount)
    if abs_amt >= Decimal("1000000"):
        return f"${(amount / Decimal('1000000')):,.2f}M"
    return f"${amount:,.2f}"


def _signed_currency(amount: Decimal) -> str:
    """``+$1,234.56`` / ``-$1,234.56`` with thousands separators."""
    abs_amt = abs(amount)
    if abs_amt >= Decimal("1000000"):
        body = f"${(abs_amt / Decimal('1000000')):,.2f}M"
    else:
        body = f"${abs_amt:,.2f}"
    sign = "+" if amount >= 0 else "-"
    return f"{sign}{body}"


def _pnl_color(amount: Decimal) -> tuple:
    return LONG_GREEN if amount >= 0 else SHORT_RED


def _draw_pill(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    text: str,
    fg: tuple,
    bg: tuple,
    font: ImageFont.ImageFont,
    padding_x: int = 18,
    padding_y: int = 8,
) -> tuple[int, int]:
    """Rounded-rect badge anchored at top-left ``xy``. Returns the bottom-right corner."""
    x, y = xy
    bbox = draw.textbbox((0, 0), text, font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]
    rect = (x, y, x + text_w + padding_x * 2, y + text_h + padding_y * 2)
    radius = (rect[3] - rect[1]) // 2
    draw.rounded_rectangle(rect, radius=radius, fill=bg)
    draw.text((x + padding_x - bbox[0], y + padding_y - bbox[1]), text, fill=fg, font=font)
    return rect[2], rect[3]


def _draw_left_gradient(img: Image.Image) -> Image.Image:
    """Composite a left-half darkening gradient via Image.alpha_composite.

    Builds a tiny 1-pixel-tall horizontal alpha ramp and stretches it vertically;
    far cheaper than ImageDraw.line per row (per prompt guidance).
    """
    width, height = img.size
    span = max(1, int(width * 0.55))
    ramp = Image.new("RGBA", (width, 1), (0, 0, 0, 0))
    ramp_pixels = ramp.load()
    for x in range(width):
        if x >= span:
            alpha = 0
        else:
            t = x / span
            alpha = int(150 * math.exp(-3.2 * t))
        ramp_pixels[x, 0] = (0x05, 0x08, 0x14, alpha)
    overlay = ramp.resize((width, height), Image.NEAREST)
    return Image.alpha_composite(img, overlay)


def _load_coin_icon(coin_key: Optional[str]) -> Optional[Image.Image]:
    """Return an open RGBA coin icon, or ``None`` if both specific and generic are missing."""
    if coin_key:
        candidate = COIN_DIR / f"{coin_key.lower()}.png"
        if candidate.exists():
            try:
                return Image.open(candidate).convert("RGBA")
            except OSError as exc:
                logger.warning("pnl_card.coin_icon_load_failed", extra={"path": str(candidate), "err": str(exc)})
    if COIN_FALLBACK.exists():
        try:
            return Image.open(COIN_FALLBACK).convert("RGBA")
        except OSError as exc:
            logger.warning("pnl_card.coin_fallback_load_failed", extra={"path": str(COIN_FALLBACK), "err": str(exc)})
    return None


def _open_optional_logo(path: Path) -> Optional[Image.Image]:
    if not path.exists():
        logger.warning("pnl_card.logo_missing", extra={"path": str(path)})
        return None
    try:
        return Image.open(path).convert("RGBA")
    except OSError as exc:
        logger.warning("pnl_card.logo_load_failed", extra={"path": str(path), "err": str(exc)})
        return None


def _open_nadobro_mark_image() -> Optional[Image.Image]:
    """NB monogram: shipped asset first, then legacy glyph / full raster."""
    for path in (LOGO_NADOBRO_NB_PATH, LOGO_NADOBRO_GLYPH_PATH, LOGO_NADOBRO_PATH):
        if not path.exists():
            continue
        try:
            return Image.open(path).convert("RGBA")
        except OSError as exc:
            logger.warning("pnl_card.logo_load_failed", extra={"path": str(path), "err": str(exc)})
    logger.warning(
        "pnl_card.logo_missing",
        extra={
            "paths": [
                str(LOGO_NADOBRO_NB_PATH),
                str(LOGO_NADOBRO_GLYPH_PATH),
                str(LOGO_NADOBRO_PATH),
            ],
        },
    )
    return None


def _paste_icon(canvas: Image.Image, icon: Image.Image, xy: tuple[int, int], size: int) -> None:
    resized = icon.resize((size, size), Image.LANCZOS)
    dest = (int(xy[0]), int(xy[1]))
    canvas.alpha_composite(resized, dest=dest)


def _paste_icon_fit(canvas: Image.Image, icon: Image.Image, xy: tuple[int, int], max_size: int) -> tuple[int, int]:
    """Scale ``icon`` to fit inside a ``max_size`` square; preserve aspect ratio."""
    w, h = icon.size
    if w <= 0 or h <= 0:
        return 0, 0
    scale = min(max_size / w, max_size / h)
    nw = max(1, int(round(w * scale)))
    nh = max(1, int(round(h * scale)))
    resized = icon.resize((nw, nh), Image.LANCZOS)
    dest = (int(xy[0]), int(xy[1]))
    canvas.alpha_composite(resized, dest=dest)
    return nw, nh


def _render(data: PnLCardData) -> bytes:
    """Compose a 1360x768 PnL card. Pure / synchronous; safe to run in a worker thread."""
    if not BG_PATH.exists():
        raise CardAssetError(f"missing background asset: {BG_PATH}")
    try:
        bg = Image.open(BG_PATH).convert("RGBA")
    except OSError as exc:
        raise CardAssetError(f"failed to open background {BG_PATH}: {exc}") from exc

    canvas = bg.resize((CANVAS_W, CANVAS_H), Image.LANCZOS)
    canvas = _draw_left_gradient(canvas)
    draw = ImageDraw.Draw(canvas)

    pad = _PAD_X
    header_logo_max = 60
    logo_nadobro = _open_nadobro_mark_image()

    if logo_nadobro is not None:
        nw, nh = _paste_icon_fit(canvas, logo_nadobro, (pad, _Y_HEADER), header_logo_max)
        cursor_x = pad + nw + 14
        wb = draw.textbbox((0, 0), BRAND_WORDMARK, font=_FONT_BOLD_52)
        word_h = wb[3] - wb[1]
        word_y = _Y_HEADER + (nh - word_h) // 2 - wb[1]
        draw.text((cursor_x, word_y), BRAND_WORDMARK, fill=WHITE, font=_FONT_BOLD_52)
    else:
        draw.text((pad, _Y_HEADER + 6), BRAND_WORDMARK, fill=WHITE, font=_FONT_BOLD_52)

    if data.is_running:
        live_x = CANVAS_W - 200
        live_y = _Y_HEADER
        draw.ellipse((live_x, live_y + 12, live_x + 20, live_y + 32), fill=NB_GREEN)
        draw.text((live_x + 30, live_y + 4), "LIVE", fill=WHITE, font=_FONT_BOLD_30)

    symbol_x = pad
    coin_icon = _load_coin_icon(data.coin_key)
    if coin_icon is not None:
        _paste_icon(canvas, coin_icon, (symbol_x, _Y_SYMBOL), 52)
        symbol_x += 64
    draw.text((symbol_x, _Y_SYMBOL + 2), data.symbol, fill=WHITE, font=_FONT_BOLD_44)

    sym_bbox = draw.textbbox((symbol_x, _Y_SYMBOL + 2), data.symbol, font=_FONT_BOLD_44)
    pill_x = sym_bbox[2] + 16
    _draw_pill(
        draw,
        (pill_x, _Y_SYMBOL + 4),
        data.strategy_display,
        fg=WHITE,
        bg=(0x2A, 0x35, 0x40, 238),
        font=_FONT_REG_30,
        padding_x=18,
        padding_y=8,
    )

    draw.text((pad, _Y_VOLUME_LABEL), "Volume", fill=GRAY_LIGHT, font=_FONT_REG_32)
    vol_str = _format_currency(data.volume_usd)
    draw.text((pad, _Y_VOLUME_VALUE), vol_str, fill=WHITE, font=_FONT_BOLD_96)

    sub_y = _Y_ON_NADO
    sub_x = float(pad)
    logo_nado = _open_optional_logo(LOGO_NADO_PATH)
    if logo_nado is not None:
        _paste_icon(canvas, logo_nado, (int(sub_x), sub_y - 2), 32)
        sub_x += 40.0
    draw.text((int(sub_x), sub_y), "On ", fill=GRAY_LIGHT, font=_FONT_REG_30)
    sub_x += draw.textlength("On ", font=_FONT_REG_30)
    draw.text((int(sub_x), sub_y), "Nado", fill=WHITE, font=_FONT_BOLD_30)

    draw.text((pad, _Y_FEES_LABEL), "Net Fees", fill=GRAY_LIGHT, font=_FONT_REG_32)
    fee_str = _signed_currency(data.net_fees)
    draw.text(
        (pad, _Y_FEES_VALUE),
        fee_str,
        fill=_pnl_color(data.net_fees),
        font=_FONT_BOLD_64,
    )

    pnl_label = PNL_LABEL_BY_STRATEGY.get(data.strategy, "PnL")
    draw.text((_X_PNL_COL, _Y_FEES_LABEL), pnl_label, fill=GRAY_LIGHT, font=_FONT_REG_32)
    pnl_str = _signed_currency(data.realized_pnl)
    draw.text(
        (_X_PNL_COL, _Y_FEES_VALUE),
        pnl_str,
        fill=_pnl_color(data.realized_pnl),
        font=_FONT_BOLD_64,
    )

    footer_y = _Y_FOOTER
    draw.text((pad, footer_y), JOIN_NOW_PREFIX, fill=WHITE, font=_FONT_REG_30)
    draw.text((pad, footer_y + 28), JOIN_NOW_URL, fill=WHITE, font=_FONT_BOLD_28)

    if data.referral_code:
        ref_prefix = "Referral Code: "
        code = str(data.referral_code).strip()
        pw = draw.textlength(ref_prefix, font=_FONT_REG_30)
        cw = draw.textlength(code, font=_FONT_BOLD_30)
        x0 = CANVAS_W - pad - pw - cw
        draw.text((x0, footer_y), ref_prefix, fill=WHITE, font=_FONT_REG_30)
        draw.text((x0 + pw, footer_y), code, fill=WHITE, font=_FONT_BOLD_30)

    if data.network and data.network.lower() == "testnet":
        wm_text = "TESTNET"
        wm_bbox = draw.textbbox((0, 0), wm_text, font=_FONT_BOLD_96)
        wm_w = max(1, wm_bbox[2] - wm_bbox[0]) + 32
        wm_h = max(1, wm_bbox[3] - wm_bbox[1]) + 32
        wm_layer = Image.new("RGBA", (wm_w, wm_h), (0, 0, 0, 0))
        wm_draw = ImageDraw.Draw(wm_layer)
        wm_draw.text((16 - wm_bbox[0], 16 - wm_bbox[1]), wm_text, fill=(*GRAY_LIGHT, 90), font=_FONT_BOLD_96)
        wm_layer = wm_layer.rotate(20, resample=Image.BICUBIC, expand=True)
        wm_x = CANVAS_W - wm_layer.size[0] - 32
        wm_y = CANVAS_H - wm_layer.size[1] - 32
        canvas.alpha_composite(wm_layer, dest=(wm_x, wm_y))

    out = canvas.convert("RGB")
    buf = io.BytesIO()
    out.save(buf, format="PNG", optimize=True, compress_level=6)
    return buf.getvalue()


class PnLCardGenerator:
    """Async-friendly wrapper around the synchronous ``_render`` pipeline."""

    async def generate(self, data: PnLCardData) -> bytes:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _render, data)

    def _render(self, data: PnLCardData) -> bytes:
        return _render(data)


pnl_card_generator = PnLCardGenerator()
