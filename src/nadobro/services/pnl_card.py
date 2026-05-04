"""PnL share card: background image + fixed-layout text and strategy pill.

Output is always 1024×578 PNG. Source art is preferred from
``assets/cards/PnL background.jpg`` (the clean designer plate). Falls back to
``pnl_card_bg.png`` then ``pnl_card_bg.jpg``.

Layout matches ``assets/cards/pnl_card_master.png`` (1024×578) so user-specific
data renders at the same positions as the design master. The renderer is pure
``data dict → PNG bytes`` — no DB, no Telegram, no I/O beyond reading the
asset files.
"""
from __future__ import annotations

import io
import logging
import sys
from pathlib import Path
from typing import Iterable

from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

CANVAS_W: int = 1024
CANVAS_H: int = 578

# ---------------------------------------------------------------------------
# Layout (master 1024×578) — measured from assets/cards/pnl_card_master.png.
# Don't tweak without re-comparing the rendered preview against the master.
# ---------------------------------------------------------------------------

_X_LEFT = 36           # left rail used by every left-column text
_X_PNL_COL = 322       # PnL column (label + value)

# NADOBRO header
_X_HEADER_LOGO = 28
_Y_HEADER_LOGO = 24
_HEADER_LOGO_SIZE = 76
_X_WORDMARK = 122
_Y_WORDMARK = 38
_SZ_WORDMARK = 52

# Symbol + strategy pill
_Y_SYMBOL = 132
_SZ_SYMBOL = 44
_X_BADGE_GAP = 16      # gap between symbol text and the strategy pill
_SZ_BADGE = 22

# Volume block
_Y_VOLUME_LABEL = 198
_SZ_VOLUME_LABEL = 28
_Y_VOLUME_VALUE = 230
_SZ_VOLUME_VALUE = 72

# "On Nado" with paper-plane glyph
_Y_ON_NADO = 322
_SZ_ON_NADO = 24

# Net Fees + PnL row
_Y_FEES_LABEL = 376
_SZ_FEES_LABEL = 26
_Y_FEES_VALUE = 406
_SZ_FEES_VALUE = 52
_SZ_PNL_LABEL = 28
_SZ_PNL_VALUE = 52

# Footer — QR code (replaces former "Join now: …" two-line text) + referral
_X_QR = 36
_Y_QR = 462           # top edge; bottom = _Y_QR + _SZ_QR ≈ 572 (6px from canvas bottom)
_SZ_QR = 110
_X_REFERRAL_RIGHT = 990   # right edge for "Referral Code: …" (anchored right)
_Y_REFERRAL = 538
_SZ_REFERRAL = 22

# ---------------------------------------------------------------------------
# Colors (RGB)
# ---------------------------------------------------------------------------
WHITE = (255, 255, 255)
LIGHT_GREY = (185, 192, 199)
SALMON = (255, 138, 138)        # "Net Fees" label
RED = (255, 76, 76)             # negative values
GREEN = (60, 230, 110)          # positive values / paper-plane glyph
PILL_BG = (32, 44, 56)
PILL_FG = (210, 222, 232)


# ---------------------------------------------------------------------------
# Asset paths
# ---------------------------------------------------------------------------
ASSETS: Path = Path(__file__).resolve().parents[3] / "assets"
CARDS_DIR: Path = ASSETS / "cards"
LOGOS_DIR: Path = ASSETS / "logos"
FONTS_DIR: Path = ASSETS / "fonts"

# Background image candidates, tried in order.
_BG_CANDIDATES: tuple[str, ...] = (
    "PnL background.jpg",   # designer-provided clean plate
    "pnl_card_bg.png",      # legacy path used by older code
    "pnl_card_bg.jpg",      # alt naming
)

_LOGO_CANDIDATES: tuple[Path, ...] = (
    LOGOS_DIR / "nadobro_nb_logo.png",
    LOGOS_DIR / "nadobro_glyph.png",
    LOGOS_DIR / "nadobro.png",
    ASSETS / "logo" / "NadoBro logo sq.png",
    ASSETS / "logo" / "nadobro.png",
)

# Small "Nado" send-glyph rendered next to the "On Nado" line under Volume.
_NADO_ICON_CANDIDATES: tuple[Path, ...] = (
    LOGOS_DIR / "nado.png",
    ASSETS / "logo" / "nado.png",
)

# Telegram-bot QR code rendered in the bottom-left footer (replaces the
# former "Join now: …" two-line text).
_QR_CANDIDATES: tuple[Path, ...] = (
    CARDS_DIR / "Nadobro tg QR code.jpeg",
    CARDS_DIR / "nadobro_bot_qr.png",
    CARDS_DIR / "nadobro_bot_qr.jpg",
)


# ---------------------------------------------------------------------------
# Font fallback chain
#
# Pillow's ``ImageFont.load_default()`` ignores the size parameter and produces
# a tiny ~10px bitmap font; that's why early renders looked broken. We try
# Inter (when shipped under assets/fonts), then platform sans-serifs, then
# bail to load_default as a last resort.
# ---------------------------------------------------------------------------

_FONT_BOLD_CHAIN: tuple[tuple[Path, int], ...] = (
    (FONTS_DIR / "Inter-Bold.ttf", 0),
    # macOS — TTC files; index picks a face. Helvetica.ttc index 1 is Bold.
    (Path("/System/Library/Fonts/Helvetica.ttc"), 1),
    (Path("/System/Library/Fonts/HelveticaNeue.ttc"), 1),
    (Path("/Library/Fonts/Arial Unicode.ttf"), 0),
    (Path("/System/Library/Fonts/Supplemental/Arial Bold.ttf"), 0),
    # Linux
    (Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"), 0),
    (Path("/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"), 0),
)

_FONT_REGULAR_CHAIN: tuple[tuple[Path, int], ...] = (
    (FONTS_DIR / "Inter-Regular.ttf", 0),
    (Path("/System/Library/Fonts/Helvetica.ttc"), 0),
    (Path("/System/Library/Fonts/HelveticaNeue.ttc"), 0),
    (Path("/Library/Fonts/Arial Unicode.ttf"), 0),
    (Path("/System/Library/Fonts/Supplemental/Arial.ttf"), 0),
    (Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"), 0),
    (Path("/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"), 0),
)


def _load_font(chain: Iterable[tuple[Path, int]], size: int) -> ImageFont.ImageFont:
    for path, index in chain:
        if not path.exists():
            continue
        try:
            return ImageFont.truetype(str(path), size, index=index)
        except OSError:
            continue
    # Last resort — ``load_default`` is bitmap-only but at least won't crash.
    logger.warning("PnL card: no TrueType font available; using Pillow default bitmap font")
    return ImageFont.load_default()


def _get_font(size: int, *, bold: bool = False) -> ImageFont.ImageFont:
    return _load_font(_FONT_BOLD_CHAIN if bold else _FONT_REGULAR_CHAIN, size)


# ---------------------------------------------------------------------------
# Strategy → display label
# ---------------------------------------------------------------------------
STRATEGY_LABELS: dict[str, str] = {
    "bro": "Bro Mode",
    "bro mode": "Bro Mode",
    "bro_mode": "Bro Mode",
    "studio": "Strategy Studio",
    "strategy studio": "Strategy Studio",
    "grid": "Grid Mode",
    "rgrid": "R-Grid Mode",
    "r-grid": "R-Grid Mode",
    "r_grid": "R-Grid Mode",
    "dgrid": "D-Grid Mode",
    "d-grid": "D-Grid Mode",
    "d_grid": "D-Grid Mode",
    "volume": "Volume Mode",
    "volume_bot": "Volume Mode",
    "volume bot": "Volume Mode",
    "mm": "MM Mode",
    "mm_bot": "MM Mode",
    "market maker": "MM Mode",
    "delta_neutral": "Delta Neutral",
    "dn": "Delta Neutral",
    "copy": "Copy Mode",
    "copy trading": "Copy Mode",
    "copy_trading": "Copy Mode",
}


def _strategy_label(raw: object) -> str:
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return "Bro Mode"
    s = str(raw).strip().lower().replace("-", " ").replace("_", " ")
    s = " ".join(s.split())  # collapse whitespace
    # Try with spaces first then with underscores for the dict
    return STRATEGY_LABELS.get(s, STRATEGY_LABELS.get(s.replace(" ", "_"), "Bro Mode"))


# ---------------------------------------------------------------------------
# Background resolution
# ---------------------------------------------------------------------------
def _resolve_bg_path() -> Path:
    """Return the first existing background image from the candidate list.

    Returns the first candidate path even if it doesn't exist, so callers can
    surface a clear error from ``generate_pnl_card``.
    """
    for name in _BG_CANDIDATES:
        p = CARDS_DIR / name
        if p.exists():
            return p
    return CARDS_DIR / _BG_CANDIDATES[0]


def _resolve_logo_path() -> Path | None:
    for p in _LOGO_CANDIDATES:
        if p.exists():
            return p
    return None


# ---------------------------------------------------------------------------
# Drawing helpers
# ---------------------------------------------------------------------------
def _draw_strategy_pill(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    text: str,
    font: ImageFont.ImageFont,
    *,
    padding_x: int = 14,
    padding_y: int = 7,
) -> int:
    """Draw the rounded "Bro Mode" pill. Returns its right edge X coordinate."""
    x, y = xy
    bbox = draw.textbbox((0, 0), text, font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]
    rect = (x, y, x + text_w + padding_x * 2, y + text_h + padding_y * 2)
    radius = max(6, (rect[3] - rect[1]) // 2)
    draw.rounded_rectangle(rect, radius=radius, fill=PILL_BG)
    draw.text(
        (x + padding_x - bbox[0], y + padding_y - bbox[1]),
        text,
        fill=PILL_FG,
        font=font,
    )
    return rect[2]


def _resolve_nado_icon_path() -> Path | None:
    for p in _NADO_ICON_CANDIDATES:
        if p.exists():
            return p
    return None


def _load_nado_glyph(target_height: int) -> Image.Image | None:
    """Load ``assets/logos/nado.png``, key out the dark background, crop to
    the glyph's tight bbox, and scale to ``target_height``.

    Returns ``None`` if the asset is missing.
    """
    icon_path = _resolve_nado_icon_path()
    if icon_path is None:
        return None
    icon = Image.open(icon_path).convert("RGBA")
    # Color-key the dark photo background so only the white send-glyph stays.
    px = icon.load()
    w, h = icon.size
    for j in range(h):
        for i in range(w):
            r, g, b, _a = px[i, j]
            if r < 40 and g < 40 and b < 40:
                px[i, j] = (0, 0, 0, 0)
    # Crop to the visible glyph so the icon fills the requested size instead
    # of being padded by the source's empty margin.
    bbox = icon.getbbox()
    if bbox:
        icon = icon.crop(bbox)
    aspect = icon.width / icon.height if icon.height else 1.0
    new_h = target_height
    new_w = max(1, int(round(new_h * aspect)))
    return icon.resize((new_w, new_h), Image.LANCZOS)


def _paste_nado_icon_aligned_to_text(
    canvas: Image.Image,
    draw: ImageDraw.ImageDraw,
    text_xy: tuple[int, int],
    text: str,
    font: ImageFont.ImageFont,
    *,
    glyph_height: int,
    gap: int = 8,
) -> tuple[int, int]:
    """Paste the Nado glyph centred to the *visual* midline of ``text``.

    Returns the (x, y) where the text should be drawn so it sits flush right
    of the glyph with consistent spacing. The y matches the requested
    ``text_xy[1]`` — only the glyph position is computed.
    """
    text_x, text_y = text_xy
    text_bbox = draw.textbbox((text_x, text_y), text, font=font)
    text_visual_h = text_bbox[3] - text_bbox[1]
    text_center_y = text_bbox[1] + text_visual_h // 2

    glyph = _load_nado_glyph(glyph_height)
    if glyph is None:
        # Defensive: draw a minimal arrow if the asset is missing.
        draw.polygon(
            [
                (text_x, text_center_y),
                (text_x + glyph_height, text_center_y - glyph_height // 2),
                (text_x + int(glyph_height * 0.55), text_center_y),
                (text_x + glyph_height, text_center_y + glyph_height // 2),
            ],
            fill=WHITE,
        )
        return text_x + glyph_height + gap, text_y

    glyph_y = text_center_y - glyph.height // 2
    canvas.alpha_composite(glyph, dest=(text_x, glyph_y))
    return text_x + glyph.width + gap, text_y


def _paste_logo(canvas: Image.Image) -> None:
    logo_path = _resolve_logo_path()
    if logo_path is None:
        return
    try:
        logo = Image.open(logo_path).convert("RGBA")
    except (OSError, ValueError):
        return
    # Fit into the header logo box, preserving aspect ratio.
    logo.thumbnail((_HEADER_LOGO_SIZE, _HEADER_LOGO_SIZE), Image.LANCZOS)
    # Centre vertically inside the box for crisp alignment.
    box_x = _X_HEADER_LOGO + (_HEADER_LOGO_SIZE - logo.width) // 2
    box_y = _Y_HEADER_LOGO + (_HEADER_LOGO_SIZE - logo.height) // 2
    canvas.alpha_composite(logo, dest=(box_x, box_y))


def _resolve_qr_path() -> Path | None:
    for p in _QR_CANDIDATES:
        if p.exists():
            return p
    return None


def _paste_telegram_qr(canvas: Image.Image) -> None:
    """Composite the Telegram-bot QR code in the bottom-left footer.

    Pasted as a square at ``(_X_QR, _Y_QR)`` sized ``_SZ_QR``. The source
    JPEG already includes the NB logo / NADOBRO wordmark / @NADBRO_BOT
    handle baked in, so users only need to scan the visible QR pattern.

    Silently skipped if the asset is missing — keeps the renderer robust
    in environments without the QR file.
    """
    qr_path = _resolve_qr_path()
    if qr_path is None:
        return
    try:
        qr = Image.open(qr_path).convert("RGBA")
    except (OSError, ValueError):
        return
    qr.thumbnail((_SZ_QR, _SZ_QR), Image.LANCZOS)
    # Centre inside the QR box so non-square sources still align cleanly.
    box_x = _X_QR + (_SZ_QR - qr.width) // 2
    box_y = _Y_QR + (_SZ_QR - qr.height) // 2
    canvas.alpha_composite(qr, dest=(box_x, box_y))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def generate_pnl_card(data: dict) -> bytes:
    """Render a PnL card PNG (1024×578).

    ``data`` keys (all strings; amounts are pre-formatted by the caller):

    * ``symbol`` (default ``"BTC-PERP"``)
    * ``strategy`` — ``"bro"``, ``"grid"``, ``"rgrid"``, ``"dgrid"``,
      ``"volume"``, ``"copy_trading"`` / ``"copy trading"``, ``"mm"``,
      ``"delta_neutral"``, ``"studio"``; unknown → ``"Bro Mode"``
    * ``volume`` — e.g. ``"$1.23M"``
    * ``net_fees`` — signed string, e.g. ``"-$12.34"``
    * ``pnl`` — signed string, e.g. ``"+$234.56"``
    * ``referral_code`` — when truthy, drawn at the bottom-right

    Raises:
        FileNotFoundError: if no background image exists.
    """
    bg_path = _resolve_bg_path()
    if not bg_path.exists():
        raise FileNotFoundError(
            f"Background image not found: looked for "
            + ", ".join(str(CARDS_DIR / n) for n in _BG_CANDIDATES)
        )

    img = Image.open(bg_path).convert("RGBA")
    if img.size != (CANVAS_W, CANVAS_H):
        img = img.resize((CANVAS_W, CANVAS_H), Image.LANCZOS)
    draw = ImageDraw.Draw(img)

    # Fonts
    f_wordmark = _get_font(_SZ_WORDMARK, bold=True)
    f_symbol = _get_font(_SZ_SYMBOL, bold=True)
    f_badge = _get_font(_SZ_BADGE, bold=False)
    f_vol_lbl = _get_font(_SZ_VOLUME_LABEL, bold=False)
    f_vol_val = _get_font(_SZ_VOLUME_VALUE, bold=True)
    f_on_nado = _get_font(_SZ_ON_NADO, bold=False)
    f_fees_lbl = _get_font(_SZ_FEES_LABEL, bold=False)
    f_fees_val = _get_font(_SZ_FEES_VALUE, bold=True)
    f_pnl_lbl = _get_font(_SZ_PNL_LABEL, bold=False)
    f_pnl_val = _get_font(_SZ_PNL_VALUE, bold=True)
    f_ref = _get_font(_SZ_REFERRAL, bold=False)

    # 1) Header — NB box logo + "NADOBRO" wordmark
    _paste_logo(img)
    draw.text((_X_WORDMARK, _Y_WORDMARK), "NADOBRO", fill=WHITE, font=f_wordmark)

    # 2) Symbol + strategy pill (pill anchored to symbol's right edge)
    symbol = str(data.get("symbol", "BTC-PERP"))
    draw.text((_X_LEFT, _Y_SYMBOL), symbol, fill=WHITE, font=f_symbol)
    sym_bbox = draw.textbbox((_X_LEFT, _Y_SYMBOL), symbol, font=f_symbol)
    badge_text = _strategy_label(data.get("strategy"))
    pill_x = sym_bbox[2] + _X_BADGE_GAP
    # Vertically centre the pill against the symbol baseline.
    sym_h = sym_bbox[3] - sym_bbox[1]
    pill_y = _Y_SYMBOL + max(0, (sym_h - _SZ_BADGE) // 2)
    _draw_strategy_pill(draw, (pill_x, pill_y), badge_text, f_badge)

    # 3) Volume
    draw.text((_X_LEFT, _Y_VOLUME_LABEL), "Volume", fill=LIGHT_GREY, font=f_vol_lbl)
    volume = str(data.get("volume", "$0"))
    draw.text((_X_LEFT, _Y_VOLUME_VALUE), volume, fill=WHITE, font=f_vol_val)

    # 4) Nado send-icon + "On Nado" — glyph vertically centred to text midline
    text_x, text_y = _paste_nado_icon_aligned_to_text(
        img,
        draw,
        text_xy=(_X_LEFT, _Y_ON_NADO),
        text="On Nado",
        font=f_on_nado,
        glyph_height=_SZ_ON_NADO - 4,  # slightly smaller than text ascender height
        gap=8,
    )
    draw.text((text_x, text_y), "On Nado", fill=LIGHT_GREY, font=f_on_nado)

    # 5) Net Fees + PnL row
    draw.text((_X_LEFT, _Y_FEES_LABEL), "Net Fees", fill=SALMON, font=f_fees_lbl)
    fees = str(data.get("net_fees", "$0"))
    fees_color = RED if fees.lstrip().startswith("-") else GREEN
    draw.text((_X_LEFT, _Y_FEES_VALUE), fees, fill=fees_color, font=f_fees_val)

    draw.text((_X_PNL_COL, _Y_FEES_LABEL), "PnL", fill=WHITE, font=f_pnl_lbl)
    pnl = str(data.get("pnl", "$0"))
    pnl_color = GREEN if pnl.lstrip().startswith("+") else (
        RED if pnl.lstrip().startswith("-") else WHITE
    )
    draw.text((_X_PNL_COL, _Y_FEES_VALUE), pnl, fill=pnl_color, font=f_pnl_val)

    # 6) Footer — Telegram-bot QR (left, replaces former "Join now: …" text);
    #    referral code right-aligned at the bottom. The QR image is the only
    #    join CTA on the card now — users scan it to open the bot.
    _paste_telegram_qr(img)

    referral = data.get("referral_code")
    if referral:
        ref_line = f"Referral Code: {str(referral).strip()}"
        ref_bbox = draw.textbbox((0, 0), ref_line, font=f_ref)
        ref_w = ref_bbox[2] - ref_bbox[0]
        draw.text(
            (_X_REFERRAL_RIGHT - ref_w, _Y_REFERRAL),
            ref_line,
            fill=WHITE,
            font=f_ref,
        )

    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="PNG", optimize=True, compress_level=6)
    return buf.getvalue()
