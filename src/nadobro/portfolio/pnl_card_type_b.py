"""Type B PnL share card — strategy sessions (Grid / RGrid / DGrid / Mid /
Volume / Delta Neutral).

``generate_type_b_card(data) -> PNG bytes``. Composites one session's own
stats onto the master Type B artwork (trophy background for a gain, sad-miner
for a loss), matching ``assets/cards/Master {Positive,Negative} PnL Type B.png``.

Type A (per-trade desk/copy cards) is a separate renderer — this module only
handles the strategy-session cards surfaced from the Performance view.
"""
from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Optional

from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

_ASSETS: Path = Path(__file__).resolve().parents[3] / "assets"
_CARDS: Path = _ASSETS / "cards"
_LOGOS: Path = _ASSETS / "logos"
_ICONS: Path = _ASSETS / "market_icons"
_FONTS: Path = _ASSETS / "fonts"

_BG = {
    True: _CARDS / "Background Positive PnL Type B.png",
    False: _CARDS / "Background Negative PnL Type B.png",
}
_LOGO_CANDIDATES = (
    _LOGOS / "Nadobro Logo trans v2.png",
    _CARDS / "Nadobro Logo trans v2.png",
    _LOGOS / "nadobro logo v2.png",
)
# The official "NADOBRO" wordmark, white on transparent for dark cards (the real
# brand logotype — custom letterforms + mint accents — not typeset text).
_WORDMARK_CANDIDATES = (
    _LOGOS / "nadobro_wordmark_white.png",
    _CARDS / "nadobro_wordmark_white.png",
)
_NADO_CANDIDATES = (
    _LOGOS / "nado.png",
    _CARDS / "nado.png",
)

# ── palette (sampled from the masters) ──────────────────────────
_WHITE = (255, 255, 255)
_MUTED = (160, 164, 172)          # labels: Volume / Net Fees / PnL / On / Mode / Referral
# Positive variant.
_POS_ACCENT = (14, 142, 224)      # badge pill + Mode strategy word (blue)
_POS_PNL = (4, 211, 148)          # PnL value (green)
# Negative variant.
_NEG_ACCENT = (73, 222, 173)      # badge pill + Mode strategy word (green)
_NEG_PNL = (245, 60, 65)          # PnL value (red)
# The Nado mark ("On Nado") is the original logo, unchanged — the SAME in every
# variant, never tinted to the PnL colour.

# ── strategy identity ───────────────────────────────────────────
_STRATEGY_ALIASES = {
    "grid": "grid",
    "dgrid": "dgrid", "d grid": "dgrid", "dynamic grid": "dgrid",
    "rgrid": "rgrid", "r grid": "rgrid", "reverse grid": "rgrid",
    "mid": "mid", "mid mode": "mid", "mm": "mid", "market maker": "mid",
    "market making": "mid",
    "vol": "vol", "volume": "vol", "volume bot": "vol",
    "dn": "dn", "delta neutral": "dn",
}
# Badge pill wording (friendly names) and the short Mode word.
_BADGE_LABEL = {
    "grid": "Grid Strategy", "rgrid": "RGrid Strategy", "dgrid": "DGrid Strategy",
    "mid": "Mid Mode", "vol": "Volume Bot", "dn": "Delta Neutral",
}
_MODE_LABEL = {
    "grid": "Grid", "rgrid": "RGrid", "dgrid": "DGrid",
    "mid": "Mid", "vol": "Volume", "dn": "Delta Neutral",
}


def _strategy_key(raw: object) -> str:
    norm = " ".join(str(raw or "").strip().lower().replace("-", " ").replace("_", " ").split())
    return _STRATEGY_ALIASES.get(norm, "grid")


# ── fonts (Space Grotesk — the brand display face, matching the master) ──
# Space Grotesk is a condensed geometric sans that matches the master's
# headline numerals; Poppins is the fallback so the card still renders on a
# host without the bundled Space Grotesk files.
_BOLD_CHAIN = (
    str(_FONTS / "SpaceGrotesk-Bold.ttf"),
    str(_FONTS / "SpaceGrotesk-SemiBold.ttf"),
    str(_FONTS / "Poppins-Bold.ttf"),
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
)
_SEMIBOLD_CHAIN = (
    str(_FONTS / "SpaceGrotesk-SemiBold.ttf"),
    str(_FONTS / "SpaceGrotesk-Medium.ttf"),
    str(_FONTS / "SpaceGrotesk-Bold.ttf"),
    str(_FONTS / "Poppins-SemiBold.ttf"),
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
)
_REG_CHAIN = (
    str(_FONTS / "SpaceGrotesk-Regular.ttf"),
    str(_FONTS / "SpaceGrotesk-Medium.ttf"),
    str(_FONTS / "Poppins-Regular.ttf"),
    "/System/Library/Fonts/Supplemental/Arial.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
)


def _font(size: int, bold: bool = True, semibold: bool = False) -> ImageFont.FreeTypeFont:
    chain = _SEMIBOLD_CHAIN if semibold else (_BOLD_CHAIN if bold else _REG_CHAIN)
    for p in chain:
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _text_w(draw: ImageDraw.ImageDraw, text: str, font) -> int:
    l, _t, r, _b = draw.textbbox((0, 0), text, font=font)
    return r - l


def _draw_vcenter(draw: ImageDraw.ImageDraw, x: int, cy: int, text: str, font, fill) -> int:
    """Draw ``text`` left-anchored at ``x`` with its glyph box vertically
    centered on ``cy``. Returns the text width."""
    l, t, r, b = draw.textbbox((0, 0), text, font=font)
    draw.text((x - l, cy - (t + b) // 2), text, font=font, fill=fill)
    return r - l


# ── asset helpers ───────────────────────────────────────────────

def _load_logo(px: int) -> Optional[Image.Image]:
    for cand in _LOGO_CANDIDATES:
        if not cand.exists():
            continue
        try:
            img = Image.open(cand).convert("RGBA")
            bbox = img.getbbox()
            if bbox:
                img = img.crop(bbox)
            w, h = img.size
            scale = px / max(w, h)
            return img.resize((max(1, int(w * scale)), max(1, int(h * scale))), Image.LANCZOS)
        except Exception:
            logger.debug("type-b logo load failed for %s", cand, exc_info=True)
    return None


def _load_wordmark(px_height: int) -> Optional[Image.Image]:
    """The official NADOBRO wordmark scaled to ``px_height`` tall."""
    for cand in _WORDMARK_CANDIDATES:
        if not cand.exists():
            continue
        try:
            img = Image.open(cand).convert("RGBA")
            bbox = img.getbbox()
            if bbox:
                img = img.crop(bbox)
            w, h = img.size
            scale = px_height / h
            return img.resize((max(1, int(w * scale)), px_height), Image.LANCZOS)
        except Exception:
            logger.debug("type-b wordmark load failed for %s", cand, exc_info=True)
    return None


def _load_icon(symbol: str, px: int) -> Optional[Image.Image]:
    if not symbol:
        return None
    cand = _ICONS / f"{symbol.upper()}.png"
    if not cand.exists():
        return None
    try:
        img = Image.open(cand).convert("RGBA")
        return img.resize((px, px), Image.LANCZOS)
    except Exception:
        return None


def _nado_plane(px: int) -> Optional[Image.Image]:
    """The original Nado mark scaled to fit ``px`` — the SAME in every variant,
    never tinted. The source is a white plane on black, so its luminance becomes
    the alpha (black → transparent, white plane → opaque), giving a clean mark
    on any background."""
    for cand in _NADO_CANDIDATES:
        if not cand.exists():
            continue
        try:
            src = Image.open(cand).convert("RGB")
            mark = Image.new("RGBA", src.size, (255, 255, 255, 0))
            mark.putalpha(src.convert("L"))       # luminance → alpha
            bbox = mark.getbbox()
            if bbox:
                mark = mark.crop(bbox)
            w, h = mark.size
            scale = px / max(w, h)
            return mark.resize((max(1, int(w * scale)), max(1, int(h * scale))), Image.LANCZOS)
        except Exception:
            logger.debug("type-b nado mark load failed for %s", cand, exc_info=True)
    return None


# ── number formatting ───────────────────────────────────────────

def _fmt_volume(v: float) -> str:
    a = abs(v)
    if a >= 1_000_000:
        return f"${v / 1_000_000:,.2f}M"
    return f"${v:,.2f}"


def _fmt_fees(v: float) -> str:
    # Fees render as a plain magnitude ("$3.51") — no sign, matching the master.
    return f"${abs(v):,.2f}"


def _fmt_pnl(v: float) -> str:
    # Positive: no leading '+'. Negative: leading '-'. Matches the master.
    return f"-${abs(v):,.2f}" if v < 0 else f"${v:,.2f}"


# ── main ────────────────────────────────────────────────────────

def generate_type_b_card(data: dict) -> bytes:
    """Render a Type B strategy-session card to PNG bytes.

    Expected ``data`` keys: strategy (raw key), product, base_symbol, volume
    (float), net_fees (float magnitude), pnl (signed float), referral_code.
    """
    pnl = float(data.get("pnl") or 0.0)
    positive = pnl >= 0
    accent = _POS_ACCENT if positive else _NEG_ACCENT
    pnl_col = _POS_PNL if positive else _NEG_PNL

    canvas = Image.open(_BG[positive]).convert("RGBA")
    W, H = canvas.size
    draw = ImageDraw.Draw(canvas)

    skey = _strategy_key(data.get("strategy"))

    # ── header: monogram + official NADOBRO wordmark ────────────
    logo = _load_logo(82)
    lx = 76
    logo_end = lx
    if logo is not None:
        canvas.alpha_composite(logo, (lx, 110 - logo.height // 2))
        logo_end = lx + logo.width
    wordmark = _load_wordmark(38)
    if wordmark is not None:
        canvas.alpha_composite(wordmark, (logo_end + 20, 110 - wordmark.height // 2))
    else:
        # Fallback only if the brand asset is missing: typeset the name.
        word_font = _font(36, bold=True)
        wx = logo_end + 20
        for ch in "NADOBRO":
            wx += _draw_vcenter(draw, wx, 112, ch, word_font, _WHITE) + 8

    # ── product row: token icon + symbol + strategy badge ───────
    row_cy = 270
    icon = _load_icon(str(data.get("base_symbol") or ""), 80)
    sym_x = 76
    if icon is not None:
        canvas.alpha_composite(icon, (76, row_cy - 40))
        sym_x = 76 + 80 + 22
    product = str(data.get("product") or "").upper()
    prod_w = _draw_vcenter(draw, sym_x, row_cy, product, _font(58, bold=True), _WHITE)

    # strategy badge pill (text-only, per-variant outline)
    badge = _BADGE_LABEL.get(skey, "Grid Strategy")
    badge_font = _font(28, bold=True, semibold=True)
    pad_x, ph = 22, 54
    bx0 = sym_x + prod_w + 24
    bw = _text_w(draw, badge, badge_font) + pad_x * 2
    draw.rounded_rectangle((bx0, row_cy - ph // 2, bx0 + bw, row_cy + ph // 2),
                           radius=ph // 2, outline=accent, width=3)
    _draw_vcenter(draw, bx0 + pad_x, row_cy, badge, badge_font, accent)

    # ── Volume (headline) ───────────────────────────────────────
    _draw_vcenter(draw, 76, 393, "Volume", _font(30, bold=False), _MUTED)
    vol_text = _fmt_volume(float(data.get("volume") or 0.0))
    vsize = 100
    vol_font = _font(vsize, bold=True)
    while _text_w(draw, vol_text, vol_font) > 860 and vsize > 72:
        vsize -= 4
        vol_font = _font(vsize, bold=True)
    _draw_vcenter(draw, 76, 476, vol_text, vol_font, _WHITE)

    # ── "On [nado] Nado" — the original Nado mark, identical in every variant ──
    on_cy = 552
    on_font = _font(34, bold=False)
    nado_font = _font(34, bold=True, semibold=True)
    on_w = _draw_vcenter(draw, 76, on_cy, "On", on_font, _MUTED)
    mark_x = 76 + on_w + 18
    plane = _nado_plane(46)
    if plane is not None:
        canvas.alpha_composite(plane, (mark_x, on_cy - plane.height // 2))
        next_x = mark_x + plane.width + 16
    else:
        next_x = mark_x
    _draw_vcenter(draw, next_x, on_cy, "Nado", nado_font, _WHITE)

    # ── Net Fees / PnL ──────────────────────────────────────────
    lbl_font = _font(30, bold=False)
    val_font = _font(55, bold=True)
    _draw_vcenter(draw, 76, 670, "Net Fees", lbl_font, _MUTED)
    _draw_vcenter(draw, 76, 722, _fmt_fees(float(data.get("net_fees") or 0.0)),
                  val_font, _WHITE)
    _draw_vcenter(draw, 512, 670, "PnL", lbl_font, _MUTED)
    _draw_vcenter(draw, 512, 722, _fmt_pnl(pnl), val_font, pnl_col)

    # ── Mode line ───────────────────────────────────────────────
    mode_font = _font(32, bold=True, semibold=True)
    mlabel_font = _font(32, bold=False)
    mode_cy = 855
    mlabel_w = _draw_vcenter(draw, 76, mode_cy, "Mode: ", mlabel_font, _MUTED)
    _draw_vcenter(draw, 76 + mlabel_w, mode_cy, _MODE_LABEL.get(skey, "Grid"),
                  mode_font, accent)

    # ── Referral code (bottom-right) ────────────────────────────
    ref = str(data.get("referral_code") or "").upper()
    if ref:
        rl_font = _font(32, bold=False)
        rc_font = _font(36, bold=True, semibold=True)
        ref_cy = 888
        label = "Referral Code: "
        label_w = _text_w(draw, label, rl_font)
        gap = 14
        total = label_w + gap + _text_w(draw, ref, rc_font)
        rx = W - 24 - total
        _draw_vcenter(draw, rx, ref_cy, label, rl_font, _MUTED)
        _draw_vcenter(draw, rx + label_w + gap, ref_cy, ref, rc_font, _WHITE)

    out = io.BytesIO()
    canvas.convert("RGB").save(out, format="PNG")
    return out.getvalue()


if __name__ == "__main__":  # pragma: no cover - manual visual check
    sample = {
        "strategy": "dgrid", "product": "BTC:PERP-USDC", "base_symbol": "BTC",
        "volume": 35076.68, "net_fees": 3.51, "pnl": 52.07,
        "referral_code": "3UOEDJUW",
    }
    Path("/tmp/type_b_positive.png").write_bytes(generate_type_b_card(sample))
    Path("/tmp/type_b_negative.png").write_bytes(
        generate_type_b_card({**sample, "strategy": "mid", "pnl": -52.07})
    )
    print("wrote /tmp/type_b_positive.png and /tmp/type_b_negative.png")
