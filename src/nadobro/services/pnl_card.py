"""Nadobro PnL share card renderer.

``generate_pnl_card(data)`` keeps the public ``dict -> PNG bytes`` API used by
Telegram handlers, but renders the v2 1600x900 design. User-specific content is
drawn dynamically: symbol, strategy tabs, metrics, reaction badge, referral
code, and the bottom-right strategy callout.
"""
from __future__ import annotations

import io
import logging
import math
import re
from pathlib import Path
from typing import Iterable, Literal, NamedTuple

from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

CANVAS_W: int = 1600
CANVAS_H: int = 900

Reaction = Literal["positive", "negative", "bullish", "bearish"]

ASSETS: Path = Path(__file__).resolve().parents[3] / "assets"
CARDS_DIR: Path = ASSETS / "cards"
FONTS_DIR: Path = ASSETS / "fonts"
LOGOS_DIR: Path = ASSETS / "logos"
MASCOTS_DIR: Path = ASSETS / "mascots"
MARKET_ICONS_DIR: Path = ASSETS / "market_icons"

PNL_V2_DIR: Path = CARDS_DIR / "pnl_v2"
_V2_BG_NAMES: dict[Reaction, str] = {
    "positive": "positive.png",
    "negative": "negative.png",
    "bullish": "bullish.png",
    "bearish": "bearish.png",
}

# Legacy background candidates stay as a resilience fallback only.
_LEGACY_BG_CANDIDATES: tuple[str, ...] = (
    "PnL background.jpg",
    "pnl_card_bg.png",
    "pnl_card_bg.jpg",
)

MIDNIGHT_NAVY = (7, 43, 82)
DEEP_BLUE = (13, 79, 139)
ELECTRIC_CYAN = (25, 200, 255)
MINT_SIGNAL = (56, 242, 160)
SOFT_ICE = (234, 244, 250)

WHITE = (255, 255, 255)
SOFT_WHITE = SOFT_ICE
MUTED = (185, 197, 214)
CYAN = ELECTRIC_CYAN
BLUE = ELECTRIC_CYAN
GREEN = MINT_SIGNAL
GREEN_DARK_TEXT = (4, 24, 28)
RED = (255, 76, 91)
PANEL = (4, 8, 24, 230)
PANEL_STROKE = (42, 78, 118, 180)
TAB_STROKE = (70, 102, 150, 180)

_DISPLAY_BOLD_CHAIN: tuple[tuple[Path, int], ...] = (
    (FONTS_DIR / "SpaceGrotesk-Bold.ttf", 0),
    (FONTS_DIR / "SpaceGrotesk-SemiBold.ttf", 0),
    (FONTS_DIR / "Inter-Bold.ttf", 0),
    (Path("/System/Library/Fonts/Helvetica.ttc"), 1),
    (Path("/System/Library/Fonts/HelveticaNeue.ttc"), 1),
    (Path("/Library/Fonts/Arial Unicode.ttf"), 0),
    (Path("/System/Library/Fonts/Supplemental/Arial Bold.ttf"), 0),
    (Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"), 0),
    (Path("/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"), 0),
)

_DISPLAY_REGULAR_CHAIN: tuple[tuple[Path, int], ...] = (
    (FONTS_DIR / "SpaceGrotesk-Regular.ttf", 0),
    (FONTS_DIR / "Inter-Regular.ttf", 0),
    (Path("/System/Library/Fonts/Helvetica.ttc"), 0),
    (Path("/System/Library/Fonts/HelveticaNeue.ttc"), 0),
    (Path("/Library/Fonts/Arial Unicode.ttf"), 0),
    (Path("/System/Library/Fonts/Supplemental/Arial.ttf"), 0),
    (Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"), 0),
    (Path("/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"), 0),
)

_BODY_BOLD_CHAIN: tuple[tuple[Path, int], ...] = (
    (FONTS_DIR / "Inter-Bold.ttf", 0),
    (Path("/System/Library/Fonts/Helvetica.ttc"), 1),
    (Path("/System/Library/Fonts/HelveticaNeue.ttc"), 1),
    (Path("/Library/Fonts/Arial Unicode.ttf"), 0),
    (Path("/System/Library/Fonts/Supplemental/Arial Bold.ttf"), 0),
    (Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"), 0),
    (Path("/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"), 0),
)

_BODY_REGULAR_CHAIN: tuple[tuple[Path, int], ...] = (
    (FONTS_DIR / "Inter-Regular.ttf", 0),
    (Path("/System/Library/Fonts/Helvetica.ttc"), 0),
    (Path("/System/Library/Fonts/HelveticaNeue.ttc"), 0),
    (Path("/Library/Fonts/Arial Unicode.ttf"), 0),
    (Path("/System/Library/Fonts/Supplemental/Arial.ttf"), 0),
    (Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"), 0),
    (Path("/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"), 0),
)

_DATA_BOLD_CHAIN: tuple[tuple[Path, int], ...] = (
    (FONTS_DIR / "IBMPlexMono-Bold.ttf", 0),
    (FONTS_DIR / "IBM-Plex-Mono-Bold.ttf", 0),
    (FONTS_DIR / "IBM Plex Mono Bold.ttf", 0),
    (Path("/System/Library/Fonts/Menlo.ttc"), 1),
    (Path("/System/Library/Fonts/Supplemental/Courier New Bold.ttf"), 0),
    (Path("/System/Library/Fonts/Courier.ttc"), 1),
    (Path("/System/Library/Fonts/SFNSMono.ttf"), 0),
    (Path("/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf"), 0),
    (Path("/usr/share/fonts/truetype/liberation/LiberationMono-Bold.ttf"), 0),
)

_DATA_REGULAR_CHAIN: tuple[tuple[Path, int], ...] = (
    (FONTS_DIR / "IBMPlexMono-Regular.ttf", 0),
    (FONTS_DIR / "IBM-Plex-Mono-Regular.ttf", 0),
    (FONTS_DIR / "IBM Plex Mono Regular.ttf", 0),
    (Path("/System/Library/Fonts/Menlo.ttc"), 0),
    (Path("/System/Library/Fonts/Supplemental/Courier New.ttf"), 0),
    (Path("/System/Library/Fonts/Courier.ttc"), 0),
    (Path("/System/Library/Fonts/SFNSMono.ttf"), 0),
    (Path("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"), 0),
    (Path("/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf"), 0),
)

_EMOJI_FONT_CHAIN: tuple[tuple[Path, int], ...] = (
    (Path("/System/Library/Fonts/Apple Color Emoji.ttc"), 0),
    (Path("/System/Library/Fonts/Apple Color Emoji.ttf"), 0),
    (Path("/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf"), 0),
)

_BRAND_LOGO_CANDIDATES: tuple[Path, ...] = (
    LOGOS_DIR / "Nadobro Logo v3.png",
    LOGOS_DIR / "Nadobro Logo trans v2.png",
    LOGOS_DIR / "nadobro logo v2.png",
)

_NADO_MARK_CANDIDATES: tuple[Path, ...] = (
    LOGOS_DIR / "nado.png",
    ASSETS / "logo" / "nado.png",
    Path(__file__).resolve().parents[3] / "design" / "assets" / "nado.png",
)

_MASCOT_CANDIDATES: tuple[Path, ...] = (
    MASCOTS_DIR / "nadobro_mascot_trans.png",
    LOGOS_DIR / "Nadobro mascot trans.png",
    LOGOS_DIR / "Nadobro mascot.png",
)

# Verified from https://gateway.prod.nado.xyz/v2/assets on 2026-07-04.
NADO_ASSET_SYMBOLS: frozenset[str] = frozenset(
    {
        "AAPL-PERP",
        "AAVE-PERP",
        "ADA-PERP",
        "AMD-PERP",
        "AMZN-PERP",
        "ARB-PERP",
        "ASTER-PERP",
        "AVAX-PERP",
        "AVGO-PERP",
        "AXS-PERP",
        "BCH-PERP",
        "BERA-PERP",
        "BNB-PERP",
        "BTC-PERP",
        "CHIP-PERP",
        "DELL-PERP",
        "DOGE-PERP",
        "ENA-PERP",
        "ETH-PERP",
        "EURUSD-PERP",
        "FARTCOIN-PERP",
        "GBPUSD-PERP",
        "GOOGL-PERP",
        "HYPE-PERP",
        "INTC-PERP",
        "JUP-PERP",
        "KBTC",
        "LINK-PERP",
        "LIT-PERP",
        "LTC-PERP",
        "MEGA-PERP",
        "META-PERP",
        "MON-PERP",
        "MRVL-PERP",
        "MSFT-PERP",
        "MU-PERP",
        "NEAR-PERP",
        "NLP",
        "NVDA-PERP",
        "ONDO-PERP",
        "PENG-PERP",
        "PENGU-PERP",
        "PUMP-PERP",
        "QQQ-PERP",
        "SKR-PERP",
        "SKY-PERP",
        "SNDK-PERP",
        "SOL-PERP",
        "SPCX-PERP",
        "SPY-PERP",
        "SUI-PERP",
        "TAO-PERP",
        "TON-PERP",
        "TSLA-PERP",
        "UNI-PERP",
        "USDC",
        "USDT0",
        "USDJPY-PERP",
        "USELESS-PERP",
        "VIRTUAL-PERP",
        "VVV-PERP",
        "WETH",
        "WLD-PERP",
        "WLFI-PERP",
        "WTI-PERP",
        "XAG-PERP",
        "XAUT-PERP",
        "XAUT0",
        "XMR-PERP",
        "XPL-PERP",
        "XRP-PERP",
        "ZEC-PERP",
        "ZRO-PERP",
        "kBONK-PERP",
        "kPEPE-PERP",
        "wAAPLx",
        "wAMZNx",
        "wGOOGLx",
        "wMETAx",
        "wMSFTx",
        "wNVDAx",
        "wQQQx",
        "wSPYx",
        "wTSLAx",
    }
)


class StrategyCopy(NamedTuple):
    title: str
    subtitle: str
    subtitle2: str
    icon: str


STRATEGIES: tuple[str, ...] = ("DGRID", "GRID", "RGRID", "MID MODE", "VOLUME BOT", "DN")

STRATEGY_EMOJIS: dict[str, str] = {
    "GRID": "🤖",
    "RGRID": "🧱",
    "DGRID": "⚡",
    "MID MODE": "🎯",
    "DN": "⚖️",
    "VOLUME BOT": "🔁",
}

STRATEGY_ALIASES: dict[str, str] = {
    "grid": "GRID",
    "dgrid": "DGRID",
    "d grid": "DGRID",
    "dynamic grid": "DGRID",
    "rgrid": "RGRID",
    "r grid": "RGRID",
    "reverse grid": "RGRID",
    "mid": "MID MODE",
    "mid mode": "MID MODE",
    "mm": "MID MODE",
    "market maker": "MID MODE",
    "volume": "VOLUME BOT",
    "volume bot": "VOLUME BOT",
    "dn": "DN",
    "delta neutral": "DN",
}

STRATEGY_COPY: dict[str, StrategyCopy] = {
    "DGRID": StrategyCopy(
        "DGRID STRATEGY ACTIVE",
        "Defending range.",
        "Managing drawdown.",
        "dots",
    ),
    "GRID": StrategyCopy(
        "GRID STRATEGY ACTIVE",
        "Capturing opportunities.",
        "Building results.",
        "target",
    ),
    "RGRID": StrategyCopy(
        "RGRID STRATEGY ACTIVE",
        "Reversing the range.",
        "Fading the move.",
        "reverse",
    ),
    "MID MODE": StrategyCopy(
        "MID MODE ACTIVE",
        "Balanced execution.",
        "Staying market neutral.",
        "balance",
    ),
    "VOLUME BOT": StrategyCopy(
        "VOLUME BOT ACTIVE",
        "Momentum unlocked.",
        "Scaling winning flow.",
        "bars",
    ),
    "DN": StrategyCopy(
        "DN ACTIVE",
        "Volatility stress.",
        "Risk defense engaged.",
        "shield",
    ),
}


def _load_font(chain: Iterable[tuple[Path, int]], size: int) -> ImageFont.ImageFont:
    for path, index in chain:
        if not path.exists():
            continue
        try:
            return ImageFont.truetype(str(path), size, index=index)
        except OSError:
            continue
    logger.warning("PnL card: no TrueType font available; using Pillow default bitmap font")
    return ImageFont.load_default()


def _font_chain(role: str, bold: bool) -> Iterable[tuple[Path, int]]:
    if role == "display":
        return _DISPLAY_BOLD_CHAIN if bold else _DISPLAY_REGULAR_CHAIN
    if role == "data":
        return _DATA_BOLD_CHAIN if bold else _DATA_REGULAR_CHAIN
    return _BODY_BOLD_CHAIN if bold else _BODY_REGULAR_CHAIN


def _get_font(size: int, *, bold: bool = False, role: str = "body") -> ImageFont.ImageFont:
    return _load_font(_font_chain(role, bold), size)


def _get_emoji_font(size: int) -> ImageFont.ImageFont:
    return _load_font(_EMOJI_FONT_CHAIN, size)


def _fit_font(
    draw: ImageDraw.ImageDraw,
    text: str,
    *,
    max_width: int,
    start_size: int,
    min_size: int = 24,
    bold: bool = False,
    role: str = "body",
) -> ImageFont.ImageFont:
    for size in range(start_size, min_size - 1, -2):
        font = _get_font(size, bold=bold, role=role)
        bbox = draw.textbbox((0, 0), text, font=font)
        if bbox[2] - bbox[0] <= max_width:
            return font
    return _get_font(min_size, bold=bold, role=role)


def _draw_emoji(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    emoji: str,
    font: ImageFont.ImageFont,
) -> None:
    try:
        draw.text(xy, emoji, font=font, embedded_color=True)
    except TypeError:
        draw.text(xy, emoji, fill=WHITE, font=font)


_NUM_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)")


def _parse_float(raw: object, *, default: float = 0.0) -> float:
    """Parse user-facing money/volume strings such as ``+$1.2M`` or ``-$42``."""
    if raw is None:
        return default
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        value = float(raw)
        return value if math.isfinite(value) else default

    s = str(raw).strip()
    if not s:
        return default
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()").lower()
    s = (
        s.replace("$", "")
        .replace(",", "")
        .replace("_", "")
        .replace("usd", "")
        .replace("usdc", "")
        .replace(" ", "")
    )
    match = _NUM_RE.match(s)
    if not match:
        return default
    try:
        value = float(match.group(0))
    except ValueError:
        return default
    suffix = s[match.end() : match.end() + 1]
    if suffix == "k":
        value *= 1_000
    elif suffix == "m":
        value *= 1_000_000
    elif suffix == "b":
        value *= 1_000_000_000
    return -abs(value) if neg else value


def _format_currency(value: float, *, signed: bool = False) -> str:
    sign = ""
    if signed:
        sign = "+" if value >= 0 else "-"
    elif value < 0:
        sign = "-"
    return f"{sign}${abs(value):,.2f}"


def _display_amount(raw: object, *, signed: bool = False) -> str:
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return _format_currency(0.0, signed=signed)
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return _format_currency(float(raw), signed=signed)
    return str(raw).strip()


def _normalize_key(raw: object) -> str:
    return " ".join(str(raw or "").strip().lower().replace("-", " ").replace("_", " ").split())


def _strategy_key(raw: object) -> str:
    return STRATEGY_ALIASES.get(_normalize_key(raw), "GRID")


def _strategy_label(raw: object) -> str:
    """Return the v2 tab label for tests and legacy callers that imported it."""
    return _strategy_key(raw)


def _choose_reaction(pnl: float, volume: float) -> Reaction:
    if pnl < -1000:
        return "bearish"
    if pnl < 0:
        return "negative"
    if pnl >= 1000 and volume >= 100_000:
        return "bullish"
    return "positive"


def _resolve_bg_path(reaction: Reaction) -> Path:
    v2_path = PNL_V2_DIR / _V2_BG_NAMES[reaction]
    if v2_path.exists():
        return v2_path
    for name in _LEGACY_BG_CANDIDATES:
        candidate = CARDS_DIR / name
        if candidate.exists():
            return candidate
    return v2_path


def _first_existing(paths: Iterable[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def _load_transparent_asset(path: Path, *, key_black: bool = False) -> Image.Image | None:
    try:
        img = Image.open(path).convert("RGBA")
    except (OSError, ValueError):
        return None
    if key_black:
        px = img.load()
        for y in range(img.height):
            for x in range(img.width):
                r, g, b, a = px[x, y]
                if a and r < 24 and g < 24 and b < 24:
                    px[x, y] = (0, 0, 0, 0)
    bbox = img.getbbox()
    return img.crop(bbox) if bbox else img


def _tint_visible_pixels(img: Image.Image, color: tuple[int, int, int]) -> Image.Image:
    tinted = Image.new("RGBA", img.size, color + (0,))
    tinted.putalpha(img.getchannel("A"))
    return tinted


def _paste_contained(
    canvas: Image.Image,
    asset: Image.Image,
    box: tuple[int, int, int, int],
    *,
    anchor: str = "center",
) -> None:
    x1, y1, x2, y2 = box
    target_w = max(1, x2 - x1)
    target_h = max(1, y2 - y1)
    img = asset.copy()
    img.thumbnail((target_w, target_h), Image.LANCZOS)
    if anchor == "left":
        x = x1
    elif anchor == "right":
        x = x2 - img.width
    else:
        x = x1 + (target_w - img.width) // 2
    y = y1 + (target_h - img.height) // 2
    canvas.alpha_composite(img, dest=(x, y))


def _draw_brand_header(canvas: Image.Image, draw: ImageDraw.ImageDraw) -> None:
    logo_path = _first_existing(_BRAND_LOGO_CANDIDATES)
    if logo_path:
        logo = _load_transparent_asset(logo_path, key_black=False)
        if logo:
            _paste_contained(canvas, logo, (58, 42, 145, 124), anchor="left")
    wordmark_font = _get_font(42, bold=True, role="display")
    draw.text((165, 66), "NadoBro", fill=WHITE, font=wordmark_font)


def _load_nado_mark() -> Image.Image | None:
    path = _first_existing(_NADO_MARK_CANDIDATES)
    if path is None:
        return None
    mark = _load_transparent_asset(path, key_black=True)
    return mark


def _paste_nado_mark(
    canvas: Image.Image,
    draw: ImageDraw.ImageDraw,
    box: tuple[int, int, int, int],
) -> None:
    mark = _load_nado_mark()
    if mark is None:
        _draw_plane(draw, box[0], box[1], min(box[2] - box[0], box[3] - box[1]), WHITE)
        return
    _paste_contained(canvas, mark, box)


def _draw_plane(draw: ImageDraw.ImageDraw, x: int, y: int, size: int, fill: tuple[int, int, int]) -> None:
    pts = [
        (x, y + size // 2),
        (x + size, y),
        (x + int(size * 0.68), y + size),
        (x + int(size * 0.45), y + int(size * 0.62)),
        (x + int(size * 0.24), y + int(size * 0.82)),
        (x + int(size * 0.36), y + int(size * 0.55)),
    ]
    draw.polygon(pts, fill=fill)


def _draw_icon(
    draw: ImageDraw.ImageDraw,
    icon: str,
    box: tuple[int, int, int, int],
    color: tuple[int, int, int],
    *,
    width: int = 4,
) -> None:
    x1, y1, x2, y2 = box
    w = x2 - x1
    h = y2 - y1
    cx = x1 + w // 2
    cy = y1 + h // 2

    if icon == "dots":
        r = max(2, w // 16)
        gap = w // 4
        start_x = cx - gap
        start_y = cy - gap
        for row in range(3):
            for col in range(3):
                x = start_x + col * gap
                y = start_y + row * gap
                draw.ellipse((x - r, y - r, x + r, y + r), fill=color)
    elif icon == "target":
        draw.ellipse((x1 + 4, y1 + 4, x2 - 4, y2 - 4), outline=color, width=width)
        draw.line((cx, y1 + 8, cx, y2 - 8), fill=color, width=width)
        draw.line((x1 + 8, cy, x2 - 8, cy), fill=color, width=width)
        draw.ellipse((cx - 6, cy - 6, cx + 6, cy + 6), fill=color)
    elif icon == "reverse":
        draw.arc((x1 + 3, y1 + 6, x2 - 8, cy + 18), 185, 350, fill=color, width=width)
        draw.arc((x1 + 8, cy - 18, x2 - 3, y2 - 6), 5, 170, fill=color, width=width)
        draw.polygon([(x1 + 12, cy + 16), (x1 + 2, cy + 16), (x1 + 8, cy + 26)], fill=color)
        draw.polygon([(x2 - 12, cy - 16), (x2 - 2, cy - 16), (x2 - 8, cy - 26)], fill=color)
    elif icon == "balance":
        bar_w = max(3, w // 12)
        for i, frac in enumerate((0.55, 0.9, 0.7)):
            bx = x1 + w // 4 + i * w // 5
            by = y2 - int(h * frac)
            draw.rounded_rectangle((bx, by, bx + bar_w, y2 - 5), radius=2, fill=color)
        draw.line((x1 + 7, cy, x2 - 7, cy), fill=color, width=width)
    elif icon == "bars":
        bar_w = max(4, w // 10)
        for i, frac in enumerate((0.35, 0.55, 0.78)):
            bx = x1 + w // 4 + i * w // 5
            by = y2 - int(h * frac)
            draw.rounded_rectangle((bx, by, bx + bar_w, y2 - 5), radius=2, fill=color)
    elif icon == "shield":
        pts = [(cx, y1 + 5), (x2 - 8, y1 + 15), (x2 - 12, cy + 18), (cx, y2 - 4), (x1 + 12, cy + 18), (x1 + 8, y1 + 15)]
        draw.line(pts + [pts[0]], fill=color, width=width)
        bolt = [
            (cx + 3, y1 + 18),
            (cx - 8, cy + 2),
            (cx + 2, cy + 2),
            (cx - 5, y2 - 14),
            (cx + 11, cy - 5),
            (cx + 1, cy - 5),
        ]
        draw.polygon(bolt, fill=color)
    elif icon == "down":
        draw.line((x1 + 9, y1 + 14, cx, y2 - 12, x2 - 9, y1 + 14), fill=color, width=width)
    elif icon == "up":
        draw.line((x1 + 9, y2 - 12, cx, y1 + 14, x2 - 9, y2 - 12), fill=color, width=width)


def _asset_key(symbol: str) -> str:
    raw = symbol.strip()
    before_quote = re.split(r"[:/_]", raw, maxsplit=1)[0]
    if before_quote.upper().endswith("-PERP"):
        nado_symbol = before_quote.upper()
    else:
        nado_symbol = before_quote.split("-")[0].upper()
    if nado_symbol.endswith("-PERP"):
        nado_symbol = nado_symbol.removesuffix("-PERP")
    aliases = {
        "WETH": "ETH",
        "ETHEREUM": "ETH",
        "KBTC": "BTC",
        "XBT": "BTC",
        "BITCOIN": "BTC",
        "SOLANA": "SOL",
        "USDC.E": "USDC",
        "NLP": "NADO",
        "NADOBRO": "NADO",
        "USDT0": "USDT",
    }
    if nado_symbol.startswith("W") and nado_symbol.endswith("X"):
        nado_symbol = nado_symbol[1:-1]
    return aliases.get(nado_symbol or "NADO", nado_symbol or "NADO")


def _resolve_market_icon_path(symbol: str) -> Path | None:
    key = _asset_key(symbol)
    candidates = (
        MARKET_ICONS_DIR / f"{key}.png",
        MARKET_ICONS_DIR / f"{key}.jpg",
        MARKET_ICONS_DIR / f"{key}.jpeg",
        MARKET_ICONS_DIR / f"{key.lower()}.png",
    )
    if key == "NADO":
        candidates = (LOGOS_DIR / "Nadobro Logo v3.png",) + candidates
    return _first_existing(candidates)


def _draw_symbol_badge(canvas: Image.Image, draw: ImageDraw.ImageDraw, symbol: str) -> None:
    box = (58, 173, 132, 247)
    draw.ellipse(box, fill=(10, 28, 68, 215), outline=(106, 176, 255), width=2)
    icon_path = _resolve_market_icon_path(symbol)
    if icon_path is None:
        logger.warning("PnL card: no market icon asset found for symbol=%r", symbol)
        return
    icon = _load_transparent_asset(icon_path, key_black=False)
    if icon is None:
        logger.warning("PnL card: market icon asset could not be loaded: %s", icon_path)
        return
    _paste_contained(canvas, icon, (66, 181, 124, 239))


def _draw_strategy_tabs(draw: ImageDraw.ImageDraw, selected: str) -> None:
    x = 58
    y = 330
    h = 54
    widths = {
        "DGRID": 132,
        "GRID": 132,
        "RGRID": 132,
        "MID MODE": 154,
        "VOLUME BOT": 176,
        "DN": 72,
    }
    emoji_font = _get_emoji_font(26)
    dn_emoji_font = _get_emoji_font(20)
    for label in STRATEGIES:
        w = widths[label]
        is_selected = label == selected
        fill = GREEN if is_selected else PANEL
        outline = GREEN if is_selected else TAB_STROKE
        stroke_width = 1 if is_selected else 1
        text_color = GREEN_DARK_TEXT if is_selected else SOFT_WHITE
        draw.rounded_rectangle(
            (x, y, x + w, y + h),
            radius=10,
            fill=fill,
            outline=outline,
            width=stroke_width,
        )
        if label == "DN":
            _draw_emoji(draw, (x + 10, y + 16), STRATEGY_EMOJIS[label], dn_emoji_font)
            text_x = x + 44
            start_size = 15
            min_size = 13
        else:
            _draw_emoji(draw, (x + 22, y + 14), STRATEGY_EMOJIS[label], emoji_font)
            text_x = x + 66
            start_size = 21
            min_size = 15
        tab_font = _fit_font(
            draw,
            label,
            max_width=x + w - text_x - 10,
            start_size=start_size,
            min_size=min_size,
            bold=True,
            role="display",
        )
        bbox = draw.textbbox((0, 0), label, font=tab_font)
        draw.text((text_x, y + (h - (bbox[3] - bbox[1])) // 2 - bbox[1]), label, fill=text_color, font=tab_font)
        x += w + 10


def _draw_reaction_badge(draw: ImageDraw.ImageDraw, reaction: Reaction) -> None:
    is_green = reaction in {"positive", "bullish"}
    color = GREEN if is_green else RED
    label = reaction.upper()
    box = (58, 682, 345, 748)
    draw.rounded_rectangle(box, radius=10, fill=(4, 8, 24, 255), outline=color, width=1)
    icon_box = (78, 692, 124, 738)
    draw.ellipse(icon_box, outline=color, width=4)
    if is_green:
        _draw_icon(draw, "up", (86, 700, 116, 730), color, width=4)
    else:
        _draw_icon(draw, "down", (86, 700, 116, 730), color, width=4)
    font = _get_font(27, bold=True, role="display")
    draw.text((162, 704), label, fill=color, font=font)


def _draw_strategy_box(draw: ImageDraw.ImageDraw, selected: str) -> None:
    copy = STRATEGY_COPY[selected]
    color = RED if selected == "DN" else GREEN
    box = (1000, 742, 1418, 852)
    draw.rounded_rectangle(box, radius=14, fill=(4, 8, 24, 248), outline=PANEL_STROKE, width=2)
    icon_outer = (1024, 762, 1094, 832)
    draw.ellipse(icon_outer, outline=color, width=3)
    _draw_emoji(draw, (1037, 773), STRATEGY_EMOJIS[selected], _get_emoji_font(48))
    title_font = _fit_font(
        draw,
        copy.title,
        max_width=280,
        start_size=24,
        min_size=18,
        bold=True,
        role="display",
    )
    body_font = _get_font(21, bold=False)
    draw.text((1112, 765), copy.title, fill=color, font=title_font)
    draw.text((1112, 801), copy.subtitle, fill=WHITE, font=body_font)
    draw.text((1112, 827), copy.subtitle2, fill=WHITE, font=body_font)


def _draw_card_overlay(
    img: Image.Image,
    data: dict,
    *,
    selected_strategy: str,
    reaction: Reaction,
    pnl_value: float,
) -> None:
    draw = ImageDraw.Draw(img, "RGBA")
    _draw_brand_header(img, draw)

    symbol = str(data.get("symbol") or "BTC:PERP-USDC").strip()
    _draw_symbol_badge(img, draw, symbol)
    symbol_font = _fit_font(
        draw,
        symbol,
        max_width=600,
        start_size=68,
        min_size=36,
        bold=True,
        role="display",
    )
    draw.text((165, 184), symbol, fill=WHITE, font=symbol_font)

    on_nado_font = _get_font(31, bold=False)
    draw.text((58, 276), "On Nado", fill=SOFT_WHITE, font=on_nado_font)
    _paste_nado_mark(img, draw, (207, 273, 242, 308))

    _draw_strategy_tabs(draw, selected_strategy)

    label_font = _get_font(25, bold=False)
    volume_font = _fit_font(
        draw,
        _display_amount(data.get("volume")),
        max_width=395,
        start_size=62,
        min_size=36,
        bold=True,
        role="data",
    )
    fees_font = _fit_font(
        draw,
        _display_amount(data.get("net_fees")),
        max_width=320,
        start_size=43,
        min_size=28,
        bold=True,
        role="data",
    )
    pnl_text = _display_amount(data.get("pnl"), signed=True)
    pnl_font = _fit_font(
        draw,
        pnl_text,
        max_width=500,
        start_size=88,
        min_size=48,
        bold=True,
        role="data",
    )

    draw.text((58, 444), "VOLUME", fill=SOFT_WHITE, font=label_font)
    draw.text((58, 488), _display_amount(data.get("volume")), fill=WHITE, font=volume_font)
    draw.text((58, 570), "NET FEES", fill=SOFT_WHITE, font=label_font)
    draw.text((58, 610), _display_amount(data.get("net_fees")), fill=WHITE, font=fees_font)
    draw.line((462, 478, 462, 652), fill=(215, 225, 235, 150), width=2)
    draw.text((510, 486), "PnL", fill=SOFT_WHITE, font=_get_font(32, bold=False, role="body"))
    draw.text((520, 568), pnl_text, fill=GREEN if pnl_value >= 0 else RED, font=pnl_font)

    _draw_reaction_badge(draw, reaction)

    referral = str(data.get("referral_code") or "").strip()
    if referral:
        draw.text((58, 780), "REFERRAL CODE", fill=SOFT_WHITE, font=_get_font(21, bold=False))
        code_font = _fit_font(
            draw,
            referral,
            max_width=250,
            start_size=33,
            min_size=22,
            bold=False,
            role="data",
        )
        draw.text((58, 808), referral, fill=BLUE, font=code_font)

    _draw_strategy_box(draw, selected_strategy)


def generate_pnl_card(data: dict) -> bytes:
    """Render a v2 PnL card PNG as bytes.

    Accepted keys are unchanged: ``symbol``, ``strategy``, ``volume``,
    ``net_fees``, ``pnl``, and ``referral_code``. Numeric strings may include
    currency symbols, commas, signs, and K/M/B suffixes.
    """
    pnl_value = _parse_float(data.get("pnl"))
    volume_value = _parse_float(data.get("volume"))
    reaction = _choose_reaction(pnl_value, volume_value)
    bg_path = _resolve_bg_path(reaction)
    if not bg_path.exists():
        looked_for = [str(PNL_V2_DIR / name) for name in _V2_BG_NAMES.values()]
        looked_for.extend(str(CARDS_DIR / name) for name in _LEGACY_BG_CANDIDATES)
        raise FileNotFoundError("Background image not found: looked for " + ", ".join(looked_for))

    img = Image.open(bg_path).convert("RGBA")
    if img.size != (CANVAS_W, CANVAS_H):
        img = img.resize((CANVAS_W, CANVAS_H), Image.LANCZOS)

    selected_strategy = _strategy_key(data.get("strategy"))
    _draw_card_overlay(
        img,
        data,
        selected_strategy=selected_strategy,
        reaction=reaction,
        pnl_value=pnl_value,
    )

    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="PNG", optimize=True, compress_level=6)
    return buf.getvalue()


def generate_dev_preview_cards(output_dir: str | Path) -> dict[str, Path]:
    """Generate four local preview cards for v2 layout checks."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    samples = {
        "positive_grid": {
            "symbol": "ETH:PERP-USDC",
            "strategy": "grid",
            "volume": "$24,842.19",
            "net_fees": "$14.28",
            "pnl": "+$428.32",
            "referral_code": "NBGRID01",
        },
        "negative_dgrid": {
            "symbol": "ETH:PERP-USDC",
            "strategy": "dgrid",
            "volume": "$12,604.11",
            "net_fees": "$8.24",
            "pnl": "-$138.92",
            "referral_code": "NBDGRID02",
        },
        "bullish_volume_bot": {
            "symbol": "ETH:PERP-USDC",
            "strategy": "volume_bot",
            "volume": "$328,904.57",
            "net_fees": "$92.40",
            "pnl": "+$12,482.66",
            "referral_code": "NBVOL03",
        },
        "bearish_dn": {
            "symbol": "ETH:PERP-USDC",
            "strategy": "dn",
            "volume": "$86,112.34",
            "net_fees": "$26.75",
            "pnl": "-$6,248.71",
            "referral_code": "NBDN04",
        },
    }
    written: dict[str, Path] = {}
    for name, sample in samples.items():
        path = out / f"{name}.png"
        path.write_bytes(generate_pnl_card(sample))
        written[name] = path
    return written
