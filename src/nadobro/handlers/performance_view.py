from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from src.nadobro.db import query_all
from src.nadobro.utils.visual import b, divider, esc, money, pnl_dot, signed, signed_money


PROJECT_ROOT = Path(__file__).resolve().parents[3]
CARD_TEMPLATE = PROJECT_ROOT / "assets" / "cards" / "session_card_template.png"


PAGE_SIZE = 5


def render_performance_view(
    user_id: int,
    network: str,
    page: int = 0,
    page_size: int = PAGE_SIZE,
) -> tuple[str, InlineKeyboardMarkup]:
    """Strategy-only performance, paginated 5 sessions per page.

    Each card on the page shows the per-session stats from the workflow
    plan: Strategy Mode, Pair, Volume Completed, Fees paid, Realized PnL
    and Cost/$1M. Every card has its own Generate PnL button that calls
    ``portfolio:share_pnl:{session_id}`` so the rendered image reflects
    THAT specific session, not cumulative stats.
    """
    page = max(0, int(page))
    offset = page * page_size
    sessions = query_all(
        """
        SELECT *
        FROM strategy_sessions
        WHERE user_id = %s AND network = %s
        ORDER BY COALESCE(stopped_at, started_at, now()) DESC
        LIMIT %s OFFSET %s
        """,
        (int(user_id), network, int(page_size) + 1, int(offset)),
    )
    has_next = len(sessions) > page_size
    sessions = sessions[:page_size]

    lines = [
        f"📊 <b>Performance</b> · {esc(network.upper())} · page {page + 1}",
        "Strategy sessions, newest first",
        divider(),
    ]
    rows: list[list[InlineKeyboardButton]] = []
    if not sessions:
        lines.append("No strategy sessions yet.")
    for idx, session in enumerate(sessions, start=1):
        rows.extend(_render_session_card(lines, session, page * page_size + idx))

    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ Back", callback_data=f"portfolio:performance:{page - 1}"))
    if has_next:
        nav.append(InlineKeyboardButton("Next ➡", callback_data=f"portfolio:performance:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("📜 History", callback_data="portfolio:history")])
    rows.append([InlineKeyboardButton("⬅ Portfolio", callback_data="portfolio:view")])
    return "\n".join(lines)[:3500], InlineKeyboardMarkup(rows)


def _render_session_card(
    lines: list[str], session: dict[str, Any], display_idx: int
) -> list[list[InlineKeyboardButton]]:
    sid = int(session.get("id") or 0)
    strategy = str(session.get("strategy") or "—")
    pair = str(session.get("product_name") or "—")
    volume = _dec(session.get("total_volume_usd") or 0)
    fees = abs(_dec(session.get("total_fees_paid") or 0))
    funding = _dec(session.get("total_funding_paid") or 0)
    pnl = _dec(session.get("realized_pnl") or 0)
    state = str(session.get("status") or "stopped").lower()
    badge = "🟢 running" if state == "running" else "⏹ ended"
    cost_per_million = (
        (fees + abs(funding)) / volume * Decimal("1000000") if volume > 0 else Decimal("0")
    )

    net = pnl - fees - funding
    lines.extend([
        f"{display_idx}. {b(strategy.upper())} · {b(pair)}  {badge}",
        f"    {_fmt_dt(session.get('started_at'))} · ran {_duration(session)}",
        f"    Net {pnl_dot(net)} {signed_money(net)}  (gross {signed_money(pnl)} − fees {money(fees)}"
        f" {'−' if funding >= 0 else '+'} funding {money(abs(funding))})",
        f"    Vol {money(volume)} · Cost/$1M {money(cost_per_million)}",
        "",
    ])
    return [[
        InlineKeyboardButton(
            f"📤 Generate PnL Card · #{display_idx}",
            callback_data=f"portfolio:share_pnl:{sid}",
        )
    ]]


def ensure_session_card_template(path: Path = CARD_TEMPLATE) -> Path:
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    img = Image.new("RGB", (1280, 800), "#101827")
    draw = ImageDraw.Draw(img)
    for y in range(800):
        shade = int(16 + (y / 800) * 28)
        draw.line([(0, y), (1280, y)], fill=(shade, shade + 8, shade + 18))
    draw.rounded_rectangle((48, 48, 1232, 752), radius=36, outline="#2dd4bf", width=3)
    draw.text((72, 70), "Nadobro Strategy Session", fill="#f8fafc", font=_font(44))
    draw.text((72, 720), "TODO: replace with designer asset", fill="#94a3b8", font=_font(24))
    img.save(path)
    return path


def generate_session_card(session: dict[str, Any], network: str, output_path: Path) -> Path:
    template = ensure_session_card_template()
    img = Image.open(template).convert("RGB")
    draw = ImageDraw.Draw(img)
    label = str(session.get("strategy_label") or session.get("strategy") or "Strategy")
    pnl = _dec(session.get("realized_pnl") or 0)
    fees = abs(_dec(session.get("fees") or session.get("total_fees_paid") or 0))
    funding = _dec(session.get("funding") or session.get("total_funding_paid") or 0)
    volume = _dec(session.get("volume") or session.get("total_volume_usd") or 0)
    cost = ((fees + abs(funding)) / volume * Decimal("1000000")) if volume else Decimal("0")
    wins = int(session.get("win_count") or 0)
    losses = int(session.get("loss_count") or 0)
    win_rate = Decimal(wins) / Decimal(max(1, wins + losses)) * Decimal(100) if wins + losses else Decimal("0")

    draw.text((72, 130), label[:40], fill="#f8fafc", font=_font(54))
    draw.text((72, 260), "VOLUME", fill="#94a3b8", font=_font(26))
    draw.text((72, 300), money(volume), fill="#f8fafc", font=_font(70))
    draw.text((720, 250), f"PNL {signed(pnl)}", fill="#22c55e" if pnl >= 0 else "#ef4444", font=_font(48))
    draw.text((720, 330), f"COST/$1M {money(cost)}", fill="#f8fafc", font=_font(36))
    draw.text(
        (72, 650),
        f"Fees -{money(fees)} · Funding {signed(funding)} · Win {win_rate:.1f}% ({wins}/{losses}) · {network.upper()}",
        fill="#f8fafc",
        font=_font(28),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(output_path)
    return output_path


def _font(size: int):
    for path in ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "/System/Library/Fonts/Supplemental/Arial.ttf"):
        try:
            return ImageFont.truetype(path, size=size)
        except Exception:
            continue
    return ImageFont.load_default()


def _dec(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _fmt_dt(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M UTC")
    return str(value or "—")


def _duration(row: dict[str, Any]) -> str:
    start = row.get("started_at")
    end = row.get("ended_at") or row.get("stopped_at") or datetime.now(timezone.utc)
    if not isinstance(start, datetime):
        return "—"
    if isinstance(end, datetime):
        seconds = int((end - start).total_seconds())
        return f"{max(0, seconds) // 3600:02d}:{(max(0, seconds) % 3600) // 60:02d}"
    return "—"
