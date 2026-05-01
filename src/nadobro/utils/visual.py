from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

DIVIDER = "─────────────────────────"


def _d(value) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value or "0"))


def divider() -> str:
    return DIVIDER


def header(emoji: str, label: str) -> str:
    return f"*{emoji} {label}*"


def kv(label: str, value: str) -> str:
    return f"{label} {value}"


def _quantize(value: Decimal, decimals: int) -> Decimal:
    quantum = Decimal(1).scaleb(-max(0, int(decimals)))
    return value.quantize(quantum, rounding=ROUND_HALF_UP)


def signed(amount: Decimal, decimals: int = 2) -> str:
    value = _quantize(_d(amount), decimals)
    sign = "+" if value >= 0 else "-"
    return f"{sign}{abs(value):,.{max(0, int(decimals))}f}"


def pct(value: Decimal) -> str:
    return f"{signed(_d(value), 2)}%"


def position_badges(isolated: bool, is_long: bool) -> str:
    margin = "🔒 ISO" if isolated else "⚖️ CROSS"
    direction = "📈 LONG" if is_long else "📉 SHORT"
    return f"{margin} {direction}"


def money(value: Decimal, ccy: str = "USDT", decimals: int = 2) -> str:
    amount = _quantize(_d(value), decimals)
    prefix = "$" if ccy.upper() in {"USD", "USDT", "USDC"} else ""
    suffix = "" if prefix else f" {ccy.upper()}"
    return f"{prefix}{amount:,.{max(0, int(decimals))}f}{suffix}"


def time_ago(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - ts.astimezone(timezone.utc)
    seconds = max(0, int(delta.total_seconds()))
    if seconds < 5:
        return "just now"
    if seconds < 60:
        return f"{seconds}s ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    return f"{hours // 24}d ago"


def stale_banner(last_sync: datetime, threshold_s: int) -> Optional[str]:
    if last_sync.tzinfo is None:
        last_sync = last_sync.replace(tzinfo=timezone.utc)
    age_s = (datetime.now(timezone.utc) - last_sync.astimezone(timezone.utc)).total_seconds()
    if age_s <= int(threshold_s):
        return None
    return f"⚠ Stale · last sync {time_ago(last_sync)}"
