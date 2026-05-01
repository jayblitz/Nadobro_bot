from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src.nadobro.utils.visual import (
    divider,
    header,
    kv,
    money,
    pct,
    position_badges,
    signed,
    stale_banner,
    time_ago,
)


def test_basic_style_helpers():
    assert divider() == "─────────────────────────"
    assert header("🚀", "Open Positions") == "*🚀 Open Positions*"
    assert kv("Value", "$1.00") == "Value $1.00"


def test_number_formatting_helpers():
    assert signed(Decimal("1234.555")) == "+1,234.56"
    assert signed(Decimal("-12.3")) == "-12.30"
    assert pct(Decimal("1.234")) == "+1.23%"
    assert money(Decimal("1000")) == "$1,000.00"
    assert money(Decimal("1.23456"), ccy="BTC", decimals=4) == "1.2346 BTC"


def test_position_badges():
    assert position_badges(True, True) == "🔒 ISO 📈 LONG"
    assert position_badges(False, False) == "⚖️ CROSS 📉 SHORT"


def test_time_ago_edges():
    now = datetime.now(timezone.utc)
    assert time_ago(now - timedelta(seconds=2)) == "just now"
    assert time_ago(now - timedelta(seconds=30)) == "30s ago"
    assert time_ago(now - timedelta(minutes=3)) == "3m ago"


def test_stale_banner_threshold():
    now = datetime.now(timezone.utc)
    assert stale_banner(now - timedelta(seconds=10), threshold_s=30) is None
    assert stale_banner(now - timedelta(seconds=60), threshold_s=30).startswith(
        "⚠ Stale · last sync"
    )
