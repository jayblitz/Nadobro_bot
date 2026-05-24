from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from src.nadobro.utils.x18 import from_x18


ZERO = Decimal("0")


@dataclass(frozen=True)
class PortfolioPosition:
    product_id: int
    symbol: str
    isolated: bool
    is_long: bool
    amount: Decimal
    notional_value: Decimal
    avg_entry_price: Decimal | None
    est_liq_price: Decimal | None
    est_pnl: Decimal | None
    margin_used: Decimal | None
    leverage: Decimal | None
    initial_health: Decimal | None
    maintenance_health: Decimal | None
    upnl_pct: Decimal | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def decimal_value(value: Any, default: Decimal = ZERO) -> Decimal:
    if value is None or value == "":
        return default
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def optional_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return decimal_value(value)


def _pick(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return default


def _position_amount(row: dict[str, Any]) -> Decimal:
    raw_x18 = _pick(row, "amount_x18")
    if raw_x18 is not None:
        return from_x18(raw_x18)
    return decimal_value(_pick(row, "position_size", "amount", "signed_amount", default=0))


def normalize_position(row: dict[str, Any], *, isolated: bool) -> PortfolioPosition:
    amount = _position_amount(row)
    notional = decimal_value(_pick(row, "notional_value", "value", default=0))
    est_pnl = optional_decimal(_pick(row, "est_pnl", "unrealized_pnl"))
    margin_used = optional_decimal(_pick(row, "margin_used", "net_margin"))
    leverage = optional_decimal(_pick(row, "leverage"))
    avg_entry = optional_decimal(_pick(row, "avg_entry_price", "entry_price"))
    liq = optional_decimal(_pick(row, "est_liq_price", "liquidation_price"))
    upnl_pct = unrealized_pnl_pct(
        est_pnl=est_pnl,
        margin_used=margin_used,
        notional_value=notional,
        leverage=leverage,
        isolated=isolated,
    )
    return PortfolioPosition(
        product_id=int(_pick(row, "product_id", default=0) or 0),
        symbol=str(_pick(row, "symbol", "product_name", default="")),
        isolated=bool(isolated),
        is_long=amount >= ZERO,
        amount=amount,
        notional_value=notional,
        avg_entry_price=avg_entry,
        est_liq_price=liq,
        est_pnl=est_pnl,
        margin_used=margin_used,
        leverage=leverage,
        initial_health=optional_decimal(_pick(row, "initial_health")),
        maintenance_health=optional_decimal(_pick(row, "maintenance_health")),
        upnl_pct=upnl_pct,
    )


def positions_from_account_summary(summary: dict[str, Any]) -> list[PortfolioPosition]:
    positions: list[PortfolioPosition] = []
    for row in summary.get("cross_positions") or []:
        positions.append(normalize_position(dict(row), isolated=False))
    for row in summary.get("isolated_positions") or []:
        positions.append(normalize_position(dict(row), isolated=True))
    return positions


def fill_price(base_amount: str | int | Decimal, quote_amount: str | int | Decimal, fee_amount: str | int | Decimal = 0) -> Decimal:
    if isinstance(base_amount, bool) or isinstance(quote_amount, bool) or isinstance(fee_amount, bool):
        raise TypeError("fill_price amounts must be numeric, not bool")
    base = from_x18(base_amount) if isinstance(base_amount, (str, int)) else decimal_value(base_amount)
    quote = from_x18(quote_amount) if isinstance(quote_amount, (str, int)) else decimal_value(quote_amount)
    fee = from_x18(fee_amount) if isinstance(fee_amount, (str, int)) else decimal_value(fee_amount)
    if base == ZERO:
        return ZERO
    return abs(quote + fee) / abs(base)


def funding_payment_amount(payment: dict[str, Any]) -> Decimal:
    return _decimal_from_possible_x18(payment, "amount_x18", "amount")


def funding_payment_label(payment: dict[str, Any]) -> tuple[Decimal, str]:
    amount = funding_payment_amount(payment)
    return abs(amount), "paid" if amount > ZERO else "received"


def funding_rate_conversions(daily_rate: Decimal) -> dict[str, Decimal]:
    daily = decimal_value(daily_rate)
    return {
        "1h": daily / Decimal(24),
        "8h": daily / Decimal(3),
        "1y": daily * Decimal(365),
    }


def unrealized_pnl_pct(
    *,
    est_pnl: Decimal | None,
    margin_used: Decimal | None,
    notional_value: Decimal,
    leverage: Decimal | None,
    isolated: bool,
) -> Decimal | None:
    if est_pnl is None:
        return None
    if isolated:
        denominator = margin_used
    else:
        denominator = (notional_value / leverage) if leverage and leverage != ZERO else None
    if denominator is None or denominator == ZERO:
        return None
    return (est_pnl / denominator) * Decimal(100)


def account_leverage(notional_values: list[Decimal], unweighted_health: Decimal) -> Decimal:
    health = decimal_value(unweighted_health)
    if health == ZERO:
        return ZERO
    return sum(abs(decimal_value(v)) for v in notional_values) / health


def clamp_margin_usage(value: Decimal) -> Decimal:
    raw = decimal_value(value)
    if raw < ZERO:
        return ZERO
    if raw > Decimal(1):
        return Decimal(1)
    return raw


def _row_time(row: dict[str, Any]) -> datetime | None:
    raw = _pick(row, "filled_at", "paid_at", "timestamp")
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw / 1000 if raw > 10_000_000_000 else raw, tz=timezone.utc)
    text = str(raw)
    if text.isdigit():
        value = int(text)
        return datetime.fromtimestamp(value / 1000 if value > 10_000_000_000 else value, tz=timezone.utc)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def aggregate_trading_stats(
    fills: list[dict[str, Any]],
    funding_payments: list[dict[str, Any]] | None = None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    windows = {
        "24h": Decimal(24 * 60 * 60),
        "7d": Decimal(7 * 24 * 60 * 60),
        "30d": Decimal(30 * 24 * 60 * 60),
    }
    keys_with_all = ("24h", "7d", "30d", "all")
    volume_windows = {k: ZERO for k in keys_with_all}
    fees_windows = {k: ZERO for k in keys_with_all}
    pnl_windows = {k: ZERO for k in keys_with_all}
    funding_windows = {k: ZERO for k in keys_with_all}
    wins = 0
    losses = 0
    trade_keys: set[str] = set()
    anonymous_fills = 0

    for fill in fills or []:
        quote = _decimal_from_possible_x18(fill, "quote_filled_x18", "quote_filled")
        fee = abs(_decimal_from_possible_x18(fill, "fee_x18", "fee"))
        pnl = _decimal_from_possible_x18(fill, "realized_pnl_x18", "realized_pnl")
        key = _trade_key(fill)
        if key:
            trade_keys.add(key)
        else:
            anonymous_fills += 1
        volume = abs(quote)
        volume_windows["all"] += volume
        fees_windows["all"] += fee
        pnl_windows["all"] += pnl
        if pnl > ZERO:
            wins += 1
        elif pnl < ZERO:
            losses += 1

        ts = _row_time(fill)
        if ts is None:
            continue
        age = Decimal(max(0, (now - ts.astimezone(timezone.utc)).total_seconds()))
        for win_key, threshold in windows.items():
            if age <= threshold:
                volume_windows[win_key] += volume
                fees_windows[win_key] += fee
                pnl_windows[win_key] += pnl

    for payment in funding_payments or []:
        amount = funding_payment_amount(payment)
        funding_windows["all"] += amount
        ts = _row_time(payment)
        if ts is None:
            continue
        age = Decimal(max(0, (now - ts.astimezone(timezone.utc)).total_seconds()))
        for win_key, threshold in windows.items():
            if age <= threshold:
                funding_windows[win_key] += amount

    decisive = wins + losses
    total_trades = len(trade_keys) + anonymous_fills
    return {
        "volume_windows": volume_windows,
        "fees_windows": fees_windows,
        "pnl_windows": pnl_windows,
        "funding_windows": funding_windows,
        "total_volume": volume_windows["all"],
        "total_fees": fees_windows["all"],
        "total_funding": funding_windows["all"],
        "total_pnl": pnl_windows["all"],
        "wins": wins,
        "losses": losses,
        "total_trades": total_trades,
        "win_rate": (Decimal(wins) / Decimal(decisive) * Decimal(100)) if decisive else ZERO,
    }


def compute_total_equity(
    summary: dict[str, Any] | None,
    spot_balances: dict[Any, Any] | None = None,
) -> dict[str, Decimal]:
    """Compute the Total Balance breakdown for the Overview deck.

    ``Total = Spot + Cross + Isolated`` per the workflow plan (Q1: full
    account net worth, excluding locked NLP).

    - ``spot``: USDC + USDT0 (and any other USD-anchored quote) free
      balances from ``client.get_balance().balances``.
    - ``cross``: sum of cross-position equity from ``calculate_account_summary``.
      Falls back to ``initial_health`` -> ``unweighted_health`` -> the
      account's reported ``equity``/``net_value``/``total_value`` field.
    - ``isolated``: sum of margin_used + est_pnl across isolated positions.
    """
    summary = summary or {}
    spot_balances = spot_balances or {}

    spot = ZERO
    for key, val in spot_balances.items():
        if val is None or val == "":
            continue
        # USDC product id is 0; treat unknown stables as spot too.
        try:
            spot += abs(decimal_value(val))
        except Exception:
            continue

    cross = ZERO
    cross_keys = ("cross_equity", "cross_value", "equity", "net_value", "total_value", "initial_health", "unweighted_health")
    for key in cross_keys:
        if key in summary and summary[key] not in (None, ""):
            try:
                cross = decimal_value(summary[key])
                break
            except Exception:
                continue

    isolated = ZERO
    for row in summary.get("isolated_positions") or []:
        if not isinstance(row, dict):
            continue
        margin = optional_decimal(_pick(row, "margin_used", "net_margin")) or ZERO
        pnl = optional_decimal(_pick(row, "est_pnl", "unrealized_pnl")) or ZERO
        isolated += margin + pnl

    total = spot + cross + isolated
    return {
        "spot": spot,
        "cross": cross,
        "isolated": isolated,
        "total": total,
    }


def _decimal_from_possible_x18(row: dict[str, Any], x18_key: str, human_key: str) -> Decimal:
    if row.get(x18_key) is not None:
        return from_x18(row.get(x18_key))
    value = row.get(human_key)
    if value is None or value == "":
        return ZERO
    text = str(value)
    if any(ch in text for ch in ".eE"):
        return decimal_value(value)
    try:
        integer = int(text)
    except (TypeError, ValueError):
        return decimal_value(value)
    if abs(integer) >= 1_000_000_000:
        return from_x18(integer)
    return Decimal(integer)


def _trade_key(fill: dict[str, Any]) -> str | None:
    for key in ("submission_idx", "order_digest", "digest", "tx_hash"):
        value = fill.get(key)
        if value is not None and value != "":
            return f"{key}:{value}"
    order = fill.get("order") or {}
    if isinstance(order, dict):
        for key in ("digest", "order_digest"):
            value = order.get(key)
            if value is not None and value != "":
                return f"order.{key}:{value}"
    return None
