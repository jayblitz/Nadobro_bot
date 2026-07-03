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
        # Prefer notional/leverage (margin actually committed); the SDK
        # summary often omits ``leverage`` for cross rows, so fall back to
        # margin_used rather than returning None (which renders as a fake
        # 0.00% in the deck).
        if leverage and leverage != ZERO:
            denominator = notional_value / leverage
        elif margin_used and margin_used != ZERO:
            denominator = margin_used
        else:
            denominator = None
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


_EPOCH0 = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _fill_signed_base_price_ts(
    row: dict[str, Any],
) -> tuple[int, Decimal, Decimal, datetime | None] | None:
    """Normalize a fill/trade row to ``(product_id, signed_base, price, ts)``.

    ``signed_base`` is +base for long/buy, -base for short/sell. ``price`` is the
    per-unit fill price (prefers venue ``quote_filled_x18 / base_filled_x18``,
    falls back to the recorder ``fill_price``/``price``). Returns ``None`` when the
    row has no usable base / price / side."""
    base = abs(_decimal_from_possible_x18(row, "base_filled_x18", "fill_size"))
    if base <= ZERO:
        base = abs(decimal_value(_pick(row, "fill_size", "size", default=0)))
    if base <= ZERO:
        return None
    quote = abs(_decimal_from_possible_x18(row, "quote_filled_x18", "quote_filled"))
    price = (quote / base) if quote > ZERO else decimal_value(_pick(row, "fill_price", "price", default=0))
    if price <= ZERO:
        return None
    side = str(_pick(row, "side", default="")).lower()
    if side in ("long", "buy"):
        signed = base
    elif side in ("short", "sell"):
        signed = -base
    else:
        return None
    pid = int(_pick(row, "product_id", default=0) or 0)
    return pid, signed, price, _row_time(row)


def realized_pnl_windows_from_rows(
    rows: list[dict[str, Any]] | None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Position-aware realized PnL over a COMPLETE set of fills.

    This venue reports no per-fill realized PnL (the indexer match has only
    base/quote/fee), so realized PnL MUST be derived: replay fills per product in
    time order, keep a running signed position + average entry cost, and realize
    PnL (gross of fees) on the portion each fill CLOSES — ``(exit - entry) * closed``
    for longs, ``(entry - exit) * closed`` for shorts. A position flip realizes the
    closed leg then opens the remainder at the fill price. Realized PnL is attributed
    to the closing fill's timestamp and bucketed into 24h / 7d / 30d / all windows.

    Accuracy depends on ``rows`` being the FULL per-product history — a truncated
    feed misses earlier entry basis. Callers should pass complete
    ``trades_<network>`` fills (see ``get_account_realized_pnl_windows``).
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    thresholds = {"24h": Decimal(24 * 3600), "7d": Decimal(7 * 24 * 3600), "30d": Decimal(30 * 24 * 3600)}
    keys = ("24h", "7d", "30d", "all")
    pnl_windows = {k: ZERO for k in keys}
    wins = 0
    losses = 0
    # Per-window win/loss counters so window-scoped surfaces (Night HOWL's
    # "your last 24h") can report a win rate over the SAME closes as the
    # window's PnL, not the account's whole history.
    wins_windows = {k: 0 for k in keys}
    losses_windows = {k: 0 for k in keys}

    norm = [c for c in (_fill_signed_base_price_ts(r) for r in (rows or [])) if c is not None]
    # Chronological replay; ts-less rows (shouldn't happen for DB rows) sort oldest.
    norm.sort(key=lambda c: c[3] or _EPOCH0)

    state: dict[int, tuple[Decimal, Decimal]] = {}  # product_id -> (signed_pos, avg_entry)
    for pid, signed_qty, price, ts in norm:
        pos, entry = state.get(pid, (ZERO, ZERO))
        realized = ZERO
        if pos == ZERO or (pos > ZERO) == (signed_qty > ZERO):
            # Opening or adding in the same direction: update the average entry.
            new_abs = abs(pos) + abs(signed_qty)
            entry = (entry * abs(pos) + price * abs(signed_qty)) / new_abs
            pos = pos + signed_qty
        else:
            # Reducing / closing / flipping: realize PnL on the closed portion.
            closing = min(abs(pos), abs(signed_qty))
            realized = (price - entry) * closing if pos > ZERO else (entry - price) * closing
            if abs(signed_qty) < abs(pos):
                pos = pos + signed_qty  # move toward flat; entry unchanged
            elif abs(signed_qty) == abs(pos):
                pos, entry = ZERO, ZERO
            else:
                remaining = abs(signed_qty) - abs(pos)  # flip: open the remainder
                pos = remaining if signed_qty > ZERO else -remaining
                entry = price
        state[pid] = (pos, entry)

        if realized != ZERO:
            pnl_windows["all"] += realized
            if realized > ZERO:
                wins += 1
                wins_windows["all"] += 1
            elif realized < ZERO:
                losses += 1
                losses_windows["all"] += 1
            if ts is not None:
                age = Decimal(max(0, int((now - ts.astimezone(timezone.utc)).total_seconds())))
                for wk, thr in thresholds.items():
                    if age <= thr:
                        pnl_windows[wk] += realized
                        if realized > ZERO:
                            wins_windows[wk] += 1
                        else:
                            losses_windows[wk] += 1

    decisive = wins + losses
    return {
        "pnl_windows": pnl_windows,
        "total_pnl": pnl_windows["all"],
        "wins": wins,
        "losses": losses,
        "win_rate": (Decimal(wins) / Decimal(decisive) * Decimal(100)) if decisive else ZERO,
        "wins_windows": wins_windows,
        "losses_windows": losses_windows,
    }


def _fill_fee(row: dict[str, Any]) -> Decimal:
    """Total fee for a fill row (venue ``fee_x18`` preferred, else recorder
    ``fill_fee``/``fees`` + ``builder_fee``)."""
    if row.get("fee_x18") is not None:
        return abs(from_x18(row.get("fee_x18")))
    fee = abs(decimal_value(_pick(row, "fill_fee", "fees", default=0)))
    builder = abs(decimal_value(_pick(row, "builder_fee", default=0)))
    return fee + builder


@dataclass(frozen=True)
class PairedTrade:
    """One position round-trip (flat -> flat, or the leg before a flip), built by
    pairing fills. ``realized_pnl`` is GROSS of fees; ``fees`` is the total paid
    across every fill in the trade; ``net_pnl`` subtracts them."""
    product_id: int
    direction: str            # 'long' or 'short' (the entry direction)
    size: Decimal             # base closed (== entry base for a fully-closed trade)
    entry_price: Decimal      # size-weighted average entry
    exit_price: Decimal | None  # size-weighted average exit (None while open)
    realized_pnl: Decimal     # gross of fees
    fees: Decimal
    opened_at: datetime | None
    closed_at: datetime | None
    closed: bool

    @property
    def net_pnl(self) -> Decimal:
        return self.realized_pnl - self.fees

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["net_pnl"] = self.net_pnl
        return d


class _OpenLeg:
    """Mutable accumulator for the position currently open on one product."""

    __slots__ = ("pid", "direction", "pos", "entry_base", "entry_cost",
                 "exit_base", "exit_proceeds", "realized", "fees", "opened_at", "closed_at")

    def __init__(self, pid, signed_qty, price, fee, ts):
        self.pid = pid
        self.direction = "long" if signed_qty > ZERO else "short"
        self.pos = signed_qty
        self.entry_base = abs(signed_qty)
        self.entry_cost = abs(signed_qty) * price
        self.exit_base = ZERO
        self.exit_proceeds = ZERO
        self.realized = ZERO
        self.fees = fee
        self.opened_at = ts
        self.closed_at = ts

    def _finish(self) -> PairedTrade:
        entry_price = (self.entry_cost / self.entry_base) if self.entry_base > ZERO else ZERO
        exit_price = (self.exit_proceeds / self.exit_base) if self.exit_base > ZERO else None
        return PairedTrade(
            product_id=self.pid, direction=self.direction, size=self.exit_base,
            entry_price=entry_price, exit_price=exit_price, realized_pnl=self.realized,
            fees=self.fees, opened_at=self.opened_at, closed_at=self.closed_at, closed=True,
        )

    def _open_snapshot(self) -> PairedTrade:
        entry_price = (self.entry_cost / self.entry_base) if self.entry_base > ZERO else ZERO
        exit_price = (self.exit_proceeds / self.exit_base) if self.exit_base > ZERO else None
        return PairedTrade(
            product_id=self.pid, direction=self.direction, size=abs(self.pos),
            entry_price=entry_price, exit_price=exit_price, realized_pnl=self.realized,
            fees=self.fees, opened_at=self.opened_at, closed_at=self.closed_at, closed=False,
        )


def pair_fills_into_trades(rows: list[dict[str, Any]] | None) -> list[PairedTrade]:
    """Pair a COMPLETE fill ledger into per-position round-trips.

    Replays fills per product in time order on a running signed position with an
    average entry cost (the venue reports no realized PnL, so it is derived). Each
    time the position returns to flat (or flips) the round-trip is emitted as a
    closed ``PairedTrade`` carrying avg entry, avg exit, realized PnL (gross of
    fees), and the fees paid across the trade. A position still open at the end is
    emitted last with ``closed=False`` (its ``realized_pnl`` is whatever earlier
    partial closes booked; unrealized is left to the live position feed).

    Accuracy depends on ``rows`` being the full per-product history — pass
    venue-confirmed fills (see ``database.get_paired_trades``). Closed trades are
    returned in close-time order; a trailing open trade (if any) comes last."""
    norm = []
    for row in rows or []:
        c = _fill_signed_base_price_ts(row)
        if c is not None:
            pid, signed, price, ts = c
            norm.append((pid, signed, price, _fill_fee(row), ts))
    norm.sort(key=lambda c: c[4] or _EPOCH0)

    open_legs: dict[int, _OpenLeg] = {}
    closed: list[PairedTrade] = []
    for pid, signed_qty, price, fee, ts in norm:
        leg = open_legs.get(pid)
        if leg is None:
            open_legs[pid] = _OpenLeg(pid, signed_qty, price, fee, ts)
            continue
        leg.fees += fee
        leg.closed_at = ts
        if (leg.pos > ZERO) == (signed_qty > ZERO):
            # Same direction: add to the position, re-weighting the avg entry.
            leg.entry_base += abs(signed_qty)
            leg.entry_cost += abs(signed_qty) * price
            leg.pos += signed_qty
            continue
        # Opposite direction: realize PnL on the closed portion.
        closing = min(abs(leg.pos), abs(signed_qty))
        entry_avg = (leg.entry_cost / leg.entry_base) if leg.entry_base > ZERO else ZERO
        leg.realized += (price - entry_avg) * closing if leg.pos > ZERO else (entry_avg - price) * closing
        leg.exit_base += closing
        leg.exit_proceeds += closing * price
        if abs(signed_qty) < abs(leg.pos):
            leg.pos += signed_qty  # partial close; entry avg unchanged
        elif abs(signed_qty) == abs(leg.pos):
            closed.append(leg._finish())
            del open_legs[pid]
        else:
            # Flip: close the current trade fully, open a new one with the remainder.
            closed.append(leg._finish())
            remaining = abs(signed_qty) - abs(leg.pos)
            signed_remaining = remaining if signed_qty > ZERO else -remaining
            open_legs[pid] = _OpenLeg(pid, signed_remaining, price, ZERO, ts)

    result = list(closed)
    result.extend(leg._open_snapshot() for leg in open_legs.values())
    return result


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
