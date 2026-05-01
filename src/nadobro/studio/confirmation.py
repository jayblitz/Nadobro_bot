"""Telegram confirmation card rendering for Strategy Studio."""

from __future__ import annotations

from datetime import timezone

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from src.nadobro.studio.intent import Condition, PriceLevel, Quantity, TradingIntent


def _quantity(q: Quantity | None) -> str:
    if q is None:
        return "not set"
    if q.type == "percent_balance":
        return f"{q.value:g}% of balance"
    if q.type == "usdt":
        return f"{q.value:g} USDT"
    return f"{q.value:g} contracts"


def _price_level(level: PriceLevel | None) -> str:
    if level is None:
        return "not set"
    suffix = " trailing" if level.trailing else ""
    if level.type == "percent":
        return f"{level.value:+g}%{suffix}"
    return f"${level.value:,.2f}{suffix}"


def _condition(condition: Condition) -> str:
    period = f"({condition.period})" if condition.period else ""
    return f"{condition.indicator}{period} {condition.timeframe} {condition.operator} {condition.value:g}"


def build_confirmation_card(intent: TradingIntent, session_id: int | None = None) -> tuple[str, InlineKeyboardMarkup]:
    symbol = intent.symbol or "?"
    network = intent.network or "user default"
    action = intent.action.upper()
    order_type = intent.order_type or "not set"
    trigger = "; ".join(_condition(c) for c in intent.conditions) if intent.conditions else "none"
    entry = f"${intent.entry_price:,.2f}" if intent.entry_price else ("market" if order_type == "market" else "not set")
    time_limit = "not set"
    if intent.time_limit:
        time_limit = intent.time_limit.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    text = (
        "📋 Strategy Summary\n"
        "──────────────────\n"
        f"Action: {action} {symbol} (Nado {network})\n"
        f"Type:   {order_type.title()}\n"
        f"Trigger: {trigger}\n"
        f"Entry:  {entry}\n"
        f"Size:   {_quantity(intent.quantity)}\n"
        f"TP:     {_price_level(intent.take_profit)}\n"
        f"SL:     {_price_level(intent.stop_loss)}\n"
        f"Leverage: {intent.leverage or 'not set'}x\n"
        f"Auto-close: {time_limit}\n"
        "──────────────────"
    )
    sid = str(session_id or "")
    markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm", callback_data=f"studio:confirm:{sid}"),
            InlineKeyboardButton("✏️ Edit", callback_data=f"studio:edit:{sid}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"studio:cancel:{sid}"),
        ],
        [InlineKeyboardButton("📊 Backtest first", callback_data=f"studio:backtest:{sid}")],
    ])
    return text, markup
