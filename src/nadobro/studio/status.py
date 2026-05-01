"""Status-card rendering for active Strategy Studio sessions."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from src.nadobro.models.database import get_active_conditional_orders_for_user, get_active_studio_sessions_for_user


def _decode(value, default):
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _age(ts) -> str:
    try:
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        seconds = max(0, int((datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds()))
        hours, rem = divmod(seconds, 3600)
        minutes = rem // 60
        return f"{hours}h {minutes}m" if hours else f"{minutes}m"
    except Exception:
        return "unknown"


def build_status_cards(telegram_id: int, network: str | None = None, page: int = 0, page_size: int = 3) -> tuple[str, InlineKeyboardMarkup | None]:
    sessions = get_active_studio_sessions_for_user(telegram_id, network)
    conditionals = get_active_conditional_orders_for_user(telegram_id, network)
    cards = []
    for row in sessions:
        intent = _decode(row.get("intent_json"), {})
        if not intent:
            continue
        cards.append(_session_card(row, intent))
    for row in conditionals:
        cards.append(_conditional_card(row))
    if not cards:
        return "No active strategies. Send a message describing a trade to start one.", None
    start = max(0, int(page)) * page_size
    visible = cards[start : start + page_size]
    text = "\n\n────────────\n\n".join(visible)
    rows = []
    if start > 0 or start + page_size < len(cards):
        rows.append([
            InlineKeyboardButton("‹ Prev", callback_data=f"status:studio:{max(0, page - 1)}"),
            InlineKeyboardButton("Next ›", callback_data=f"status:studio:{page + 1}"),
        ])
    return text[:3900], InlineKeyboardMarkup(rows) if rows else None


def _session_card(row: dict, intent: dict) -> str:
    sid = str(row.get("id"))[-6:]
    network = row.get("network", "?")
    state = str(row.get("state", "?")).lower()
    symbol = intent.get("symbol") or "?"
    action = intent.get("action") or "?"
    order_type = intent.get("order_type") or "?"
    qty = intent.get("quantity") or {}
    time_limit = intent.get("time_limit") or "no auto-close set"
    tp = intent.get("take_profit") or {}
    sl = intent.get("stop_loss") or {}
    return (
        f"Strategy `{sid}` | {network} | {state} | opened {_age(row.get('created_at'))} ago\n"
        f"{action.upper()} {symbol} | {order_type} | leverage {intent.get('leverage') or '?'}x\n"
        f"Volume: {qty.get('value', '?')} {qty.get('type', '')}\n"
        f"Direction: {'↑ long' if action == 'buy' else '↓ short' if action == 'sell' else action}\n"
        f"Auto-close: {time_limit}\n"
        f"TP / SL: {tp.get('value', 'not set')} / {sl.get('value', 'not set')}\n"
        "RnL: pending live mark/funding refresh\n"
        "Last action: awaiting next Studio update"
    )


def _conditional_card(row: dict) -> str:
    intent = _decode(row.get("intent_json"), {})
    conditions = _decode(row.get("conditions_json"), [])
    trigger = "; ".join(
        f"{c.get('indicator')}({c.get('period') or ''}) {c.get('timeframe')} {c.get('operator')} {c.get('value')}"
        for c in conditions
    ) or "condition not set"
    return (
        f"Strategy `{str(row.get('id'))[-6:]}` | {row.get('network')} | armed | armed for {_age(row.get('created_at'))}\n"
        f"{(intent.get('action') or '?').upper()} {row.get('symbol')} | conditional\n"
        f"Trigger: {trigger}\n"
        f"Auto-close: {row.get('time_limit') or 'no auto-close set'}\n"
        f"Last action: {row.get('last_evaluation') or 'not evaluated yet'}"
    )
