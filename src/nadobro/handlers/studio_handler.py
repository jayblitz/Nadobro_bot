"""Telegram handlers for Strategy Studio free-text and inline callbacks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from telegram import Update
from telegram.ext import CallbackContext

from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.feature_flags import studio_enabled
from src.nadobro.services.user_service import get_user, get_user_nado_client
from src.nadobro.studio import conversation
from src.nadobro.studio.backtest import backtest
from src.nadobro.studio.candles import fetch_candles
from src.nadobro.studio.clarifier import next_question
from src.nadobro.studio.confirmation import build_confirmation_card
from src.nadobro.studio.execution_bridge import execute_intent
from src.nadobro.studio.extractor import extract

_CANCEL_WORDS = {"cancel", "nevermind", "never mind", "stop"}


def _network(telegram_id: int) -> str:
    user = get_user(telegram_id)
    return user.network_mode.value if user else "mainnet"


async def handle_studio_text(update: Update, context: CallbackContext) -> bool:
    if not studio_enabled() or not update.message or not update.effective_user:
        return False
    telegram_id = update.effective_user.id
    network = await run_blocking(_network, telegram_id)
    raw = (update.message.text or "").strip()
    row = await run_blocking(conversation.active_session, telegram_id, network)
    if raw.lower() in _CANCEL_WORDS:
        if row:
            await run_blocking(conversation.save_turn, int(row["id"]), conversation.StudioState.CANCELLED, None, conversation.load_history(row))
            await update.message.reply_text("Strategy Studio cancelled.")
            return True
        return False
    if not row:
        row = await run_blocking(conversation.start_session, telegram_id, network, raw)
    history = conversation.load_history(row)
    prior = conversation.load_intent(row) if row.get("intent_json") else None
    try:
        intent = await extract(raw, prior, history)
        if intent.network is None:
            intent.network = network
    except Exception as e:
        await update.message.reply_text(f"I couldn't parse that strategy yet. Raw error: `{str(e)[:300]}`")
        return True
    history.append({"role": "user", "content": raw})
    question = next_question(intent)
    if question:
        history.append({"role": "assistant", "content": question})
        await run_blocking(conversation.save_turn, int(row["id"]), conversation.StudioState.CLARIFYING, intent, history)
        await update.message.reply_text(question)
        return True
    await run_blocking(conversation.save_turn, int(row["id"]), conversation.StudioState.CONFIRMING, intent, history)
    text, markup = build_confirmation_card(intent, int(row["id"]))
    await update.message.reply_text(text, reply_markup=markup)
    return True


async def handle_studio_callback(query, context: CallbackContext) -> bool:
    data = query.data or ""
    if not data.startswith("studio:"):
        return False
    await query.answer()
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    session_id = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
    if not session_id:
        await query.edit_message_text("Studio session not found.")
        return True
    row = await run_blocking(conversation.get_studio_session_row, session_id)
    if not row:
        await query.edit_message_text("Studio session not found.")
        return True
    intent = conversation.load_intent(row)
    history = conversation.load_history(row)
    if action == "cancel":
        await run_blocking(conversation.save_turn, session_id, conversation.StudioState.CANCELLED, intent, history)
        await query.edit_message_text("Strategy Studio cancelled.")
        return True
    if action == "edit":
        await run_blocking(conversation.save_turn, session_id, conversation.StudioState.CLARIFYING, intent, history)
        await query.edit_message_text("What would you like to change? Reply in chat, for example: `change leverage to 5x`.")
        return True
    if action == "backtest" and intent:
        report_text = await _run_backtest(row, intent)
        text, markup = build_confirmation_card(intent, session_id)
        await query.edit_message_text(f"{report_text}\n\n{text}", reply_markup=markup)
        return True
    if action == "confirm" and intent:
        await run_blocking(conversation.save_turn, session_id, conversation.StudioState.EXECUTING, intent, history)
        result = await run_blocking(execute_intent, int(row["telegram_id"]), intent, session_id)
        await run_blocking(
            conversation.save_turn,
            session_id,
            conversation.StudioState.DONE if result.get("success") else conversation.StudioState.CLARIFYING,
            intent,
            history,
            result.get("strategy_session_id"),
        )
        await query.edit_message_text(f"Strategy execution result:\n`{result}`")
        return True
    await query.edit_message_text("Studio action could not be completed.")
    return True


async def _run_backtest(row: dict, intent) -> str:
    user_id = int(row["telegram_id"])
    network = intent.network or row.get("network") or "mainnet"
    client = await run_blocking(get_user_nado_client, user_id, network)
    if not client or not intent.symbol:
        return "📊 Backtest unavailable: wallet or symbol not ready."
    timeframe = intent.conditions[0].timeframe if intent.conditions else "1h"
    candles = await run_blocking(fetch_candles, client, intent.symbol, timeframe, 500, network)
    report = backtest(intent, candles, datetime.now(timezone.utc) - timedelta(days=30), datetime.now(timezone.utc))
    return (
        "📊 Backtest first\n"
        f"PnL: ${report.pnl_usd:+.2f}\n"
        f"Drawdown: {report.max_drawdown_pct:.2f}%\n"
        f"Hit rate: {report.hit_rate_pct:.2f}%\n"
        f"Trades: {report.trades}\n"
        f"Equity: {report.ascii_curve()}"
    )
