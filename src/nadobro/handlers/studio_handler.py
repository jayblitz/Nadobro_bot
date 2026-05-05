"""Telegram handlers for Strategy Studio free-text and inline callbacks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
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
_STUDIO_LIVE_MODE_KEY = "studio_live_mode"


_STUDIO_HOME_TEXT = (
    "🧠 *Strategy Studio*\n\n"
    "Describe your trade in plain English and I'll handle the rest — extraction, "
    "clarification, confirmation, and execution.\n\n"
    "*Quick examples:*\n"
    "• `long 0.05 ETH 5x with 3% TP and 2% SL`\n"
    "• `short BTC perp $100 max lev, close in 30 min`\n"
    "• `buy SOL spot $50 if price drops below 140`\n\n"
    "Just type your trade in chat and I'll show you a confirmation card before "
    "anything goes live."
)

_STUDIO_EXAMPLES_TEXT = (
    "📚 *Strategy Studio — more examples*\n\n"
    "• `scalp BTC perp $200 5x, TP 0.4% SL 0.2%, 10 minute time limit`\n"
    "• `DCA into ETH spot $50 every hour for 6 hours`\n"
    "• `open a 3x LONG on SOL when RSI(14) on 15m crosses above 30`\n"
    "• `flatten my BTC short if mark price hits 65000`\n"
    "• `volume run on ETH for $5000 total turnover, $100 per trade`\n"
    "• `mean-reversion mm grid on BTC, $200 budget, 4 levels, 5bp spread`\n\n"
    "Type any of these (or your own) in chat to start a session."
)


async def handle_studio_home(query, context: CallbackContext) -> bool:
    """Strategy Studio home card — entry point from the strategy hub button."""
    context.user_data[_STUDIO_LIVE_MODE_KEY] = True
    telegram_id = int(query.from_user.id)
    network = await run_blocking(_network, telegram_id)
    await run_blocking(conversation.abandon_active_studio_sessions, telegram_id, network)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📚 More Examples", callback_data="studio:examples")],
        [InlineKeyboardButton("◀ Back", callback_data="nav:strategy_hub")],
    ])
    await query.edit_message_text(
        _STUDIO_HOME_TEXT,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=keyboard,
    )
    return True


async def _handle_studio_examples(query, context: CallbackContext) -> bool:
    context.user_data[_STUDIO_LIVE_MODE_KEY] = True
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ Back", callback_data="studio:home")],
    ])
    await query.edit_message_text(
        _STUDIO_EXAMPLES_TEXT,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=keyboard,
    )
    return True


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
    studio_live_mode = bool(context.user_data.get(_STUDIO_LIVE_MODE_KEY))
    if raw.lower() in _CANCEL_WORDS:
        if row:
            await run_blocking(conversation.save_turn, int(row["id"]), conversation.StudioState.CANCELLED, None, conversation.load_history(row))
            await update.message.reply_text("Strategy Studio cancelled.")
            context.user_data.pop(_STUDIO_LIVE_MODE_KEY, None)
            return True
        return False
    if not studio_live_mode:
        # Persisted session without in-memory opt-in (restart, worker hop, home nav) must not hijack chat.
        await run_blocking(conversation.abandon_active_studio_sessions, telegram_id, network)
        return False
    if not row:
        row = await run_blocking(conversation.start_session, telegram_id, network, raw)
        context.user_data[_STUDIO_LIVE_MODE_KEY] = True
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
    # Menu actions don't require an active session — they are entry points from the
    # strategy hub button (studio:home) and the in-card examples link.
    if action == "home":
        return await handle_studio_home(query, context)
    if action == "examples":
        return await _handle_studio_examples(query, context)
    if not session_id:
        await query.edit_message_text("Studio session not found.")
        return True
    row = await run_blocking(conversation.get_studio_session_row, session_id)
    if not row:
        await query.edit_message_text("Studio session not found.")
        return True
    if int(row.get("telegram_id") or 0) != int(query.from_user.id):
        await query.edit_message_text("Studio session not found.")
        return True
    intent = conversation.load_intent(row)
    history = conversation.load_history(row)
    if action == "cancel":
        await run_blocking(conversation.save_turn, session_id, conversation.StudioState.CANCELLED, intent, history)
        context.user_data.pop(_STUDIO_LIVE_MODE_KEY, None)
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
        if result.get("success"):
            context.user_data.pop(_STUDIO_LIVE_MODE_KEY, None)
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
