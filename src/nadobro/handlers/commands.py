import logging
import time
from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.services.user_service import (
    get_or_create_user, get_user_nado_client, get_user, get_user_wallet_info, switch_network,
)
from src.nadobro.handlers.formatters import (
    escape_md, fmt_dashboard, fmt_help, fmt_onboarding_step, fmt_status_overview,
)
from src.nadobro.handlers.keyboards import (
    main_menu_kb,
    onboarding_mode_kb,
    onboarding_key_kb,
    onboarding_funding_kb,
    onboarding_risk_kb,
    onboarding_template_kb,
    onboarding_nav_kb,
)
from src.nadobro.services.bot_runtime import get_user_bot_status, stop_user_bot
from src.nadobro.services.trade_service import close_all_positions
from src.nadobro.services.onboarding_service import (
    get_resume_step,
    get_onboarding_progress,
    evaluate_readiness,
    set_current_step,
)

logger = logging.getLogger(__name__)


async def cmd_start(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    username = update.effective_user.username

    user, is_new, _ = get_or_create_user(telegram_id, username)

    resume_step = get_resume_step(telegram_id)
    if resume_step != "complete":
        set_current_step(telegram_id, resume_step)
        await _send_onboarding_step(update, telegram_id, resume_step)
        return

    network = user.network_mode.value
    balance = None
    positions = None
    prices = None

    try:
        client = get_user_nado_client(telegram_id)
        if client:
            balance = client.get_balance()
            positions = client.get_all_positions()
            prices = client.get_all_market_prices()
    except Exception as e:
        logger.warning(f"Failed to fetch data for dashboard: {e}")

    dashboard = fmt_dashboard(user, balance, positions, prices, network)
    await update.message.reply_text(
        dashboard,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=main_menu_kb(),
    )


async def cmd_help(update: Update, context: CallbackContext):
    await update.message.reply_text(
        fmt_help(),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=main_menu_kb(),
    )


async def cmd_status(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    status = get_user_bot_status(telegram_id)
    onboarding = evaluate_readiness(telegram_id)
    text = fmt_status_overview(status, onboarding)
    if status.get("last_error"):
        text += f"\nLast error: {escape_md(str(status.get('last_error')))}"

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=main_menu_kb(),
    )


async def cmd_stop_all(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    ok, msg = stop_user_bot(telegram_id, cancel_orders=False)
    close_result = close_all_positions(telegram_id)
    closed_msg = (
        f"Closed {close_result.get('cancelled', 0)} open order\\(s\\)\\."
        if close_result.get("success")
        else f"No open orders closed \\({escape_md(close_result.get('error', 'none'))}\\)\\."
    )
    prefix = "🛑" if ok else "⚠️"
    await update.message.reply_text(
        f"{prefix} {escape_md(msg)}\n\n{closed_msg}",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=main_menu_kb(),
    )


async def cmd_import_key(update: Update, context: CallbackContext):
    telegram_id = update.effective_user.id
    user = get_user(telegram_id)
    if not user:
        await update.message.reply_text(
            "User not found\\. Run /start first\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    network = user.network_mode.value
    if context.args and context.args[0].lower() in ("testnet", "mainnet"):
        network = context.args[0].lower()
        switch_network(telegram_id, network)

    context.user_data["pending_key_import"] = {
        "network": network,
        "started_at": time.time(),
    }
    set_current_step(telegram_id, "key")
    await update.message.reply_text(
        f"🔑 *Import Dedicated Trading Key* \\({escape_md(network.upper())}\\)\n\n"
        "Send your *private key* now \\(64 hex chars, with or without `0x`\\)\\.\n\n"
        "⚠️ Never send a seed phrase\\. Never send your main wallet key\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=main_menu_kb(),
    )


async def _send_onboarding_step(update: Update, telegram_id: int, step: str):
    progress = get_onboarding_progress(telegram_id)
    readiness = evaluate_readiness(telegram_id)
    progress_text = f"{progress.get('done', 0)}/{progress.get('total', 0)}"
    text = fmt_onboarding_step(
        step,
        readiness.get("network", "testnet"),
        readiness,
        {"progress": progress_text, "selected_template": progress.get("state", {}).get("selected_template")},
    )
    network = readiness.get("network", "testnet")
    if step == "mode":
        kb = onboarding_mode_kb()
    elif step == "key":
        kb = onboarding_key_kb(network)
    elif step == "funding":
        kb = onboarding_funding_kb(network)
    elif step == "risk":
        kb = onboarding_risk_kb()
    elif step == "template":
        kb = onboarding_template_kb()
    else:
        kb = onboarding_nav_kb(step, allow_skip=False, allow_back=False)

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=kb,
    )
