import logging
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.services.user_service import (
    get_or_create_user, get_user_nado_client, get_user_wallet_info, get_user,
    ensure_active_wallet_ready,
)
from src.nadobro.services.trade_service import execute_market_order
from src.nadobro.services.alert_service import create_alert
from src.nadobro.services.knowledge_service import answer_nado_question
from src.nadobro.services.settings_service import get_user_settings, update_user_settings
from src.nadobro.services.onboarding_service import get_resume_step
from src.nadobro.services.debug_logger import debug_log
from src.nadobro.services.crypto import (
    is_probable_mnemonic,
    normalize_private_key,
    derive_address_from_private_key,
    private_key_fingerprint,
)
from src.nadobro.config import get_product_id
from src.nadobro.handlers.formatters import (
    escape_md, fmt_positions, fmt_trade_preview,
)
from src.nadobro.handlers.keyboards import (
    main_menu_kb, persistent_menu_kb, trade_confirm_kb, REPLY_BUTTON_MAP,
)

logger = logging.getLogger(__name__)


async def handle_message(update: Update, context: CallbackContext):
    if not update.message or not update.message.text:
        return

    telegram_id = update.effective_user.id
    username = update.effective_user.username
    text = update.message.text.strip()

    get_or_create_user(telegram_id, username)
    # region agent log
    debug_log(
        "baseline",
        "H5",
        "messages.py:51",
        "message_received",
        {
            "telegram_id": telegram_id,
            "text_len": len(text),
            "has_pending_trade": bool(context.user_data.get("pending_trade")),
            "has_pending_alert": bool(context.user_data.get("pending_alert")),
            "has_pending_question": bool(context.user_data.get("pending_question")),
        },
    )
    # endregion

    if text in REPLY_BUTTON_MAP:
        callback_data = REPLY_BUTTON_MAP[text]
        await _dispatch_reply_button(update, context, telegram_id, callback_data)
        return

    if await _handle_pending_key_import(update, context, telegram_id, text):
        return

    if await _handle_pending_trade(update, context, telegram_id, text):
        return

    if await _handle_pending_alert(update, context, telegram_id, text):
        return

    if await _handle_pending_strategy_input(update, context, telegram_id, text):
        return

    await _handle_nado_question(update, context, text)


async def _dispatch_reply_button(update, context, telegram_id, callback_data):
    from src.nadobro.handlers.callbacks import handle_callback as _cb_handler
    from src.nadobro.services.user_service import get_user
    from src.nadobro.services.onboarding_service import get_resume_step, evaluate_readiness

    prefix = callback_data.split(":")[0]
    action = callback_data.split(":")[1] if ":" in callback_data else ""

    if callback_data == "onboarding:resume":
        step = get_resume_step(telegram_id)
        if step == "complete":
            await _send_dashboard_msg(update, telegram_id)
        else:
            from src.nadobro.services.onboarding_service import set_current_step
            set_current_step(telegram_id, step)
            from src.nadobro.handlers.commands import _send_onboarding_step
            await _send_onboarding_step(update, telegram_id, step)
        return

    if callback_data == "nav:help":
        from src.nadobro.handlers.formatters import fmt_help
        await update.message.reply_text(
            fmt_help(),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if callback_data == "nav:strategy_hub":
        from src.nadobro.handlers.keyboards import strategy_hub_kb
        await update.message.reply_text(
            "🧭 *Strategy Hub*\n\n"
            "Pick a strategy, review setup, then start with pre\\-trade analytics\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_hub_kb(),
        )
        return

    if prefix == "trade":
        step = get_resume_step(telegram_id)
        if step != "complete":
            await update.message.reply_text(
                f"⚠️ Setup incomplete\\. Resume onboarding at *{escape_md(step.upper())}*\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🧭 Continue Setup", callback_data="onboarding:resume")],
                ]),
            )
            return
        from src.nadobro.services.user_service import ensure_active_wallet_ready
        wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
        if not wallet_ready:
            await update.message.reply_text(
                f"⚠️ {escape_md(wallet_msg)}",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return

        if action in ("long", "short"):
            from src.nadobro.handlers.keyboards import trade_product_kb
            action_label = "🟢 BUY / LONG" if action == "long" else "🔴 SELL / SHORT"
            await update.message.reply_text(
                f"*{escape_md(action_label)}*\n\nSelect a product:",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=trade_product_kb(action),
            )
        elif action in ("limit_long", "limit_short"):
            from src.nadobro.handlers.keyboards import trade_product_kb
            context.user_data["pending_trade"] = {"action": action, "step": "product_select"}
            action_label = "LIMIT LONG" if action == "limit_long" else "LIMIT SHORT"
            await update.message.reply_text(
                f"*{escape_md(action_label)}*\n\nSelect a product:",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=trade_product_kb(action),
            )
        return

    if callback_data == "wallet:view":
        info = get_user_wallet_info(telegram_id)
        from src.nadobro.handlers.formatters import fmt_wallet_info
        from src.nadobro.handlers.keyboards import wallet_kb
        msg = fmt_wallet_info(info)
        await update.message.reply_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=wallet_kb(),
        )
        return

    if callback_data == "pos:view":
        client = get_user_nado_client(telegram_id)
        if not client:
            await update.message.reply_text(
                "⚠️ Wallet not initialized\\. Use /start first\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return
        positions = client.get_all_positions()
        prices = None
        try:
            prices = client.get_all_market_prices()
        except Exception:
            pass
        from src.nadobro.handlers.keyboards import positions_kb
        msg = fmt_positions(positions, prices)
        await update.message.reply_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=positions_kb(positions or []),
        )
        return

    if callback_data == "strategy:status":
        from src.nadobro.services.bot_runtime import get_user_bot_status
        from src.nadobro.handlers.formatters import fmt_status_overview
        st = get_user_bot_status(telegram_id)
        readiness = evaluate_readiness(telegram_id)
        text = fmt_status_overview(st, readiness)
        if st.get("last_error"):
            text += f"\nLast error: {escape_md(str(st.get('last_error')))}"
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
        return

    if callback_data == "strategy:stop":
        from src.nadobro.services.bot_runtime import stop_user_bot
        ok, msg = stop_user_bot(telegram_id, cancel_orders=True)
        prefix_emoji = "🛑" if ok else "⚠️"
        await update.message.reply_text(
            f"{prefix_emoji} {escape_md(msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if callback_data == "mkt:menu":
        from src.nadobro.handlers.keyboards import markets_kb
        await update.message.reply_text(
            "💹 *Markets*\n\nPick a market view:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=markets_kb(),
        )
        return

    if callback_data == "alert:menu":
        from src.nadobro.handlers.keyboards import alerts_kb
        await update.message.reply_text(
            "🔔 *Alerts*\n\nManage your price alerts\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=alerts_kb(),
        )
        return

    if callback_data == "settings:view":
        from src.nadobro.handlers.formatters import fmt_settings
        from src.nadobro.handlers.keyboards import settings_kb
        user_settings = _get_user_settings(telegram_id, context)
        msg = fmt_settings(user_settings)
        lev = user_settings.get("default_leverage", 1)
        slip = user_settings.get("slippage", 1)
        await update.message.reply_text(
            msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=settings_kb(lev, slip),
        )
        return

    await update.message.reply_text(
        f"Use the inline buttons or type a question for AI chat\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )


async def _send_dashboard_msg(update, telegram_id):
    from src.nadobro.services.user_service import get_user
    from src.nadobro.handlers.formatters import fmt_dashboard
    from src.nadobro.services.onboarding_service import evaluate_readiness
    user = get_user(telegram_id)
    if not user:
        await update.message.reply_text("User not found\\. Use /start first\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return
    network = user.network_mode.value
    balance = positions = prices = None
    try:
        client = get_user_nado_client(telegram_id)
        if client:
            balance = client.get_balance()
            positions = client.get_all_positions()
            prices = client.get_all_market_prices()
    except Exception:
        pass
    dashboard = fmt_dashboard(user, balance, positions, prices, network)
    readiness = evaluate_readiness(telegram_id)
    if readiness.get("onboarding_complete"):
        dashboard += "\n\n✅ *Setup:* Complete"
    else:
        next_step = readiness.get("missing_step", "welcome")
        dashboard += (
            f"\n\n⚠️ *Setup:* Incomplete\n"
            f"Next step: *{escape_md(str(next_step).upper())}*"
        )
    await update.message.reply_text(
        dashboard,
        parse_mode=ParseMode.MARKDOWN_V2,
    )


async def _handle_pending_trade(update, context, telegram_id, text):
    pending = context.user_data.get("pending_trade")
    if not pending:
        return False

    step = pending.get("step", "")
    # region agent log
    debug_log(
        "baseline",
        "H2",
        "messages.py:138",
        "pending_trade_message_step",
        {
            "telegram_id": telegram_id,
            "step": step,
            "action": pending.get("action"),
            "product": pending.get("product"),
        },
    )
    # endregion

    if step == "custom_size":
        try:
            parts = text.split()
            size = float(parts[0])
            leverage = _get_user_settings(telegram_id, context).get("default_leverage", 3)
            if len(parts) >= 2:
                lev_str = parts[1].replace("x", "").replace("X", "")
                leverage = int(float(lev_str))
        except (ValueError, IndexError):
            await update.message.reply_text(
                "⚠️ Invalid size\\. Enter a number \\(e\\.g\\. `0\\.01`\\)\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return True

        action = pending["action"]
        product = pending["product"]

        price = 0
        try:
            client = get_user_nado_client(telegram_id)
            if client:
                pid = get_product_id(product)
                if pid is not None:
                    mp = client.get_market_price(pid)
                    price = mp.get("mid", 0)
        except Exception:
            pass

        est_margin = (size * price) / leverage if leverage > 0 and price else None

        context.user_data["pending_trade"] = {
            "action": action,
            "product": product,
            "size": size,
            "leverage": leverage,
            "price": price,
            "est_margin": est_margin,
            "slippage_pct": _get_user_settings(telegram_id, context).get("slippage", 1),
        }

        preview = fmt_trade_preview(action, product, size, price, leverage, est_margin)
        await update.message.reply_text(
            preview,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_confirm_kb(),
        )
        return True

    if step == "limit_input":
        try:
            parts = text.split()
            size = float(parts[0])
            price = float(parts[1])
        except (ValueError, IndexError):
            await update.message.reply_text(
                "⚠️ Enter size and price \\(e\\.g\\. `0\\.01 95000`\\)\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return True

        action = pending["action"]
        product = pending["product"]

        context.user_data["pending_trade"] = {
            "action": action,
            "product": product,
            "size": size,
            "leverage": _get_user_settings(telegram_id, context).get("default_leverage", 3),
            "price": price,
            "slippage_pct": _get_user_settings(telegram_id, context).get("slippage", 1),
        }

        preview = fmt_trade_preview(
            action,
            product,
            size,
            price,
            _get_user_settings(telegram_id, context).get("default_leverage", 3),
        )
        await update.message.reply_text(
            preview,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=trade_confirm_kb(),
        )
        return True

    return False


async def _handle_pending_key_import(update, context, telegram_id, text):
    pending_confirm = context.user_data.get("pending_key_confirm")
    if pending_confirm:
        await _delete_user_message(update)
        await update.message.reply_text(
            "⚠️ You already have a key import awaiting confirmation\\. "
            "Use the *Confirm Import* or *Cancel* button\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Confirm Import", callback_data="keyimp:confirm")],
                [InlineKeyboardButton("❌ Cancel", callback_data="keyimp:cancel")],
            ]),
        )
        return True

    pending = context.user_data.get("pending_key_import")
    if not pending:
        return False

    started_at = float(pending.get("started_at") or 0)
    if started_at and time.time() - started_at > 300:
        await _delete_user_message(update)
        context.user_data.pop("pending_key_import", None)
        await update.message.reply_text(
            "⌛ Key import session expired\\. Run /import\\_key again\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
        return True

    if is_probable_mnemonic(text):
        await _delete_user_message(update)
        await update.message.reply_text(
            "🛑 This looks like a seed phrase\\. Nadobro accepts *private key only* for dedicated trading wallets\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
        context.user_data.pop("pending_key_import", None)
        return True

    network = pending.get("network", "testnet")
    try:
        normalized = normalize_private_key(text.strip())
        address = derive_address_from_private_key(normalized)
        fingerprint = private_key_fingerprint(normalized)
    except Exception as e:
        await _delete_user_message(update)
        await update.message.reply_text(
            f"❌ {escape_md(str(e))}\n\nTry again with a dedicated private key for this mode\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
        return True

    context.user_data["pending_key_confirm"] = {
        "network": network,
        "private_key": normalized,
        "address": address,
        "fingerprint": fingerprint,
        "started_at": time.time(),
    }
    await _delete_user_message(update)
    context.user_data.pop("pending_key_import", None)
    await update.message.reply_text(
        f"🔐 *Confirm Key Import* \\({escape_md(network.upper())}\\)\n\n"
        f"Address: `{escape_md(address)}`\n"
        f"Fingerprint: `fp\\-{escape_md(fingerprint)}`\n\n"
        "⚠️ Ensure this is your *dedicated trading key* \\(not your main wallet\\) before confirming\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm Import", callback_data="keyimp:confirm")],
            [InlineKeyboardButton("❌ Cancel", callback_data="keyimp:cancel")],
        ]),
    )
    return True


async def _delete_user_message(update: Update):
    try:
        if update.message:
            await update.message.delete()
    except Exception:
        pass


async def _handle_pending_alert(update, context, telegram_id, text):
    pending = context.user_data.get("pending_alert")
    if not pending:
        return False

    product = pending["product"]
    context.user_data.pop("pending_alert", None)

    try:
        parts = text.lower().split()
        condition = parts[0]
        target = float(parts[1])
    except (ValueError, IndexError):
        await update.message.reply_text(
            "⚠️ Invalid format\\. Use: `above 100000` or `below 90000`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return True

    if condition not in ("above", "below"):
        await update.message.reply_text(
            "⚠️ Invalid condition\\. Use: above, below",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return True

    result = create_alert(telegram_id, product, condition, target)
    if result["success"]:
        await update.message.reply_text(
            f"✅ Alert set\\!\n"
            f"{escape_md(result['product'])} {escape_md(condition)} "
            f"{escape_md(f'${target:,.2f}')}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
    else:
        await update.message.reply_text(
            f"❌ {escape_md(result['error'])}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
    return True


async def _handle_pending_strategy_input(update, context, telegram_id, text):
    pending = context.user_data.get("pending_strategy_input")
    if not pending:
        return False

    strategy = pending.get("strategy")
    field = pending.get("field")
    if strategy not in ("mm", "grid", "dn") or field not in ("notional_usd", "spread_bp", "interval_seconds", "tp_pct", "sl_pct"):
        context.user_data.pop("pending_strategy_input", None)
        return False

    try:
        value = float(text.strip())
    except (TypeError, ValueError):
        await update.message.reply_text(
            "⚠️ Invalid value\\. Please enter a number\\. Example: `1\\.2`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return True

    limits = {
        "notional_usd": (1, 1000000),
        "spread_bp": (0.1, 200),
        "interval_seconds": (10, 3600),
        "tp_pct": (0.05, 100),
        "sl_pct": (0.05, 100),
    }
    lo, hi = limits[field]
    if value < lo or value > hi:
        await update.message.reply_text(
            f"⚠️ Value out of range\\. Allowed: {escape_md(str(lo))} to {escape_md(str(hi))}",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return True

    def _mutate(s):
        strategies = s.setdefault("strategies", {})
        cfg = strategies.setdefault(strategy, {})
        if field == "interval_seconds":
            cfg[field] = int(value)
        else:
            cfg[field] = value

    network, settings = update_user_settings(telegram_id, _mutate)
    conf = settings.get("strategies", {}).get(strategy, {})
    context.user_data.pop("pending_strategy_input", None)
    await update.message.reply_text(
        _fmt_strategy_update(strategy, network, conf),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⚙️ Continue Editing", callback_data=f"strategy:config:{strategy}")],
            [InlineKeyboardButton("🧭 Strategy Hub", callback_data="nav:strategy_hub")],
        ]),
    )
    return True


async def _handle_nado_question(update, context, question):
    thinking_msg = await update.message.reply_text(
        "🧠 _Thinking\\.\\.\\._",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    try:
        answer = await answer_nado_question(question)
        await thinking_msg.edit_text(
            f"🧠 *Ask Nado*\n\n{escape_md(answer)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
    except Exception as e:
        logger.error(f"Nado Q&A error: {e}", exc_info=True)
        await thinking_msg.edit_text(
            "⚠️ Something went wrong answering your question\\. Please try again\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )


def _get_user_settings(telegram_id: int, context: CallbackContext) -> dict:
    network, settings = get_user_settings(telegram_id)
    context.user_data[f"settings:{network}"] = settings
    context.user_data["settings"] = settings
    return settings


def _fmt_strategy_update(strategy: str, network: str, conf: dict) -> str:
    notional = float(conf.get("notional_usd", 100.0))
    spread_bp = float(conf.get("spread_bp", 5.0))
    interval_seconds = int(conf.get("interval_seconds", 60))
    tp_pct = float(conf.get("tp_pct", 1.0))
    sl_pct = float(conf.get("sl_pct", 0.5))
    return (
        f"✅ *{escape_md(strategy.upper())} updated* \\({escape_md(network.upper())}\\)\n\n"
        f"Notional: {escape_md(f'${notional:,.2f}')}\n"
        f"Spread: {escape_md(f'{spread_bp:.1f} bp')}\n"
        f"Interval: {escape_md(f'{interval_seconds}s')}\n"
        f"TP: {escape_md(f'{tp_pct:.2f}%')}\n"
        f"SL: {escape_md(f'{sl_pct:.2f}%')}"
    )


