import logging
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext
from telegram.constants import ParseMode
from src.nadobro.services.user_service import (
    get_or_create_user, get_user_nado_client, get_user_wallet_info, get_user,
    import_user_private_key, ensure_active_wallet_ready,
)
from src.nadobro.services.trade_service import (
    execute_market_order, close_position, close_all_positions,
    get_trade_history, get_trade_analytics,
)
from src.nadobro.services.alert_service import create_alert, get_user_alerts
from src.nadobro.services.admin_service import is_trading_paused
from src.nadobro.services.ai_parser import parse_user_message
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
from src.nadobro.config import get_product_id, PRODUCTS
from src.nadobro.handlers.formatters import (
    escape_md, fmt_positions, fmt_balance, fmt_prices, fmt_funding,
    fmt_trade_preview, fmt_trade_result, fmt_history, fmt_analytics,
    fmt_alerts, fmt_price,
)
from src.nadobro.handlers.keyboards import (
    main_menu_kb, trade_confirm_kb, back_kb,
    trade_leverage_kb,
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

    if await _handle_pending_question(update, context, text):
        return

    if await _handle_pending_key_import(update, context, telegram_id, text):
        return

    if await _handle_pending_trade(update, context, telegram_id, text):
        return

    if await _handle_pending_alert(update, context, telegram_id, text):
        return

    if await _handle_pending_strategy_input(update, context, telegram_id, text):
        return

    parsed = await parse_user_message(text)
    intent = parsed.get("intent", "chat")
    # region agent log
    debug_log(
        "baseline",
        "H5",
        "messages.py:84",
        "message_intent_parsed",
        {
            "telegram_id": telegram_id,
            "intent": intent,
            "action": parsed.get("action"),
            "product": parsed.get("product"),
            "has_size": parsed.get("size") is not None,
        },
    )
    # endregion

    try:
        if intent == "trade":
            await _handle_trade_intent(update, context, telegram_id, parsed)
        elif intent == "query":
            await _handle_query_intent(update, context, telegram_id, parsed)
        elif intent == "command":
            await _handle_command_intent(update, context, telegram_id, parsed)
        elif intent == "nado_question":
            await _handle_nado_question(update, context, text)
        else:
            if _should_route_to_nado_support(text):
                await _handle_nado_question(update, context, text)
            else:
                ai_msg = parsed.get("message", "I'm not sure what you mean.")
                await update.message.reply_text(
                    f"💬 {escape_md(ai_msg)}\n\n"
                    f"Use the buttons below to navigate:",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=main_menu_kb(),
                )
    except Exception as e:
        # region agent log
        debug_log(
            "baseline",
            "H5",
            "messages.py:106",
            "message_handler_exception",
            {"telegram_id": telegram_id, "error": str(e), "intent": intent},
        )
        # endregion
        logger.error(f"Message handler error: {e}", exc_info=True)
        await update.message.reply_text(
            "⚠️ Something went wrong\\. Please try again\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )


async def _handle_pending_question(update, context, text):
    if not context.user_data.get("pending_question"):
        return False

    context.user_data.pop("pending_question", None)

    await _handle_nado_question(update, context, text)
    return True


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


async def _handle_trade_intent(update, context, telegram_id, parsed):
    action = parsed.get("action", "long")
    product = parsed.get("product")
    size = parsed.get("size")
    settings = _get_user_settings(telegram_id, context)
    leverage = parsed.get("leverage", settings.get("default_leverage", 3)) or settings.get("default_leverage", 3)

    if action == "close":
        if product:
            result = close_position(telegram_id, product)
            if result["success"]:
                msg = f"✅ Closed {escape_md(str(result['cancelled']))} order\\(s\\) on {escape_md(result['product'])}\\."
            else:
                msg = f"❌ {escape_md(result['error'])}"
        else:
            result = close_all_positions(telegram_id)
            if result["success"]:
                products = ", ".join(result.get("products", []))
                msg = f"✅ Closed {escape_md(str(result['cancelled']))} order\\(s\\) on {escape_md(products)}\\."
            else:
                msg = f"❌ {escape_md(result['error'])}"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())
        return

    if action == "close_all":
        result = close_all_positions(telegram_id)
        if result["success"]:
            products = ", ".join(result.get("products", []))
            msg = f"✅ Closed {escape_md(str(result['cancelled']))} order\\(s\\) on {escape_md(products)}\\."
        else:
            msg = f"❌ {escape_md(result['error'])}"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())
        return

    if not product or not size:
        ai_msg = parsed.get("message", "Please specify product and size.")
        await update.message.reply_text(
            f"💬 {escape_md(ai_msg)}\n\nUse the buttons for easier trading:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
        return

    if is_trading_paused():
        await update.message.reply_text(
            "⏸ Trading is temporarily paused by admin\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
        return
    resume_step = get_resume_step(telegram_id)
    if resume_step != "complete":
        await update.message.reply_text(
            f"⚠️ Setup incomplete\\. Continue onboarding at *{escape_md(resume_step.upper())}*\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🧭 Continue Setup", callback_data="onboarding:resume")],
                [InlineKeyboardButton("Dashboard", callback_data="nav:main")],
            ]),
        )
        return
    wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
    if not wallet_ready:
        await update.message.reply_text(
            f"⚠️ {escape_md(wallet_msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )
        return

    parsed_price = parsed.get("price")
    price = parsed_price if parsed_price else 0
    try:
        client = get_user_nado_client(telegram_id)
        if client:
            pid = get_product_id(product)
            if pid is not None:
                if not parsed_price:
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
        "slippage_pct": settings.get("slippage", 1),
    }

    preview = fmt_trade_preview(action, product, size, price, leverage, est_margin)
    await update.message.reply_text(
        preview,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=trade_confirm_kb(),
    )


async def _handle_query_intent(update, context, telegram_id, parsed):
    action = parsed.get("action", "")
    product = parsed.get("product")

    client = get_user_nado_client(telegram_id)
    if not client:
        await update.message.reply_text(
            "⚠️ Wallet not initialized\\. Use /start first\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    if action == "positions":
        positions = client.get_all_positions()
        prices = None
        try:
            prices = client.get_all_market_prices()
        except Exception:
            pass
        msg = fmt_positions(positions, prices)
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())

    elif action == "balance":
        balance = client.get_balance()
        info = get_user_wallet_info(telegram_id)
        addr = info.get("active_address") if info else None
        msg = fmt_balance(balance, addr)
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())

    elif action == "price":
        if product:
            pid = get_product_id(product)
            if pid is not None:
                mp = client.get_market_price(pid)
                bid_str = "$" + fmt_price(mp["bid"], product)
                ask_str = "$" + fmt_price(mp["ask"], product)
                mid_str = "$" + fmt_price(mp["mid"], product)
                msg = (
                    f"💹 *{escape_md(product)}\\-PERP*\n\n"
                    f"Bid: {escape_md(bid_str)}\n"
                    f"Ask: {escape_md(ask_str)}\n"
                    f"Mid: {escape_md(mid_str)}"
                )
            else:
                msg = f"❌ Unknown product: {escape_md(product)}"
        else:
            prices = client.get_all_market_prices()
            msg = fmt_prices(prices)
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())

    elif action == "funding":
        funding = {}
        all_rates = client.get_all_funding_rates()
        for name, info in PRODUCTS.items():
            if info["type"] == "perp":
                if product and name.upper() != product.upper():
                    continue
                fr = all_rates.get(info["id"])
                if fr:
                    funding[name] = fr.get("funding_rate", 0)
        msg = fmt_funding(funding)
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())

    elif action == "history":
        trades = get_trade_history(telegram_id)
        msg = fmt_history(trades)
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())

    elif action == "analytics":
        stats = get_trade_analytics(telegram_id)
        msg = fmt_analytics(stats)
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())

    else:
        ai_msg = parsed.get("message", "")
        await update.message.reply_text(
            f"💬 {escape_md(ai_msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=main_menu_kb(),
        )


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


async def _handle_command_intent(update, context, telegram_id, parsed):
    action = parsed.get("action", "")
    product = parsed.get("product")

    if action == "alerts":
        condition = parsed.get("alert_condition")
        value = parsed.get("alert_value")

        parsed_value = None
        if value is not None:
            try:
                parsed_value = float(value)
            except (TypeError, ValueError):
                parsed_value = None

        if product and condition and parsed_value is not None:
            result = create_alert(telegram_id, product, condition, parsed_value)
            if result["success"]:
                msg = (
                    f"✅ Alert set\\!\n"
                    f"{escape_md(result['product'])} {escape_md(condition)} "
                    f"{escape_md(f'${parsed_value:,.2f}')}"
                )
            else:
                msg = f"❌ {escape_md(result['error'])}"
        else:
            alerts = get_user_alerts(telegram_id)
            msg = fmt_alerts(alerts)

        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=main_menu_kb())

    else:
        ai_msg = parsed.get("message", "I'm not sure what you mean.")
        await update.message.reply_text(
            f"💬 {escape_md(ai_msg)}",
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


def _should_route_to_nado_support(text: str) -> bool:
    txt = (text or "").strip()
    if not txt:
        return False
    lowered = txt.lower()
    if lowered in {"hi", "hello", "hey", "gm", "gn"}:
        return False
    if "?" in txt:
        return True
    support_keywords = [
        "nado", "support", "help", "docs", "api", "deposit", "withdraw",
        "liquidation", "margin", "funding", "fees", "network", "chain",
        "subaccount", "signer", "wallet", "error", "issue", "troubleshoot",
    ]
    return any(k in lowered for k in support_keywords)
