import logging
import re
from typing import Optional
from src.nadobro.config import get_product_name, PRODUCTS
from src.nadobro.i18n import get_active_language, localize_text

logger = logging.getLogger(__name__)


def _loc(text):
    return localize_text(text, get_active_language())


def escape_md(text):
    if text is None:
        return ""
    text = str(text)
    text = text.replace('\\', '\\\\')
    special = r'_*[]()~`>#+-=|{}.!'
    return re.sub(r'([' + re.escape(special) + r'])', r'\\\1', text)


def _calc_position_pnl(position: dict, current_price: float) -> Optional[float]:
    def _inventory_pnl() -> Optional[float]:
        v_quote = position.get("v_quote_balance")
        signed_amount = position.get("signed_amount")
        if v_quote is None or signed_amount is None or not current_price:
            return None
        try:
            return float(v_quote) + float(signed_amount) * float(current_price)
        except Exception:
            return None

    def _directional_pnl() -> Optional[float]:
        entry = float(position.get("price", 0) or 0)
        amount = abs(float(position.get("amount", 0) or 0))
        if not current_price or not entry or not amount:
            return None
        side = str(position.get("side", "LONG")).upper()
        if side == "LONG":
            return (float(current_price) - entry) * amount
        return (entry - float(current_price)) * amount

    inventory = _inventory_pnl()
    directional = _directional_pnl()

    if inventory is None:
        return directional
    if directional is None:
        return inventory

    diff = abs(inventory - directional)
    scale = max(abs(inventory), abs(directional), 1.0)
    # When formulas diverge materially, directional (entry/side based) is
    # typically closer to platform UI expectations for a single position row.
    if diff > max(25.0, scale * 0.35):
        logger.warning(
            "PnL formula divergence for %s: side=%s entry=%.8f mark=%.8f inv=%.4f dir=%.4f",
            position.get("product_name", "unknown"),
            str(position.get("side", "LONG")).upper(),
            float(position.get("price", 0) or 0),
            float(current_price or 0),
            inventory,
            directional,
        )
        return directional

    return inventory


def fmt_price(price, product="BTC"):
    if price is None or price == 0:
        return "N/A"
    product_upper = str(product).upper().replace("-PERP", "")
    if product_upper in ("BTC", "ETH", "BNB"):
        return f"{price:,.2f}"
    if price >= 1:
        return f"{price:,.2f}"
    return f"{price:,.4f}"



def fmt_positions(positions, prices=None):
    if not positions:
        return _loc("📋 *Open Orders*") + "\n\n" + _loc("No open orders found\\.")

    lines = [
        _loc("📋 *Open Orders*"),
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
    ]

    for i, p in enumerate(positions, 1):
        side = p.get("side", "LONG")
        side_emoji = "🟢" if side == "LONG" else "🔴"
        amount = abs(p.get("amount", 0))
        pname = p.get("product_name", "???")
        entry = p.get("price", 0)
        base = pname.replace("-PERP", "")

        lines.append(
            f"{side_emoji} *{escape_md(side)}* {escape_md(f'{amount:.4f}')} "
            f"{escape_md(pname)} @ {escape_md(f'${entry:,.2f}')}"
        )

        current = 0
        if prices and base in prices:
            current = prices[base].get("mid", 0)

        if current:
            pnl = _calc_position_pnl(p, current)
            if pnl is not None:
                pnl_str = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"
                pnl_emoji = "🟢" if pnl >= 0 else "🔴"
                mark_str = f"${fmt_price(current, base)}"
                lines.append(
                    f"  └ {_loc('Mark')}: {escape_md(mark_str)} \\| "
                    f"{_loc('PnL')}: {pnl_emoji} {escape_md(pnl_str)}"
                )

    lines.append("")
    lines.append(f"{_loc('Total')}: {escape_md(str(len(positions)))} {_loc('order(s)')}")
    return "\n".join(lines)


def fmt_balance(balance_data, wallet_addr=None):
    lines = [
        _loc("💰 *Wallet Vault Balance*"),
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
    ]

    if not balance_data or not balance_data.get("exists"):
        lines.append(_loc("⚠️ No subaccount found\\."))
        lines.append("")
        if wallet_addr:
            lines.append(_loc("Deposit ≥ \\$5 USDT0 to:"))
            lines.append(f"`{escape_md(wallet_addr)}`")
            lines.append("")
            lines.append(f"🚰 {_loc('Faucet')}: {escape_md('https://testnet.nado.xyz/portfolio/faucet')}")
        return "\n".join(lines)

    balances = balance_data.get("balances", {}) or {}
    usdt = balances.get(0, balances.get("0", 0))
    lines.append(f"💵 *USDT0:* {escape_md(f'${usdt:,.2f}')}")

    for pid_raw, bal in balances.items():
        try:
            pid = int(pid_raw)
        except (TypeError, ValueError):
            continue
        if pid != 0 and bal != 0:
            pname = get_product_name(pid)
            lines.append(f"  └ {escape_md(pname)}: {escape_md(f'{bal:.6f}')}")

    if wallet_addr:
        lines.append("")
        lines.append(f"📋 {_loc('Address')}: `{escape_md(wallet_addr)}`")

    return "\n".join(lines)


def fmt_trade_preview(action, product, size, price, leverage=1, est_margin=None):
    action_upper = action.upper()
    emoji = "🟢" if "LONG" in action_upper else "🔴"

    if est_margin is None and price:
        est_margin = (size * price) / leverage if leverage > 1 else size * price

    lines = [
        f"{emoji} *{_loc('Trade Preview')}*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"📌 *{_loc('Action')}:* {escape_md(action_upper)}",
        f"🪙 *{_loc('Product')}:* {escape_md(product)}\\-PERP",
        f"📏 *{_loc('Size')}:* {escape_md(str(size))}",
        f"💲 *{_loc('Price')}:* {escape_md(f'~${fmt_price(price, product)}')}",
        f"⚡ *{_loc('Leverage')}:* {escape_md(f'{leverage}x')}",
    ]

    if est_margin is not None:
        lines.append(f"💰 *{_loc('Est. Margin')}:* {escape_md(f'${est_margin:,.2f}')}")

    lines.append("")
    lines.append(_loc("Confirm to execute this trade\\."))

    return "\n".join(lines)


def fmt_trade_result(result):
    if result.get("success"):
        r_price = result.get("price", 0)
        r_product = result.get("product", "BTC")
        price_str = "$" + fmt_price(r_price, r_product)
        lines = [
            _loc("✅ *Trade Executed\\!*"),
            escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
            "",
            f"📌 *{_loc('Side')}:* {escape_md(result.get('side', '?'))}",
            f"🪙 *{_loc('Product')}:* {escape_md(result.get('product', '?'))}",
            f"📏 *{_loc('Size')}:* {escape_md(str(result.get('size', '?')))}",
            f"💲 *{_loc('Price')}:* {escape_md(price_str)}",
            "",
            f"🌐 *{_loc('Network:')}* {escape_md(result.get('network', '?'))}",
        ]
        if result.get("tp_requested"):
            if result.get("tp_set"):
                lines.append(f"📈 *{_loc('Take Profit')}:* {escape_md(str(result.get('tp_price')))}")
            else:
                lines.append(f"⚠️ *{_loc('Take Profit')}:* {escape_md(str(result.get('tp_error', _loc('Failed to place TP order.'))))}")
        if result.get("sl_requested"):
            if result.get("sl_armed"):
                lines.append(f"🛡 *{_loc('Stop Loss')}:* {escape_md(str(result.get('sl_price')))}")
            else:
                lines.append(f"⚠️ *{_loc('Stop Loss')}:* {escape_md(str(result.get('sl_error', _loc('Failed to arm SL rule.'))))}")
        order_type = result.get("type", "MARKET")
        if order_type != "MARKET":
            lines.insert(3, f"📋 *{_loc('Type')}:* {escape_md(order_type)}")
        return "\n".join(lines)
    else:
        error = result.get("error", _loc("Unknown error"))
        return f"❌ *{_loc('Trade Failed')}*\n\n{escape_md(error)}"


def fmt_wallet_info(wallet_info):
    if not wallet_info:
        return _loc("💼 *Wallet Vault*") + "\n\n" + _loc("Wallet not found\\. Use /start first\\.")

    net = wallet_info.get("network", "testnet")
    net_emoji = "🧪" if net == "testnet" else "🌐"
    signer_linked = bool(wallet_info.get("linked_signer_address"))

    lines = [
        _loc("💼 *Wallet Vault*"),
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"📊 *{_loc('Network:')}* {net_emoji} {escape_md(net.upper())}",
    ]

    addr = wallet_info.get("active_address")
    if addr:
        lines.append("")
        lines.append(f"📋 *{_loc('Main Wallet:')}*")
        lines.append(f"`{escape_md(addr)}`")
    else:
        lines.append("")
        lines.append(f"📋 *{_loc('Main Wallet:')}* {_loc('Not set')}")

    if signer_linked:
        lines.append("")
        lines.append(f"🔐 *{_loc('1CT Address')}:*")
        lines.append(f"`{escape_md(wallet_info['linked_signer_address'])}`")

    verification = wallet_info.get("signer_verification")
    if verification:
        lines.append("")
        if verification.get("error"):
            lines.append(f"⚠️ *{_loc('Signer Check')}:* {escape_md(_loc('Could not verify') + ' — ' + str(verification['error'])[:60])}")
        elif verification.get("verified"):
            lines.append(f"✅ *{_loc('Signer Check')}:* {_loc('1CT key is linked on Nado')}")
        elif verification.get("current_signer"):
            current = verification["current_signer"]
            expected = verification.get("expected_signer", "")
            lines.append(f"❌ *{_loc('Signer Check')}:* {_loc('MISMATCH')}")
            lines.append(f"  {_loc('Exchange has')}: `{escape_md(current[:10])}\\.\\.\\.$`")
            lines.append(f"  {_loc('Bot expects')}: `{escape_md(expected[:10])}\\.\\.\\.$`")
            lines.append(escape_md(_loc("→ Disable 1-Click Trading on Nado, then re-link using Advanced 1CT with the bot's key.")))
        else:
            lines.append(f"❌ *{_loc('Signer Check')}:* {_loc('No signer linked on exchange')}")
            lines.append(escape_md(_loc("→ Go to Nado Settings → 1-Click Trading → Advanced 1CT → paste bot's key → enable and save.")))
    elif signer_linked:
        lines.append("")
        lines.append(f"🔗 *{_loc('1CT Signer')}:* {escape_md(_loc('LINKED (not verified)'))}")
    else:
        lines.append("")
        lines.append(f"🔗 *{_loc('1CT Signer')}:* {escape_md(_loc('NOT LINKED'))}")

    lines.append("")
    lines.append(_loc("Use the 👛 Wallet button to link or revoke your 1CT key\\."))

    return "\n".join(lines)


def fmt_alerts(alerts):
    if not alerts:
        return _loc("🔔 *Alert Engine*") + "\n\n" + _loc("No active alerts\\. Use Set Alert to create one\\.")

    lines = [
        _loc("🔔 *Alert Engine*"),
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
    ]

    for a in alerts:
        target_str = f"${a['target']:,.2f}"
        lines.append(
            f"\\#{escape_md(str(a['id']))} {escape_md(a['product'])} "
            f"{escape_md(a['condition'])} {escape_md(target_str)} "
            f"\\({escape_md(a['network'])}\\)"
        )

    return "\n".join(lines)



def fmt_points_dashboard(payload: dict) -> str:
    if not payload or not payload.get("ok"):
        err = (payload or {}).get("error") or "Points data unavailable."
        return f"🏆 *Nado Points*\n\n{escape_md(err)}"

    no_activity = bool(payload.get("no_activity"))
    points = float(payload.get("points") or 0.0)
    volume_usd = float(payload.get("volume_usd") or 0.0)
    cpp = float(payload.get("cost_per_point") or 0.0)
    total_costs = float(payload.get("total_costs") or 0.0)
    ppm = float(payload.get("ppm") or 0.0)
    window = payload.get("window_label") or "Last 7 Days"

    lines = [
        "🏆 *Your Nado Points Dashboard*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        f"📅 *Window:* {escape_md(window)}",
        "",
    ]
    if no_activity:
        lines.extend(
            [
                "No points activity found for this period\\.",
                "",
                "Tip: increase real trading activity and check again after the next weekly epoch\\.",
            ]
        )
        return "\n".join(lines)

    lines.extend(
        [
            f"⭐ *Points:* {escape_md(f'{points:,.2f}')}",
            f"💰 *Volume:* {escape_md(f'${volume_usd:,.2f}')}",
            f"🧾 *Cost / Point:* {escape_md(f'${cpp:,.4f}')}",
            f"💸 *Est. Costs:* {escape_md(f'${total_costs:,.2f}')}",
            f"📊 *Points / $1M:* {escape_md(f'{ppm:,.2f}')}",
        ]
    )
    return "\n".join(lines)


def _compute_exchange_stats(positions, prices):
    unrealized_pnl = 0.0
    position_value = 0.0
    for p in (positions or []):
        amount = abs(float(p.get("amount", 0) or 0))
        pname = p.get("product_name", "???")
        base = pname.replace("-PERP", "")
        current = 0.0
        if prices and base in prices:
            try:
                current = float((prices.get(base) or {}).get("mid", 0) or 0)
            except Exception:
                current = 0.0
        if current:
            pnl = _calc_position_pnl(p, current)
            if pnl is not None:
                unrealized_pnl += pnl
            position_value += amount * current
    return unrealized_pnl, position_value


def fmt_portfolio(stats, positions, prices=None):
    total_trades = int(stats.get("total_trades", 0) or 0)

    unrealized_pnl, position_value = _compute_exchange_stats(positions, prices)

    upnl_emoji = "🟢" if unrealized_pnl >= 0 else "🔴"
    upnl_str = f"+${unrealized_pnl:,.2f}" if unrealized_pnl >= 0 else f"-${abs(unrealized_pnl):,.2f}"

    lines = [
        _loc("📁 *Portfolio Deck*"),
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"📌 *{_loc('Open Positions:')}* {escape_md(str(len(positions or [])))}",
        f"💎 *{_loc('Position Value:')}* {escape_md(f'${position_value:,.2f}')}",
        f"{upnl_emoji} *{_loc('Unrealized PnL:')}* {escape_md(upnl_str)}",
    ]

    if total_trades > 0:
        total_volume = float(stats.get("total_volume", 0) or 0)
        total_pnl = float(stats.get("total_pnl", 0) or 0)
        win_rate = float(stats.get("win_rate", 0) or 0)
        rpnl_emoji = "🟢" if total_pnl >= 0 else "🔴"
        rpnl_str = f"+${total_pnl:,.2f}" if total_pnl >= 0 else f"-${abs(total_pnl):,.2f}"
        lines.extend([
            "",
            f"*{_loc('Bot Trading Stats')}*",
            f"💰 *{_loc('Volume:')}* {escape_md(f'${total_volume:,.2f}')}",
            f"{rpnl_emoji} *{_loc('Realized PnL:')}* {escape_md(rpnl_str)}",
            f"🏆 *{_loc('Win Rate:')}* {escape_md(f'{win_rate:.1f}%')} \\| {_loc('Trades:')} {escape_md(str(total_trades))}",
        ])

    if not positions:
        lines.append("")
        lines.append(_loc("No open positions right now\\."))
        return "\n".join(lines)

    lines.extend(["", f"*{_loc('Top Open Positions')}*"])
    for p in (positions or [])[:5]:
        side = p.get("side", "LONG")
        amount = abs(float(p.get("amount", 0) or 0))
        pname = p.get("product_name", "???")
        base = pname.replace("-PERP", "")
        current = 0.0
        if prices and base in prices:
            try:
                current = float((prices.get(base) or {}).get("mid", 0) or 0)
            except Exception:
                current = 0.0

        pnl_text = _loc("N/A")
        if current:
            pnl = _calc_position_pnl(p, current)
            if pnl is not None:
                pnl_text = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"

        lines.append(
            f"• {escape_md(side)} {escape_md(f'{amount:.4f}')} {escape_md(pname)} \\| {_loc('PnL')}: {escape_md(pnl_text)}"
        )

    if len(positions) > 5:
        lines.append(f"• \\.\\.\\. {_loc('and')} {escape_md(str(len(positions) - 5))} {_loc('more')}")
    return "\n".join(lines)


def fmt_settings(user_data):
    leverage = user_data.get("default_leverage", 1)
    slippage = user_data.get("slippage", 1)

    lines = [
        _loc("⚙️ *Control Panel*"),
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"⚡ *{_loc('Default Leverage')}:* {escape_md(f'{leverage}x')}",
        f"📊 *{_loc('Slippage')}:* {escape_md(f'{slippage}%')}",
    ]

    return "\n".join(lines)


def fmt_help():
    _HELP_TEXT = (
        "📖 *Trading Bot Guide*\n"
        + escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━") + "\n"
        "\n"
        "*Available Commands:*\n"
        "/start \\- Open command center\n"
        "/help \\- Show this guide\n"
        "/status \\- Runtime health and strategy status\n"
        "/revoke \\- Show 1CT revoke steps\n"
        "/stop\\_all \\- Stop running strategy loops\n"
        "\n"
        "*Sections:*\n"
        "\n"
        "💼 *Wallet Vault*\n"
        "Link your wallet with secure 1CT flow, view balances, and manage signer access\\.\n"
        "\n"
        "🤖 *Trading Console*\n"
        "Place market or limit orders from guided flow or natural language commands\\.\n"
        "\n"
        "🧠 *Strategy Lab*\n"
        "Configure and run automated strategies: MM Bot, Grid Reactor, Mirror DN, and Volume Engine\\.\n"
        "Each dashboard includes a \"How it works\" explainer and pre\\-trade analytics\\.\n"
        "\n"
        "📁 *Portfolio Deck*\n"
        "Track open positions, realized/unrealized PnL, and runtime performance stats\\.\n"
        "\n"
        "🔒 *Security*\n"
        "• 1CT signer keys are encrypted with server key\n"
        "• Never share your private key or seed phrase\n"
        "• Use dedicated wallets for automation\n"
        "\n"
        "🧠 *Ask NadoBro AI*\n"
        "Ask docs, API, trading, and troubleshooting questions directly in chat\\.\n"
        "\n"
        "*Examples:*\n"
        "  • `Long BTC 0\\.01`\n"
        "  • `Short ETH 0\\.05 at 10x`\n"
        "  • `What is unified margin?`\n"
        "  • `Show my positions`\n"
        "  • `Close all`\n"
        "\n"
        "Need support? Ask in chat with full error context and command used\\."
    )
    return _loc(_HELP_TEXT)


def _fmt_action_label(action: str) -> str:
    labels = {
        "hold": _loc("Holding"),
        "open_long": _loc("Opened Long"),
        "open_short": _loc("Opened Short"),
        "close": _loc("Closed Position"),
        "emergency_flatten": _loc("Emergency Flatten"),
        "blocked": _loc("Blocked"),
        "cycle": _loc("Cycle Complete"),
    }
    return labels.get(action, action.replace("_", " ").title() if action else "—")


def _fmt_uptime(started_at: str) -> str:
    if not started_at:
        return "—"
    try:
        from datetime import datetime, timezone
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - start
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        minutes, _ = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}h {minutes}m"
        return f"{minutes}m"
    except Exception:
        return "—"


def fmt_status_overview(status: dict, onboarding: dict):
    running = status.get("running")
    complete = onboarding.get("onboarding_complete")
    mode = onboarding.get("network", "testnet").upper()
    key_ready = onboarding.get("has_key")
    funded = onboarding.get("funded")

    lines = [
        _loc("📡 *Nadobro Status*"),
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"{_loc('Network:')} *{escape_md(mode)}*",
    ]

    if not complete:
        step = onboarding.get("missing_step") or "unknown"
        lines.append(f"{_loc('Setup:')} *{_loc('IN PROGRESS')}* — {escape_md(step.replace('_', ' ').title())}")
        if not key_ready:
            lines.append(f"{_loc('Key:')} *{_loc('NOT SET')}* — {_loc('use /onboard to continue')}")
        elif not funded:
            lines.append(f"{_loc('Funding:')} *{_loc('NEEDED')}* — {_loc('deposit to your wallet')}")
    lines.append("")

    if not running:
        lines.append(f"{_loc('Strategy:')} *{_loc('NOT RUNNING')}*")
        last_action = status.get("last_action")
        if last_action:
            lines.append(f"{_loc('Last Action:')} {escape_md(_fmt_action_label(last_action))}")
        if status.get("last_error"):
            lines.append(f"{_loc('Stopped:')} {escape_md(str(status.get('last_error'))[:100])}")
    else:
        strategy = (status.get("strategy") or "").upper()
        product = str(status.get("product", "BTC"))
        runs = status.get("runs", 0)
        interval = status.get("interval_seconds", 0)
        uptime = _fmt_uptime(status.get("started_at"))
        next_in = status.get("next_cycle_in", 0)

        lines.append(f"{_loc('Strategy:')} *{escape_md(strategy)}* — {_loc('RUNNING')}")
        if product != "MULTI":
            lines.append(f"{_loc('Pair:')} *{escape_md(product)}\\-PERP*")
        else:
            lines.append(f"{_loc('Mode:')} *Multi\\-Asset*")
        lines.append(f"{_loc('Uptime:')} *{escape_md(uptime)}* \\| {_loc('Cycles:')} *{escape_md(str(runs))}*")
        if next_in > 0:
            lines.append(f"{_loc('Next Scan:')} *{escape_md(str(next_in))}s*")
        else:
            lines.append(f"{_loc('Interval:')} *{escape_md(str(interval))}s*")

        last_action = status.get("last_action")
        if last_action:
            detail = status.get("last_action_detail") or ""
            action_text = _fmt_action_label(last_action)
            if detail:
                action_text += f" — {detail[:80]}"
            lines.append(f"{_loc('Last:')} {escape_md(action_text)}")

        if status.get("is_paused"):
            lines.append(f"⚠️ *{_loc('PAUSED')}*: {escape_md(str(status.get('pause_reason') or _loc('Unknown')))}")

        error_streak = status.get("error_streak", 0)
        if error_streak >= 3:
            lines.append(f"⚠️ {escape_md(str(error_streak))} {_loc('consecutive errors')}")

        bro_state = status.get("bro_state") or {}
        if strategy == "BRO" and bro_state:
            trade_count = bro_state.get("trade_count", 0)
            total_pnl = bro_state.get("total_pnl", 0.0)
            active = len(bro_state.get("active_positions", []))
            lines.append("")
            lines.append(f"{_loc('Trades:')} *{escape_md(str(trade_count))}* \\| {_loc('Open Positions:')} *{escape_md(str(active))}*")
            pnl_sign = "+" if total_pnl >= 0 else ""
            lines.append(f"{_loc('Session PnL:')} *{escape_md(f'{pnl_sign}${total_pnl:,.2f}')}*")

        maker_fill_ratio = status.get("maker_fill_ratio")
        if maker_fill_ratio is not None:
            cancellation_ratio = status.get("cancellation_ratio")
            lines.append("")
            lines.append(
                f"{_loc('Fill Rate')}: *{escape_md(f'{float(maker_fill_ratio) * 100:.1f}%')}* \\| "
                f"{_loc('Cancel Rate')}: *{escape_md(f'{float(cancellation_ratio or 0) * 100:.1f}%')}*"
            )
        avg_quote_distance_bp = status.get("avg_quote_distance_bp")
        if avg_quote_distance_bp is not None:
            quote_refresh_rate = status.get("quote_refresh_rate")
            lines.append(
                f"{_loc('Quote Dist')}: *{escape_md(f'{float(avg_quote_distance_bp):.2f} bp')}* \\| "
                f"{_loc('Refresh')}: *{escape_md(f'{float(quote_refresh_rate or 0):.2f}/s')}*"
            )
        inventory_skew_usd = status.get("inventory_skew_usd")
        if inventory_skew_usd is not None:
            lines.append(f"{_loc('Inventory Skew')}: *{escape_md(f'${float(inventory_skew_usd):,.2f}')}*")

    return "\n".join(lines)


def fmt_strategy_update(strategy: str, network: str, conf: dict) -> str:
    notional = float(conf.get("notional_usd", 100.0))
    spread_bp = float(conf.get("spread_bp", 5.0))
    interval_seconds = int(conf.get("interval_seconds", 60))
    tp_pct = float(conf.get("tp_pct", 1.0))
    sl_pct = float(conf.get("sl_pct", 0.5))
    return (
        f"✅ *{escape_md(strategy.upper())} {_loc('updated')}* \\({escape_md(network.upper())}\\)\n\n"
        f"{_loc('Notional')}: {escape_md(f'${notional:,.2f}')}\n"
        f"{_loc('Spread')}: {escape_md(f'{spread_bp:.1f} bp')}\n"
        f"{_loc('Interval')}: {escape_md(f'{interval_seconds}s')}\n"
        f"{_loc('TP')}: {escape_md(f'{tp_pct:.2f}%')}\n"
        f"{_loc('SL')}: {escape_md(f'{sl_pct:.2f}%')}"
    )
