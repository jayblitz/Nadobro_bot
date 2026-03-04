import logging
import re
from typing import Optional
from src.nadobro.config import get_product_name, PRODUCTS

logger = logging.getLogger(__name__)


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
        return "📋 *Open Orders*\n\nNo open orders found\\."

    lines = [
        "📋 *Open Orders*",
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
                    f"  └ Mark: {escape_md(mark_str)} \\| "
                    f"PnL: {pnl_emoji} {escape_md(pnl_str)}"
                )

    lines.append("")
    lines.append(f"Total: {escape_md(str(len(positions)))} order\\(s\\)")
    return "\n".join(lines)


def fmt_balance(balance_data, wallet_addr=None):
    lines = [
        "💰 *Wallet Vault Balance*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
    ]

    if not balance_data or not balance_data.get("exists"):
        lines.append("⚠️ No subaccount found\\.")
        lines.append("")
        if wallet_addr:
            lines.append(f"Deposit ≥ \\$5 USDT0 to:")
            lines.append(f"`{escape_md(wallet_addr)}`")
            lines.append("")
            lines.append(f"🚰 Faucet: {escape_md('https://testnet.nado.xyz/portfolio/faucet')}")
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
        lines.append(f"📋 Address: `{escape_md(wallet_addr)}`")

    return "\n".join(lines)


def fmt_prices(prices):
    if not prices:
        return "📡 *Market Radar*\n\nCould not fetch prices\\."

    lines = [
        "📡 *Market Radar*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
    ]

    for name, p in prices.items():
        mid = p.get("mid", 0)
        bid = p.get("bid", 0)
        ask = p.get("ask", 0)
        spread = ask - bid if ask and bid else 0

        lines.append(
            f"*{escape_md(name)}\\-PERP:* {escape_md(f'${fmt_price(mid, name)}')}"
        )
        lines.append(
            f"  Bid: {escape_md(f'${fmt_price(bid, name)}')} \\| "
            f"Ask: {escape_md(f'${fmt_price(ask, name)}')}"
        )

    return "\n".join(lines)


def fmt_funding(funding_data):
    if not funding_data:
        return "📊 *Funding Scanner*\n\nCould not fetch funding data\\."

    lines = [
        "📊 *Funding Scanner*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
    ]

    for name, rate in funding_data.items():
        lines.append(
            f"*{escape_md(name)}\\-PERP:* {escape_md(f'{rate:.6f}')} \\(index\\)"
        )

    return "\n".join(lines)


def fmt_trade_preview(action, product, size, price, leverage=1, est_margin=None):
    action_upper = action.upper()
    emoji = "🟢" if "LONG" in action_upper else "🔴"

    if est_margin is None and price:
        est_margin = (size * price) / leverage if leverage > 1 else size * price

    lines = [
        f"{emoji} *Trade Preview*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"📌 *Action:* {escape_md(action_upper)}",
        f"🪙 *Product:* {escape_md(product)}\\-PERP",
        f"📏 *Size:* {escape_md(str(size))}",
        f"💲 *Price:* {escape_md(f'~${fmt_price(price, product)}')}",
        f"⚡ *Leverage:* {escape_md(f'{leverage}x')}",
    ]

    if est_margin is not None:
        lines.append(f"💰 *Est\\. Margin:* {escape_md(f'${est_margin:,.2f}')}")

    lines.append("")
    lines.append("Confirm to execute this trade\\.")

    return "\n".join(lines)


def fmt_trade_result(result):
    if result.get("success"):
        r_price = result.get("price", 0)
        r_product = result.get("product", "BTC")
        price_str = "$" + fmt_price(r_price, r_product)
        lines = [
            "✅ *Trade Executed\\!*",
            escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
            "",
            f"📌 *Side:* {escape_md(result.get('side', '?'))}",
            f"🪙 *Product:* {escape_md(result.get('product', '?'))}",
            f"📏 *Size:* {escape_md(str(result.get('size', '?')))}",
            f"💲 *Price:* {escape_md(price_str)}",
            "",
            f"🌐 *Network:* {escape_md(result.get('network', '?'))}",
        ]
        if result.get("tp_requested"):
            if result.get("tp_set"):
                lines.append(f"📈 *Take Profit:* {escape_md(str(result.get('tp_price')))}")
            else:
                lines.append(f"⚠️ *Take Profit:* {escape_md(str(result.get('tp_error', 'Failed to place TP order.')))}")
        if result.get("sl_requested"):
            if result.get("sl_armed"):
                lines.append(f"🛡 *Stop Loss:* {escape_md(str(result.get('sl_price')))}")
            else:
                lines.append(f"⚠️ *Stop Loss:* {escape_md(str(result.get('sl_error', 'Failed to arm SL rule.')))}")
        order_type = result.get("type", "MARKET")
        if order_type != "MARKET":
            lines.insert(3, f"📋 *Type:* {escape_md(order_type)}")
        return "\n".join(lines)
    else:
        error = result.get("error", "Unknown error")
        return f"❌ *Trade Failed*\n\n{escape_md(error)}"


def fmt_wallet_info(wallet_info):
    if not wallet_info:
        return "💼 *Wallet Vault*\n\nWallet not found\\. Use /start first\\."

    net = wallet_info.get("network", "testnet")
    net_emoji = "🧪" if net == "testnet" else "🌐"
    signer_linked = bool(wallet_info.get("linked_signer_address"))

    lines = [
        "💼 *Wallet Vault*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"📊 *Network:* {net_emoji} {escape_md(net.upper())}",
    ]

    addr = wallet_info.get("active_address")
    if addr:
        lines.append("")
        lines.append("📋 *Main Wallet:*")
        lines.append(f"`{escape_md(addr)}`")
    else:
        lines.append("")
        lines.append("📋 *Main Wallet:* Not set")

    if signer_linked:
        lines.append("")
        lines.append("🔐 *1CT Address:*")
        lines.append(f"`{escape_md(wallet_info['linked_signer_address'])}`")

    verification = wallet_info.get("signer_verification")
    if verification:
        lines.append("")
        if verification.get("error"):
            lines.append(f"⚠️ *Signer Check:* {escape_md('Could not verify — ' + str(verification['error'])[:60])}")
        elif verification.get("verified"):
            lines.append("✅ *Signer Check:* 1CT key is linked on Nado")
        elif verification.get("current_signer"):
            current = verification["current_signer"]
            expected = verification.get("expected_signer", "")
            lines.append("❌ *Signer Check:* MISMATCH")
            lines.append(f"  Exchange has: `{escape_md(current[:10])}\\.\\.\\.$`")
            lines.append(f"  Bot expects: `{escape_md(expected[:10])}\\.\\.\\.$`")
            lines.append(escape_md("→ Disable 1-Click Trading on Nado, then re-link using Advanced 1CT with the bot's key."))
        else:
            lines.append("❌ *Signer Check:* No signer linked on exchange")
            lines.append(escape_md("→ Go to Nado Settings → 1-Click Trading → Advanced 1CT → paste bot's key → enable and save."))
    elif signer_linked:
        lines.append("")
        lines.append(f"🔗 *1CT Signer:* {escape_md('LINKED (not verified)')}")
    else:
        lines.append("")
        lines.append(f"🔗 *1CT Signer:* {escape_md('NOT LINKED')}")

    lines.append("")
    lines.append("Use the 👛 Wallet button to link or revoke your 1CT key\\.")

    return "\n".join(lines)


def fmt_alerts(alerts):
    if not alerts:
        return "🔔 *Alert Engine*\n\nNo active alerts\\. Use Set Alert to create one\\."

    lines = [
        "🔔 *Alert Engine*",
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
        "📁 *Portfolio Deck*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"📌 *Open Positions:* {escape_md(str(len(positions or [])))}",
        f"💎 *Position Value:* {escape_md(f'${position_value:,.2f}')}",
        f"{upnl_emoji} *Unrealized PnL:* {escape_md(upnl_str)}",
    ]

    if total_trades > 0:
        total_volume = float(stats.get("total_volume", 0) or 0)
        total_pnl = float(stats.get("total_pnl", 0) or 0)
        win_rate = float(stats.get("win_rate", 0) or 0)
        rpnl_emoji = "🟢" if total_pnl >= 0 else "🔴"
        rpnl_str = f"+${total_pnl:,.2f}" if total_pnl >= 0 else f"-${abs(total_pnl):,.2f}"
        lines.extend([
            "",
            "*Bot Trading Stats*",
            f"💰 *Volume:* {escape_md(f'${total_volume:,.2f}')}",
            f"{rpnl_emoji} *Realized PnL:* {escape_md(rpnl_str)}",
            f"🏆 *Win Rate:* {escape_md(f'{win_rate:.1f}%')} \\| Trades: {escape_md(str(total_trades))}",
        ])

    if not positions:
        lines.append("")
        lines.append("No open positions right now\\.")
        return "\n".join(lines)

    lines.extend(["", "*Top Open Positions*"])
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

        pnl_text = "N/A"
        if current:
            pnl = _calc_position_pnl(p, current)
            if pnl is not None:
                pnl_text = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"

        lines.append(
            f"• {escape_md(side)} {escape_md(f'{amount:.4f}')} {escape_md(pname)} \\| PnL: {escape_md(pnl_text)}"
        )

    if len(positions) > 5:
        lines.append(f"• \\...and {escape_md(str(len(positions) - 5))} more")
    return "\n".join(lines)


def fmt_settings(user_data):
    leverage = user_data.get("default_leverage", 1)
    slippage = user_data.get("slippage", 1)

    lines = [
        "⚙️ *Control Panel*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"⚡ *Default Leverage:* {escape_md(f'{leverage}x')}",
        f"📊 *Slippage:* {escape_md(f'{slippage}%')}",
    ]

    return "\n".join(lines)


def fmt_help():
    return (
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
        "• 1CT signer keys are encrypted with your passphrase\n"
        "• Never share your passphrase, private key, or seed phrase\n"
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


def fmt_status_overview(status: dict, onboarding: dict):
    running = status.get("running")
    step = onboarding.get("missing_step") or "complete"
    complete = onboarding.get("onboarding_complete")
    mode = onboarding.get("network", "testnet").upper()
    key_ready = "YES" if onboarding.get("has_key") else "NO"
    funded = "YES" if onboarding.get("funded") else "NO"

    lines = [
        "📡 *Nadobro Status*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"Mode: *{escape_md(mode)}*",
        f"Onboarding: *{escape_md('COMPLETE' if complete else 'IN PROGRESS')}*",
        f"Next Step: *{escape_md(step.upper())}*",
        f"Key: *{escape_md(key_ready)}* \\| Funding: *{escape_md(funded)}*",
        "",
    ]
    if not running:
        lines.append("Strategy Runtime: *IDLE*")
    else:
        lines.extend([
            f"Strategy Runtime: *{escape_md((status.get('strategy') or '').upper())}*",
            f"Pair: *{escape_md(str(status.get('product', 'BTC')))}\\-PERP*",
            f"Cycles: *{escape_md(str(status.get('runs', 0)))}*",
            f"Interval: *{escape_md(str(status.get('interval_seconds', 0)))}s*",
        ])
        if status.get("is_paused"):
            lines.append(f"Pause: *{escape_md(str(status.get('pause_reason') or 'PAUSED'))}*")
        maker_fill_ratio = status.get("maker_fill_ratio")
        cancellation_ratio = status.get("cancellation_ratio")
        avg_quote_distance_bp = status.get("avg_quote_distance_bp")
        quote_refresh_rate = status.get("quote_refresh_rate")
        inventory_skew_usd = status.get("inventory_skew_usd")
        if maker_fill_ratio is not None:
            lines.append(
                f"Maker Fill Ratio: *{escape_md(f'{float(maker_fill_ratio) * 100:.1f}%')}* \\| "
                f"Cancel Ratio: *{escape_md(f'{float(cancellation_ratio or 0) * 100:.1f}%')}*"
            )
        if avg_quote_distance_bp is not None:
            lines.append(
                f"Quote Dist: *{escape_md(f'{float(avg_quote_distance_bp):.2f} bp')}* \\| "
                f"Refresh Rate: *{escape_md(f'{float(quote_refresh_rate or 0):.2f}/s')}*"
            )
        if inventory_skew_usd is not None:
            lines.append(f"Inventory Skew: *{escape_md(f'${float(inventory_skew_usd):,.2f}')}*")
    return "\n".join(lines)


def fmt_strategy_update(strategy: str, network: str, conf: dict) -> str:
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
