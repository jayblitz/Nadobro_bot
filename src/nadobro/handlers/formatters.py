import re
from typing import Optional
from src.nadobro.config import get_product_name, PRODUCTS


def escape_md(text):
    if text is None:
        return ""
    text = str(text)
    text = text.replace('\\', '\\\\')
    special = r'_*[]()~`>#+-=|{}.!'
    return re.sub(r'([' + re.escape(special) + r'])', r'\\\1', text)


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

        if current and entry:
            if side == "LONG":
                pnl = (current - entry) * amount
            else:
                pnl = (entry - current) * amount
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
        "💰 *Wallet Balance*",
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
        return "💹 *Market Prices*\n\nCould not fetch prices\\."

    lines = [
        "💹 *Market Prices*",
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
        return "📊 *Funding Snapshot*\n\nCould not fetch funding data\\."

    lines = [
        "📊 *Funding Snapshot*",
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
        order_type = result.get("type", "MARKET")
        if order_type != "MARKET":
            lines.insert(3, f"📋 *Type:* {escape_md(order_type)}")
        return "\n".join(lines)
    else:
        error = result.get("error", "Unknown error")
        return f"❌ *Trade Failed*\n\n{escape_md(error)}"


def fmt_wallet_info(wallet_info):
    if not wallet_info:
        return "👛 *Wallet Info*\n\nWallet not found\\. Use /start first\\."

    net = wallet_info.get("network", "testnet")
    net_emoji = "🧪" if net == "testnet" else "🌐"
    signer_linked = bool(wallet_info.get("linked_signer_address"))

    lines = [
        "👛 *Wallet Info*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"📊 *Network:* {net_emoji} {escape_md(net.upper())}",
        f"🔗 *1CT Signer:* {escape_md('LINKED' if signer_linked else 'NOT LINKED')}",
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

    lines.append("")
    lines.append("Use the 👛 Wallet button to link or revoke your 1CT key\\.")

    return "\n".join(lines)


def fmt_alerts(alerts):
    if not alerts:
        return "🔔 *Price Alerts*\n\nNo active alerts\\. Use Set Alert to create one\\."

    lines = [
        "🔔 *Price Alerts*",
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



def fmt_portfolio(stats, positions, prices=None):
    total_trades = int(stats.get("total_trades", 0) or 0)
    total_volume = float(stats.get("total_volume", 0) or 0)
    total_pnl = float(stats.get("total_pnl", 0) or 0)
    win_rate = float(stats.get("win_rate", 0) or 0)

    pnl_emoji = "🟢" if total_pnl >= 0 else "🔴"
    pnl_str = f"+${total_pnl:,.2f}" if total_pnl >= 0 else f"-${abs(total_pnl):,.2f}"

    lines = [
        "📁 *Portfolio*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"📌 *Open Positions:* {escape_md(str(len(positions or [])))}",
        f"💰 *Total Volume:* {escape_md(f'${total_volume:,.2f}')}",
        f"{pnl_emoji} *Total PnL:* {escape_md(pnl_str)}",
        f"🏆 *Win Rate:* {escape_md(f'{win_rate:.1f}%')} \\| Trades: {escape_md(str(total_trades))}",
    ]

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
        entry = float(p.get("price", 0) or 0)
        current = 0.0
        if prices and base in prices:
            try:
                current = float((prices.get(base) or {}).get("mid", 0) or 0)
            except Exception:
                current = 0.0

        pnl_text = "N/A"
        if current and entry:
            pnl = (current - entry) * amount if side == "LONG" else (entry - current) * amount
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
        "⚙️ *Settings*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"⚡ *Default Leverage:* {escape_md(f'{leverage}x')}",
        f"📊 *Slippage:* {escape_md(f'{slippage}%')}",
    ]

    return "\n".join(lines)


def fmt_help():
    return (
        "❓ *NADOBRO \\- Help*\n"
        + escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━") + "\n"
        "\n"
        "⚡ *Quick Start:*\n"
        "Use *Continue Setup* for guided onboarding:\n"
        "mode \\-> key \\-> funding \\-> risk \\-> template\\.\n"
        "Use dedicated private keys per mode \\(testnet/mainnet\\)\\.\n"
        "Never paste a seed phrase or main wallet key\\.\n"
        "Key import uses a confirm step with masked fingerprint before saving\\.\n"
        "Settings and strategy params are saved separately for testnet/mainnet\\.\n"
        "Each strategy supports custom Notional, Spread, Interval, TP and SL values\\.\n"
        "\n"
        "🤖 *Button Trading:*\n"
        "Use the buttons below to trade\\. Select\n"
        "Buy/Long or Sell/Short, pick a product,\n"
        "choose size and leverage, then confirm\\.\n"
        "Limit Buy/Sell is also available from main menu\\.\n"
        "\n"
        "💬 *Natural Language:*\n"
        "You can also type commands like:\n"
        "  • `Long BTC 0\\.01`\n"
        "  • `Short ETH 0\\.05 at 10x`\n"
        "  • `What's BTC price?`\n"
        "  • `Show my positions`\n"
        "  • `Close all`\n"
        "\n"
        "🧠 *Ask Nado \\(AI Knowledge\\):*\n"
        "Ask anything about Nado DEX:\n"
        "  • `What is unified margin?`\n"
        "  • `How do liquidations work?`\n"
        "  • `What are the trading fees?`\n"
        "\n"
        "🪙 *Products:*\n"
        "BTC, ETH, SOL, XRP, BNB, LINK, DOGE, AVAX\n"
        "\n"
        "📌 *Commands:*\n"
        "/start \\- Dashboard\n"
        "/help \\- This help message\n"
        "/status \\- Running strategy bot status\n"
        "/revoke \\- Revoke 1CT linked signer\n"
        "/stop\\_all \\- Stop strategy bot and cancel open orders\n"
        "\n"
        "🔗 *Useful Links:*\n"
        "• Testnet Faucet: testnet\\.nado\\.xyz/portfolio/faucet\n"
        "• ETH Faucet: docs\\.inkonchain\\.com/tools/faucets"
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
