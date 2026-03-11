import logging
import re
from typing import Optional
from src.nadobro.config import get_product_name, get_product_base_symbol, PRODUCTS
from src.nadobro.i18n import get_active_language, localize_text

logger = logging.getLogger(__name__)


def _loc(text: str) -> str:
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
        return "📋 *Open Positions*\n\nNo open positions or orders found\\."

    lines = [
        "📋 *Open Positions*",
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
        is_limit = p.get("is_limit_order", False)

        limit_tag = " (Limit)" if is_limit else ""
        lines.append(
            f"{side_emoji} *{escape_md(side)}* {escape_md(f'{amount:.4f}')} "
            f"{escape_md(pname)} @ {escape_md(f'${entry:,.2f}')}{escape_md(limit_tag)}"
        )

        # PnL only for filled positions, not resting limit orders
        if is_limit:
            continue
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
    lines.append(f"*Total:* {escape_md(str(len(positions)))} position\\(s\\)")
    return "\n".join(lines)


def fmt_position_pnl_panel(position: dict, current_price: float) -> str:
    side = str(position.get("side", "LONG")).upper()
    side_emoji = "🟢" if side == "LONG" else "🔴"
    amount = abs(float(position.get("amount", 0) or 0))
    pname = str(position.get("product_name", "BTC-PERP"))
    base = pname.replace("-PERP", "")
    entry = float(position.get("price", 0) or 0)
    mark = float(current_price or 0)
    value = amount * mark if mark > 0 else 0.0
    lev = float(position.get("leverage", 1) or 1)
    pnl = _calc_position_pnl(position, mark) if mark > 0 else None
    pnl_str = f"+${pnl:,.2f}" if pnl is not None and pnl >= 0 else f"-${abs(pnl):,.2f}" if pnl is not None else "N/A"
    pnl_emoji = "🟢" if pnl is None or pnl >= 0 else "🔴"
    entry_str = f"${fmt_price(entry, base)}" if entry > 0 else "N/A"
    mark_str = f"${fmt_price(mark, base)}" if mark > 0 else "N/A"
    return "\n".join([
        "💼 *Position PnL Monitor*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"{escape_md(base)} {side_emoji} *{escape_md(side)}*",
        f"├ Size: *{escape_md(f'{amount:.6f}')}*",
        f"├ Value: *{escape_md(f'${value:,.2f}')}*",
        f"├ Entry: *{escape_md(entry_str)}*",
        f"├ Mark: *{escape_md(mark_str)}*",
        f"├ Leverage: *{escape_md(f'{lev:g}x')}*",
        f"└ PnL: {pnl_emoji} *{escape_md(pnl_str)}*",
    ])


def fmt_balance(balance_data, wallet_addr=None):
    lines = [
        "💰 *Wallet Vault*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
    ]

    if not balance_data or not balance_data.get("exists"):
        lines.append("⚠️ *No subaccount found*\\. ")
        lines.append("")
        if wallet_addr:
            lines.append(f"Deposit ≥ \\$5 USDT0 to:")
            lines.append(f"`{escape_md(wallet_addr)}`")
            lines.append("")
            lines.append(f"🚰 Faucet: {escape_md('https://testnet.nado.xyz/portfolio/faucet')}")
        return "\n".join(lines)

    balances = balance_data.get("balances", {}) or {}
    usdt = balances.get(0, balances.get("0", 0))
    lines.append(f"💵 *USDT0:* *{escape_md(f'${usdt:,.2f}')}*")

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


def fmt_pre_trade_analytics(
    margin: float,
    est_volume: float,
    max_loss: float | None,
    estimated_fees: float,
    fees_label: str = "Est\\. Fees",
) -> str:
    """Format Pre-Trade Analytics: Margin, Est. Volume, Max Loss, Est. Fees.
    Use fees_label='Est\\. Fees/cycle' for strategy dashboard per-cycle fees."""
    if margin <= 0 and est_volume <= 0:
        return ""
    max_loss_str = escape_md(f"${max_loss:,.2f}") if max_loss is not None else "N/A"
    lines = [
        "",
        "*Pre\\-Trade Analytics*",
        f"*Margin:* {escape_md(f'${margin:,.2f}')}",
        f"*Est\\. Volume:* {escape_md(f'${est_volume:,.2f}')}",
        f"*Max Loss:* {max_loss_str}",
        f"*{fees_label}:* {escape_md(f'${estimated_fees:,.2f}')}",
    ]
    return "\n".join(lines)


def fmt_trade_preview(action, product, size, price, leverage=1, est_margin=None, analytics=None):
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

    if analytics and isinstance(analytics, dict) and "margin" in analytics:
        lines.append(fmt_pre_trade_analytics(
            margin=analytics["margin"],
            est_volume=analytics["est_volume"],
            max_loss=analytics.get("max_loss"),
            estimated_fees=analytics["estimated_fees"],
        ))

    lines.append("")
    lines.append("*Confirm to execute this trade\\.*")

    return "\n".join(lines)


def fmt_trade_result(result):
    if result.get("success"):
        r_price = result.get("price", 0)
        r_product = result.get("product", "BTC")
        price_str = "$" + fmt_price(r_price, r_product)
        order_type = result.get("type", "MARKET")
        is_limit_pending = order_type == "LIMIT" and not bool(result.get("filled", True))
        lines = [
            "✅ *Limit Order Placed\\!*" if is_limit_pending else "✅ *Trade Executed\\!*",
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
        if order_type != "MARKET":
            lines.insert(3, f"📋 *Type:* {escape_md(order_type)}")
        if is_limit_pending:
            lines.append("⏳ *Status:* OPEN")
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
        return "🔔 *Alert Engine*\n\nNo active alerts\\. Use *Set Alert* to create one\\."

    lines = [
        "🔔 *Alert Engine*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
    ]

    for a in alerts:
        target_str = f"${a['target']:,.2f}"
        lines.append(
            f"*\\#{escape_md(str(a['id']))}* {escape_md(a['product'])} "
            f"*{escape_md(a['condition'])}* {escape_md(target_str)} "
            f"\\({escape_md(a['network'])}\\)"
        )

    return "\n".join(lines)



def _compute_exchange_stats(positions, prices):
    unrealized_pnl = 0.0
    position_value = 0.0
    for p in (positions or []):
        if p.get("is_limit_order"):
            continue  # Resting limit orders have no position/PnL yet
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


def _compute_directional_bias(positions, prices):
    """Returns (bias_label, long_value, short_value, ls_ratio_str)."""
    long_value = 0.0
    short_value = 0.0
    for p in (positions or []):
        if p.get("is_limit_order"):
            continue  # Resting limit orders are not positions
        side = str(p.get("side", "LONG")).upper()
        amount = abs(float(p.get("amount", 0) or 0))
        pname = p.get("product_name", "???")
        base = pname.replace("-PERP", "")
        current = 0.0
        if prices and base in prices:
            try:
                current = float((prices.get(base) or {}).get("mid", 0) or 0)
            except Exception:
                current = 0.0
        if not current:
            continue
        val = amount * current
        if side == "LONG":
            long_value += val
        else:
            short_value += val

    total = long_value + short_value
    if total <= 0:
        return "Delta Neutral", 0.0, 0.0, "1.00"
    diff_ratio = abs(long_value - short_value) / total
    if diff_ratio < 0.1:
        bias = "Delta Neutral"
    elif long_value > short_value:
        bias = "Long"
    else:
        bias = "Short"
    if short_value > 0:
        ls_ratio = long_value / short_value
        ls_str = f"{ls_ratio:.2f}"
    elif long_value > 0:
        ls_str = "∞"
    else:
        ls_str = "1.00"
    return bias, long_value, short_value, ls_str


def _compute_liquidation_metrics(positions, total_equity, prices):
    """Compute estimated liquidation-related metrics. Full health from API not yet available."""
    if not positions:
        return {"margin_used": 0.0, "avg_leverage": 0.0}
    margin_used = 0.0
    total_notional = 0.0
    for p in positions:
        if p.get("is_limit_order"):
            continue  # Resting limit orders don't use margin yet
        amount = abs(float(p.get("amount", 0) or 0))
        pname = p.get("product_name", "???")
        base = pname.replace("-PERP", "")
        current = 0.0
        if prices and base in prices:
            try:
                current = float((prices.get(base) or {}).get("mid", 0) or 0)
            except Exception:
                current = 0.0
        if current > 0:
            notional = amount * current
            lev = float(p.get("leverage", 1) or 1)
            lev = max(1.0, lev)
            margin_used += notional / lev
            total_notional += notional
    avg_lev = total_notional / margin_used if margin_used > 0 else 0.0
    return {"margin_used": margin_used, "avg_leverage": avg_lev, "total_notional": total_notional}


def _compute_asset_breakdown(balance, positions, prices):
    """Returns dict with perp_usd, spot_usd, cash_usd."""
    cash_usd = 0.0
    spot_usd = 0.0
    perp_usd = 0.0
    if balance and balance.get("exists"):
        balances = balance.get("balances", {}) or {}
        for pid_raw, amount in balances.items():
            try:
                pid = int(pid_raw)
            except (TypeError, ValueError):
                continue
            amt = float(amount or 0)
            if pid == 0:
                cash_usd += amt
            elif amt > 0:
                base = get_product_base_symbol(pid)
                if base and prices and base in prices:
                    try:
                        mid = float((prices.get(base) or {}).get("mid", 0) or 0)
                        spot_usd += amt * mid
                    except Exception:
                        pass
    for p in (positions or []):
        if p.get("is_limit_order"):
            continue  # Resting limit orders are not open positions
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
            perp_usd += amount * current
    return {"cash": cash_usd, "spot": spot_usd, "perp": perp_usd}


def _compute_total_equity(balance, positions, prices):
    """Total equity = cash (USDT0) + spot value + unrealized perp PnL."""
    cash = 0.0
    spot_value = 0.0
    if balance and balance.get("exists"):
        balances = balance.get("balances", {}) or {}
        for pid_raw, amount in balances.items():
            try:
                pid = int(pid_raw)
            except (TypeError, ValueError):
                continue
            amt = float(amount or 0)
            if pid == 0:
                cash += amt
            elif amt > 0:
                base = get_product_base_symbol(pid)
                if base and prices and base in prices:
                    try:
                        mid = float((prices.get(base) or {}).get("mid", 0) or 0)
                        spot_value += amt * mid
                    except Exception:
                        pass
    unrealized_pnl, _ = _compute_exchange_stats(positions, prices)
    return cash + spot_value + unrealized_pnl, cash, spot_value, unrealized_pnl


def fmt_portfolio(stats, positions, prices=None, balance=None, equity_1d_pct=None, equity_7d_pct=None):
    total_trades = int(stats.get("total_trades", 0) or 0)

    unrealized_pnl, position_value = _compute_exchange_stats(positions, prices)
    total_equity, cash, spot_value, _ = _compute_total_equity(balance, positions, prices)
    bias_label, long_val, short_val, ls_ratio = _compute_directional_bias(positions, prices)
    breakdown = _compute_asset_breakdown(balance, positions, prices)
    liq = _compute_liquidation_metrics(positions, total_equity, prices)

    upnl_emoji = "🟢" if unrealized_pnl >= 0 else "🔴"
    upnl_str = f"+${unrealized_pnl:,.2f}" if unrealized_pnl >= 0 else f"-${abs(unrealized_pnl):,.2f}"
    roi_pct = (unrealized_pnl / position_value * 100) if position_value > 0 else None
    roi_str = f"{roi_pct:+.2f}%" if roi_pct is not None else "N/A"

    eq_change = ""
    if equity_1d_pct is not None or equity_7d_pct is not None:
        parts = []
        if equity_1d_pct is not None:
            parts.append(f"1d: {escape_md(f'{equity_1d_pct:+.2f}%')}")
        if equity_7d_pct is not None:
            parts.append(f"7d: {escape_md(f'{equity_7d_pct:+.2f}%')}")
        if parts:
            sep = " \\| "
            eq_change = f" \\({sep.join(parts)}\\)"

    perp_fmt = f'${breakdown.get("perp", 0):,.2f}'
    spot_fmt = f'${breakdown.get("spot", 0):,.2f}'
    cash_fmt = f'${breakdown.get("cash", 0):,.2f}'
    lines = [
        "📁 *Portfolio Deck*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"💰 *Total Equity:* {escape_md(f'${total_equity:,.2f}')}{eq_change}",
        f"📊 *Directional Bias:* {escape_md(bias_label)} \\(L/S: {escape_md(ls_ratio)}\\)",
        "",
        f"📌 *Open Positions:* {escape_md(str(len(positions or [])))}",
        f"💎 *Position Value:* {escape_md(f'${position_value:,.2f}')}",
        f"{upnl_emoji} *Unrealized PnL:* {escape_md(upnl_str)} \\(ROI: {escape_md(roi_str)}\\)",
        "",
        "*Asset Breakdown*",
        f"*📈 Perpetual Futures:* {escape_md(perp_fmt)}",
        f"*📉 Spot:* {escape_md(spot_fmt)}",
        f"*💵 Cash \\(USDT0\\):* {escape_md(cash_fmt)}",
    ]
    if liq.get("margin_used", 0) > 0:
        margin_fmt = f'${liq.get("margin_used", 0):,.2f}'
        avg_lev_fmt = f'{liq.get("avg_leverage", 0):.1f}x'
        lines.extend([
            "",
            "*Liquidation Risk \\(est\\.\\)*",
            f"*Margin Used:* {escape_md(margin_fmt)} \\| "
            f"*Avg Leverage:* {escape_md(avg_lev_fmt)}",
        ])

    if total_trades > 0:
        total_volume = float(stats.get("total_volume", 0) or 0)
        total_pnl = float(stats.get("total_pnl", 0) or 0)
        total_fees = float(stats.get("total_fees", 0) or 0)
        win_rate = float(stats.get("win_rate", 0) or 0)
        rpnl_emoji = "🟢" if total_pnl >= 0 else "🔴"
        rpnl_str = f"+${total_pnl:,.2f}" if total_pnl >= 0 else f"-${abs(total_pnl):,.2f}"
        lines.extend([
            "",
            "*Bot Trading Stats*",
            f"*💰 Volume:* {escape_md(f'${total_volume:,.2f}')}",
            f"{rpnl_emoji} *Realized PnL:* {escape_md(rpnl_str)}",
            f"*📋 Fees:* {escape_md(f'${total_fees:,.2f}')}",
            f"*🏆 Win Rate:* {escape_md(f'{win_rate:.1f}%')} \\| *Trades:* {escape_md(str(total_trades))}",
        ])

    if not positions:
        lines.append("")
        lines.append("No open positions right now\\.")
        return "\n".join(lines)

    lines.extend(["", "*Top Open Positions*"])
    for p in (positions or [])[:5]:
        is_limit = p.get("is_limit_order", False)
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
        if not is_limit and current:
            pnl = _calc_position_pnl(p, current)
            if pnl is not None:
                pnl_text = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"

        limit_suffix = " (Limit)" if is_limit else ""
        lines.append(
            f"• {escape_md(side)} {escape_md(f'{amount:.4f}')} {escape_md(pname)}{escape_md(limit_suffix)} \\| PnL: {escape_md(pnl_text)}"
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

    return _loc("\n".join(lines))


def fmt_help():
    return _loc(
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
        "Configure and run automated strategies: MM Bot, Grid Reactor, Delta Neutral, and Volume Engine\\.\n"
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
        strategy_name = str(status.get("strategy") or "").upper()
        lines.extend([
            f"Strategy Runtime: *{escape_md(strategy_name)}*",
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
        if strategy_name in ("MM", "GRID"):
            session_done = float(status.get("mm_session_done_usd") or 0.0)
            session_cap = float(status.get("mm_session_cap_usd") or 0.0)
            if session_cap > 0:
                pct = min(100.0, (session_done / session_cap) * 100.0)
                lines.append(
                    f"Session: *{escape_md(f'${session_done:,.2f}')}* / *{escape_md(f'${session_cap:,.2f}')}* "
                    f"\\(*{escape_md(f'{pct:.1f}')}%*\\)"
                )
                lines.append(
                    f"Status: *{escape_md('In progress' if session_done < session_cap else 'Session complete')}*"
                )
            else:
                lines.append(f"Session Volume: *{escape_md(f'${session_done:,.2f}')}*")
        elif strategy_name == "VOL":
            vol_done = float(status.get("vol_volume_done_usd") or 0.0)
            vol_target = float(status.get("vol_target_volume_usd") or 0.0)
            if vol_target > 0:
                pct = min(100.0, (vol_done / vol_target) * 100.0)
                lines.append(
                    f"Volume: *{escape_md(f'${vol_done:,.2f}')}* / *{escape_md(f'${vol_target:,.2f}')}* "
                    f"\\(*{escape_md(f'{pct:.1f}')}%*\\)"
                )
                lines.append(
                    f"Status: *{escape_md('In progress' if vol_done < vol_target else 'Target reached')}*"
                )
            else:
                lines.append(f"Volume: *{escape_md(f'${vol_done:,.2f}')}* completed")
        elif strategy_name == "DN":
            dn_asset = str(status.get("product") or "BTC").upper()
            fr = float(status.get("dn_last_funding_rate") or 0.0)
            f_cycle = float(status.get("dn_last_funding_cycle") or 0.0)
            f_recv = float(status.get("dn_funding_received") or 0.0)
            f_paid = float(status.get("dn_funding_paid") or 0.0)
            f_net = float(status.get("dn_funding_net") or 0.0)
            spot_size = float(status.get("dn_spot_size") or 0.0)
            perp_size = float(status.get("dn_perp_size") or 0.0)
            hedge_diff = float(status.get("dn_hedge_diff_size") or 0.0)
            cycle_remaining = int(status.get("dn_cycle_remaining_seconds") or 0)
            cycle_duration = int(status.get("dn_cycle_duration_seconds") or 0)
            f_cycle_str = f"+${f_cycle:,.4f}" if f_cycle >= 0 else f"-${abs(f_cycle):,.4f}"
            f_net_str = f"+${f_net:,.4f}" if f_net >= 0 else f"-${abs(f_net):,.4f}"
            lines.extend([
                "",
                "*DN Funding Report \\(live est\\.)*",
                f"Rate: *{escape_md(f'{fr:.8f}')}* \\| Last Cycle: *{escape_md(f_cycle_str)}*",
                f"Received: *{escape_md(f'${f_recv:,.4f}')}* \\| Paid: *{escape_md(f'${f_paid:,.4f}')}*",
                f"Net Funding: *{escape_md(f_net_str)}*",
                "",
                "*DN Hedge Progress*",
                f"Spot {escape_md(dn_asset)}: *{escape_md(f'{spot_size:.6f}')}* \\| "
                f"Perp {escape_md(dn_asset)}: *{escape_md(f'{perp_size:.6f}')}*",
                f"Hedge Diff: *{escape_md(f'{hedge_diff:.6f} {dn_asset}')}*",
            ])
            if cycle_duration > 0:
                lines.append(
                    f"Roll Window: *{escape_md(_fmt_duration_compact(cycle_remaining))}* remaining"
                )
            lines.append("Status: *In progress*")
    return _loc("\n".join(lines))


def fmt_strategy_update(strategy: str, network: str, conf: dict) -> str:
    notional = float(conf.get("notional_usd", 100.0))
    spread_bp = float(conf.get("spread_bp", 5.0))
    interval_seconds = int(conf.get("interval_seconds", 60))
    tp_pct = float(conf.get("tp_pct", 1.0))
    sl_pct = float(conf.get("sl_pct", 0.5))
    return _loc(
        f"✅ *{escape_md(strategy.upper())} updated* \\({escape_md(network.upper())}\\)\n\n"
        f"Notional: {escape_md(f'${notional:,.2f}')}\n"
        f"Spread: {escape_md(f'{spread_bp:.1f} bp')}\n"
        f"Interval: {escape_md(f'{interval_seconds}s')}\n"
        f"TP: {escape_md(f'{tp_pct:.2f}%')}\n"
        f"SL: {escape_md(f'{sl_pct:.2f}%')}"
    )


def _fmt_duration_compact(total_seconds: int) -> str:
    if total_seconds <= 0:
        return "0m"
    minutes = total_seconds // 60
    hours = minutes // 60
    rem_minutes = minutes % 60
    if hours <= 0:
        return f"{int(rem_minutes)}m"
    return f"{int(hours)}h {int(rem_minutes)}m"


def fmt_points_dashboard(points: dict) -> str:
    if not points or not points.get("ok"):
        err = (points or {}).get("error", "Could not load Nado points.")
        return _loc(f"🏆 *Nado Points*\n\n{escape_md(err)}")

    if str(points.get("points_source", "")).lower() == "lowiqpts_bridge":
        volume = float(points.get("volume_usd", 0) or 0)
        score = float(points.get("points", 0) or 0)
        cpp = float(points.get("cost_per_point", 0) or 0)
        if bool(points.get("no_activity")):
            return _loc(
                "🏆 *Nado Points Dashboard*\n\n"
                f"*Window:* *{escape_md(str(points.get('window_label', 'Last 7 Days')))}*\n\n"
                "*No mainnet points activity found yet\\.*\n\n"
                "If you've only traded on testnet, this is expected\\."
            )
        if cpp <= 0:
            mood = "⚪"
            mascot = "🤖 Awaiting full bridge stats"
        elif cpp < 8:
            mood = "🟢"
            mascot = "😎 Super happy robot"
        elif cpp <= 15:
            mood = "🟡"
            mascot = "😊 Confident robot"
        else:
            mood = "🔴"
            mascot = "😤 We can improve this legend"
        return _loc(
            "🏆 *Nado Points Dashboard*\n\n"
            f"*Window:* *{escape_md(str(points.get('window_label', 'Last 7 Days')))}*\n"
            f"*Volume:* *{escape_md(f'${volume:,.2f}')}*\n"
            f"*Points:* *{escape_md(f'{score:,.2f}')}*\n"
            f"*Cost/Point:* *{escape_md(f'${cpp:,.2f}')}* {mood}\n\n"
            f"{escape_md(mascot)}"
        )

    points_label = " \\(estimated\\)" if points.get("points_estimated") else ""
    points_source = points.get("points_source", "estimated")
    maker_pct = float(points.get("maker_pct", 0) or 0)
    taker_pct = float(points.get("taker_pct", 0) or 0)
    maker_count = int(points.get("maker_count", 0) or 0)
    taker_count = int(points.get("taker_count", 0) or 0)
    volume_str = f"${float(points.get('volume_usd', 0) or 0):,.2f}"
    points_str = f"{float(points.get('points', 0) or 0):.2f}"
    cpp_str = f"${float(points.get('cost_per_point', 0) or 0):.2f}"
    fees_str = f"${float(points.get('fees_paid', 0) or 0):,.2f}"
    ppm_str = f"{float(points.get('ppm', 0) or 0):.0f}"
    positions_str = str(points.get("positions", 0) or 0)
    avg_hold_str = _fmt_duration_compact(int(points.get("avg_hold_seconds", 0) or 0))
    missing_fields = points.get("missing_fields") or []
    partial = ""
    if missing_fields:
        partial = f"\n\n⚠️ Partial data: {escape_md(', '.join(str(x) for x in missing_fields))}"
    source_line = "estimated from fills/fees" if points.get("points_estimated") else str(points_source)

    return _loc(
        "🏆 *Nado Points Dashboard*\n\n"
        f"*📊 Epoch:* {escape_md(str(points.get('epoch', 1)))}\n"
        f"*Volume:* {escape_md(volume_str)}\n"
        f"*Points{points_label}:* {escape_md(points_str)}\n"
        f"*Cost/Point:* {escape_md(cpp_str)}\n\n"
        f"*Maker* {escape_md(f'{maker_pct:.1f}%')} {escape_md(points.get('maker_bar', '░░░░░░░░░░'))} "
        f"{escape_md(f'{taker_pct:.1f}%')} *Taker*\n"
        f"*Fills:* Maker {escape_md(str(maker_count))} \\| Taker {escape_md(str(taker_count))}\n"
        f"*Fees:* {escape_md(fees_str)}  \\|  *PPM:* {escape_md(ppm_str)}  \\|  "
        f"*Positions:* {escape_md(positions_str)}  \\|  *Avg Hold:* {escape_md(avg_hold_str)}\n"
        f"*Source:* {escape_md(source_line)}"
        f"{partial}"
    )
