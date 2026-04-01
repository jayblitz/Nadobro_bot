import re
import time
import logging
from typing import Optional
from src.nadobro.config import get_product_name, PRODUCTS
from src.nadobro.i18n import get_active_language, localize_text

logger = logging.getLogger(__name__)


def _loc(text):
    return localize_text(text, get_active_language())


def _loc_md(text):
    return escape_md(_loc(text))


def escape_md(text):
    if text is None:
        return ""
    text = str(text)
    text = text.replace('\\', '\\\\')
    special = r'_*[]()~`>#+-=|{}.!'
    return re.sub(r'([' + re.escape(special) + r'])', r'\\\1', text)


def format_ai_response(text: str) -> str:
    """Convert LLM markdown output to Telegram MarkdownV2.

    Preserves bold, bullet points, numbered lists, and emojis while
    escaping everything else for safe Telegram rendering.  Falls back
    to full ``escape_md`` on any error.
    """
    if not text:
        return ""
    try:
        return _md_to_tg_md2(text)
    except Exception:
        logger.warning("format_ai_response fallback to escape_md", exc_info=True)
        return escape_md(text)


# ── Telegram MarkdownV2 converter ────────────────────────────────────

# Characters that Telegram requires escaped outside of formatting spans.
_TG_SPECIAL = set(r'_[]()~`>#+-=|{}.!')

def _escape_tg(s: str) -> str:
    """Escape for MarkdownV2 but leave already-escaped chars alone."""
    out: list[str] = []
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == '\\' and i + 1 < len(s):
            # already escaped – pass through
            out.append(ch)
            out.append(s[i + 1])
            i += 2
            continue
        if ch in _TG_SPECIAL:
            out.append('\\')
        out.append(ch)
        i += 1
    return "".join(out)


def _escape_and_convert_inline(raw: str) -> str:
    """Process inline markdown on a RAW (unescaped) string.

    Finds **bold** and `code` spans first, escapes everything else.
    This avoids the problem of escaping destroying markdown markers.
    """
    result: list[str] = []
    i = 0
    while i < len(raw):
        # Bold: **...**
        if raw[i:i+2] == '**':
            end = raw.find('**', i + 2)
            if end != -1:
                inner = _escape_tg(raw[i+2:end])
                result.append(f'*{inner}*')
                i = end + 2
                continue
        # Inline code: `...`
        if raw[i] == '`':
            end = raw.find('`', i + 1)
            if end != -1:
                inner = raw[i+1:end]  # code content not escaped in TG
                result.append(f'`{inner}`')
                i = end + 1
                continue
        # Regular character — escape if special
        ch = raw[i]
        if ch == '\\' and i + 1 < len(raw):
            result.append(ch)
            result.append(raw[i + 1])
            i += 2
            continue
        if ch in _TG_SPECIAL:
            result.append('\\')
        result.append(ch)
        i += 1
    return "".join(result)


def _md_to_tg_md2(text: str) -> str:
    """Convert standard markdown produced by the LLM into Telegram MarkdownV2."""
    lines = text.split('\n')
    out: list[str] = []

    for raw_line in lines:
        line = raw_line.rstrip()

        # Empty line → preserve spacing
        if not line:
            out.append("")
            continue

        # Bullet point: - text  or • text
        bullet_match = re.match(r'^(\s*)([-•])\s+(.+)$', line)
        if bullet_match:
            indent = bullet_match.group(1)
            body = _escape_and_convert_inline(bullet_match.group(3))
            out.append(f'{indent}\\- {body}')
            continue

        # Numbered list: 1. text
        num_match = re.match(r'^(\s*)(\d+)\.\s+(.+)$', line)
        if num_match:
            indent = num_match.group(1)
            num = _escape_tg(num_match.group(2))
            body = _escape_and_convert_inline(num_match.group(3))
            out.append(f'{indent}{num}\\. {body}')
            continue

        # Section header: ### text or ## text — render as bold line
        header_match = re.match(r'^#{1,4}\s+(.+)$', line)
        if header_match:
            body = _escape_tg(header_match.group(1))
            out.append(f'*{body}*')
            continue

        # Regular line — process inline formatting on raw text
        out.append(_escape_and_convert_inline(line))

    return '\n'.join(out)


def _calc_position_pnl(position: dict, current_price: float) -> Optional[float]:
    """Unrealized PnL aligned with Nado: prefer exchange-reported uPnL, else v_quote settlement."""
    raw = position.get("unrealized_pnl")
    if raw is not None:
        try:
            return float(raw)
        except Exception:
            pass

    v_quote = position.get("v_quote_balance")
    signed_amount = position.get("signed_amount")
    if v_quote is not None and signed_amount is not None and current_price:
        try:
            return float(v_quote) + float(signed_amount) * float(current_price)
        except Exception:
            pass

    entry = float(position.get("price", 0) or 0)
    amount = abs(float(position.get("amount", 0) or 0))
    if not current_price or not entry or not amount:
        return None
    side = str(position.get("side", "LONG")).upper()
    if side == "LONG":
        return (float(current_price) - entry) * amount
    return (entry - float(current_price)) * amount


def _has_exchange_unrealized_pnl(position: dict) -> bool:
    """True when the position dict carries a parseable uPnL from the exchange."""
    raw = position.get("unrealized_pnl")
    if raw is None:
        return False
    try:
        float(raw)
        return True
    except Exception:
        return False


def fmt_price(price, product="BTC"):
    if price is None or price == 0:
        return "N/A"
    product_upper = str(product).upper().replace("-PERP", "")
    if product_upper in ("BTC", "ETH", "BNB"):
        return f"{price:,.2f}"
    if price >= 1:
        return f"{price:,.2f}"
    return f"{price:,.4f}"



def fmt_positions(positions, prices=None, mode_label: str | None = None):
    if not positions:
        header = [_loc("📋 *Open Positions*")]
        if mode_label:
            header.append(f"🌐 *{_loc('Mode:')}* {escape_md(mode_label)}")
        return "\n".join(header) + "\n\n" + _loc("No open positions\\.")

    lines = [
        _loc("📋 *Open Positions*"),
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
    ]
    if mode_label:
        lines.append(f"🌐 *{_loc('Mode:')}* {escape_md(mode_label)}")
        lines.append("")
    any_estimated_pnl = False

    for i, p in enumerate(positions, 1):
        side = p.get("side", "LONG")
        side_emoji = "🟢" if side == "LONG" else "🔴"
        amount = abs(p.get("amount", 0))
        pname = p.get("product_name", "???")
        entry = float(p.get("price", 0) or 0)
        base = pname.replace("-PERP", "")

        lines.append(f"{side_emoji} *{escape_md(side)}* {escape_md(pname)}")

        current = 0.0
        if prices and base in prices:
            current = float(prices[base].get("mid", 0) or 0)

        mark_str = f"${fmt_price(current, base)}" if current else "—"
        lines.append(
            f"  • {_loc('Entry price')}: {escape_md(f'${entry:,.2f}')}"
        )
        lines.append(
            f"  • {_loc('Current price')}: {escape_md(mark_str)}"
        )

        pnl = None
        if current:
            pnl = _calc_position_pnl(p, current)
        if pnl is not None:
            if not _has_exchange_unrealized_pnl(p):
                any_estimated_pnl = True
            pnl_str = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"
            pnl_emoji = "🟢" if pnl >= 0 else "🔴"
            lines.append(
                f"  • {_loc('uPnL')}: {pnl_emoji} {escape_md(pnl_str)}"
            )
        else:
            lines.append(f"  • {_loc('uPnL')}: —")

        liq_raw = p.get("liquidation_price")
        if liq_raw is not None:
            try:
                liq_v = float(liq_raw)
                if liq_v > 0:
                    lines.append(
                        f"  • {_loc('Liquidation price')}: {escape_md(f'${fmt_price(liq_v, base)}')}"
                    )
                else:
                    lines.append(f"  • {_loc('Liquidation price')}: —")
            except Exception:
                lines.append(f"  • {_loc('Liquidation price')}: —")
        else:
            lines.append(f"  • {_loc('Liquidation price')}: —")

        lines.append(
            f"  • {_loc('Position size')}: {escape_md(f'{amount:.4f}')} {escape_md(base)}"
        )
        lines.append("")

    lines.append(f"{_loc('Total')}: {escape_md(str(len(positions)))} {_loc('position(s)')}")
    if any_estimated_pnl:
        lines.append("")
        lines.append(
            _loc(
                "ℹ️ PnL is estimated from mark vs\\. entry when the exchange does not report uPnL for this position\\."
            )
        )
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
        f"{emoji} *{_loc_md('Trade Preview')}*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
        f"📌 *{_loc_md('Action')}:* {escape_md(action_upper)}",
        f"🪙 *{_loc_md('Product')}:* {escape_md(product)}\\-PERP",
        f"📏 *{_loc_md('Size')}:* {escape_md(str(size))}",
        f"💲 *{_loc_md('Price')}:* {escape_md(f'~${fmt_price(price, product)}')}",
        f"⚡ *{_loc_md('Leverage')}:* {escape_md(f'{leverage}x')}",
    ]

    if est_margin is not None:
        lines.append(f"💰 *{_loc_md('Est. Margin')}:* {escape_md(f'${est_margin:,.2f}')}")

    lines.append("")
    lines.append(escape_md(_loc("Confirm to execute this trade.")))

    return "\n".join(lines)


def build_trade_preview_text(
    action: str,
    product: str,
    size: float,
    price: float,
    leverage: int = 1,
    est_margin=None,
    tp=None,
    sl=None,
) -> str:
    preview = fmt_trade_preview(action, product, size, price, leverage, est_margin)
    if tp:
        preview += f"\n\n📈 *{_loc_md('Take Profit')}:* {escape_md(str(tp))}"
    if sl:
        preview += f"\n📉 *{_loc_md('Stop Loss')}:* {escape_md(str(sl))}"
    return preview


def humanize_exchange_error(error) -> str:
    """Turn raw JSON / engine errors into user-friendly text (Telegram-safe)."""
    if error is None:
        return ""
    s = str(error).strip()
    low = s.lower()
    if "2070" in s or "maximum open interest" in low:
        return (
            "This market has hit its maximum open interest on the exchange (error 2070). "
            "You cannot open new positions until capacity frees up; you can still close or reduce positions."
        )
    try:
        import json

        j = json.loads(s)
        if isinstance(j, dict):
            code = j.get("error_code")
            msg = str(j.get("error") or "")
            if code == 2070 or "maximum open interest" in msg.lower():
                return humanize_exchange_error(msg or s)
    except Exception:
        pass
    return s


def fmt_trade_result(result):
    if result.get("success"):
        r_price = result.get("price", 0)
        r_product = result.get("product", "BTC")
        price_str = "$" + fmt_price(r_price, r_product)
        order_type_u = str(result.get("type", "MARKET") or "MARKET").upper()
        status_u = str(result.get("status", "") or "").lower()
        is_limit_pending = order_type_u == "LIMIT" and status_u == "pending"
        header = _loc("✅ *Limit order submitted*") if is_limit_pending else _loc("✅ *Trade Executed\\!*")
        price_label = _loc_md("Limit price") if is_limit_pending else _loc_md("Fill price")
        lines = [
            header,
            escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
            "",
            f"📌 *{_loc_md('Side')}:* {escape_md(result.get('side', '?'))}",
            f"🪙 *{_loc_md('Product')}:* {escape_md(result.get('product', '?'))}",
            f"📏 *{_loc_md('Size')}:* {escape_md(str(result.get('size', '?')))}",
            f"💲 *{price_label}:* {escape_md(price_str)}",
            "",
            f"🌐 *{_loc_md('Network:')}* {escape_md(result.get('network', '?'))}",
        ]
        if result.get("fee") is not None:
            try:
                fee_v = float(result.get("fee") or 0)
                lines.insert(-2, f"🧾 *{_loc_md('Fee')}:* {escape_md(f'${fee_v:,.4f}')}")
            except Exception:
                pass
        if result.get("tp_requested"):
            if result.get("tp_set"):
                lines.append(f"📈 *{_loc_md('Take Profit')}:* {escape_md(str(result.get('tp_price')))}")
            else:
                lines.append(f"⚠️ *{_loc_md('Take Profit')}:* {escape_md(str(result.get('tp_error', _loc('Failed to place TP order.'))))}")
        if result.get("sl_requested"):
            if result.get("sl_armed"):
                lines.append(f"🛡 *{_loc_md('Stop Loss')}:* {escape_md(str(result.get('sl_price')))}")
            else:
                lines.append(f"⚠️ *{_loc_md('Stop Loss')}:* {escape_md(str(result.get('sl_error', _loc('Failed to arm SL rule.'))))}")
        order_type = result.get("type", "MARKET")
        if order_type != "MARKET":
            lines.insert(3, f"📋 *{_loc_md('Type')}:* {escape_md(order_type)}")
        return "\n".join(lines)
    else:
        error = humanize_exchange_error(result.get("error", _loc("Unknown error")))
        return f"❌ *{_loc('Trade Failed')}*\n\n{escape_md(error)}"


def fmt_bracket_result(result: dict) -> str:
    """Format TP/SL placement on an existing position (natural-language commands)."""
    if result.get("success"):
        lines = [
            _loc("✅ *TP/SL updated*"),
            escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
            "",
            f"🪙 *{_loc_md('Product')}:* {escape_md(result.get('product', '?'))}",
            f"🌐 *{_loc_md('Network:')}* {escape_md(result.get('network', '?'))}",
        ]
        if result.get("tp_requested"):
            if result.get("tp_set"):
                lines.append(f"📈 *{_loc_md('Take Profit')}:* {escape_md(str(result.get('tp_price')))}")
            else:
                lines.append(
                    f"⚠️ *{_loc_md('Take Profit')}:* {escape_md(str(result.get('tp_error', _loc('Failed to place TP order.'))))}"
                )
        if result.get("sl_requested"):
            if result.get("sl_armed"):
                lines.append(f"🛡 *{_loc_md('Stop Loss')}:* {escape_md(str(result.get('sl_price')))}")
            else:
                lines.append(
                    f"⚠️ *{_loc_md('Stop Loss')}:* {escape_md(str(result.get('sl_error', _loc('Failed to arm SL rule.'))))}"
                )
        return "\n".join(lines)
    err = humanize_exchange_error(result.get("error", _loc("Unknown error")))
    return f"❌ *{_loc('Trade Failed')}* — TP/SL\n\n{escape_md(err)}"


def fmt_limit_close_result(result: dict) -> str:
    """Reduce-only limit order used to close or scale out of a position."""
    if result.get("success"):
        price_str = "$" + fmt_price(float(result.get("limit_price", 0)), result.get("product", "BTC"))
        lines = [
            _loc("✅ *Limit close placed*"),
            escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
            "",
            f"🪙 *{_loc_md('Product')}:* {escape_md(result.get('product', '?'))}",
            f"📏 *{_loc_md('Size')}:* {escape_md(str(result.get('size', '?')))}",
            f"💲 *{_loc_md('Limit')}:* {escape_md(price_str)}",
            f"📌 *{_loc_md('Side')}:* {escape_md(result.get('side', '?'))}",
            "",
            f"🌐 *{_loc_md('Network:')}* {escape_md(result.get('network', '?'))}",
        ]
        return "\n".join(lines)
    err = humanize_exchange_error(result.get("error", _loc("Unknown error")))
    return f"❌ *{_loc('Trade Failed')}* — limit close\n\n{escape_md(err)}"


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
        condition = str(a.get("condition") or "")
        target = float(a.get("target") or 0)
        if condition.startswith("funding"):
            target_str = f"{target:,.4f}%"
        else:
            target_str = f"${target:,.2f}"
        lines.append(
            f"\\#{escape_md(str(a['id']))} {escape_md(a['product'])} "
            f"{escape_md(condition)} {escape_md(target_str)} "
            f"\\({escape_md(a.get('network', 'mainnet'))}\\)"
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


def fmt_portfolio(stats, positions, prices=None, open_orders=None, mode_label: str | None = None):
    total_trades = int(stats.get("total_trades", 0) or 0)
    open_orders = open_orders or []

    unrealized_pnl, position_value = _compute_exchange_stats(positions, prices)

    upnl_emoji = "🟢" if unrealized_pnl >= 0 else "🔴"
    upnl_str = f"+${unrealized_pnl:,.2f}" if unrealized_pnl >= 0 else f"-${abs(unrealized_pnl):,.2f}"

    lines = [
        _loc("📁 *Portfolio Deck*"),
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
    ]
    if mode_label:
        lines.extend([
            f"🌐 *{_loc('Mode:')}* {escape_md(mode_label)}",
            "",
        ])
    lines.extend([
        f"📌 *{_loc('Open Positions:')}* {escape_md(str(len(positions or [])))}",
        f"📬 *{_loc('Open Orders:')}* {escape_md(str(len(open_orders)))}",
        f"💎 *{_loc('Position Value:')}* {escape_md(f'${position_value:,.2f}')}",
        f"{upnl_emoji} *{_loc('Unrealized PnL:')}* {escape_md(upnl_str)}",
    ])

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

    lines.extend(["", f"*{_loc('Open Orders')}*"])
    if not open_orders:
        lines.append(_loc("No open orders right now\\."))
    else:
        for order in open_orders[:5]:
            o_type = str(order.get("type") or "LIMIT").upper()
            side = str(order.get("side") or "?").upper()
            product = str(order.get("product") or "?")
            size = float(order.get("size") or 0)
            limit_price = float(order.get("limit_price") or 0)
            created = str(order.get("created_at") or "")[:16]
            status = str(order.get("status") or "pending")
            filled_size = float(order.get("filled_size") or 0)
            requested_size = float(order.get("requested_size") or size)
            limit_price_str = f"${fmt_price(limit_price, product.replace('-PERP', ''))}"
            fill_progress = ""
            if requested_size > 0 and filled_size > 0:
                fill_progress = f" \\| {_loc('Filled')}: {escape_md(f'{filled_size:.4f}/{requested_size:.4f}')}"
            lines.append(
                f"• {escape_md(o_type)} \\| {escape_md(side)} \\| {escape_md(product)} \\| "
                f"{_loc('Size')}: {escape_md(f'{size:.4f}')} \\| {_loc('Limit')}: {escape_md(limit_price_str)}"
            )
            lines.append(
                f"  {_loc('Status')}: *{escape_md(status)}*{fill_progress} \\| {_loc('Created')}: {escape_md(created if created else '—')}"
            )
        if len(open_orders) > 5:
            lines.append(f"• \\.\\.\\. {_loc('and')} {escape_md(str(len(open_orders) - 5))} {_loc('more')}")

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


def fmt_trade_history(trades, page=0, page_size=10, mode_label: str | None = None):
    start = page * page_size
    page_trades = trades[start:start + page_size]

    lines = [
        _loc("📜 *Trade History*"),
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
    ]
    if mode_label:
        lines.extend(["", f"🌐 *{_loc('Mode:')}* {escape_md(mode_label)}"])

    if not page_trades:
        lines.append("")
        lines.append(_loc("No trade history yet\\."))
        return "\n".join(lines)

    total = len(trades)
    lines.append(f"*{_loc('Showing')}* {escape_md(str(start + 1))}\\-{escape_md(str(min(start + page_size, total)))} {_loc('of')} {escape_md(str(total))}")
    lines.append("")

    for t in page_trades:
        product = t.get("product", "???")
        side = (t.get("side") or "???").upper()
        side_emoji = "🟢" if side == "LONG" else "🔴"
        status = (t.get("status") or "???").upper()
        price = t.get("price")
        close_price = t.get("close_price")
        pnl = t.get("pnl")
        created = t.get("created_at", "")[:16]

        price_str = fmt_price(float(price), product.replace("-PERP", "")) if price else "N/A"
        close_str = fmt_price(float(close_price), product.replace("-PERP", "")) if close_price else "—"

        pnl_str = "—"
        if pnl is not None:
            pnl_val = float(pnl)
            pnl_str = f"+${pnl_val:,.2f}" if pnl_val >= 0 else f"-${abs(pnl_val):,.2f}"

        lines.append(f"{side_emoji} *{escape_md(side)}* {escape_md(product)} \\| {escape_md(status)}")
        lines.append(f"   {_loc('Entry:')} {escape_md(price_str)} → {_loc('Exit:')} {escape_md(close_str)}")
        lines.append(f"   {_loc('PnL:')} {escape_md(pnl_str)} \\| {escape_md(created)}")
        lines.append("")

    return "\n".join(lines)


def fmt_analytics(stats, mode_label: str | None = None):
    lines = [
        _loc("📊 *Trading Analytics*"),
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        "",
    ]
    if mode_label:
        lines.extend([f"🌐 *{_loc('Mode:')}* {escape_md(mode_label)}", ""])

    total_trades = int(stats.get("total_trades", 0) or 0)
    if total_trades == 0:
        lines.append(_loc("No trades recorded yet\\."))
        return "\n".join(lines)

    filled = int(stats.get("filled", 0) or 0)
    closed = int(stats.get("closed", 0) or 0)
    failed = int(stats.get("failed", 0) or 0)
    wins = int(stats.get("wins", 0) or 0)
    losses = int(stats.get("losses", 0) or 0)
    win_rate = float(stats.get("win_rate", 0) or 0)
    total_pnl = float(stats.get("total_pnl", 0) or 0)
    total_volume = float(stats.get("total_volume", 0) or 0)

    pnl_emoji = "🟢" if total_pnl >= 0 else "🔴"
    pnl_str = f"+${total_pnl:,.2f}" if total_pnl >= 0 else f"-${abs(total_pnl):,.2f}"

    avg_trade = total_volume / max(filled + closed, 1)

    lines.extend([
        f"*{_loc('Performance')}*",
        f"{pnl_emoji} *{_loc('Total PnL:')}* {escape_md(pnl_str)}",
        f"🏆 *{_loc('Win Rate:')}* {escape_md(f'{win_rate:.1f}%')}",
        f"✅ *{_loc('Wins:')}* {escape_md(str(wins))} \\| ❌ *{_loc('Losses:')}* {escape_md(str(losses))}",
        "",
        f"*{_loc('Volume')}*",
        f"💰 *{_loc('Total Volume:')}* {escape_md(f'${total_volume:,.2f}')}",
        f"📏 *{_loc('Avg Trade Size:')}* {escape_md(f'${avg_trade:,.2f}')}",
        "",
        f"*{_loc('Trades')}*",
        f"📋 *{_loc('Total:')}* {escape_md(str(total_trades))}",
        f"✅ *{_loc('Filled:')}* {escape_md(str(filled))} \\| 🔒 *{_loc('Closed:')}* {escape_md(str(closed))} \\| ❌ *{_loc('Failed:')}* {escape_md(str(failed))}",
    ])

    # Per-product breakdown if available
    by_product = stats.get("by_product")
    if by_product:
        lines.extend(["", f"*{_loc('PnL by Product')}*"])
        for product, data in by_product.items():
            p_pnl = float(data.get("pnl", 0))
            p_emoji = "🟢" if p_pnl >= 0 else "🔴"
            p_str = f"+${p_pnl:,.2f}" if p_pnl >= 0 else f"-${abs(p_pnl):,.2f}"
            p_count = int(data.get("count", 0))
            lines.append(f"{p_emoji} {escape_md(product)}: {escape_md(p_str)} \\({escape_md(str(p_count))} {_loc('trades')}\\)")

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
        "📖 *Nadobro Guide*\n"
        + escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━") + "\n"
        "\n"
        "*Available Commands:*\n"
        "/start \\- Open the home dashboard\n"
        "/help \\- Show commands, modules, and examples\n"
        "/status \\- View runtime health, setup, and strategy status\n"
        "/ops \\- View order flow and runtime diagnostics\n"
        "/revoke \\- Show 1CT signer revoke steps\n"
        "/stop\\_all \\- Stop all running strategy loops\n"
        "\n"
        "*Core Modules:*\n"
        "\n"
        "💼 *Wallet Vault*\n"
        "Link your wallet with the secure 1CT flow, check balances, and manage signer access\\.\n"
        "\n"
        "🤖 *Trading Console*\n"
        "Place market or limit orders from the guided flow or with plain\\-language trade commands\\.\n"
        "\n"
        "🧠 *Strategy Lab*\n"
        "Configure and run automated strategies including GRID, Reverse GRID, Delta Neutral, Volume, and Bro mode\\.\n"
        "Each strategy dashboard includes controls, safety settings, and pre\\-trade context before launch\\.\n"
        "\n"
        "📁 *Portfolio Deck*\n"
        "Refresh open positions, realized and unrealized PnL, trade history, and analytics in one place\\.\n"
        "\n"
        "🏆 *Points And Market Radar*\n"
        "Check points updates, market radar, and LOWIQPTS refresh flows from the same Telegram workspace\\.\n"
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
        "  • `Long BTC 0\\.01 at 5x`\n"
        "  • `Short ETH 0\\.05 limit 2400`\n"
        "  • `Show my portfolio`\n"
        "  • `Show my positions`\n"
        "  • `What is unified margin?`\n"
        "  • `Close all positions`\n"
        "\n"
        "Need support? Ask in chat with the error details and the command or button flow you used\\."
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


def _fmt_age_seconds(ts: float) -> str:
    if not ts:
        return "—"
    try:
        age = max(0, int(time.time() - float(ts)))
        if age < 60:
            return f"{age}s"
        mins, secs = divmod(age, 60)
        if mins < 60:
            return f"{mins}m {secs}s"
        hours, mins = divmod(mins, 60)
        return f"{hours}h {mins}m"
    except Exception:
        return "—"


def fmt_status_overview(status: dict, onboarding: dict):
    running = status.get("running")
    complete = onboarding.get("onboarding_complete")
    mode = onboarding.get("network", "testnet").upper()
    key_ready = onboarding.get("has_key")
    funded = onboarding.get("funded")

    lines = [
        _loc("📡 *Status*"),
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
    other_running = status.get("other_running_networks") or []
    if other_running:
        lines.append(
            f"⚠️ {_loc('Other running network(s)')}: *{escape_md(', '.join(str(n).upper() for n in other_running))}*"
        )
        lines.append("")

    rs = status.get("running_sessions") or []
    if len(rs) > 1:
        lines.append(f"📋 *{_loc('Active sessions (database)')}*")
        for s in rs[:6]:
            st = str(s.get("strategy") or "").upper()
            pn = str(s.get("product_name") or "?").replace("-PERP", "")
            sid = s.get("id")
            tc = int(s.get("total_cycles") or 0)
            lines.append(
                f"• *{escape_md(st)}* · *{escape_md(pn)}* · \\#{escape_md(str(sid))} · "
                f"{_loc('DB cycles')} *{escape_md(str(tc))}*"
            )
        lines.append("")

    if not running:
        lines.append(f"{_loc('Strategy:')} *{_loc('OFF')}*")
        last_action = status.get("last_action")
        if last_action:
            lines.append(f"{_loc('Last:')} {escape_md(_fmt_action_label(last_action))}")
        if status.get("last_error"):
            lines.append(f"{_loc('Note:')} {escape_md(str(status.get('last_error'))[:120])}")
    else:
        strategy = (status.get("strategy") or "").upper()
        product = str(status.get("product", "BTC"))
        runs = status.get("runs", 0)
        interval = status.get("interval_seconds", 0)
        uptime = _fmt_uptime(status.get("started_at"))
        next_in = status.get("next_cycle_in", 0)

        lines.append(f"{_loc('Strategy:')} *{escape_md(strategy)}* · {_loc('ON')}")
        if product != "MULTI":
            lines.append(f"{_loc('Pair:')} *{escape_md(product)}\\-PERP*")
        else:
            lines.append(f"{_loc('Mode:')} *Multi*")
        margin = status.get("notional_usd")
        if margin is not None and strategy in ("MM", "GRID", "DN", "VOL"):
            cyc = status.get("cycle_notional_usd")
            if cyc is not None and strategy == "MM":
                lines.append(
                    f"{_loc('Margin')}: *{escape_md(f'${float(margin):,.0f}')}* \\| "
                    f"{_loc('Per cycle')}: *{escape_md(f'${float(cyc):,.0f}')}*"
                )
            else:
                lines.append(f"{_loc('Margin')}: *{escape_md(f'${float(margin):,.0f}')}*")
        lines.append(f"{_loc('Uptime')} *{escape_md(uptime)}* · {_loc('Cycles')} *{escape_md(str(runs))}*")
        if next_in > 0:
            lines.append(f"{_loc('Next in')}: *{escape_md(str(next_in))}s*")
        else:
            lines.append(f"{_loc('Every')}: *{escape_md(str(interval))}s*")

        last_action = status.get("last_action")
        if last_action:
            detail = status.get("last_action_detail") or ""
            action_text = _fmt_action_label(last_action)
            if detail:
                action_text += f" — {detail[:100]}"
            lines.append(f"{_loc('Last:')} {escape_md(action_text)}")

        if status.get("is_paused"):
            lines.append(f"⚠️ *{_loc('PAUSED')}*: {escape_md(str(status.get('pause_reason') or _loc('Unknown')))}")

        error_streak = status.get("error_streak", 0)
        if error_streak >= 3:
            lines.append(f"⚠️ {escape_md(str(error_streak))} {_loc('errors in a row')}")

        worker_group = status.get("worker_group")
        if worker_group:
            heartbeat = _fmt_age_seconds(float(status.get("worker_last_heartbeat") or 0.0))
            cycle_ms = float(status.get("last_cycle_ms") or 0.0)
            lines.append(
                f"{_loc('Worker')}: *{escape_md(str(worker_group).upper())}* · "
                f"{_loc('hb')}: *{escape_md(heartbeat)}* · "
                f"{escape_md(f'{cycle_ms:.0f}ms')}"
            )
        runtime_diag = status.get("runtime_diagnostics") or {}
        queue_diag = runtime_diag.get("queue") or {}
        strategy_qsize = int(queue_diag.get("strategy_qsize") or 0)
        strategy_qmax = int(queue_diag.get("strategy_qmax") or 0)
        pending_ticks = int(runtime_diag.get("pending_coalesced_ticks") or 0)
        if strategy_qmax > 0 or pending_ticks > 0:
            lines.append(
                f"{_loc('Queue')}: *{escape_md(str(strategy_qsize))}/{escape_md(str(strategy_qmax))}* · "
                f"{_loc('coalesced')}: *{escape_md(str(pending_ticks))}*"
            )

        order_obs = status.get("order_observability") or {}
        if order_obs:
            obs_placed = int(order_obs.get("orders_placed") or 0)
            obs_filled = int(order_obs.get("orders_filled") or 0)
            obs_cancelled = int(order_obs.get("orders_cancelled") or 0)
            obs_cycles = int(order_obs.get("cycles") or 0)
            obs_zero = int(order_obs.get("zero_order_cycles") or 0)
            lines.append(
                f"{_loc('Order Flow')}: "
                f"{_loc('placed')} *{escape_md(str(obs_placed))}* · "
                f"{_loc('filled')} *{escape_md(str(obs_filled))}* · "
                f"{_loc('cancelled')} *{escape_md(str(obs_cancelled))}*"
            )
            if obs_cycles > 0:
                lines.append(
                    f"{_loc('Orderless cycles')}: *{escape_md(str(obs_zero))}/{escape_md(str(obs_cycles))}*"
                )

        if strategy == "DN":
            dn_mode = status.get("dn_mode") or "enter_anyway"
            dn_fr = float(status.get("dn_last_funding_rate") or 0.0)
            dn_unf = int(status.get("dn_unfavorable_count") or 0)
            lines.append(
                f"{_loc('Funding Mode')}: *{escape_md(str(dn_mode).upper())}* \\| "
                f"{_loc('Funding')}: *{escape_md(f'{dn_fr:.6f}')}*"
            )
            lines.append(f"{_loc('Unfavorable Cycles')}: *{escape_md(str(dn_unf))}*")

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
        if strategy == "MM" and maker_fill_ratio is not None:
            cancellation_ratio = status.get("cancellation_ratio")
            lines.append(
                f"{_loc('Quotes')}: "
                f"{escape_md(f'{float(maker_fill_ratio) * 100:.0f}%')} {_loc('fills')} · "
                f"{escape_md(f'{float(cancellation_ratio or 0) * 100:.0f}%')} {_loc('cancels')}"
            )
        if strategy == "RGRID":
            anchor = float(status.get("rgrid_anchor_price") or 0.0)
            buy_exp = float(status.get("rgrid_buy_exposure_price") or 0.0)
            sell_exp = float(status.get("rgrid_sell_exposure_price") or 0.0)
            drift_pct = float(status.get("rgrid_drift_from_anchor_pct") or 0.0)
            reset_active = bool(status.get("rgrid_reset_active"))
            reset_side = str(status.get("rgrid_reset_side") or "none").upper()
            cycle_pnl = float(status.get("rgrid_last_cycle_pnl_usd") or 0.0)
            sl_pct = float(status.get("rgrid_stop_loss_pct") or 0.0)
            tp_pct = float(status.get("rgrid_take_profit_pct") or 0.0)
            reset_threshold = float(status.get("rgrid_reset_threshold_pct") or 0.0)
            reset_timeout = int(float(status.get("rgrid_reset_timeout_seconds") or 0.0))
            discretion = float(status.get("rgrid_discretion") or 0.0)
            pnl_sign = "+" if cycle_pnl >= 0 else ""
            lines.append("")
            lines.append("*Reverse GRID Telemetry*")
            lines.append(
                f"Anchor: *{escape_md(f'{anchor:,.6f}') if anchor > 0 else 'n/a'}* \\| "
                f"Drift: *{escape_md(f'{drift_pct:.3f}%')}*"
            )
            lines.append(
                f"Exposure VWAP: Buy *{escape_md(f'{buy_exp:,.6f}') if buy_exp > 0 else 'n/a'}* · "
                f"Sell *{escape_md(f'{sell_exp:,.6f}') if sell_exp > 0 else 'n/a'}*"
            )
            lines.append(
                f"Soft Reset: *{escape_md('ON' if reset_active else 'OFF')}* \\| "
                f"Side *{escape_md(reset_side)}* \\| "
                f"Threshold *{escape_md(f'{reset_threshold:.2f}%')}* \\| "
                f"Timeout *{escape_md(f'{reset_timeout}s' if reset_timeout > 0 else 'n/a')}*"
            )
            lines.append(
                f"PnL Cycle: *{escape_md(f'{pnl_sign}${cycle_pnl:,.2f}')}* \\| "
                f"SL/TP: *{escape_md(f'{sl_pct:.2f}%/{tp_pct:.2f}%')}* \\| "
                f"Discretion: *{escape_md(f'{discretion:.2f}')}*"
            )

    return "\n".join(lines)


def fmt_ops_overview(status: dict, ops: dict) -> str:
    runtime_diag = status.get("runtime_diagnostics") or {}
    queue_diag = runtime_diag.get("queue") or {}
    order_obs = status.get("order_observability") or {}

    strategy = str(status.get("strategy") or "none").upper()
    running = bool(status.get("running"))

    lines = [
        "🧪 *Ops Snapshot*",
        escape_md("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"),
        f"{_loc('Strategy')}: *{escape_md(strategy)}* · {'ON' if running else 'OFF'}",
    ]

    lines.append(
        f"{_loc('Queue')}: *{escape_md(str(int(queue_diag.get('strategy_qsize') or 0)))}*"
        f"/{escape_md(str(int(queue_diag.get('strategy_qmax') or 0)))} · "
        f"{_loc('coalesced')}: *{escape_md(str(int(runtime_diag.get('pending_coalesced_ticks') or 0)))}*"
    )

    if order_obs:
        lines.append(
            f"{_loc('Order Flow')}: {_loc('placed')} *{escape_md(str(int(order_obs.get('orders_placed') or 0)))}* · "
            f"{_loc('filled')} *{escape_md(str(int(order_obs.get('orders_filled') or 0)))}* · "
            f"{_loc('cancelled')} *{escape_md(str(int(order_obs.get('orders_cancelled') or 0)))}*"
        )
        lines.append(
            f"{_loc('Cycles')}: *{escape_md(str(int(order_obs.get('ok_cycles') or 0)))}* ok / "
            f"*{escape_md(str(int(order_obs.get('failed_cycles') or 0)))}* failed / "
            f"*{escape_md(str(int(order_obs.get('zero_order_cycles') or 0)))}* orderless"
        )
        last_reason = str(order_obs.get("last_reason") or "").strip()
        if last_reason:
            lines.append(f"{_loc('Last reason')}: {escape_md(last_reason[:160])}")

    queue_stats = queue_diag.get("stats") or {}
    strategy_workers_running = int(queue_diag.get("strategy_workers_running") or 0)
    strategy_workers_target = int(queue_diag.get("strategy_workers_target") or 0)
    alert_workers_running = int(queue_diag.get("alert_workers_running") or 0)
    alert_workers_target = int(queue_diag.get("alert_workers_target") or 0)
    lines.append(
        f"{_loc('Workers')}: strategy *{escape_md(str(strategy_workers_running))}/{escape_md(str(strategy_workers_target))}* · "
        f"alert *{escape_md(str(alert_workers_running))}/{escape_md(str(alert_workers_target))}*"
    )
    lines.append(
        f"{_loc('Queue stats')}: enq *{escape_md(str(int(queue_stats.get('strategy_enqueued') or 0)))}* · "
        f"dedup *{escape_md(str(int(queue_stats.get('strategy_deduped') or 0)))}* · "
        f"drop *{escape_md(str(int(queue_stats.get('strategy_dropped') or 0)))}*"
    )

    perf = ops.get("perf") or {}
    if perf:
        lines.append(
            f"{_loc('Perf')}: active *{escape_md(str(int(perf.get('active_timers') or 0)))}* · "
            f"totals *{escape_md(str(int(perf.get('total_records') or 0)))}*"
        )

    return "\n".join(lines)


def fmt_strategy_update(strategy: str, network: str, conf: dict) -> str:
    notional = float(conf.get("notional_usd", 100.0))
    spread_bp = float(conf.get("spread_bp", 5.0))
    interval_seconds = int(conf.get("interval_seconds", 60))
    tp_pct = float(conf.get("tp_pct", 1.0))
    sl_pct = float(conf.get("sl_pct", 0.5))
    return (
        f"✅ *{escape_md(strategy.upper())} {_loc('updated')}* \\({escape_md(network.upper())}\\)\n\n"
        f"{_loc('Margin')}: {escape_md(f'${notional:,.2f}')}\n"
        f"{_loc('Spread')}: {escape_md(f'{spread_bp:.1f} bp')}\n"
        f"{_loc('Interval')}: {escape_md(f'{interval_seconds}s')}\n"
        f"{_loc('TP')}: {escape_md(f'{tp_pct:.2f}%')}\n"
        f"{_loc('SL')}: {escape_md(f'{sl_pct:.2f}%')}"
    )
