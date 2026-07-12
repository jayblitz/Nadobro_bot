"""Copy-trading callback handlers (copy:* callback data).

Extracted from callbacks.py (decomposition slice, 2026-06). May import
shared utils from callbacks at module level — callbacks only imports this
module lazily inside its _handle_copy shim, so there is no cycle.
"""
from __future__ import annotations

import logging

from src.nadobro.handlers.formatters import escape_md
from src.nadobro.handlers.keyboards import back_kb, copy_hub_kb, copy_trader_preview_kb, copy_budget_kb, copy_risk_kb, copy_leverage_kb, copy_confirm_kb, copy_dashboard_kb, copy_admin_menu_kb
from src.nadobro.i18n import localize_text, get_active_language
from src.nadobro.services.admin_service import is_admin
from src.nadobro.core.async_utils import run_blocking
from src.nadobro.services.onboarding_service import is_new_onboarding_complete
from src.nadobro.services.user_service import get_user, ensure_active_wallet_ready
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from src.nadobro.handlers.callbacks import _edit_loc  # noqa: E402

logger = logging.getLogger(__name__)


async def _handle_copy(query, data, context, telegram_id):
    from src.nadobro.services.copy_service import (
        get_available_traders, get_user_copies, start_copy, stop_copy,
        pause_copy, resume_copy, get_trader_stats, get_trader_preview,
    )
    from src.nadobro.services.admin_service import (
        add_copy_trader, remove_copy_trader, list_copy_traders,
    )

    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "hub":
        traders = await run_blocking(get_available_traders, telegram_id)
        admin_flag = is_admin(telegram_id)
        if traders:
            lang = get_active_language()
            l_pnl = localize_text("PnL", lang)
            l_vol = localize_text("Vol", lang)
            l_wr = localize_text("WR", lang)
            l_trades = localize_text("Trades", lang)
            lines = [localize_text("🔁 *Copy Trading*\n", lang)]
            for t in traders:
                curated = " ⭐" if t.get("is_curated") else ""
                wallet_snip = t["wallet"][:6] + "\\.\\.\\." + t["wallet"][-4:]
                stats = await run_blocking(get_trader_stats, t["id"])
                vol_str = f"${stats['volume_usd']:,.0f}" if stats["volume_usd"] else "\\-"
                wr_str = f"{stats['win_rate']:.0f}%" if stats["total_trades"] > 0 else "\\-"
                pnl = stats["pnl_usd"]
                pnl_str = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"
                lines.append(
                    f"• *{escape_md(t['label'])}*{curated} · `{wallet_snip}`\n"
                    f"  {l_pnl}: *{escape_md(pnl_str)}* \\| {l_vol}: {escape_md(vol_str)} \\| {l_wr}: {escape_md(wr_str)} \\| {l_trades}: {stats['total_trades']}"
                )
            lines.append("\n" + localize_text("Select a trader to view details and start copying\\.", lang))
        else:
            lines = ["🔁 *Copy Trading*\n", "No traders available yet\\. Add a custom wallet or ask an admin to add traders\\."]
        await _edit_loc(query,
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_hub_kb(traders, is_admin_user=admin_flag),
        )

    elif action == "trader" and len(parts) >= 3:
        trader_id = int(parts[2])
        traders = await run_blocking(get_available_traders, telegram_id)
        trader = next((t for t in traders if t["id"] == trader_id), None)
        if not trader:
            await _edit_loc(query, "⚠️ Trader not found\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return

        user = get_user(telegram_id)
        trader_network = user.network_mode.value if user else "mainnet"
        preview = await run_blocking(get_trader_preview, trader_id, trader_network, telegram_id)
        if not preview.get("found"):
            await _edit_loc(query, "⚠️ Trader not found\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return
        equity = float(preview.get("equity_usd") or 0.0)
        equity_str = f"${equity:,.0f}" if equity > 0 else "N/A"
        curated = " ⭐ Curated" if trader.get("is_curated") else ""
        open_count = int(preview.get("open_positions") or 0)

        stats = await run_blocking(get_trader_stats, trader_id)
        vol_str = f"${stats['volume_usd']:,.0f}" if stats["volume_usd"] else "N/A"
        wr_str = f"{stats['win_rate']:.0f}%" if stats["total_trades"] > 0 else "N/A"
        pnl_preview = stats["pnl_usd"]
        pnl_preview_str = f"+${pnl_preview:,.2f}" if pnl_preview >= 0 else f"-${abs(pnl_preview):,.2f}"

        wallet_snip = trader["wallet"][:6] + "..." + trader["wallet"][-4:]

        await _edit_loc(query,
            "🔁 *Trader Preview*{curated}\n\nLabel: *{label}*\nWallet: `{wallet}`\nEquity: *{equity}*\nOpen Positions: *{positions}*\nPnL: *{pnl}*\nVolume: *{volume}*\nWin Rate: *{winrate}* \\({filled} filled / {total} total\\)\n\nTap Start Copying to set your budget and risk parameters\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_trader_preview_kb(trader_id),
            curated=escape_md(curated), label=escape_md(trader['label']),
            wallet=escape_md(wallet_snip), equity=escape_md(equity_str),
            positions=open_count, pnl=escape_md(pnl_preview_str),
            volume=escape_md(vol_str), winrate=escape_md(wr_str),
            filled=stats['filled'], total=stats['total_trades'],
        )

    elif action == "start" and len(parts) >= 3:
        trader_id = int(parts[2])
        context.user_data["copy_setup"] = {"trader_id": trader_id, "step": "budget"}
        await _edit_loc(query,
            "💰 *Set Copy Budget*\n\nHow much USD to allocate for copy trading this trader?",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_budget_kb(),
        )

    elif action == "budget" and len(parts) >= 3:
        setup = context.user_data.get("copy_setup")
        if not setup:
            await _edit_loc(query, "⚠️ No setup in progress\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return
        setup["budget_usd"] = float(parts[2])
        setup["step"] = "risk"
        await _edit_loc(query,
            "⚖️ *Set Risk Factor*\n\nBudget: *${budget}*\n\nRisk factor scales the per\\-trade budget slice inside your total copy allocation\\. Higher values copy more aggressively\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_risk_kb(),
            budget=f"{setup['budget_usd']:.0f}",
        )

    elif action == "risk" and len(parts) >= 3:
        setup = context.user_data.get("copy_setup")
        if not setup:
            await _edit_loc(query, "⚠️ No setup in progress\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return
        setup["risk_factor"] = float(parts[2])
        setup["step"] = "leverage"
        await _edit_loc(query,
            "📐 *Set Max Leverage*\n\nBudget: *${budget}* \\| Risk: *{risk}x*\n\nSet the maximum leverage cap for copied trades\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_leverage_kb(),
            budget=f"{setup['budget_usd']:.0f}", risk=setup['risk_factor'],
        )

    elif action == "lev" and len(parts) >= 3:
        setup = context.user_data.get("copy_setup")
        if not setup:
            await _edit_loc(query, "⚠️ No setup in progress\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return
        setup["max_leverage"] = float(parts[2])
        setup["step"] = "cumulative_sl"
        from src.nadobro.handlers.keyboards import copy_cumulative_sl_kb
        await _edit_loc(query,
            "🛡 *Cumulative Stop Loss*\n\nBudget: *${budget}* \\| Risk: *{risk}x* \\| Leverage: *{leverage}x*\n\nSet a cumulative loss limit \\(% of budget\\)\\. Copying stops if total losses hit this threshold\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_cumulative_sl_kb(),
            budget=f"{setup['budget_usd']:.0f}",
            risk=setup['risk_factor'],
            leverage=f"{setup['max_leverage']:.0f}",
        )

    elif action == "csl" and len(parts) >= 3:
        setup = context.user_data.get("copy_setup")
        if not setup:
            await _edit_loc(query, "⚠️ No setup in progress\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return
        csl_pct = float(parts[2])
        setup["cumulative_stop_loss_pct"] = csl_pct if csl_pct > 0 else None
        setup["step"] = "cumulative_tp"
        from src.nadobro.handlers.keyboards import copy_cumulative_tp_kb
        sl_label = f"{csl_pct:.0f}%" if csl_pct > 0 else "None"
        await _edit_loc(query,
            "🎯 *Cumulative Take Profit*\n\nBudget: *${budget}* \\| SL: *{sl}*\n\nSet a cumulative profit target \\(% of budget\\)\\. Copying stops when total profits hit this threshold\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_cumulative_tp_kb(),
            budget=f"{setup['budget_usd']:.0f}",
            sl=escape_md(sl_label),
        )

    elif action == "ctp" and len(parts) >= 3:
        setup = context.user_data.get("copy_setup")
        if not setup:
            await _edit_loc(query, "⚠️ No setup in progress\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return
        ctp_pct = float(parts[2])
        setup["cumulative_take_profit_pct"] = ctp_pct if ctp_pct > 0 else None
        setup["step"] = "confirm"

        traders = await run_blocking(get_available_traders, telegram_id)
        trader = next((t for t in traders if t["id"] == setup["trader_id"]), None)
        trader_label = trader["label"] if trader else "Unknown"

        csl = setup.get("cumulative_stop_loss_pct")
        ctp = setup.get("cumulative_take_profit_pct")
        sl_str = f"{csl:.0f}%" if csl else "None"
        tp_str = f"{ctp:.0f}%" if ctp else "None"

        await _edit_loc(query,
            "✅ *Confirm Copy Setup*\n\nTrader: *{trader}*\nBudget: *${budget}*\nRisk Factor: *{risk}x*\nMax Leverage: *{leverage}x*\nCumulative SL: *{sl}*\nCumulative TP: *{tp}*\n\nReady to start?",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_confirm_kb(),
            trader=escape_md(trader_label),
            budget=f"{setup['budget_usd']:.0f}",
            risk=setup['risk_factor'],
            leverage=f"{setup['max_leverage']:.0f}",
            sl=escape_md(sl_str),
            tp=escape_md(tp_str),
        )

    elif action == "confirm":
        setup = context.user_data.get("copy_setup")
        if not setup:
            await _edit_loc(query, "⚠️ No setup to confirm\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return
        if not is_new_onboarding_complete(telegram_id):
            await _edit_loc(query,
                "⚠️ Complete setup first (language + accept terms).",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶ Complete setup", callback_data="onboarding:resume")],
                    [InlineKeyboardButton("Exit", callback_data="nav:main")],
                ]),
            )
            return

        wallet_ready, wallet_msg = ensure_active_wallet_ready(telegram_id)
        if not wallet_ready:
            await _edit_loc(query,
                f"⚠️ {escape_md(wallet_msg)}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(),
            )
            return
        context.user_data.pop("copy_setup", None)

        from src.nadobro.handlers.messages import execute_action_directly
        action_data = {
            "type": "start_copy",
            "trader_id": setup["trader_id"],
            "budget_usd": setup["budget_usd"],
            "risk_factor": setup["risk_factor"],
            "max_leverage": setup["max_leverage"],
        }
        if setup.get("cumulative_stop_loss_pct"):
            action_data["cumulative_stop_loss_pct"] = setup["cumulative_stop_loss_pct"]
        if setup.get("cumulative_take_profit_pct"):
            action_data["cumulative_take_profit_pct"] = setup["cumulative_take_profit_pct"]
        await execute_action_directly(query, context, telegram_id, action_data)

    elif action == "pause" and len(parts) >= 3:
        mirror_id = int(parts[2])
        ok, msg = await run_blocking(pause_copy, telegram_id, mirror_id)
        prefix = "⏸" if ok else "⚠️"
        mirrors = await run_blocking(get_user_copies, telegram_id)
        await _edit_loc(query,
            f"{prefix} {escape_md(msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_dashboard_kb(mirrors, lang=get_active_language()),
        )

    elif action == "resume" and len(parts) >= 3:
        mirror_id = int(parts[2])
        ok, msg = await run_blocking(resume_copy, telegram_id, mirror_id)
        prefix = "▶" if ok else "⚠️"
        mirrors = await run_blocking(get_user_copies, telegram_id)
        await _edit_loc(query,
            f"{prefix} {escape_md(msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_dashboard_kb(mirrors, lang=get_active_language()),
        )

    elif action == "stop" and len(parts) >= 3:
        mirror_id = int(parts[2])
        ok, msg = await run_blocking(stop_copy, telegram_id, mirror_id)
        prefix = "✅" if ok else "⚠️"
        mirrors = await run_blocking(get_user_copies, telegram_id)
        await _edit_loc(query,
            f"{prefix} {escape_md(msg)}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_dashboard_kb(mirrors, lang=get_active_language()),
        )

    elif action == "dashboard":
        mirrors = await run_blocking(get_user_copies, telegram_id)
        lang = get_active_language()
        if mirrors:
            l_alloc = localize_text("Allocated", lang)
            l_margin = localize_text("Margin/Trade", lang)
            l_lev = localize_text("Lev", lang)
            l_positions = localize_text("Open Positions", lang)
            l_pnl = localize_text("PnL", lang)
            lines = [localize_text("📋 *My Copy Trades*\n", lang)]
            for m in mirrors:
                status_label = localize_text("PAUSED", lang) if m.get("paused") else localize_text("ACTIVE", lang)
                status_icon = f"⏸ {status_label}" if m.get("paused") else f"🟢 {status_label}"
                # Mirrors keep running on their own network across a mode
                # switch — tag each row so testnet and mainnet mirrors are
                # never mistaken for one another on this shared dashboard.
                net_tag = "🧪 TESTNET" if str(m.get("network", "mainnet")).lower() == "testnet" else "🌐 MAINNET"
                pnl = float(m.get("cumulative_pnl", 0) or 0)
                pnl_str = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"
                pnl_str = escape_md(pnl_str)
                lines.append(
                    f"• *{escape_md(m['trader_label'])}* · {status_icon} · {escape_md(net_tag)}\n"
                    f"  {l_alloc}: ${float(m.get('total_allocated_usd', 0) or 0):.0f} \\| "
                    f"{l_margin}: ${float(m.get('margin_per_trade', 0) or 0):.0f} \\| {l_lev}: {float(m.get('max_leverage', 0) or 0):.0f}x\n"
                    f"  {l_positions}: {int(m.get('open_positions', 0) or 0)}\n"
                    f"  {l_pnl}: *{pnl_str}*"
                )
        else:
            lines = [localize_text("📋 *My Copy Trades*\n", lang), localize_text("You have no active copy mirrors\\.", lang)]
        await _edit_loc(query,
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=copy_dashboard_kb(mirrors, lang=get_active_language()),
        )

    elif action == "add_custom":
        context.user_data["pending_copy_wallet"] = True
        await _edit_loc(query,
            "➕ *Add Custom Wallet*\n\nSend the Ethereum wallet address \\(0x\\.\\.\\.\\) of the trader you want to copy\\.\n\nThe address must be 42 characters starting with `0x`\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="copy:hub")],
            ]),
        )

    elif action == "admin" and len(parts) >= 3:
        if not is_admin(telegram_id):
            await _edit_loc(query, "⚠️ Admin access required\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=back_kb())
            return

        sub = parts[2]
        if sub == "menu":
            traders = await run_blocking(list_copy_traders)
            await _edit_loc(query,
                "⚙️ *Manage Copy Traders*\n\nAdd or remove traders from the copy trading pool\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=copy_admin_menu_kb(traders, lang=get_active_language()),
            )
        elif sub == "add":
            context.user_data["pending_admin_copy_wallet"] = True
            await _edit_loc(query,
                "➕ *Add Trader*\n\nSend the Ethereum wallet address \\(0x\\.\\.\\.\\) and an optional label separated by a space\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel", callback_data="copy:admin:menu")],
                ]),
            )
        elif sub == "remove" and len(parts) >= 4:
            trader_id = int(parts[3])
            ok, msg = await run_blocking(remove_copy_trader, telegram_id, trader_id)
            prefix = "✅" if ok else "⚠️"
            traders = await run_blocking(list_copy_traders)
            await _edit_loc(query,
                f"{prefix} {escape_md(msg)}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=copy_admin_menu_kb(traders, lang=get_active_language()),
            )
