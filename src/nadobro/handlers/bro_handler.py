"""Trading-Bro AI callback handlers (bro:* callback data).

Extracted from callbacks.py (decomposition slice, 2026-06). May import
shared utils from callbacks at module level — callbacks only imports this
module lazily inside its _handle_bro shim, so there is no cycle.
"""
from __future__ import annotations

import logging

from src.nadobro.config import get_perp_products, get_product_id
from src.nadobro.handlers.formatters import escape_md
from src.nadobro.handlers.keyboards import back_kb
from src.nadobro.core.async_utils import run_blocking
from src.nadobro.strategy.bot_runtime import get_user_bot_status
from src.nadobro.services.settings_service import get_user_settings, update_user_settings
from src.nadobro.services.user_service import get_user
from telegram.constants import ParseMode

from src.nadobro.handlers.callbacks import _edit_loc  # noqa: E402

logger = logging.getLogger(__name__)


async def _handle_bro(query, data, telegram_id, context):
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "config":
        network, settings = get_user_settings(telegram_id)
        conf = settings.get("strategies", {}).get("bro", {})
        context.user_data.pop("bro_config_section", None)
        b_budget = float(conf.get("budget_usd", 500))
        b_risk = conf.get("risk_level", "balanced").upper()
        b_conf_val = float(conf.get("min_confidence", 0.65))
        b_lev = int(conf.get("leverage_cap", 5))
        b_tp = float(conf.get("tp_pct", 2.0))
        b_sl = float(conf.get("sl_pct", 1.5))
        b_maxp = int(conf.get("max_positions", 3))
        b_maxl = float(conf.get("max_loss_pct", 15))
        b_profile = conf.get("bro_profile", "normal").upper()
        profile_emoji = {"CHILL": "😎", "NORMAL": "🤙", "DEGEN": "🔥"}.get(b_profile, "🤙")
        text = (
            "⚙️ *Alpha Agent · Advanced*\n\n"
            f"Preset: {profile_emoji} *{escape_md(b_profile)}*\n"
            f"Budget: *{escape_md(f'${b_budget:,.0f}')}* \\| Risk style: *{escape_md(b_risk)}*\n"
            f"Confidence: *{escape_md(f'{b_conf_val:.0%}')}* \\| Max leverage: *{escape_md(f'{b_lev}x')}*\n"
            f"TP/SL: *{escape_md(f'{b_tp:.1f}%/{b_sl:.1f}%')}* \\| Max positions: *{escape_md(str(b_maxp))}*\n"
            f"Max loss: *{escape_md(f'{b_maxl:.0f}%')}*\n\n"
            "Choose one section below to keep setup simple\\."
        )
        from src.nadobro.handlers.keyboards import bro_config_menu_kb
        await _edit_loc(query, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=bro_config_menu_kb())

    elif action == "config_section" and len(parts) >= 3:
        from src.nadobro.handlers.keyboards import bro_config_section_kb
        section = parts[2]
        if section not in {"preset", "risk", "exits", "risk_style"}:
            return
        context.user_data["bro_config_section"] = section
        _network, settings = get_user_settings(telegram_id)
        conf = settings.get("strategies", {}).get("bro", {})
        if section == "preset":
            text = (
                "⚙️ *Alpha Agent · Preset*\n\n"
                "Pick a personality preset to apply a ready-made risk profile\\."
            )
        elif section == "risk_style":
            text = (
                "⚙️ *Alpha Agent · Risk Style*\n\n"
                f"Current style: *{escape_md(str(conf.get('risk_level', 'balanced')).upper())}*\n\n"
                "Choose how aggressive the AI should trade\\."
            )
        elif section == "risk":
            budget_str = f"${float(conf.get('budget_usd', 500)):,.0f}"
            confidence_str = f"{float(conf.get('min_confidence', 0.65)):.0%}"
            max_leverage_str = f"{int(conf.get('leverage_cap', 5))}x"
            max_positions_str = str(int(conf.get("max_positions", 3)))
            text = (
                "⚙️ *Alpha Agent · Risk*\n\n"
                f"Budget: *{escape_md(budget_str)}* \\| "
                f"Confidence: *{escape_md(confidence_str)}*\n"
                f"Max leverage: *{escape_md(max_leverage_str)}* \\| "
                f"Max positions: *{escape_md(max_positions_str)}*\n\n"
                "Tune the core risk controls here\\."
            )
        else:
            tp_sl_str = f"{float(conf.get('tp_pct', 2.0)):.1f}% / {float(conf.get('sl_pct', 1.5)):.1f}%"
            text = (
                "⚙️ *Alpha Agent · Exits*\n\n"
                f"Current TP/SL: *{escape_md(tp_sl_str)}*\n\n"
                "Set how Alpha Agent locks profit and cuts risk\\."
            )
        await _edit_loc(query, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=bro_config_section_kb(section))

    elif action == "risk" and len(parts) >= 3:
        profile = parts[2]
        presets = {
            "conservative": {"risk_level": "conservative", "leverage_cap": 3, "max_positions": 2, "min_confidence": 0.75, "tp_pct": 1.5, "sl_pct": 1.0},
            "balanced": {"risk_level": "balanced", "leverage_cap": 5, "max_positions": 3, "min_confidence": 0.65, "tp_pct": 2.0, "sl_pct": 1.5},
            "aggressive": {"risk_level": "aggressive", "leverage_cap": 10, "max_positions": 4, "min_confidence": 0.55, "tp_pct": 3.0, "sl_pct": 2.0},
        }
        chosen = presets.get(profile)
        if not chosen:
            return
        def _mutate(s):
            strategies = s.setdefault("strategies", {})
            bro = strategies.setdefault("bro", {})
            bro.update(chosen)
        update_user_settings(telegram_id, _mutate)
        from src.nadobro.handlers.keyboards import bro_config_section_kb
        context.user_data["bro_config_section"] = "risk"
        await _edit_loc(query, 
            f"✅ Alpha Agent risk set to *{escape_md(profile.upper())}*\n\n"
            f"Leverage cap: {chosen['leverage_cap']}x \\| Confidence: {chosen['min_confidence']:.0%}\n"
            f"TP/SL: {chosen['tp_pct']:.1f}%/{chosen['sl_pct']:.1f}%\n"
            f"Max positions: {chosen['max_positions']}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=bro_config_section_kb("risk"),
        )

    elif action == "set" and len(parts) >= 3:
        field = parts[2]
        allowed = {"budget_usd", "min_confidence", "leverage_cap", "max_positions", "tp_sl", "risk_level"}
        if field not in allowed:
            return
        if field == "tp_sl":
            context.user_data["pending_bro_input"] = {"field": "tp_sl", "section": "exits"}
            context.user_data["bro_config_section"] = "exits"
            await _edit_loc(query, 
                "✏️ *Set TP/SL*\n\nEnter as `TP,SL` \\(example: `2.0,1.5`\\)",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb("bro:config_section:exits"),
            )
        else:
            section = "risk"
            if field == "risk_level":
                section = "risk_style"
            context.user_data["pending_bro_input"] = {"field": field, "section": section}
            context.user_data["bro_config_section"] = section
            hints = {
                "budget_usd": "Enter budget in USD \\(example: `500`\\)",
                "min_confidence": "Enter min confidence 0\\-1 \\(example: `0.65`\\)",
                "leverage_cap": "Enter max leverage \\(example: `5`\\)",
                "max_positions": "Enter max simultaneous positions \\(example: `3`\\)",
                "risk_level": "Enter: `conservative`, `balanced`, or `aggressive`",
            }
            await _edit_loc(query, 
                f"✏️ *Set {escape_md(field)}*\n\n{hints.get(field, 'Enter value')}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=back_kb(f"bro:config_section:{section}"),
            )

    elif action == "set_text" and len(parts) >= 4:
        field = parts[2]
        raw_value = parts[3]
        if field != "risk_level" or raw_value not in {"conservative", "balanced", "aggressive"}:
            return
        context.user_data["bro_config_section"] = "risk_style"
        def _mutate(s):
            s.setdefault("strategies", {}).setdefault("bro", {})["risk_level"] = raw_value
        update_user_settings(telegram_id, _mutate)
        from src.nadobro.handlers.keyboards import bro_config_section_kb
        await _edit_loc(
            query,
            f"✅ Risk style set to *{escape_md(raw_value.upper())}*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=bro_config_section_kb("risk_style"),
        )

    elif action == "status":
        from src.nadobro.strategy.bot_runtime import get_user_bot_status
        from src.nadobro.trading.budget_guard import get_budget_status
        from src.nadobro.services.settings_service import get_strategy_settings
        bot_status = get_user_bot_status(telegram_id)
        _, bro_conf = get_strategy_settings(telegram_id, "bro")
        bro_settings = {"budget_usd": bro_conf.get("budget_usd", 500), "risk_level": bro_conf.get("risk_level", "balanced"), "max_loss_pct": bro_conf.get("max_loss_pct", 15)}

        is_running = bot_status.get("running") and bot_status.get("strategy") == "bro"
        status_text = "🟢 ACTIVE" if is_running else "⚪ INACTIVE"
        runs = bot_status.get("runs", 0)
        last_error = bot_status.get("last_error", "")

        budget_status = await run_blocking(get_budget_status, telegram_id, bro_settings)
        exposure = budget_status.get("current_exposure", 0)
        copy_exp = budget_status.get("copy_exposure", 0)
        remaining = budget_status.get("remaining_budget", 0)
        positions = budget_status.get("position_count", 0)
        util = budget_status.get("utilization_pct", 0)

        b_profile = bro_conf.get("bro_profile", "normal").upper()
        profile_emoji = {"CHILL": "😎", "NORMAL": "🤙", "DEGEN": "🔥"}.get(b_profile, "🤙")

        text = (
            f"📊 *Alpha Agent Status*\n\n"
            f"Status: {escape_md(status_text)} \\| Profile: {profile_emoji} {escape_md(b_profile)}\n"
            f"Cycles: *{escape_md(str(runs))}*\n"
            f"Exposure: *{escape_md(f'${exposure:,.0f}')}* \\| Copy: *{escape_md(f'${copy_exp:,.0f}')}*\n"
            f"Remaining: *{escape_md(f'${remaining:,.0f}')}* \\| Utilization: *{escape_md(f'{util:.0f}%')}*\n"
            f"Positions: *{escape_md(str(positions))}*\n"
        )
        if last_error:
            text += f"\nLast error: _{escape_md(str(last_error)[:150])}_"
        from src.nadobro.handlers.keyboards import bro_action_kb
        await _edit_loc(query, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=bro_action_kb())

    elif action == "profile" and len(parts) >= 3:
        profile = parts[2]
        if profile not in ("chill", "normal", "degen"):
            return
        from src.nadobro.trading.budget_guard import get_bro_profile, BRO_PROFILES
        profile_data = get_bro_profile(profile)
        emoji_map = {"chill": "😎", "normal": "🤙", "degen": "🔥"}

        def _mutate(s):
            strategies = s.setdefault("strategies", {})
            bro = strategies.setdefault("bro", {})
            bro["bro_profile"] = profile
            bro["risk_level"] = profile_data["risk_level"]
            bro["leverage_cap"] = profile_data["leverage_cap"]
            bro["max_positions"] = profile_data["max_positions"]
            bro["min_confidence"] = profile_data["min_confidence"]
            bro["tp_pct"] = profile_data["tp_pct"]
            bro["sl_pct"] = profile_data["sl_pct"]
            bro["max_loss_pct"] = profile_data["max_loss_pct"]

        update_user_settings(telegram_id, _mutate)
        from src.nadobro.handlers.keyboards import bro_config_section_kb
        context.user_data["bro_config_section"] = "preset"
        await _edit_loc(query,
            f"{emoji_map.get(profile, '🤙')} *Bro Profile: {escape_md(profile.upper())}*\n\n"
            f"_{escape_md(profile_data['description'])}_\n\n"
            f"Leverage: {profile_data['leverage_cap']}x \\| Confidence: {profile_data['min_confidence']:.0%}\n"
            f"TP/SL: {profile_data['tp_pct']:.1f}%/{profile_data['sl_pct']:.1f}%\n"
            f"Max positions: {profile_data['max_positions']} \\| Max loss: {profile_data['max_loss_pct']}%",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=bro_config_section_kb("preset"),
        )

    elif action == "explain":
        from src.nadobro.trading.budget_guard import get_budget_status
        from src.nadobro.services.settings_service import get_strategy_settings
        from src.nadobro.llm.bro_llm import explain_position
        _, bro_conf = get_strategy_settings(telegram_id, "bro")
        bro_settings = {"budget_usd": bro_conf.get("budget_usd", 500), "risk_level": bro_conf.get("risk_level", "balanced"), "max_loss_pct": bro_conf.get("max_loss_pct", 15)}
        budget_status = await run_blocking(get_budget_status, telegram_id, bro_settings)
        positions = budget_status.get("positions", [])
        if not positions:
            from src.nadobro.handlers.keyboards import bro_action_kb
            await _edit_loc(query,
                "🧠 *Why?*\n\nNo open positions to explain\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=bro_action_kb(),
            )
            return

        bot_status = get_user_bot_status(telegram_id)
        bro_state = bot_status.get("bro_state", {}) if bot_status else {}
        trades_log = bro_state.get("trades_log", [])

        explanations = []
        for pos in positions:
            product = pos.get("product", "?")
            side = pos.get("side", "?")
            entry = pos.get("entry_price", 0)
            pnl = pos.get("unrealized_pnl", 0)
            notional = pos.get("notional_usd", 0)

            matching_trade = None
            for t in reversed(trades_log):
                if t.get("product", "").upper() == product.upper() and t.get("side") == side:
                    matching_trade = t
                    break

            reasoning = matching_trade.get("reasoning", "No entry data") if matching_trade else "Opened before current session"
            signals = matching_trade.get("signals", []) if matching_trade else []

            from src.nadobro.services.user_service import get_user_readonly_client as _get_ro
            ro = _get_ro(telegram_id)
            current_price = entry
            if ro:
                try:
                    user = get_user(telegram_id)
                    network = user.network_mode.value if user else "mainnet"
                    pid = get_product_id(product, network=network, client=ro)
                    if pid is not None:
                        mp = ro.get_market_price(pid)
                        current_price = float(mp.get("mid", entry))
                except Exception:
                    pass

            explanation = await run_blocking(
                explain_position,
                product, side, entry, current_price, pnl, reasoning, signals,
            )
            if explanation:
                explanations.append(f"*{escape_md(product)} {escape_md(side.upper())}* \\(${escape_md(f'{notional:.0f}')} PnL=${escape_md(f'{pnl:+.2f}')}\\)\n{escape_md(explanation)}")
            else:
                explanations.append(f"*{escape_md(product)} {escape_md(side.upper())}* \\(${escape_md(f'{notional:.0f}')} PnL=${escape_md(f'{pnl:+.2f}')}\\)\n_{escape_md(reasoning[:150])}_")

        text = "🧠 *Why These Positions?*\n\n" + "\n\n".join(explanations)
        from src.nadobro.handlers.keyboards import bro_action_kb
        await _edit_loc(query, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=bro_action_kb())

    elif action == "gameplan":
        from src.nadobro.trading.budget_guard import get_budget_status
        from src.nadobro.services.settings_service import get_strategy_settings
        from src.nadobro.llm.bro_llm import generate_game_plan
        _, bro_conf = get_strategy_settings(telegram_id, "bro")
        bro_settings = {"budget_usd": bro_conf.get("budget_usd", 500), "risk_level": bro_conf.get("risk_level", "balanced"), "max_loss_pct": bro_conf.get("max_loss_pct", 15)}
        budget_status = await run_blocking(get_budget_status, telegram_id, bro_settings)
        positions = budget_status.get("positions", [])
        remaining = budget_status.get("remaining_budget", 0)
        budget = bro_conf.get("budget_usd", 500)
        bro_profile = bro_conf.get("bro_profile", "normal")

        bot_status = get_user_bot_status(telegram_id)
        bro_state = bot_status.get("bro_state", {}) if bot_status else {}
        decisions_log = bro_state.get("decisions_log", [])

        plan = await run_blocking(
            generate_game_plan,
            bro_conf.get("products", get_perp_products()[:6] or ["BTC", "ETH", "SOL"]),
            budget, remaining, positions, bro_profile, decisions_log,
        )

        if plan:
            text = f"📋 *Bro's 24h Game Plan*\n\n{escape_md(plan)}"
        else:
            text = "📋 *Game Plan*\n\nCouldn't generate a plan right now\\. Try again later\\."
        from src.nadobro.handlers.keyboards import bro_action_kb
        await _edit_loc(query, text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=bro_action_kb())

    elif action == "howl":
        from src.nadobro.llm.howl_service import get_pending_howl, format_howl_message
        from src.nadobro.handlers.keyboards import howl_approval_kb
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "mainnet"
        pending = get_pending_howl(telegram_id, network)
        if pending:
            text = format_howl_message(pending)
            suggestions = pending.get("suggestions", [])
            pending_count = sum(1 for s in suggestions if s.get("status", "pending") == "pending")
            # format_howl_message is HTML — send it as such, never MD-escaped.
            await _edit_loc(query,
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=howl_approval_kb(len(suggestions)) if pending_count > 0 else back_kb("strategy:preview:bro"),
            )
        else:
            from src.nadobro.handlers.keyboards import bro_action_kb
            await _edit_loc(query, 
                "🐺 *HOWL*\n\nNo pending optimization suggestions\\.\nHOWL runs nightly and will notify you when it has suggestions\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=bro_action_kb(),
            )
