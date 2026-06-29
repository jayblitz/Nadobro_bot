"""Strategy-domain callback handlers (extracted from callbacks.py, 2026-06).

Owns the strategy menu, per-strategy config sections, previews, and the
start/stop flow dispatched from ``strategy:*`` callback data. Shared UI
utilities (``_edit_loc``, ``_get_user_settings``) and cross-domain
navigation (``_handle_nav``) remain in ``callbacks.py``; this module may
import from callbacks at module level because callbacks only imports this
module lazily (inside its ``_handle_strategy`` shim), so there is no cycle.
"""
from __future__ import annotations

import logging

from src.nadobro.config import get_dn_pair, get_dn_products, get_perp_products, get_product_id, get_product_max_leverage, get_spot_product_id, list_volume_spot_product_names, normalize_volume_spot_symbol
from src.nadobro.handlers.commands import build_status_dashboard_parts
from src.nadobro.handlers.formatters import escape_md, fmt_price
from src.nadobro.handlers.keyboards import back_kb, dn_funding_rates_kb, strategy_action_kb, strategy_product_picker_kb
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.bot_runtime import stop_user_bot, get_user_bot_status
from src.nadobro.services.onboarding_service import is_new_onboarding_complete
from src.nadobro.services.perf import timed_metric
from src.nadobro.services.settings_service import get_user_settings, update_user_settings
from src.nadobro.services.strategy_pending_input import persist_strategy_pending_input
from src.nadobro.services.user_service import get_user_readonly_client, get_user_wallet_info, get_user, ensure_active_wallet_ready
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
import time

from src.nadobro.handlers.callbacks import (  # noqa: E402
    _edit_loc,
    _get_user_settings,
    _handle_nav,
)

logger = logging.getLogger(__name__)


def _strategy_available_products(strategy_id: str, network: str, vol_market: str | None = None) -> tuple[str, ...]:
    sid = str(strategy_id or "").lower().strip()
    if sid == "dn":
        return tuple(get_dn_products(network=network) or ("BTC", "ETH"))
    if sid == "vol" and str(vol_market or "perp").strip().lower() == "spot":
        names = list_volume_spot_product_names(network=network)
        return tuple(names) if names else tuple()
    return tuple(get_perp_products(network=network) or ("BTC", "ETH", "SOL"))


# F6 (Phase 5 audit): tiny per-process TTL cache for ``client.get_balance()``.
# A strategy preview render calls _build_strategy_preview_text and (for MM
# strategies) _append_mm_pretrade_breakdown back-to-back via run_blocking; both
# need the wallet balance and previously hit the SDK twice. Cache TTL is small
# (3s) so balance changes still propagate quickly. Race-safe under asyncio:
# Python dict ops are atomic at the GIL level, and a duplicate concurrent fetch
# is idempotent.
_BALANCE_CACHE_TTL_SECONDS = 3.0
# Moved from callbacks.py with _cached_user_balance: the extraction's AST
# mover collected Assign nodes but not AnnAssign, so this annotated dict
# was left behind while its consumer moved (prod NameError 2026-06-11).
_balance_cache: dict[int, tuple[float, dict]] = {}


def _cached_user_balance(telegram_id: int) -> dict:
    now = time.time()
    cached = _balance_cache.get(int(telegram_id))
    if cached and (now - cached[0]) < _BALANCE_CACHE_TTL_SECONDS:
        return cached[1]
    client = get_user_readonly_client(telegram_id)
    bal: dict = {}
    if client:
        try:
            bal = client.get_balance() or {}
        except Exception:
            bal = {}
    _balance_cache[int(telegram_id)] = (now, bal)
    return bal


def _wallet_collateral_usd_from_balance(bal: dict) -> float:
    if not isinstance(bal, dict) or not bal.get("exists"):
        return 0.0
    bals = bal.get("balances") or {}
    try:
        v = float(bals.get(0, 0) or 0.0)
    except (TypeError, ValueError):
        v = 0.0
    if v == 0.0:
        try:
            v = float(bals.get("0", 0) or 0.0)
        except (TypeError, ValueError):
            v = 0.0
    return v


def _vol_market_pref(context) -> str:
    # Volume strategy is spot-only as of 2026-05. The user_data slot and
    # ``strategy:volmarket`` callback are retained as no-ops only so cached
    # menus / deep links don't crash; we always return "spot".
    return "spot"


def _build_dn_funding_ranking(telegram_id: int) -> tuple[str, list[tuple[str, float | None, bool]]]:
    """Rank DN underlyings by their perp's current funding rate.

    Funding is a signed daily rate (positive ⇒ longs pay shorts ⇒ favorable for
    the DN short leg), settled hourly. Most-positive first = best short carry.
    Pulls every DN perp's rate in one batched indexer call
    (``client.get_perp_funding_rates``). Returns ``(markdown_text, rows)`` where
    each row is ``(product, daily_rate_or_None, entry_allowed)`` in display order.
    """
    user = get_user(telegram_id)
    network = user.network_mode.value if user else "mainnet"
    client = get_user_readonly_client(telegram_id)

    products = list(_strategy_available_products("dn", network))
    pid_by_product: dict[str, int] = {}
    entry_allowed_by_product: dict[str, bool] = {}
    for product in products:
        pair = get_dn_pair(product, network=network, client=client) or {}
        entry_allowed_by_product[product] = bool(pair.get("entry_allowed", True))
        pid = pair.get("perp_product_id")
        if pid is not None:
            try:
                pid_by_product[product] = int(pid)
            except (TypeError, ValueError):
                pass

    rates_by_pid: dict = {}
    if client and pid_by_product:
        try:
            rates_by_pid = client.get_perp_funding_rates(list(pid_by_product.values())) or {}
        except Exception:  # degrade-ok: funding screen still renders without rates
            rates_by_pid = {}

    rows: list[tuple[str, float | None, bool]] = []
    for product in products:
        rate: float | None = None
        pid = pid_by_product.get(product)
        if pid is not None:
            entry = rates_by_pid.get(pid) or {}
            raw = entry.get("funding_rate")
            if raw is not None:
                try:
                    rate = float(raw)
                except (TypeError, ValueError):
                    rate = None
        rows.append((product, rate, entry_allowed_by_product.get(product, True)))

    # Most-positive funding first; unknown rates sink to the bottom.
    rows.sort(key=lambda t: (t[1] is None, -(t[1] if t[1] is not None else 0.0)))

    lines = [
        "💹 *Delta Neutral — Funding Rates*",
        "Highest funding first \\(best carry for the DN *short* leg\\)\\.",
        "",
    ]
    have_rates = any(r is not None for _p, r, _e in rows)
    if not have_rates:
        lines.append("_Funding rates are unavailable right now — try Refresh in a moment\\._")
    else:
        for idx, (product, rate, entry_allowed) in enumerate(rows[:8], start=1):
            prod = escape_md(str(product).upper())
            if rate is None:
                detail = "n/a"
            else:
                bias = "short earns" if rate > 0 else ("short pays" if rate < 0 else "flat")
                detail = escape_md(f"{rate * 100:+.4f}%/day ({bias})")
            lock = "" if entry_allowed else "🔒 "
            lines.append(f"{idx}\\. {lock}*{prod}* — {detail}")
    lines.append("")
    lines.append("_Daily rate, settled hourly \\(about rate/24 per hour\\)\\. Tap a market to select it,_")
    lines.append("_then set your margin and start\\. Short hold/size can be fee\\-dominated\\._")
    return "\n".join(lines), rows


async def _handle_strategy(query, data, context, telegram_id):
    supported = ("grid", "rgrid", "dgrid", "mid", "dn", "vol", "bro")
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    strategy_id = parts[2] if len(parts) > 2 else ""

    if action == "volmarket" and len(parts) >= 4:
        # Volume is now spot-only — coerce any legacy ``perp`` selection to spot
        # and refresh the preview so cached menus reconverge.
        sid = parts[2]
        if sid != "vol":
            return
        context.user_data["vol_market:vol"] = "spot"
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "mainnet"
        names = list_volume_spot_product_names(network=network)
        if names:
            context.user_data["strategy_pair:vol"] = names[0]
        await _handle_strategy(query, f"strategy:preview:{sid}", context, telegram_id)
        return

    if action == "preview":
        if strategy_id not in supported:
            return
        if strategy_id == "bro":
            # Legacy Alpha Agent dashboard remains reachable only when the operator
            # has explicitly re-enabled the legacy autoloop via env var.
            from src.nadobro.services.feature_flags import legacy_bro_autoloop_enabled
            if not legacy_bro_autoloop_enabled():
                # Alpha Agent has been retired; route back to the strategy hub.
                await _handle_nav(query, "nav:strategy_hub", telegram_id, context)
                return
            from src.nadobro.handlers.keyboards import bro_action_kb
            with timed_metric("cb.strategy.preview.bro"):
                preview_text = await run_blocking(_build_bro_preview_text, telegram_id)
            bot_status = get_user_bot_status(telegram_id) or {}
            is_running = bool(bot_status.get("running") and bot_status.get("strategy") == "bro")
            await _edit_loc(query,
                preview_text,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=bro_action_kb(is_running=is_running),
            )
            return
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "mainnet"
        vm = _vol_market_pref(context) if strategy_id == "vol" else None
        available_pairs = _strategy_available_products(strategy_id, network, vm)
        if strategy_id == "vol" and vm == "spot" and not available_pairs:
            # Distinguish "catalog returned but nothing listed for {network}" vs
            # "catalog endpoint is failing/Cloudflare-blocked right now" so the
            # user gets accurate guidance for the active mode.
            try:
                from src.nadobro.services.product_catalog import is_spot_catalog_dynamic

                catalog_live = is_spot_catalog_dynamic(network)
            except Exception:
                catalog_live = False
            network_label = "Testnet" if str(network).lower() == "testnet" else "Mainnet"
            if catalog_live:
                body = (
                    f"⚠️ *Volume Spot*\n\nNo spot books are listed for {network_label} right now\\. "
                    "Pick *Perp* or come back when Nado lists a spot pair on this network\\."
                )
            else:
                body = (
                    f"⚠️ *Volume Spot*\n\nSpot catalog temporarily unavailable on {network_label} "
                    "\\(upstream is being challenged by Cloudflare\\)\\. Please retry in ~30s\\."
                )
            await _edit_loc(
                query,
                body,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=strategy_action_kb("vol", "BTC", ["BTC"], vol_market=vm),
            )
            return
        selected_product = str(context.user_data.get(f"strategy_pair:{strategy_id}", available_pairs[0]) or available_pairs[0]).upper()
        if selected_product not in available_pairs:
            selected_product = available_pairs[0]
            context.user_data[f"strategy_pair:{strategy_id}"] = selected_product
        vkb_prev = _vol_market_pref(context) if strategy_id == "vol" else None
        with timed_metric("cb.strategy.preview"):
            preview_text = await run_blocking(
                _build_strategy_preview_text,
                telegram_id,
                strategy_id,
                selected_product,
                vkb_prev,
            )
        # Phase 3: append Tread-style breakdown for MM strategies. Built off the
        # mm_dashboard module so the pre-trade card and /mm_status share math.
        if strategy_id in ("grid", "rgrid", "dgrid", "mid"):
            preview_text = await run_blocking(
                _append_mm_pretrade_breakdown,
                telegram_id,
                strategy_id,
                selected_product,
                preview_text,
            )
        bot_status = get_user_bot_status(telegram_id) or {}
        is_running = bool(
            bot_status.get("running")
            and str(bot_status.get("strategy") or "").lower() == strategy_id
        )
        vkb = _vol_market_pref(context) if strategy_id == "vol" else "perp"
        await _edit_loc(query, 
            preview_text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_action_kb(
                strategy_id,
                selected_product,
                list(available_pairs),
                is_running=is_running,
                vol_market=vkb,
            ),
        )
    elif action == "custom" and len(parts) >= 4:
        strategy_id = parts[2]
        if strategy_id not in supported:
            return
        try:
            page = int(parts[3])
        except (TypeError, ValueError):
            page = 0
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "mainnet"
        vm = _vol_market_pref(context) if strategy_id == "vol" else None
        available_pairs = _strategy_available_products(strategy_id, network, vm)
        if not available_pairs:
            await _edit_loc(query, "⚠️ No assets available for this mode\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return
        selected_product = str(context.user_data.get(f"strategy_pair:{strategy_id}", available_pairs[0]) or available_pairs[0]).upper()
        if selected_product not in available_pairs:
            selected_product = available_pairs[0]
        await _edit_loc(
            query,
            f"🎯 *Select Asset for {escape_md(strategy_id.upper())}*",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_product_picker_kb(
                strategy_id=strategy_id,
                selected_product=selected_product,
                available_products=list(available_pairs),
                page=page,
            ),
        )
    elif action == "pair" and len(parts) >= 4:
        strategy_id = parts[2]
        selected_product = parts[3].upper()
        if strategy_id not in supported:
            return
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "mainnet"
        vm = _vol_market_pref(context) if strategy_id == "vol" else None
        allowed_pairs = _strategy_available_products(strategy_id, network, vm)
        if selected_product not in allowed_pairs:
            return
        context.user_data[f"strategy_pair:{strategy_id}"] = selected_product
        vkb_pair = _vol_market_pref(context) if strategy_id == "vol" else None
        with timed_metric("cb.strategy.preview"):
            preview_text = await run_blocking(
                _build_strategy_preview_text,
                telegram_id,
                strategy_id,
                selected_product,
                vkb_pair,
            )
        if strategy_id in ("grid", "rgrid", "dgrid", "mid"):
            preview_text = await run_blocking(
                _append_mm_pretrade_breakdown,
                telegram_id,
                strategy_id,
                selected_product,
                preview_text,
            )
        bot_status = get_user_bot_status(telegram_id) or {}
        is_running = bool(
            bot_status.get("running")
            and str(bot_status.get("strategy") or "").lower() == strategy_id
        )
        vkb = _vol_market_pref(context) if strategy_id == "vol" else "perp"
        await _edit_loc(query,
            preview_text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=strategy_action_kb(
                strategy_id,
                selected_product,
                list(allowed_pairs),
                is_running=is_running,
                vol_market=vkb,
            ),
        )
    elif action == "funding":
        # DN-only: rank the corresponding-spot perps by funding so the user picks
        # the best short carry before sizing margin. Tapping a row routes through
        # the normal pair selection and lands back on the DN dashboard.
        if strategy_id != "dn":
            return
        with timed_metric("cb.strategy.funding.dn"):
            text, ranked = await run_blocking(_build_dn_funding_ranking, telegram_id)
        await _edit_loc(
            query,
            text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=dn_funding_rates_kb(ranked),
        )
    elif action == "config":
        if strategy_id not in supported:
            return
        network, settings = get_user_settings(telegram_id)
        conf = settings.get("strategies", {}).get(strategy_id, {})
        context.user_data.pop(f"strategy_config_section:{strategy_id}", None)
        await _edit_loc(query, 
            _strategy_config_menu_text(strategy_id, conf, network),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_strategy_config_menu_kb(strategy_id),
        )
    elif action == "config_section" and len(parts) >= 4:
        section = parts[3]
        if strategy_id not in supported:
            return
        valid_sections = {name for name, _label in _strategy_config_sections(strategy_id)}
        if section not in valid_sections:
            return
        context.user_data[f"strategy_config_section:{strategy_id}"] = section
        network, settings = get_user_settings(telegram_id)
        conf = settings.get("strategies", {}).get(strategy_id, {})
        await _edit_loc(
            query,
            _strategy_config_section_text(strategy_id, conf, network, section),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_strategy_config_section_kb(strategy_id, section),
        )
    elif action == "preset" and len(parts) >= 4:
        # MM Tiny Budget / Standard preset.
        # Tiny Budget Preset auto-derives the leverage required to clear the venue
        # min_size floor for the selected pair and pins a tight safety factor so a
        # small wallet (e.g. $50) can quote on pairs that would otherwise reject.
        # Standard Preset clears those overrides and returns to per-asset MAX leverage
        # with the legacy safety cushion.
        strategy_id = parts[2]
        preset_name = parts[3]
        if strategy_id not in {"grid", "rgrid", "dgrid", "mid"} or preset_name not in {"tiny", "standard"}:
            return
        network, settings = get_user_settings(telegram_id)
        cfg_now = settings.get("strategies", {}).get(strategy_id, {}) or {}
        # Selected product for the strategy was set during preview; default to BTC.
        selected_product = str(
            context.user_data.get(f"strategy_pair:{strategy_id}", "BTC") or "BTC"
        ).upper()
        if preset_name == "standard":
            def _mutate_std(s):
                strategies = s.setdefault("strategies", {})
                cfg = strategies.setdefault(strategy_id, {})
                cfg.pop("mm_leverage_override", None)
                cfg.pop("min_order_notional_usd", None)
                cfg.pop("mm_collateral_safety_factor", None)
                cfg["mm_preset"] = "standard"

            network, settings = update_user_settings(telegram_id, _mutate_std)
            conf = settings.get("strategies", {}).get(strategy_id, {})
            section = "setup"
            context.user_data[f"strategy_config_section:{strategy_id}"] = section
            # Confirmation note so Standard is no longer a silent no-op: it clears
            # the Tiny override and returns to the default leverage (margin × 3x).
            margin_std = float(conf.get("notional_usd", 100.0) or 0.0)
            deployed_std = margin_std * _mm_effective_leverage(conf)
            std_note = (
                "*Standard preset applied*\n"
                f"✅ Cleared the Tiny override — back to default leverage "
                f"\\({escape_md(f'{_mm_effective_leverage(conf):.0f}x')}\\)\\. "
                f"Position \\= *{escape_md(f'${deployed_std:,.0f}')}* "
                f"\\(margin × leverage\\)\\. Pick a Lev button to override\\."
            )
            body_std = _strategy_config_section_text(strategy_id, conf, network, section)
            await _edit_loc(
                query,
                f"{body_std}\n\n{std_note}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=_strategy_config_section_kb(strategy_id, section),
            )
            return

        # Tiny Budget — resolve venue minimum and required leverage.
        try:
            from src.nadobro.services.product_catalog import (
                get_product_min_quote_notional_usd,
            )
            venue_min = get_product_min_quote_notional_usd(selected_product, network=network)
            lev_cap = float(get_product_max_leverage(selected_product, network=network))
        except Exception:
            venue_min = None
            lev_cap = 1.0
        try:
            collateral = float(cfg_now.get("notional_usd") or 0.0)
        except (TypeError, ValueError):
            collateral = 0.0
        if not venue_min or venue_min <= 0:
            await _edit_loc(
                query,
                "⚠️ *Tiny Budget Preset*\n\nVenue minimum could not be resolved for "
                f"`{escape_md(selected_product)}` from Nado catalog\\. Try again in a moment\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=_strategy_config_section_kb(strategy_id, "setup"),
            )
            return
        if collateral <= 0:
            await _edit_loc(
                query,
                "⚠️ *Tiny Budget Preset*\n\nSet your margin first \\(Custom Margin\\), "
                "then press Tiny Budget Preset to fit leverage to the venue floor\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=_strategy_config_section_kb(strategy_id, "setup"),
            )
            return
        import math as _math
        required_lev = max(1.0, _math.ceil(venue_min / collateral))
        # 1.1× cushion to absorb the 1.10 safety factor without overshooting cap.
        target_lev = min(_math.ceil(required_lev * 1.1), int(lev_cap))
        target_lev = max(int(required_lev), int(target_lev))
        target_lev = min(int(target_lev), int(lev_cap))

        def _mutate_tiny(s):
            strategies = s.setdefault("strategies", {})
            cfg = strategies.setdefault(strategy_id, {})
            cfg["mm_leverage_override"] = int(target_lev)
            cfg["min_order_notional_usd"] = float(venue_min)
            cfg["mm_collateral_safety_factor"] = 1.10
            cfg["mm_preset"] = "tiny"

        network, settings = update_user_settings(telegram_id, _mutate_tiny)
        conf = settings.get("strategies", {}).get(strategy_id, {})
        section = "setup"
        context.user_data[f"strategy_config_section:{strategy_id}"] = section
        notional_after = collateral * float(target_lev)
        if notional_after >= venue_min:
            preflight_note = (
                f"✅ ${collateral:.0f} × {int(target_lev)}× \\= ${notional_after:.0f} notional "
                f"≥ pair minimum ${venue_min:.0f} \\(USDT0\\)\\. Cleared to quote\\."
            )
        else:
            preflight_note = (
                f"⚠️ ${collateral:.0f} × {int(target_lev)}× \\= ${notional_after:.0f} notional "
                f"< pair minimum ${venue_min:.0f}\\. Add collateral or pick a smaller-min pair\\."
            )
        body = _strategy_config_section_text(strategy_id, conf, network, section)
        await _edit_loc(
            query,
            f"{body}\n\n*Tiny Budget Preset applied for {escape_md(selected_product)}*\n{preflight_note}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_strategy_config_section_kb(strategy_id, section),
        )
        return
    elif action == "set" and len(parts) >= 5:
        strategy_id = parts[2]
        field = parts[3]
        raw_value = parts[4]
        if strategy_id not in supported:
            return
        if strategy_id == "vol" and field not in {"sl_pct", "session_margin_usd", "target_volume_usd"}:
            return
        allowed_numeric_fields = {
            "notional_usd", "spread_bp", "interval_seconds", "tp_pct", "sl_pct",
            "levels", "min_range_pct", "max_range_pct", "threshold_bp", "close_offset_bp",
            "cycle_notional_usd", "session_notional_cap_usd", "inventory_soft_limit_usd",
            "quote_ttl_seconds", "min_spread_bp", "max_spread_bp", "vol_sensitivity",
            "grid_reset_threshold_pct", "grid_reset_timeout_seconds",
            "rgrid_spread_bp", "rgrid_stop_loss_pct", "rgrid_take_profit_pct",
            "rgrid_reset_threshold_pct", "rgrid_reset_timeout_seconds", "rgrid_discretion",
            "dgrid_trend_on_variance_ratio", "dgrid_range_on_variance_ratio",
            "dgrid_spread_bp", "dgrid_min_spread_bp", "dgrid_max_spread_bp",
            "dgrid_short_window_points", "dgrid_long_window_points",
            "auto_close_on_maintenance", "is_long_bias",
            # R-Grid trend-follow toggle (1 = fill-anchored taker-momentum, the
            # default for rgrid; 0 = classic one-sided ladder).
            "fill_anchored",
            # Mid Mode accepts directional_bias as a continuous float in [-1, +1].
            "directional_bias",
            # Volume Bot (spot, 2026-05) accepts session margin + target volume.
            "session_margin_usd", "target_volume_usd",
            # Delta Neutral (engine v2) — the controller reads these directly:
            # per-leg size, hold duration, cycle count + gap, hedge drift gate.
            "fixed_margin_usd", "dn_hold_seconds", "dn_cycles",
            "dn_cycle_gap_seconds", "dn_max_drift_pct", "dn_hedge_ratio",
            # MM/grid leverage: turns margin into deployed notional (margin x lev).
            "mm_leverage_override",
            # MM run duration in minutes (participation/TWAP). Hard cap for
            # Mid/D-Grid; soft target for Grid/R-Grid.
            "mm_duration_minutes",
            # TWAP fast-move pause threshold (bp/cycle); 0 = off.
            "twap_pause_move_bp",
        }
        if field not in allowed_numeric_fields:
            return
        # Mid Mode: directional_bias is only valid as a number for the mid strategy
        # (other strategies still use the set_text path with neutral/long/short).
        if field == "directional_bias" and strategy_id != "mid":
            return
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return
        # Mid Mode spread is capped at 0: a post-only LIMIT_MAKER can't cross the
        # book, so a negative spread is impossible to honor (it was previously
        # accepted but silently floored). 0 = quote as tight as the fee floor
        # allows (the speed end). Other strategies stay positive-only.
        if field == "spread_bp" and strategy_id == "mid":
            spread_lo, spread_hi = 0.0, 100.0
        else:
            spread_lo, spread_hi = 0.1, 200.0
        limits = {
            "notional_usd": (1, 1000000),
            "spread_bp": (spread_lo, spread_hi),
            "directional_bias": (-1.0, 1.0),
            "interval_seconds": (10, 3600),
            "tp_pct": (0.05, 100),
            "sl_pct": (0.05, 100),
            "session_margin_usd": (10, 1000000),
            "target_volume_usd": (100, 100000000),
            "levels": (1, 20),
            "min_range_pct": (0.1, 20),
            "max_range_pct": (0.1, 40),
            "threshold_bp": (0, 500),
            "close_offset_bp": (1, 1000),
            "cycle_notional_usd": (1, 1000000),
            "session_notional_cap_usd": (0, 10000000),
            "inventory_soft_limit_usd": (1, 1000000),
            "quote_ttl_seconds": (5, 86400),
            "min_spread_bp": (0.1, 200),
            "max_spread_bp": (0.1, 500),
            "vol_sensitivity": (0.0, 1.0),
            "grid_reset_threshold_pct": (0.05, 20),
            "grid_reset_timeout_seconds": (15, 86400),
            "rgrid_spread_bp": (0.1, 200),
            "rgrid_stop_loss_pct": (0.05, 100),
            "rgrid_take_profit_pct": (0.05, 200),
            "rgrid_reset_threshold_pct": (0.05, 20),
            "rgrid_reset_timeout_seconds": (15, 86400),
            "rgrid_discretion": (0.01, 0.5),
            "dgrid_trend_on_variance_ratio": (1.0, 5.0),
            "dgrid_range_on_variance_ratio": (0.1, 2.0),
            "dgrid_spread_bp": (0.1, 200.0),
            "dgrid_min_spread_bp": (0.0, 50.0),
            "dgrid_max_spread_bp": (1.0, 200.0),
            "dgrid_short_window_points": (2, 50),
            "dgrid_long_window_points": (4, 200),
            "auto_close_on_maintenance": (0, 1),
            "is_long_bias": (0, 1),
            "fill_anchored": (0, 1),
            # Delta Neutral (engine v2).
            "fixed_margin_usd": (1, 1000000),
            "dn_hold_seconds": (60, 86400),        # 1 minute .. 24 hours
            "dn_cycles": (1, 100),
            "dn_cycle_gap_seconds": (0, 86400),
            "dn_max_drift_pct": (0.5, 50),
            "dn_hedge_ratio": (0.1, 5.0),
            "mm_leverage_override": (1, 50),
            "mm_duration_minutes": (1, 14400),   # 1 min .. 240h (POV upper bound)
            "twap_pause_move_bp": (0, 5000),     # 0 = off .. 50% per-cycle move
        }
        lo, hi = limits[field]
        if value < lo or value > hi:
            return
        int_fields = {
            "interval_seconds", "levels", "max_open_orders",
            "auto_close_on_maintenance", "is_long_bias", "rgrid_reset_timeout_seconds",
            "dn_hold_seconds", "dn_cycles", "dn_cycle_gap_seconds", "mm_leverage_override",
            "fill_anchored", "mm_duration_minutes", "twap_pause_move_bp",
        }

        def _mutate(s):
            strategies = s.setdefault("strategies", {})
            cfg = strategies.setdefault(strategy_id, {})
            if field in int_fields:
                cfg[field] = int(value)
            else:
                cfg[field] = value
            if field == "notional_usd":
                from src.nadobro.services.settings_service import sync_cycle_notional_with_margin

                sync_cycle_notional_with_margin(strategies, strategy_id)

        network, settings = update_user_settings(telegram_id, _mutate)
        conf = settings.get("strategies", {}).get(strategy_id, {})
        section = context.user_data.get(f"strategy_config_section:{strategy_id}") or _strategy_section_for_field(strategy_id, field)
        context.user_data[f"strategy_config_section:{strategy_id}"] = section
        await _edit_loc(query, 
            _strategy_config_section_text(strategy_id, conf, network, section),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_strategy_config_section_kb(strategy_id, section),
        )
    elif action == "set_text" and len(parts) >= 5:
        strategy_id = parts[2]
        field = parts[3]
        raw_value = parts[4]
        if strategy_id not in supported:
            return
        allowed_text = {
            "reference_mode": {"mid", "ema_fast", "ema_slow"},
            "directional_bias": {"neutral", "long_bias", "short_bias"},
            "vol_direction": {"long", "short"},
            "funding_entry_mode": {"wait", "enter_anyway"},
            # Phase 2: Tread Fi POV / participation preset.
            "participation_preset": {"aggressive", "normal", "passive", "off"},
        }
        allowed_vals = allowed_text.get(field, set())
        if raw_value not in allowed_vals:
            return
        # participation_preset is only meaningful for the MM family.
        if field == "participation_preset" and strategy_id not in ("grid", "rgrid", "dgrid", "mid"):
            return

        def _mutate(s):
            strategies = s.setdefault("strategies", {})
            cfg = strategies.setdefault(strategy_id, {})
            if field == "participation_preset" and raw_value == "off":
                cfg.pop("participation_preset", None)
            else:
                cfg[field] = raw_value

        network, settings = update_user_settings(telegram_id, _mutate)
        conf = settings.get("strategies", {}).get(strategy_id, {})
        section = context.user_data.get(f"strategy_config_section:{strategy_id}") or _strategy_section_for_field(strategy_id, field)
        context.user_data[f"strategy_config_section:{strategy_id}"] = section
        await _edit_loc(query, 
            _strategy_config_section_text(strategy_id, conf, network, section),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=_strategy_config_section_kb(strategy_id, section),
        )
    elif action == "input" and len(parts) >= 4:
        strategy_id = parts[2]
        field = parts[3]
        if strategy_id not in supported:
            return
        allowed_inputs = (
            "notional_usd", "spread_bp", "interval_seconds", "tp_pct", "sl_pct",
            "levels", "min_range_pct", "max_range_pct", "threshold_bp", "close_offset_bp",
            "cycle_notional_usd", "session_notional_cap_usd", "inventory_soft_limit_usd",
            "quote_ttl_seconds", "min_spread_bp", "max_spread_bp", "vol_sensitivity",
            "rgrid_spread_bp", "rgrid_stop_loss_pct", "rgrid_take_profit_pct",
            "rgrid_reset_threshold_pct", "rgrid_reset_timeout_seconds", "rgrid_discretion",
            "dgrid_trend_on_variance_ratio", "dgrid_range_on_variance_ratio",
            "dgrid_spread_bp", "dgrid_min_spread_bp", "dgrid_max_spread_bp",
            "dgrid_short_window_points", "dgrid_long_window_points",
            "directional_bias", "mm_leverage_override", "mm_duration_minutes",
            "twap_pause_move_bp",
            # Delta Neutral (engine v2) custom inputs.
            "fixed_margin_usd", "dn_hold_seconds", "dn_cycles",
        )
        if strategy_id == "vol" and field not in {"sl_pct", "session_margin_usd", "target_volume_usd"}:
            return
        if field not in allowed_inputs:
            return
        section = context.user_data.get(f"strategy_config_section:{strategy_id}") or _strategy_section_for_field(strategy_id, field)
        context.user_data[f"strategy_config_section:{strategy_id}"] = section
        context.user_data["pending_strategy_input"] = {
            "strategy": strategy_id,
            "field": field,
            "section": section,
        }
        await run_blocking(
            persist_strategy_pending_input,
            int(telegram_id),
            {"strategy": strategy_id, "field": field, "section": section},
        )
        help_text = {
            "notional_usd": "Enter margin in USD \\(example: `150`\\)",
            "spread_bp": "Enter spread in bps \\(example: `6`\\)",
            "interval_seconds": "Enter loop interval seconds \\(example: `45`\\)",
            "tp_pct": "Enter take profit % \\(example: `1\\.2`\\)",
            "sl_pct": "Enter stop loss % \\(example: `0\\.7`\\)",
            "levels": "Enter grid levels \\(example: `4`\\)",
            "min_range_pct": "Enter min range % \\(example: `1\\.0`\\)",
            "max_range_pct": "Enter max range % \\(example: `2\\.0`\\)",
            "threshold_bp": "Enter threshold in bps \\(example: `0` to disable, or `12`\\)",
            "close_offset_bp": "Enter close offset in bps \\(example: `25`\\)",
            "cycle_notional_usd": "Enter per\\-cycle budget in USD \\(usually same as margin\\)",
            "session_notional_cap_usd": "Enter optional session cap in USD \\(example: `5000`, or `0` to disable\\)",
            "inventory_soft_limit_usd": "Enter inventory soft limit in USD \\(example: `45`\\)",
            "quote_ttl_seconds": "Enter quote TTL seconds \\(example: `90`\\)",
            "min_spread_bp": "Enter minimum spread in bps \\(example: `2`\\)",
            "max_spread_bp": "Enter maximum spread in bps \\(example: `20`\\)",
            "vol_sensitivity": "Enter volatility sensitivity \\(example: `0\\.02`\\)",
            "grid_reset_threshold_pct": "Enter GRID reset threshold % \\(example: `0\\.8`\\)",
            "grid_reset_timeout_seconds": "Enter GRID reset timeout seconds \\(example: `120`\\)",
            "rgrid_spread_bp": "Enter RGRID spread in bps \\(example: `10`\\)",
            "rgrid_stop_loss_pct": "Enter RGRID PnL stop loss % of margin \\(example: `0\\.8`\\)",
            "rgrid_take_profit_pct": "Enter RGRID PnL take profit % of margin \\(example: `1\\.2`\\)",
            "rgrid_reset_threshold_pct": "Enter RGRID reset threshold % \\(example: `1\\.0`\\)",
            "rgrid_reset_timeout_seconds": "Enter RGRID reset timeout seconds \\(example: `120`\\)",
            "rgrid_discretion": "Enter RGRID discretion \\(example: `0\\.06`\\)",
            "dgrid_trend_on_variance_ratio": "Enter DGRID trend switch variance ratio \\(example: `1\\.25`\\)",
            "dgrid_range_on_variance_ratio": "Enter DGRID range switch variance ratio \\(example: `1\\.15`\\)",
            "dgrid_spread_bp": "Enter DGRID spread in bps \\(example: `8`\\)",
            "dgrid_min_spread_bp": "Enter DGRID minimum spread in bps \\(example: `2`\\)",
            "dgrid_max_spread_bp": "Enter DGRID maximum spread in bps \\(example: `50`\\)",
            "dgrid_short_window_points": "Enter DGRID short volatility window points \\(example: `4`\\)",
            "dgrid_long_window_points": "Enter DGRID long volatility window points \\(example: `12`\\)",
            "session_margin_usd": "Enter session margin in USD \\(per\\-cycle notional, example: `500`\\)",
            "target_volume_usd": "Enter target cumulative volume in USD \\(example: `25000`\\)",
            "mm_leverage_override": "Enter leverage \\(1 – 50; position size \\= margin × leverage, example: `5`\\)",
            "mm_duration_minutes": "Enter run duration in minutes \\(hard cap for Mid/D\\-Grid; a target for Grid/R\\-Grid, example: `60`\\)",
            "twap_pause_move_bp": "Enter fast\\-move pause threshold in bp per cycle \\(0 \\= off; pauses re\\-quoting when price jumps more, example: `200` \\= 2%\\)",
            "fixed_margin_usd": "Enter per\\-leg size in USD \\(example: `100`\\)",
            "dn_hold_seconds": "Enter *minimum* hold in seconds \\(60 – 86400; example: `3600` for 1h\\)\\. After this, the hedge stays open while funding is favorable and closes on a funding flip\\.",
            "dn_cycles": "Enter how many open→hold→close cycles to run \\(example: `3`\\)",
        }
        await _edit_loc(query, 
            f"✏️ *Custom {escape_md(field)}*\n\n"
            f"{help_text.get(field, 'Enter value')}\n\n"
            "Your next message will be used as this value\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=back_kb(f"strategy:config_section:{strategy_id}:{section}"),
        )
    elif action == "activate":
        context.user_data["active_setup"] = strategy_id
        await _edit_loc(query, 
            f"✅ Active setup is now *{escape_md(strategy_id.upper())}*\\.\n\n"
            "Next: open Buy/Long or Sell/Short and execute with preview\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    elif action == "start" and len(parts) >= 4:
        strategy_id = parts[2]
        product = str(parts[3] or "").upper()
        start_direction = "long"
        if strategy_id == "vol" and len(parts) >= 5:
            start_direction = "short" if str(parts[4]).lower() == "short" else "long"
        if strategy_id not in supported:
            return
        user = get_user(telegram_id)
        network = user.network_mode.value if user else "mainnet"
        vm = _vol_market_pref(context) if strategy_id == "vol" else None
        available_pairs = _strategy_available_products(strategy_id, network, vm)
        allowed_pairs = set(available_pairs)
        if product not in allowed_pairs:
            vkb_err = _vol_market_pref(context) if strategy_id == "vol" else "perp"
            await _edit_loc(
                query,
                f"⚠️ {escape_md(product)} is not currently available on {escape_md(network)}\\.\nPlease pick another asset\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=strategy_action_kb(
                    strategy_id,
                    available_pairs[0],
                    list(available_pairs),
                    vol_market=vkb_err,
                ),
            )
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
        settings = _get_user_settings(telegram_id, context)
        from src.nadobro.handlers.messages import execute_action_directly
        strategy_leverage = 1 if strategy_id == "vol" else settings.get("default_leverage", 3)
        if strategy_id == "dn":
            strategy_leverage = max(1, min(float(strategy_leverage), 5))
        if strategy_id in ("grid", "rgrid", "dgrid"):
            try:
                from src.nadobro.config import get_product_max_leverage as _gpml
                strategy_leverage = float(_gpml(product, network=network))
            except Exception:
                strategy_leverage = float(settings.get("default_leverage", 3))
        strategy_conf = (settings.get("strategies", {}) or {}).get(strategy_id, {}) or {}
        wallet_usdt_pf = None
        ro = get_user_readonly_client(telegram_id)
        if ro:
            try:
                b = ro.get_balance() or {}
                if b.get("exists"):
                    wallet_usdt_pf = float((b.get("balances") or {}).get(0, 0) or 0.0)
                    if wallet_usdt_pf == 0:
                        wallet_usdt_pf = float((b.get("balances") or {}).get("0", 0) or 0.0)
            except Exception:
                pass
        mm_budget_ok, mm_collateral_budget, mm_required_min_collateral, mm_min_order_notional, mm_max_quotes_est, mm_margin_per_quote_est = _mm_cycle_budget_preflight(
            strategy_id, strategy_conf, float(strategy_leverage or 1.0), wallet_usdt=wallet_usdt_pf
        )
        if not mm_budget_ok:
            vkb_block = _vol_market_pref(context) if strategy_id == "vol" else "perp"
            await _edit_loc(
                query,
                "⚠️ *Cannot start strategy*\n\n"
                "Collateral is below the estimated minimum for one venue\\-sized resting quote\\.\n"
                f"Each quote still carries roughly *{escape_md(f'${mm_min_order_notional:,.2f}')}* notional \\(exchange floor\\)\\.\n"
                f"Configured collateral cap: *{escape_md(f'${mm_collateral_budget:,.2f}')}*\n"
                f"Estimated margin per quote \\(~{escape_md(f'{strategy_leverage:.0f}x')}\\): "
                f"*{escape_md(f'${mm_margin_per_quote_est:,.2f}')}* \\(incl\\. safety buffer\\)\n"
                f"Need at least: *{escape_md(f'${mm_required_min_collateral:,.2f}')}*\n\n"
                "Deposit USDT, raise configured margin, or pick a product with a lower minimum order size\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=strategy_action_kb(
                    strategy_id,
                    product,
                    list(available_pairs),
                    vol_market=vkb_block,
                ),
            )
            return
        vol_m = _vol_market_pref(context) if strategy_id == "vol" else "perp"
        if strategy_id == "vol" and vol_m == "spot":
            start_direction = "long"
        start_payload = {
            "type": "start_strategy",
            "strategy": strategy_id,
            "product": product,
            "leverage": strategy_leverage,
            "slippage_pct": settings.get("slippage", 1),
            "direction": start_direction,
        }
        if strategy_id == "vol":
            start_payload["vol_market"] = vol_m
        await execute_action_directly(query, context, telegram_id, start_payload)
    elif action == "status":
        # Same merged dashboard as /status (Refresh/Stop) whether opened from
        # strategy cards (GRID/RGRID/DGRID/MID/DN/VOL/BRO) or commands.
        body, merged_kb = await build_status_dashboard_parts(telegram_id)
        await _edit_loc(
            query,
            body,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=merged_kb,
        )
    elif action == "stop":
        ok, msg = await run_blocking(stop_user_bot, telegram_id, True)
        body, merged_kb = await build_status_dashboard_parts(telegram_id)
        prefix = "🛑" if ok else "⚠️"
        await _edit_loc(
            query,
            "{body}\n\n{prefix} {msg}",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=merged_kb,
            body=body,
            prefix=prefix,
            msg=escape_md(msg),
        )


def _mm_cycle_budget_preflight(
    strategy_id: str,
    strategy_conf: dict,
    leverage: float,
    *,
    wallet_usdt: float | None = None,
) -> tuple[bool, float, float, float, int, float]:
    """MM grid family: collateral vs estimated margin per venue-sized quote.

    Returns
        ok, collateral_budget, required_min_collateral_1q, min_order_notional,
        max_resting_quotes_est, margin_per_quote_est
    """
    if strategy_id not in ("grid", "rgrid", "dgrid", "mid"):
        return True, 0.0, 0.0, 0.0, 0, 0.0

    from src.nadobro.services.mm_quote_math import DEFAULT_MIN_ORDER_NOTIONAL_USD, estimate_mm_quote_capacity

    margin_usd = max(0.0, float(strategy_conf.get("notional_usd", 100.0) or 0.0))
    cycle_cfg = max(0.0, float(strategy_conf.get("cycle_notional_usd", margin_usd) or 0.0))
    collateral_cfg = max(cycle_cfg, margin_usd)
    if wallet_usdt is not None and float(wallet_usdt) > 0:
        collateral_budget = min(collateral_cfg, float(wallet_usdt))
    else:
        collateral_budget = collateral_cfg

    min_order_notional = max(
        1.0,
        float(strategy_conf.get("min_order_notional_usd") or DEFAULT_MIN_ORDER_NOTIONAL_USD),
    )
    try:
        max_open_orders = int(strategy_conf.get("max_open_orders", 6) or 6)
    except (TypeError, ValueError):
        max_open_orders = 6
    max_open_orders = max(1, max_open_orders)

    lev = max(1.0, float(leverage or 1.0))
    cap = estimate_mm_quote_capacity(
        collateral_budget,
        min_order_notional,
        lev,
        max_open_orders=max_open_orders,
    )
    max_q = int(cap["max_resting_quotes"])
    margin_per = float(cap["margin_per_quote_est_usd"])
    required_min = float(cap["min_collateral_1_quote_usd"])
    ok = max_q >= 1
    return ok, collateral_budget, required_min, min_order_notional, max_q, margin_per


def _fmt_strategy_config_text(strategy: str, conf: dict, network: str) -> str:
    if strategy == "vol":
        tp_pct = float(conf.get("tp_pct", 1.0))
        sl_pct = float(conf.get("sl_pct", 1.0))
        direction = "SHORT" if str(conf.get("vol_direction", "long")).lower() == "short" else "LONG"
        return (
            "⚙️ *VOL*\n\n"
            f"Mode: *{escape_md(network.upper())}*\n"
            f"Fixed margin: *{escape_md('$100.00')}* · Fixed leverage: *{escape_md('1x')}*\n"
            f"Direction: *{escape_md(direction)}*\n"
            "Entry: *Limit @ mid* · Exit: *Market close after 60s from fill*\n"
            f"Session TP/SL: *{escape_md(f'{tp_pct:.2f}%/{sl_pct:.2f}%')}* of fixed margin\n\n"
            "Use controls below to change direction or TP/SL only\\."
        )

    notional = float(conf.get("notional_usd", 100.0))
    spread_bp = float(conf.get("spread_bp", 5.0))
    if strategy == "rgrid":
        spread_bp = float(conf.get("rgrid_spread_bp", conf.get("grid_spread_bp", spread_bp)))
    interval_seconds = int(conf.get("interval_seconds", 60))
    tp_pct = float(conf.get("tp_pct", 1.0))
    sl_pct = float(conf.get("sl_pct", 0.5))
    base = (
        f"⚙️ *{escape_md(strategy.upper())}*\n\n"
        f"Mode: *{escape_md(network.upper())}*\n"
        f"Margin: *{escape_md(f'${notional:,.2f}')}* · Spread: *{escape_md(f'{spread_bp:.1f} bp')}*\n"
        f"Interval: *{escape_md(f'{interval_seconds}s')}*"
    )
    if strategy != "rgrid":
        base += f" · TP/SL: *{escape_md(f'{tp_pct:.2f}%/{sl_pct:.2f}%')}*"
    base += "\n\n"
    extra = ""
    if strategy == "rgrid":
        grid_sl = float(conf.get("rgrid_stop_loss_pct", conf.get("grid_stop_loss_pct", sl_pct)))
        grid_tp = float(conf.get("rgrid_take_profit_pct", conf.get("grid_take_profit_pct", tp_pct)))
        grid_discretion = float(conf.get("rgrid_discretion", conf.get("grid_discretion", 0.06)))
        reset_threshold = float(conf.get("rgrid_reset_threshold_pct", conf.get("grid_reset_threshold_pct", 1.0)))
        reset_timeout = int(conf.get("rgrid_reset_timeout_seconds", conf.get("grid_reset_timeout_seconds", 120)))
        spread_hint = "Reverse breakout width"
        extra = (
            f"Reverse Levels: *{escape_md(str(int(conf.get('levels', 4))))}* \\| "
            f"Spread mode: *{escape_md(spread_hint)}*\n"
            f"PnL SL/TP: *{escape_md(f'{grid_sl:.2f}% / {grid_tp:.2f}%')}* \\| "
            f"Reset: *{escape_md(f'{reset_threshold:.2f}% / {reset_timeout}s')}*\n"
            f"Discretion: *{escape_md(f'{grid_discretion:.2f}')}*\n\n"
        )
    elif strategy == "dgrid":
        trend_on = float(conf.get("dgrid_trend_on_variance_ratio", 1.25))
        range_on = float(conf.get("dgrid_range_on_variance_ratio", 1.15))
        dgrid_spread = float(conf.get("dgrid_spread_bp", conf.get("spread_bp", 8.0)))
        extra = (
            f"Auto phase: *GRID ⇄ RGRID* \\| Variance: *{escape_md(f'{range_on:.2f} / {trend_on:.2f}')}*\n"
            f"Spread: *{escape_md(f'{dgrid_spread:.1f} bp')}*\n\n"
        )
    elif strategy == "grid":
        threshold = f"{float(conf.get('threshold_bp', 12.0)):.1f} bp"
        close_offset = f"{float(conf.get('close_offset_bp', 24.0)):.1f} bp"
        ref_mode = str(conf.get("reference_mode", "ema_fast")).upper()
        bias = str(conf.get("directional_bias", "neutral")).upper()
        cycle_notional = float(conf.get("cycle_notional_usd", notional))
        session_cap = float(conf.get("session_notional_cap_usd", 0) or 0)
        inv_soft = float(conf.get("inventory_soft_limit_usd", notional * 0.6))
        quote_ttl = int(conf.get("quote_ttl_seconds", 90))
        min_spread = float(conf.get("min_spread_bp", 2.0))
        max_spread = float(conf.get("max_spread_bp", 20.0))
        vol_sensitivity = float(conf.get("vol_sensitivity", 0.02))
        cap_str = f"${session_cap:,.0f}" if session_cap > 0 else "OFF"
        spread_band = f"{min_spread:.1f} - {max_spread:.1f} bp"
        extra = (
            f"Move to quote: *{escape_md(threshold)}* · Close offset: *{escape_md(close_offset)}*\n"
            f"Ref: *{escape_md(ref_mode)}* · Bias: *{escape_md(bias)}*\n"
            f"Per\\-cycle budget: *{escape_md(f'${cycle_notional:,.0f}')}* · Cap: *{escape_md(cap_str)}*\n"
            f"Inventory limit: *{escape_md(f'${inv_soft:,.0f}')}* · Quote TTL: *{escape_md(f'{quote_ttl}s')}*\n"
            f"Spread band: *{escape_md(spread_band)}* · Vol: *{escape_md(f'{vol_sensitivity:.3f}')}*\n\n"
        )
    elif strategy == "dn":
        auto_close = "ON" if float(conf.get("auto_close_on_maintenance", 1) or 0) >= 0.5 else "OFF"
        leg_size = float(conf.get("fixed_margin_usd", conf.get("notional_usd", 100.0)) or 100.0)
        hold_s = int(conf.get("dn_hold_seconds", 3600) or 3600)
        cycles = int(conf.get("dn_cycles", 1) or 1)
        drift_pct = float(conf.get("dn_max_drift_pct", 5.0) or 5.0)
        extra = (
            "Hedge model: *Spot long \\+ 1x perp short* \\(BTC, ETH, QQQ, SPY…\\)\n"
            f"Size \\(per leg\\): *{escape_md(f'${leg_size:,.0f}')}* · "
            f"Hold: *{escape_md(_fmt_hold_duration(hold_s))}* · Cycles: *{escape_md(str(cycles))}*\n"
            f"Hedge drift gate: *{escape_md(f'{drift_pct:.1f}%')}*\n"
            f"Auto-close on maintenance: *{escape_md(auto_close)}*\n\n"
        )
    return base + extra + "Use presets or set custom values below\\."


def _strategy_config_default_section(strategy: str) -> str:
    return "direction" if strategy == "vol" else "setup"


def _strategy_config_sections(strategy: str) -> list[tuple[str, str]]:
    if strategy == "vol":
        return [("direction", "🎯 Direction"), ("risk", "🛡 TP / SL")]
    if strategy == "grid":
        return [("setup", "⚙️ Core"), ("execution", "📐 Spread"), ("risk", "🛡 Risk")]
    if strategy == "rgrid":
        return [("setup", "⚙️ Core"), ("risk", "🛡 Risk"), ("reset", "🔄 Reset")]
    if strategy == "dgrid":
        return [("setup", "⚙️ Core"), ("regime", "⚡ Regime"), ("risk", "🛡 Risk")]
    if strategy == "mid":
        # Mid Mode is intentionally lean (Tread parity): Core (margin/spread/levels)
        # + Risk (TP/SL). No regime, no momentum, no soft-reset, no anchor.
        return [("setup", "⚙️ Core"), ("risk", "🛡 Risk")]
    if strategy == "dn":
        return [("setup", "⚙️ Core"), ("safety", "🛡 Safety")]
    return [("setup", "⚙️ Core")]


def _strategy_section_for_field(strategy: str, field: str) -> str:
    if strategy == "vol":
        return "direction" if field == "vol_direction" else "risk"
    if strategy == "grid":
        if field in {"min_spread_bp", "max_spread_bp"}:
            return "execution"
        if field in {"cycle_notional_usd", "inventory_soft_limit_usd", "session_notional_cap_usd"}:
            return "risk"
        return "setup"
    if strategy == "rgrid":
        if field in {"rgrid_reset_threshold_pct", "rgrid_reset_timeout_seconds", "rgrid_discretion"}:
            return "reset"
        if field in {"rgrid_stop_loss_pct", "rgrid_take_profit_pct"}:
            return "risk"
        return "setup"
    if strategy == "dgrid":
        if field.startswith("dgrid_"):
            return "regime"
        if field in {"rgrid_stop_loss_pct", "rgrid_take_profit_pct", "tp_pct", "sl_pct"}:
            return "risk"
        return "setup"
    if strategy == "mid":
        if field in {"tp_pct", "sl_pct", "rgrid_stop_loss_pct", "rgrid_take_profit_pct"}:
            return "risk"
        return "setup"
    if strategy == "dn":
        return "safety" if field in {"auto_close_on_maintenance", "dn_max_drift_pct"} else "setup"
    return "setup"


def _fmt_hold_duration(seconds: int) -> str:
    """Human-friendly hold duration: 3600 -> '1h', 21600 -> '6h', 5400 -> '90m'."""
    s = max(0, int(seconds or 0))
    if s and s % 3600 == 0:
        return f"{s // 3600}h"
    if s and s % 60 == 0:
        return f"{s // 60}m"
    return f"{s}s"


def _strategy_config_menu_text(strategy: str, conf: dict, network: str) -> str:
    titles = {
        "grid": "GRID",
        "rgrid": "Reverse GRID",
        "dgrid": "Dynamic GRID",
        "mid": "Mid Mode",
        "dn": "Delta Neutral",
        "vol": "Volume Bot",
    }
    return (
        f"⚙️ *{escape_md(titles.get(strategy, strategy.upper()))} Advanced*\n\n"
        f"Mode: *{escape_md(network.upper())}*\n\n"
        "Choose one section below to edit\\. This keeps the setup clean and focused\\."
    )


def _strategy_config_menu_kb(strategy: str):
    rows = []
    section_buttons = [
        InlineKeyboardButton(label, callback_data=f"strategy:config_section:{strategy}:{section}")
        for section, label in _strategy_config_sections(strategy)
    ]
    for i in range(0, len(section_buttons), 2):
        rows.append(section_buttons[i:i + 2])
    rows.append([InlineKeyboardButton("◀ Back", callback_data=f"strategy:preview:{strategy}")])
    return InlineKeyboardMarkup(rows)


def _mm_effective_leverage(conf: dict) -> float:
    """Display-side mirror of engine_runtime._effective_leverage: an explicit
    mm_leverage_override wins, else the MM default (3x). Used so the card shows
    the SAME leverage the engine will deploy."""
    override = float(conf.get("mm_leverage_override", 0) or 0)
    return override if override >= 1 else 3.0


def _mm_sizing_line(conf: dict) -> str:
    """'Leverage Nx -> Position $X (margin x lev)' + preset label for the Core
    card, so picking Tiny/Standard or a leverage button VISIBLY changes the card
    and the resulting position size (the #4 'nothing changes' fix)."""
    margin = float(conf.get("notional_usd", 100.0) or 0.0)
    override = float(conf.get("mm_leverage_override", 0) or 0)
    eff_lev = _mm_effective_leverage(conf)
    deployed = margin * eff_lev
    preset = str(conf.get("mm_preset") or "").lower()
    preset_label = {"tiny": "Tiny Budget", "standard": "Standard"}.get(preset, "—")
    src = "preset/custom" if override >= 1 else "default"
    return (
        f"Leverage: *{escape_md(f'{eff_lev:.0f}x')}* \\({escape_md(src)}\\) \\| "
        f"Position: *{escape_md(f'${deployed:,.0f}')}* \\(margin×lev\\)\n"
        f"Preset: *{escape_md(preset_label)}*"
    )


def _strategy_config_section_text(strategy: str, conf: dict, network: str, section: str) -> str:
    if strategy == "vol":
        direction = "SHORT" if str(conf.get("vol_direction", "long")).lower() == "short" else "LONG"
        tp_pct = float(conf.get("tp_pct", 1.0))
        sl_pct = float(conf.get("sl_pct", 1.0))
        if section == "direction":
            return (
                "⚙️ *Volume Bot · Direction*\n\n"
                f"Mode: *{escape_md(network.upper())}*\n"
                f"Current direction: *{escape_md(direction)}*\n\n"
                "Pick the side you want the volume loop to favor\\."
            )
        return (
            "⚙️ *Volume Bot · TP / SL*\n\n"
            f"Current TP/SL: *{escape_md(f'{tp_pct:.2f}% / {sl_pct:.2f}%')}*\n\n"
            "Choose quick presets or set custom values\\."
        )

    notional = float(conf.get("notional_usd", 100.0))
    spread_bp = float(conf.get("spread_bp", 5.0))
    interval_seconds = int(conf.get("interval_seconds", 60))
    tp_pct = float(conf.get("tp_pct", 1.0))
    sl_pct = float(conf.get("sl_pct", 0.5))

    if strategy == "grid":
        if section == "execution":
            threshold_str = f"{float(conf.get('threshold_bp', 12.0)):.1f} bp"
            close_offset_str = f"{float(conf.get('close_offset_bp', 24.0)):.1f} bp"
            ref_mode = str(conf.get("reference_mode", "ema_fast")).upper()
            bias = str(conf.get("directional_bias", "neutral")).upper()
            pov_label = str(conf.get("participation_preset") or "OFF").upper()
            return (
                "⚙️ *GRID · Execution*\n\n"
                f"Threshold: *{escape_md(threshold_str)}* \\| "
                f"Close offset: *{escape_md(close_offset_str)}*\n"
                f"Reference: *{escape_md(ref_mode)}* \\| "
                f"Bias: *{escape_md(bias)}*\n"
                f"POV: *{escape_md(pov_label)}* \\(per\\-cycle pacing from Nado 24h volume\\)\n\n"
                "Tune how quotes react to the market\\."
            )
        if section == "risk":
            cycle_budget = f"${float(conf.get('cycle_notional_usd', notional)):,.0f}"
            inventory_limit = f"${float(conf.get('inventory_soft_limit_usd', notional * 0.6)):,.0f}"
            ttl_str = f"{int(conf.get('quote_ttl_seconds', 90))}s"
            session_cap_value = float(conf.get("session_notional_cap_usd", 0) or 0)
            session_cap = f"${session_cap_value:,.0f}" if session_cap_value > 0 else "OFF"
            return (
                "⚙️ *GRID · Risk*\n\n"
                f"PnL TP/SL: *{escape_md(f'{tp_pct:.2f}% / {sl_pct:.2f}%')}* of margin\n"
                f"Cycle budget: *{escape_md(cycle_budget)}* \\| "
                f"Inventory limit: *{escape_md(inventory_limit)}*\n"
                f"TTL: *{escape_md(ttl_str)}* \\| "
                f"Session cap: *{escape_md(session_cap)}*\n\n"
                "Control downside and pacing here\\. These are PnL stops, not raw price\\-move stops\\."
            )
        return (
            "⚙️ *GRID · Core*\n\n"
            f"Margin: *{escape_md(f'${notional:,.0f}')}* \\| Spread: *{escape_md(f'{spread_bp:.1f} bp')}* \\| Interval: *{escape_md(f'{interval_seconds}s')}*\n"
            f"{_mm_sizing_line(conf)}\n\n"
            "Set the main loop size and cadence\\. *Position size \\= margin × leverage*\\."
        )

    if strategy == "rgrid":
        if section == "risk":
            pnl_sl = f"{float(conf.get('rgrid_stop_loss_pct', sl_pct)):.2f}%"
            pnl_tp = f"{float(conf.get('rgrid_take_profit_pct', tp_pct)):.2f}%"
            return (
                "⚙️ *Reverse GRID · Risk*\n\n"
                f"PnL stop: *{escape_md(pnl_sl)}* \\| "
                f"PnL take profit: *{escape_md(pnl_tp)}*\n\n"
                "Set when the strategy should cut or lock gains based on realized/open PnL, not raw market drift\\."
            )
        if section == "reset":
            reset_threshold = f"{float(conf.get('rgrid_reset_threshold_pct', 1.0)):.2f}%"
            reset_timeout = f"{int(conf.get('rgrid_reset_timeout_seconds', 120))}s"
            discretion = f"{float(conf.get('rgrid_discretion', 0.06)):.2f}"
            return (
                "⚙️ *Reverse GRID · Reset*\n\n"
                f"Reset threshold: *{escape_md(reset_threshold)}* \\| "
                f"Timeout: *{escape_md(reset_timeout)}*\n"
                f"Discretion: *{escape_md(discretion)}*\n\n"
                "Use these only if you want tighter re-anchoring\\."
            )
        levels = str(int(conf.get("levels", 4)))
        rgrid_spread = f"{float(conf.get('rgrid_spread_bp', spread_bp)):.1f} bp"
        pov_label = str(conf.get("participation_preset") or "OFF").upper()
        return (
            "⚙️ *Reverse GRID · Core*\n\n"
            f"Margin: *{escape_md(f'${notional:,.0f}')}* \\| Interval: *{escape_md(f'{interval_seconds}s')}*\n"
            f"Levels: *{escape_md(levels)}* \\| Spread: *{escape_md(rgrid_spread)}*\n"
            f"POV: *{escape_md(pov_label)}*\n"
            f"{_mm_sizing_line(conf)}\n\n"
            "Set the basic breakout loop here\\. *Position size \\= margin × leverage*\\."
        )

    if strategy == "dgrid":
        if section == "regime":
            trend_on = float(conf.get("dgrid_trend_on_variance_ratio", 1.25))
            range_on = float(conf.get("dgrid_range_on_variance_ratio", 1.15))
            min_spread = float(conf.get("dgrid_min_spread_bp", 2.0))
            max_spread = float(conf.get("dgrid_max_spread_bp", 50.0))
            return (
                "⚡ *Dynamic GRID · Regime*\n\n"
                f"Switch to RGRID: *{escape_md(f'{trend_on:.2f}')}* variance ratio\n"
                f"Switch to GRID: *{escape_md(f'{range_on:.2f}')}* variance ratio\n"
                f"Spread band: *{escape_md(f'{min_spread:.1f} - {max_spread:.1f} bp')}*\n\n"
                "DGRID uses hysteresis so it does not flip\\-flop in mixed regimes\\."
            )
        if section == "risk":
            pnl_sl = f"{float(conf.get('rgrid_stop_loss_pct', sl_pct)):.2f}%"
            pnl_tp = f"{float(conf.get('rgrid_take_profit_pct', tp_pct)):.2f}%"
            return (
                "⚡ *Dynamic GRID · Risk*\n\n"
                f"PnL stop: *{escape_md(pnl_sl)}* \\| PnL take profit: *{escape_md(pnl_tp)}*\n\n"
                "GRID phase watches realized PnL; RGRID phase watches open exposure risk\\."
            )
        levels = str(int(conf.get("levels", 4)))
        pov_label = str(conf.get("participation_preset") or "OFF").upper()
        return (
            "⚡ *Dynamic GRID · Core*\n\n"
            f"Margin: *{escape_md(f'${notional:,.0f}')}* \\| Interval: *{escape_md(f'{interval_seconds}s')}*\n"
            f"Levels: *{escape_md(levels)}* \\| Starting spread: *{escape_md(f'{spread_bp:.1f} bp')}*\n"
            f"POV: *{escape_md(pov_label)}*\n"
            f"{_mm_sizing_line(conf)}\n\n"
            "DGRID auto\\-switches GRID↔RGRID, ladders into the move, trails take\\-profit, "
            "and flips long↔short on a confirmed reversal\\. *Position size \\= margin × leverage*\\."
        )

    if strategy == "mid":
        # Mid Mode: lean Tread parity — pure mid ± spread×level. No anchor /
        # no soft-reset. Bias is a continuous float; we render it numerically.
        levels = str(int(conf.get("levels", 2)))
        ref_mode = str(conf.get("reference_mode", "mid")).upper()
        try:
            bias_val = float(conf.get("directional_bias", 0.0) or 0.0)
        except (TypeError, ValueError):
            bias_val = 0.0
        bias_str = f"{bias_val:+.2f}"
        if section == "risk":
            return (
                "⚙️ *MID MODE · Risk*\n\n"
                f"PnL TP/SL: *{escape_md(f'{tp_pct:.2f}% / {sl_pct:.2f}%')}* of margin\n\n"
                "Stops are applied to *margin* \\(notional / leverage\\) per Tread spec\\."
            )
        pov_label = str(conf.get("participation_preset") or "OFF").upper()
        return (
            "⚙️ *MID MODE · Core*\n\n"
            f"Margin: *{escape_md(f'${notional:,.0f}')}* \\| Interval: *{escape_md(f'{interval_seconds}s')}*\n"
            f"Spread: *{escape_md(f'{spread_bp:+.1f} bp')}* \\| Levels: *{escape_md(levels)}*\n"
            f"Reference: *{escape_md(ref_mode)}* \\| Bias: *{escape_md(bias_str)}*\n"
            f"POV: *{escape_md(pov_label)}* \\(per\\-cycle pacing from Nado 24h volume\\)\n"
            f"{_mm_sizing_line(conf)}\n\n"
            "Pure mid ± spread×level\\. No anchor, no soft\\-reset\\. "
            "Bias range −1\\.0 → \\+1\\.0; \\|1\\.0\\| adds 20% margin\\."
        )

    if strategy == "dn":
        leg_size = float(conf.get("fixed_margin_usd", conf.get("notional_usd", 100.0)) or 100.0)
        hold_s = int(conf.get("dn_hold_seconds", 3600) or 3600)
        cycles = int(conf.get("dn_cycles", 1) or 1)
        drift_pct = float(conf.get("dn_max_drift_pct", 5.0) or 5.0)
        if section == "safety":
            auto_close = "ON" if float(conf.get("auto_close_on_maintenance", 1) or 0) >= 0.5 else "OFF"
            return (
                "⚙️ *Delta Neutral · Safety*\n\n"
                f"Hedge drift gate: *{escape_md(f'{drift_pct:.1f}%')}*\n"
                f"Auto-close on maintenance: *{escape_md(auto_close)}*\n\n"
                "If the two legs drift apart by more than the gate, both are closed immediately\\."
            )
        return (
            "⚙️ *Delta Neutral · Core*\n\n"
            f"Size \\(per leg\\): *{escape_md(f'${leg_size:,.0f}')}* \\| Short: *1x*\n"
            f"Hold: *{escape_md(_fmt_hold_duration(hold_s))}* \\| Cycles: *{escape_md(str(cycles))}*\n\n"
            "Buys spot \\+ 1x\\-shorts the perp, holds, then exits both legs together, repeated per cycle\\."
        )

    return _fmt_strategy_config_text(strategy, conf, network)


def _strategy_config_section_kb(strategy: str, section: str):
    if strategy == "vol":
        # Volume is spot-only as of 2026-05. The user-tunable params are:
        # session margin (per-cycle notional), stop loss %, target volume.
        rows = [
            [
                InlineKeyboardButton("Margin $100", callback_data="strategy:set:vol:session_margin_usd:100"),
                InlineKeyboardButton("Margin $500", callback_data="strategy:set:vol:session_margin_usd:500"),
                InlineKeyboardButton("Margin $1000", callback_data="strategy:set:vol:session_margin_usd:1000"),
            ],
            [
                InlineKeyboardButton("✍️ Custom Margin", callback_data="strategy:input:vol:session_margin_usd"),
            ],
            [
                InlineKeyboardButton("SL 0.5%", callback_data="strategy:set:vol:sl_pct:0.5"),
                InlineKeyboardButton("SL 1.0%", callback_data="strategy:set:vol:sl_pct:1.0"),
                InlineKeyboardButton("SL 2.0%", callback_data="strategy:set:vol:sl_pct:2.0"),
            ],
            [
                InlineKeyboardButton("✍️ Custom SL", callback_data="strategy:input:vol:sl_pct"),
            ],
            [
                InlineKeyboardButton("Target $10k", callback_data="strategy:set:vol:target_volume_usd:10000"),
                InlineKeyboardButton("Target $25k", callback_data="strategy:set:vol:target_volume_usd:25000"),
                InlineKeyboardButton("Target $100k", callback_data="strategy:set:vol:target_volume_usd:100000"),
            ],
            [
                InlineKeyboardButton("✍️ Custom Target", callback_data="strategy:input:vol:target_volume_usd"),
            ],
        ]
        rows.append([InlineKeyboardButton("◀ Back", callback_data="strategy:config:vol")])
        return InlineKeyboardMarkup(rows)

    rows: list[list[InlineKeyboardButton]] = []
    if strategy == "grid":
        if section == "setup":
            rows = [
                [
                    InlineKeyboardButton("🎯 Tiny Budget Preset", callback_data="strategy:preset:grid:tiny"),
                    InlineKeyboardButton("Standard", callback_data="strategy:preset:grid:standard"),
                ],
                [
                    InlineKeyboardButton("Margin $50", callback_data="strategy:set:grid:notional_usd:50"),
                    InlineKeyboardButton("Margin $100", callback_data="strategy:set:grid:notional_usd:100"),
                    InlineKeyboardButton("Margin $250", callback_data="strategy:set:grid:notional_usd:250"),
                ],
                [
                    InlineKeyboardButton("Lev 1x", callback_data="strategy:set:grid:mm_leverage_override:1"),
                    InlineKeyboardButton("3x", callback_data="strategy:set:grid:mm_leverage_override:3"),
                    InlineKeyboardButton("5x", callback_data="strategy:set:grid:mm_leverage_override:5"),
                    InlineKeyboardButton("10x", callback_data="strategy:set:grid:mm_leverage_override:10"),
                ],
                [
                    InlineKeyboardButton("Spread 2bp", callback_data="strategy:set:grid:spread_bp:2"),
                    InlineKeyboardButton("Spread 5bp", callback_data="strategy:set:grid:spread_bp:5"),
                    InlineKeyboardButton("Spread 10bp", callback_data="strategy:set:grid:spread_bp:10"),
                ],
                [
                    InlineKeyboardButton("30s", callback_data="strategy:set:grid:interval_seconds:30"),
                    InlineKeyboardButton("60s", callback_data="strategy:set:grid:interval_seconds:60"),
                    InlineKeyboardButton("120s", callback_data="strategy:set:grid:interval_seconds:120"),
                ],
                [
                    InlineKeyboardButton("⚡ Aggressive", callback_data="strategy:set_text:grid:participation_preset:aggressive"),
                    InlineKeyboardButton("Normal", callback_data="strategy:set_text:grid:participation_preset:normal"),
                    InlineKeyboardButton("Passive", callback_data="strategy:set_text:grid:participation_preset:passive"),
                ],
                [
                    InlineKeyboardButton("Duration 30m", callback_data="strategy:set:grid:mm_duration_minutes:30"),
                    InlineKeyboardButton("2h", callback_data="strategy:set:grid:mm_duration_minutes:120"),
                    InlineKeyboardButton("✍️ Custom Duration", callback_data="strategy:input:grid:mm_duration_minutes"),
                ],
                [
                    InlineKeyboardButton("Pause: Off", callback_data="strategy:set:grid:twap_pause_move_bp:0"),
                    InlineKeyboardButton("1%", callback_data="strategy:set:grid:twap_pause_move_bp:100"),
                    InlineKeyboardButton("2%", callback_data="strategy:set:grid:twap_pause_move_bp:200"),
                    InlineKeyboardButton("✍️", callback_data="strategy:input:grid:twap_pause_move_bp"),
                ],
                [
                    InlineKeyboardButton("Custom Margin", callback_data="strategy:input:grid:notional_usd"),
                    InlineKeyboardButton("Custom Lev", callback_data="strategy:input:grid:mm_leverage_override"),
                    InlineKeyboardButton("Custom Interval", callback_data="strategy:input:grid:interval_seconds"),
                ],
            ]
        elif section == "execution":
            # Spread bounds: the floor/cap the ATR auto-spread clamps to (and the
            # manual-spread floor). Replaces the previous dead controls
            # (threshold/close-offset/reference-mode/bias/POV — none were wired).
            rows = [
                [
                    InlineKeyboardButton("Min Spread 2bp", callback_data="strategy:set:grid:min_spread_bp:2"),
                    InlineKeyboardButton("5bp", callback_data="strategy:set:grid:min_spread_bp:5"),
                    InlineKeyboardButton("10bp", callback_data="strategy:set:grid:min_spread_bp:10"),
                ],
                [
                    InlineKeyboardButton("Max Spread 30bp", callback_data="strategy:set:grid:max_spread_bp:30"),
                    InlineKeyboardButton("50bp", callback_data="strategy:set:grid:max_spread_bp:50"),
                    InlineKeyboardButton("100bp", callback_data="strategy:set:grid:max_spread_bp:100"),
                ],
                [
                    InlineKeyboardButton("Custom Min Spread", callback_data="strategy:input:grid:min_spread_bp"),
                    InlineKeyboardButton("Custom Max Spread", callback_data="strategy:input:grid:max_spread_bp"),
                ],
            ]
        else:
            rows = [
                [
                    InlineKeyboardButton("TP 0.5%", callback_data="strategy:set:grid:tp_pct:0.5"),
                    InlineKeyboardButton("TP 1.0%", callback_data="strategy:set:grid:tp_pct:1.0"),
                    InlineKeyboardButton("TP 2.0%", callback_data="strategy:set:grid:tp_pct:2.0"),
                ],
                [
                    InlineKeyboardButton("SL 0.25%", callback_data="strategy:set:grid:sl_pct:0.25"),
                    InlineKeyboardButton("SL 0.5%", callback_data="strategy:set:grid:sl_pct:0.5"),
                    InlineKeyboardButton("SL 1.0%", callback_data="strategy:set:grid:sl_pct:1.0"),
                ],
                [
                    InlineKeyboardButton("Cycle $50", callback_data="strategy:set:grid:cycle_notional_usd:50"),
                    InlineKeyboardButton("Cycle $100", callback_data="strategy:set:grid:cycle_notional_usd:100"),
                    InlineKeyboardButton("Cycle $250", callback_data="strategy:set:grid:cycle_notional_usd:250"),
                ],
                [
                    InlineKeyboardButton("Inv $30", callback_data="strategy:set:grid:inventory_soft_limit_usd:30"),
                    InlineKeyboardButton("Inv $60", callback_data="strategy:set:grid:inventory_soft_limit_usd:60"),
                ],
                [
                    InlineKeyboardButton("Reset 0.8%", callback_data="strategy:set:grid:grid_reset_threshold_pct:0.8"),
                    InlineKeyboardButton("1.5%", callback_data="strategy:set:grid:grid_reset_threshold_pct:1.5"),
                ],
                [
                    InlineKeyboardButton("Custom TP", callback_data="strategy:input:grid:tp_pct"),
                    InlineKeyboardButton("Custom SL", callback_data="strategy:input:grid:sl_pct"),
                ],
                [
                    InlineKeyboardButton("Session Cap", callback_data="strategy:input:grid:session_notional_cap_usd"),
                    InlineKeyboardButton("Custom Reset", callback_data="strategy:input:grid:grid_reset_threshold_pct"),
                ],
            ]
    elif strategy == "dgrid":
        if section == "setup":
            rows = [
                [
                    InlineKeyboardButton("🎯 Tiny Budget Preset", callback_data="strategy:preset:dgrid:tiny"),
                    InlineKeyboardButton("Standard", callback_data="strategy:preset:dgrid:standard"),
                ],
                [
                    InlineKeyboardButton("Margin $50", callback_data="strategy:set:dgrid:notional_usd:50"),
                    InlineKeyboardButton("Margin $100", callback_data="strategy:set:dgrid:notional_usd:100"),
                    InlineKeyboardButton("Margin $250", callback_data="strategy:set:dgrid:notional_usd:250"),
                ],
                [
                    InlineKeyboardButton("Lev 1x", callback_data="strategy:set:dgrid:mm_leverage_override:1"),
                    InlineKeyboardButton("3x", callback_data="strategy:set:dgrid:mm_leverage_override:3"),
                    InlineKeyboardButton("5x", callback_data="strategy:set:dgrid:mm_leverage_override:5"),
                    InlineKeyboardButton("10x", callback_data="strategy:set:dgrid:mm_leverage_override:10"),
                ],
                [
                    InlineKeyboardButton("Levels 3", callback_data="strategy:set:dgrid:levels:3"),
                    InlineKeyboardButton("Levels 4", callback_data="strategy:set:dgrid:levels:4"),
                    InlineKeyboardButton("Levels 6", callback_data="strategy:set:dgrid:levels:6"),
                ],
                [
                    InlineKeyboardButton("30s", callback_data="strategy:set:dgrid:interval_seconds:30"),
                    InlineKeyboardButton("60s", callback_data="strategy:set:dgrid:interval_seconds:60"),
                ],
                [
                    InlineKeyboardButton("⚡ Aggressive", callback_data="strategy:set_text:dgrid:participation_preset:aggressive"),
                    InlineKeyboardButton("Normal", callback_data="strategy:set_text:dgrid:participation_preset:normal"),
                    InlineKeyboardButton("Passive", callback_data="strategy:set_text:dgrid:participation_preset:passive"),
                ],
                [
                    InlineKeyboardButton("Duration 30m", callback_data="strategy:set:dgrid:mm_duration_minutes:30"),
                    InlineKeyboardButton("2h", callback_data="strategy:set:dgrid:mm_duration_minutes:120"),
                    InlineKeyboardButton("✍️ Custom Duration", callback_data="strategy:input:dgrid:mm_duration_minutes"),
                ],
                [
                    InlineKeyboardButton("Pause: Off", callback_data="strategy:set:dgrid:twap_pause_move_bp:0"),
                    InlineKeyboardButton("1%", callback_data="strategy:set:dgrid:twap_pause_move_bp:100"),
                    InlineKeyboardButton("2%", callback_data="strategy:set:dgrid:twap_pause_move_bp:200"),
                    InlineKeyboardButton("✍️", callback_data="strategy:input:dgrid:twap_pause_move_bp"),
                ],
                [
                    InlineKeyboardButton("Custom Margin", callback_data="strategy:input:dgrid:notional_usd"),
                    InlineKeyboardButton("Custom Interval", callback_data="strategy:input:dgrid:interval_seconds"),
                ],
            ]
        elif section == "regime":
            rows = [
                [
                    InlineKeyboardButton("Trend 1.25", callback_data="strategy:set:dgrid:dgrid_trend_on_variance_ratio:1.25"),
                    InlineKeyboardButton("Trend 1.50", callback_data="strategy:set:dgrid:dgrid_trend_on_variance_ratio:1.50"),
                ],
                [
                    InlineKeyboardButton("Range 1.15", callback_data="strategy:set:dgrid:dgrid_range_on_variance_ratio:1.15"),
                    InlineKeyboardButton("Range 1.05", callback_data="strategy:set:dgrid:dgrid_range_on_variance_ratio:1.05"),
                ],
                [
                    InlineKeyboardButton("Spread 2bp", callback_data="strategy:set:dgrid:dgrid_spread_bp:2"),
                    InlineKeyboardButton("Spread 8bp", callback_data="strategy:set:dgrid:dgrid_spread_bp:8"),
                    InlineKeyboardButton("Spread 15bp", callback_data="strategy:set:dgrid:dgrid_spread_bp:15"),
                ],
                [
                    InlineKeyboardButton("Min Spread 2bp", callback_data="strategy:set:dgrid:dgrid_min_spread_bp:2"),
                    InlineKeyboardButton("Max 30bp", callback_data="strategy:set:dgrid:dgrid_max_spread_bp:30"),
                    InlineKeyboardButton("Max 50bp", callback_data="strategy:set:dgrid:dgrid_max_spread_bp:50"),
                ],
                [
                    InlineKeyboardButton("Custom Trend", callback_data="strategy:input:dgrid:dgrid_trend_on_variance_ratio"),
                    InlineKeyboardButton("✍️ Custom Spread", callback_data="strategy:input:dgrid:dgrid_spread_bp"),
                ],
            ]
        else:
            rows = [
                [
                    InlineKeyboardButton("PnL SL 0.5%", callback_data="strategy:set:dgrid:rgrid_stop_loss_pct:0.5"),
                    InlineKeyboardButton("1.0%", callback_data="strategy:set:dgrid:rgrid_stop_loss_pct:1.0"),
                ],
                [
                    InlineKeyboardButton("PnL TP 1.5%", callback_data="strategy:set:dgrid:rgrid_take_profit_pct:1.5"),
                    InlineKeyboardButton("2.0%", callback_data="strategy:set:dgrid:rgrid_take_profit_pct:2.0"),
                ],
                [
                    InlineKeyboardButton("Custom PnL SL", callback_data="strategy:input:dgrid:rgrid_stop_loss_pct"),
                    InlineKeyboardButton("Custom PnL TP", callback_data="strategy:input:dgrid:rgrid_take_profit_pct"),
                ],
            ]
    elif strategy == "rgrid":
        if section == "setup":
            rows = [
                [
                    InlineKeyboardButton("🎯 Tiny Budget Preset", callback_data="strategy:preset:rgrid:tiny"),
                    InlineKeyboardButton("Standard", callback_data="strategy:preset:rgrid:standard"),
                ],
                [
                    InlineKeyboardButton("Margin $50", callback_data="strategy:set:rgrid:notional_usd:50"),
                    InlineKeyboardButton("Margin $100", callback_data="strategy:set:rgrid:notional_usd:100"),
                    InlineKeyboardButton("Margin $250", callback_data="strategy:set:rgrid:notional_usd:250"),
                ],
                [
                    InlineKeyboardButton("Lev 1x", callback_data="strategy:set:rgrid:mm_leverage_override:1"),
                    InlineKeyboardButton("3x", callback_data="strategy:set:rgrid:mm_leverage_override:3"),
                    InlineKeyboardButton("5x", callback_data="strategy:set:rgrid:mm_leverage_override:5"),
                    InlineKeyboardButton("10x", callback_data="strategy:set:rgrid:mm_leverage_override:10"),
                ],
                [
                    InlineKeyboardButton("Levels 3", callback_data="strategy:set:rgrid:levels:3"),
                    InlineKeyboardButton("Levels 5", callback_data="strategy:set:rgrid:levels:5"),
                    InlineKeyboardButton("Levels 7", callback_data="strategy:set:rgrid:levels:7"),
                ],
                [
                    InlineKeyboardButton("Spread 5bp", callback_data="strategy:set:rgrid:rgrid_spread_bp:5"),
                    InlineKeyboardButton("10bp", callback_data="strategy:set:rgrid:rgrid_spread_bp:10"),
                    InlineKeyboardButton("20bp", callback_data="strategy:set:rgrid:rgrid_spread_bp:20"),
                ],
                [
                    InlineKeyboardButton("60s", callback_data="strategy:set:rgrid:interval_seconds:60"),
                    InlineKeyboardButton("120s", callback_data="strategy:set:rgrid:interval_seconds:120"),
                ],
                [
                    InlineKeyboardButton("🌊 Trend-follow (default)", callback_data="strategy:set:rgrid:fill_anchored:1"),
                    InlineKeyboardButton("📊 Classic ladder", callback_data="strategy:set:rgrid:fill_anchored:0"),
                ],
                [
                    InlineKeyboardButton("Discretion 0.06", callback_data="strategy:set:rgrid:rgrid_discretion:0.06"),
                    InlineKeyboardButton("0.12", callback_data="strategy:set:rgrid:rgrid_discretion:0.12"),
                    InlineKeyboardButton("0.25", callback_data="strategy:set:rgrid:rgrid_discretion:0.25"),
                ],
                [
                    InlineKeyboardButton("Custom Levels", callback_data="strategy:input:rgrid:levels"),
                    InlineKeyboardButton("Custom Spread", callback_data="strategy:input:rgrid:rgrid_spread_bp"),
                ],
                [
                    InlineKeyboardButton("⚡ Aggressive", callback_data="strategy:set_text:rgrid:participation_preset:aggressive"),
                    InlineKeyboardButton("Normal", callback_data="strategy:set_text:rgrid:participation_preset:normal"),
                    InlineKeyboardButton("Passive", callback_data="strategy:set_text:rgrid:participation_preset:passive"),
                ],
                [
                    InlineKeyboardButton("Duration 30m (target)", callback_data="strategy:set:rgrid:mm_duration_minutes:30"),
                    InlineKeyboardButton("✍️ Custom Duration", callback_data="strategy:input:rgrid:mm_duration_minutes"),
                ],
                [
                    InlineKeyboardButton("Custom Margin", callback_data="strategy:input:rgrid:notional_usd"),
                    InlineKeyboardButton("Custom Interval", callback_data="strategy:input:rgrid:interval_seconds"),
                ],
            ]
        elif section == "risk":
            rows = [
                [
                    InlineKeyboardButton("PnL SL 0.5%", callback_data="strategy:set:rgrid:rgrid_stop_loss_pct:0.5"),
                    InlineKeyboardButton("1.0%", callback_data="strategy:set:rgrid:rgrid_stop_loss_pct:1.0"),
                ],
                [
                    InlineKeyboardButton("PnL TP 1.5%", callback_data="strategy:set:rgrid:rgrid_take_profit_pct:1.5"),
                    InlineKeyboardButton("2.0%", callback_data="strategy:set:rgrid:rgrid_take_profit_pct:2.0"),
                ],
                [
                    InlineKeyboardButton("Custom PnL SL", callback_data="strategy:input:rgrid:rgrid_stop_loss_pct"),
                    InlineKeyboardButton("Custom PnL TP", callback_data="strategy:input:rgrid:rgrid_take_profit_pct"),
                ],
            ]
        else:
            rows = [
                [
                    InlineKeyboardButton("Reset 0.8%", callback_data="strategy:set:rgrid:rgrid_reset_threshold_pct:0.8"),
                    InlineKeyboardButton("1.5%", callback_data="strategy:set:rgrid:rgrid_reset_threshold_pct:1.5"),
                ],
                [
                    InlineKeyboardButton("Disc 0.06", callback_data="strategy:set:rgrid:rgrid_discretion:0.06"),
                    InlineKeyboardButton("Disc 0.10", callback_data="strategy:set:rgrid:rgrid_discretion:0.10"),
                ],
                [
                    InlineKeyboardButton("Custom Reset", callback_data="strategy:input:rgrid:rgrid_reset_threshold_pct"),
                    InlineKeyboardButton("Custom Disc", callback_data="strategy:input:rgrid:rgrid_discretion"),
                ],
            ]
    elif strategy == "mid":
        if section == "risk":
            rows = [
                [
                    InlineKeyboardButton("TP 0.5%", callback_data="strategy:set:mid:tp_pct:0.5"),
                    InlineKeyboardButton("TP 1.0%", callback_data="strategy:set:mid:tp_pct:1.0"),
                    InlineKeyboardButton("TP 2.0%", callback_data="strategy:set:mid:tp_pct:2.0"),
                ],
                [
                    InlineKeyboardButton("SL 0.25%", callback_data="strategy:set:mid:sl_pct:0.25"),
                    InlineKeyboardButton("SL 0.5%", callback_data="strategy:set:mid:sl_pct:0.5"),
                    InlineKeyboardButton("SL 1.0%", callback_data="strategy:set:mid:sl_pct:1.0"),
                ],
                [
                    InlineKeyboardButton("Custom TP", callback_data="strategy:input:mid:tp_pct"),
                    InlineKeyboardButton("Custom SL", callback_data="strategy:input:mid:sl_pct"),
                ],
            ]
        else:
            # Setup: Tiny Budget Preset, margin, spread (signed), levels, reference, bias.
            rows = [
                [
                    InlineKeyboardButton("🎯 Tiny Budget Preset", callback_data="strategy:preset:mid:tiny"),
                    InlineKeyboardButton("Standard", callback_data="strategy:preset:mid:standard"),
                ],
                [
                    InlineKeyboardButton("Margin $50", callback_data="strategy:set:mid:notional_usd:50"),
                    InlineKeyboardButton("Margin $100", callback_data="strategy:set:mid:notional_usd:100"),
                    InlineKeyboardButton("Margin $250", callback_data="strategy:set:mid:notional_usd:250"),
                ],
                [
                    InlineKeyboardButton("Lev 1x", callback_data="strategy:set:mid:mm_leverage_override:1"),
                    InlineKeyboardButton("3x", callback_data="strategy:set:mid:mm_leverage_override:3"),
                    InlineKeyboardButton("5x", callback_data="strategy:set:mid:mm_leverage_override:5"),
                    InlineKeyboardButton("10x", callback_data="strategy:set:mid:mm_leverage_override:10"),
                ],
                [
                    InlineKeyboardButton("Tight 2bp", callback_data="strategy:set:mid:spread_bp:2"),
                    InlineKeyboardButton("Spread 5bp", callback_data="strategy:set:mid:spread_bp:5"),
                    InlineKeyboardButton("Spread 25bp", callback_data="strategy:set:mid:spread_bp:25"),
                ],
                [
                    InlineKeyboardButton("30s", callback_data="strategy:set:mid:interval_seconds:30"),
                    InlineKeyboardButton("60s", callback_data="strategy:set:mid:interval_seconds:60"),
                    InlineKeyboardButton("120s", callback_data="strategy:set:mid:interval_seconds:120"),
                ],
                [
                    InlineKeyboardButton("Bias −0.5", callback_data="strategy:set:mid:directional_bias:-0.5"),
                    InlineKeyboardButton("Bias 0", callback_data="strategy:set:mid:directional_bias:0"),
                    InlineKeyboardButton("Bias +0.5", callback_data="strategy:set:mid:directional_bias:0.5"),
                ],
                [
                    InlineKeyboardButton("Min Spread 2bp", callback_data="strategy:set:mid:min_spread_bp:2"),
                    InlineKeyboardButton("Max 30bp", callback_data="strategy:set:mid:max_spread_bp:30"),
                    InlineKeyboardButton("Max 50bp", callback_data="strategy:set:mid:max_spread_bp:50"),
                ],
                [
                    InlineKeyboardButton("⚡ Aggressive", callback_data="strategy:set_text:mid:participation_preset:aggressive"),
                    InlineKeyboardButton("Normal", callback_data="strategy:set_text:mid:participation_preset:normal"),
                    InlineKeyboardButton("Passive", callback_data="strategy:set_text:mid:participation_preset:passive"),
                ],
                [
                    InlineKeyboardButton("Duration 30m", callback_data="strategy:set:mid:mm_duration_minutes:30"),
                    InlineKeyboardButton("2h", callback_data="strategy:set:mid:mm_duration_minutes:120"),
                    InlineKeyboardButton("✍️ Custom Duration", callback_data="strategy:input:mid:mm_duration_minutes"),
                ],
                [
                    InlineKeyboardButton("Pause: Off", callback_data="strategy:set:mid:twap_pause_move_bp:0"),
                    InlineKeyboardButton("1%", callback_data="strategy:set:mid:twap_pause_move_bp:100"),
                    InlineKeyboardButton("2%", callback_data="strategy:set:mid:twap_pause_move_bp:200"),
                    InlineKeyboardButton("✍️", callback_data="strategy:input:mid:twap_pause_move_bp"),
                ],
                [
                    InlineKeyboardButton("Custom Margin", callback_data="strategy:input:mid:notional_usd"),
                    InlineKeyboardButton("Custom Spread", callback_data="strategy:input:mid:spread_bp"),
                ],
                [
                    InlineKeyboardButton("Custom Bias", callback_data="strategy:input:mid:directional_bias"),
                ],
            ]
    elif strategy == "dn":
        if section == "setup":
            # Engine-v2 Delta Neutral knobs: per-leg size, hold duration, and how
            # many open→hold→close cycles to run. The short is strictly 1x, so
            # there is no leverage control here.
            rows = [
                [
                    InlineKeyboardButton("Size $50", callback_data="strategy:set:dn:fixed_margin_usd:50"),
                    InlineKeyboardButton("Size $100", callback_data="strategy:set:dn:fixed_margin_usd:100"),
                    InlineKeyboardButton("Size $250", callback_data="strategy:set:dn:fixed_margin_usd:250"),
                ],
                [
                    InlineKeyboardButton("Min hold 1h", callback_data="strategy:set:dn:dn_hold_seconds:3600"),
                    InlineKeyboardButton("Min hold 6h", callback_data="strategy:set:dn:dn_hold_seconds:21600"),
                    InlineKeyboardButton("Min hold 24h", callback_data="strategy:set:dn:dn_hold_seconds:86400"),
                ],
                [
                    InlineKeyboardButton("1 Cycle", callback_data="strategy:set:dn:dn_cycles:1"),
                    InlineKeyboardButton("5 Cycles", callback_data="strategy:set:dn:dn_cycles:5"),
                    InlineKeyboardButton("10 Cycles", callback_data="strategy:set:dn:dn_cycles:10"),
                ],
                [
                    InlineKeyboardButton("✍️ Custom Size", callback_data="strategy:input:dn:fixed_margin_usd"),
                    InlineKeyboardButton("✍️ Custom Hold (s)", callback_data="strategy:input:dn:dn_hold_seconds"),
                ],
            ]
        else:
            rows = [
                [
                    InlineKeyboardButton("Drift 3%", callback_data="strategy:set:dn:dn_max_drift_pct:3"),
                    InlineKeyboardButton("Drift 5%", callback_data="strategy:set:dn:dn_max_drift_pct:5"),
                    InlineKeyboardButton("Drift 10%", callback_data="strategy:set:dn:dn_max_drift_pct:10"),
                ],
                [
                    InlineKeyboardButton("Auto-Close ON", callback_data="strategy:set:dn:auto_close_on_maintenance:1"),
                    InlineKeyboardButton("Auto-Close OFF", callback_data="strategy:set:dn:auto_close_on_maintenance:0"),
                ],
            ]

    rows.append([InlineKeyboardButton("◀ Back", callback_data=f"strategy:config:{strategy}")])
    return InlineKeyboardMarkup(rows)


def _build_bro_preview_text(telegram_id: int) -> str:
    network, settings = get_user_settings(telegram_id)
    conf = settings.get("strategies", {}).get("bro", {})
    budget = float(conf.get("budget_usd", 500))
    risk_level = conf.get("risk_level", "balanced")
    max_positions = int(conf.get("max_positions", 3))
    leverage_cap = int(conf.get("leverage_cap", 5))
    tp_pct = float(conf.get("tp_pct", 2.0))
    sl_pct = float(conf.get("sl_pct", 1.5))
    min_confidence = float(conf.get("min_confidence", 0.65))
    products = conf.get("products", get_perp_products()[:6] or ["BTC", "ETH", "SOL"])
    max_loss = float(conf.get("max_loss_pct", 15))
    cycle_seconds = int(conf.get("cycle_seconds", 300))

    available_margin = 0.0
    client = get_user_readonly_client(telegram_id)
    if client:
        try:
            bal = client.get_balance()
            if bal and bal.get("exists"):
                available_margin = float((bal.get("balances", {}) or {}).get(0, 0) or 0)
                if available_margin == 0:
                    available_margin = float((bal.get("balances", {}) or {}).get("0", 0) or 0)
        except Exception:
            pass

    from src.nadobro.services.bot_runtime import get_user_bot_status
    bot_status = get_user_bot_status(telegram_id)
    is_running = bool(bot_status.get("running") and bot_status.get("strategy") == "bro")
    if is_running:
        status_emoji = "⏸️" if bool(bot_status.get("is_paused")) else "🟢"
        status_label = "PAUSED" if bool(bot_status.get("is_paused")) else "LIVE"
    else:
        status_emoji = "🟠"
        status_label = "READY"

    wallet_ready, _wallet_msg = ensure_active_wallet_ready(telegram_id)
    wallet_info = get_user_wallet_info(telegram_id, verify_signer=False) or {}
    wallet_addr = str(wallet_info.get("active_address") or "")
    wallet_short = "N/A"
    if wallet_addr:
        wallet_short = f"{wallet_addr[:6]}...{wallet_addr[-4:]}" if len(wallet_addr) >= 10 else wallet_addr
    account_status = "✅ Connected" if wallet_ready else "⚠️ Setup Needed"

    risk_emoji = {"conservative": "🛡️", "balanced": "⚖️", "aggressive": "🔥"}.get(risk_level, "⚖️")
    products_str = ", ".join(products)
    session_volume = float(bot_status.get("session_volume_usd") or 0.0)
    session_pnl = float((bot_status.get("bro_state") or {}).get("total_pnl") or 0.0)
    trade_count = int((bot_status.get("bro_state") or {}).get("trade_count") or 0)
    active_positions = len((bot_status.get("bro_state") or {}).get("active_positions") or [])
    warning = ""
    if not wallet_ready:
        warning = "⚠️ Open Wallet to link your 1CT signer and fund this mode\\."
    elif available_margin < budget * 0.2:
        warning = f"⚠️ Keep at least {escape_md(f'${budget * 0.2:,.2f}')} available for BRO allocations\\."

    return (
        "🧠 *Alpha Agent Dashboard*\n"
        f"Status: {status_emoji} *{escape_md(status_label)}*\n\n"
        "🔑 *Account*\n"
        f"• Status: *{escape_md(account_status)}*\n"
        f"• Wallet: `{escape_md(wallet_short)}`\n"
        f"• Balance: *{escape_md(f'${available_margin:,.2f}')}*\n\n"
        "⚙️ *Configuration*\n"
        f"• Budget: *{escape_md(f'${budget:,.0f}')}*\n"
        f"• Risk: {escape_md(risk_emoji)} *{escape_md(risk_level.upper())}*\n"
        f"• Assets: *{escape_md(products_str)}*\n"
        f"• Max Positions: *{escape_md(str(max_positions))}*\n"
        f"• Max Leverage: *{escape_md(f'{leverage_cap}x')}*\n"
        f"• TP/SL: *{escape_md(f'{tp_pct:.1f}% / {sl_pct:.1f}%')}*\n"
        f"• Min Confidence: *{escape_md(f'{min_confidence:.0%}')}*\n"
        f"• Cycle: *{escape_md(f'{cycle_seconds}s')}*\n"
        f"• Max Loss: *{escape_md(f'{max_loss:.0f}%')}*\n\n"
        "📊 *Statistics*\n"
        f"• Total Volume: *{escape_md(f'${session_volume:,.2f}')}*\n"
        f"• Trades: *{escape_md(str(trade_count))}*\n"
        f"• Open Positions: *{escape_md(str(active_positions))}*\n"
        f"• PnL: *{escape_md(f'{session_pnl:+,.2f} USD')}*\n\n"
        "ℹ️ *How it works*\n"
        "Scans supported markets, scores setups with AI and sentiment, then opens only high-confidence trades under risk guardrails\\."
        + (f"\n\n{warning}" if warning else "")
    )


def _append_mm_pretrade_breakdown(
    telegram_id: int,
    strategy_id: str,
    product: str,
    base_preview: str,
) -> str:
    """Phase 3 pre-trade card: appends a Tread-style breakdown for MM strategies.

    Numbers come from ``mm_dashboard.build_pretrade_breakdown`` so the preview
    and the live ``/mm_status`` command stay in sync.
    """
    try:
        from src.nadobro.services import mm_dashboard
        from src.nadobro.services.nado_archive import get_pair_24h_volume_usd

        network, settings = get_user_settings(telegram_id)
        conf = settings.get("strategies", {}).get(strategy_id, {})
        try:
            from src.nadobro.config import get_product_max_leverage as _gpml
            leverage = float(_gpml(product, network=network))
        except Exception:
            leverage = float(settings.get("default_leverage", 3))
        wallet_collateral = None
        try:
            # F6: read from the per-process TTL cache so we don't double-fetch
            # the balance after _build_strategy_preview_text just fetched it.
            v = _wallet_collateral_usd_from_balance(_cached_user_balance(telegram_id))
            wallet_collateral = v if v > 0 else None
        except Exception:
            wallet_collateral = None
        pair_volume = None
        if conf.get("participation_preset"):
            try:
                pid = get_product_id(product, network=network)
                if pid is not None:
                    pair_volume = get_pair_24h_volume_usd(network=network, product_id=int(pid))
            except Exception:
                pair_volume = None
        breakdown = mm_dashboard.build_pretrade_breakdown(
            strategy_id=strategy_id,
            conf=conf,
            network=network,
            product=product,
            leverage=leverage,
            wallet_collateral_usd=wallet_collateral,
            pair_24h_volume_usd=pair_volume,
        )
        lines = mm_dashboard.render_pretrade_card_lines(breakdown)
    except Exception:
        logger.warning("Failed to render pre-trade breakdown for user=%s strategy=%s", telegram_id, strategy_id, exc_info=True)
        return base_preview

    body = "\n".join(f"• {escape_md(line)}" for line in lines)
    return f"{base_preview}\n\n📐 *Tread Breakdown*\n{body}"


def _build_strategy_preview_text(
    telegram_id: int,
    strategy_id: str,
    product: str,
    vol_market: str | None = None,
) -> str:
    names = {
        "grid": "GRID",
        "rgrid": "Reverse GRID",
        "dgrid": "Dynamic GRID",
        "mid": "Mid Mode",
        "dn": "Delta Neutral",
        "vol": "Volume Bot",
    }
    network, settings = get_user_settings(telegram_id)
    conf = settings.get("strategies", {}).get(strategy_id, {})
    margin_usd = float(conf.get("notional_usd", 100.0))
    cycle_notional_cfg = float(conf.get("cycle_notional_usd", margin_usd))
    spread_bp = float(conf.get("spread_bp", 5.0))
    if strategy_id == "rgrid":
        spread_bp = float(conf.get("rgrid_spread_bp", conf.get("grid_spread_bp", spread_bp)))
    interval_seconds = int(conf.get("interval_seconds", 60))
    tp_pct = float(conf.get("tp_pct", 1.0))
    sl_pct = float(conf.get("sl_pct", 0.5))
    available_margin = 0.0
    mid = 0.0
    funding_rate = 0.0
    # F6: route through the per-process TTL balance cache so the immediate
    # follow-up call from _append_mm_pretrade_breakdown reuses this fetch.
    bal = _cached_user_balance(telegram_id)
    available_margin = _wallet_collateral_usd_from_balance(bal)
    client = get_user_readonly_client(telegram_id)

    # CEO directive (2026-05): MM family and Volume Perp run at per-asset MAX
    # leverage at runtime (mm_bot.py / volume_bot.py overwrite state["leverage"]
    # at cycle start). Reflect that in the preview so the dashboard shows what
    # the bot will actually use, and so the budget preflight uses the real value.
    if strategy_id in ("grid", "rgrid", "dgrid", "mid"):
        try:
            from src.nadobro.config import get_product_max_leverage as _gpml
            leverage = float(_gpml(product, network=network))
        except Exception:
            leverage = float(settings.get("default_leverage", 3))
    elif strategy_id == "vol":
        if (vol_market or "perp").lower() == "spot":
            leverage = 1.0
        else:
            try:
                from src.nadobro.config import get_product_max_leverage as _gpml
                leverage = float(_gpml(product, network=network))
            except Exception:
                leverage = 1.0
    else:
        leverage = float(settings.get("default_leverage", 3))
    if strategy_id == "dn":
        leverage = max(1.0, min(leverage, 5.0))
    wallet_for_preflight = available_margin if available_margin > 0 else None
    mm_budget_ok, mm_collateral_budget, mm_required_min_collateral, mm_min_order_notional, mm_max_quotes_est, mm_margin_per_quote_est = _mm_cycle_budget_preflight(
        strategy_id, conf, leverage, wallet_usdt=wallet_for_preflight
    )

    def _fmt_usd(value: float) -> str:
        return f"${value:,.2f}"

    dn_pair = get_dn_pair(product, network=network, client=client) if strategy_id == "dn" else {}
    if client:
        try:
            user = get_user(telegram_id)
            network = user.network_mode.value if user else "mainnet"
            vm_prev = str(vol_market or "perp").strip().lower() if strategy_id == "vol" else "perp"
            if strategy_id == "vol" and vm_prev == "spot":
                sym = normalize_volume_spot_symbol(product)
                pid = get_spot_product_id(sym, network=network, client=client)
            else:
                pid = get_product_id(product, network=network, client=client)
            if pid is not None:
                mp = client.get_market_price(pid)
                mid = float(mp.get("mid", 0) or 0.0)
                if strategy_id == "vol" and vm_prev == "spot":
                    funding_rate = 0.0
                else:
                    fr = client.get_funding_rate(pid) or {}
                    funding_rate = float(fr.get("funding_rate", 0) or 0.0)
        except Exception:
            pass

    bot_status = get_user_bot_status(telegram_id) or {}
    active_same_strategy = (
        str(bot_status.get("strategy") or "").lower() == strategy_id
        and str(bot_status.get("product") or "").upper() == str(product or "").upper()
    )
    if strategy_id == "vol":
        vm_prev = str(vol_market or "perp").strip().lower()
        st_vm = str(bot_status.get("vol_market") or "perp").strip().lower()
        active_same_strategy = active_same_strategy and st_vm == vm_prev
    if active_same_strategy and bool(bot_status.get("running")):
        status_emoji = "⏸️" if bool(bot_status.get("is_paused")) else "🟢"
        status_label = "PAUSED" if bool(bot_status.get("is_paused")) else "LIVE"
    elif active_same_strategy:
        status_emoji = "⚪"
        status_label = "STOPPED"
    else:
        status_emoji = "🟠"
        status_label = "READY"

    wallet_ready, _wallet_msg = ensure_active_wallet_ready(telegram_id)
    wallet_info = get_user_wallet_info(telegram_id, verify_signer=False) or {}
    wallet_addr = str(wallet_info.get("active_address") or "")
    wallet_short = "N/A"
    if wallet_addr:
        wallet_short = f"{wallet_addr[:6]}...{wallet_addr[-4:]}" if len(wallet_addr) >= 10 else wallet_addr
    account_status = "✅ Connected" if wallet_ready else "⚠️ Setup Needed"

    cycle_notional = (
        max(cycle_notional_cfg, margin_usd * max(1.0, leverage))
        if strategy_id in ("grid", "rgrid", "mid")
        else cycle_notional_cfg
    )
    required_margin = margin_usd if strategy_id in ("grid", "rgrid", "mid") else (cycle_notional / leverage if leverage > 0 else cycle_notional)
    inventory_soft_limit = float(conf.get("inventory_soft_limit_usd", margin_usd * 0.6))
    recommended_buffer = max(5.0, required_margin * 0.20)
    recommended_available = required_margin + (inventory_soft_limit / max(leverage, 1.0)) + recommended_buffer
    mid_str = f"${fmt_price(mid, product)}" if mid > 0 else "N/A"

    # Stats belong to the ONE strategy currently held in bot_status. Only show
    # them on that strategy's dashboard — otherwise the last run (e.g. D-Grid)
    # leaks its volume/PnL onto every other mode's dashboard. Each mode is
    # unique and shows only its own session.
    _stats_owner = str(bot_status.get("strategy") or "").lower() == strategy_id
    trades_count = int(bot_status.get("session_trade_count") or 0) if _stats_owner else 0
    session_volume = float(bot_status.get("session_volume_usd") or 0.0) if _stats_owner else 0.0
    session_fees = float(bot_status.get("session_fees_usd") or 0.0) if _stats_owner else 0.0
    session_pnl = float(bot_status.get("session_analytics_pnl_usd") or 0.0) if _stats_owner else 0.0

    # Route the MM dashboards through the SAME venue-authoritative, per-run
    # source as /status (get_live_session_snapshot) for THIS strategy's latest
    # run. PnL is per-SESSION = realized (this run, gross) + current open uPnL,
    # updated in real time — not a per-position figure. Falls back to the
    # bot_status values above when no session exists or the read fails.
    _snapshot_applied = False
    if strategy_id in ("grid", "rgrid", "dgrid", "mid"):
        try:
            from src.nadobro.models.database import get_strategy_sessions_by_user
            from src.nadobro.services.live_session import get_live_session_snapshot

            _runs = get_strategy_sessions_by_user(
                telegram_id, strategy=strategy_id, network=network, limit=1
            )
            if _runs:
                _snap = get_live_session_snapshot(
                    telegram_id, network, _runs[0], state=conf, client=client
                )
                session_volume = float(_snap.get("volume") or 0.0)
                session_fees = float(_snap.get("fees") or 0.0)
                trades_count = int(_snap.get("fills") or 0)
                session_pnl = float(_snap.get("session_pnl") or 0.0)
                _snapshot_applied = True
        except Exception:  # pragma: no cover - dashboard must always render
            logger.debug("live-session snapshot for dashboard failed", exc_info=True)

    if strategy_id == "vol":
        # Volume is spot-only as of 2026-05. "Session margin" doubles as the
        # per-cycle notional; the bot rotates that amount through one
        # post-only buy → wait fill → post-only sell loop per cycle.
        session_margin = float(conf.get("session_margin_usd") or conf.get("fixed_margin_usd") or 100.0)
        target_volume = float(conf.get("target_volume_usd") or 10000.0)
        sl_pct_vol = float(conf.get("sl_pct") or 0.5)
        from src.nadobro.config import EST_FEE_RATE as _SPOT_FEE_RATE_EST
        maker_fee_bp = float(conf.get("vol_maker_fee_bp") or (_SPOT_FEE_RATE_EST * 10_000.0))
        volume_done = float(bot_status.get("volume_done_usd") or 0.0)
        volume_remaining = float(
            bot_status.get("volume_remaining_usd") or max(0.0, target_volume - volume_done)
        )
        if _stats_owner:
            session_fees = float(bot_status.get("session_fees_usd") or session_fees)
            session_pnl = float(bot_status.get("session_realized_pnl_usd") or session_pnl)
        # Each cycle pushes 2× session_margin of volume (buy leg + sell leg).
        est_cycles = max(1, int((target_volume + (2 * session_margin) - 1) // (2 * session_margin))) if session_margin > 0 else 0
        est_fees_usd = target_volume * (maker_fee_bp / 10_000.0)
        est_pnl_at_sl_usd = -session_margin * (sl_pct_vol / 100.0) if sl_pct_vol > 0 else 0.0
        market_label = f"{str(product).upper()} SPOT"
        phase = str(bot_status.get("vol_phase") or "idle").upper()
        warning = ""
        if not wallet_ready:
            warning = "⚠️ Open Wallet to link your 1CT signer and fund this mode\\."
        elif available_margin < session_margin:
            warning = (
                f"⚠️ Add margin before starting "
                f"\\(need {escape_md(_fmt_usd(session_margin))}\\)\\."
            )
        return (
            "🔁 *Volume Bot Dashboard*\n"
            f"Status: {status_emoji} *{status_label}*\n\n"
            "🔑 *Account*\n"
            f"• Status: *{escape_md(account_status)}*\n"
            f"• Wallet: `{escape_md(wallet_short)}`\n"
            f"• Balance: *{escape_md(_fmt_usd(available_margin))}*\n\n"
            "⚙️ *Configuration*\n"
            f"• Market: *{escape_md(market_label)}*\n"
            f"• Session margin: *{escape_md(_fmt_usd(session_margin))}*\n"
            f"• Stop loss: *{escape_md(f'{sl_pct_vol:.2f}%')}*\n"
            f"• Target volume: *{escape_md(_fmt_usd(target_volume))}*\n\n"
            "🧮 *Pre\\-flight analytics*\n"
            f"• Est\\. cycles to target: *{escape_md(str(est_cycles))}*\n"
            f"• Est\\. fees \\(maker {escape_md(f'{maker_fee_bp:.1f}bp')}\\): *{escape_md(_fmt_usd(est_fees_usd))}*\n"
            f"• Est\\. PnL if SL hits: *{escape_md(f'{est_pnl_at_sl_usd:+,.2f} USD')}*\n"
            f"• Slippage: *post\\-only \\(maker fills only\\)*\n\n"
            "📊 *Live statistics*\n"
            f"• Volume done: *{escape_md(_fmt_usd(volume_done))}* / *{escape_md(_fmt_usd(target_volume))}*\n"
            f"• Volume remaining: *{escape_md(_fmt_usd(volume_remaining))}*\n"
            f"• Fees paid: *{escape_md(_fmt_usd(session_fees))}*\n"
            f"• Realized PnL: *{escape_md(f'{session_pnl:+,.2f} USD')}*\n"
            f"• Phase: *{escape_md(phase)}*\n\n"
            "ℹ️ *How it works*\n"
            "Post\\-only spot buy at the bid → wait for fill → post\\-only sell at the ask\\. "
            "Loop repeats until target volume is reached, or the session SL halts the bot at "
            f"*{escape_md(f'{sl_pct_vol:.2f}%')}* of margin\\."
            + (f"\n\n{warning}" if warning else "")
        )

    if strategy_id == "dgrid":
        levels = int(conf.get("levels", 4) or 4)
        trend_on = float(conf.get("dgrid_trend_on_variance_ratio", 1.25))
        range_on = float(conf.get("dgrid_range_on_variance_ratio", 1.15))
        min_spread = float(conf.get("dgrid_min_spread_bp", 2.0))
        max_spread = float(conf.get("dgrid_max_spread_bp", 50.0))
        phase = str(bot_status.get("dgrid_phase") or "grid").upper()
        variance = float(bot_status.get("dgrid_variance_ratio") or 0.0)
        realized_move = float(bot_status.get("dgrid_realized_move_bp") or 0.0)
        reset_bp = float(bot_status.get("dgrid_reset_threshold_bp") or 0.0)
        if _stats_owner and not _snapshot_applied:
            session_volume = float(bot_status.get("session_notional_done_usd") or session_volume)
            session_pnl = float(bot_status.get("rgrid_last_cycle_pnl_usd") or session_pnl)
        warning = ""
        if not wallet_ready:
            warning = "⚠️ Open Wallet to link your 1CT signer and fund this mode\\."
        elif available_margin < required_margin:
            warning = (
                f"⚠️ Recommended available margin {escape_md(_fmt_usd(recommended_available))} "
                f"\\(trade {escape_md(_fmt_usd(required_margin))} + buffer {escape_md(_fmt_usd(recommended_available - required_margin))}\\)\\."
            )
        if not mm_budget_ok:
            warning = (
                f"⚠️ Collateral looks tight for MM quoting \\(each resting order ~*{escape_md(_fmt_usd(mm_min_order_notional))}* notional\\)\\.\n"
                f"Budget after wallet cap: *{escape_md(_fmt_usd(mm_collateral_budget))}* · Need ~*{escape_md(_fmt_usd(mm_required_min_collateral))}* for one quote "
                f"\\(~*{escape_md(_fmt_usd(mm_margin_per_quote_est))}* est\\. margin at *{escape_md(f'{leverage:.0f}x')}*\\)\\."
            )
        return (
            "⚡ *Dynamic GRID Dashboard*\n"
            f"Status: {status_emoji} *{status_label}*\n\n"
            "🔑 *Account*\n"
            f"• Status: *{escape_md(account_status)}*\n"
            f"• Wallet: `{escape_md(wallet_short)}`\n"
            f"• Balance: *{escape_md(_fmt_usd(available_margin))}*\n\n"
            "⚙️ *Configuration*\n"
            f"• Market: *{escape_md(product)}\\-PERP*\n"
            f"• Margin \\(collateral cap\\): *{escape_md(_fmt_usd(margin_usd))}*\n"
            f"• Est\\. max resting quotes: *{escape_md(str(mm_max_quotes_est))}* \\| "
            f"*~{escape_md(_fmt_usd(mm_margin_per_quote_est))}* margin/quote \\(est\\.\\)\n"
            f"• Levels: *{escape_md(str(levels))}*\n"
            f"• Phase: *{escape_md(phase)}* \\| Variance: *{escape_md(f'{variance:.2f}')}*\n"
            f"• Realized move: *{escape_md(f'{realized_move:.1f}bp')}* \\| Reset: *{escape_md(f'{reset_bp:.1f}bp')}*\n"
            f"• Hysteresis: *{escape_md(f'{range_on:.2f} / {trend_on:.2f}')}* \\| Spread: *{escape_md(f'{min_spread:.0f}-{max_spread:.0f}bp')}*\n\n"
            "📊 *Statistics*\n"
            f"• Total Volume: *{escape_md(_fmt_usd(session_volume))}*\n"
            f"• Total Trades: *{escape_md(str(trades_count))}*\n"
            f"• Fees Paid: *{escape_md(_fmt_usd(session_fees))}*\n"
            f"• PnL: *{escape_md(f'{session_pnl:+,.2f} USD')}*\n\n"
            "ℹ️ *How it works*\n"
            "Automatically switches between GRID in range regimes and RGRID in trend regimes, while resizing spread from recent realized movement\\."
            + (f"\n\n{warning}" if warning else "")
        )

    if strategy_id == "grid":
        levels = int(conf.get("levels", 2) or 2)
        min_spread = float(conf.get("min_spread_bp", 2.0))
        max_spread = float(conf.get("max_spread_bp", 20.0))
        reset_threshold = float(conf.get("grid_reset_threshold_pct", 0.8))
        reset_timeout = int(conf.get("grid_reset_timeout_seconds", 120))
        if _stats_owner and not _snapshot_applied:
            session_volume = float(bot_status.get("session_notional_done_usd") or session_volume)
        warning = ""
        if not wallet_ready:
            warning = "⚠️ Open Wallet to link your 1CT signer and fund this mode\\."
        elif available_margin < required_margin:
            warning = (
                f"⚠️ Recommended available margin {escape_md(_fmt_usd(recommended_available))} "
                f"\\(trade {escape_md(_fmt_usd(required_margin))} + buffer {escape_md(_fmt_usd(recommended_available - required_margin))}\\)\\."
            )
        if not mm_budget_ok:
            warning = (
                f"⚠️ Collateral looks tight for MM quoting \\(each resting order ~*{escape_md(_fmt_usd(mm_min_order_notional))}* notional\\)\\.\n"
                f"Budget after wallet cap: *{escape_md(_fmt_usd(mm_collateral_budget))}* · Need ~*{escape_md(_fmt_usd(mm_required_min_collateral))}* for one quote "
                f"\\(~*{escape_md(_fmt_usd(mm_margin_per_quote_est))}* est\\. margin at *{escape_md(f'{leverage:.0f}x')}*\\)\\."
            )
        return (
            "📊 *GRID Dashboard*\n"
            f"Status: {status_emoji} *{status_label}*\n\n"
            "🔑 *Account*\n"
            f"• Status: *{escape_md(account_status)}*\n"
            f"• Wallet: `{escape_md(wallet_short)}`\n"
            f"• Balance: *{escape_md(_fmt_usd(available_margin))}*\n\n"
            "⚙️ *Configuration*\n"
            f"• Market: *{escape_md(product)}\\-PERP*\n"
            f"• Margin \\(collateral cap\\): *{escape_md(_fmt_usd(margin_usd))}*\n"
            f"• Est\\. max resting quotes: *{escape_md(str(mm_max_quotes_est))}* \\| "
            f"*~{escape_md(_fmt_usd(mm_margin_per_quote_est))}* margin/quote \\(est\\.\\)\n"
            f"• Levels: *{escape_md(str(levels))}*\n"
            f"• Spread: *{escape_md(f'{min_spread:.0f}bp - {max_spread:.0f}bp')}*\n"
            f"• Timing: *{escape_md(f'{interval_seconds}s')}*\n"
            f"• Leverage: *{escape_md(f'MAX ({leverage:.0f}x per-asset)')}*\n"
            f"• Reset: *{escape_md(f'{reset_threshold:.2f}% / {reset_timeout}s')}*\n"
            f"• TP/SL: *{escape_md(f'{tp_pct:.1f}% / {sl_pct:.1f}%')}*\n\n"
            "📊 *Statistics*\n"
            f"• Total Volume: *{escape_md(_fmt_usd(session_volume))}*\n"
            f"• Total Trades: *{escape_md(str(trades_count))}*\n"
            f"• Fees Paid: *{escape_md(_fmt_usd(session_fees))}*\n"
            f"• PnL: *{escape_md(f'{session_pnl:+,.2f} USD')}*\n"
            f"• Est\\. quote depth cap: *{escape_md(str(mm_max_quotes_est))}* resting \\(wallet \\& collateral limited\\)\n\n"
            "ℹ️ *How it works*\n"
            "Places maker-only bids and asks around the market to harvest spread\\."
            + (f"\n\n{warning}" if warning else "")
        )

    if strategy_id == "rgrid":
        levels = int(conf.get("levels", 4) or 4)
        grid_tp = float(conf.get("rgrid_take_profit_pct", conf.get("grid_take_profit_pct", tp_pct)))
        max_loss_pct = float(conf.get("rgrid_stop_loss_pct", conf.get("grid_stop_loss_pct", sl_pct)))
        discretion = float(conf.get("rgrid_discretion", conf.get("grid_discretion", 0.06)))
        reset_threshold = float(conf.get("rgrid_reset_threshold_pct", conf.get("grid_reset_threshold_pct", 1.0)))
        reset_timeout = int(conf.get("rgrid_reset_timeout_seconds", conf.get("grid_reset_timeout_seconds", 120)))
        if _stats_owner and not _snapshot_applied:
            session_volume = float(bot_status.get("session_notional_done_usd") or session_volume)
            session_pnl = float(bot_status.get("rgrid_last_cycle_pnl_usd") or session_pnl)
        warning = ""
        if not wallet_ready:
            warning = "⚠️ Open Wallet to link your 1CT signer and fund this mode\\."
        elif available_margin < required_margin:
            warning = (
                f"⚠️ Recommended available margin {escape_md(_fmt_usd(recommended_available))} "
                f"\\(trade {escape_md(_fmt_usd(required_margin))} + buffer {escape_md(_fmt_usd(recommended_available - required_margin))}\\)\\."
            )
        if not mm_budget_ok:
            warning = (
                f"⚠️ Collateral looks tight for MM quoting \\(each resting order ~*{escape_md(_fmt_usd(mm_min_order_notional))}* notional\\)\\.\n"
                f"Budget after wallet cap: *{escape_md(_fmt_usd(mm_collateral_budget))}* · Need ~*{escape_md(_fmt_usd(mm_required_min_collateral))}* for one quote "
                f"\\(~*{escape_md(_fmt_usd(mm_margin_per_quote_est))}* est\\. margin at *{escape_md(f'{leverage:.0f}x')}*\\)\\."
            )
        return (
            "🧮 *Reverse GRID Dashboard*\n"
            f"Status: {status_emoji} *{status_label}*\n\n"
            "🔑 *Account*\n"
            f"• Status: *{escape_md(account_status)}*\n"
            f"• Wallet: `{escape_md(wallet_short)}`\n"
            f"• Balance: *{escape_md(_fmt_usd(available_margin))}*\n\n"
            "⚙️ *Configuration*\n"
            f"• Market: *{escape_md(product)}\\-PERP*\n"
            f"• Margin \\(collateral cap\\): *{escape_md(_fmt_usd(margin_usd))}*\n"
            f"• Est\\. max resting quotes: *{escape_md(str(mm_max_quotes_est))}* \\| "
            f"*~{escape_md(_fmt_usd(mm_margin_per_quote_est))}* margin/quote \\(est\\.\\)\n"
            f"• Levels: *{escape_md(str(levels))}*\n"
            f"• Spread: *{escape_md(f'{spread_bp:.0f}bp')}*\n"
            f"• Timing: *{escape_md(f'{interval_seconds}s')}*\n"
            f"• Leverage: *{escape_md(f'MAX ({leverage:.0f}x per-asset)')}*\n"
            f"• Reset: *{escape_md(f'{reset_threshold:.2f}% / {reset_timeout}s')}*\n"
            f"• Discretion: *{escape_md(f'{discretion:.2f}')}*\n"
            f"• PnL SL/TP: *{escape_md(f'{max_loss_pct:.2f}% / {grid_tp:.2f}%')}*\n\n"
            "📊 *Statistics*\n"
            f"• Total Volume: *{escape_md(_fmt_usd(session_volume))}*\n"
            f"• Total Trades: *{escape_md(str(trades_count))}*\n"
            f"• Fees Paid: *{escape_md(_fmt_usd(session_fees))}*\n"
            f"• PnL: *{escape_md(f'{session_pnl:+,.2f} USD')}*\n"
            f"• Est\\. quote depth cap: *{escape_md(str(mm_max_quotes_est))}* resting \\(wallet \\& collateral limited\\)\n\n"
            "ℹ️ *How it works*\n"
            "Anchors to exposure and places buy above / sell below to capture continuation\\."
            + (f"\n\n{warning}" if warning else "")
        )

    if strategy_id == "mid":
        levels = int(conf.get("levels", 2) or 2)
        directional_bias = float(conf.get("directional_bias", 0.0) or 0.0)
        warning = ""
        if not wallet_ready:
            warning = "⚠️ Open Wallet to link your 1CT signer and fund this mode\\."
        elif available_margin < required_margin:
            warning = (
                f"⚠️ Recommended available margin {escape_md(_fmt_usd(recommended_available))} "
                f"\\(trade {escape_md(_fmt_usd(required_margin))} + buffer {escape_md(_fmt_usd(recommended_available - required_margin))}\\)\\."
            )
        if not mm_budget_ok:
            warning = (
                f"⚠️ Collateral looks tight for MM quoting \\(each resting order ~*{escape_md(_fmt_usd(mm_min_order_notional))}* notional\\)\\.\n"
                f"Budget after wallet cap: *{escape_md(_fmt_usd(mm_collateral_budget))}* · Need ~*{escape_md(_fmt_usd(mm_required_min_collateral))}* for one quote "
                f"\\(~*{escape_md(_fmt_usd(mm_margin_per_quote_est))}* est\\. margin at *{escape_md(f'{leverage:.0f}x')}*\\)\\."
            )
        return (
            "📈 *Mid Mode Dashboard*\n"
            f"Status: {status_emoji} *{status_label}*\n\n"
            "🔑 *Account*\n"
            f"• Status: *{escape_md(account_status)}*\n"
            f"• Wallet: `{escape_md(wallet_short)}`\n"
            f"• Balance: *{escape_md(_fmt_usd(available_margin))}*\n\n"
            "⚙️ *Configuration*\n"
            f"• Market: *{escape_md(product)}\\-PERP*\n"
            f"• Margin \\(collateral cap\\): *{escape_md(_fmt_usd(margin_usd))}*\n"
            f"• Est\\. max resting quotes: *{escape_md(str(mm_max_quotes_est))}* \\| "
            f"*~{escape_md(_fmt_usd(mm_margin_per_quote_est))}* margin/quote \\(est\\.\\)\n"
            f"• Levels: *{escape_md(str(levels))}*\n"
            f"• Spread: *{escape_md(f'{spread_bp:.0f}bp')}*\n"
            f"• Directional bias: *{escape_md(f'{directional_bias:+.2f}')}*\n"
            f"• Timing: *{escape_md(f'{interval_seconds}s')}*\n"
            f"• Leverage: *{escape_md(f'MAX ({leverage:.0f}x per-asset)')}*\n"
            f"• TP/SL: *{escape_md(f'{tp_pct:.2f}% / {sl_pct:.2f}%')}*\n\n"
            "📊 *Statistics*\n"
            f"• Total Volume: *{escape_md(_fmt_usd(session_volume))}*\n"
            f"• Total Trades: *{escape_md(str(trades_count))}*\n"
            f"• Fees Paid: *{escape_md(_fmt_usd(session_fees))}*\n"
            f"• PnL: *{escape_md(f'{session_pnl:+,.2f} USD')}*\n\n"
            "ℹ️ *How it works*\n"
            "Quotes a two\\-sided market around mid \\(maker\\-only\\) to capture the "
            "spread; optional directional bias skews the quotes\\."
            + (f"\n\n{warning}" if warning else "")
        )

    funding_bias = "FAVORABLE" if funding_rate > 0.000001 else "UNFAVORABLE"
    auto_close = "ON" if float(conf.get("auto_close_on_maintenance", 1) or 0) >= 0.5 else "OFF"
    # Engine-v2 DN settings (what the controller actually runs).
    dn_leg_size = float(conf.get("fixed_margin_usd", margin_usd) or margin_usd)
    dn_hold_label = _fmt_hold_duration(int(conf.get("dn_hold_seconds", 3600) or 3600))
    dn_cycles = int(conf.get("dn_cycles", 1) or 1)
    dn_cycles_label = str(dn_cycles) + (
        "" if dn_cycles == 1 else f" · {int(conf.get('dn_cycle_gap_seconds', 30) or 0)}s gap"
    )
    dn_spot_symbol = str(dn_pair.get("spot_symbol") or product).upper()
    dn_perp_symbol = str(dn_pair.get("perp_symbol") or f"{product}-PERP").upper()
    dn_market_label = f"{dn_spot_symbol} spot / {dn_perp_symbol}"
    # Economics over the configured hold × cycles. The short earns funding when
    # the daily rate is positive; it accrues on the perp notional (≈ per-leg size
    # at 1x) pro-rated over the hold, summed across cycles. Sign is preserved, so
    # an unfavorable rate shows as a negative contribution, not a phantom gain.
    # (The old preview multiplied |rate| × margin × 3 — wrong magnitude AND it
    # hid the sign; funding is a daily rate, not a 3-interval one.)
    from src.nadobro.config import EST_FEE_RATE
    dn_hold_seconds = int(conf.get("dn_hold_seconds", 3600) or 3600)
    dn_est_funding = funding_rate * (dn_hold_seconds / 86400.0) * dn_leg_size * dn_cycles
    # Round-trip taker fees: open + close on BOTH legs each cycle (4 fills × per-leg notional).
    dn_est_fees = 4.0 * dn_leg_size * EST_FEE_RATE * dn_cycles
    dn_est_net = dn_est_funding - dn_est_fees
    dn_net_dot = "🟢" if dn_est_net >= 0 else "🟠"
    warning = ""
    if not wallet_ready:
        warning = "⚠️ Open Wallet to link your 1CT signer and fund this mode\\."
    elif available_margin < required_margin:
        warning = f"⚠️ Add margin before starting \\(need {escape_md(_fmt_usd(required_margin))}\\)\\."
    elif strategy_id == "dn" and dn_pair and not bool(dn_pair.get("entry_allowed", True)):
        warning = f"⚠️ {escape_md(str(dn_pair.get('entry_block_reason') or 'This DN pair is not currently tradable.'))}"
    elif strategy_id == "dn" and not dn_pair:
        warning = "⚠️ This DN pair is not currently available\\."
    return (
        "⚖️ *Delta Neutral Dashboard*\n"
        f"Strategy Status: {status_emoji} *{status_label}*\n\n"
        "📊 *Your Stats*\n"
        f"• Volume Traded: *{escape_md(_fmt_usd(session_volume))}*\n"
        f"• Positions Created: *{escape_md(str(trades_count))}*\n"
        f"• Fees Paid: *{escape_md(_fmt_usd(session_fees))}*\n"
        f"• Funding: *{escape_md(f'{funding_rate:.6f}')}*\n\n"
        "🔑 *Exchange Account*\n"
        f"• Status: *{escape_md(account_status)}*\n"
        f"• Wallet: `{escape_md(wallet_short)}`\n"
        f"• Balance: *{escape_md(_fmt_usd(available_margin))}*\n\n"
        "⚙️ *Current Settings*\n"
        f"• Market: *{escape_md(dn_market_label)}*\n"
        f"• Size \\(per leg\\): *{escape_md(_fmt_usd(dn_leg_size))}*\n"
        f"• Short Leverage: *1x* \\(fixed\\)\n"
        f"• Min hold: *{escape_md(dn_hold_label)}*\n"
        f"• Cycles: *{escape_md(dn_cycles_label)}*\n"
        f"• Auto-close on maintenance: *{escape_md(auto_close)}*\n\n"
        "ℹ️ *How it works*\n"
        "Buys spot \\+ 1x\\-shorts the same perp, holds for *at least* the min hold, then keeps the "
        "hedge open while funding stays favorable and closes BOTH legs the moment funding flips "
        "\\(or you close it\\)\\. Farms spot \\+ perp volume and funding while staying delta neutral\\.\n"
        f"Funding now: *{escape_md(funding_bias)}* \\({escape_md(f'{funding_rate * 100:+.4f}')}%/day\\)\n"
        f"Est\\. over {escape_md(dn_hold_label)} × {escape_md(str(dn_cycles))}: "
        f"funding *{escape_md(_fmt_usd(dn_est_funding))}* · fees *{escape_md(_fmt_usd(dn_est_fees))}* · "
        f"net {dn_net_dot} *{escape_md(_fmt_usd(dn_est_net))}*"
        + (f"\n\n{warning}" if warning else "")
    )


