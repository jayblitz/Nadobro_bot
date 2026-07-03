"""Desk text-to-trade handler: NL message -> preview card -> confirm -> plan.

Flow: ``handle_desk_text`` parses the message (regex fast-path, then LLM),
validates deterministically, resolves %-triggers against the mid the user is
shown, persists a DRAFT, and renders an HTML preview card with inline
Confirm / Cancel buttons. ``handle_desk_callback`` owns the ``desk:*``
namespace: confirm (daily-cap gated, guarded draft->awaiting_trigger
transition), discard, the "My Desk" plan list, and cancelling active plans
(the runner pulls resting orders within a tick; fills are always kept).

Everything here is HTML parse mode built with ``visual.esc``/``b`` — the
parse-mode lint applies. All DB/SDK calls go through run_blocking.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from src.nadobro.handlers.callbacks import _edit_loc
from src.nadobro.i18n import get_active_language, localize_text
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services import desk_store
from src.nadobro.services.desk_parser import looks_like_desk_text, parse_desk_intent
from src.nadobro.services.desk_plans import (
    ST_AWAITING_TRIGGER,
    ST_DRAFT,
    ST_RUNNING,
    ExecutionPlan,
    daily_plan_cap,
    describe_trigger,
    resolve_catalogs,
    resolve_trigger,
    validate_plan,
)
from src.nadobro.services.onboarding_service import get_resume_step
from src.nadobro.services.user_service import (
    ensure_active_wallet_ready,
    get_user,
    get_user_readonly_client,
)
from src.nadobro.utils.visual import b, divider, esc, price as _px, qty as _qty

logger = logging.getLogger(__name__)

DRAFT_TTL_SECONDS = 600.0

_CLARIFY_HINTS = {
    "product": "which asset (e.g. ETH, BTC, QQQX)",
    "size": "how much (e.g. 0.5 ETH or $500)",
    "side": "buy/sell (spot) or long/short (perp)",
}


def _network_of(telegram_id: int) -> str:
    user = get_user(telegram_id)
    return user.network_mode.value if user else "mainnet"


def _mid_for_plan(telegram_id: int, plan: ExecutionPlan, network: str) -> Optional[float]:
    """Mid price from the product's own book (spot id for spot, perp id for perp)."""
    from src.nadobro.config import get_product_id, get_spot_product_id

    client = get_user_readonly_client(telegram_id, network=network)
    if client is None:
        return None
    if plan.market == "spot":
        pid = get_spot_product_id(plan.product, network=network)
    else:
        pid = get_product_id(plan.product, network=network, client=client)
    if pid is None:
        return None
    price = client.get_market_price(int(pid)) or {}
    mid = float(price.get("mid") or 0.0)
    return mid if mid > 0 else None


def _balance_note(telegram_id: int, plan: ExecutionPlan, network: str, mid: float) -> str:
    """Best-effort affordability line for the preview card (never blocks)."""
    from src.nadobro.config import get_spot_product_id

    try:
        client = get_user_readonly_client(telegram_id, network=network)
        if client is None:
            return ""
        balances = (client.get_balance() or {}).get("balances", {}) or {}
        usdt0 = float(balances.get(0, balances.get("0", 0)) or 0.0)
        notional = float(plan.size_quote or (plan.size_base or 0) * mid)
        if plan.market == "spot" and plan.side == "sell":
            pid = get_spot_product_id(plan.product, network=network)
            held = float(balances.get(pid, balances.get(str(pid), 0)) or 0.0) if pid is not None else 0.0
            need = float(plan.size_base or (notional / mid if mid else 0))
            ok = held >= need * 0.999
            return f"{'✅' if ok else '⚠️'} You hold {held:,.6f} {plan.product} (need {need:,.6f})"
        need = notional / float(plan.leverage or 1) if plan.market == "perp" else notional
        ok = usdt0 >= need * 0.98
        what = "margin" if plan.market == "perp" else "USDT0"
        return f"{'✅' if ok else '⚠️'} Needs ~${need:,.2f} {what}, you have ${usdt0:,.2f}"
    except Exception:  # noqa: BLE001 - preview must render even if balances are down
        logger.warning("desk: balance note failed", exc_info=True)
        return ""


# ---------------------------------------------------------------------------
# rendering (HTML)
# ---------------------------------------------------------------------------

def _exits_line(plan: ExecutionPlan) -> str:
    ex = plan.exits
    if ex is None or ex.is_empty():
        return ""
    bits = []
    if ex.tp_pct is not None:
        bits.append(f"TP +{ex.tp_pct:g}%")
    if ex.tp_price is not None:
        bits.append(f"TP {_px(ex.tp_price)}")
    if ex.sl_pct is not None:
        bits.append(f"SL -{ex.sl_pct:g}%")
    if ex.sl_price is not None:
        bits.append(f"SL {_px(ex.sl_price)}")
    if ex.trailing_pct is not None:
        bits.append(f"trail {ex.trailing_pct:g}%")
    return " / ".join(bits)


def render_preview_card(plan: ExecutionPlan, mid: float, balance_note: str) -> str:
    market_badge = "🟢 SPOT" if plan.market == "spot" else "🟣 PERP"
    side_txt = plan.side.upper() if plan.side else "?"
    if plan.market == "perp":
        side_txt = "LONG" if plan.side == "buy" else "SHORT"
    size_txt = (
        f"${plan.size_quote:,.2f}" if plan.size_quote
        else f"{_qty(plan.size_base)} {plan.product}"
    )
    notional = float(plan.size_quote or (plan.size_base or 0) * mid)
    lines = [
        f"🧾 {b('Desk plan preview')}  ·  {b(esc(market_badge))}",
        divider(),
        f"{b(esc(side_txt))} {b(esc(plan.product or '?'))} · {esc(size_txt)}",
    ]
    if plan.algo == "twap":
        slices = plan.twap_slices() or 0
        mode = "maker" if plan.exec_mode == "maker" else "taker"
        lines.append(esc(
            f"TWAP over {plan.duration_minutes:g} min · {slices} slices "
            f"every {plan.interval_seconds:g}s · {mode}"
        ))
    elif plan.algo == "limit":
        lines.append(esc(f"Limit order at {_px(plan.limit_price)}"))
    else:
        lines.append(esc("Market order"))
    if plan.market == "perp" and plan.leverage:
        lines.append(esc(f"Leverage: {plan.leverage}x"))
    lines.append(esc(f"Starts: {describe_trigger(plan.entry_trigger)}"))
    exits = _exits_line(plan)
    if exits:
        lines.append(esc(f"Exits: {exits} (vs actual avg entry)"))
    lines.append(divider())
    lines.append(esc(f"Mid now {_px(mid)} · est. notional ~${notional:,.2f}"))
    if balance_note:
        lines.append(esc(balance_note))
    lines.append("")
    lines.append(esc("Nothing executes until you confirm."))
    return "\n".join(lines)


def _preview_kb(plan_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm", callback_data=f"desk:confirm:{plan_id}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"desk:discard:{plan_id}"),
        ],
        [InlineKeyboardButton("🧾 My Desk", callback_data="desk:view")],
    ])


_STATUS_BADGE = {
    ST_AWAITING_TRIGGER: "⏳ waiting for trigger",
    ST_RUNNING: "▶️ running",
    "completed": "✅ done",
    "cancelled": "🛑 cancelled",
    "failed": "❌ failed",
}


def render_desk_view(active: list[dict], recent: list[dict]) -> str:
    lines = [f"🧾 {b('Desk plans')}", divider()]
    if not active and not recent:
        lines.append(esc("No plans yet. Try: \"Accumulate 5 ETH over 24h, but only "
                         "start once ETH dumps 2%\""))
        return "\n".join(lines)
    for rec in active:
        plan: ExecutionPlan = rec["plan"]
        badge = _STATUS_BADGE.get(str(rec.get("status")), str(rec.get("status")))
        lines.append(f"{b(esc(plan.describe()))}")
        state = rec.get("state") or {}
        filled_q = float(state.get("filled_quote") or 0)
        target_q = float(state.get("target_quote") or 0)
        if target_q > 0:
            badge += f" · {min(100, int(filled_q / target_q * 100))}% filled"
        lines.append(esc(badge))
        lines.append("")
    done = [r for r in recent if str(r.get("status")) not in (ST_AWAITING_TRIGGER, ST_RUNNING)]
    if done:
        lines.append(divider())
        for rec in done[:5]:
            plan = rec["plan"]
            badge = _STATUS_BADGE.get(str(rec.get("status")), str(rec.get("status")))
            err = rec.get("error")
            tail = f": {err}" if err else ""
            lines.append(esc(f"{badge}: {plan.describe()}{tail}"))
    return "\n".join(lines)


def _desk_view_kb(active: list[dict]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(
            f"🛑 Stop {((rec['plan'].product or '?'))} {rec['plan'].algo.upper()}",
            callback_data=f"desk:stop:{rec['plan'].plan_id}",
        )]
        for rec in active[:6]
    ]
    rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="desk:view")])
    return InlineKeyboardMarkup(rows)


# ---------------------------------------------------------------------------
# text entry point (called from messages.py before the legacy trade parser)
# ---------------------------------------------------------------------------

LLM_PARSE_TIMEOUT_SECONDS = 12.0


async def handle_desk_text(update, context, telegram_id: int, text: str) -> bool:
    if not looks_like_desk_text(text):
        return False
    network = await run_blocking(_network_of, telegram_id)
    try:
        perps, spots = await run_blocking(resolve_catalogs, network, None)
    except Exception:  # noqa: BLE001 - catalog down: let the legacy parser try
        logger.warning("desk: catalog resolution failed", exc_info=True)
        return False

    # Fast path first (deterministic, zero latency). Only consult the LLM
    # tier when gaps remain, and never let a slow provider stall the chat —
    # the underlying client timeout is 45s with retries, far too long here.
    result = await run_blocking(
        parse_desk_intent, text, perp_symbols=perps, spot_symbols=spots, allow_llm=False,
    )
    if not (result.plan is not None and not result.clarify):
        import asyncio

        try:
            result = await asyncio.wait_for(
                run_blocking(parse_desk_intent, text, perp_symbols=perps,
                             spot_symbols=spots, allow_llm=True),
                timeout=LLM_PARSE_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning("desk: llm parse timed out — using fast-path result")
    if result.not_trade or result.plan is None:
        return False
    plan = result.plan

    lang = get_active_language()
    step = await run_blocking(get_resume_step, telegram_id)
    if step != "complete":
        await update.message.reply_text(
            localize_text("⚠️ Finish onboarding first. Resume at {step}.", lang)
            .format(step=str(step).upper()),
        )
        return True
    wallet_ready, wallet_msg = await run_blocking(ensure_active_wallet_ready, telegram_id)
    if not wallet_ready:
        await update.message.reply_text(f"⚠️ {wallet_msg}")
        return True

    if result.clarify:
        wants = "; ".join(_CLARIFY_HINTS.get(c, c) for c in result.clarify)
        await update.message.reply_text(
            localize_text("Almost there. I still need: {wants}.", lang).format(wants=wants)
            + "\n" + localize_text("Reply with the full instruction in one message.", lang),
        )
        return True

    max_lev = None
    if plan.market == "perp" and plan.product:
        from src.nadobro.config import get_product_max_leverage

        try:
            max_lev = await run_blocking(get_product_max_leverage, plan.product, network)
        except Exception:  # noqa: BLE001
            max_lev = None

    mid = await run_blocking(_mid_for_plan, telegram_id, plan, network)
    problems = validate_plan(
        plan, perp_symbols=perps, spot_symbols=spots,
        max_leverage=max_lev, mid_price=mid,
    )
    if mid is None:
        problems.append(f"Could not fetch a live price for {plan.product or 'that product'}.")
    if problems:
        body = "\n".join(f"• {p}" for p in problems)
        await update.message.reply_text(
            localize_text("I can't run this plan yet:", lang) + f"\n{body}",
        )
        return True

    # Pin %-moves / directionless levels to the price shown on this card.
    if plan.entry_trigger is not None:
        plan.entry_trigger = resolve_trigger(plan.entry_trigger, arrival_mid=mid)

    balance_note = await run_blocking(_balance_note, telegram_id, plan, network, mid)
    row_id = await run_blocking(desk_store.insert_draft, telegram_id, plan, network)
    if row_id is None:
        await update.message.reply_text(localize_text("Could not stage the plan. Try again.", lang))
        return True

    await update.message.reply_text(
        render_preview_card(plan, mid, balance_note),
        parse_mode=ParseMode.HTML,
        reply_markup=_preview_kb(plan.plan_id),
    )
    return True


# ---------------------------------------------------------------------------
# callbacks: desk:*
# ---------------------------------------------------------------------------

async def handle_desk_callback(query, data: str, telegram_id: int, context) -> None:
    network = await run_blocking(_network_of, telegram_id)
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "view"

    if action == "view":
        active = await run_blocking(desk_store.list_active_plans, telegram_id, network)
        recent = await run_blocking(desk_store.list_recent_plans, telegram_id, network, 8)
        await _edit_loc(
            query, render_desk_view(active, recent),
            parse_mode=ParseMode.HTML, reply_markup=_desk_view_kb(active),
        )
        return

    plan_id = parts[2] if len(parts) > 2 else ""
    rec = await run_blocking(desk_store.get_plan, plan_id, network) if plan_id else None
    lang = get_active_language()

    if action == "confirm":
        if not rec or int(rec.get("user_id") or 0) != int(telegram_id):
            await _edit_loc(query, localize_text("This plan is gone. Send the instruction again.", lang))
            return
        if rec.get("status") != ST_DRAFT:
            await _edit_loc(query, localize_text("Already handled. Check My Desk.", lang),
                            reply_markup=_preview_kb_view_only())
            return
        plan: ExecutionPlan = rec["plan"]
        if time.time() - float(plan.created_ts or 0) > DRAFT_TTL_SECONDS:
            await _edit_loc(query, localize_text(
                "This preview expired (prices moved). Send the instruction again.", lang))
            return
        cap = daily_plan_cap()
        used = await run_blocking(desk_store.count_confirmed_today, telegram_id, network)
        if used >= cap:
            await _edit_loc(query, localize_text(
                "Daily Desk limit reached ({used}/{cap} plans today). Try again tomorrow.", lang)
                .format(used=used, cap=cap))
            return
        ok = await run_blocking(desk_store.confirm_plan, plan_id, telegram_id, network, plan)
        if not ok:
            await _edit_loc(query, localize_text("Could not arm the plan. It may have expired.", lang))
            return
        await _edit_loc(
            query,
            localize_text(
                "✅ Plan armed ({used}/{cap} today). I'll notify you when it starts, "
                "fills, and completes.", lang).format(used=used + 1, cap=cap),
            reply_markup=_preview_kb_view_only(),
        )
        return

    if action == "discard":
        if rec and rec.get("status") == ST_DRAFT and int(rec.get("user_id") or 0) == int(telegram_id):
            await run_blocking(desk_store.discard_draft, plan_id, telegram_id, network)
        await _edit_loc(query, localize_text("Plan discarded. Nothing was placed.", lang))
        return

    if action == "stop":
        ok = await run_blocking(desk_store.cancel_plan, plan_id, telegram_id, network)
        if ok:
            await _edit_loc(query, localize_text(
                "🛑 Cancelling. Resting orders are being pulled. Anything already "
                "filled stays in your account.", lang),
                reply_markup=_preview_kb_view_only())
        else:
            await _edit_loc(query, localize_text("That plan is not active anymore.", lang),
                            reply_markup=_preview_kb_view_only())
        return

    await _edit_loc(query, localize_text("Unknown Desk action.", lang))


def _preview_kb_view_only() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🧾 My Desk", callback_data="desk:view")]])


# ---------------------------------------------------------------------------
# /desk command
# ---------------------------------------------------------------------------

async def cmd_desk(update, context) -> None:
    telegram_id = update.effective_user.id
    network = await run_blocking(_network_of, telegram_id)
    active = await run_blocking(desk_store.list_active_plans, telegram_id, network)
    recent = await run_blocking(desk_store.list_recent_plans, telegram_id, network, 8)
    await update.message.reply_text(
        render_desk_view(active, recent),
        parse_mode=ParseMode.HTML,
        reply_markup=_desk_view_kb(active),
    )
