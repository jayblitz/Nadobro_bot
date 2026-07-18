"""Telegram handlers for the Nado NLP Vault (deposit / withdraw flows)."""

from __future__ import annotations

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import CallbackContext

from src.nadobro.core.async_utils import run_blocking
from src.nadobro.vault.nlp_vault_service import (
    deposit_to_vault,
    estimate_withdraw_fee_usdt0,
    get_user_vault_snapshot,
    withdraw_from_vault,
)
from src.nadobro.users.user_service import get_user
from src.nadobro.vault.vault_deposit_watch_service import (
    disable_deposit_watch,
    enable_deposit_watch,
)

logger = logging.getLogger(__name__)

_DEPOSIT_PENDING_KEY = "vault_pending_amount"

DEPOSIT_PRESETS_USDT0 = (100.0, 500.0, 1_000.0, 5_000.0)
WITHDRAW_PRESETS_PCT = (25, 50, 75, 100)


def _fmt_usd(value: float) -> str:
    return f"${value:,.2f}"


def _fmt_signed_usd(value: float) -> str:
    if value >= 0:
        return f"+{_fmt_usd(value)}"
    return f"-{_fmt_usd(abs(value))}"


def _fmt_lockup(seconds: int) -> str:
    if seconds <= 0:
        return "Unlocked"
    hours = seconds / 3600.0
    if hours >= 24:
        return f"{hours / 24:.1f}d remaining"
    return f"{hours:.1f}h remaining"


def _fmt_apr(snapshot: dict) -> str:
    pool = snapshot.get("pool") or {}
    apr = pool.get("apr_pct")
    if apr is None:
        return "—"
    return f"{float(apr):.2f}%"


def _user_network(telegram_id: int) -> str:
    user = get_user(telegram_id)
    if not user:
        return "mainnet"
    return str(getattr(getattr(user, "network_mode", None), "value", None) or "mainnet")


def _vault_home_card(snapshot: dict) -> tuple[str, InlineKeyboardMarkup]:
    if snapshot.get("error"):
        text = f"💰 *Nado Vault*\n\n⚠️ {snapshot['error']}"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="vault:refresh")],
            [InlineKeyboardButton("🏠 Home", callback_data="nav:main")],
        ])
        return text, kb

    usdt0 = float(snapshot.get("usdt0_balance") or 0.0)
    lp_balance = float(snapshot.get("lp_balance") or 0.0)
    position = float(snapshot.get("position_usdt0") or snapshot.get("lp_value_usdt0") or 0.0)
    all_time = float(snapshot.get("all_time_earned_usdt0") or 0.0)
    unrealized = float(snapshot.get("unrealized_pnl_usdt0") or 0.0)
    room = float(snapshot.get("deposit_room_usdt0") or 0.0)
    max_mintable = float(snapshot.get("max_mintable_usdt0") or 0.0)
    lockup = int(snapshot.get("lockup_seconds_remaining") or 0)
    pool = snapshot.get("pool") or {}
    tvl = float(pool.get("tvl_usdt0") or 0.0)
    watch_enabled = bool(snapshot.get("deposit_watch_enabled"))

    lines = [
        "💰 *Nado Liquidity Provider (NLP)*",
        "",
        f"TVL: `{_fmt_usd(tvl)}`    APR: `{_fmt_apr(snapshot)}`",
        "",
        "*Your Position*",
        f"Position: `{_fmt_usd(position)}`",
        f"Balance: `{lp_balance:.6f}` NLP",
        f"All-time Earned: `{_fmt_signed_usd(all_time)}`",
        f"Unrealized PnL: `{_fmt_signed_usd(unrealized)}`",
        "",
        # "USDT0 balance", NOT "Idle": this is the raw spot balance from
        # subaccount_info, and on a cross-margin venue it simultaneously backs
        # open positions/orders. Labeling it "Idle" contradicted the
        # margin-locked explainer directly below it and sent users hunting for
        # a phantom deposit blocker.
        f"USDT0 balance: `{_fmt_usd(usdt0)}`",
        f"Free to deposit (no borrow): `{_fmt_usd(max_mintable)}`",
        f"Deposit room: `{_fmt_usd(room)}`",
        f"Lockup: `{_fmt_lockup(lockup)}`",
    ]
    margin_locked = snapshot.get("deposit_blocked_reason") == "margin_locked"
    if max_mintable <= 1.0:
        borrow_mintable = float(snapshot.get("mintable_with_borrow_usdt0") or 0.0)
        lines.append("")
        if margin_locked:
            lines.append(
                f"Deposits are *open*, but your `{_fmt_usd(usdt0)}` USDT0 is "
                "currently backing open positions or resting orders (margin), "
                "so none of it can enter the vault without borrowing — and a "
                "vault mint never borrows against your trading account."
            )
            if borrow_mintable > 1.0:
                lines.append(
                    f"(Nado would allow `{_fmt_usd(borrow_mintable)}` *with* "
                    "borrowing — the bot keeps this off deliberately.)"
                )
            lines.append(
                "Close/reduce positions, cancel resting orders, or deposit "
                "more USDT0 to free margin for minting."
            )
        else:
            lines.append("Vault capacity is currently *closed* for new deposits.")
    text = "\n".join(lines)

    deposit_btn = InlineKeyboardButton("⬇️ Deposit", callback_data="vault:deposit")
    if max_mintable <= 1.0:
        deposit_btn = (
            InlineKeyboardButton("🔒 Margin in use", callback_data="vault:home")
            if margin_locked
            else InlineKeyboardButton("⛔ Deposits closed", callback_data="vault:home")
        )

    watch_label = "🔕 Stop deposit alerts" if watch_enabled else "🔔 Notify when deposits open"
    watch_cb = "vault:watch:off" if watch_enabled else "vault:watch:on"

    kb = InlineKeyboardMarkup([
        [deposit_btn, InlineKeyboardButton("⬆️ Withdraw", callback_data="vault:withdraw")],
        [InlineKeyboardButton(watch_label, callback_data=watch_cb)],
        [InlineKeyboardButton("🔄 Refresh", callback_data="vault:refresh")],
        [InlineKeyboardButton("◀ Back", callback_data="nav:strategy_hub")],
    ])
    return text, kb


def _deposit_picker(snapshot: dict) -> tuple[str, InlineKeyboardMarkup]:
    usdt0 = float(snapshot.get("usdt0_balance") or 0.0)
    room = float(snapshot.get("deposit_room_usdt0") or 0.0)
    max_deposit = min(usdt0, room)
    text = (
        "⬇️ *Deposit USDT0 → NLP*\n\n"
        f"USDT0 balance: `{_fmt_usd(usdt0)}`\n"
        f"Deposit room: `{_fmt_usd(room)}`\n"
        f"Max you can deposit now: `{_fmt_usd(max_deposit)}`\n\n"
        "Choose an amount:"
    )
    rows = []
    presets = [a for a in DEPOSIT_PRESETS_USDT0 if a <= max_deposit + 1e-9]
    if presets:
        row = []
        for amt in presets:
            row.append(InlineKeyboardButton(f"${int(amt):,}", callback_data=f"vault:deposit:preset:{amt}"))
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
    if max_deposit > 0:
        rows.append([InlineKeyboardButton(f"Max ({_fmt_usd(max_deposit)})", callback_data=f"vault:deposit:preset:{max_deposit}")])
    rows.append([InlineKeyboardButton("✍️ Custom amount", callback_data="vault:deposit:custom")])
    rows.append([InlineKeyboardButton("◀ Back", callback_data="vault:home")])
    return text, InlineKeyboardMarkup(rows)


def _withdraw_picker(snapshot: dict) -> tuple[str, InlineKeyboardMarkup]:
    lp_balance = float(snapshot.get("lp_balance") or 0.0)
    lockup = int(snapshot.get("lockup_seconds_remaining") or 0)
    lines = [
        "⬆️ *Withdraw NLP → USDT0*",
        "",
        f"NLP balance: `{lp_balance:.6f}`",
        f"Lockup: `{_fmt_lockup(lockup)}`",
    ]
    if lockup > 0:
        lines.append("")
        lines.append("⏳ Burns are blocked until the 4-day post-mint lockup ends.")
    else:
        lines.append("")
        lines.append("Fees on burn: $1 sequencer + max($1, 10 bps of withdrawn).")
        lines.append("Choose a percentage:")
    text = "\n".join(lines)

    rows: list[list[InlineKeyboardButton]] = []
    if lockup <= 0 and lp_balance > 0:
        row = []
        for pct in WITHDRAW_PRESETS_PCT:
            row.append(InlineKeyboardButton(f"{pct}%", callback_data=f"vault:withdraw:pct:{pct}"))
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([InlineKeyboardButton("✍️ Custom NLP amount", callback_data="vault:withdraw:custom")])
    rows.append([InlineKeyboardButton("◀ Back", callback_data="vault:home")])
    return text, InlineKeyboardMarkup(rows)


def _deposit_confirm_card(amount_usdt0: float) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "⬇️ *Confirm Deposit*\n\n"
        f"Mint NLP using `{_fmt_usd(amount_usdt0)}` USDT0.\n"
        "Borrow protection is on (`spot_leverage=false`) so this will be rejected "
        "if it would force a margin borrow.\n\n"
        "Proceed?"
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm", callback_data=f"vault:deposit:confirm:{amount_usdt0}"),
            InlineKeyboardButton("❌ Cancel", callback_data="vault:home"),
        ],
    ])
    return text, kb


def _withdraw_confirm_card(nlp_amount: float, usdt0_estimate: float, fee_estimate: float) -> tuple[str, InlineKeyboardMarkup]:
    net = max(0.0, usdt0_estimate - fee_estimate)
    text = (
        "⬆️ *Confirm Withdraw*\n\n"
        f"Burn `{nlp_amount:.6f}` NLP.\n"
        f"Estimated USDT0 out (pre-fees): `{_fmt_usd(usdt0_estimate)}`\n"
        f"Estimated fees: `{_fmt_usd(fee_estimate)}`\n"
        f"Net to your account (approx.): `{_fmt_usd(net)}`\n\n"
        "Final amount is settled at the vault's NAV at burn time."
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirm", callback_data=f"vault:withdraw:confirm:{nlp_amount}"),
            InlineKeyboardButton("❌ Cancel", callback_data="vault:home"),
        ],
    ])
    return text, kb


async def _show_home(query, telegram_id: int, *, flash: str | None = None) -> None:
    snapshot = await run_blocking(get_user_vault_snapshot, telegram_id)
    text, kb = _vault_home_card(snapshot)
    if flash:
        text = f"{flash}\n\n{text}"
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)


async def handle_vault_callback(query, context: CallbackContext) -> bool:
    data = (query.data or "").strip()
    if not data.startswith("vault:"):
        return False
    telegram_id = int(query.from_user.id)
    network = _user_network(telegram_id)
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "home"

    if action in ("home", "refresh"):
        await _show_home(query, telegram_id)
        return True

    if action == "watch" and len(parts) >= 3:
        sub = parts[2]
        if sub == "on":
            ok, msg = await run_blocking(enable_deposit_watch, telegram_id, network)
        else:
            ok, msg = await run_blocking(disable_deposit_watch, telegram_id, network)
        prefix = "✅" if ok else "⚠️"
        await _show_home(query, telegram_id, flash=f"{prefix} {msg}")
        return True

    if action == "deposit":
        snapshot = await run_blocking(get_user_vault_snapshot, telegram_id)
        sub = parts[2] if len(parts) > 2 else ""
        if sub == "":
            if float(snapshot.get("max_mintable_usdt0") or 0.0) <= 1.0:
                if snapshot.get("deposit_blocked_reason") == "margin_locked":
                    flash = (
                        "🔒 Your USDT0 is backing open positions — a vault mint never "
                        "borrows against your trading account. Close/reduce positions "
                        "or deposit more USDT0, then try again."
                    )
                else:
                    flash = "⚠️ Vault deposit capacity is closed right now."
                await _show_home(query, telegram_id, flash=flash)
                return True
            text, kb = _deposit_picker(snapshot)
            await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
            return True
        if sub == "preset" and len(parts) >= 4:
            try:
                amount = float(parts[3])
            except ValueError:
                amount = 0.0
            text, kb = _deposit_confirm_card(amount)
            await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
            return True
        if sub == "custom":
            context.user_data[_DEPOSIT_PENDING_KEY] = "deposit"
            await query.edit_message_text(
                "✍️ Reply with the USDT0 amount you want to deposit (e.g. `250`).",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel", callback_data="vault:home")],
                ]),
            )
            return True
        if sub == "confirm" and len(parts) >= 4:
            try:
                amount = float(parts[3])
            except ValueError:
                amount = 0.0
            result = await run_blocking(deposit_to_vault, telegram_id, amount)
            await _show_result(query, telegram_id, result, default_success=f"Deposited ${amount:,.2f} USDT0.")
            return True

    if action == "withdraw":
        snapshot = await run_blocking(get_user_vault_snapshot, telegram_id)
        sub = parts[2] if len(parts) > 2 else ""
        if sub == "":
            text, kb = _withdraw_picker(snapshot)
            await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
            return True
        if sub == "pct" and len(parts) >= 4:
            try:
                pct = max(0, min(100, int(parts[3])))
            except ValueError:
                pct = 0
            lp_balance = float(snapshot.get("lp_balance") or 0.0)
            lp_value = float(snapshot.get("lp_value_usdt0") or 0.0)
            nlp_amount = lp_balance * (pct / 100.0)
            est_usdt0 = lp_value * (pct / 100.0)
            est_fee = estimate_withdraw_fee_usdt0(est_usdt0)
            text, kb = _withdraw_confirm_card(nlp_amount, est_usdt0, est_fee)
            await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
            return True
        if sub == "custom":
            context.user_data[_DEPOSIT_PENDING_KEY] = "withdraw"
            await query.edit_message_text(
                "✍️ Reply with the NLP token amount to burn (e.g. `1.25`).",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel", callback_data="vault:home")],
                ]),
            )
            return True
        if sub == "confirm" and len(parts) >= 4:
            try:
                nlp_amount = float(parts[3])
            except ValueError:
                nlp_amount = 0.0
            result = await run_blocking(withdraw_from_vault, telegram_id, nlp_amount)
            await _show_result(query, telegram_id, result, default_success=f"Burned {nlp_amount:.6f} NLP.")
            return True

    await _show_home(query, telegram_id)
    return True


async def _show_result(query, telegram_id: int, result: dict, *, default_success: str) -> None:
    if result.get("success"):
        flash = f"✅ {default_success}"
        digest = result.get("digest")
        if digest:
            flash += f"\nDigest: `{str(digest)[:18]}…`"
    else:
        flash = f"⚠️ {result.get('error') or 'Operation failed.'}"
    await _show_home(query, telegram_id, flash=flash)


async def handle_vault_text(update: Update, context: CallbackContext) -> bool:
    pending = (context.user_data.get(_DEPOSIT_PENDING_KEY) or "").strip()
    if pending not in {"deposit", "withdraw"}:
        return False
    if not update.message or not update.effective_user:
        return False
    raw = (update.message.text or "").strip().lstrip("$").replace(",", "")
    try:
        amount = float(raw)
    except ValueError:
        await update.message.reply_text("Please reply with a positive number, e.g. `250`.")
        return True
    if amount <= 0:
        await update.message.reply_text("Amount must be greater than zero.")
        return True
    context.user_data.pop(_DEPOSIT_PENDING_KEY, None)
    telegram_id = int(update.effective_user.id)
    if pending == "deposit":
        text, kb = _deposit_confirm_card(amount)
    else:
        snapshot = await run_blocking(get_user_vault_snapshot, telegram_id)
        lp_balance = float(snapshot.get("lp_balance") or 0.0)
        lp_value = float(snapshot.get("lp_value_usdt0") or 0.0)
        if lp_balance <= 0:
            await update.message.reply_text("You don't have any NLP to burn yet.")
            return True
        est_usdt0 = lp_value * (amount / lp_balance) if lp_balance > 0 else 0.0
        est_fee = estimate_withdraw_fee_usdt0(est_usdt0)
        text, kb = _withdraw_confirm_card(amount, est_usdt0, est_fee)
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    return True
